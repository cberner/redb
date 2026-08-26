use std::fs::File;
use std::io;
use std::ops::Range;
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
use std::os::unix::io::AsRawFd;

/// Byte-range locks, which the multi-process locking protocol (docs/design.md) coordinates
/// through. The methods default to reporting the locks unsupported, which is what platforms
/// with no implementation below get.
pub(crate) trait RangeLock {
    /// Whether these locks and [`File::lock`]'s conflict, which decides whether a handle
    /// excluding both kinds of holder must take one of each. `None` where the answer belongs
    /// to the filesystem rather than the platform -- Linux keeps the two apart, but a network
    /// filesystem may emulate one with the other -- and [`RangeLock::query_lock`] asks.
    const CONFLICTS_WITH_STD_FILE_LOCK: Option<bool> = None;

    /// `Ok(false)` means a conflicting lock is held elsewhere. An end of `u64::MAX` covers
    /// the file however it grows, past the last offset fcntl's signed arguments can express.
    fn try_lock_range(&self, _range: Range<u64>) -> io::Result<bool> {
        Err(unsupported())
    }

    fn try_lock_shared_range(&self, _range: Range<u64>) -> io::Result<bool> {
        Err(unsupported())
    }

    fn unlock_range(&self, _range: Range<u64>) -> io::Result<()> {
        Err(unsupported())
    }

    /// Whether an exclusive lock over the range would conflict with one already held. The
    /// locks this description holds are not conflicts, but [`File::lock`]'s is wherever it
    /// would in fact block a range lock taken here, which is what makes this the way to
    /// answer [`RangeLock::CONFLICTS_WITH_STD_FILE_LOCK`] at runtime.
    fn query_lock(&self, _range: Range<u64>) -> io::Result<bool> {
        Err(unsupported())
    }
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "byte-range locks are not supported on this platform",
    )
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
impl RangeLock for File {}

// std's whole-file lock is itself a LockFileEx over every byte, so it is one of these
#[cfg(windows)]
impl RangeLock for File {
    const CONFLICTS_WITH_STD_FILE_LOCK: Option<bool> = Some(true);
}

// The lock-type constants are c_int on Linux and already c_short on the Apple platforms
#[cfg(target_os = "linux")]
fn lock_type(kind: libc::c_int) -> libc::c_short {
    kind.try_into().unwrap()
}

#[cfg(target_vendor = "apple")]
fn lock_type(kind: libc::c_short) -> libc::c_short {
    kind
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn flock_struct(kind: libc::c_short, range: Range<u64>) -> libc::flock {
    debug_assert!(!range.is_empty());
    let len = if range.end == u64::MAX {
        0
    } else {
        range.end - range.start
    };
    // Zeroed rather than written field by field: struct flock's layout differs between Linux
    // and the Apple platforms
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = kind;
    lock.l_whence = libc::SEEK_SET.try_into().unwrap();
    lock.l_start = libc::off_t::try_from(range.start).unwrap();
    lock.l_len = libc::off_t::try_from(len).unwrap();
    lock
}

/// The last lock failure, as `Unsupported` where the filesystem has no byte-range locks.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn lock_error() -> io::Error {
    let err = io::Error::last_os_error();
    if matches!(err.raw_os_error(), Some(code) if code == libc::EINVAL
        || code == libc::ENOTSUP
        || code == libc::EOPNOTSUPP)
    {
        io::Error::new(io::ErrorKind::Unsupported, err)
    } else {
        err
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn set_lock(file: &File, exclusive: bool, range: Range<u64>) -> io::Result<bool> {
    let kind = lock_type(if exclusive {
        libc::F_WRLCK
    } else {
        libc::F_RDLCK
    });
    let mut lock = flock_struct(kind, range);
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &raw mut lock) };
    if rc == 0 {
        return Ok(true);
    }
    let err = lock_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN | libc::EACCES) => Ok(false),
        _ => Err(err),
    }
}

// Open file description locks, never POSIX record locks: a record lock belongs to the
// process, and is released when any descriptor for the file is closed anywhere in it,
// which a library embedded in an arbitrary program cannot rule out.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
impl RangeLock for File {
    // The Apple platforms answer flock() with the same locks fcntl() takes; Linux keeps them
    // apart, except where a network filesystem emulates one with the other
    const CONFLICTS_WITH_STD_FILE_LOCK: Option<bool> = if cfg!(target_vendor = "apple") {
        Some(true)
    } else {
        None
    };

    fn try_lock_range(&self, range: Range<u64>) -> io::Result<bool> {
        set_lock(self, true, range)
    }

    fn try_lock_shared_range(&self, range: Range<u64>) -> io::Result<bool> {
        set_lock(self, false, range)
    }

    fn unlock_range(&self, range: Range<u64>) -> io::Result<()> {
        let mut lock = flock_struct(lock_type(libc::F_UNLCK), range);
        let rc = unsafe { libc::fcntl(self.as_raw_fd(), libc::F_OFD_SETLK, &raw mut lock) };
        if rc == 0 { Ok(()) } else { Err(lock_error()) }
    }

    fn query_lock(&self, range: Range<u64>) -> io::Result<bool> {
        let mut lock = flock_struct(lock_type(libc::F_WRLCK), range);
        let rc = unsafe { libc::fcntl(self.as_raw_fd(), libc::F_OFD_GETLK, &raw mut lock) };
        if rc != 0 {
            return Err(lock_error());
        }
        // The lock-type constants are c_int on Linux and c_short on the Apple platforms, so
        // both sides are widened rather than compared directly
        Ok(i32::from(lock.l_type) != i32::from(lock_type(libc::F_UNLCK)))
    }
}
