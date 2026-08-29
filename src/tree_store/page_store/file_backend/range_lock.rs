use std::fs::File;
use std::io;
use std::ops::Range;
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
use std::os::unix::io::AsRawFd;

pub(crate) trait RangeLock {
    /// Whether these locks and [`File::lock`]'s conflict.
    /// `None` indicates that it can only be determined at runtime, e.g. because it is filesystem
    /// dependent.
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

    /// Whether an exclusive lock over the range would conflict with one already held.
    /// [`File::lock`] is included wherever it would in fact block a range lock
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

// LockFileEx region locks: per-handle, so they carry the same ownership model as open file
// description locks, and mandatory rather than advisory. Unlike fcntl's, they do not split or
// merge -- a lock is released only by an unlock over the range it was taken over, and a range
// this handle already holds cannot be locked again -- so each range is taken and released whole.
// The calls are the ones std's own whole-file locks use, made over a range rather than the file.
#[cfg(windows)]
mod windows_imp {
    use super::{File, RangeLock};
    use std::io;
    use std::ops::Range;
    use std::os::windows::io::AsRawHandle;

    type Handle = *mut core::ffi::c_void;
    type Dword = u32;
    type Bool = i32;

    const LOCKFILE_FAIL_IMMEDIATELY: Dword = 0x0000_0001;
    const LOCKFILE_EXCLUSIVE_LOCK: Dword = 0x0000_0002;
    const ERROR_LOCK_VIOLATION: i32 = 33;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: Dword,
        offset_high: Dword,
        event: Handle,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LockFileEx(
            file: Handle,
            flags: Dword,
            reserved: Dword,
            bytes_low: Dword,
            bytes_high: Dword,
            overlapped: *mut Overlapped,
        ) -> Bool;
        fn UnlockFile(
            file: Handle,
            offset_low: Dword,
            offset_high: Dword,
            bytes_low: Dword,
            bytes_high: Dword,
        ) -> Bool;
    }

    // The Win32 calls take 64-bit offsets and lengths as dword halves
    fn low_dword(value: u64) -> Dword {
        Dword::try_from(value & u64::from(Dword::MAX)).unwrap()
    }

    fn high_dword(value: u64) -> Dword {
        Dword::try_from(value >> 32).unwrap()
    }

    fn try_lock(file: &File, exclusive: bool, range: Range<u64>) -> io::Result<bool> {
        debug_assert!(!range.is_empty());
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
        } else {
            LOCKFILE_FAIL_IMMEDIATELY
        };
        let len = range.end - range.start;
        // Only the offset is carried, and the struct is left on the stack, as std's try_lock
        // does: LOCKFILE_FAIL_IMMEDIATELY is answered rather than queued, so nothing completes
        // into it after the call returns
        let mut overlapped = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: low_dword(range.start),
            offset_high: high_dword(range.start),
            event: core::ptr::null_mut(),
        };
        let ok = unsafe {
            LockFileEx(
                file.as_raw_handle(),
                flags,
                0,
                low_dword(len),
                high_dword(len),
                &raw mut overlapped,
            )
        };
        if ok != 0 {
            return Ok(true);
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION) {
            Ok(false)
        } else {
            Err(err)
        }
    }

    impl RangeLock for File {
        // std's whole-file lock is itself a LockFileEx over every byte, so it is one of these
        const CONFLICTS_WITH_STD_FILE_LOCK: Option<bool> = Some(true);

        fn try_lock_range(&self, range: Range<u64>) -> io::Result<bool> {
            try_lock(self, true, range)
        }

        fn try_lock_shared_range(&self, range: Range<u64>) -> io::Result<bool> {
            try_lock(self, false, range)
        }

        // UnlockFile rather than its Ex form, as std's unlock uses: it takes the range as
        // arguments, so there is no overlapped structure for a queued request to complete into.
        // One call releases the lock because the protocol takes one per range, where std has to
        // unlock twice for a handle that took both an exclusive and a shared lock.
        fn unlock_range(&self, range: Range<u64>) -> io::Result<()> {
            debug_assert!(!range.is_empty());
            let len = range.end - range.start;
            let ok = unsafe {
                UnlockFile(
                    self.as_raw_handle(),
                    low_dword(range.start),
                    high_dword(range.start),
                    low_dword(len),
                    high_dword(len),
                )
            };
            if ok != 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }

        // There is no query operation, so the range is acquired and released again to answer
        fn query_lock(&self, range: Range<u64>) -> io::Result<bool> {
            if try_lock(self, true, range.clone())? {
                self.unlock_range(range)?;
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }
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
