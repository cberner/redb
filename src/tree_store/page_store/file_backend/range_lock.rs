//! Byte-range file locks, the primitive the multi-process locking protocol coordinates
//! through (see docs/design.md). Every lock here belongs to the open file description it was
//! taken through, so two handles on one file -- in one process or in two -- contend exactly
//! the same way, and a lock never outlives its description.
//!
//! On Unix these are open file description locks, never POSIX record locks: a record lock
//! belongs to the process, and is released when any descriptor for the file is closed
//! anywhere in it -- which a library embedded in an arbitrary program cannot rule out. On
//! Windows they are `LockFileEx` region locks, which are per-handle and so carry the same
//! ownership model, and are mandatory rather than advisory. Windows handles must be
//! ordinary synchronous ones, as everything std opens is: a handle opened for asynchronous
//! I/O queues a lock request instead of answering it, and is refused here.
//!
//! Two behaviors every caller must design around, both properties of the description
//! ownership: a lock never conflicts with its own description, so a probe is blind to locks
//! its own handle holds; and re-locking a byte through one description does not nest -- the
//! second acquisition is absorbed and a single release drops the byte -- so a handle that can
//! hold one byte for several reasons has to count them itself.

#[cfg(any(test, redb_multiprocess))]
use std::fs::File;
#[cfg(any(test, redb_multiprocess))]
use std::io;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum LockKind {
    Shared,
    Exclusive,
}

/// What a probe of a range found. Windows cannot report where a conflicting lock starts, so
/// `Conflict { start: None }` must be handled wherever a probe is used.
#[cfg(redb_multiprocess)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Probe {
    Free,
    Conflict { start: Option<u64> },
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
mod imp {
    use super::LockKind;
    #[cfg(redb_multiprocess)]
    use super::Probe;
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    // The lock-type constants are c_int on Linux and already c_short on the Apple platforms
    #[cfg(target_os = "linux")]
    fn lock_type(kind: libc::c_int) -> libc::c_short {
        kind.try_into().unwrap()
    }

    #[cfg(target_vendor = "apple")]
    fn lock_type(kind: libc::c_short) -> libc::c_short {
        kind
    }

    fn kind_type(kind: LockKind) -> libc::c_short {
        lock_type(match kind {
            LockKind::Shared => libc::F_RDLCK,
            LockKind::Exclusive => libc::F_WRLCK,
        })
    }

    fn flock_struct(kind: libc::c_short, start: u64, len: u64) -> libc::flock {
        // Zeroed rather than written field by field: the layout of struct flock differs
        // between Linux and the Apple platforms, and libc carries the right one for each
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = kind;
        lock.l_whence = libc::SEEK_SET.try_into().unwrap();
        lock.l_start = libc::off_t::try_from(start).unwrap();
        lock.l_len = libc::off_t::try_from(len).unwrap();
        lock
    }

    pub(crate) fn try_lock(file: &File, start: u64, len: u64, kind: LockKind) -> io::Result<bool> {
        let mut lock = flock_struct(kind_type(kind), start, len);
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &raw mut lock) };
        if rc == 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EAGAIN | libc::EACCES) => Ok(false),
            _ => Err(err),
        }
    }

    /// Waits for the range. The kernel provides no deadlock detection for these locks, the
    /// same as for the whole-file locks the ordinary modes take.
    #[cfg(redb_multiprocess)]
    pub(crate) fn lock_blocking(
        file: &File,
        start: u64,
        len: u64,
        kind: LockKind,
    ) -> io::Result<()> {
        loop {
            let mut lock = flock_struct(kind_type(kind), start, len);
            let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLKW, &raw mut lock) };
            if rc == 0 {
                return Ok(());
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(err);
            }
        }
    }

    pub(crate) fn unlock(file: &File, start: u64, len: u64) -> io::Result<()> {
        let mut lock = flock_struct(lock_type(libc::F_UNLCK), start, len);
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &raw mut lock) };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Asks whether the range is free without acquiring it, reporting where a conflicting
    /// lock starts. Blind to locks held through `file`'s own description.
    #[cfg(redb_multiprocess)]
    pub(crate) fn probe(file: &File, start: u64, len: u64, kind: LockKind) -> io::Result<Probe> {
        let mut lock = flock_struct(kind_type(kind), start, len);
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &raw mut lock) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // The lock-type constants are c_int on Linux and c_short on Apple platforms, so both
        // sides are widened rather than compared directly
        if i32::from(lock.l_type) == i32::from(lock_type(libc::F_UNLCK)) {
            Ok(Probe::Free)
        } else {
            Ok(Probe::Conflict {
                start: Some(u64::try_from(lock.l_start).unwrap()),
            })
        }
    }

    /// The filesystem does not support range locks (fcntl on a kernel or filesystem without
    /// them reports `EINVAL` or a not-supported error)
    pub(crate) fn unsupported(err: &io::Error) -> bool {
        matches!(err.raw_os_error(), Some(code) if code == libc::EINVAL
            || code == libc::ENOTSUP
            || code == libc::EOPNOTSUPP)
    }
}

#[cfg(windows)]
mod imp {
    use super::LockKind;
    #[cfg(redb_multiprocess)]
    use super::Probe;
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    type Handle = *mut core::ffi::c_void;
    type Dword = u32;
    type Bool = i32;

    const LOCKFILE_FAIL_IMMEDIATELY: Dword = 0x0000_0001;
    const LOCKFILE_EXCLUSIVE_LOCK: Dword = 0x0000_0002;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    const ERROR_IO_PENDING: i32 = 997;

    #[repr(C)]
    struct Overlapped {
        internal: usize,
        internal_high: usize,
        offset: Dword,
        offset_high: Dword,
        event: Handle,
    }

    unsafe extern "system" {
        fn LockFileEx(
            file: Handle,
            flags: Dword,
            reserved: Dword,
            bytes_low: Dword,
            bytes_high: Dword,
            overlapped: *mut Overlapped,
        ) -> Bool;
        fn UnlockFileEx(
            file: Handle,
            reserved: Dword,
            bytes_low: Dword,
            bytes_high: Dword,
            overlapped: *mut Overlapped,
        ) -> Bool;
    }

    // The Win32 calls take 64-bit offsets and lengths as dword halves
    fn low_dword(value: u64) -> Dword {
        Dword::try_from(value & u64::from(Dword::MAX)).unwrap()
    }

    fn high_dword(value: u64) -> Dword {
        Dword::try_from(value >> 32).unwrap()
    }

    fn overlapped_at(start: u64) -> Overlapped {
        Overlapped {
            internal: 0,
            internal_high: 0,
            offset: low_dword(start),
            offset_high: high_dword(start),
            event: core::ptr::null_mut(),
        }
    }

    // The Overlapped is heap-allocated because a handle opened for asynchronous I/O -- which
    // nothing in the crate creates and std's file operations cannot use -- queues the request
    // instead of refusing it, and the kernel later completes into this storage. On that path
    // the box is leaked, so the completion lands in memory that stays valid, and the caller
    // gets an error naming the handle unsupported.
    fn asynchronous_handles_unsupported(overlapped: Box<Overlapped>) -> io::Error {
        Box::leak(overlapped);
        io::Error::other("file handles opened for asynchronous I/O are not supported")
    }

    fn lock_with_flags(file: &File, start: u64, len: u64, flags: Dword) -> io::Result<bool> {
        let mut overlapped = Box::new(overlapped_at(start));
        let ok = unsafe {
            LockFileEx(
                file.as_raw_handle(),
                flags,
                0,
                low_dword(len),
                high_dword(len),
                &raw mut *overlapped,
            )
        };
        if ok != 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(ERROR_LOCK_VIOLATION) => Ok(false),
            Some(ERROR_IO_PENDING) => Err(asynchronous_handles_unsupported(overlapped)),
            _ => Err(err),
        }
    }

    fn exclusive_flag(kind: LockKind) -> Dword {
        match kind {
            LockKind::Shared => 0,
            LockKind::Exclusive => LOCKFILE_EXCLUSIVE_LOCK,
        }
    }

    pub(crate) fn try_lock(file: &File, start: u64, len: u64, kind: LockKind) -> io::Result<bool> {
        lock_with_flags(
            file,
            start,
            len,
            exclusive_flag(kind) | LOCKFILE_FAIL_IMMEDIATELY,
        )
    }

    #[cfg(redb_multiprocess)]
    pub(crate) fn lock_blocking(
        file: &File,
        start: u64,
        len: u64,
        kind: LockKind,
    ) -> io::Result<()> {
        if lock_with_flags(file, start, len, exclusive_flag(kind))? {
            Ok(())
        } else {
            Err(io::Error::other("blocking range lock reported a conflict"))
        }
    }

    pub(crate) fn unlock(file: &File, start: u64, len: u64) -> io::Result<()> {
        let mut overlapped = Box::new(overlapped_at(start));
        let ok = unsafe {
            UnlockFileEx(
                file.as_raw_handle(),
                0,
                low_dword(len),
                high_dword(len),
                &raw mut *overlapped,
            )
        };
        if ok != 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(ERROR_IO_PENDING) {
            return Err(asynchronous_handles_unsupported(overlapped));
        }
        Err(err)
    }

    /// Windows has no query operation, so a probe is an acquisition that is released at
    /// once. It therefore excludes other holders for an instant, and it cannot say where a
    /// conflicting lock starts.
    #[cfg(redb_multiprocess)]
    pub(crate) fn probe(file: &File, start: u64, len: u64, kind: LockKind) -> io::Result<Probe> {
        if try_lock(file, start, len, kind)? {
            unlock(file, start, len)?;
            Ok(Probe::Free)
        } else {
            Ok(Probe::Conflict { start: None })
        }
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) use imp::unsupported;
#[cfg(redb_multiprocess)]
pub(crate) use imp::{lock_blocking, probe};
pub(crate) use imp::{try_lock, unlock};

/// Every offset the file can address: a length of zero means exactly this to fcntl, and it
/// is the only way to say it, since an explicit length cannot reach the last addressable
/// byte with the signed offsets fcntl takes. On Windows, where the ordinary open modes rely
/// on std's own whole-file lock instead, no equivalent is needed.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) const WHOLE_FILE_LEN: u64 = 0;

#[cfg(any(test, redb_multiprocess))]
pub(crate) fn try_lock_byte(file: &File, offset: u64, kind: LockKind) -> io::Result<bool> {
    try_lock(file, offset, 1, kind)
}

#[cfg(all(redb_multiprocess, test))]
pub(crate) fn lock_byte_blocking(file: &File, offset: u64, kind: LockKind) -> io::Result<()> {
    lock_blocking(file, offset, 1, kind)
}

#[cfg(any(test, redb_multiprocess))]
pub(crate) fn unlock_byte(file: &File, offset: u64) -> io::Result<()> {
    unlock(file, offset, 1)
}
