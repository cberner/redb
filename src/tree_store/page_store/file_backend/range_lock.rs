use std::fs::File;
use std::io;
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
use std::ops::Bound;
use std::ops::RangeBounds;
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
use std::os::unix::io::AsRawFd;

#[cfg(any(
    all(target_os = "linux", not(target_env = "gnu")),
    target_vendor = "apple"
))]
use libc::flock as Flock;
#[cfg(all(target_os = "linux", target_env = "gnu"))]
use libc::flock64 as Flock;

pub(crate) trait RangeLock {
    /// `Ok(false)` means a conflicting lock is held elsewhere. An unbounded end covers the
    /// file however it grows, past the last offset fcntl's signed arguments can express.
    fn try_lock_range(&self, _range: impl RangeBounds<u64>) -> io::Result<bool> {
        Err(unsupported())
    }

    fn try_lock_shared_range(&self, _range: impl RangeBounds<u64>) -> io::Result<bool> {
        Err(unsupported())
    }

    /// Waits for the range. No deadlock detection, like the whole-file locks.
    fn lock_range(&self, _range: impl RangeBounds<u64>) -> io::Result<()> {
        Err(unsupported())
    }

    fn lock_shared_range(&self, _range: impl RangeBounds<u64>) -> io::Result<()> {
        Err(unsupported())
    }

    fn unlock_range(&self, _range: impl RangeBounds<u64>) -> io::Result<()> {
        Err(unsupported())
    }

    /// Whether an exclusive lock over the range would conflict with one already held.
    /// [`File::lock`] is included wherever it would in fact block a range lock
    #[cfg_attr(feature = "experimental-api-5", allow(dead_code))]
    fn query_lock(&self, _range: impl RangeBounds<u64>) -> io::Result<bool> {
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

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
fn checked_range(range: impl RangeBounds<u64>) -> io::Result<(u64, u64)> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty or unrepresentable lock range",
        )
    };
    let start = match range.start_bound() {
        Bound::Unbounded => 0,
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start.checked_add(1).ok_or_else(invalid)?,
    };
    let end = match range.end_bound() {
        Bound::Unbounded => None,
        Bound::Included(end) => Some(end.checked_add(1).ok_or_else(invalid)?),
        Bound::Excluded(end) => Some(*end),
    };
    // Unix uses a zero length for an unbounded end; Windows needs an explicit length.
    #[cfg(windows)]
    let end = Some(end.unwrap_or(u64::MAX));
    if end.is_some_and(|end| start >= end) {
        return Err(invalid());
    }
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    if end.is_some_and(|end| end - 1 > i64::MAX as u64) {
        return Err(invalid());
    }
    Ok((start, end.map_or(0, |end| end - start)))
}

// LockFileEx region locks: per-handle, so they carry the same ownership model as open file
// description locks, and mandatory rather than advisory. Unlike fcntl's, they do not split or
// merge -- a lock is released only by an unlock over the range it was taken over, and a range
// this handle already holds cannot be locked again -- so each range is taken and released whole.
// The calls are the ones std's own whole-file locks use, made over a range rather than the file.
#[cfg(windows)]
mod windows_imp {
    use super::{File, RangeLock, checked_range};
    use std::io;
    use std::ops::RangeBounds;
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
        fn CreateEventW(
            attributes: *mut core::ffi::c_void,
            manual_reset: Bool,
            initial_state: Bool,
            name: *const u16,
        ) -> Handle;
        fn GetOverlappedResult(
            file: Handle,
            overlapped: *mut Overlapped,
            transferred: *mut Dword,
            wait: Bool,
        ) -> Bool;
        fn CloseHandle(handle: Handle) -> Bool;
    }

    // The Win32 calls take 64-bit offsets and lengths as dword halves
    fn low_dword(value: u64) -> Dword {
        Dword::try_from(value & u64::from(Dword::MAX)).unwrap()
    }

    fn high_dword(value: u64) -> Dword {
        Dword::try_from(value >> 32).unwrap()
    }

    fn try_lock(file: &File, exclusive: bool, range: impl RangeBounds<u64>) -> io::Result<bool> {
        let (start, len) = checked_range(range)?;
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
        } else {
            LOCKFILE_FAIL_IMMEDIATELY
        };
        // Only the offset is carried, and the struct is left on the stack, as std's try_lock
        // does: LOCKFILE_FAIL_IMMEDIATELY is answered rather than queued, so nothing completes
        // into it after the call returns
        let mut overlapped = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: low_dword(start),
            offset_high: high_dword(start),
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

    // Waits for the range, the way std's own File::lock waits for the whole file. A handle
    // opened for asynchronous I/O answers ERROR_IO_PENDING and completes the request later,
    // which is what the event is for: waiting on it settles the request before this returns,
    // so the overlapped structure is safe on the stack.
    fn lock_blocking(file: &File, exclusive: bool, range: impl RangeBounds<u64>) -> io::Result<()> {
        let (start, len) = checked_range(range)?;
        let flags = if exclusive {
            LOCKFILE_EXCLUSIVE_LOCK
        } else {
            0
        };
        let handle = file.as_raw_handle();
        unsafe {
            let event = CreateEventW(core::ptr::null_mut(), 0, 0, core::ptr::null());
            if event.is_null() {
                return Err(io::Error::last_os_error());
            }
            let mut overlapped = Overlapped {
                internal: 0,
                internal_high: 0,
                offset: low_dword(start),
                offset_high: high_dword(start),
                event,
            };
            let acquired = LockFileEx(
                handle,
                flags,
                0,
                low_dword(len),
                high_dword(len),
                &raw mut overlapped,
            );

            let result = if acquired != 0 {
                Ok(())
            } else {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(ERROR_IO_PENDING) {
                    let mut transferred: Dword = 0;
                    let completed =
                        GetOverlappedResult(handle, &raw mut overlapped, &raw mut transferred, 1);
                    if completed != 0 {
                        Ok(())
                    } else {
                        Err(io::Error::last_os_error())
                    }
                } else {
                    Err(err)
                }
            };
            CloseHandle(event);

            result
        }
    }

    impl RangeLock for File {
        fn try_lock_range(&self, range: impl RangeBounds<u64>) -> io::Result<bool> {
            try_lock(self, true, range)
        }

        fn try_lock_shared_range(&self, range: impl RangeBounds<u64>) -> io::Result<bool> {
            try_lock(self, false, range)
        }

        fn lock_range(&self, range: impl RangeBounds<u64>) -> io::Result<()> {
            lock_blocking(self, true, range)
        }

        fn lock_shared_range(&self, range: impl RangeBounds<u64>) -> io::Result<()> {
            lock_blocking(self, false, range)
        }

        // UnlockFile rather than its Ex form, as std's unlock uses: it takes the range as
        // arguments, so there is no overlapped structure for a queued request to complete into.
        // One call releases the lock because the protocol takes one per range, where std has to
        // unlock twice for a handle that took both an exclusive and a shared lock.
        fn unlock_range(&self, range: impl RangeBounds<u64>) -> io::Result<()> {
            let (start, len) = checked_range(range)?;
            let ok = unsafe {
                UnlockFile(
                    self.as_raw_handle(),
                    low_dword(start),
                    high_dword(start),
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
        fn query_lock(&self, range: impl RangeBounds<u64>) -> io::Result<bool> {
            let range = (range.start_bound().cloned(), range.end_bound().cloned());
            if try_lock(self, true, range)? {
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
fn flock_struct(kind: libc::c_short, range: impl RangeBounds<u64>) -> io::Result<Flock> {
    let (start, len) = checked_range(range)?;
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "unrepresentable lock range");
    // Zeroed rather than written field by field: struct flock's layout differs between Linux
    // and the Apple platforms. A zero length covers all future growth.
    let mut lock: Flock = unsafe { std::mem::zeroed() };
    lock.l_type = kind;
    lock.l_whence = libc::SEEK_SET.try_into().unwrap();
    lock.l_start = start.try_into().map_err(|_| invalid())?;
    lock.l_len = len.try_into().map_err(|_| invalid())?;
    Ok(lock)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn fcntl_lock(file: &File, command: libc::c_int, lock: &mut Flock) -> libc::c_long {
    // GNU's off_t can be 32 bits. Use the kernel's large-file ABI directly: glibc's fcntl
    // translates through off_t, and its fcntl64 symbol requires glibc 2.28 or later.
    // x32 and RISC-V already use 64-bit offsets with SYS_fcntl.
    #[cfg(any(
        target_pointer_width = "64",
        target_arch = "x86_64",
        target_arch = "riscv32"
    ))]
    use libc::SYS_fcntl as SYSCALL;
    #[cfg(not(any(
        target_pointer_width = "64",
        target_arch = "x86_64",
        target_arch = "riscv32"
    )))]
    use libc::SYS_fcntl64 as SYSCALL;

    unsafe { libc::syscall(SYSCALL, file.as_raw_fd(), command, &raw mut *lock) }
}

#[cfg(any(
    all(target_os = "linux", not(target_env = "gnu")),
    target_vendor = "apple"
))]
fn fcntl_lock(file: &File, command: libc::c_int, lock: &mut Flock) -> libc::c_int {
    unsafe { libc::fcntl(file.as_raw_fd(), command, &raw mut *lock) }
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
fn set_lock(file: &File, exclusive: bool, range: impl RangeBounds<u64>) -> io::Result<bool> {
    let kind = lock_type(if exclusive {
        libc::F_WRLCK
    } else {
        libc::F_RDLCK
    });
    let mut lock = flock_struct(kind, range)?;
    let rc = fcntl_lock(file, libc::F_OFD_SETLK, &mut lock);
    if rc == 0 {
        return Ok(true);
    }
    let err = lock_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN | libc::EACCES) => Ok(false),
        _ => Err(err),
    }
}

// EINTR is the caller's signal handler having run, not a failure to take the lock.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn set_lock_blocking(file: &File, exclusive: bool, range: impl RangeBounds<u64>) -> io::Result<()> {
    let kind = lock_type(if exclusive {
        libc::F_WRLCK
    } else {
        libc::F_RDLCK
    });
    let mut lock = flock_struct(kind, range)?;
    loop {
        let rc = fcntl_lock(file, libc::F_OFD_SETLKW, &mut lock);
        if rc == 0 {
            return Ok(());
        }
        let err = lock_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
impl RangeLock for File {
    fn try_lock_range(&self, range: impl RangeBounds<u64>) -> io::Result<bool> {
        set_lock(self, true, range)
    }

    fn try_lock_shared_range(&self, range: impl RangeBounds<u64>) -> io::Result<bool> {
        set_lock(self, false, range)
    }

    fn lock_range(&self, range: impl RangeBounds<u64>) -> io::Result<()> {
        set_lock_blocking(self, true, range)
    }

    fn lock_shared_range(&self, range: impl RangeBounds<u64>) -> io::Result<()> {
        set_lock_blocking(self, false, range)
    }

    fn unlock_range(&self, range: impl RangeBounds<u64>) -> io::Result<()> {
        let mut lock = flock_struct(lock_type(libc::F_UNLCK), range)?;
        let rc = fcntl_lock(self, libc::F_OFD_SETLK, &mut lock);
        if rc == 0 { Ok(()) } else { Err(lock_error()) }
    }

    fn query_lock(&self, range: impl RangeBounds<u64>) -> io::Result<bool> {
        let mut lock = flock_struct(lock_type(libc::F_WRLCK), range)?;
        let rc = fcntl_lock(self, libc::F_OFD_GETLK, &mut lock);
        if rc != 0 {
            return Err(lock_error());
        }
        // The lock-type constants are c_int on Linux and c_short on the Apple platforms, so
        // both sides are widened rather than compared directly
        Ok(i32::from(lock.l_type) != i32::from(lock_type(libc::F_UNLCK)))
    }
}
