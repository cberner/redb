use super::*;
use std::ffi::c_void;
use std::os::windows::io::AsRawFd;
use std::os::windows::io::RawHandle;

#[repr(C)]
struct OVERLAPPED {
    internal: usize,
    internal_high: usize,
    anonymous: OVERLAPPED_0,
    event: RawHandle,
}

#[repr(C)]
union OVERLAPPED_0 {
    pub anonymous: OVERLAPPED_0_0,
    pub pointer: *mut c_void,
}

#[repr(C)]
struct OVERLAPPED_0_0 {
    offset: u32,
    offset_high: u32,
}

const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x00000002;
const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x00000001;
const ERROR_IO_PENDING: i32 = 997;

extern "system" {
    /// <https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex>
    fn LockFileEx(
        file: RawHandle,
        flags: u32,
        _reserved: u32,
        length_low: u32,
        length_high: u32,
        overlapped: *mut OVERLAPPED,
    ) -> i32;

    fn UnlockFileEx(
        file: RawHandle,
        _reserved: u32,
        length_low: u32,
        length_high: u32,
        overlapped: *mut OVERLAPPED,
    ) -> i32;
}

struct FileLock {
    handle: RawHandle,
    overlapped: OVERLAPPED,
}

impl FileLock {
    fn new(file: &File) -> Result<Self> {
        let handle = file.as_raw_handle();
        let overlapped = std::mem::zeroed();
        let result = unsafe {
            LockFileEx(
                handle,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };
        if result == 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_IO_PENDING) {
                Err(Error::DatabaseAlreadyOpen)
            } else {
                Err(Error::Io(err))
            }
        } else {
            Ok(Self { fd })
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe { UnlockFileEx(self.handle, 0, u32::MAX, u32::MAX, &mut self.overlapped) };
    }
}

pub(super) struct MmapInner {
    pub(super) mmap: *mut u8,
    pub(super) capacity: usize,
}

impl MmapInner {
    pub(super) fn create_mapping(file: &File, max_capacity: usize) -> Result<*mut u8> {
        // CreateFileMappingW
        // MapViewOfFile
    }

    // Safety: if new_len < len(), caller must ensure that no references to memory in new_len..len() exist
    pub(crate) unsafe fn resize(&self, new_len: usize) -> Result<()> {
        assert!(new_len <= self.capacity);
        self.check_fsync_failure()?;
        self.file.set_len(new_len as u64)?;

        let mmap = libc::mmap(
            self.mmap as *mut libc::c_void,
            self.capacity as libc::size_t,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_FIXED,
            self.file.as_raw_fd(),
            0,
        );

        if mmap == libc::MAP_FAILED {
            Err(io::Error::last_os_error().into())
        } else {
            assert_eq!(mmap as *mut u8, self.mmap);
            self.len.store(new_len, Ordering::Release);
            Ok(())
        }
    }

    /// Safety: if new_len < len(), caller must ensure that no references to memory in new_len..len() exist
    pub(super) unsafe fn resize(&self, owner: &Mmap) -> Result<()> {
        let mmap = libc::mmap(
            self.mmap as *mut libc::c_void,
            self.capacity as libc::size_t,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_FIXED,
            owner.file.as_raw_fd(),
            0,
        );

        if mmap == libc::MAP_FAILED {
            Err(io::Error::last_os_error().into())
        } else {
            assert_eq!(mmap as *mut u8, self.mmap);
            Ok(())
        }
    }

    pub(super) fn flush(&self, owner: &Mmap) -> Result {
        // Disable fsync when fuzzing, since it doesn't test crash consistency
        #[cfg(not(fuzzing))]
        {
            #[cfg(not(target_os = "macos"))]
            {
                let result = unsafe {
                    libc::msync(
                        self.mmap as *mut libc::c_void,
                        owner.len() as libc::size_t,
                        libc::MS_SYNC,
                    )
                };
                if result != 0 {
                    return Err(io::Error::last_os_error().into());
                }
            }
            #[cfg(target_os = "macos")]
            {
                let code = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_FULLFSYNC) };
                if code == -1 {
                    return Err(io::Error::last_os_error().into());
                }
            }
        }
        Ok(())
    }

    pub(super) fn eventual_flush(&self, owner: &Mmap) -> Result {
        #[cfg(not(target_os = "macos"))]
        {
            return self.flush(owner);
        }
        #[cfg(all(target_os = "macos", not(fuzzing)))]
        {
            // TODO: It may be unsafe to mix F_BARRIERFSYNC with writes to the mmap.
            //       Investigate switching to `write()`
            let code = unsafe { libc::fcntl(owner.file.as_raw_fd(), libc::F_BARRIERFSYNC) };
            if code == -1 {
                Err(io::Error::last_os_error().into());
            } else {
                Ok(())
            }
        }
    }
}

impl Drop for MmapInner {
    fn drop(&mut self) {
        unsafe {
            // UnmapViewOfFile
            // CloseHandle
        }
    }
}
