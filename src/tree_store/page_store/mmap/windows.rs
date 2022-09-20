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
const PAGE_READWRITE: u32 = 0x04;

#[repr(C)]
struct SECURITY_ATTRIBUTES {
    length: u32,
    descriptor: *mut c_void,
    inherit: u32,
}

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

    /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-unlockfileex>
    fn UnlockFileEx(
        file: RawHandle,
        _reserved: u32,
        length_low: u32,
        length_high: u32,
        overlapped: *mut OVERLAPPED,
    ) -> i32;

    /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw>
    fn CreateFileMappingW(
        file: RawHandle,
        attributes: *mut SECURITY_ATTRIBUTES,
        protect: u32,
        max_size_high: u32,
        max_size_low: u32,
        name: *const u16,
    ) -> RawHandle;
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
        self.eventual_flush(owner)?;

        #[cfg(not(fuzzing))]
        {
            if let Some(handle) = self.handle {
                let ok = unsafe { FlushFileBuffers(handle) };
                if ok == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub(super) fn eventual_flush(&self, owner: &Mmap) -> Result {
        if self.mmap == empty_slice_ptr() {
            return Ok(());
        }

        #[cfg(not(fuzzing))]
        {
            let result = unsafe { FlushViewOfFile(self.ptr.add(offset), len as SIZE_T) };
            if result != 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }

        Ok(())
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
