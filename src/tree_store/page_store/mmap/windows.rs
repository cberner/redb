#![allow(clippy::upper_case_acronyms)]

use super::*;
use std::ffi::c_void;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::RawHandle;
use std::ptr;

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

#[derive(Copy, Clone)]
#[repr(C)]
struct OVERLAPPED_0_0 {
    offset: u32,
    offset_high: u32,
}

const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x00000002;
const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x00000001;
const ERROR_IO_PENDING: i32 = 997;
const PAGE_READWRITE: u32 = 0x4;

const STANDARD_RIGHTS_REQUIRED: u32 = 0x000f0000;

const SECTION_QUERY: u32 = 0x0001;
const SECTION_MAP_WRITE: u32 = 0x0002;
const SECTION_MAP_READ: u32 = 0x0004;
const SECTION_MAP_EXECUTE: u32 = 0x0008;
const SECTION_EXTEND_SIZE: u32 = 0x0010;
const SECTION_ALL_ACCESS: u32 = STANDARD_RIGHTS_REQUIRED
    | SECTION_QUERY
    | SECTION_MAP_WRITE
    | SECTION_MAP_READ
    | SECTION_MAP_EXECUTE
    | SECTION_EXTEND_SIZE;

const FILE_MAP_ALL_ACCESS: u32 = SECTION_ALL_ACCESS;

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

    /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-mapviewoffileex>
    fn MapViewOfFileEx(
        file_mapping: RawHandle,
        desired_access: u32,
        offset_high: u32,
        offset_low: u32,
        bytes_to_map: usize,
        base_address: *mut u8,
    ) -> *mut u8;

    /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers>
    fn FlushFileBuffers(file: RawHandle) -> u32;

    /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-flushviewoffile>
    fn FlushViewOfFile(base_address: *const u8, number_of_bytes_to_flush: usize) -> u32;

    /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-unmapviewoffile>
    fn UnmapViewOfFile(base_address: *const u8) -> u32;

    /// <https://learn.microsoft.com/en-us/windows/win32/api/handleapi/nf-handleapi-closehandle>
    fn CloseHandle(handle: RawHandle) -> u32;
}

struct AutoHandle {
    inner: RawHandle,
}

impl Drop for AutoHandle {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.inner);
        }
    }
}

pub(super) struct FileLock {
    handle: RawHandle,
    overlapped: OVERLAPPED,
}

impl FileLock {
    pub(super) fn new(file: &File) -> Result<Self> {
        let handle = file.as_raw_handle();
        let overlapped = unsafe {
            let mut overlapped = std::mem::zeroed();
            let result = LockFileEx(
                handle,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            );

            if result == 0 {
                let err = io::Error::last_os_error();
                return if err.raw_os_error() == Some(ERROR_IO_PENDING) {
                    Err(Error::DatabaseAlreadyOpen)
                } else {
                    Err(Error::Io(err))
                };
            }

            overlapped
        };

        Ok(Self { handle, overlapped })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe { UnlockFileEx(self.handle, 0, u32::MAX, u32::MAX, &mut self.overlapped) };
    }
}

pub(super) struct MmapInner {
    mmap: *mut u8,
    capacity: usize,
}

impl MmapInner {
    pub(super) fn create_mapping(file: &File, len: u64, max_capacity: usize) -> Result<Self> {
        // `CreateFileMappingW` documents:
        //
        // https://docs.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw
        // > An attempt to map a file with a length of 0 (zero) fails with an error code
        // > of ERROR_FILE_INVALID. Applications should test for files with a length of 0
        // > (zero) and reject those files.
        assert!(len > 0);

        let mmap = unsafe { Self::map_file(file, len, ptr::null_mut())? };

        Ok(Self {
            mmap,
            capacity: max_capacity,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn base_addr(&self) -> *mut u8 {
        self.mmap
    }

    unsafe fn map_file(file: &File, len: u64, map_addr: *mut u8) -> Result<*mut u8> {
        let handle = file.as_raw_handle();

        #[allow(clippy::cast_possible_truncation)]
        let lo = (len & u32::MAX as u64) as u32;
        let hi = (len >> 32) as u32;

        let ptr = {
            let mapping = AutoHandle {
                inner: CreateFileMappingW(
                    handle,
                    ptr::null_mut(),
                    PAGE_READWRITE,
                    hi,
                    lo,
                    ptr::null(),
                ),
            };
            if mapping.inner.is_null() {
                return Err(Error::Io(io::Error::last_os_error()));
            }

            MapViewOfFileEx(
                mapping.inner,
                FILE_MAP_ALL_ACCESS,
                0,
                0,
                len.try_into().unwrap(),
                map_addr,
            )
        };

        Ok(ptr)
    }

    pub(super) unsafe fn resize(&self, _len: u64, _owner: &Mmap) -> Result<()> {
        unreachable!("Mmap resizing is not supported on Windows");
        // UnmapViewOfFile(self.mmap);

        // owner.file.set_len(len)?;
        // let ptr = Self::map_file(&owner.file, len, self.mmap)?;

        // // This is the same logic as in unix
        // assert_eq!(ptr, self.mmap);

        // Ok(())
    }

    pub(super) fn flush(&self, owner: &Mmap) -> Result {
        self.eventual_flush(owner)?;

        #[cfg(not(fuzzing))]
        {
            if unsafe { FlushFileBuffers(owner.file.as_raw_handle()) } == 0 {
                return Err(Error::Io(io::Error::last_os_error()));
            }
        }
        Ok(())
    }

    #[inline]
    pub(super) fn eventual_flush(&self, owner: &Mmap) -> Result {
        #[cfg(not(fuzzing))]
        {
            let result = unsafe { FlushViewOfFile(self.mmap, owner.len()) };
            if result != 0 {
                Ok(())
            } else {
                Err(Error::Io(io::Error::last_os_error()))
            }
        }

        #[cfg(fuzzing)]
        {
            Ok(())
        }
    }
}

impl Drop for MmapInner {
    fn drop(&mut self) {
        unsafe {
            UnmapViewOfFile(self.mmap);
        }
    }
}
