use super::*;
use std::ffi::c_void;
use std::mem::MaybeUninit;
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
const PAGE_READWRITE: u32 = 0x4;
const SECTION_MAP_READ: u32 = 0x2;
const SECTION_MAP_READ: u32 = 0x4;
const SECTION_EXTEND_SIZE: u32 = 0x10;
const SEC_COMMIT: u32 = 0x8000000;

#[repr(C)]
struct SECURITY_ATTRIBUTES {
    length: u32,
    descriptor: *mut c_void,
    inherit: u32,
}

#[repr(C)]
struct SYSTEM_INFO {
    processor_arch: u16,
    reserved: u16,
    page_size: u16,
    min_app_address: *mut c_void,
    max_app_address: *mut c_void,
    active_processor_mask: usize,
    number_of_processors: u32,
    processor_type: u32,
    allocation_granularity: u32,
    processor_level: u16,
    processor_revision: u16,
}

#[repr(C)]
struct UNICODE_STRING {
    length: u16,
    max_length: u16,
    buffer: *mut u16,
}

#[repr(C)]
struct OBJECT_ATTRIBUTES {
    length: u32,
    root_directory: RawHandle,
    name: *mut UNICODE_STRING,
    attributes: u32,
    security_description: *mut c_void,
    security_quality_of_service: *mut c_void,
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

    /// <https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/wdm/nf-wdm-zwcreatesection>
    fn NtCreateSection(
        section_handle: *mut RawHandle,
        desired_access: u32,
        obj_attrs: Option<*mut OBJECT_ATTRIBUTES>,
        maximum_size: Option<*mut i64>,
        section_page_protection: u32,
        allocation_attributes: u32,
        file_handle: RawHandle,
    ) -> i32;

    /// Undocumented, see <https://stackoverflow.com/a/41081832>
    fn NtExtendSection(section: RawHandle, new_size: *mut i64) -> i32;

    // /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw>
    // fn CreateFileMappingW(
    //     file: RawHandle,
    //     attributes: *mut SECURITY_ATTRIBUTES,
    //     protect: u32,
    //     max_size_high: u32,
    //     max_size_low: u32,
    //     name: *const u16,
    // ) -> RawHandle;

    // /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-mapviewoffile>
    // fn MapViewOfFile(
    //     file_mapping: RawHandle,
    //     desired_access: u32,
    //     offset_high: u32,
    //     offset_low: u32,
    //     bytes_to_map: usize,
    // ) -> *mut c_void;

    // /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers>
    // fn FlushFileBuffers(file: RawHandle) -> u32;

    // /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-flushviewoffile>
    // fn FlushViewOfFile(base_address: *const c_void, number_of_bytes_to_flush: usize) -> u32;

    // /// <https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-unmapviewoffile>
    // fn UnmapViewOfFile(base_address: *const c_void) -> u32;

    // /// <https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getsysteminfo>
    // fn GetSystemInfo(system_info: *mut SYSTEM_INFO);
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

/// Returns a fixed pointer that is valid for `slice::from_raw_parts::<u8>` with `len == 0`.
#[inline]
fn empty_slice_ptr() -> *mut c_void {
    std::ptr::NonNull::<u8>::dangling().cast().as_ptr()
}

pub(super) struct MmapInner {
    pub(super) mmap: *mut u8,
    pub(super) capacity: usize,
    mapping: RawHandle,
}

impl MmapInner {
    pub(super) fn create_mapping(file: &File, len: u64, max_capacity: usize) -> Result<*mut u8> {
        // `CreateFileMappingW` documents:
        //
        // https://docs.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-createfilemappingw
        // > An attempt to map a file with a length of 0 (zero) fails with an error code
        // > of ERROR_FILE_INVALID. Applications should test for files with a length of 0
        // > (zero) and reject those files.
        assert!(len > 0);

        unsafe {
            let mut max_size = max_capacity as i64;
            let mut section = MaybeUninit::new();
            let status = NtCreateSection(
                section.as_mut_ptr(),
                SECTION_EXTEND_SIZE | SECTION_MAP_READ | SECTION_MAP_WRITE,
                0,
                Some(&mut max_size),
                PAGE_READWRITE,
                SEC_COMMIT,
                file.as_raw_handle(),
            );

            let mapping = CreateFileMappingW(handle, ptr::null_mut(), protect, 0, 0, ptr::null());
            if mapping.is_null() {
                return Err(io::Error::last_os_error());
            }

            let ptr = MapViewOfFile(
                mapping,
                access,
                (aligned_offset >> 16 >> 16) as DWORD,
                (aligned_offset & 0xffffffff) as DWORD,
                aligned_len as SIZE_T,
            );
            CloseHandle(mapping);
            if ptr.is_null() {
                return Err(io::Error::last_os_error());
            }

            let mut new_handle = 0 as RawHandle;
            let cur_proc = GetCurrentProcess();
            let ok = DuplicateHandle(
                cur_proc,
                handle,
                cur_proc,
                &mut new_handle,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            );
            if ok == 0 {
                UnmapViewOfFile(ptr);
                return Err(io::Error::last_os_error());
            }

            Ok(MmapInner {
                handle: Some(new_handle),
                ptr: ptr.offset(alignment as isize),
                len: len as usize,
                copy,
            })
        }
        // CreateFileMappingW
        // MapViewOfFile
    }

    pub(super) unsafe fn resize(&self, len: u64, owner: &Mmap) -> Result<()> {
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
            let result = unsafe { FlushViewOfFile(self.mmap.add(offset), len as SIZE_T) };
            if result != 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
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
            // UnmapViewOfFile
            // CloseHandle
        }
    }
}

#[inline]
fn allocation_granularity() -> usize {
    let info = unsafe {
        let mut info = std::mem::MaybeUninit::uninit();
        GetSystemInfo(info.as_mut_ptr());
        info.assume_init()
    };

    info.dwAllocationGranularity as usize
}
