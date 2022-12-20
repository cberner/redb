#![allow(clippy::upper_case_acronyms)]

use crate::{Error, Result};
use std::ffi::c_void;
use std::fs::File;
use std::io;
use std::os::windows::io::AsRawHandle;
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

#[derive(Copy, Clone)]
#[repr(C)]
struct OVERLAPPED_0_0 {
    offset: u32,
    offset_high: u32,
}

const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x00000002;
const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x00000001;
const ERROR_LOCK_VIOLATION: i32 = 0x21;
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

    /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-unlockfileex>
    fn UnlockFileEx(
        file: RawHandle,
        _reserved: u32,
        length_low: u32,
        length_high: u32,
        overlapped: *mut OVERLAPPED,
    ) -> i32;
}

pub(crate) struct FileLock {
    handle: RawHandle,
    overlapped: OVERLAPPED,
}

impl FileLock {
    pub(crate) fn new(file: &File) -> Result<Self> {
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
                return if err.raw_os_error() == Some(ERROR_IO_PENDING)
                    || err.raw_os_error() == Some(ERROR_LOCK_VIOLATION)
                {
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
