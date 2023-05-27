#![allow(clippy::upper_case_acronyms)]

use crate::{DatabaseError, Result};
use std::fs::File;
use std::io;
use std::os::windows::fs::FileExt;
use std::os::windows::io::AsRawHandle;
use std::os::windows::io::RawHandle;

const ERROR_LOCK_VIOLATION: i32 = 0x21;
const ERROR_IO_PENDING: i32 = 997;

extern "system" {
    /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfile>
    fn LockFile(
        file: RawHandle,
        offset_low: u32,
        offset_high: u32,
        length_low: u32,
        length_high: u32,
    ) -> i32;

    /// <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-unlockfile>
    fn UnlockFile(
        file: RawHandle,
        offset_low: u32,
        offset_high: u32,
        length_low: u32,
        length_high: u32,
    ) -> i32;
}

pub(crate) struct LockedFile {
    file: File,
}

impl LockedFile {
    pub(crate) fn new(file: File) -> Result<Self, DatabaseError> {
        let handle = file.as_raw_handle();
        unsafe {
            let result = LockFile(handle, 0, 0, u32::MAX, u32::MAX);

            if result == 0 {
                let err = io::Error::last_os_error();
                return if err.raw_os_error() == Some(ERROR_IO_PENDING)
                    || err.raw_os_error() == Some(ERROR_LOCK_VIOLATION)
                {
                    Err(DatabaseError::DatabaseAlreadyOpen)
                } else {
                    Err(err.into())
                };
            }
        };

        Ok(Self { file })
    }

    pub(crate) fn read(&self, mut offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut buffer = vec![0; len];
        let mut data_offset = 0;
        while data_offset < buffer.len() {
            let read = self.file.seek_read(&mut buffer[data_offset..], offset)?;
            offset += read as u64;
            data_offset += read;
        }
        Ok(buffer)
    }

    pub(crate) fn write(&self, mut offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut data_offset = 0;
        while data_offset < data.len() {
            let written = self.file.seek_write(&data[data_offset..], offset)?;
            offset += written as u64;
            data_offset += written;
        }
        Ok(())
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        unsafe { UnlockFile(self.file.as_raw_handle(), 0, 0, u32::MAX, u32::MAX) };
    }
}
