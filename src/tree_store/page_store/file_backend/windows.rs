#![allow(clippy::upper_case_acronyms)]

use crate::{DatabaseError, Result, StorageBackend};
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

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
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
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.metadata()?.len())
    }

    fn read(&self, mut offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut buffer = vec![0; len];
        let mut data_offset = 0;
        while data_offset < buffer.len() {
            let read = self.file.seek_read(&mut buffer[data_offset..], offset)?;
            offset += read as u64;
            data_offset += read;
        }
        Ok(buffer)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.set_len(len)
    }

    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
        self.file.sync_data()
    }

    fn write(&self, mut offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut data_offset = 0;
        while data_offset < data.len() {
            let written = self.file.seek_write(&data[data_offset..], offset)?;
            offset += written as u64;
            data_offset += written;
        }
        Ok(())
    }
}

impl Drop for FileBackend {
    fn drop(&mut self) {
        unsafe { UnlockFile(self.file.as_raw_handle(), 0, 0, u32::MAX, u32::MAX) };
    }
}
