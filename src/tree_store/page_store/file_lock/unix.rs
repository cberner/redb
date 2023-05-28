// TODO once Rust's libc has flock implemented for WASI, this file needs to be revisited.
// What needs to be changed is commented below.
// See also: https://github.com/WebAssembly/wasi-filesystem/issues/2

// Remove this line once wasi-libc has flock
#![cfg_attr(target_os = "wasi", allow(unused_imports))]

use crate::{DatabaseError, Result};
use std::fs::File;
use std::io;

#[cfg(unix)]
use std::os::unix::{fs::FileExt, io::AsRawFd};

#[cfg(target_os = "wasi")]
use std::os::wasi::{fs::FileExt, io::AsRawFd};

pub(crate) struct LockedFile {
    file: File,
}

impl LockedFile {
    // This is a no-op until we get flock in wasi-libc.
    // Delete this function when we get flock.
    #[cfg(target_os = "wasi")]
    pub(crate) fn new(file: File) -> Result<Self> {
        Ok(Self { file })
    }

    #[cfg(unix)] // remove this line when wasi-libc gets flock
    pub(crate) fn new(file: File) -> Result<Self, DatabaseError> {
        let fd = file.as_raw_fd();
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                Err(DatabaseError::DatabaseAlreadyOpen)
            } else {
                Err(err.into())
            }
        } else {
            Ok(Self { file })
        }
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    pub(crate) fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut buffer = vec![0; len];
        self.file.read_exact_at(&mut buffer, offset)?;
        Ok(buffer)
    }

    pub(crate) fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.file.write_all_at(data, offset)
    }
}

#[cfg(unix)] // remove this line when wasi-libc gets flock
impl Drop for LockedFile {
    fn drop(&mut self) {
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
