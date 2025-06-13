use crate::{DatabaseError, Result, StorageBackend};
use std::fs::{File, TryLockError};
use std::io;

#[cfg(feature = "logging")]
use log::warn;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(target_os = "wasi")]
use std::os::wasi::fs::FileExt;

#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    lock_supported: bool,
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        match file.try_lock() {
            Ok(()) => Ok(Self {
                file,
                lock_supported: true,
            }),
            Err(TryLockError::WouldBlock) => Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) if err.kind() == io::ErrorKind::Unsupported => {
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file, at a time"
                );

                Ok(Self {
                    file,
                    lock_supported: false,
                })
            }
            Err(TryLockError::Error(err)) => Err(err.into()),
        }
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.metadata()?.len())
    }

    #[cfg(any(unix, target_os = "wasi"))]
    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut buffer = vec![0; len];
        self.file.read_exact_at(&mut buffer, offset)?;
        Ok(buffer)
    }

    #[cfg(windows)]
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

    #[cfg(not(target_os = "macos"))]
    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
        self.file.sync_data()
    }

    #[cfg(target_os = "macos")]
    fn sync_data(&self, eventual: bool) -> Result<(), io::Error> {
        if eventual {
            let code = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_BARRIERFSYNC) };
            if code == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        } else {
            self.file.sync_data()
        }
    }

    #[cfg(any(unix, target_os = "wasi"))]
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.file.write_all_at(data, offset)
    }

    #[cfg(windows)]
    fn write(&self, mut offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut data_offset = 0;
        while data_offset < data.len() {
            let written = self.file.seek_write(&data[data_offset..], offset)?;
            offset += written as u64;
            data_offset += written;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), io::Error> {
        if self.lock_supported {
            self.file.unlock()?;
        }

        Ok(())
    }
}
