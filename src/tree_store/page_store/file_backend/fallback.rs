use crate::{DatabaseError, Result, StorageBackend};
#[cfg(feature = "logging")]
use log::warn;
use std::fs::{File, TryLockError};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    lock_supported: bool,
    file: Mutex<File>,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, false)
    }

    pub(crate) fn new_internal(file: File, read_only: bool) -> Result<Self, DatabaseError> {
        let result = if read_only {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };

        match result {
            Ok(()) => Ok(Self {
                lock_supported: true,
                file: Mutex::new(file),
            }),
            Err(TryLockError::WouldBlock) => Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) if err.kind() == io::ErrorKind::Unsupported => {
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file, at a time"
                );

                Ok(Self {
                    lock_supported: false,
                    file: Mutex::new(file),
                })
            }
            Err(TryLockError::Error(err)) => Err(err.into()),
        }
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.lock().unwrap().metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(out)?;
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.lock().unwrap().set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.file.lock().unwrap().sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }

    fn close(&self) -> Result<(), io::Error> {
        if self.lock_supported {
            self.file.lock().unwrap().unlock()?;
        }

        Ok(())
    }
}
