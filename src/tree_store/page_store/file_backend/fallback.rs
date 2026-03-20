use crate::error::BackendError;
use crate::{DatabaseError, Result, StorageBackend};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    file: Mutex<File>,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, false)
    }

    pub(crate) fn new_internal(file: File, _: bool) -> Result<Self, DatabaseError> {
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> core::result::Result<u64, BackendError> {
        Ok(self
            .file
            .lock()
            .unwrap()
            .metadata()
            .map_err(BackendError::Io)?
            .len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> core::result::Result<(), BackendError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))
            .map_err(BackendError::Io)?;
        file.read_exact(out).map_err(BackendError::Io)?;
        Ok(())
    }

    fn set_len(&self, len: u64) -> core::result::Result<(), BackendError> {
        self.file
            .lock()
            .unwrap()
            .set_len(len)
            .map_err(BackendError::Io)
    }

    fn sync_data(&self) -> core::result::Result<(), BackendError> {
        self.file
            .lock()
            .unwrap()
            .sync_data()
            .map_err(BackendError::Io)
    }

    fn write(&self, offset: u64, data: &[u8]) -> core::result::Result<(), BackendError> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))
            .map_err(BackendError::Io)?;
        file.write_all(data).map_err(BackendError::Io)
    }
}
