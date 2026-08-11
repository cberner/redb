use crate::{DatabaseError, Result, StorageBackend};
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    file: Mutex<File>,
}

/// Which whole-file advisory lock a [`FileBackend`] takes when it is created. This platform has no
/// file locking, so the choice is ignored.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum FileLockKind {
    Exclusive,
    Shared,
    None,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, FileLockKind::Exclusive)
    }

    pub(crate) fn new_internal(file: File, _: FileLockKind) -> Result<Self, DatabaseError> {
        Ok(Self {
            file: Mutex::new(file),
        })
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
}
