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

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.lock().unwrap().metadata()?.len())
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut result = vec![0; len];
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut result)?;
        Ok(result)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.lock().unwrap().set_len(len)
    }

    fn sync_data(&self, _eventual: bool) -> Result<(), io::Error> {
        self.file.lock().unwrap().sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }
}
