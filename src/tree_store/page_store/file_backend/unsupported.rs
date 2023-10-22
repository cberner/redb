use crate::{DatabaseError, Result, StorageBackend};
use std::fs::File;
use std::io;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend;

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(_: File) -> Result<Self, DatabaseError> {
        unimplemented!("Not supported on this platform.")
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        unimplemented!()
    }

    fn read(&self, _: u64, _: usize) -> Result<Vec<u8>, io::Error> {
        unimplemented!()
    }

    fn set_len(&self, _: u64) -> Result<(), io::Error> {
        unimplemented!()
    }

    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
        unimplemented!()
    }

    fn write(&self, _: u64, _: &[u8]) -> Result<(), io::Error> {
        unimplemented!()
    }
}
