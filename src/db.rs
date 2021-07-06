use crate::storage::Storage;
use crate::table::Table;
use crate::Error;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;

pub struct Database {
    storage: Storage,
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must only be concurrently modified by compatible versions
    /// of redb
    pub unsafe fn open(path: &Path) -> Result<Database, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // TODO: what value should we use here?
        file.set_len(4 * 1024 * 1024 * 1024)?;

        let mmap = MmapMut::map_mut(&file)?;
        let storage = Storage::new(mmap);
        storage.initialize()?;
        Ok(Database { storage })
    }

    pub fn open_table(&self, _name: &str) -> Result<Table, Error> {
        Table::new(&self.storage)
    }
}
