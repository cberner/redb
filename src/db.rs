use crate::storage::{DbStats, Storage};
use crate::table::Table;
use crate::types::{RedbKey, RedbValue};
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
        let mut db_size = 16 * 1024 * 1024 * 1024;
        // Ensure that db_size is a multiple of page size, which is required by mmap
        db_size -= db_size % page_size::get();
        file.set_len(db_size as u64)?;

        let mmap = MmapMut::map_mut(&file)?;
        let storage = Storage::new(mmap)?;
        Ok(Database { storage })
    }

    pub fn open_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: &[u8],
    ) -> Result<Table<K, V>, Error> {
        assert!(!name.is_empty());
        let (id, root) = self
            .storage
            .get_or_create_table(name, self.storage.get_root_page_number())?;
        self.storage.commit(Some(root))?;
        Table::new(id, &self.storage)
    }

    pub fn stats(&self) -> Result<DbStats, Error> {
        self.storage.storage_stats()
    }
}
