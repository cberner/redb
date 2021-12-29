use crate::multimap_table::MultimapTable;
use crate::table::{ReadOnlyTable, Table};
use crate::tree_store::{DbStats, NodeHandle, Storage, TableType};
use crate::types::{RedbKey, RedbValue};
use crate::{Error, ReadOnlyMultimapTable};
use memmap2::MmapMut;
use std::cell::Cell;
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
    /// `db_size`: the maximum size in bytes of the database. Note: this cannot be changed after the
    /// database is created.
    /// TODO: remove the restriction that db_size cannot be changed
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must only be concurrently modified by compatible versions
    /// of redb
    pub unsafe fn open(path: impl AsRef<Path>, db_size: usize) -> Result<Database, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.set_len(db_size as u64)?;

        let mmap = MmapMut::map_mut(&file)?;
        let storage = Storage::new(mmap, None)?;
        Ok(Database { storage })
    }

    pub fn begin_write(&self) -> Result<DatabaseTransaction, Error> {
        Ok(DatabaseTransaction::new(&self.storage))
    }

    pub fn begin_read(&self) -> Result<ReadOnlyDatabaseTransaction, Error> {
        Ok(ReadOnlyDatabaseTransaction::new(&self.storage))
    }

    pub fn stats(&self) -> Result<DbStats, Error> {
        self.storage.storage_stats()
    }

    pub fn print_debug(&self) {
        self.storage.print_debug()
    }
}

pub struct DatabaseBuilder {
    page_size: Option<usize>,
}

impl DatabaseBuilder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { page_size: None }
    }

    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = Some(size);
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database. Note: this cannot be changed after the
    /// database is created.
    /// TODO: remove the restriction that db_size cannot be changed
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must only be concurrently modified by compatible versions
    /// of redb
    pub unsafe fn open(self, path: impl AsRef<Path>, db_size: usize) -> Result<Database, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.set_len(db_size as u64)?;

        let mmap = MmapMut::map_mut(&file)?;
        let storage = Storage::new(mmap, self.page_size)?;
        Ok(Database { storage })
    }
}

pub struct DatabaseTransaction<'a> {
    storage: &'a Storage,
    transaction_id: u128,
    root_page: Cell<Option<NodeHandle>>,
}

impl<'a> DatabaseTransaction<'a> {
    fn new(storage: &'a Storage) -> Self {
        let transaction_id = storage.allocate_write_transaction();
        let root_page = storage.get_root_page_number();
        Self {
            storage,
            transaction_id,
            root_page: Cell::new(root_page),
        }
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    pub fn open_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<Table<'a, '_, K, V>, Error> {
        assert!(!name.as_ref().is_empty());
        let (definition, root) = self.storage.get_or_create_table(
            name.as_ref(),
            TableType::Normal,
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(Some(root));

        Ok(Table::new(
            definition.get_id(),
            self.transaction_id,
            &self.root_page,
            self.storage,
        ))
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        assert!(!name.as_ref().is_empty());
        let original_root = self.root_page.get();
        let root = self.storage.delete_table(
            name.as_ref(),
            TableType::Normal,
            self.transaction_id,
            original_root,
        )?;

        self.root_page.set(root);

        Ok(root != original_root)
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_multimap_table(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        assert!(!name.as_ref().is_empty());
        let original_root = self.root_page.get();
        let root = self.storage.delete_table(
            name.as_ref(),
            TableType::Multimap,
            self.transaction_id,
            original_root,
        )?;

        self.root_page.set(root);

        Ok(root != original_root)
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    pub fn open_multimap_table<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<MultimapTable<'a, '_, K, V>, Error> {
        assert!(!name.as_ref().is_empty());
        let (definition, root) = self.storage.get_or_create_table(
            name.as_ref(),
            TableType::Multimap,
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(Some(root));

        Ok(MultimapTable::new(
            definition.get_id(),
            self.transaction_id,
            &self.root_page,
            self.storage,
        ))
    }

    pub fn commit(self) -> Result<(), Error> {
        self.storage
            .commit(self.root_page.get(), self.transaction_id)?;
        Ok(())
    }

    /// Note: pages are only freed during commit(). So exclusively using this function may result
    /// in an out-of-memory error
    pub fn non_durable_commit(self) -> Result<(), Error> {
        self.storage
            .non_durable_commit(self.root_page.get(), self.transaction_id)?;
        Ok(())
    }

    pub fn abort(self) -> Result<(), Error> {
        self.storage.rollback_uncommited_writes(self.transaction_id)
    }
}

pub struct ReadOnlyDatabaseTransaction<'a> {
    storage: &'a Storage,
    transaction_id: u128,
    root_page: Option<NodeHandle>,
}

impl<'a> ReadOnlyDatabaseTransaction<'a> {
    fn new(storage: &'a Storage) -> Self {
        let transaction_id = storage.allocate_read_transaction();
        let root_page = storage.get_root_page_number();
        Self {
            storage,
            transaction_id,
            root_page,
        }
    }

    /// Open the given table
    pub fn open_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<ReadOnlyTable<'a, K, V>, Error> {
        assert!(!name.as_ref().is_empty());
        let definition = self
            .storage
            .get_table(name.as_ref(), TableType::Normal, self.root_page)?
            .ok_or_else(|| {
                Error::DoesNotExist(format!(
                    "Table '{}' does not exist",
                    String::from_utf8_lossy(name.as_ref())
                ))
            })?;

        Ok(ReadOnlyTable::new(
            definition.get_id(),
            self.root_page,
            self.storage,
        ))
    }

    /// Open the given table
    pub fn open_multimap_table<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<ReadOnlyMultimapTable<'a, K, V>, Error> {
        assert!(!name.as_ref().is_empty());
        let definition = self
            .storage
            .get_table(name.as_ref(), TableType::Multimap, self.root_page)?
            .ok_or_else(|| {
                Error::DoesNotExist(format!(
                    "Table '{}' does not exist",
                    String::from_utf8_lossy(name.as_ref())
                ))
            })?;

        Ok(ReadOnlyMultimapTable::new(
            definition.get_id(),
            self.root_page,
            self.storage,
        ))
    }
}

impl<'a> Drop for ReadOnlyDatabaseTransaction<'a> {
    fn drop(&mut self) {
        self.storage
            .deallocate_read_transaction(self.transaction_id);
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, ReadOnlyTable, ReadableTable, Table};
    use tempfile::NamedTempFile;

    #[test]
    fn non_page_size_multiple() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        let db_size = 1024 * 1024 + 1;
        let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
        let txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();

        let key = vec![0; 1024];
        let value = vec![0; 1];
        table.insert(&key, &value).unwrap();
        txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(table.len().unwrap(), 1);
    }

    #[test]
    fn transaction_id_persistence() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        let first_txn_id = write_txn.transaction_id;
        write_txn.commit().unwrap();
        drop(db);

        let db2 = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db2.begin_write().unwrap();
        assert!(write_txn.transaction_id > first_txn_id);
    }
}
