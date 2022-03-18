use crate::multimap_table::MultimapTable;
use crate::table::{ReadOnlyTable, Table};
use crate::tree_store::{
    get_db_size, DatabaseStats, PageNumber, Storage, TableType, TransactionId, FREED_TABLE,
};
use crate::types::{RedbKey, RedbValue};
use crate::Result;
use crate::{Error, ReadOnlyMultimapTable};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, panic};

// TODO: add trait bounds once const_fn_trait_bound is stable
pub struct TableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> TableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        // TODO: enable once const_panic is stable
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<'a, K: ?Sized, V: ?Sized> Clone for TableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized, V: ?Sized> Copy for TableDefinition<'a, K, V> {}

pub struct MultimapTableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> MultimapTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<'a, K: ?Sized, V: ?Sized> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized, V: ?Sized> Copy for MultimapTableDefinition<'a, K, V> {}

pub struct Database {
    storage: Storage,
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn create(path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let file = if path.as_ref().exists() && File::open(path.as_ref())?.metadata()?.len() > 0 {
            let existing_size = get_db_size(path.as_ref())?;
            if existing_size != db_size {
                return Err(Error::DbSizeMismatch {
                    path: path.as_ref().to_string_lossy().to_string(),
                    size: existing_size,
                    requested_size: db_size,
                });
            }
            OpenOptions::new().read(true).write(true).open(path)?
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            file
        };

        let storage = Storage::new(file, db_size, None, true)?;
        Ok(Database { storage })
    }

    /// Opens an existing redb database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn open(path: impl AsRef<Path>) -> Result<Database> {
        if File::open(path.as_ref())?.metadata()?.len() > 0 {
            let existing_size = get_db_size(path.as_ref())?;
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            let storage = Storage::new(file, existing_size, None, true)?;
            Ok(Database { storage })
        } else {
            Err(Error::Io(io::Error::from(ErrorKind::InvalidData)))
        }
    }

    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    pub fn begin_write(&self) -> Result<WriteTransaction> {
        WriteTransaction::new(&self.storage)
    }

    pub fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(ReadTransaction::new(&self.storage))
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        self.storage.storage_stats()
    }

    pub fn print_debug(&self) {
        self.storage.print_debug()
    }
}

pub struct DatabaseBuilder {
    page_size: Option<usize>,
    dynamic_growth: bool,
}

impl DatabaseBuilder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            page_size: None,
            dynamic_growth: true,
        }
    }

    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = Some(size);
        self
    }

    /// Whether to grow the database file dynamically.
    /// When set to true, the database file will start at a small size and grow as insertions are made
    /// When set to false, the database file will be statically sized
    pub fn set_grow(&mut self, dynamic: bool) -> &mut Self {
        self.dynamic_growth = dynamic;
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must only be concurrently modified by compatible versions
    /// of redb
    pub unsafe fn create(&self, path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let storage = Storage::new(file, db_size, self.page_size, self.dynamic_growth)?;
        Ok(Database { storage })
    }
}

#[derive(Copy, Clone)]
pub enum Durability {
    /// Commits with this durability level will not be persisted to disk unless followed by a
    /// commit with a higher durability level.
    ///
    /// Note: Pages are only freed during commits with higher durability levels. Exclusively using
    /// this function may result in Error::OutOfSpace.
    None,
    /// Commits with this durability level have been queued for persitance to disk, and should be
    /// persistant some time after [WriteTransaction::commit] returns.
    Eventual,
    /// Commits with this durability level are guaranteed to be persistant as soon as
    /// [WriteTransaction::commit] returns.
    Immediate,
}

#[allow(clippy::type_complexity)]
pub struct WriteTransaction<'a> {
    storage: &'a Storage,
    transaction_id: TransactionId,
    root_page: Cell<Option<PageNumber>>,
    pending_table_root_changes: RefCell<HashMap<String, Rc<Cell<Option<PageNumber>>>>>,
    open_tables: RefCell<HashMap<String, &'static panic::Location<'static>>>,
    completed: AtomicBool,
    durability: Durability,
}

impl<'a> WriteTransaction<'a> {
    fn new(storage: &'a Storage) -> Result<Self> {
        let transaction_id = storage.allocate_write_transaction()?;
        let root_page = storage.get_root_page_number();
        Ok(Self {
            storage,
            transaction_id,
            root_page: Cell::new(root_page),
            pending_table_root_changes: RefCell::new(Default::default()),
            open_tables: RefCell::new(Default::default()),
            completed: Default::default(),
            durability: Durability::Immediate,
        })
    }

    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    pub fn open_table<'s: 't, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &'s self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'a, 't, K, V>> {
        assert!(!definition.name.is_empty());
        assert_ne!(definition.name, FREED_TABLE);
        if let Some(location) = self.open_tables.borrow().get(definition.name) {
            return Err(Error::TableAlreadyOpen(
                definition.name.to_string(),
                location,
            ));
        }
        self.open_tables
            .borrow_mut()
            .insert(definition.name.to_string(), panic::Location::caller());

        let (header, root) = self.storage.get_or_create_table::<K, V>(
            definition.name,
            TableType::Normal,
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(Some(root));

        let root = self
            .pending_table_root_changes
            .borrow_mut()
            .entry(definition.name.to_string())
            .or_insert_with(|| Rc::new(Cell::new(header.get_root())))
            .clone();

        Ok(Table::new(
            definition.name,
            self.transaction_id,
            root,
            self.storage,
            self,
        ))
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    pub fn open_multimap_table<'t, K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &'t self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'a, 't, K, V>> {
        assert!(!definition.name.is_empty());
        assert_ne!(definition.name, FREED_TABLE);
        if let Some(location) = self.open_tables.borrow().get(definition.name) {
            return Err(Error::TableAlreadyOpen(
                definition.name.to_string(),
                location,
            ));
        }
        self.open_tables
            .borrow_mut()
            .insert(definition.name.to_string(), panic::Location::caller());

        let (header, root) = self.storage.get_or_create_table::<K, V>(
            definition.name,
            TableType::Multimap,
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(Some(root));

        let root = self
            .pending_table_root_changes
            .borrow_mut()
            .entry(definition.name.to_string())
            .or_insert_with(|| Rc::new(Cell::new(header.get_root())))
            .clone();

        Ok(MultimapTable::new(
            definition.name,
            self.transaction_id,
            root,
            self.storage,
            self,
        ))
    }

    pub(crate) fn close_table(&self, name: &str) {
        self.open_tables.borrow_mut().remove(name).unwrap();
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<bool> {
        assert!(!definition.name.is_empty());
        assert_ne!(definition.name, FREED_TABLE);
        let original_root = self.root_page.get();
        let (root, found) = self.storage.delete_table::<K, V>(
            definition.name,
            TableType::Normal,
            self.transaction_id,
            original_root,
        )?;

        self.root_page.set(root);

        Ok(found)
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_multimap_table<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<bool> {
        assert!(!definition.name.is_empty());
        let original_root = self.root_page.get();
        let (root, found) = self.storage.delete_table::<K, V>(
            definition.name,
            TableType::Multimap,
            self.transaction_id,
            original_root,
        )?;

        self.root_page.set(root);

        Ok(found)
    }

    /// List all the tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_tables(&self) -> Result<impl Iterator<Item = String> + '_> {
        self.storage
            .list_tables(TableType::Normal, self.root_page.get())
    }

    /// List all the multimap tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = String> + '_> {
        self.storage
            .list_tables(TableType::Multimap, self.root_page.get())
    }

    pub fn commit(self) -> Result {
        match self.commit_inner() {
            Ok(_) => Ok(()),
            Err(err) => match err {
                // Rollback the transaction if we ran out of space during commit, so that user may
                // continue with another transaction (like a delete)
                Error::OutOfSpace => {
                    self.abort()?;
                    Err(err)
                }
                err => Err(err),
            },
        }
    }

    fn commit_inner(&self) -> Result {
        // Update all the table roots in the master table, before committing
        for (name, update) in self.pending_table_root_changes.borrow_mut().drain() {
            let new_root = self.storage.update_table_root(
                &name,
                update.get(),
                self.transaction_id,
                self.root_page.get(),
            )?;
            self.root_page.set(Some(new_root));
        }

        match self.durability {
            Durability::None => self
                .storage
                .non_durable_commit(self.root_page.get(), self.transaction_id)?,
            Durability::Eventual => {
                self.storage
                    .durable_commit(self.root_page.get(), self.transaction_id, true)?
            }
            Durability::Immediate => {
                self.storage
                    .durable_commit(self.root_page.get(), self.transaction_id, false)?
            }
        }

        self.completed.store(true, Ordering::SeqCst);

        Ok(())
    }

    pub fn abort(self) -> Result {
        self.storage
            .rollback_uncommited_writes(self.transaction_id)?;
        self.completed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl<'a> Drop for WriteTransaction<'a> {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::SeqCst) {
            self.storage
                .record_leaked_write_transaction(self.transaction_id);
        }
    }
}

pub struct ReadTransaction<'a> {
    storage: &'a Storage,
    transaction_id: TransactionId,
    root_page: Option<PageNumber>,
}

impl<'a> ReadTransaction<'a> {
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
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<'a, K, V>> {
        assert!(!definition.name.is_empty());
        assert_ne!(definition.name, FREED_TABLE);
        let header = self
            .storage
            .get_table::<K, V>(definition.name, TableType::Normal, self.root_page)?
            .ok_or_else(|| Error::TableDoesNotExist(definition.name.to_string()))?;

        Ok(ReadOnlyTable::new(header.get_root(), self.storage))
    }

    /// Open the given table
    pub fn open_multimap_table<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<ReadOnlyMultimapTable<'a, K, V>> {
        assert!(!definition.name.is_empty());
        assert_ne!(definition.name, FREED_TABLE);
        let header = self
            .storage
            .get_table::<K, V>(definition.name, TableType::Multimap, self.root_page)?
            .ok_or_else(|| Error::TableDoesNotExist(definition.name.to_string()))?;

        Ok(ReadOnlyMultimapTable::new(header.get_root(), self.storage))
    }

    /// List all the tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_tables(&self) -> Result<impl Iterator<Item = String> + '_> {
        self.storage.list_tables(TableType::Normal, self.root_page)
    }

    /// List all the multimap tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = String> + '_> {
        self.storage
            .list_tables(TableType::Multimap, self.root_page)
    }
}

impl<'a> Drop for ReadTransaction<'a> {
    fn drop(&mut self) {
        self.storage
            .deallocate_read_transaction(self.transaction_id);
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, TableDefinition};
    use tempfile::NamedTempFile;

    const X: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    #[test]
    fn transaction_id_persistence() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world").unwrap();
        }
        let first_txn_id = write_txn.transaction_id;
        write_txn.commit().unwrap();
        drop(db);

        let db2 = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db2.begin_write().unwrap();
        assert!(write_txn.transaction_id > first_txn_id);
    }
}
