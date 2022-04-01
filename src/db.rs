use crate::tree_store::btree_utils::{get_mut_value, make_mut_single_leaf};
use crate::tree_store::{
    get_db_size, page_numbers_iter_start_state, AllPageNumbersBtreeIter, BtreeRangeIter,
    FreedTableKey, InternalTableDefinition, PageNumber, TableType, TransactionalMemory,
    FREED_TABLE,
};
use crate::types::RedbValue;
use crate::Error;
use crate::{ReadTransaction, Result, WriteTransaction};
use std::collections::btree_set::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::RangeFull;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::{io, panic};

pub(crate) type TransactionId = u64;
type AtomicTransactionId = AtomicU64;

// TODO: add trait bounds once const_fn_trait_bound is stable
pub struct TableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> TableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        // assert_ne! isn't stable, so we have to do it manually
        let name_bytes = name.as_bytes();
        let freed_name_bytes = FREED_TABLE.as_bytes();
        if name_bytes.len() == freed_name_bytes.len() {
            let mut equal = true;
            let mut i = 0;
            while i < name.len() {
                if name_bytes[i] != freed_name_bytes[i] {
                    equal = false;
                }
                i += 1;
            }
            if equal {
                panic!("Table name may not be equal to the internal free page table");
            }
        }
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.name
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
        assert!(!name.is_empty());
        // assert_ne! isn't stable, so we have to do it manually
        let name_bytes = name.as_bytes();
        let freed_name_bytes = FREED_TABLE.as_bytes();
        if name_bytes.len() == freed_name_bytes.len() {
            let mut equal = true;
            let mut i = 0;
            while i < name.len() {
                if name_bytes[i] != freed_name_bytes[i] {
                    equal = false;
                }
                i += 1;
            }
            if equal {
                panic!("Table name may not be equal to the internal free page table");
            }
        }
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.name
    }
}

impl<'a, K: ?Sized, V: ?Sized> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized, V: ?Sized> Copy for MultimapTableDefinition<'a, K, V> {}

pub struct Database {
    mem: TransactionalMemory,
    next_transaction_id: AtomicTransactionId,
    live_read_transactions: Mutex<BTreeSet<TransactionId>>,
    live_write_transaction: Mutex<Option<TransactionId>>,
    leaked_write_transaction: Mutex<Option<&'static panic::Location<'static>>>,
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

        Database::new(file, db_size, None, true)
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
            Database::new(file, existing_size, None, true)
        } else {
            Err(Error::Io(io::Error::from(ErrorKind::InvalidData)))
        }
    }

    pub(crate) fn get_memory(&self) -> &TransactionalMemory {
        &self.mem
    }

    fn new(
        file: File,
        max_capacity: usize,
        page_size: Option<usize>,
        dynamic_growth: bool,
    ) -> Result<Self> {
        let mem = TransactionalMemory::new(file, max_capacity, page_size, dynamic_growth)?;
        if mem.needs_repair()? {
            let root = mem
                .get_primary_root_page()
                .expect("Tried to repair an empty database");
            // Clear the freed table. We're about to rebuild the allocator state by walking
            // all the reachable pages, which will implicitly free them
            {
                // Safety: we own the mem object, and just created it, so there can't be other references.
                let mut bytes = unsafe {
                    get_mut_value::<str>(mem.get_page_mut(root), FREED_TABLE.as_bytes(), &mem)
                        .unwrap()
                };
                // TODO: this needs to be done in a transaction. Otherwise a torn write could happen
                let mut header = InternalTableDefinition::from_bytes(bytes.as_mut());
                header.table_root = None;
                assert_eq!(bytes.as_mut().len(), header.as_bytes().len());
                bytes.as_mut().copy_from_slice(&header.as_bytes());
            }

            // Repair the allocator state
            let root_page = mem.get_page(root);
            let start = page_numbers_iter_start_state(root_page);

            // All pages in the master table
            let mut all_pages_iter: Box<dyn Iterator<Item = PageNumber>> =
                Box::new(AllPageNumbersBtreeIter::new(start, &mem));

            // Iterate over all other tables
            let mut iter: BtreeRangeIter<[u8], [u8]> =
                BtreeRangeIter::new::<RangeFull, [u8]>(.., Some(root), &mem);

            // Chain all the other tables to the master table iter
            while let Some(entry) = iter.next() {
                let definition = InternalTableDefinition::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    let page = mem.get_page(table_root);
                    let table_start = page_numbers_iter_start_state(page);
                    let table_pages_iter = AllPageNumbersBtreeIter::new(table_start, &mem);
                    all_pages_iter = Box::new(all_pages_iter.chain(table_pages_iter));
                }
            }

            mem.repair_allocator(all_pages_iter)?;
        }

        let mut next_transaction_id = mem.get_last_committed_transaction_id()? + 1;
        if mem.get_primary_root_page().is_none() {
            assert_eq!(next_transaction_id, 1);
            // Empty database, so insert the freed table.
            let freed_table = InternalTableDefinition {
                table_root: None,
                table_type: TableType::Normal,
                key_type: FreedTableKey::redb_type_name().to_string(),
                value_type: <[u8]>::redb_type_name().to_string(),
            };
            let (new_root, _) =
                make_mut_single_leaf(FREED_TABLE.as_bytes(), &freed_table.as_bytes(), &mem)?;
            mem.set_secondary_root_page(new_root)?;
            mem.commit(next_transaction_id, false)?;
            next_transaction_id += 1;
        }

        Ok(Database {
            mem,
            next_transaction_id: AtomicTransactionId::new(next_transaction_id),
            live_write_transaction: Mutex::new(None),
            live_read_transactions: Mutex::new(Default::default()),
            leaked_write_transaction: Mutex::new(Default::default()),
        })
    }

    pub(crate) fn record_leaked_write_transaction(&self, transaction_id: TransactionId) {
        assert_eq!(
            transaction_id,
            self.live_write_transaction.lock().unwrap().unwrap()
        );
        *self.leaked_write_transaction.lock().unwrap() = Some(panic::Location::caller());
    }

    pub(crate) fn allocate_write_transaction(&self) -> Result<TransactionId> {
        let guard = self.leaked_write_transaction.lock().unwrap();
        if let Some(leaked) = *guard {
            return Err(Error::LeakedWriteTransaction(leaked));
        }
        drop(guard);

        assert!(self.live_write_transaction.lock().unwrap().is_none());
        let id = self.next_transaction_id.fetch_add(1, Ordering::AcqRel);
        *self.live_write_transaction.lock().unwrap() = Some(id);
        Ok(id)
    }

    pub(crate) fn allocate_read_transaction(&self) -> TransactionId {
        let id = self.next_transaction_id.fetch_add(1, Ordering::AcqRel);
        self.live_read_transactions.lock().unwrap().insert(id);
        id
    }

    pub(crate) fn deallocate_read_transaction(&self, id: TransactionId) {
        self.live_read_transactions.lock().unwrap().remove(&id);
    }

    pub(crate) fn deallocate_write_transaction(&self, id: TransactionId) {
        let mut live = self.live_write_transaction.lock().unwrap();
        assert_eq!(Some(id), *live);
        *live = None;
    }

    pub(crate) fn oldest_live_read_transaction(&self) -> Option<TransactionId> {
        self.live_read_transactions
            .lock()
            .unwrap()
            .iter()
            .next()
            .cloned()
    }

    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    pub fn begin_write(&self) -> Result<WriteTransaction> {
        let transaction_id = self.allocate_write_transaction()?;
        // Safety: Allocating the transaction asserts that there is no live write transaction in progress
        unsafe { WriteTransaction::new(self, transaction_id) }
    }

    pub fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(ReadTransaction::new(self))
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
    pub fn set_dynamic_growth(&mut self, enabled: bool) -> &mut Self {
        self.dynamic_growth = enabled;
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

        Database::new(file, db_size, self.page_size, self.dynamic_growth)
    }
}
