use crate::tree_store::{
    get_db_size, AllPageNumbersBtreeIter, BtreeRangeIter, InternalTableDefinition, PageNumber,
    TransactionalMemory,
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

/// Defines the name and types of a table
///
/// A [`TableDefinition`] should be opened for use by calling [`ReadTransaction::open_table`] or [`WriteTransaction::open_table`]
// TODO: add trait bounds once const_fn_trait_bound is stable
pub struct TableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> TableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
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

/// Defines the name and types of a multimap table
///
/// A [`MultimapTableDefinition`] should be opened for use by calling [`ReadTransaction::open_multimap_table`] or [`WriteTransaction::open_multimap_table`]
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
///
pub struct MultimapTableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> MultimapTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
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

/// Opened redb database file
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`] object that can be used to read from the database
/// Use [`Self::begin_write`] to get a [`WriteTransaction`] object that can be used to read or write to the database
///
/// Multiple reads may be performed concurrently, with each other, and with writes. Only a single write
/// may be in progress at a time.
///
/// # Examples
///
/// Basic usage:
///
/// ```rust
/// use redb::*;
/// # use tempfile::NamedTempFile;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
/// # let filename = tmpfile.path();
/// # let db_max_size = 1024 * 1024;
/// let db = unsafe { Database::create(filename, db_max_size)? };
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// write_txn.commit()?;
/// # Ok(())
/// # }
/// ```
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
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?
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
                .get_data_root()
                .expect("Tried to repair an empty database");

            // Repair the allocator state
            // All pages in the master table
            let mut all_pages_iter: Box<dyn Iterator<Item = PageNumber>> =
                Box::new(AllPageNumbersBtreeIter::new(root, &mem));

            // Iterate over all other tables
            let mut iter: BtreeRangeIter<[u8], [u8]> =
                BtreeRangeIter::new::<RangeFull, [u8]>(.., Some(root), &mem);

            // Chain all the other tables to the master table iter
            while let Some(entry) = iter.next() {
                let definition = InternalTableDefinition::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    let table_pages_iter = AllPageNumbersBtreeIter::new(table_root, &mem);
                    all_pages_iter = Box::new(all_pages_iter.chain(table_pages_iter));
                }
            }

            mem.repair_allocator(all_pages_iter)?;

            // Clear the freed table. We just rebuilt the allocator state by walking all the
            // reachable data pages, which implicitly frees the pages for the freed table
            let transaction_id = mem.get_last_committed_transaction_id()? + 1;
            mem.commit(Some(root), None, transaction_id, false)?;
        }

        let next_transaction_id = mem.get_last_committed_transaction_id()? + 1;

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

    /// Convenience method for [`DatabaseBuilder::new`]
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a single
    /// write may be in progress at a time
    pub fn begin_write(&self) -> Result<WriteTransaction> {
        let guard = self.leaked_write_transaction.lock().unwrap();
        if let Some(leaked) = *guard {
            return Err(Error::LeakedWriteTransaction(leaked));
        }
        drop(guard);

        assert!(self.live_write_transaction.lock().unwrap().is_none());
        let id = self.next_transaction_id.fetch_add(1, Ordering::AcqRel);
        *self.live_write_transaction.lock().unwrap() = Some(id);
        // Safety: We just asserted there was no previous write in progress
        unsafe { WriteTransaction::new(self, id) }
    }

    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    pub fn begin_read(&self) -> Result<ReadTransaction> {
        let id = self.next_transaction_id.fetch_add(1, Ordering::AcqRel);
        self.live_read_transactions.lock().unwrap().insert(id);
        Ok(ReadTransaction::new(self, id))
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

    /// Set the internal page size of the database
    /// Larger page sizes will reduce the database file's overhead, but may decrease write performance
    /// Defaults to the native OS page size
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
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn create(&self, path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Database::new(file, db_size, self.page_size, self.dynamic_growth)
    }
}
