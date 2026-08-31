use crate::io;
use crate::transaction_tracker::{TransactionId, TransactionTracker};
use crate::tree_store::LocklessBackend;
#[cfg(feature = "experimental-multiprocess")]
use crate::tree_store::MultiProcessWriterGuard;
#[cfg(not(redb_no_std))]
use crate::tree_store::ReadOnlyBackend;
use crate::tree_store::{
    AllocationPolicy, BtreeHeader, InternalTableDefinition, PAGE_SIZE, PageHint, PageNumber,
    PageResolver, ShrinkPolicy, TableTree, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{
    CompactionError, DatabaseError, Error, ReadOnlyTable, ReadableTable, SavepointError,
    StorageError, TableError,
};
use crate::{ReadTransaction, Result, WriteTransaction};
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use core::fmt::{Debug, Display, Formatter};

use alloc::sync::Arc;
use core::marker::PhantomData;
use core::ops::Range;
#[cfg(not(redb_no_std))]
use std::fs::{File, OpenOptions};
#[cfg(not(redb_no_std))]
use std::path::Path;

use crate::error::TransactionError;
use crate::sealed::{Sealed, SealedInApi5};
use crate::transactions::{
    ALLOCATOR_STATE_TABLE_NAME, AllocatorStateKey, AllocatorStateTree, DATA_ALLOCATED_TABLE,
    DATA_FREED_TABLE, PageList, SYSTEM_FREED_TABLE, SystemTableDefinition,
    TransactionIdWithPagination,
};
#[cfg(not(redb_no_std))]
use crate::tree_store::file_backend::FileBackend;
#[cfg(feature = "logging")]
use log::{debug, warn};

#[allow(clippy::len_without_is_empty)]
/// Implements persistent storage for a database.
///
/// Failures are reported as [`io::Error`], which is [`std::io::Error`] whenever std is available.
pub trait StorageBackend: 'static + Debug + Send + Sync {
    /// Gets the current length of the storage.
    fn len(&self) -> core::result::Result<u64, io::Error>;

    /// Reads the specified array of bytes from the storage.
    ///
    /// If `out.len()` + `offset` exceeds the length of the storage an appropriate `Error` must be returned.
    fn read(&self, offset: u64, out: &mut [u8]) -> core::result::Result<(), io::Error>;

    /// Sets the length of the storage.
    ///
    /// New positions in the storage must be initialized to zero.
    fn set_len(&self, len: u64) -> core::result::Result<(), io::Error>;

    /// Syncs all buffered data with the persistent storage.
    fn sync_data(&self) -> core::result::Result<(), io::Error>;

    /// Writes the specified array to the storage.
    fn write(&self, offset: u64, data: &[u8]) -> core::result::Result<(), io::Error>;

    /// Release any resources held by the backend
    ///
    /// Note: redb will not access the backend after calling this method and will call it exactly
    /// once: when the [`Database`] is dropped, or, if a [`WriteTransaction`] was live at that
    /// point, when that transaction completes, or if opening the database fails
    fn close(&self) -> core::result::Result<(), io::Error> {
        Ok(())
    }
}

#[cfg_attr(redb_no_std, allow(dead_code))]
pub(crate) const FULL_RANGE: Range<u64> = 0..u64::MAX;

#[cfg_attr(not(any(windows, unix, target_os = "wasi")), allow(dead_code))]
const LOCK_BASE: u64 = 1 << 62;
/// An offset that is not used in the multi-process locking protocol. Used to detect whether
/// `flock()` and range locks share the same namespace, which only matters while a whole-file
/// lock is taken alongside the ranges.
#[cfg(not(feature = "experimental-api-5"))]
#[cfg_attr(not(any(windows, unix, target_os = "wasi")), allow(dead_code))]
pub(crate) const NAMESPACE_PROBE_BYTE: u64 = LOCK_BASE - 2;
/// Held exclusively by the writing process in single-writer mode.
#[cfg(feature = "experimental-multiprocess")]
pub(crate) const WRITER_BYTE: u64 = LOCK_BASE;
/// Whether the database is open for a single writer (held exclusively by it) or for many (held
/// shared by each writing process while the database is open).
#[cfg_attr(not(any(windows, unix, target_os = "wasi")), allow(dead_code))]
pub(crate) const SHARED_WRITER_BYTE: u64 = LOCK_BASE + 1;
/// Held shared by every read-only multi-process handle while the database is open, so that a
/// single-process open conflicts with a live reader no matter what the reader is doing.
#[cfg(feature = "experimental-multiprocess")]
pub(crate) const SHARED_READER_BYTE: u64 = LOCK_BASE + 2;
#[cfg(feature = "experimental-multiprocess")]
pub(crate) const IMMUTABLE_READER_BYTE: u64 = LOCK_BASE + 3;
/// Base of the "active transaction range": a handle reading transaction `t` holds `TXN_BASE + t`
/// shared for as long as it is reading it
#[cfg(feature = "experimental-multiprocess")]
pub(crate) const TXN_BASE: u64 = LOCK_BASE + 1024;

pub(crate) fn byte_range(offset: u64) -> Range<u64> {
    offset..offset + 1
}

/// A range reaching [`u64::MAX`] covers the entire storage.
#[cfg_attr(redb_no_std, allow(dead_code))]
pub(crate) trait InternalStorageBackend: StorageBackend {
    /// Whether this backend has locks to take at all -- custom backends do not.
    fn locks_expected(&self) -> bool {
        true
    }

    /// `Ok(false)` means a conflicting lock is held elsewhere.
    fn try_lock_range(&self, range: Range<u64>) -> core::result::Result<bool, io::Error>;

    /// `Ok(false)` means a conflicting lock is held elsewhere.
    fn try_lock_shared_range(&self, range: Range<u64>) -> core::result::Result<bool, io::Error>;

    /// Waits for the range rather than reporting a conflict. Only the multi-process header
    /// lock waits: the ranges an open takes are refused rather than queued.
    #[cfg(feature = "experimental-multiprocess")]
    fn lock_range(&self, range: Range<u64>) -> core::result::Result<(), io::Error>;

    #[cfg(feature = "experimental-multiprocess")]
    fn lock_shared_range(&self, range: Range<u64>) -> core::result::Result<(), io::Error>;

    fn unlock_range(&self, range: Range<u64>) -> core::result::Result<(), io::Error>;

    /// Whether an exclusive lock over the range would conflict with one held elsewhere.
    fn query_lock_range(&self, range: Range<u64>) -> core::result::Result<bool, io::Error>;
}

pub trait TableHandle: Sealed {
    // Returns the name of the table
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedTableHandle {
    name: String,
}

impl UntypedTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl TableHandle for UntypedTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Sealed for UntypedTableHandle {}

pub trait MultimapTableHandle: Sealed {
    // Returns the name of the multimap table
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedMultimapTableHandle {
    name: String,
}

impl UntypedMultimapTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl MultimapTableHandle for UntypedMultimapTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Sealed for UntypedMultimapTableHandle {}

/// Defines the name and types of a table
///
/// A [`TableDefinition`] should be opened for use by calling [`ReadTransaction::open_table`] or [`WriteTransaction::open_table`]
///
/// Note that the lifetime of the `K` and `V` type parameters does not impact the lifetimes of the data
/// that is stored or retreived from the table
pub struct TableDefinition<'a, K: Key + 'static, V: Value + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> TableDefinition<'a, K, V> {
    /// Construct a new table with given `name`
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty. When `name` is a non-empty string literal
    /// this is checked at compile time, but callers that build the name at
    /// runtime are responsible for ensuring it is non-empty.
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for TableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Value> Sealed for TableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Clone for TableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Value + 'static> Copy for TableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Display for TableDefinition<'_, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

/// Defines the name and types of a multimap table
///
/// A [`MultimapTableDefinition`] should be opened for use by calling [`ReadTransaction::open_multimap_table`] or [`WriteTransaction::open_multimap_table`]
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
///
/// Note that the lifetime of the `K` and `V` type parameters does not impact the lifetimes of the data
/// that is stored or retreived from the table
pub struct MultimapTableDefinition<'a, K: Key + 'static, V: Key + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Key + 'static> MultimapTableDefinition<'a, K, V> {
    /// Construct a new multimap table with given `name`
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty. When `name` is a non-empty string literal
    /// this is checked at compile time, but callers that build the name at
    /// runtime are responsible for ensuring it is non-empty.
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Key + 'static> MultimapTableHandle for MultimapTableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Key> Sealed for MultimapTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Key + 'static> Clone for MultimapTableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Key + 'static> Copy for MultimapTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Key + 'static> Display for MultimapTableDefinition<'_, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

/// Information regarding the usage of the in-memory cache
///
/// Note: these metrics are only collected when the "`cache_metrics`" feature is enabled
#[derive(Debug)]
pub struct CacheStats {
    pub(crate) evictions: u64,
    pub(crate) read_hits: u64,
    pub(crate) read_misses: u64,
    pub(crate) write_hits: u64,
    pub(crate) write_misses: u64,
    pub(crate) used_bytes: usize,
}

impl CacheStats {
    /// Number of times that data has been evicted, due to the cache being full
    ///
    /// To increase the cache size use [`Builder::set_cache_size`]
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Number of times that unmodified data has been read from the cache
    pub fn read_hits(&self) -> u64 {
        self.read_hits
    }

    /// Number of times that unmodified data was not in the cache and was read from storage
    pub fn read_misses(&self) -> u64 {
        self.read_misses
    }

    /// Number of times that data modified in a transaction has been read from the cache
    pub fn write_hits(&self) -> u64 {
        self.write_hits
    }

    /// Number of times that data modified in a transaction was not in the cache and was read from storage
    pub fn write_misses(&self) -> u64 {
        self.write_misses
    }

    /// Number of bytes in the cache
    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }
}

pub(crate) enum TransactionGuard {
    Read {
        tracker: Arc<TransactionTracker>,
        transaction_id: TransactionId,
        // `Some` while this guard owns the transaction's reference, carrying what the tracker
        // needs to release it. `None` when a savepoint becomes persistent: the database
        // owns the reference from then on and releases it when the savepoint is deleted
        reference: Option<Arc<TransactionalMemory>>,
    },
    Write {
        tracker: Arc<TransactionTracker>,
        transaction_id: TransactionId,
        // Owned alongside the write slot so the two can be ordered against each other: a
        // thread waiting on that slot takes the byte on this same file description the moment
        // it is free, so the byte must be released first and taken after.
        // None indicates that the writer lock is held at the database level -- i.e. in a single writer mode
        #[cfg(feature = "experimental-multiprocess")]
        multi_process_writer: Option<MultiProcessWriterGuard>,
    },
    // Used for internal accesses that happen outside of any tracked transaction,
    // such as opening the database, repairing it, and running integrity checks.
    Untracked,
}

impl TransactionGuard {
    pub(crate) fn new_read(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
        mem: &Arc<TransactionalMemory>,
    ) -> Self {
        Self::Read {
            tracker,
            transaction_id,
            reference: Some(mem.clone()),
        }
    }

    // A guard for a transaction whose reference the database already owns
    pub(crate) fn new_read_unowned(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
    ) -> Self {
        Self::Read {
            tracker,
            transaction_id,
            reference: None,
        }
    }

    // Leak the reference to the transaction. The caller becomes responsible for decrementing the
    // reference count.
    // Dropping this guard then releases nothing.
    pub(crate) fn release_to_database(&mut self) {
        let Self::Read { reference, .. } = self else {
            unreachable!("only a read transaction's reference may be leaked")
        };
        drop(reference.take());
    }

    pub(crate) fn owns_reference(&self) -> bool {
        matches!(self, Self::Read { reference, .. } if reference.is_some())
    }

    pub(crate) fn tracker(&self) -> &Arc<TransactionTracker> {
        match self {
            Self::Read { tracker, .. } | Self::Write { tracker, .. } => tracker,
            Self::Untracked => unreachable!("an untracked guard has no tracker"),
        }
    }

    pub(crate) fn allocate_read(
        tracker: Arc<TransactionTracker>,
        mem: &Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let id = tracker.register_read_transaction(mem)?;

        Ok(Self::new_read(id, tracker, mem))
    }

    pub(crate) fn new_write(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
        #[cfg(feature = "experimental-multiprocess")] mem: &Arc<TransactionalMemory>,
    ) -> Result<Self> {
        #[allow(unused_mut)]
        let mut guard = Self::Write {
            tracker,
            transaction_id,
            #[cfg(feature = "experimental-multiprocess")]
            multi_process_writer: None,
        };
        // After the guard owns the slot, so that a failure here releases it rather than
        // leaving the tracker with a live write transaction nothing will ever end
        #[cfg(feature = "experimental-multiprocess")]
        if let Self::Write {
            multi_process_writer,
            ..
        } = &mut guard
        {
            *multi_process_writer = TransactionalMemory::lock_multi_process_writer(mem)?;
        }

        Ok(guard)
    }

    pub(crate) fn untracked() -> Self {
        Self::Untracked
    }

    pub(crate) fn id(&self) -> TransactionId {
        match self {
            Self::Read { transaction_id, .. } | Self::Write { transaction_id, .. } => {
                *transaction_id
            }
            Self::Untracked => {
                panic!("TransactionGuard::id() called on an untracked guard")
            }
        }
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        match self {
            Self::Read {
                tracker,
                transaction_id,
                reference,
            } => {
                if let Some(mem) = reference {
                    tracker.deallocate_read_transaction(mem, *transaction_id);
                }
            }
            Self::Write {
                tracker,
                transaction_id,
                #[cfg(feature = "experimental-multiprocess")]
                    multi_process_writer: writer,
            } => {
                // Drop the "writer byte" multi-process file lock, before our in-process lock.
                // Otherwise, another transaction in our process could re-enter the file lock
                #[cfg(feature = "experimental-multiprocess")]
                drop(writer.take());
                if let Some(mem) = tracker.end_write_transaction(*transaction_id) {
                    // The Database was dropped while this transaction was live, deferring
                    // the database close to the end of this transaction
                    close_database(tracker, &mem);
                }
            }
            Self::Untracked => {}
        }
    }
}

pub trait ReadableDatabase: SealedInApi5 {
    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError>;

    /// Information regarding the usage of the in-memory cache
    ///
    /// Note: these metrics are only collected when the "`cache_metrics`" feature is enabled
    fn cache_stats(&self) -> CacheStats;
}

// Unavailable without std: every route to one goes through a path, and the file-backed API is
// gated out below.
/// A redb database opened in read-only mode
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`] object that can be used to read from the database
///
/// Multiple processes may open a [`ReadOnlyDatabase`], but it may not be opened concurrently
/// with a [`Database`].
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
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename)?;
/// let txn = db.begin_write()?;
/// {
///     let mut table = txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// txn.commit()?;
/// drop(db);
///
/// let db = ReadOnlyDatabase::open(filename)?;
/// let txn = db.begin_read()?;
/// {
///     let mut table = txn.open_table(TABLE)?;
///     println!("{}", table.get(&0)?.unwrap().value());
/// }
/// # Ok(())
/// # }
/// ```
#[cfg(not(redb_no_std))]
pub struct ReadOnlyDatabase {
    mem: Arc<TransactionalMemory>,
    transaction_tracker: Arc<TransactionTracker>,
}

#[cfg(not(redb_no_std))]
impl Sealed for ReadOnlyDatabase {}

#[cfg(not(redb_no_std))]
impl ReadableDatabase for ReadOnlyDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let guard = TransactionGuard::allocate_read(self.transaction_tracker.clone(), &self.mem)?;
        #[cfg(feature = "logging")]
        debug!("Beginning read transaction id={:?}", guard.id());

        ReadTransaction::new(self.mem.clone(), guard)
    }

    fn cache_stats(&self) -> CacheStats {
        self.mem.cache_stats()
    }
}

#[cfg(not(redb_no_std))]
impl ReadOnlyDatabase {
    /// Opens an existing redb database.
    #[cfg(not(redb_no_std))]
    pub fn open(path: impl AsRef<Path>) -> Result<ReadOnlyDatabase, DatabaseError> {
        Builder::new().open_read_only(path)
    }

    fn new(
        file: Box<dyn InternalStorageBackend>,
        page_size: usize,
        region_size: Option<u64>,
        cache_size: usize,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        debug!("Opening database in read-only {:?}", &file_path);
        let mem = TransactionalMemory::new(
            Box::new(ReadOnlyBackend::new(file)),
            false,
            page_size,
            region_size,
            cache_size,
            true,
            concurrency_mode,
        )?;
        let mem = Arc::new(mem);
        // If the last transaction used 2-phase commit and updated the allocator state table, then
        // we can just load the allocator state from there. Otherwise, we need a full repair
        if let Some(tree) = Database::get_allocator_state_table(&mem)? {
            mem.load_allocator_state(&tree)?;
        } else {
            #[cfg(feature = "logging")]
            warn!(
                "Database {:?} not shutdown cleanly. Repair required",
                &file_path
            );
            return Err(DatabaseError::RepairAborted);
        }

        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();
        let db = Self {
            mem,
            transaction_tracker: Arc::new(TransactionTracker::new(next_transaction_id)),
        };

        Ok(db)
    }
}

/// Opened redb database file
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`] object that can be used to read from the database
/// Use [`Self::begin_write`] to get a [`WriteTransaction`] object that can be used to read or write to the database
///
/// Multiple reads may be performed concurrently, with each other, and with writes. Only a single write
/// may be in progress at a time.
///
/// # Close semantics
///
/// Dropping the [`Database`] closes the database: buffered data is flushed, the file lock is
/// released, and outstanding [`ReadTransaction`]s are invalidated (their operations will return
/// [`StorageError::DatabaseClosed`]).
///
/// A live [`WriteTransaction`] keeps the database open, however: if one exists when the
/// [`Database`] is dropped, the transaction remains fully usable and the close described above
/// is deferred until the transaction commits, aborts, or is dropped. Until then the database
/// file remains locked, so re-opening it fails with [`DatabaseError::DatabaseAlreadyOpen`].
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
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename)?;
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
    mem: Arc<TransactionalMemory>,
    transaction_tracker: Arc<TransactionTracker>,
}

impl Sealed for Database {}

impl ReadableDatabase for Database {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let guard = TransactionGuard::allocate_read(self.transaction_tracker.clone(), &self.mem)?;
        #[cfg(feature = "logging")]
        debug!("Beginning read transaction id={:?}", guard.id());
        ReadTransaction::new(self.get_memory(), guard)
    }

    fn cache_stats(&self) -> CacheStats {
        self.mem.cache_stats()
    }
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    #[cfg(not(redb_no_std))]
    pub fn create(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Self::builder().create(path)
    }

    /// Opens an existing redb database.
    #[cfg(not(redb_no_std))]
    pub fn open(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Self::builder().open(path)
    }

    pub(crate) fn get_memory(&self) -> Arc<TransactionalMemory> {
        self.mem.clone()
    }

    pub(crate) fn verify_primary_checksums(mem: Arc<TransactionalMemory>) -> Result<bool> {
        let data_root = mem.get_data_root();
        let system_root = mem.get_system_root();
        Self::verify_checksums(mem, data_root, system_root)
    }

    // Verifies the checksums reachable from the given data and system roots, reading pages through
    // `mem` (i.e. from disk if its cache was invalidated first).
    fn verify_checksums(
        mem: Arc<TransactionalMemory>,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
    ) -> Result<bool> {
        let resolver = PageResolver::new(mem.clone());
        let table_tree = TableTree::new(
            data_root,
            PageHint::None,
            Arc::new(TransactionGuard::untracked()),
            resolver.clone(),
        )?;
        if !table_tree.verify_checksums()? {
            return Ok(false);
        }
        let system_table_tree = TableTree::new(
            system_root,
            PageHint::None,
            Arc::new(TransactionGuard::untracked()),
            resolver,
        )?;
        if !system_table_tree.verify_checksums()? {
            return Ok(false);
        }

        Ok(true)
    }

    /// Force a check of the integrity of the database file, and repair it if possible.
    ///
    /// Note: Calling this function is unnecessary during normal operation. redb will automatically
    /// detect and recover from crashes, power loss, and other unclean shutdowns. This function is
    /// quite slow and should only be used when you suspect the database file may have been modified
    /// externally to redb, or that a redb bug may have left the database in a corrupted state.
    ///
    /// Returns `Ok(true)` if the database passed integrity checks; `Ok(false)` if it failed but was repaired,
    /// and `Err(Corrupted)` if the check failed and the file could not be repaired.
    ///
    /// Returns [`DatabaseError::TransactionInProgress`] if any read or write transaction, or an
    /// ephemeral [`Savepoint`](crate::Savepoint), is still alive when this method is called.
    ///
    /// Transactions committed with [`Durability::None`](crate::Durability::None) that have not yet
    /// been made durable are made durable if the check passes, or rolled back if the database must
    /// be repaired.
    pub fn check_integrity(&mut self) -> Result<bool, DatabaseError> {
        if Arc::get_mut(&mut self.mem).is_none() {
            return Err(DatabaseError::TransactionInProgress);
        }
        // An ephemeral savepoint may pin a non-durable transaction whose pages the reload below
        // discards; restoring it afterwards could corrupt the database. Persistent savepoints are
        // durable, so they are unaffected.
        if self.transaction_tracker.any_ephemeral_savepoint_exists() {
            return Err(DatabaseError::TransactionInProgress);
        }

        // Report a latched I/O failure as such, not as the discarded allocator state it also causes
        self.mem.check_io_errors()?;
        // Once the allocator state has been discarded (by a failed commit or integrity check),
        // the database must be reopened to rebuild it; this check requires one to compare against
        if !self.mem.allocator_state_loaded() {
            return Err(StorageError::Corrupted(
                "Allocator state was discarded by a failed integrity check or commit; reopen the database to repair it".to_string(),
            )
            .into());
        }

        // Repairing rebuilds the allocator state, so a failure part way through leaves one that
        // describes neither the file nor anything else. Holding an allocator state must continue
        // to mean it describes the file.
        let result = self.check_integrity_inner();
        if result.is_err() {
            self.mem.invalidate_allocator_state();
        }
        result
    }

    fn check_integrity_inner(&mut self) -> Result<bool, DatabaseError> {
        // A pending Durability::None commit is acknowledged, live data that the reload below would
        // discard. If the live state verifies, promote it to durable rather than losing it -- even
        // if the durable state it replaces turns out to be corrupt, in which case we recover from
        // the live state and report not-clean.
        let mut rolling_back_non_durable = false;
        if self.mem.pending_non_durable_commit() {
            // Verify from disk, not the page cache, so external modification is detected.
            self.mem.clear_read_cache();
            // Don't promote over a truncated or extended file -- the committed layout would be
            // inconsistent with it. Fall through to reload + repair instead.
            if self.mem.file_len_matches_layout()?
                && let Some(live_allocator_clean) = self.repair_live_state()?
            {
                // The live tree is intact (its allocator state was rebuilt above if it was stale),
                // so promote the acknowledged commit rather than rolling it back. The result is
                // clean only if neither the allocator nor the durable state below needed repair.
                let durable_clean = self.durable_state_clean()?;
                let mut txn = self
                    .begin_write()
                    .map_err(|e| DatabaseError::Storage(e.into_storage_error()))?;
                txn.disable_post_commit_free();
                txn.commit()
                    .map_err(|e| DatabaseError::Storage(e.into_storage_error()))?;
                // The rebuild above reclaimed any leaked pages, so the close may record a
                // clean shutdown again
                self.mem.clear_needs_repair();
                return Ok(live_allocator_clean && durable_clean);
            }
            // The live tree is corrupt (or the file size changed), so the reload rolls the commit
            // back -- not clean, even if the durable state it falls back to is intact.
            rolling_back_non_durable = true;
        }

        // No pending commit, or fall-through: verify and repair the durable state. Capture the
        // allocator hash to compare against the rebuild below; with the pending case handled above,
        // the live and durable states are identical here, so this is a valid check.
        let allocator_hash = self.mem.allocator_hash();
        let mem = Arc::get_mut(&mut self.mem).unwrap();
        let mut was_clean = mem.clear_cache_and_reload()?;

        let old_roots = [self.mem.get_data_root(), self.mem.get_system_root()];

        let new_roots = Self::do_repair(&mut self.mem, &|_| {}).map_err(|err| match err {
            DatabaseError::Storage(storage_err) => storage_err,
            _ => unreachable!(),
        })?;

        if old_roots != new_roots
            || allocator_hash != self.mem.allocator_hash()
            || rolling_back_non_durable
        {
            was_clean = false;
        }

        if !was_clean {
            let next_transaction_id = self.mem.get_last_committed_transaction_id()?.next();
            let [data_root, system_root] = new_roots;
            self.mem.commit(
                data_root,
                system_root,
                next_transaction_id,
                true,
                ShrinkPolicy::Never,
            )?;
            // Reserve the id, or the next write transaction would commit with the same one,
            // which crash recovery could then resolve to the wrong slot
            self.transaction_tracker
                .reserve_repair_transaction_id(next_transaction_id);
        }

        // The rebuild reclaimed any leaked pages, so the close may record a clean shutdown
        // again
        self.mem.clear_needs_repair();
        self.mem.begin_writable()?;

        Ok(was_clean)
    }

    // Verifies, and repairs in memory, the live (possibly non-durable) state. Returns:
    // - `None` if the live tree is corrupt and the commit must be rolled back;
    // - `Some(true)` if the live state is fully clean;
    // - `Some(false)` if the tree is intact but its allocator state was stale and has been rebuilt,
    //   so promoting it repairs the allocator while the check reports not-clean.
    // The allocator is rebuilt from the live roots -- a rebuild from the durable roots would
    // falsely differ when the live state is ahead of durable (e.g. a durable commit's free-page
    // epilogue).
    fn repair_live_state(&mut self) -> Result<Option<bool>, DatabaseError> {
        match Self::verify_primary_checksums(self.mem.clone()) {
            Ok(true) => {
                let live_allocator_hash = self.mem.allocator_hash();
                let live_roots = [self.mem.get_data_root(), self.mem.get_system_root()];
                match Self::rebuild_allocator_state(&mut self.mem, &|_| {}) {
                    // Only a durable root can be rewritten from here, so a live root whose table
                    // count was recomputed must be rolled back and repaired by the reload below
                    Ok(roots) if roots != live_roots => Ok(None),
                    Ok(_) => Ok(Some(live_allocator_hash == self.mem.allocator_hash())),
                    Err(DatabaseError::Storage(StorageError::Corrupted(_))) => Ok(None),
                    Err(err) => Err(err),
                }
            }
            Ok(false) | Err(StorageError::Corrupted(_)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    // Whether the durable (primary slot) state is intact. Any corruption -- a bad primary slot
    // checksum, a checksum mismatch, or an error raised while walking a malformed tree -- counts as
    // not-clean: the caller promotes the verified live commit to recover from it, so corruption
    // here must not abort the check. Only non-corruption errors (e.g. I/O) propagate.
    fn durable_state_clean(&self) -> Result<bool, DatabaseError> {
        match self.verify_durable_state() {
            Ok(clean) => Ok(clean),
            Err(DatabaseError::Storage(StorageError::Corrupted(_))) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn verify_durable_state(&self) -> Result<bool, DatabaseError> {
        if self.mem.durable_primary_slot_corrupt()? {
            return Ok(false);
        }
        let data_root = self.mem.get_durable_data_root();
        let system_root = self.mem.get_durable_system_root();
        Ok(Self::verify_checksums(
            self.mem.clone(),
            data_root,
            system_root,
        )?)
    }

    /// Compacts the database file
    ///
    /// Returns `true` if compaction was performed, and `false` if no futher compaction was possible
    pub fn compact(&mut self) -> Result<bool, CompactionError> {
        // These checks must run before begin_write(): the caller may legally hold an open
        // WriteTransaction (it is not lifetime-bound to the Database), and if that transaction
        // created a savepoint, blocking in begin_write() below would deadlock. Savepoints must
        // be diagnosed before read references, because every live savepoint also holds a read
        // reference. The tracker covers persistent savepoints created by previous Database
        // instances, because they are re-registered when the database is opened.
        if self.transaction_tracker.any_persistent_savepoint_exists() {
            return Err(CompactionError::PersistentSavepointExists);
        }
        if self.transaction_tracker.any_savepoint_exists() {
            return Err(CompactionError::EphemeralSavepointExists);
        }
        if self.transaction_tracker.any_user_read_reference_exists() {
            return Err(CompactionError::TransactionInProgress);
        }
        // Use 2-phase commit to avoid any possible security issues. Plus this compaction is going to be so slow that it doesn't matter.
        // Once https://github.com/cberner/redb/issues/829 is fixed, we should upgrade this to use quick-repair -- that way the user
        // can cancel the compaction without requiring a full repair afterwards
        let txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        // Re-check inside the write transaction: a concurrent writer may have created a
        // savepoint between the checks above and the start of this transaction.
        if txn.list_persistent_savepoints()?.next().is_some() {
            return Err(CompactionError::PersistentSavepointExists);
        }
        if self.transaction_tracker.any_savepoint_exists() {
            return Err(CompactionError::EphemeralSavepointExists);
        }
        if self.transaction_tracker.any_user_read_reference_exists() {
            return Err(CompactionError::TransactionInProgress);
        }
        txn.abort()?;
        // Commit to free up any pending free pages
        self.drain_pending_free_pages(ShrinkPolicy::Maximum)?;

        let mut compacted = false;
        // Iteratively compact until no progress is made
        loop {
            let mut progress = false;

            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            if txn.compact_pages()? {
                progress = true;
                txn.commit().map_err(|e| e.into_storage_error())?;
            } else {
                txn.abort()?;
            }

            // Drain pages freed by compact_pages(), including system pages queued by any
            // post-commit cleanup root updates.
            self.drain_pending_free_pages(ShrinkPolicy::Maximum)?;

            if !progress {
                break;
            }

            compacted = true;
        }

        Ok(compacted)
    }

    fn drain_pending_free_pages(&self, shrink_policy: ShrinkPolicy) -> Result {
        // Preserve compact()'s empty durable commit, which also publishes pending
        // non-durable roots before checking for pending frees.
        let mut force_commit = true;
        loop {
            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            if !force_commit && !txn.pending_free_pages()? {
                txn.abort()?;
                return Ok(());
            }
            force_commit = false;
            txn.set_two_phase_commit(true);
            txn.set_shrink_policy(shrink_policy);
            txn.commit().map_err(|e| e.into_storage_error())?;
        }
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    fn check_repaired_allocated_pages_table(
        system_root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
    ) -> Result {
        let resolver = PageResolver::new(mem.clone());
        let table_tree = TableTree::new(
            system_root,
            PageHint::None,
            Arc::new(TransactionGuard::untracked()),
            resolver.clone(),
        )?;
        if let Some(table_def) = table_tree
            .get_table::<TransactionIdWithPagination, PageList>(
                DATA_ALLOCATED_TABLE.name(),
                TableType::Normal,
            )
            .map_err(|e| e.into_storage_error_or_corrupted("Allocated pages table corrupted"))?
        {
            let InternalTableDefinition::Normal { table_root, .. } = table_def else {
                unreachable!()
            };
            let table: ReadOnlyTable<TransactionIdWithPagination, PageList> = ReadOnlyTable::new(
                DATA_ALLOCATED_TABLE.name().to_string(),
                table_root,
                PageHint::None,
                Arc::new(TransactionGuard::untracked()),
                resolver,
            )?;
            for result in ReadableTable::iter(&table)? {
                let (_, pages) = result?;
                for i in 0..pages.value().len() {
                    assert!(mem.is_allocated(pages.value().get(i)));
                }
            }
        }

        Ok(())
    }

    fn visit_freed_tree<K: Key, V: Value, F>(
        system_root: Option<BtreeHeader>,
        table_def: SystemTableDefinition<K, V>,
        mem: Arc<TransactionalMemory>,
        mut visitor: F,
    ) -> Result
    where
        F: FnMut(PageNumber) -> Result,
    {
        let untracked_guard = Arc::new(TransactionGuard::untracked());
        let resolver = PageResolver::new(mem.clone());
        let system_tree = TableTree::new(
            system_root,
            PageHint::None,
            untracked_guard,
            resolver.clone(),
        )?;
        let table_name = table_def.name();
        let result = match system_tree.get_table::<K, V>(table_name, TableType::Normal) {
            Ok(result) => result,
            Err(TableError::Storage(err)) => {
                return Err(err);
            }
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(());
            }
            Err(_) => {
                return Err(StorageError::Corrupted(format!(
                    "Unable to open {table_name}"
                )));
            }
        };

        if let Some(definition) = result {
            let table_root = match definition {
                InternalTableDefinition::Normal { table_root, .. } => table_root,
                InternalTableDefinition::Multimap { .. } => unreachable!(),
            };
            let table: ReadOnlyTable<TransactionIdWithPagination, PageList<'static>> =
                ReadOnlyTable::new(
                    table_name.to_string(),
                    table_root,
                    PageHint::None,
                    Arc::new(TransactionGuard::untracked()),
                    resolver,
                )?;
            for result in ReadableTable::iter(&table)? {
                let (_, page_list) = result?;
                for i in 0..page_list.value().len() {
                    visitor(page_list.value().get(i))?;
                }
            }
        }

        Ok(())
    }

    #[cfg(debug_assertions)]
    fn mark_allocated_page_for_debug(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
    ) -> Result {
        let data_root = mem.get_data_root();
        {
            let untracked = Arc::new(TransactionGuard::untracked());
            let tables = TableTree::new(
                data_root,
                PageHint::None,
                untracked,
                PageResolver::new(mem.clone()),
            )?;
            tables.visit_all_pages(|path| {
                mem.mark_debug_allocated_page(path.page_number());
                Ok(())
            })?;
        }

        let system_root = mem.get_system_root();
        {
            let untracked = Arc::new(TransactionGuard::untracked());
            let system_tables = TableTree::new(
                system_root,
                PageHint::None,
                untracked,
                PageResolver::new(mem.clone()),
            )?;
            system_tables.visit_all_pages(|path| {
                mem.mark_debug_allocated_page(path.page_number());
                Ok(())
            })?;
        }

        Self::visit_freed_tree(system_root, DATA_FREED_TABLE, mem.clone(), |page| {
            mem.mark_debug_allocated_page(page);
            Ok(())
        })?;
        Self::visit_freed_tree(system_root, SYSTEM_FREED_TABLE, mem.clone(), |page| {
            mem.mark_debug_allocated_page(page);
            Ok(())
        })?;

        Ok(())
    }

    // Whether the primary slot's trees verify. A Corrupted error counts as "they do not", so that
    // a torn slot carrying an invalid page number falls back to the secondary like any other bad
    // primary, rather than aborting the repair. Other errors (e.g. I/O) still propagate.
    fn primary_verifies(mem: &Arc<TransactionalMemory>) -> Result<bool> {
        match Self::verify_primary_checksums(mem.clone()) {
            Ok(verified) => Ok(verified),
            Err(StorageError::Corrupted(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn do_repair(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<[Option<BtreeHeader>; 2], DatabaseError> {
        if !Self::primary_verifies(mem)? {
            if mem.used_two_phase_commit() {
                return Err(DatabaseError::Storage(StorageError::Corrupted(
                    "Primary is corrupted despite 2-phase commit".to_string(),
                )));
            }

            // 0.3 because the repair takes 3 full scans and the first is done now
            let mut handle = RepairSession::new(0.3);
            repair_callback(&mut handle);
            if handle.aborted() {
                return Err(DatabaseError::RepairAborted);
            }

            mem.repair_primary_corrupted();
            // We need to invalidate the userspace cache, because walking the tree in verify_primary_checksums() may
            // have poisoned it with pages that just got rolled back by repair_primary_corrupted(), since
            // that rolls back a partially committed transaction.
            mem.clear_read_cache();
            if !Self::primary_verifies(mem)? {
                return Err(DatabaseError::Storage(StorageError::Corrupted(
                    "Failed to repair database. All roots are corrupted".to_string(),
                )));
            }
        }
        // 0.6 because the repair takes 3 full scans and the second is done now
        let mut handle = RepairSession::new(0.6);
        repair_callback(&mut handle);
        if handle.aborted() {
            return Err(DatabaseError::RepairAborted);
        }

        let [data_root, system_root] = Self::rebuild_allocator_state(mem, repair_callback)?;

        mem.clear_recovery_required()?;

        // We need to invalidate the userspace cache, because we're about to implicitly free the freed table
        // by storing an empty root during the below commit()
        mem.clear_read_cache();

        Ok([data_root, system_root])
    }

    // Rebuilds the in-memory allocator state by marking every page reachable from the current
    // roots (including the pages referenced by the freed-page tables) as allocated. Operates
    // purely on in-memory state and does not modify the file.
    //
    // The returned roots carry table counts recounted from the trees that were walked. These
    // counts are stored in the commit slot rather than in a page, so no page checksum covers
    // them; recounting here is what lets the rest of the codebase trust them.
    fn rebuild_allocator_state(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<[Option<BtreeHeader>; 2], DatabaseError> {
        mem.reset_allocator_state()?;

        let data_root = {
            let root = mem.get_data_root();
            let untracked = Arc::new(TransactionGuard::untracked());
            let tables = TableTree::new(
                root,
                PageHint::None,
                untracked,
                PageResolver::new(mem.clone()),
            )?;
            tables.visit_all_pages(|path| mem.mark_page_allocated(path.page_number()))?;
            Self::with_recounted_length(root, tables.count_tables()?)
        };

        // 0.9 because the repair takes 3 full scans and the third is done now. There is just some system tables left
        let mut handle = RepairSession::new(0.9);
        repair_callback(&mut handle);
        if handle.aborted() {
            return Err(DatabaseError::RepairAborted);
        }

        let system_root = {
            let root = mem.get_system_root();
            let untracked = Arc::new(TransactionGuard::untracked());
            let system_tables = TableTree::new(
                root,
                PageHint::None,
                untracked,
                PageResolver::new(mem.clone()),
            )?;
            system_tables.visit_all_pages(|path| mem.mark_page_allocated(path.page_number()))?;
            Self::with_recounted_length(root, system_tables.count_tables()?)
        };

        Self::visit_freed_tree(system_root, DATA_FREED_TABLE, mem.clone(), |page| {
            mem.mark_page_allocated(page)
        })?;
        Self::visit_freed_tree(system_root, SYSTEM_FREED_TABLE, mem.clone(), |page| {
            mem.mark_page_allocated(page)
        })?;
        // Non-durable commits hold their data-freed records in memory rather than in
        // DATA_FREED_TABLE, so the walk above does not reach those pages. They are still allocated
        // until a commit processes the records, so mark them like any other freed-table page.
        for page in mem.unpersisted_data_freed_pages() {
            mem.mark_page_allocated(page)?;
        }
        #[cfg(debug_assertions)]
        {
            Self::check_repaired_allocated_pages_table(system_root, mem.clone())?;
        }

        Ok([data_root, system_root])
    }

    fn with_recounted_length(root: Option<BtreeHeader>, length: u64) -> Option<BtreeHeader> {
        root.map(|header| BtreeHeader::new(header.root, header.checksum, length))
    }

    fn new(
        file: Box<dyn InternalStorageBackend>,
        allow_initialize: bool,
        page_size: usize,
        region_size: Option<u64>,
        cache_size: usize,
        concurrency_mode: ConcurrencyMode,
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        debug!("Opening database {:?}", &file_path);
        let mem = TransactionalMemory::new(
            file,
            allow_initialize,
            page_size,
            region_size,
            cache_size,
            false,
            concurrency_mode,
        )?;
        let mut mem = Arc::new(mem);
        // If the last transaction used 2-phase commit and updated the allocator state table, then
        // we can just load the allocator state from there. Otherwise, we need a full repair
        if let Some(tree) = Self::get_allocator_state_table(&mem)? {
            #[cfg(feature = "logging")]
            debug!("Found valid allocator state, full repair not needed");
            mem.load_allocator_state(&tree)?;
            #[cfg(debug_assertions)]
            Self::mark_allocated_page_for_debug(&mut mem)?;
        } else {
            #[cfg(feature = "logging")]
            warn!("Database {:?} not shutdown cleanly. Repairing", &file_path);
            let mut handle = RepairSession::new(0.0);
            repair_callback(&mut handle);
            if handle.aborted() {
                return Err(DatabaseError::RepairAborted);
            }
            let [data_root, system_root] = Self::do_repair(&mut mem, repair_callback)?;
            let next_transaction_id = mem.get_last_committed_transaction_id()?.next();
            mem.commit(
                data_root,
                system_root,
                next_transaction_id,
                true,
                ShrinkPolicy::Never,
            )?;
        }

        mem.begin_writable()?;
        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();

        let db = Database {
            mem,
            transaction_tracker: Arc::new(TransactionTracker::new(next_transaction_id)),
        };

        // Restore the tracker state for any persistent savepoints
        let txn = db.begin_write().map_err(|e| e.into_storage_error())?;
        if let Some(next_id) = txn.next_persistent_savepoint_id()? {
            db.transaction_tracker
                .restore_savepoint_counter_state(next_id);
        }
        for id in txn.list_persistent_savepoints()? {
            let savepoint = match txn.get_persistent_savepoint(id) {
                Ok(savepoint) => savepoint,
                Err(err) => match err {
                    SavepointError::InvalidSavepoint
                    | SavepointError::ImmediateDurabilityRequired => unreachable!(),
                    SavepointError::Storage(storage) => {
                        return Err(storage.into());
                    }
                },
            };
            db.transaction_tracker
                .register_persistent_savepoint(&db.mem, &savepoint)?;
        }
        txn.abort()?;

        Ok(db)
    }

    fn get_allocator_state_table(
        mem: &Arc<TransactionalMemory>,
    ) -> Result<Option<AllocatorStateTree>> {
        // The allocator state table is only valid if the primary was written using 2-phase commit
        if !mem.used_two_phase_commit() {
            return Ok(None);
        }

        // See if it's present in the system table tree
        let resolver = PageResolver::new(mem.clone());
        let system_table_tree = TableTree::new(
            mem.get_system_root(),
            PageHint::None,
            Arc::new(TransactionGuard::untracked()),
            resolver.clone(),
        )?;
        let Some(allocator_state_table) = system_table_tree
            .get_table::<AllocatorStateKey, &[u8]>(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?
        else {
            return Ok(None);
        };

        // Load the allocator state table
        let InternalTableDefinition::Normal { table_root, .. } = allocator_state_table else {
            unreachable!();
        };
        let tree = AllocatorStateTree::new(
            table_root,
            PageHint::None,
            Arc::new(TransactionGuard::untracked()),
            resolver,
        )?;

        // Make sure this isn't stale allocator state left over from a previous transaction
        if !mem.is_valid_allocator_state(&tree)? {
            return Ok(None);
        }

        Ok(Some(tree))
    }

    /// Convenience method for [`Builder::new`]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a single
    /// write may be in progress at a time. If a write is in progress, this function will block
    /// until it completes.
    ///
    /// The returned transaction is not lifetime-bound to this [`Database`] and keeps the
    /// database open: if the [`Database`] is dropped while the transaction is live, the
    /// transaction remains usable and the database closes when the transaction completes.
    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        begin_write_with_allocation_policy(
            &self.transaction_tracker,
            &self.mem,
            AllocationPolicy::Default,
        )
    }
}

// The allocation policy is fixed for the lifetime of the transaction; every page allocation
// this transaction makes goes through it.
fn begin_write_with_allocation_policy(
    transaction_tracker: &Arc<TransactionTracker>,
    mem: &Arc<TransactionalMemory>,
    allocation_policy: AllocationPolicy,
) -> Result<WriteTransaction, TransactionError> {
    // Fail early if there has been an I/O error -- nothing can be committed in that case
    mem.check_io_errors()?;
    let guard = TransactionGuard::new_write(
        transaction_tracker.start_write_transaction(),
        transaction_tracker.clone(),
        #[cfg(feature = "experimental-multiprocess")]
        mem,
    )?;
    // Re-checked after acquiring the write slot: the writer this call blocked on can fail its
    // commit, latching an I/O error and discarding the allocator state. The I/O check comes
    // first so a backend failure is not misreported as corruption. Returning drops the guard,
    // releasing the slot.
    mem.check_io_errors()?;
    if !mem.allocator_state_loaded() {
        return Err(StorageError::Corrupted(
            "Allocator state was discarded by a failed integrity check or commit; reopen the database to repair it".to_string(),
        )
        .into());
    }
    WriteTransaction::new(
        guard,
        transaction_tracker.clone(),
        mem.clone(),
        allocation_policy,
    )
    .map_err(|e| e.into())
}

fn ensure_allocator_state_table_and_trim(
    transaction_tracker: &Arc<TransactionTracker>,
    mem: &Arc<TransactionalMemory>,
) -> Result<(), Error> {
    // Make a new quick-repair commit to update the allocator state table
    #[cfg(feature = "logging")]
    debug!("Writing allocator state table");
    // If compact() left no free pages, the default allocator lands this
    // commit's writes at high page indices (see AllocationPolicy::Lowest)
    // and try_shrink can't reclaim the growth. See
    // https://github.com/cberner/redb/issues/1165
    let mut tx =
        begin_write_with_allocation_policy(transaction_tracker, mem, AllocationPolicy::Lowest)?;
    tx.set_quick_repair(true);
    tx.disable_post_commit_free();
    tx.set_shrink_policy(ShrinkPolicy::Maximum);
    tx.commit()?;

    Ok(())
}

// Closes the database: persists the allocator state table, so that the next open does not
// require a repair, and closes the storage backend. Runs exactly once, when the database
// closes: from Database::drop, or from the end of the write transaction that was live at
// that point. In both cases the Database is being, or has been, dropped, so no new write
// transaction can be started concurrently and the commit in here cannot block on the
// write-transaction slot.
fn close_database(transaction_tracker: &Arc<TransactionTracker>, mem: &Arc<TransactionalMemory>) {
    // No saved allocator state when it needs repair: the next open must rebuild it instead
    // of trusting the saved one
    if !crate::panicking()
        && !mem.needs_repair()
        && ensure_allocator_state_table_and_trim(transaction_tracker, mem).is_err()
    {
        #[cfg(feature = "logging")]
        warn!("Failed to write allocator state table. Repair may be required at restart.");
    }

    if mem.close().is_err() {
        #[cfg(feature = "logging")]
        warn!("Failed to flush database file. Repair may be required at restart.");
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self
            .transaction_tracker
            .defer_close_if_write_transaction_live(&self.mem)
        {
            // The write transaction holds the memory and tracker alive, so it remains fully
            // usable; TransactionGuard::drop performs the deferred close when it ends
            #[cfg(feature = "logging")]
            warn!(
                "Database dropped while a write transaction is in progress. The database will remain open until the write transaction completes."
            );
            return;
        }

        close_database(&self.transaction_tracker, &self.mem);
    }
}

pub struct RepairSession {
    progress: f64,
    aborted: bool,
}

impl RepairSession {
    pub(crate) fn new(progress: f64) -> Self {
        Self {
            progress,
            aborted: false,
        }
    }

    pub(crate) fn aborted(&self) -> bool {
        self.aborted
    }

    /// Abort the repair process. The coorresponding call to [`Builder::open`] or [`Builder::create`] will return an error
    pub fn abort(&mut self) {
        self.aborted = true;
    }

    /// Returns an estimate of the repair progress in the range [0.0, 1.0). At 1.0 the repair is complete.
    pub fn progress(&self) -> f64 {
        self.progress
    }
}

/// The sharing mode between processes accessing the database
///
/// Every process opening one database concurrently must use a compatible mode: one
/// `SingleWriterProcess` writer or any number of `MultiWriterProcess` writers, plus read-only
/// handles in either case. The multi-process modes need byte-range file locks, so they are
/// supported on Linux, the Apple platforms and Windows; elsewhere, opening a database in one of
/// them fails.
#[cfg_attr(
    not(feature = "experimental-multiprocess"),
    allow(dead_code, unreachable_pub, clippy::enum_variant_names)
)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub enum ConcurrencyMode {
    /// The database is not shared: one process may open it for writing, or several read-only.
    /// Enforced by locking the whole file. The default.
    #[default]
    SingleProcess,
    /// One process writes; any number of processes may, concurrently, open the database read-only.
    SingleWriterProcess,
    /// Any number of processes may, concurrently open the database for reading and writing.
    /// Only one write transaction may be open at a time.
    MultiWriterProcess,
}

#[cfg(feature = "experimental-multiprocess")]
impl ConcurrencyMode {
    /// Whether another process may have the database open, concurrently, and one process
    /// (possibly this one) is a writer
    pub(crate) fn is_multi_process_writable(self) -> bool {
        !matches!(self, ConcurrencyMode::SingleProcess)
    }
}

/// Configuration builder of a redb [Database].
pub struct Builder {
    page_size: usize,
    region_size: Option<u64>,
    cache_size: usize,
    concurrency_mode: ConcurrencyMode,
    repair_callback: Box<dyn Fn(&mut RepairSession)>,
}

impl Builder {
    /// Construct a new [Builder] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `cache_size_bytes`: 1GiB
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            // Default to 4k pages. Benchmarking showed that this was a good default on all platforms,
            // including MacOS with 16k pages. Therefore, users are not allowed to configure it at the moment.
            // It is part of the file format, so can be enabled in the future.
            page_size: PAGE_SIZE,
            region_size: None,
            concurrency_mode: ConcurrencyMode::SingleProcess,
            cache_size: 1024 * 1024 * 1024,
            repair_callback: Box::new(|_| {}),
        }
    }

    /// Set a callback which will be invoked periodically in the event that the database file needs
    /// to be repaired.
    ///
    /// The [`RepairSession`] argument can be used to control the repair process.
    ///
    /// If the database file needs repair, the callback will be invoked at least once.
    /// There is no upper limit on the number of times it may be called.
    pub fn set_repair_callback(
        &mut self,
        callback: impl Fn(&mut RepairSession) + 'static,
    ) -> &mut Self {
        self.repair_callback = Box::new(callback);
        self
    }

    /// Set the internal page size of the database
    ///
    /// Valid values are powers of two, greater than or equal to 512
    ///
    /// ## Defaults
    ///
    /// Default to 4 Kib pages.
    #[cfg(any(fuzzing, test))]
    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = core::cmp::max(size, 512);
        self
    }

    /// Set the amount of memory (in bytes) used for caching data
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.cache_size = bytes;
        self
    }

    /// Set how processes may share this database. Defaults to [`ConcurrencyMode::SingleProcess`].
    #[cfg(feature = "experimental-multiprocess")]
    pub fn set_concurrency_mode(&mut self, mode: ConcurrencyMode) -> &mut Self {
        self.concurrency_mode = mode;
        self
    }

    #[cfg(any(test, fuzzing))]
    pub fn set_region_size(&mut self, size: u64) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    #[cfg(not(redb_no_std))]
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Database::new(
            Box::new(FileBackend::new(file)?),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            self.concurrency_mode,
            &self.repair_callback,
        )
    }

    /// Opens an existing redb database.
    #[cfg(not(redb_no_std))]
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Database::new(
            Box::new(FileBackend::new(file)?),
            false,
            self.page_size,
            None,
            self.cache_size,
            self.concurrency_mode,
            &self.repair_callback,
        )
    }

    /// Opens an existing redb database.
    ///
    /// If the file has been opened for writing (i.e. as a [`Database`]) [`DatabaseError::DatabaseAlreadyOpen`]
    /// will be returned on platforms which support file locks (macOS, Windows, Linux). On other platforms,
    /// the caller MUST avoid calling this method when the database is open for writing.
    #[cfg(not(redb_no_std))]
    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let file = OpenOptions::new().read(true).open(path)?;

        ReadOnlyDatabase::new(
            Box::new(FileBackend::new(file)?),
            self.page_size,
            None,
            self.cache_size,
            self.concurrency_mode,
        )
    }

    /// Open an existing or create a new database in the given `file`.
    ///
    /// The file must be empty or contain a valid database.
    #[cfg(not(redb_no_std))]
    pub fn create_file(&self, file: File) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(FileBackend::new(file)?),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            self.concurrency_mode,
            &self.repair_callback,
        )
    }

    /// Open an existing or create a new database with the given backend.
    pub fn create_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            LocklessBackend::boxed(backend),
            true,
            self.page_size,
            self.region_size,
            self.cache_size,
            self.concurrency_mode,
            &self.repair_callback,
        )
    }
}

impl core::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

#[cfg(test)]
mod test {
    use crate::backends::FileBackend;
    use crate::{
        CommitError, Database, DatabaseError, Durability, ReadableTable, StorageBackend,
        StorageError, TableDefinition, TransactionError,
    };
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::fs::File;
    use std::io::{ErrorKind, Read, Seek, SeekFrom};

    #[derive(Debug)]
    struct FailingBackend {
        inner: FileBackend,
        countdown: Arc<AtomicU64>,
    }

    impl FailingBackend {
        fn new(backend: FileBackend, countdown: u64) -> Self {
            Self {
                inner: backend,
                countdown: Arc::new(AtomicU64::new(countdown)),
            }
        }

        fn check_countdown(&self) -> Result<(), std::io::Error> {
            if self.countdown.load(Ordering::SeqCst) == 0 {
                return Err(std::io::Error::from(ErrorKind::Other));
            }

            Ok(())
        }

        fn decrement_countdown(&self) -> Result<(), std::io::Error> {
            if self
                .countdown
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                    if x > 0 { Some(x - 1) } else { None }
                })
                .is_err()
            {
                return Err(std::io::Error::from(ErrorKind::Other));
            }

            Ok(())
        }
    }

    impl StorageBackend for FailingBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.check_countdown()?;
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), std::io::Error> {
            self.check_countdown()?;
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            self.decrement_countdown()?;
            self.inner.write(offset, data)
        }
    }

    #[test]
    fn crash_regression4() {
        let tmpfile = crate::create_tempfile();
        let (file, path) = tmpfile.into_parts();

        let backend = FailingBackend::new(FileBackend::new(file).unwrap(), 20);
        let db = Database::builder()
            .set_cache_size(12686)
            .set_page_size(8 * 1024)
            .set_region_size(32 * 4096)
            .create_with_backend(backend)
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        let _savepoint = tx.ephemeral_savepoint().unwrap();
        let _persistent_savepoint = tx.persistent_savepoint().unwrap();
        tx.commit().unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            let _ = table.insert_reserve(118821, 360).unwrap();
        }
        let result = tx.commit();
        assert!(result.is_err());

        drop(db);
        Database::builder()
            .set_cache_size(1024 * 1024)
            .set_page_size(8 * 1024)
            .set_region_size(32 * 4096)
            .create(&path)
            .unwrap();
    }

    #[test]
    fn transient_io_error() {
        let tmpfile = crate::create_tempfile();
        let (file, path) = tmpfile.into_parts();

        let backend = FailingBackend::new(FileBackend::new(file).unwrap(), u64::MAX);
        let countdown = backend.countdown.clone();
        let db = Database::builder()
            .set_cache_size(0)
            .create_with_backend(backend)
            .unwrap();

        let table_def: TableDefinition<u64, u64> = TableDefinition::new("x");

        // Create some garbage
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            table.insert(0, 0).unwrap();
        }
        tx.commit().unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            table.insert(0, 1).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.begin_write().unwrap();
        // Cause an error in the commit
        countdown.store(0, Ordering::SeqCst);
        let result = tx.commit().err().unwrap();
        assert!(matches!(result, CommitError::Storage(StorageError::Io(_))));
        let result = db.begin_write().err().unwrap();
        assert!(matches!(
            result,
            TransactionError::Storage(StorageError::PreviousIo)
        ));
        // Simulate a transient error
        countdown.store(u64::MAX, Ordering::SeqCst);
        drop(db);

        // Check that recovery flag is set, even though the error has "cleared"
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(9)).unwrap();
        let mut god_byte = vec![0u8];
        assert_eq!(file.read(&mut god_byte).unwrap(), 1);
        assert_ne!(god_byte[0] & 2, 0);
    }

    #[test]
    fn small_pages() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
        let txn = db.begin_write().unwrap();
        {
            txn.open_table(table_definition).unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn small_pages2() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint0 = tx.ephemeral_savepoint().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        tx.restore_savepoint(&savepoint0).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&660503, 489).unwrap().as_mut().fill(0xFF);
            assert!(t.remove(&291295).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        tx.restore_savepoint(&savepoint0).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint2 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint0);
        tx.restore_savepoint(&savepoint2).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.get(&2059).unwrap().is_none());
            assert!(t.remove(&145227).unwrap().is_none());
            assert!(t.remove(&145227).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint3 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint1);
        tx.restore_savepoint(&savepoint3).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let savepoint4 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint2);
        tx.restore_savepoint(&savepoint3).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.remove(&207936).unwrap().is_none());
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        let _savepoint5 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint3);
        // savepoint4 was invalidated by the restore_savepoint(savepoint3) call
        // above, but that transaction was aborted, so the invalidation is
        // reversed and savepoint4 is valid again. Restoring it here invalidates
        // savepoint5 (which is newer), so the next transaction restores
        // savepoint4 again rather than savepoint5.
        tx.restore_savepoint(&savepoint4).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        tx.restore_savepoint(&savepoint4).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();
    }

    #[test]
    fn small_pages3() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let mut tx = db.begin_write().unwrap();
        let _savepoint0 = tx.ephemeral_savepoint().unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 306];
            t.insert(&539717, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        tx.restore_savepoint(&savepoint1).unwrap();
        tx.set_durability(Durability::None).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 2008];
            t.insert(&784384, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();
    }

    #[test]
    fn small_pages4() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.get(&131072).unwrap().is_none());
            let value = vec![0xFF; 1130];
            t.insert(&42394, value.as_slice()).unwrap();
            t.insert_reserve(&744037, 3645).unwrap().as_mut().fill(0xFF);
            assert!(t.get(&0).unwrap().is_none());
        }
        tx.abort().unwrap();

        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&118749, 734).unwrap().as_mut().fill(0xFF);
        }
        tx.abort().unwrap();
    }

    #[test]
    fn dynamic_shrink() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
        let big_value = vec![0u8; 1024];

        let db = Database::builder()
            .set_region_size(1024 * 1024)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..2048 {
                table.insert(&i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let file_size = tmpfile.as_file().metadata().unwrap().len();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..2048 {
                table.remove(&i).unwrap();
            }
        }
        txn.commit().unwrap();

        // Perform a couple more commits to be sure the database has a chance to compact
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            table.insert(0, [].as_slice()).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            table.remove(0).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        txn.commit().unwrap();

        let final_file_size = tmpfile.as_file().metadata().unwrap().len();
        assert!(final_file_size < file_size);
    }

    #[test]
    fn create_new_db_in_empty_file() {
        let tmpfile = crate::create_tempfile();

        let _db = Database::builder()
            .create_file(tmpfile.into_file())
            .unwrap();
    }

    #[test]
    fn open_missing_file() {
        let tmpfile = crate::create_tempfile();

        let err = Database::builder()
            .open(tmpfile.path().with_extension("missing"))
            .unwrap_err();

        match err {
            DatabaseError::Storage(StorageError::Io(err)) if err.kind() == ErrorKind::NotFound => {}
            err => panic!("Unexpected error for empty file: {err}"),
        }
    }

    #[test]
    fn open_empty_file() {
        let tmpfile = crate::create_tempfile();

        let err = Database::builder().open(tmpfile.path()).unwrap_err();

        match err {
            DatabaseError::Storage(StorageError::Io(err))
                if err.kind() == ErrorKind::InvalidData => {}
            err => panic!("Unexpected error for empty file: {err}"),
        }
    }
}

/// Probed directly, since the public API reports every lock conflict the same way and so
/// cannot say which byte caused one.
#[cfg(all(
    test,
    feature = "experimental-multiprocess",
    any(target_os = "linux", target_vendor = "apple", windows)
))]
mod writer_byte_test {
    use super::{ConcurrencyMode, Database, WRITER_BYTE, byte_range};
    use crate::TableDefinition;
    use crate::tree_store::file_backend::range_lock::RangeLock;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn create(path: &Path, mode: ConcurrencyMode) -> Database {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        builder.create(path).unwrap()
    }

    /// A separate description, so its locks conflict with the database's exactly as another
    /// process's would
    fn probe(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    fn commit_one(db: &Database) {
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 0).unwrap();
        }
        write.commit().unwrap();
    }

    /// Held for the transaction and no longer, which is what lets the next writer in
    #[test]
    fn a_multi_writer_transaction_holds_the_byte_and_gives_it_back() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());

        let write = db.begin_write().unwrap();
        assert!(
            !probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "the byte was free while a write transaction was open"
        );
        write.commit().unwrap();

        assert!(
            probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "the byte was still held after the transaction ended"
        );
        probe.unlock_range(byte_range(WRITER_BYTE)).unwrap();
    }

    /// This mode settles who writes at open instead, so the byte is held from then until the
    /// database closes. A transaction taking it again would convert that hold and drop what
    /// remained on release, so it takes nothing.
    #[test]
    fn a_single_writer_open_holds_the_byte_for_its_lifetime() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::SingleWriterProcess);
        let probe = probe(tmpfile.path());

        assert!(
            !probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "the open did not take the writer byte"
        );
        commit_one(&db);
        assert!(
            !probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "a write transaction punctured the open's hold on the writer byte"
        );

        drop(db);
        assert!(
            probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "the byte was still held after the database closed"
        );
        probe.unlock_range(byte_range(WRITER_BYTE)).unwrap();
    }
}

/// The "active transaction range" locks a read transaction publishes, probed directly: nothing
/// consumes them yet, and a byte-range lock is invisible through the public API in any case
#[cfg(all(
    test,
    feature = "experimental-multiprocess",
    any(target_os = "linux", target_vendor = "apple", windows)
))]
mod active_transaction_test {
    use super::{ConcurrencyMode, Database, ReadableDatabase, TXN_BASE, byte_range};
    use crate::tree_store::file_backend::range_lock::RangeLock;
    use crate::{Durability, TableDefinition};
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");
    // The ids a freshly created database can have committed
    const SEARCHED: std::ops::Range<u64> = 0..8;

    fn create(path: &Path, mode: ConcurrencyMode) -> Database {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        let db = builder.create(path).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 0).unwrap();
        }
        write.commit().unwrap();
        db
    }

    /// A separate description, so its locks conflict with the database's exactly as another
    /// process's would
    fn probe(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    /// Probed exclusively, since the byte is held shared and another process may hold the same
    /// one. Reports the whole-file lock a single-process open holds as well, which is what
    /// `a_single_process_read_does_not_puncture_the_whole_file_lock` relies on
    fn is_held(probe: &File, id: u64) -> bool {
        let free = probe.try_lock_range(byte_range(TXN_BASE + id)).unwrap();
        if free {
            probe.unlock_range(byte_range(TXN_BASE + id)).unwrap();
        }
        !free
    }

    fn held_ids(probe: &File) -> Vec<u64> {
        SEARCHED.filter(|id| is_held(probe, *id)).collect()
    }

    #[test]
    fn a_read_transaction_locks_the_id_it_reads_until_it_ends() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());
        assert!(held_ids(&probe).is_empty());

        let read = db.begin_read().unwrap();
        assert_eq!(
            held_ids(&probe).len(),
            1,
            "a read transaction locked {:?}",
            held_ids(&probe)
        );

        drop(read);
        assert!(
            held_ids(&probe).is_empty(),
            "a lock outlived the read transaction that took it"
        );
    }

    /// Two readers of one snapshot take the byte once, since it does not nest through a single
    /// file description: it is released only as the last of them ends
    #[test]
    fn concurrent_readers_of_one_snapshot_share_the_lock() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());

        let first = db.begin_read().unwrap();
        let second = db.begin_read().unwrap();
        let held = held_ids(&probe);
        assert_eq!(held.len(), 1, "expected one active id: {held:?}");

        drop(first);
        assert!(is_held(&probe, held[0]), "released early");
        drop(second);
        assert!(!is_held(&probe, held[0]), "never released");
    }

    /// A read-only handle is a participant like any other: its snapshot is exactly what a
    /// writer in another process must not reclaim
    #[test]
    fn a_read_only_handle_locks_what_it_reads() {
        let tmpfile = crate::create_tempfile();
        // Dropped, so the read-only open is the only handle: it takes SHARED_READER_BYTE, and
        // the writer that created the file would otherwise hold the whole storage
        drop(create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess));
        let probe = probe(tmpfile.path());

        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        let db = builder.open_read_only(tmpfile.path()).unwrap();
        let read = db.begin_read().unwrap();
        assert_eq!(
            held_ids(&probe).len(),
            1,
            "a read-only handle locked {:?}",
            held_ids(&probe)
        );

        drop(read);
        assert!(held_ids(&probe).is_empty(), "the lock outlived the reader");
    }

    /// A savepoint holds a read transaction live, so its snapshot stays active for as long as
    /// the savepoint does
    #[test]
    fn an_ephemeral_savepoint_locks_the_snapshot_it_references() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());
        assert!(held_ids(&probe).is_empty());

        let write = db.begin_write().unwrap();
        let savepoint = write.ephemeral_savepoint().unwrap();
        write.commit().unwrap();
        assert_eq!(
            held_ids(&probe).len(),
            1,
            "a savepoint locked {:?}",
            held_ids(&probe)
        );

        drop(savepoint);
        assert!(
            held_ids(&probe).is_empty(),
            "the lock outlived the savepoint"
        );
    }

    /// A persistent savepoint outlives its handle -- and the process -- so its lock goes to the
    /// database with the reference rather than being released when the handle drops. Nothing
    /// releases it yet: deletion and re-locking at open both belong with the scan
    #[test]
    fn a_persistent_savepoint_keeps_its_lock_after_its_handle_drops() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());

        let write = db.begin_write().unwrap();
        write.persistent_savepoint().unwrap();
        write.commit().unwrap();

        assert_eq!(
            held_ids(&probe).len(),
            1,
            "a persistent savepoint left {:?} locked",
            held_ids(&probe)
        );
    }

    /// A persistent savepoint survives the process that made it, so reopening has to take its
    /// byte again: the snapshot its record names is still one a peer must not reclaim
    #[test]
    fn reopening_locks_a_persistent_savepoints_snapshot() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let write = db.begin_write().unwrap();
        write.persistent_savepoint().unwrap();
        write.commit().unwrap();
        drop(db);

        let probe = probe(tmpfile.path());
        assert!(
            held_ids(&probe).is_empty(),
            "the closed database left a lock"
        );

        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        let db = builder.open(tmpfile.path()).unwrap();
        assert_eq!(
            held_ids(&probe).len(),
            1,
            "reopening locked {:?}",
            held_ids(&probe)
        );
        drop(db);
    }

    /// A non-durable commit is invisible to a peer, but the durable ancestor it builds on is not,
    /// and its pages must survive until the commit is flushed
    #[test]
    fn a_non_durable_commit_locks_its_durable_ancestor() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());
        assert!(held_ids(&probe).is_empty());

        let mut write = db.begin_write().unwrap();
        write.set_durability(Durability::None).unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(1, 1).unwrap();
        }
        write.commit().unwrap();
        let ancestor = held_ids(&probe);
        assert_eq!(
            ancestor.len(),
            1,
            "a non-durable commit locked {ancestor:?}"
        );

        // The durable commit that flushes it releases the ancestor. Its own epilogue takes a
        // reference on this commit in turn, so what is locked afterwards is a different id
        let write = db.begin_write().unwrap();
        write.commit().unwrap();
        assert!(
            !held_ids(&probe).contains(&ancestor[0]),
            "the ancestor stayed locked after the commit that flushed it"
        );
    }

    /// A single-process open holds the whole file, which covers these bytes. Locking one and
    /// releasing it would punch a hole in that lock, so this mode locks nothing
    #[test]
    fn a_single_process_read_does_not_puncture_the_whole_file_lock() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::SingleProcess);
        let probe = probe(tmpfile.path());

        drop(db.begin_read().unwrap());

        assert_eq!(
            held_ids(&probe),
            SEARCHED.collect::<Vec<_>>(),
            "a read transaction punctured the whole-file lock"
        );
    }
}
