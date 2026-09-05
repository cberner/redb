use crate::io;
use crate::transaction_tracker::{TransactionId, TransactionTracker};
#[cfg(feature = "experimental-multiprocess")]
use crate::transactions::AllocatorStateLatch;
#[cfg(feature = "experimental-multiprocess")]
use crate::tree_store::HeaderGuard;
use crate::tree_store::LocklessBackend;
#[cfg(not(redb_no_std))]
use crate::tree_store::ReadOnlyBackend;
#[cfg(feature = "experimental-multiprocess")]
use crate::tree_store::WriterLock;
use crate::tree_store::{
    AllocationPolicy, BtreeHeader, InternalTableDefinition, PAGE_SIZE, PageHint, PageNumber,
    PageResolver, ShrinkPolicy, TableTree, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{
    CompactionError, DatabaseError, ReadOnlyTable, ReadableTable, SavepointError, StorageError,
    TableError,
};
use crate::{ReadTransaction, Result, WriteTransaction};
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
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
/// Held shared by a writing process from the moment its open is complete -- past recovery and
/// the allocator load -- until it closes: the file is consistent, and the recovery flag, set from
/// here on, means only that a writer is live. `SHARED_WRITER_BYTE` is taken before recovery, so
/// it cannot say that; this byte can.
#[cfg(feature = "experimental-multiprocess")]
pub(crate) const CONSISTENT_BYTE: u64 = LOCK_BASE + 4;
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
        // The writer lock, held as long as the transaction is. `Option` only so that `Drop`
        // can move it out
        #[cfg(feature = "experimental-multiprocess")]
        writer_lock: Option<Arc<WriterLock>>,
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

    /// The caller must already hold the write slot and the lock the transaction writes under.
    pub(crate) fn new_write(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
        #[cfg(feature = "experimental-multiprocess")] writer_lock: Arc<WriterLock>,
    ) -> Self {
        Self::Write {
            tracker,
            transaction_id,
            #[cfg(feature = "experimental-multiprocess")]
            writer_lock: Some(writer_lock),
        }
    }

    pub(crate) fn untracked() -> Self {
        Self::Untracked
    }

    // Renumbers the write transaction this guard holds, which the tracker has moved past a
    // commit another process made
    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn follow(&mut self, id: TransactionId) {
        let Self::Write { transaction_id, .. } = self else {
            unreachable!("only a write transaction is renumbered")
        };
        *transaction_id = id;
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
                writer_lock,
            } => {
                let deferred = tracker.end_write_transaction(
                    *transaction_id,
                    #[cfg(feature = "experimental-multiprocess")]
                    writer_lock.take(),
                );
                // The Database was dropped while this transaction was live, deferring
                // the database close to the end of this transaction
                if let Some(mem) = deferred {
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
#[cfg_attr(
    feature = "experimental-multiprocess",
    doc = "",
    doc = "The multi-process concurrency modes are the exception: a reader shares the file with the writer and follows its commits. See [`Builder::set_concurrency_mode`](crate::Builder::set_concurrency_mode)."
)]
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
    // Serializes beginning a read, so that the id a transaction locks and the root it then
    // reads come from the same state
    #[cfg(feature = "experimental-multiprocess")]
    begin_read: crate::sync::Mutex<()>,
}

#[cfg(not(redb_no_std))]
impl Sealed for ReadOnlyDatabase {}

#[cfg(not(redb_no_std))]
impl ReadableDatabase for ReadOnlyDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        #[cfg(feature = "experimental-multiprocess")]
        let _begin = self.begin_read.lock().unwrap();
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
        // A reader beside a multi-process writer never allocates, so it loads no allocator state
        #[cfg(feature = "experimental-multiprocess")]
        let multiprocess_writer = concurrency_mode.is_multi_process_writable();
        #[cfg(not(feature = "experimental-multiprocess"))]
        let multiprocess_writer = false;
        if !multiprocess_writer {
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
        }

        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();
        let db = Self {
            mem,
            transaction_tracker: Arc::new(TransactionTracker::new(next_transaction_id)),
            #[cfg(feature = "experimental-multiprocess")]
            begin_read: crate::sync::Mutex::new(()),
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
    /// ephemeral [`Savepoint`](crate::Savepoint), is still alive on this handle when this method
    /// is called.
    ///
    /// Transactions committed with [`Durability::None`](crate::Durability::None) that have not yet
    /// been made durable are made durable if the check passes, or rolled back if the database must
    /// be repaired.
    ///
    /// Where the database is shared with other processes, the check holds the writer lock: it
    /// waits for a write transaction in another process to end, and no other process's write
    /// transaction commits until it returns. A read transaction in another process neither
    /// blocks it nor is disturbed by it: what such a reader can reach, the rebuild keeps
    /// allocated.
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
        // Held until the check is over, so that the file it reloads is the file it repairs. Taken
        // without the write slot: no transaction is live, and none can begin under `&mut self`
        #[cfg(feature = "experimental-multiprocess")]
        let writer_lock = self.mem.lock_writer()?;
        let result = self.check_integrity_inner(
            #[cfg(feature = "experimental-multiprocess")]
            &writer_lock,
        );
        if result.is_err() {
            self.mem.invalidate_allocator_state();
        }
        result
    }

    fn check_integrity_inner(
        &mut self,
        #[cfg(feature = "experimental-multiprocess")] writer_lock: &Arc<WriterLock>,
    ) -> Result<bool, DatabaseError> {
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

        // No pending commit, or fall-through: verify and repair the durable state. The allocator
        // compared against the rebuild below is this handle's where the reload finds the commit
        // it already had: with the pending case handled above, the live and durable states are
        // then identical, so that allocator described the file. A commit another process made
        // since is one it lagged, and the comparison is then against the snapshot that commit
        // saved, which the next open would trust, where it saved one
        let allocator_hash = self.mem.allocator_hash();
        let mem = Arc::get_mut(&mut self.mem).unwrap();
        let (mut was_clean, adopted_peer_commits) = mem.clear_cache_and_reload()?;
        // Ids issued from here on follow the header this handle now has, which may carry another
        // process's commits
        self.transaction_tracker
            .reserve_repair_transaction_id(self.mem.get_last_committed_transaction_id()?);
        let allocator_hash = if adopted_peer_commits {
            match Self::get_allocator_state_table(&self.mem)? {
                Some(tree) => {
                    self.mem.load_allocator_state(&tree)?;
                    Some(self.mem.allocator_hash())
                }
                None => None,
            }
        } else {
            Some(allocator_hash)
        };

        let old_roots = [self.mem.get_data_root(), self.mem.get_system_root()];

        let new_roots = Self::do_repair(&mut self.mem, &|_| {}).map_err(|err| match err {
            DatabaseError::Storage(storage_err) => storage_err,
            _ => unreachable!(),
        })?;

        if old_roots != new_roots
            || allocator_hash.is_some_and(|hash| hash != self.mem.allocator_hash())
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
                #[cfg(feature = "experimental-multiprocess")]
                None,
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

        // The tracker holds the persistent savepoints the tables adopted above hold, as the
        // open's does: another process may have created, replaced or deleted any of them
        if adopted_peer_commits {
            self.restore_persistent_savepoints(
                #[cfg(feature = "experimental-multiprocess")]
                Some(writer_lock),
            )?;
        }
        // The repair's commit records no allocator state, and in multi-writer mode a write
        // transaction's does, for the next writer in any process to load rather than rebuild:
        // the repair ends with one, as compaction does. After the savepoints are held, since a
        // commit frees what nothing pins
        #[cfg(feature = "experimental-multiprocess")]
        if !was_clean && self.mem.concurrency_mode() == ConcurrencyMode::MultiWriterProcess {
            let txn = self
                .begin_write_with(Some(writer_lock), None)
                .map_err(|e| e.into_storage_error())?;
            txn.commit()
                .map_err(|e| DatabaseError::Storage(e.into_storage_error()))?;
        }

        Ok(was_clean)
    }

    // Restores the tracker state for the file's persistent savepoints, in a transaction lent
    // `writer_lock` where the caller holds it
    fn restore_persistent_savepoints(
        &self,
        #[cfg(feature = "experimental-multiprocess")] writer_lock: Option<&Arc<WriterLock>>,
    ) -> Result<(), DatabaseError> {
        let txn = self
            .begin_write_with(
                #[cfg(feature = "experimental-multiprocess")]
                writer_lock,
                #[cfg(feature = "experimental-multiprocess")]
                None,
            )
            .map_err(|e| e.into_storage_error())?;
        register_persistent_savepoints(
            &self.transaction_tracker,
            &self.mem,
            &txn,
            #[cfg(feature = "experimental-multiprocess")]
            None,
        )?;
        txn.abort()?;

        Ok(())
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
                match Self::rebuild_allocator_state(&self.mem, &|_| {}) {
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
        // Where the file is shared, held until the compaction is over and lent to the
        // transactions it runs: no other process begins a transaction under them, and one that
        // already has is a transaction in progress. Taken once a write transaction begun before
        // this call has ended: its commit would wait on the holds, and in multi-writer mode its
        // end would release the writer byte from under a hold taken meanwhile
        #[cfg(feature = "experimental-multiprocess")]
        let (writer_lock, header_lock) = if self.mem.concurrency_mode().is_multi_process_writable()
        {
            self.begin_write()
                .map_err(|e| e.into_storage_error())?
                .abort()?;
            (
                Some(self.mem.lock_writer()?),
                Some(self.mem.lock_header_exclusive()?),
            )
        } else {
            (None, None)
        };
        // Use 2-phase commit to avoid any possible security issues. Plus this compaction is going to be so slow that it doesn't matter.
        // Once https://github.com/cberner/redb/issues/829 is fixed, we should upgrade this to use quick-repair -- that way the user
        // can cancel the compaction without requiring a full repair afterwards
        let txn = self
            .begin_write_with(
                #[cfg(feature = "experimental-multiprocess")]
                writer_lock.as_ref(),
                #[cfg(feature = "experimental-multiprocess")]
                header_lock.as_ref(),
            )
            .map_err(|e| e.into_storage_error())?;
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
        // No local read is left to bound the scan, so a pin it finds is a peer's
        #[cfg(feature = "experimental-multiprocess")]
        if let Some(header_lock) = &header_lock
            && self
                .mem
                .oldest_active_transaction(None, header_lock)?
                .is_some()
        {
            return Err(CompactionError::TransactionInProgress);
        }
        txn.abort()?;
        // Commit to free up any pending free pages
        self.drain_pending_free_pages(
            ShrinkPolicy::Maximum,
            #[cfg(feature = "experimental-multiprocess")]
            writer_lock.as_ref(),
            #[cfg(feature = "experimental-multiprocess")]
            header_lock.as_ref(),
        )?;

        let mut compacted = false;
        // Iteratively compact until no progress is made
        loop {
            let mut progress = false;

            let mut txn = self
                .begin_write_with(
                    #[cfg(feature = "experimental-multiprocess")]
                    writer_lock.as_ref(),
                    #[cfg(feature = "experimental-multiprocess")]
                    header_lock.as_ref(),
                )
                .map_err(|e| e.into_storage_error())?;
            txn.skip_allocator_state_record();
            if txn.compact_pages()? {
                progress = true;
                txn.commit_with(
                    #[cfg(feature = "experimental-multiprocess")]
                    header_lock.as_ref(),
                )
                .map_err(|e| e.into_storage_error())?;
            } else {
                txn.abort()?;
            }

            // Drain pages freed by compact_pages(), including system pages queued by any
            // post-commit cleanup root updates.
            self.drain_pending_free_pages(
                ShrinkPolicy::Maximum,
                #[cfg(feature = "experimental-multiprocess")]
                writer_lock.as_ref(),
                #[cfg(feature = "experimental-multiprocess")]
                header_lock.as_ref(),
            )?;

            if !progress {
                break;
            }

            compacted = true;
        }

        // In multi-writer mode the file's latest commit records the allocator state, for the
        // next writer, in any process, to load rather than rebuild: the commit the close makes.
        // Not in the other modes, where the close records and a second record would leave the
        // file one record larger, the pages of the one it replaces being freed only by the next
        // commit
        #[cfg(feature = "experimental-multiprocess")]
        if self.mem.concurrency_mode() == ConcurrencyMode::MultiWriterProcess {
            ensure_allocator_state_table_and_trim(
                &self.transaction_tracker,
                &self.mem,
                writer_lock.as_ref(),
                header_lock.as_ref(),
            )?;
        }

        Ok(compacted)
    }

    fn drain_pending_free_pages(
        &self,
        shrink_policy: ShrinkPolicy,
        #[cfg(feature = "experimental-multiprocess")] writer_lock: Option<&Arc<WriterLock>>,
        #[cfg(feature = "experimental-multiprocess")] header_lock: Option<&HeaderGuard<'_>>,
    ) -> Result {
        // Preserve compact()'s empty durable commit, which also publishes pending
        // non-durable roots before checking for pending frees.
        let mut force_commit = true;
        loop {
            let mut txn = self
                .begin_write_with(
                    #[cfg(feature = "experimental-multiprocess")]
                    writer_lock,
                    #[cfg(feature = "experimental-multiprocess")]
                    header_lock,
                )
                .map_err(|e| e.into_storage_error())?;
            if !force_commit && !txn.pending_free_pages()? {
                txn.abort()?;
                return Ok(());
            }
            force_commit = false;
            txn.skip_allocator_state_record();
            txn.set_two_phase_commit(true);
            txn.set_shrink_policy(shrink_policy);
            txn.commit_with(
                #[cfg(feature = "experimental-multiprocess")]
                header_lock,
            )
            .map_err(|e| e.into_storage_error())?;
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
    fn mark_allocated_page_for_debug(mem: &Arc<TransactionalMemory>) -> Result {
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
    //
    // Callers must ensure nothing else uses the allocator meanwhile: either no transaction is
    // live, or the caller holds the write slot (read transactions never allocate or free).
    fn rebuild_allocator_state(
        mem: &Arc<TransactionalMemory>,
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

    /// Loads the allocator state the file's latest commit recorded, or rebuilds it from the
    /// trees where that commit recorded none: a repair's, or one made while the state needed
    /// repair
    #[cfg(feature = "experimental-multiprocess")]
    fn load_or_rebuild_allocator_state(mem: &Arc<TransactionalMemory>) -> Result {
        if let Some(tree) = Self::get_allocator_state_table(mem)? {
            mem.load_allocator_state(&tree)?;
            #[cfg(debug_assertions)]
            Self::mark_allocated_page_for_debug(mem)?;
        } else {
            Self::rebuild_allocator_state(mem, &|_| {}).map_err(|err| match err {
                DatabaseError::Storage(err) => err,
                _ => unreachable!(),
            })?;
        }
        // Loaded or rebuilt, the allocator state is the file's, without the leak a skipped abort
        // had latched in the one it replaces
        mem.clear_needs_repair();

        Ok(())
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
            Self::mark_allocated_page_for_debug(&mem)?;
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
                #[cfg(feature = "experimental-multiprocess")]
                None,
            )?;
        }

        mem.begin_writable()?;
        // Past recovery, so a reader finding this byte held knows the flag means a live writer
        #[cfg(feature = "experimental-multiprocess")]
        mem.mark_consistent()?;
        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();

        let db = Database {
            mem,
            transaction_tracker: Arc::new(TransactionTracker::new(next_transaction_id)),
        };

        // Restore the tracker state for any persistent savepoints
        db.restore_persistent_savepoints(
            #[cfg(feature = "experimental-multiprocess")]
            None,
        )?;

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
        self.begin_write_with(
            #[cfg(feature = "experimental-multiprocess")]
            None,
            #[cfg(feature = "experimental-multiprocess")]
            None,
        )
    }

    fn begin_write_with(
        &self,
        #[cfg(feature = "experimental-multiprocess")] writer_lock: Option<&Arc<WriterLock>>,
        #[cfg(feature = "experimental-multiprocess")] header_lock: Option<&HeaderGuard<'_>>,
    ) -> Result<WriteTransaction, TransactionError> {
        begin_write_with_allocation_policy(
            &self.transaction_tracker,
            &self.mem,
            #[cfg(feature = "experimental-multiprocess")]
            writer_lock,
            #[cfg(feature = "experimental-multiprocess")]
            header_lock,
            AllocationPolicy::Default,
        )
    }
}

// Holds the file's persistent savepoints, which another process may have created, replaced or
// deleted since the tracker last saw the file, and continues savepoint ids past the file's.
// Under the header lock, `header_lock` where the caller holds it
fn register_persistent_savepoints(
    transaction_tracker: &TransactionTracker,
    mem: &TransactionalMemory,
    txn: &WriteTransaction,
    #[cfg(feature = "experimental-multiprocess")] header_lock: Option<&HeaderGuard<'_>>,
) -> Result {
    if let Some(next_id) = txn.next_persistent_savepoint_id()? {
        transaction_tracker.restore_savepoint_counter_state(next_id);
    }
    let mut held = BTreeMap::new();
    for id in txn.list_persistent_savepoints()? {
        let savepoint = match txn.get_persistent_savepoint(id) {
            Ok(savepoint) => savepoint,
            Err(err) => match err {
                SavepointError::InvalidSavepoint
                | SavepointError::ImmediateDurabilityRequired
                | SavepointError::EphemeralSavepointUnsupported => unreachable!(),
                SavepointError::Storage(storage) => {
                    return Err(storage);
                }
            },
        };
        held.insert(savepoint.get_id(), savepoint.get_transaction_id());
    }
    #[cfg(feature = "experimental-multiprocess")]
    let hold = mem.header_hold(header_lock)?;
    transaction_tracker.hold_persistent_savepoints(
        mem,
        &held,
        #[cfg(feature = "experimental-multiprocess")]
        &hold,
    )
}

/// Adopts the commit another process made since this handle last loaded the file, if there is
/// one, and numbers the live write transaction `guard` holds past it. In multi-writer mode,
/// where there can be one; under the writer byte, so nothing commits meanwhile, and the header
/// lock, `header_lock` where the caller holds it
#[cfg(feature = "experimental-multiprocess")]
fn adopt_peer_commit(
    transaction_tracker: &TransactionTracker,
    mem: &Arc<TransactionalMemory>,
    guard: &mut TransactionGuard,
    header_lock: Option<&HeaderGuard<'_>>,
) -> Result<bool> {
    if mem.concurrency_mode() != ConcurrencyMode::MultiWriterProcess {
        return Ok(false);
    }
    let adopted = {
        let hold = mem.header_hold(header_lock)?;
        mem.reload_for_write(&hold)?
    };
    if !adopted {
        return Ok(false);
    }
    // Rebuilt part way, on an error or a panic the caller catches, the allocator state describes
    // neither the file nor anything else, and holding one must continue to mean it describes
    // the file
    let latch = AllocatorStateLatch::arm(mem.clone());
    Database::load_or_rebuild_allocator_state(mem)?;
    latch.disarm();
    // The recovery flag is this handle's rather than the commit's: a peer's close clears it in
    // the file while this handle is still writing, and the load above clears it in memory, so
    // the next commit writes it back, as it would have without the adoption, for a crash of
    // this handle to be recovered from
    mem.mark_recovery_required();
    let last_committed = mem.get_last_committed_transaction_id()?;
    guard.follow(transaction_tracker.follow_committed_transaction(guard.id(), last_committed));

    Ok(true)
}

// Takes the write slot and the writer lock, and builds the transaction on them. A caller that
// already holds the writer lock lends it as `writer_lock`: locking the byte again from this file
// description would return the same lock, and this transaction's end would then release the
// caller's. A caller holding the header lock lends it as `header_lock` for the same reason: its
// in-process half is a mutex, which its holder cannot take twice. The allocation policy is
// fixed for the lifetime of the transaction; every page allocation this transaction makes goes
// through it.
fn begin_write_with_allocation_policy(
    transaction_tracker: &Arc<TransactionTracker>,
    mem: &Arc<TransactionalMemory>,
    #[cfg(feature = "experimental-multiprocess")] writer_lock: Option<&Arc<WriterLock>>,
    #[cfg(feature = "experimental-multiprocess")] header_lock: Option<&HeaderGuard<'_>>,
    allocation_policy: AllocationPolicy,
) -> Result<WriteTransaction, TransactionError> {
    // Fail early if there has been an I/O error -- nothing can be committed in that case
    mem.check_io_errors()?;
    let transaction_id = transaction_tracker.start_write_transaction();
    #[cfg(feature = "experimental-multiprocess")]
    let writer_lock = match writer_lock.map_or_else(|| mem.lock_writer(), |lent| Ok(lent.clone())) {
        Ok(lock) => lock,
        Err(err) => {
            // Nothing owns the slot yet, and this database is live, so no close is deferred
            let deferred = transaction_tracker.end_write_transaction(transaction_id, None);
            assert!(deferred.is_none());
            return Err(err.into());
        }
    };
    let guard = TransactionGuard::new_write(
        transaction_id,
        transaction_tracker.clone(),
        #[cfg(feature = "experimental-multiprocess")]
        writer_lock,
    );
    // In multi-writer mode, another process may have committed since this handle last loaded
    // the file: its commit is the one this transaction follows. Under the guard, so that a
    // failure or a panic in the adoption releases the slot
    #[cfg(feature = "experimental-multiprocess")]
    let mut guard = guard;
    #[cfg(feature = "experimental-multiprocess")]
    let adopted = adopt_peer_commit(transaction_tracker, mem, &mut guard, header_lock)?;
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
    let transaction = WriteTransaction::new(
        guard,
        transaction_tracker.clone(),
        mem.clone(),
        allocation_policy,
    )?;
    // The savepoints the adopted commit's tables hold pin their transactions from here, ahead
    // of anything this transaction frees
    #[cfg(feature = "experimental-multiprocess")]
    if adopted {
        // Held part way, on an error or a panic the caller catches, the rest would be freed from
        // under by the next transaction: the allocator state is discarded, as after a rebuild
        // that stops part way, so that none begins until a reopen holds them all
        let latch = AllocatorStateLatch::arm(mem.clone());
        if let Err(err) =
            register_persistent_savepoints(transaction_tracker, mem, &transaction, header_lock)
        {
            // The abort frees nothing, and an I/O failure in it stays latched
            let _ = transaction.abort();
            return Err(err.into());
        }
        latch.disarm();
    }

    Ok(transaction)
}

// Records the allocator state in a commit that also trims the file: the close's last commit,
// and compaction's. `writer_lock` and `header_lock` where the caller lends the ones it holds, as
// compaction does; nothing is waiting on the write slot in either case
fn ensure_allocator_state_table_and_trim(
    transaction_tracker: &Arc<TransactionTracker>,
    mem: &Arc<TransactionalMemory>,
    #[cfg(feature = "experimental-multiprocess")] writer_lock: Option<&Arc<WriterLock>>,
    #[cfg(feature = "experimental-multiprocess")] header_lock: Option<&HeaderGuard<'_>>,
) -> Result {
    // Make a new quick-repair commit to update the allocator state table
    #[cfg(feature = "logging")]
    debug!("Writing allocator state table");
    // If compact() left no free pages, the default allocator lands this
    // commit's writes at high page indices (see AllocationPolicy::Lowest)
    // and try_shrink can't reclaim the growth. See
    // https://github.com/cberner/redb/issues/1165
    let mut tx = begin_write_with_allocation_policy(
        transaction_tracker,
        mem,
        #[cfg(feature = "experimental-multiprocess")]
        writer_lock,
        #[cfg(feature = "experimental-multiprocess")]
        header_lock,
        AllocationPolicy::Lowest,
    )
    .map_err(|e| e.into_storage_error())?;
    tx.set_quick_repair(true);
    tx.disable_post_commit_free();
    tx.set_shrink_policy(ShrinkPolicy::Maximum);
    tx.commit_with(
        #[cfg(feature = "experimental-multiprocess")]
        header_lock,
    )
    .map_err(|e| e.into_storage_error())?;

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
    // of trusting the saved one. Nor after a latched I/O failure, decided ahead of the lock,
    // which waits on a peer holding the writer byte
    let writing = !crate::panicking() && !mem.needs_repair() && mem.check_io_errors().is_ok();
    // One hold across the allocator state's commit and the shutdown header: a commit another
    // process made between them would be overwritten by the header. Without the hold, neither
    // is written: the commit would take one of its own and release it, and the header would
    // then overwrite a commit made between them
    #[cfg(feature = "experimental-multiprocess")]
    let writer_lock = if writing {
        mem.lock_writer().ok()
    } else {
        None
    };
    #[cfg(feature = "experimental-multiprocess")]
    let writing = writing && writer_lock.is_some();
    let recorded = writing
        && ensure_allocator_state_table_and_trim(
            transaction_tracker,
            mem,
            #[cfg(feature = "experimental-multiprocess")]
            writer_lock.as_ref(),
            #[cfg(feature = "experimental-multiprocess")]
            None,
        )
        .is_ok();
    if writing && !recorded {
        #[cfg(feature = "logging")]
        warn!("Failed to write allocator state table. Repair may be required at restart.");
    }
    // The shutdown header is this handle's, which describes the file only once the commit above
    // has adopted it: without the commit, no header, and the next open recovers from the file
    // as the last commit left it
    #[cfg(feature = "experimental-multiprocess")]
    let writer_lock = if recorded { writer_lock } else { None };

    if mem
        .close(
            #[cfg(feature = "experimental-multiprocess")]
            writer_lock.as_ref(),
        )
        .is_err()
    {
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
    #[cfg_attr(
        feature = "experimental-multiprocess",
        doc = "",
        doc = "Only a single-process writer is refused: in the multi-process concurrency modes a reader opened in the same mode as the writer shares the file with it and picks up its commits. See [`Self::set_concurrency_mode`]."
    )]
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

    /// A transaction dropped while a panic unwinds leaks its pages in the allocator state, which
    /// latches a repair; adopting a peer's commit replaces that state with the file's, which has
    /// no such leak, so the latch is released and the next commit records again
    #[test]
    fn adopting_a_peers_commit_releases_the_repair_a_panic_latched() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        let peer = builder.open(tmpfile.path()).unwrap();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let write = db.begin_write().unwrap();
            write.open_table(TABLE).unwrap().insert(1, 1).unwrap();
            panic!("unwinding through the transaction");
        }));
        assert!(unwound.is_err());
        assert!(db.mem.needs_repair());

        commit_one(&peer);
        commit_one(&db);
        assert!(!db.mem.needs_repair());
        assert!(
            Database::get_allocator_state_table(&db.mem)
                .unwrap()
                .is_some()
        );
    }

    /// A multi-writer compaction ends with a commit recording the allocator state, for the next
    /// writer, in any process, to load rather than rebuild
    #[test]
    fn a_multi_writer_compaction_records_the_allocator_state() {
        let tmpfile = crate::create_tempfile();
        let mut db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        commit_one(&db);

        db.compact().unwrap();
        assert!(
            Database::get_allocator_state_table(&db.mem)
                .unwrap()
                .is_some()
        );
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
    /// remained on release, so it holds the open's instead.
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

    /// A transaction that outlives the database keeps the lock, and the deferred close writes
    /// the allocator state under it
    #[test]
    fn a_transaction_outliving_the_database_keeps_the_writer_lock() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::SingleWriterProcess);
        let probe = probe(tmpfile.path());

        let write = db.begin_write().unwrap();
        drop(db);
        assert!(
            !probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "dropping the database released the byte from under a live transaction"
        );

        write.commit().unwrap();
        assert!(
            probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap(),
            "the byte was still held after the deferred close"
        );
        probe.unlock_range(byte_range(WRITER_BYTE)).unwrap();
    }
}

/// The consistent byte, probed directly: a byte-range lock is invisible through the public API.
#[cfg(all(
    test,
    feature = "experimental-multiprocess",
    any(target_os = "linux", target_vendor = "apple", windows)
))]
mod consistent_byte_test {
    use super::{CONSISTENT_BYTE, ConcurrencyMode, Database, byte_range};
    use crate::backends::FileBackend;
    use crate::tree_store::file_backend::range_lock::RangeLock;
    use crate::{StorageBackend, TableDefinition};
    use std::fs::{File, OpenOptions};
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn builder(mode: ConcurrencyMode) -> crate::Builder {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        builder
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

    /// True when some writer asserts the database is consistent
    fn held(probe: &File) -> bool {
        let taken = probe.try_lock_range(byte_range(CONSISTENT_BYTE)).unwrap();
        if taken {
            probe.unlock_range(byte_range(CONSISTENT_BYTE)).unwrap();
        }
        !taken
    }

    /// Power loss: once armed, every write silently does nothing, so the close writes neither
    /// the allocator record nor the clean-shutdown header
    #[derive(Debug)]
    struct CrashBackend {
        inner: FileBackend,
        dead: Arc<AtomicBool>,
    }

    impl StorageBackend for CrashBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> Result<(), std::io::Error> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.write(offset, data)
        }
    }

    /// Leaves the file as a crashed writer would: a committed transaction, no allocator record
    /// and the recovery flag still set, so the next open has to repair it. Built through a
    /// caller-supplied backend, which takes no locks, so nothing of this handle outlives it.
    fn dirty(path: &Path) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let dead = Arc::new(AtomicBool::new(false));
        let db = Database::builder()
            .create_with_backend(CrashBackend {
                inner: FileBackend::new(file).unwrap(),
                dead: Arc::clone(&dead),
            })
            .unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 0).unwrap();
        }
        write.commit().unwrap();
        // Nothing this handle does from here on reaches the file, its close included
        dead.store(true, Ordering::SeqCst);
        drop(db);
    }

    #[test]
    fn a_shared_writable_open_asserts_consistency_until_it_closes() {
        for mode in [
            ConcurrencyMode::SingleWriterProcess,
            ConcurrencyMode::MultiWriterProcess,
        ] {
            let tmpfile = crate::create_tempfile();
            let db = builder(mode).create(tmpfile.path()).unwrap();
            let probe = probe(tmpfile.path());
            assert!(held(&probe), "{mode:?} open did not take the byte");

            drop(db);
            assert!(!held(&probe), "{mode:?} close did not release the byte");
        }
    }

    /// The byte's whole purpose. `SHARED_WRITER_BYTE` is taken before recovery runs, so during a
    /// repair it says a writer is here while the file is still the one the last writer left. This
    /// byte is taken after, so it says nothing until the file is consistent.
    #[test]
    fn a_repairing_open_asserts_consistency_only_once_the_repair_is_done() {
        let tmpfile = crate::create_tempfile();
        dirty(tmpfile.path());

        let probe = Arc::new(Mutex::new(probe(tmpfile.path())));
        let during: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));

        let db = {
            let probe = probe.clone();
            let during = during.clone();
            let mut builder = builder(ConcurrencyMode::MultiWriterProcess);
            builder.set_repair_callback(move |_| {
                let mut during = during.lock().unwrap();
                if during.is_none() {
                    *during = Some(held(&probe.lock().unwrap()));
                }
            });
            builder.open(tmpfile.path()).unwrap()
        };

        assert_eq!(
            during.lock().unwrap().take(),
            Some(false),
            "the byte was held while the open was still repairing"
        );
        assert!(held(&probe.lock().unwrap()), "the open never took the byte");
        drop(db);
    }
}

/// The "active transaction range" locks a read transaction publishes, probed directly: a
/// byte-range lock is invisible through the public API
#[cfg(all(
    test,
    feature = "experimental-multiprocess",
    any(target_os = "linux", target_vendor = "apple", windows)
))]
mod active_transaction_test {
    use super::{ConcurrencyMode, Database, ReadableDatabase, TXN_BASE, WRITER_BYTE, byte_range};
    use crate::tree_store::HEADER_LOCK;
    use crate::tree_store::file_backend::range_lock::RangeLock;
    use crate::{Durability, SavepointError, SetDurabilityError, TableDefinition};
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");
    // The ids a freshly created database can have committed, with room for a few more commits
    const SEARCHED: std::ops::Range<u64> = 0..16;

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
    /// the savepoint does. In the shared mode that supports an ephemeral one
    #[test]
    fn an_ephemeral_savepoint_locks_the_snapshot_it_references() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::SingleWriterProcess);
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

    /// A non-durable commit exists only in this process's memory, so a shared mode refuses it
    #[test]
    fn a_shared_mode_refuses_durability_none() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::SingleWriterProcess);

        let mut write = db.begin_write().unwrap();
        assert!(matches!(
            write.set_durability(Durability::None),
            Err(SetDurabilityError::NonDurableCommitUnsupported)
        ));
    }

    /// Every multi-writer commit records the allocator state, for the next writer, in any
    /// process, to load rather than rebuild: turning quick-repair off has no effect there
    #[test]
    fn a_multi_writer_commit_records_the_allocator_state_regardless() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);

        let mut write = db.begin_write().unwrap();
        write.set_quick_repair(false);
        write.open_table(TABLE).unwrap().insert(1, 1).unwrap();
        write.commit().unwrap();
        assert!(
            Database::get_allocator_state_table(&db.mem)
                .unwrap()
                .is_some()
        );
    }

    /// An ephemeral savepoint would be known to this process alone, and a persistent savepoint
    /// a peer creates could take its id, so multi-writer mode refuses it
    #[test]
    fn a_multi_writer_mode_refuses_an_ephemeral_savepoint() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);

        let write = db.begin_write().unwrap();
        assert!(matches!(
            write.ephemeral_savepoint(),
            Err(SavepointError::EphemeralSavepointUnsupported)
        ));
        write.persistent_savepoint().unwrap();
        write.commit().unwrap();
    }

    /// The snapshot an adopted commit saved is what the next open would trust, so the check
    /// compares it against the rebuild as it compares this handle's own
    #[test]
    fn an_integrity_check_validates_an_adopted_allocator_snapshot() {
        use crate::tree_store::{AllocationPolicy, PageAllocator, PageTracker};

        let tmpfile = crate::create_tempfile();
        let mut db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let peer = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .open(tmpfile.path())
            .unwrap();
        // A page the peer allocates and neither frees nor references: a leak, which the snapshot
        // its close saves records as allocated
        let allocator = PageAllocator::new(peer.mem.clone(), AllocationPolicy::Default);
        drop(allocator.allocate(64, &PageTracker::ignore()).unwrap());
        drop(allocator);
        drop(peer);

        assert!(!db.check_integrity().unwrap());
        // The repair ends with a commit that records the allocator state, for a peer to load
        assert!(
            Database::get_allocator_state_table(&db.mem)
                .unwrap()
                .is_some()
        );
        assert!(db.check_integrity().unwrap());
    }

    /// A read-only participant sees what another process committed, not the header it read at open
    #[test]
    fn a_read_only_participant_picks_up_a_peers_commit() {
        let tmpfile = crate::create_tempfile();
        let writer = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);

        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        let reader = builder.open_read_only(tmpfile.path()).unwrap();
        let probe = probe(tmpfile.path());

        let read = reader.begin_read().unwrap();
        let before = held_ids(&probe);
        assert_eq!(before.len(), 1, "expected one active id: {before:?}");
        let before = before[0];
        drop(read);

        let write = writer.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(1, 1).unwrap();
        }
        write.commit().unwrap();
        drop(writer);
        assert!(
            held_ids(&probe).is_empty(),
            "the closed writer left {:?} locked",
            held_ids(&probe)
        );

        let read = reader.begin_read().unwrap();
        let after = held_ids(&probe);
        assert_eq!(after.len(), 1, "expected one active id: {after:?}");
        assert!(
            after[0] > before,
            "the reader stayed at {before} after the writer committed"
        );
        drop(read);
    }

    /// A read-only participant takes the primary as recorded: choosing between the slots is a
    /// repairing writer's job, and a newer secondary it can see is a commit whose pages are not in
    /// the file yet, or one a repair has just rolled back
    #[test]
    fn a_read_only_participant_takes_the_primary_as_recorded() {
        use std::io::{Read, Write};

        let tmpfile = crate::create_tempfile();
        drop(create(tmpfile.path(), ConcurrencyMode::SingleProcess));

        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        let reader = builder.open_read_only(tmpfile.path()).unwrap();

        // A newer commit whose pages this file never received: made in a copy, then its slot
        // spliced into this file's secondary slot
        let copy = crate::create_tempfile();
        std::fs::copy(tmpfile.path(), copy.path()).unwrap();
        let db = Database::open(copy.path()).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 1).unwrap();
        }
        write.commit().unwrap();
        drop(db);

        let header = |path: &Path| -> Vec<u8> {
            let mut header = vec![0u8; 320];
            File::open(path).unwrap().read_exact(&mut header).unwrap();
            header
        };
        let slot = |index: usize| 64 + index * 128..64 + (index + 1) * 128;
        let id = |header: &[u8], index: usize| {
            u64::from_le_bytes(header[slot(index)][104..112].try_into().unwrap())
        };
        let newer = header(copy.path());
        let mut spliced = header(tmpfile.path());
        let primary = usize::from(spliced[9] & 1);
        spliced[slot(1 - primary)].copy_from_slice(&newer[slot(usize::from(newer[9] & 1))]);
        // The god byte a repair leaves on a 1-phase history: recovery clear, 2-phase clear
        spliced[9] &= !6;
        OpenOptions::new()
            .write(true)
            .open(tmpfile.path())
            .unwrap()
            .write_all(&spliced)
            .unwrap();
        assert!(id(&spliced, 1 - primary) > id(&spliced, primary));
        let probe = probe(tmpfile.path());

        // Reloading into that state, and opening in it
        let read = reader.begin_read().unwrap();
        assert_eq!(
            held_ids(&probe),
            vec![id(&spliced, primary)],
            "the reader adopted a slot whose pages the file never received"
        );
        drop(read);
        let opened = builder.open_read_only(tmpfile.path()).unwrap();
        let read = opened.begin_read().unwrap();
        assert_eq!(held_ids(&probe), vec![id(&spliced, primary)]);
        drop(read);
    }

    /// Sharing the file forces 2-phase, whatever the transaction asked for
    #[test]
    fn a_shared_commit_is_two_phase() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);

        let mut write = db.begin_write().unwrap();
        write.set_two_phase_commit(false);
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(1, 1).unwrap();
        }
        write.commit().unwrap();

        assert!(
            db.mem.used_two_phase_commit(),
            "a shared commit published without the flush between the header writes"
        );
    }

    /// With no writer holding the file, an unclean one is refused: nothing is coming to repair it
    #[test]
    fn a_shared_read_refuses_an_unclean_file_with_no_writer() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let tmpfile = crate::create_tempfile();
        drop(create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess));

        // The god byte's recovery-required bit, which a clean shutdown clears
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmpfile.path())
                .unwrap();
            let mut byte = [0u8; 1];
            file.seek(SeekFrom::Start(9)).unwrap();
            file.read_exact(&mut byte).unwrap();
            byte[0] |= 2;
            file.seek(SeekFrom::Start(9)).unwrap();
            file.write_all(&byte).unwrap();
        }

        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        assert!(matches!(
            builder.open_read_only(tmpfile.path()),
            Err(crate::DatabaseError::RepairAborted)
        ));
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

    /// The floor is the older of this process's oldest read and a peer's pin, and a peer's pin
    /// only matters below the former, which is where the scan looks
    #[test]
    fn the_oldest_active_transaction_is_the_lower_of_ours_and_a_peers() {
        for mode in [
            ConcurrencyMode::SingleWriterProcess,
            ConcurrencyMode::MultiWriterProcess,
        ] {
            let tmpfile = crate::create_tempfile();
            let db = create(tmpfile.path(), mode);
            let scan = |local| {
                let header_lock = db.mem.lock_header_exclusive().unwrap();
                db.mem
                    .oldest_active_transaction(local, &header_lock)
                    .unwrap()
            };
            let first = db.mem.get_last_committed_transaction_id().unwrap();
            assert_eq!(scan(None), None);

            // A peer pins the last committed transaction, as its read of the header would
            let probe = probe(tmpfile.path());
            probe
                .lock_shared_range(byte_range(TXN_BASE + first.raw_id()))
                .unwrap();
            assert_eq!(scan(None), Some(first), "{mode:?}");

            let write = db.begin_write().unwrap();
            {
                let mut table = write.open_table(TABLE).unwrap();
                table.insert(1, 1).unwrap();
            }
            write.commit().unwrap();
            let second = db.mem.get_last_committed_transaction_id().unwrap();
            assert!(first < second);
            assert_eq!(scan(Some(second)), Some(first), "{mode:?}");
            probe
                .unlock_range(byte_range(TXN_BASE + first.raw_id()))
                .unwrap();
            assert_eq!(scan(Some(second)), Some(second), "{mode:?}");
        }
    }

    /// A transaction lent the exclusive header hold adopts a peer's commit under it, the
    /// savepoints it carries included, rather than taking the header lock again
    #[test]
    fn a_lent_header_hold_covers_adopting_a_peers_commit() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        fn open(path: &Path) -> Database {
            let mut builder = Database::builder();
            builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
            builder.open(path).unwrap()
        }

        let tmpfile = crate::create_tempfile();
        create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let peer = open(tmpfile.path());
        // The handle opens before the peer commits, then adopts the commit under holds it took
        // itself, as compaction does: in a thread, so that taking the header lock again is a
        // failure rather than a hang
        let path = tmpfile.path().to_path_buf();
        let (opened, wait_for_open) = mpsc::channel();
        let (committed, wait_for_commit) = mpsc::channel();
        let (adopted, wait_for_adoption) = mpsc::channel();
        let adopting = thread::spawn(move || {
            let db = open(&path);
            opened.send(()).unwrap();
            wait_for_commit.recv().unwrap();
            let writer_lock = db.mem.lock_writer().unwrap();
            let header_lock = db.mem.lock_header_exclusive().unwrap();
            let txn = db
                .begin_write_with(Some(&writer_lock), Some(&header_lock))
                .unwrap();
            let savepoints: Vec<u64> = txn.list_persistent_savepoints().unwrap().collect();
            txn.abort().unwrap();
            drop(header_lock);
            drop(writer_lock);
            adopted.send(savepoints).unwrap();
        });
        wait_for_open.recv().unwrap();
        let txn = peer.begin_write().unwrap();
        let peers_savepoint = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        // Closed first: its close would otherwise wait on the byte a hung transaction holds
        drop(peer);
        committed.send(()).unwrap();

        let savepoints = wait_for_adoption
            .recv_timeout(Duration::from_secs(10))
            .expect("the transaction took the header lock its caller holds");
        assert_eq!(savepoints, vec![peers_savepoint]);
        adopting.join().unwrap();
    }

    /// A transaction lent the exclusive header hold begins and commits under it and leaves it
    /// held
    #[test]
    fn a_lent_header_hold_outlives_the_commit() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());

        let header_lock = db.mem.lock_header_exclusive().unwrap();
        let txn = db.begin_write_with(None, Some(&header_lock)).unwrap();
        txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
        txn.commit_with(Some(&header_lock)).unwrap();
        assert!(!probe.try_lock_shared_range(HEADER_LOCK).unwrap());

        drop(header_lock);
        assert!(probe.try_lock_shared_range(HEADER_LOCK).unwrap());
        probe.unlock_range(HEADER_LOCK).unwrap();
    }

    /// A transaction lent the writer byte leaves it held when it ends
    #[test]
    fn a_lent_writer_byte_outlives_the_transaction() {
        let tmpfile = crate::create_tempfile();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess);
        let probe = probe(tmpfile.path());

        let writer_lock = db.mem.lock_writer().unwrap();
        let txn = db.begin_write_with(Some(&writer_lock), None).unwrap();
        txn.abort().unwrap();
        assert!(!probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap());

        drop(writer_lock);
        assert!(probe.try_lock_range(byte_range(WRITER_BYTE)).unwrap());
        probe.unlock_range(byte_range(WRITER_BYTE)).unwrap();
    }
}
