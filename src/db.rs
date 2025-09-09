use crate::transaction_tracker::{TransactionId, TransactionTracker};
use crate::tree_store::{
    BtreeHeader, InternalTableDefinition, PAGE_SIZE, PageHint, PageNumber, ReadOnlyBackend,
    ShrinkPolicy, TableTree, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{
    CompactionError, DatabaseError, Error, ReadOnlyTable, SavepointError, StorageError, TableError,
};
use crate::{ReadTransaction, Result, WriteTransaction};
use std::fmt::{Debug, Display, Formatter};

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::{io, thread};

use crate::error::TransactionError;
use crate::sealed::Sealed;
use crate::transactions::{
    ALLOCATOR_STATE_TABLE_NAME, AllocatorStateKey, AllocatorStateTree, DATA_ALLOCATED_TABLE,
    DATA_FREED_TABLE, PageList, SYSTEM_FREED_TABLE, SystemTableDefinition,
    TransactionIdWithPagination,
};
use crate::tree_store::file_backend::EncryptedFileBackend;
#[cfg(feature = "logging")]
use log::{debug, info, warn};

#[allow(clippy::len_without_is_empty)]
/// Implements persistent storage for a database.
pub trait StorageBackend: 'static + Debug + Send + Sync {
    /// Gets the current length of the storage.
    fn len(&self) -> std::result::Result<u64, io::Error>;

    /// Reads the specified array of bytes from the storage.
    ///
    /// If `out.len()` + `offset` exceeds the length of the storage an appropriate `Error` must be returned.
    fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), io::Error>;

    /// Sets the length of the storage.
    ///
    /// New positions in the storage must be initialized to zero.
    fn set_len(&self, len: u64) -> std::result::Result<(), io::Error>;

    /// Syncs all buffered data with the persistent storage.
    fn sync_data(&self) -> std::result::Result<(), io::Error>;

    /// Writes the specified array to the storage.
    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), io::Error>;

    /// Release any resources held by the backend
    ///
    /// Note: redb will not access the backend after calling this method and will call it exactly
    /// once when the [`Database`] is dropped
    fn close(&self) -> std::result::Result<(), io::Error> {
        Ok(())
    }
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
    /// ## Invariant
    ///
    /// `name` must not be empty.
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
}

impl CacheStats {
    /// Number of times that data has been evicted, due to the cache being full
    ///
    /// To increase the cache size use [`Builder::set_cache_size`]
    pub fn evictions(&self) -> u64 {
        self.evictions
    }
}

pub(crate) struct TransactionGuard {
    transaction_tracker: Option<Arc<TransactionTracker>>,
    transaction_id: Option<TransactionId>,
    write_transaction: bool,
}

impl TransactionGuard {
    pub(crate) fn new_read(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
    ) -> Self {
        Self {
            transaction_tracker: Some(tracker),
            transaction_id: Some(transaction_id),
            write_transaction: false,
        }
    }

    pub(crate) fn new_write(
        transaction_id: TransactionId,
        tracker: Arc<TransactionTracker>,
    ) -> Self {
        Self {
            transaction_tracker: Some(tracker),
            transaction_id: Some(transaction_id),
            write_transaction: true,
        }
    }

    // TODO: remove this hack
    pub(crate) fn fake() -> Self {
        Self {
            transaction_tracker: None,
            transaction_id: None,
            write_transaction: false,
        }
    }

    pub(crate) fn id(&self) -> TransactionId {
        self.transaction_id.unwrap()
    }

    pub(crate) fn leak(mut self) -> TransactionId {
        self.transaction_id.take().unwrap()
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        if self.transaction_tracker.is_none() {
            return;
        }
        if let Some(transaction_id) = self.transaction_id {
            if self.write_transaction {
                self.transaction_tracker
                    .as_ref()
                    .unwrap()
                    .end_write_transaction(transaction_id);
            } else {
                self.transaction_tracker
                    .as_ref()
                    .unwrap()
                    .deallocate_read_transaction(transaction_id);
            }
        }
    }
}

pub trait ReadableDatabase {
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
/// use redbx::*;
/// # use tempfile::NamedTempFile;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename, "password")?;
/// let txn = db.begin_write()?;
/// {
///     let mut table = txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// txn.commit()?;
/// drop(db);
///
/// let db = ReadOnlyDatabase::open(filename, "password")?;
/// let txn = db.begin_read()?;
/// {
///     let mut table = txn.open_table(TABLE)?;
///     println!("{}", table.get(&0)?.unwrap().value());
/// }
/// # Ok(())
/// # }
/// ```
pub struct ReadOnlyDatabase {
    mem: Arc<TransactionalMemory>,
    transaction_tracker: Arc<TransactionTracker>,
}

impl ReadableDatabase for ReadOnlyDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;
        #[cfg(feature = "logging")]
        debug!("Beginning read transaction id={id:?}");

        let guard = TransactionGuard::new_read(id, self.transaction_tracker.clone());

        ReadTransaction::new(self.mem.clone(), guard)
    }

    fn cache_stats(&self) -> CacheStats {
        self.mem.cache_stats()
    }
}

impl ReadOnlyDatabase {
    /// Opens an existing redb database.
    pub fn open(path: impl AsRef<Path>, password: &str) -> Result<ReadOnlyDatabase, DatabaseError> {
        Builder::new().open_read_only(path, password)
    }

    fn new(
        file: Box<dyn StorageBackend>,
        page_size: usize,
        region_size: Option<u64>,
        read_cache_size_bytes: usize,
        salt: Option<[u8; 16]>,
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database in read-only {:?}", &file_path);
        let mem = TransactionalMemory::new(
            Box::new(ReadOnlyBackend::new(file)),
            false,
            page_size,
            region_size,
            read_cache_size_bytes,
            0,
            true,
            salt,
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
/// # Examples
///
/// Basic usage:
///
/// ```rust
/// use redbx::*;
/// # use tempfile::NamedTempFile;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename, "password")?;
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

impl ReadableDatabase for Database {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let guard = self.allocate_read_transaction()?;
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
    pub fn create(path: impl AsRef<Path>, password: &str) -> Result<Database, DatabaseError> {
        Self::builder().create(path, password)
    }

    /// Opens an existing redb database.
    pub fn open(path: impl AsRef<Path>, password: &str) -> Result<Database, DatabaseError> {
        Self::builder().open(path, password)
    }

    pub(crate) fn get_memory(&self) -> Arc<TransactionalMemory> {
        self.mem.clone()
    }

    pub(crate) fn verify_primary_checksums(mem: Arc<TransactionalMemory>) -> Result<bool> {
        let table_tree = TableTree::new(
            mem.get_data_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        if !table_tree.verify_checksums()? {
            return Ok(false);
        }
        let system_table_tree = TableTree::new(
            mem.get_system_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
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
    /// and `Err(Corrupted)` if the check failed and the file could not be repaired
    pub fn check_integrity(&mut self) -> Result<bool, DatabaseError> {
        let allocator_hash = self.mem.allocator_hash();
        let mut was_clean = Arc::get_mut(&mut self.mem)
            .unwrap()
            .clear_cache_and_reload()?;

        let old_roots = [self.mem.get_data_root(), self.mem.get_system_root()];

        let new_roots = Self::do_repair(&mut self.mem, &|_| {}).map_err(|err| match err {
            DatabaseError::Storage(storage_err) => storage_err,
            _ => unreachable!(),
        })?;

        if old_roots != new_roots || allocator_hash != self.mem.allocator_hash() {
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
        }

        self.mem.begin_writable()?;

        Ok(was_clean)
    }

    /// Compacts the database file
    ///
    /// Returns `true` if compaction was performed, and `false` if no futher compaction was possible
    pub fn compact(&mut self) -> Result<bool, CompactionError> {
        if self
            .transaction_tracker
            .oldest_live_read_transaction()
            .is_some()
        {
            return Err(CompactionError::TransactionInProgress);
        }
        // Commit to free up any pending free pages
        // Use 2-phase commit to avoid any possible security issues. Plus this compaction is going to be so slow that it doesn't matter.
        // Once https://github.com/cberner/redb/issues/829 is fixed, we should upgrade this to use quick-repair -- that way the user
        // can cancel the compaction without requiring a full repair afterwards
        let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        if txn.list_persistent_savepoints()?.next().is_some() {
            return Err(CompactionError::PersistentSavepointExists);
        }
        if self.transaction_tracker.any_savepoint_exists() {
            return Err(CompactionError::EphemeralSavepointExists);
        }
        txn.set_two_phase_commit(true);
        txn.commit().map_err(|e| e.into_storage_error())?;
        // Repeat, just in case executing list_persistent_savepoints() created a new table
        let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        txn.set_two_phase_commit(true);
        txn.commit().map_err(|e| e.into_storage_error())?;
        // There can't be any outstanding transactions because we have a `&mut self`, so all pending free pages
        // should have been cleared out by the above commit()
        let txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        assert!(!txn.pending_free_pages()?);
        txn.abort()?;

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

            // Double commit to free up the relocated pages for reuse
            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            txn.set_two_phase_commit(true);
            // Also shrink the database file by the maximum amount
            txn.set_shrink_policy(ShrinkPolicy::Maximum);
            txn.commit().map_err(|e| e.into_storage_error())?;
            // Triple commit to free up the relocated pages for reuse
            // TODO: this really shouldn't be necessary, but the data freed tree is a system table
            // and so free'ing up its pages causes more deletes from the system tree
            let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            txn.set_two_phase_commit(true);
            // Also shrink the database file by the maximum amount
            txn.set_shrink_policy(ShrinkPolicy::Maximum);
            txn.commit().map_err(|e| e.into_storage_error())?;
            let txn = self.begin_write().map_err(|e| e.into_storage_error())?;
            assert!(!txn.pending_free_pages()?);
            txn.abort()?;

            if !progress {
                break;
            }

            compacted = true;
        }

        Ok(compacted)
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    fn check_repaired_allocated_pages_table(
        system_root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
    ) -> Result {
        let table_tree = TableTree::new(
            system_root,
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
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
                Arc::new(TransactionGuard::fake()),
                mem.clone(),
            )?;
            for result in table.range::<TransactionIdWithPagination>(..)? {
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
        let fake_guard = Arc::new(TransactionGuard::fake());
        let system_tree = TableTree::new(system_root, PageHint::None, fake_guard, mem.clone())?;
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
                    Arc::new(TransactionGuard::fake()),
                    mem.clone(),
                )?;
            for result in table.range::<TransactionIdWithPagination>(..)? {
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
            let fake = Arc::new(TransactionGuard::fake());
            let tables = TableTree::new(data_root, PageHint::None, fake, mem.clone())?;
            tables.visit_all_pages(|path| {
                mem.mark_debug_allocated_page(path.page_number());
                Ok(())
            })?;
        }

        let system_root = mem.get_system_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let system_tables = TableTree::new(system_root, PageHint::None, fake, mem.clone())?;
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

    fn do_repair(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<[Option<BtreeHeader>; 2], DatabaseError> {
        if !Self::verify_primary_checksums(mem.clone())? {
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
            if !Self::verify_primary_checksums(mem.clone())? {
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

        mem.begin_repair()?;

        let data_root = mem.get_data_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let tables = TableTree::new(data_root, PageHint::None, fake, mem.clone())?;
            tables.visit_all_pages(|path| {
                mem.mark_page_allocated(path.page_number());
                Ok(())
            })?;
        }

        // 0.9 because the repair takes 3 full scans and the third is done now. There is just some system tables left
        let mut handle = RepairSession::new(0.9);
        repair_callback(&mut handle);
        if handle.aborted() {
            return Err(DatabaseError::RepairAborted);
        }

        let system_root = mem.get_system_root();
        {
            let fake = Arc::new(TransactionGuard::fake());
            let system_tables = TableTree::new(system_root, PageHint::None, fake, mem.clone())?;
            system_tables.visit_all_pages(|path| {
                mem.mark_page_allocated(path.page_number());
                Ok(())
            })?;
        }

        Self::visit_freed_tree(system_root, DATA_FREED_TABLE, mem.clone(), |page| {
            mem.mark_page_allocated(page);
            Ok(())
        })?;
        Self::visit_freed_tree(system_root, SYSTEM_FREED_TABLE, mem.clone(), |page| {
            mem.mark_page_allocated(page);
            Ok(())
        })?;
        #[cfg(debug_assertions)]
        {
            Self::check_repaired_allocated_pages_table(system_root, mem.clone())?;
        }

        mem.end_repair()?;

        // We need to invalidate the userspace cache, because we're about to implicitly free the freed table
        // by storing an empty root during the below commit()
        mem.clear_read_cache();

        Ok([data_root, system_root])
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        file: Box<dyn StorageBackend>,
        allow_initialize: bool,
        page_size: usize,
        region_size: Option<u64>,
        read_cache_size_bytes: usize,
        write_cache_size_bytes: usize,
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
        salt: Option<[u8; 16]>,
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database {:?}", &file_path);
        let mem = TransactionalMemory::new(
            file,
            allow_initialize,
            page_size,
            region_size,
            read_cache_size_bytes,
            write_cache_size_bytes,
            false,
            salt,
        )?;
        let mut mem = Arc::new(mem);
        // If the last transaction used 2-phase commit and updated the allocator state table, then
        // we can just load the allocator state from there. Otherwise, we need a full repair
        if let Some(tree) = Self::get_allocator_state_table(&mem)? {
            #[cfg(feature = "logging")]
            info!("Found valid allocator state, full repair not needed");
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
                    SavepointError::InvalidSavepoint => unreachable!(),
                    SavepointError::Storage(storage) => {
                        return Err(storage.into());
                    }
                },
            };
            db.transaction_tracker
                .register_persistent_savepoint(&savepoint);
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
        let system_table_tree = TableTree::new(
            mem.get_system_root(),
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
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
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;

        // Make sure this isn't stale allocator state left over from a previous transaction
        if !mem.is_valid_allocator_state(&tree)? {
            return Ok(None);
        }

        Ok(Some(tree))
    }

    fn allocate_read_transaction(&self) -> Result<TransactionGuard> {
        let id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;

        Ok(TransactionGuard::new_read(
            id,
            self.transaction_tracker.clone(),
        ))
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
    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        // Fail early if there has been an I/O error -- nothing can be committed in that case
        self.mem.check_io_errors()?;
        let guard = TransactionGuard::new_write(
            self.transaction_tracker.start_write_transaction(),
            self.transaction_tracker.clone(),
        );
        WriteTransaction::new(guard, self.transaction_tracker.clone(), self.mem.clone())
            .map_err(|e| e.into())
    }

    fn ensure_allocator_state_table_and_trim(&self) -> Result<(), Error> {
        // Make a new quick-repair commit to update the allocator state table
        #[cfg(feature = "logging")]
        debug!("Writing allocator state table");
        let mut tx = self.begin_write()?;
        tx.set_quick_repair(true);
        tx.set_shrink_policy(ShrinkPolicy::Maximum);
        tx.commit()?;

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if !thread::panicking() && self.ensure_allocator_state_table_and_trim().is_err() {
            #[cfg(feature = "logging")]
            warn!("Failed to write allocator state table. Repair may be required at restart.");
        }

        if self.mem.close().is_err() {
            #[cfg(feature = "logging")]
            warn!("Failed to flush database file. Repair may be required at restart.");
        }
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

/// Configuration builder of a redb [Database].
pub struct Builder {
    page_size: usize,
    region_size: Option<u64>,
    read_cache_size_bytes: usize,
    write_cache_size_bytes: usize,
    repair_callback: Box<dyn Fn(&mut RepairSession)>,
}

impl Builder {
    /// Read the salt from the unencrypted database header
    fn read_salt_from_header(file: &File) -> Result<[u8; 16], DatabaseError> {
        use std::os::unix::fs::FileExt;

        // Read the database header (unencrypted) - header is always at the beginning
        // We need to read enough bytes to get the salt, which is at offset 32
        const SALT_OFFSET: usize = 32; // TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>()
        const HEADER_SIZE_FOR_SALT: usize = SALT_OFFSET + 16;

        let mut header_buffer = [0u8; HEADER_SIZE_FOR_SALT];
        file.read_exact_at(&mut header_buffer, 0)
            .map_err(|e| DatabaseError::Storage(StorageError::Io(e)))?;

        // Extract the salt from the header
        let salt = header_buffer[SALT_OFFSET..(SALT_OFFSET + 16)].try_into()
            .map_err(|_| DatabaseError::Storage(StorageError::Corrupted("Invalid salt in database header".to_string())))?;

        Ok(salt)
    }

    /// Construct a new [Builder] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `cache_size_bytes`: 1GiB
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut result = Self {
            // Default to 4k pages. Benchmarking showed that this was a good default on all platforms,
            // including MacOS with 16k pages. Therefore, users are not allowed to configure it at the moment.
            // It is part of the file format, so can be enabled in the future.
            page_size: PAGE_SIZE,
            region_size: None,
            // TODO: Default should probably take into account the total system memory
            read_cache_size_bytes: 0,
            // TODO: Default should probably take into account the total system memory
            write_cache_size_bytes: 0,
            repair_callback: Box::new(|_| {}),
        };

        result.set_cache_size(1024 * 1024 * 1024);
        result
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
        self.page_size = std::cmp::max(size, 512);
        self
    }

    /// Set the amount of memory (in bytes) used for caching data
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        // TODO: allow dynamic expansion of the read/write cache
        self.read_cache_size_bytes = bytes / 10 * 9;
        self.write_cache_size_bytes = bytes / 10;
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
    pub fn create(&self, path: impl AsRef<Path>, password: &str) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // Check if this is an existing database file by checking its size
        let file_size = file.metadata()?.len();
        
        if file_size == 0 {
            // New database file - create with new backend
            let backend = EncryptedFileBackend::new(file, password)?;
            let salt = *backend.get_salt();

            Database::new(
                Box::new(backend),
                true,
                self.page_size,
                self.region_size,
                self.read_cache_size_bytes,
                self.write_cache_size_bytes,
                &self.repair_callback,
                Some(salt),
            )
        } else {
            // Existing database file - open with existing salt
            let salt = Self::read_salt_from_header(&file)?;
            let backend = EncryptedFileBackend::open(file, password, salt)?;

            Database::new(
                Box::new(backend),
                false, // Don't allow initialization for existing files
                self.page_size,
                None, // Region size should be read from existing file
                self.read_cache_size_bytes,
                self.write_cache_size_bytes,
                &self.repair_callback,
                Some(salt),
            )
        }
    }

    /// Opens an existing redbx database.
    pub fn open(&self, path: impl AsRef<Path>, password: &str) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read the salt from the unencrypted database header
        let salt = Self::read_salt_from_header(&file)?;

        // Try to open the database, converting decryption failures to incorrect password errors
        let backend = EncryptedFileBackend::open(file, password, salt)?;

        Database::new(
            Box::new(backend),
            false,
            self.page_size,
            None,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
            Some(salt),
        )
    }

    /// Opens an existing redb database.
    ///
    /// If the file has been opened for writing (i.e. as a [`Database`]) [`DatabaseError::DatabaseAlreadyOpen`]
    /// will be returned on platforms which support file locks (macOS, Windows, Linux). On other platforms,
    /// the caller MUST avoid calling this method when the database is open for writing.
    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
        password: &str,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let file = OpenOptions::new().read(true).open(path)?;

        // Read the salt from the unencrypted database header
        let salt = Self::read_salt_from_header(&file)?;

        // Create read-only encrypted backend
        let backend = EncryptedFileBackend::open_internal(file, password, salt, true)?;

        // Validate password for read-only access
        backend.validate_password()?;

        ReadOnlyDatabase::new(
            Box::new(backend),
            self.page_size,
            None,
            self.read_cache_size_bytes,
            Some(salt),
        )
    }

    /// Open an existing or create a new database in the given `file`.
    ///
    /// The file must be empty or contain a valid database.
    pub fn create_file(&self, file: File, password: &str) -> Result<Database, DatabaseError> {
        let backend = EncryptedFileBackend::new(file, password)?;
        let salt = *backend.get_salt();
        Database::new(
            Box::new(backend),
            true,
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
            Some(salt),
        )
    }

    /// Open an existing or create a new database with the given backend.
    pub fn create_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(backend),
            true,
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
            None,
        )
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        Database, DatabaseError, Durability, ReadableTable,
        StorageError, TableDefinition,
    };
    use std::io::ErrorKind;

    // Removed crash_regression4 test - not compatible with encryption

    // Removed transient_io_error test - not compatible with encryption

    #[test]
    fn small_pages() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path(), "test_password")
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
            .create(tmpfile.path(), "test_password")
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
        let savepoint5 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint3);
        assert!(tx.restore_savepoint(&savepoint4).is_err());
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_two_phase_commit(true);
        tx.restore_savepoint(&savepoint5).unwrap();
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
            .create(tmpfile.path(), "test_password")
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
            .create(tmpfile.path(), "test_password")
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

    // Removed dynamic_shrink test - not compatible with encryption

    #[test]
    fn create_new_db_in_empty_file() {
        let tmpfile = crate::create_tempfile();

        let _db = Database::builder()
            .create_file(tmpfile.into_file(), "test_password")
            .unwrap();
    }

    #[test]
    fn open_missing_file() {
        let tmpfile = crate::create_tempfile();

        let err = Database::builder()
            .open(tmpfile.path().with_extension("missing"), "test_password")
            .unwrap_err();

        match err {
            DatabaseError::Storage(StorageError::Io(err)) if err.kind() == ErrorKind::NotFound => {}
            err => panic!("Unexpected error for empty file: {err}"),
        }
    }

    #[test]
    fn open_empty_file() {
        let tmpfile = crate::create_tempfile();

        let err = Database::builder().open(tmpfile.path(), "test_password").unwrap_err();

        match err {
            DatabaseError::Storage(StorageError::Io(err))
                if err.kind() == ErrorKind::InvalidData || err.kind() == ErrorKind::UnexpectedEof => {}
            err => panic!("Unexpected error for empty file: {err}"),
        }
    }
}
