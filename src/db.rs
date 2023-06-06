use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    AllPageNumbersBtreeIter, BtreeRangeIter, Checksum, FreedPageList, FreedTableKey,
    InternalTableDefinition, PageHint, PageNumber, RawBtree, SerializedSavepoint, TableTree,
    TableType, TransactionalMemory, PAGE_SIZE,
};
use crate::types::{RedbKey, RedbValue};
use crate::{
    CompactionError, DatabaseError, Durability, ReadOnlyTable, ReadableTable, SavepointError,
    StorageError,
};
use crate::{ReadTransaction, Result, WriteTransaction};
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::RangeFull;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::TransactionError;
use crate::multimap_table::parse_subtree_roots;
use crate::sealed::Sealed;
use crate::transactions::SAVEPOINT_TABLE;
#[cfg(feature = "logging")]
use log::{info, warn};

struct AtomicTransactionId {
    inner: AtomicU64,
}

impl AtomicTransactionId {
    fn new(last_id: TransactionId) -> Self {
        Self {
            inner: AtomicU64::new(last_id.0),
        }
    }

    fn next(&self) -> TransactionId {
        let id = self.inner.fetch_add(1, Ordering::AcqRel);
        TransactionId(id)
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
pub struct TableDefinition<'a, K: RedbKey + 'static, V: RedbValue + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> TableDefinition<'a, K, V> {
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

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> TableHandle for TableDefinition<'a, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: RedbKey, V: RedbValue> Sealed for TableDefinition<'_, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Clone for TableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Copy for TableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Display for TableDefinition<'a, K, V> {
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
pub struct MultimapTableDefinition<'a, K: RedbKey + 'static, V: RedbKey + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> MultimapTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> MultimapTableHandle
    for MultimapTableDefinition<'a, K, V>
{
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: RedbKey, V: RedbKey> Sealed for MultimapTableDefinition<'_, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> Copy for MultimapTableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> Display for MultimapTableDefinition<'a, K, V> {
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
    mem: TransactionalMemory,
    next_transaction_id: AtomicTransactionId,
    transaction_tracker: Arc<Mutex<TransactionTracker>>,
    pub(crate) live_write_transaction: Mutex<Option<TransactionId>>,
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub fn create(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Self::builder().create(path)
    }

    /// Opens an existing redb database.
    pub fn open(path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        Self::builder().open(path)
    }

    pub(crate) fn get_memory(&self) -> &TransactionalMemory {
        &self.mem
    }

    #[cfg(any(fuzzing, test))]
    pub fn set_crash_countdown(&self, value: u64) {
        self.mem.set_crash_countdown(value);
    }

    fn verify_primary_checksums(mem: &TransactionalMemory) -> Result<bool> {
        if let Some((root, root_checksum)) = mem.get_data_root() {
            if !RawBtree::new(
                Some((root, root_checksum)),
                <&str>::fixed_width(),
                InternalTableDefinition::fixed_width(),
                mem,
            )
            .verify_checksum()?
            {
                return Ok(false);
            }

            // Iterate over all other tables
            let iter: BtreeRangeIter<&str, InternalTableDefinition> =
                BtreeRangeIter::new::<RangeFull, &str>(&(..), Some(root), mem)?;
            for entry in iter {
                let definition = entry?.value();
                if let Some((table_root, table_checksum)) = definition.get_root() {
                    if !RawBtree::new(
                        Some((table_root, table_checksum)),
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        mem,
                    )
                    .verify_checksum()?
                    {
                        return Ok(false);
                    }
                }
            }
        }

        if let Some((root, root_checksum)) = mem.get_system_root() {
            if !RawBtree::new(
                Some((root, root_checksum)),
                <&str>::fixed_width(),
                InternalTableDefinition::fixed_width(),
                mem,
            )
            .verify_checksum()?
            {
                return Ok(false);
            }

            // Iterate over all other tables
            let iter: BtreeRangeIter<&str, InternalTableDefinition> =
                BtreeRangeIter::new::<RangeFull, &str>(&(..), Some(root), mem)?;
            for entry in iter {
                let definition = entry?.value();
                if let Some((table_root, table_checksum)) = definition.get_root() {
                    if !RawBtree::new(
                        Some((table_root, table_checksum)),
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        mem,
                    )
                    .verify_checksum()?
                    {
                        return Ok(false);
                    }
                }
            }
        }

        if let Some((freed_root, freed_checksum)) = mem.get_freed_root() {
            if !RawBtree::new(
                Some((freed_root, freed_checksum)),
                FreedTableKey::fixed_width(),
                None,
                mem,
            )
            .verify_checksum()?
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Check the integrity of the database file, and repair it if possible.
    ///
    /// Returns `Ok(true)` if the database passed integrity checks; `Ok(false)` if it failed but was repaired,
    /// and `Err(Corrupted)` if the check failed and the file could not be repaired
    pub fn check_integrity(&mut self) -> Result<bool> {
        self.mem.clear_cache_and_reload()?;

        if !self.mem.needs_repair()? && Self::verify_primary_checksums(&self.mem)? {
            return Ok(true);
        }

        Self::do_repair(&mut self.mem)?;
        self.mem.begin_writable()?;

        Ok(false)
    }

    /// Compacts the database file
    ///
    /// Returns `true` if compaction was performed, and `false` if no futher compaction was possible
    pub fn compact(&mut self) -> Result<bool, CompactionError> {
        // Commit to free up any pending free pages
        // Use 2-phase commit to avoid any possible security issues. Plus this compaction is going to be so slow that it doesn't matter
        let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        if txn.list_persistent_savepoints()?.next().is_some() {
            return Err(CompactionError::PersistentSavepointExists);
        }
        txn.set_durability(Durability::Paranoid);
        txn.commit().map_err(|e| e.into_storage_error())?;
        // Repeat, just in case executing list_persistent_savepoints() created a new table
        let mut txn = self.begin_write().map_err(|e| e.into_storage_error())?;
        txn.set_durability(Durability::Paranoid);
        txn.commit().map_err(|e| e.into_storage_error())?;
        // There can't be any outstanding transactions because we have a `&mut self`, so all pending free pages
        // should have been cleared out by the above commit()
        assert!(self.mem.get_freed_root().is_none());

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
            txn.set_durability(Durability::Paranoid);
            txn.commit().map_err(|e| e.into_storage_error())?;
            assert!(self.mem.get_freed_root().is_none());

            if !progress {
                break;
            } else {
                compacted = true;
            }
        }

        Ok(compacted)
    }

    fn mark_persistent_savepoints(
        system_root: Option<(PageNumber, Checksum)>,
        mem: &mut TransactionalMemory,
        oldest_unprocessed_free_transaction: TransactionId,
    ) -> Result {
        let freed_list = Arc::new(Mutex::new(vec![]));
        let table_tree = TableTree::new(system_root, mem, freed_list);
        let fake_transaction_tracker = Arc::new(Mutex::new(TransactionTracker::new()));
        if let Some(savepoint_table_def) = table_tree
            .get_table::<SavepointId, SerializedSavepoint>(
                SAVEPOINT_TABLE.name(),
                TableType::Normal,
            )
            .map_err(|e| {
                e.into_storage_error_or_corrupted("Persistent savepoint table corrupted")
            })?
        {
            let savepoint_table: ReadOnlyTable<SavepointId, SerializedSavepoint> =
                ReadOnlyTable::new(savepoint_table_def.get_root(), PageHint::None, mem)?;
            for result in savepoint_table.range::<SavepointId>(..)? {
                let (_, savepoint_data) = result?;
                let savepoint = savepoint_data
                    .value()
                    .to_savepoint(fake_transaction_tracker.clone());
                if let Some((root, _)) = savepoint.get_user_root() {
                    Self::mark_tables_recursive(root, mem, true)?;
                }
                Self::mark_freed_tree(
                    savepoint.get_freed_root(),
                    mem,
                    oldest_unprocessed_free_transaction,
                )?;
            }
        }

        Ok(())
    }

    fn mark_freed_tree(
        freed_root: Option<(PageNumber, Checksum)>,
        mem: &TransactionalMemory,
        oldest_unprocessed_free_transaction: TransactionId,
    ) -> Result {
        if let Some((root, _)) = freed_root {
            let freed_pages_iter = AllPageNumbersBtreeIter::new(
                root,
                FreedTableKey::fixed_width(),
                FreedPageList::fixed_width(),
                mem,
            )?;
            mem.mark_pages_allocated(freed_pages_iter, true)?;
        }

        let freed_table: ReadOnlyTable<FreedTableKey, FreedPageList<'static>> =
            ReadOnlyTable::new(freed_root, PageHint::None, mem)?;
        let lookup_key = FreedTableKey {
            transaction_id: oldest_unprocessed_free_transaction.0,
            pagination_id: 0,
        };
        for result in freed_table.range::<FreedTableKey>(lookup_key..)? {
            let (_, freed_page_list) = result?;
            let mut freed_page_list_as_vec = vec![];
            for i in 0..freed_page_list.value().len() {
                freed_page_list_as_vec.push(Ok(freed_page_list.value().get(i)));
            }
            mem.mark_pages_allocated(freed_page_list_as_vec.into_iter(), true)?;
        }

        Ok(())
    }

    fn mark_tables_recursive(
        root: PageNumber,
        mem: &TransactionalMemory,
        allow_duplicates: bool,
    ) -> Result {
        // Repair the allocator state
        // All pages in the master table
        let master_pages_iter = AllPageNumbersBtreeIter::new(root, None, None, mem)?;
        mem.mark_pages_allocated(master_pages_iter, allow_duplicates)?;

        // Iterate over all other tables
        let iter: BtreeRangeIter<&str, InternalTableDefinition> =
            BtreeRangeIter::new::<RangeFull, &str>(&(..), Some(root), mem)?;

        // Chain all the other tables to the master table iter
        for entry in iter {
            let definition = entry?.value();
            if let Some((table_root, _)) = definition.get_root() {
                let table_pages_iter = AllPageNumbersBtreeIter::new(
                    table_root,
                    definition.get_fixed_key_size(),
                    definition.get_fixed_value_size(),
                    mem,
                )?;
                mem.mark_pages_allocated(table_pages_iter, allow_duplicates)?;

                // Multimap tables may have additional subtrees in their values
                if definition.get_type() == TableType::Multimap {
                    let table_pages_iter = AllPageNumbersBtreeIter::new(
                        table_root,
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        mem,
                    )?;
                    for table_page in table_pages_iter {
                        let page = mem.get_page(table_page?)?;
                        let subtree_roots =
                            parse_subtree_roots(&page, definition.get_fixed_key_size());
                        for sub_root in subtree_roots {
                            let sub_root_iter = AllPageNumbersBtreeIter::new(
                                sub_root,
                                definition.get_fixed_value_size(),
                                <()>::fixed_width(),
                                mem,
                            )?;
                            mem.mark_pages_allocated(sub_root_iter, allow_duplicates)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn do_repair(mem: &mut TransactionalMemory) -> Result {
        if !Self::verify_primary_checksums(mem)? {
            mem.repair_primary_corrupted();
            // We need to invalidate the userspace cache, because walking the tree in verify_primary_checksums() may
            // have poisoned it with pages that just got rolled back by repair_primary_corrupted(), since
            // that rolls back a partially committed transaction.
            mem.clear_read_cache();
            if !Self::verify_primary_checksums(mem)? {
                return Err(StorageError::Corrupted(
                    "Failed to repair database. All roots are corrupted".to_string(),
                ));
            }
        }

        mem.begin_repair()?;

        let data_root = mem.get_data_root();
        if let Some((root, _)) = data_root {
            Self::mark_tables_recursive(root, mem, false)?;
        }

        let freed_root = mem.get_freed_root();
        // Allow processing of all transactions, since this is the main freed tree
        Self::mark_freed_tree(freed_root, mem, TransactionId(0))?;
        let freed_table: ReadOnlyTable<FreedTableKey, FreedPageList<'static>> =
            ReadOnlyTable::new(freed_root, PageHint::None, mem)?;
        // The persistent savepoints might hold references to older freed trees that are partially processed.
        // Make sure we don't reprocess those frees, as that would result in a double-free
        let oldest_unprocessed_transaction =
            if let Some(entry) = freed_table.range::<FreedTableKey>(..)?.next() {
                TransactionId(entry?.0.value().transaction_id)
            } else {
                mem.get_last_committed_transaction_id()?
            };
        drop(freed_table);

        let system_root = mem.get_system_root();
        if let Some((root, _)) = system_root {
            Self::mark_tables_recursive(root, mem, false)?;
        }
        Self::mark_persistent_savepoints(system_root, mem, oldest_unprocessed_transaction)?;

        mem.end_repair()?;

        // We need to invalidate the userspace cache, because we're about to implicitly free the freed table
        // by storing an empty root during the below commit()
        mem.clear_read_cache();

        let transaction_id = mem.get_last_committed_transaction_id()?.next();
        mem.commit(
            data_root,
            system_root,
            freed_root,
            transaction_id,
            false,
            true,
        )?;

        Ok(())
    }

    fn new(
        file: File,
        page_size: usize,
        region_size: Option<u64>,
        read_cache_size_bytes: usize,
        write_cache_size_bytes: usize,
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database {:?}", &file_path);
        let mut mem = TransactionalMemory::new(
            file,
            page_size,
            region_size,
            read_cache_size_bytes,
            write_cache_size_bytes,
        )?;
        if mem.needs_repair()? {
            #[cfg(feature = "logging")]
            warn!("Database {:?} not shutdown cleanly. Repairing", &file_path);
            Self::do_repair(&mut mem)?;
        }

        mem.begin_writable()?;
        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();

        let db = Database {
            mem,
            next_transaction_id: AtomicTransactionId::new(next_transaction_id),
            transaction_tracker: Arc::new(Mutex::new(TransactionTracker::new())),
            live_write_transaction: Mutex::new(None),
        };

        // Restore the tracker state for any persistent savepoints
        let txn = db.begin_write().map_err(|e| e.into_storage_error())?;
        if let Some(next_id) = txn.next_persistent_savepoint_id()? {
            db.transaction_tracker
                .lock()
                .unwrap()
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
                .lock()
                .unwrap()
                .register_persistent_savepoint(&savepoint);
        }
        txn.abort()?;

        Ok(db)
    }

    fn allocate_read_transaction(&self) -> Result<TransactionId> {
        let mut guard = self.transaction_tracker.lock().unwrap();
        let id = self.mem.get_last_committed_transaction_id()?;
        guard.register_read_transaction(id);

        Ok(id)
    }

    pub(crate) fn allocate_savepoint(&self) -> Result<(SavepointId, TransactionId)> {
        let id = self
            .transaction_tracker
            .lock()
            .unwrap()
            .allocate_savepoint();
        Ok((id, self.allocate_read_transaction()?))
    }

    pub(crate) fn increment_transaction_id(&self) -> TransactionId {
        self.next_transaction_id.next()
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
        WriteTransaction::new(self, self.transaction_tracker.clone()).map_err(|e| e.into())
    }

    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    pub fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let id = self.allocate_read_transaction()?;
        #[cfg(feature = "logging")]
        info!("Beginning read transaction id={:?}", id);
        Ok(ReadTransaction::new(
            self.get_memory(),
            self.transaction_tracker.clone(),
            id,
        ))
    }
}

/// Configuration builder of a redb [Database].
pub struct Builder {
    page_size: usize,
    region_size: Option<u64>,
    read_cache_size_bytes: usize,
    write_cache_size_bytes: usize,
}

impl Builder {
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
        };

        result.set_cache_size(1024 * 1024 * 1024);
        result
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

    #[cfg(test)]
    pub(crate) fn set_region_size(&mut self, size: u64) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Database::new(
            file,
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
        )
    }

    /// Opens an existing redb database.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        if !path.as_ref().exists() {
            Err(StorageError::Io(ErrorKind::NotFound.into()).into())
        } else if File::open(path.as_ref())?.metadata()?.len() > 0 {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            Database::new(
                file,
                self.page_size,
                None,
                self.read_cache_size_bytes,
                self.write_cache_size_bytes,
            )
        } else {
            Err(StorageError::Io(io::Error::from(ErrorKind::InvalidData)).into())
        }
    }
}

// This just makes it easier to throw `dbg` etc statements on `Result<Database>`
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

#[cfg(test)]
mod test {
    use crate::error::CommitError;
    use crate::{Database, Durability, ReadableTable, StorageError, TableDefinition, TableError};

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
        tx.set_durability(Durability::Paranoid);
        let savepoint0 = tx.ephemeral_savepoint().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        tx.restore_savepoint(&savepoint0).unwrap();
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&660503, 489).unwrap().as_mut().fill(0xFF);
            assert!(t.remove(&291295).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        tx.restore_savepoint(&savepoint0).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
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
        tx.set_durability(Durability::Paranoid);
        let savepoint3 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint1);
        tx.restore_savepoint(&savepoint3).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint4 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint2);
        tx.restore_savepoint(&savepoint3).unwrap();
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.remove(&207936).unwrap().is_none());
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint5 = tx.ephemeral_savepoint().unwrap();
        drop(savepoint3);
        assert!(tx.restore_savepoint(&savepoint4).is_err());
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        tx.restore_savepoint(&savepoint5).unwrap();
        tx.set_durability(Durability::None);
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
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 306];
            t.insert(&539717, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        tx.restore_savepoint(&savepoint1).unwrap();
        tx.set_durability(Durability::None);
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
    fn crash_regression1() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();
        db.set_crash_countdown(0);

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        {
            assert!(matches!(
                tx.open_table(table_def),
                Err(TableError::Storage(StorageError::SimulatedIOFailure))
            ));
        }
        tx.abort().unwrap();
    }

    #[test]
    fn crash_regression2() {
        let tmpfile = crate::create_tempfile();

        let db = Database::builder()
            .set_cache_size(48101213 / 5)
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();
        db.set_crash_countdown(13);

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        // TX1
        println!("\nTX1");
        let tx = db.begin_write().unwrap();
        let savepoint0 = tx.persistent_savepoint().unwrap();
        tx.commit().unwrap();
        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::None);
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        // TX2
        println!("\nTX2");
        let mut tx = db.begin_write().unwrap();
        let savepoint1 = tx.ephemeral_savepoint().unwrap();
        let _savepoint2 = tx.persistent_savepoint().unwrap();
        let temp = tx.get_persistent_savepoint(savepoint0).unwrap();
        tx.restore_savepoint(&temp).unwrap();
        drop(temp);
        drop(savepoint1);
        tx.commit().unwrap();
        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::None);
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        // TX3
        println!("\nTX3");
        let mut tx = db.begin_write().unwrap();
        let _savepoint3 = tx.ephemeral_savepoint().unwrap();
        let savepoint4 = tx.persistent_savepoint().unwrap();
        let temp = tx.get_persistent_savepoint(savepoint4).unwrap();
        tx.restore_savepoint(&temp).unwrap();
        drop(temp);
        tx.commit().unwrap();
        let tx = db.begin_write().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.delete_persistent_savepoint(savepoint0).unwrap();
        tx.delete_persistent_savepoint(savepoint4).unwrap();
        tx.commit().unwrap();

        // TX4
        println!("\nTX4");
        let tx = db.begin_write().unwrap();
        let _savepoint5 = tx.persistent_savepoint().unwrap();
        tx.commit().unwrap();

        // TX5
        println!("\nTX5");
        let mut tx = db.begin_write().unwrap();
        let savepoint6 = tx.ephemeral_savepoint().unwrap();
        let _savepoint7 = tx.persistent_savepoint().unwrap();
        tx.restore_savepoint(&savepoint6).unwrap();
        assert!(matches!(
            tx.commit(),
            Err(CommitError::Storage(StorageError::SimulatedIOFailure))
        ));

        drop(db);
        Database::builder()
            .set_cache_size(48101213)
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();
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
}
