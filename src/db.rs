use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    AllPageNumbersBtreeIter, BtreeHeader, BtreeRangeIter, FreedPageList, FreedTableKey,
    InternalTableDefinition, PageHint, PageNumber, RawBtree, SerializedSavepoint, TableTreeMut,
    TableType, TransactionalMemory, PAGE_SIZE,
};
use crate::types::{Key, Value};
use crate::{
    CompactionError, DatabaseError, Durability, ReadOnlyTable, SavepointError, StorageError,
};
use crate::{ReadTransaction, Result, WriteTransaction};
use std::fmt::{Debug, Display, Formatter};

use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::RangeFull;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::TransactionError;
use crate::multimap_table::{parse_subtree_roots, DynamicCollection};
use crate::sealed::Sealed;
use crate::transactions::SAVEPOINT_TABLE;
use crate::tree_store::file_backend::FileBackend;
#[cfg(feature = "logging")]
use log::{info, warn};

#[allow(clippy::len_without_is_empty)]
/// Implements persistent storage for a database.
pub trait StorageBackend: 'static + Debug + Send + Sync {
    /// Gets the current length of the storage.
    fn len(&self) -> std::result::Result<u64, io::Error>;

    /// Reads the specified array of bytes from the storage.
    ///
    /// If `len` + `offset` exceeds the length of the storage an appropriate `Error` should be returned or a panic may occur.
    fn read(&self, offset: u64, len: usize) -> std::result::Result<Vec<u8>, io::Error>;

    /// Sets the length of the storage.
    ///
    /// When extending the storage the new positions should be zero initialized.
    fn set_len(&self, len: u64) -> std::result::Result<(), io::Error>;

    /// Syncs all buffered data with the persistent storage.
    ///
    /// If `eventual` is true, data may become persistent at some point after this call returns,
    /// but the storage must gaurantee that a write barrier is inserted: i.e. all writes before this
    /// call to `sync_data()` will become persistent before any writes that occur after.
    fn sync_data(&self, eventual: bool) -> std::result::Result<(), io::Error>;

    /// Writes the specified array to the storage.
    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), io::Error>;
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

impl<'a, K: Key + 'static, V: Value + 'static> TableHandle for TableDefinition<'a, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Value> Sealed for TableDefinition<'_, K, V> {}

impl<'a, K: Key + 'static, V: Value + 'static> Clone for TableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: Key + 'static, V: Value + 'static> Copy for TableDefinition<'a, K, V> {}

impl<'a, K: Key + 'static, V: Value + 'static> Display for TableDefinition<'a, K, V> {
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

impl<'a, K: Key + 'static, V: Key + 'static> MultimapTableHandle
    for MultimapTableDefinition<'a, K, V>
{
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Key> Sealed for MultimapTableDefinition<'_, K, V> {}

impl<'a, K: Key + 'static, V: Key + 'static> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: Key + 'static, V: Key + 'static> Copy for MultimapTableDefinition<'a, K, V> {}

impl<'a, K: Key + 'static, V: Key + 'static> Display for MultimapTableDefinition<'a, K, V> {
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
    mem: Arc<TransactionalMemory>,
    transaction_tracker: Arc<TransactionTracker>,
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

    pub(crate) fn get_memory(&self) -> Arc<TransactionalMemory> {
        self.mem.clone()
    }

    pub(crate) fn verify_primary_checksums(mem: Arc<TransactionalMemory>) -> Result<bool> {
        let fake_freed_pages = Arc::new(Mutex::new(vec![]));
        let table_tree = TableTreeMut::new(
            mem.get_data_root(),
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
            fake_freed_pages.clone(),
        );
        if !table_tree.verify_checksums()? {
            return Ok(false);
        }
        let system_table_tree = TableTreeMut::new(
            mem.get_system_root(),
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
            fake_freed_pages.clone(),
        );
        if !system_table_tree.verify_checksums()? {
            return Ok(false);
        }
        assert!(fake_freed_pages.lock().unwrap().is_empty());

        if let Some(header) = mem.get_freed_root() {
            if !RawBtree::new(
                Some(header),
                FreedTableKey::fixed_width(),
                FreedPageList::fixed_width(),
                mem.clone(),
            )
            .verify_checksum()?
            {
                return Ok(false);
            }
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

        if !Self::verify_primary_checksums(self.mem.clone())? {
            was_clean = false;
        }

        Self::do_repair(&mut self.mem, &|_| {}).map_err(|err| match err {
            DatabaseError::Storage(storage_err) => storage_err,
            _ => unreachable!(),
        })?;
        if allocator_hash != self.mem.allocator_hash() {
            was_clean = false;
        }
        self.mem.begin_writable()?;

        Ok(was_clean)
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
        if self.transaction_tracker.any_savepoint_exists() {
            return Err(CompactionError::EphemeralSavepointExists);
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
        system_root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        oldest_unprocessed_free_transaction: TransactionId,
    ) -> Result {
        let freed_list = Arc::new(Mutex::new(vec![]));
        let table_tree = TableTreeMut::new(
            system_root,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
            freed_list,
        );
        let fake_transaction_tracker = Arc::new(TransactionTracker::new(TransactionId::new(0)));
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
                ReadOnlyTable::new(
                    "internal savepoint table".to_string(),
                    savepoint_table_def.get_root(),
                    PageHint::None,
                    Arc::new(TransactionGuard::fake()),
                    mem.clone(),
                )?;
            for result in savepoint_table.range::<SavepointId>(..)? {
                let (_, savepoint_data) = result?;
                let savepoint = savepoint_data
                    .value()
                    .to_savepoint(fake_transaction_tracker.clone());
                if let Some(header) = savepoint.get_user_root() {
                    Self::mark_tables_recursive(header.root, mem.clone(), true)?;
                }
                Self::mark_freed_tree(
                    savepoint.get_freed_root(),
                    mem.clone(),
                    oldest_unprocessed_free_transaction,
                )?;
            }
        }

        Ok(())
    }

    fn mark_freed_tree(
        freed_root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        oldest_unprocessed_free_transaction: TransactionId,
    ) -> Result {
        if let Some(header) = freed_root {
            let freed_pages_iter = AllPageNumbersBtreeIter::new(
                header.root,
                FreedTableKey::fixed_width(),
                FreedPageList::fixed_width(),
                mem.clone(),
            )?;
            mem.mark_pages_allocated(freed_pages_iter, true)?;
        }

        let freed_table: ReadOnlyTable<FreedTableKey, FreedPageList<'static>> = ReadOnlyTable::new(
            "internal freed table".to_string(),
            freed_root,
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        let lookup_key = FreedTableKey {
            transaction_id: oldest_unprocessed_free_transaction.raw_id(),
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
        mem: Arc<TransactionalMemory>,
        allow_duplicates: bool,
    ) -> Result {
        // Repair the allocator state
        // All pages in the master table
        let master_pages_iter = AllPageNumbersBtreeIter::new(root, None, None, mem.clone())?;
        mem.mark_pages_allocated(master_pages_iter, allow_duplicates)?;

        // Iterate over all other tables
        let iter: BtreeRangeIter<&str, InternalTableDefinition> =
            BtreeRangeIter::new::<RangeFull, &str>(&(..), Some(root), mem.clone())?;

        // Chain all the other tables to the master table iter
        for entry in iter {
            let definition = entry?.value();
            if let Some(header) = definition.get_root() {
                match definition.get_type() {
                    TableType::Normal => {
                        let table_pages_iter = AllPageNumbersBtreeIter::new(
                            header.root,
                            definition.get_fixed_key_size(),
                            definition.get_fixed_value_size(),
                            mem.clone(),
                        )?;
                        mem.mark_pages_allocated(table_pages_iter, allow_duplicates)?;
                    }
                    TableType::Multimap => {
                        let table_pages_iter = AllPageNumbersBtreeIter::new(
                            header.root,
                            definition.get_fixed_key_size(),
                            DynamicCollection::<()>::fixed_width_with(
                                definition.get_fixed_value_size(),
                            ),
                            mem.clone(),
                        )?;
                        mem.mark_pages_allocated(table_pages_iter, allow_duplicates)?;

                        let table_pages_iter = AllPageNumbersBtreeIter::new(
                            header.root,
                            definition.get_fixed_key_size(),
                            DynamicCollection::<()>::fixed_width_with(
                                definition.get_fixed_value_size(),
                            ),
                            mem.clone(),
                        )?;
                        for table_page in table_pages_iter {
                            let page = mem.get_page(table_page?)?;
                            let subtree_roots = parse_subtree_roots(
                                &page,
                                definition.get_fixed_key_size(),
                                definition.get_fixed_value_size(),
                            );
                            for subtree_header in subtree_roots {
                                let sub_root_iter = AllPageNumbersBtreeIter::new(
                                    subtree_header.root,
                                    definition.get_fixed_value_size(),
                                    <()>::fixed_width(),
                                    mem.clone(),
                                )?;
                                mem.mark_pages_allocated(sub_root_iter, allow_duplicates)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn do_repair(
        mem: &mut Arc<TransactionalMemory>, // Only &mut to ensure exclusivity
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<(), DatabaseError> {
        if !Self::verify_primary_checksums(mem.clone())? {
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
        if let Some(header) = data_root {
            Self::mark_tables_recursive(header.root, mem.clone(), false)?;
        }

        let freed_root = mem.get_freed_root();
        // Allow processing of all transactions, since this is the main freed tree
        Self::mark_freed_tree(freed_root, mem.clone(), TransactionId::new(0))?;
        let freed_table: ReadOnlyTable<FreedTableKey, FreedPageList<'static>> = ReadOnlyTable::new(
            "internal freed table".to_string(),
            freed_root,
            PageHint::None,
            Arc::new(TransactionGuard::fake()),
            mem.clone(),
        )?;
        // The persistent savepoints might hold references to older freed trees that are partially processed.
        // Make sure we don't reprocess those frees, as that would result in a double-free
        let oldest_unprocessed_transaction =
            if let Some(entry) = freed_table.range::<FreedTableKey>(..)?.next() {
                TransactionId::new(entry?.0.value().transaction_id)
            } else {
                mem.get_last_committed_transaction_id()?
            };
        drop(freed_table);

        // 0.9 because the repair takes 3 full scans and the third is done now. There is just some system tables left
        let mut handle = RepairSession::new(0.9);
        repair_callback(&mut handle);
        if handle.aborted() {
            return Err(DatabaseError::RepairAborted);
        }

        let system_root = mem.get_system_root();
        if let Some(header) = system_root {
            Self::mark_tables_recursive(header.root, mem.clone(), false)?;
        }
        Self::mark_persistent_savepoints(system_root, mem.clone(), oldest_unprocessed_transaction)?;

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
        file: Box<dyn StorageBackend>,
        page_size: usize,
        region_size: Option<u64>,
        read_cache_size_bytes: usize,
        write_cache_size_bytes: usize,
        repair_callback: &(dyn Fn(&mut RepairSession) + 'static),
    ) -> Result<Self, DatabaseError> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database {:?}", &file_path);
        let mem = TransactionalMemory::new(
            file,
            page_size,
            region_size,
            read_cache_size_bytes,
            write_cache_size_bytes,
        )?;
        let mut mem = Arc::new(mem);
        if mem.needs_repair()? {
            #[cfg(feature = "logging")]
            warn!("Database {:?} not shutdown cleanly. Repairing", &file_path);
            let mut handle = RepairSession::new(0.0);
            repair_callback(&mut handle);
            if handle.aborted() {
                return Err(DatabaseError::RepairAborted);
            }
            Self::do_repair(&mut mem, repair_callback)?;
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
        let guard = TransactionGuard::new_write(
            self.transaction_tracker.start_write_transaction(),
            self.transaction_tracker.clone(),
        );
        WriteTransaction::new(guard, self.transaction_tracker.clone(), self.mem.clone())
            .map_err(|e| e.into())
    }

    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    pub fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        let guard = self.allocate_read_transaction()?;
        #[cfg(feature = "logging")]
        info!("Beginning read transaction id={:?}", guard.id());
        ReadTransaction::new(self.get_memory(), guard)
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

    /// Abort the repair process. The coorresponding call to [Builder::open] or [Builder::create] will return an error
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
    /// The [RepairSession] argument can be used to control the repair process.
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
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Database::new(
            Box::new(FileBackend::new(file)?),
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
        )
    }

    /// Opens an existing redb database.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        if file.metadata()?.len() == 0 {
            return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
        }

        Database::new(
            Box::new(FileBackend::new(file)?),
            self.page_size,
            None,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
        )
    }

    /// Open an existing or create a new database in the given `file`.
    ///
    /// The file must be empty or contain a valid database.
    pub fn create_file(&self, file: File) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(FileBackend::new(file)?),
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
        )
    }

    /// Open an existing or create a new database with the given backend.
    pub fn create_with_backend(
        &self,
        backend: impl StorageBackend,
    ) -> Result<Database, DatabaseError> {
        Database::new(
            Box::new(backend),
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            &self.repair_callback,
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
    use crate::backends::FileBackend;
    use crate::{
        Database, DatabaseError, Durability, ReadableTable, StorageBackend, StorageError,
        TableDefinition,
    };
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    struct FailingBackend {
        inner: FileBackend,
        countdown: AtomicU64,
    }

    impl FailingBackend {
        fn new(backend: FileBackend, countdown: u64) -> Self {
            Self {
                inner: backend,
                countdown: AtomicU64::new(countdown),
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
                    if x > 0 {
                        Some(x - 1)
                    } else {
                        None
                    }
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

        fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
            self.check_countdown()?;
            self.inner.read(offset, len)
        }

        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self, eventual: bool) -> Result<(), std::io::Error> {
            self.check_countdown()?;
            self.inner.sync_data(eventual)
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            self.decrement_countdown()?;
            self.inner.write(offset, data)
        }
    }

    #[test]
    fn crash_regression4() {
        let tmpfile = crate::create_tempfile();

        let backend = FailingBackend::new(
            FileBackend::new(tmpfile.as_file().try_clone().unwrap()).unwrap(),
            23,
        );
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
            .create(tmpfile.path())
            .unwrap();
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
