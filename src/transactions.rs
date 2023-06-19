use crate::error::CommitError;
use crate::sealed::Sealed;
use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    Btree, BtreeMut, FreedPageList, FreedTableKey, InternalTableDefinition, PageHint, PageNumber,
    SerializedSavepoint, TableTree, TableType, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue};
use crate::{
    Database, MultimapTable, MultimapTableDefinition, MultimapTableHandle, ReadOnlyMultimapTable,
    ReadOnlyTable, ReadableTable, Result, Savepoint, SavepointError, Table, TableDefinition,
    TableError, TableHandle, UntypedMultimapTableHandle, UntypedTableHandle,
};
#[cfg(feature = "logging")]
use log::{info, warn};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::RangeFull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::{panic, thread};

const NEXT_SAVEPOINT_TABLE: SystemTableDefinition<(), SavepointId> =
    SystemTableDefinition::new("next_savepoint_id");
pub(crate) const SAVEPOINT_TABLE: SystemTableDefinition<SavepointId, SerializedSavepoint> =
    SystemTableDefinition::new("persistent_savepoints");

pub struct SystemTableDefinition<'a, K: RedbKey + 'static, V: RedbValue + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> SystemTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> TableHandle
    for SystemTableDefinition<'a, K, V>
{
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: RedbKey, V: RedbValue> Sealed for SystemTableDefinition<'_, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Clone for SystemTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Copy for SystemTableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Display for SystemTableDefinition<'a, K, V> {
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

/// Informational storage stats about the database
#[derive(Debug)]
pub struct DatabaseStats {
    pub(crate) tree_height: u32,
    pub(crate) allocated_pages: u64,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
    pub(crate) page_size: usize,
}

impl DatabaseStats {
    /// Maximum traversal distance to reach the deepest (key, value) pair, across all tables
    pub fn tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Number of pages allocated
    pub fn allocated_pages(&self) -> u64 {
        self.allocated_pages
    }

    /// Number of leaf pages that store user data
    pub fn leaf_pages(&self) -> u64 {
        self.leaf_pages
    }

    /// Number of branch pages in btrees that store user data
    pub fn branch_pages(&self) -> u64 {
        self.branch_pages
    }

    /// Number of bytes consumed by keys and values that have been inserted.
    /// Does not include indexing overhead
    pub fn stored_bytes(&self) -> u64 {
        self.stored_leaf_bytes
    }

    /// Number of bytes consumed by keys in internal branch pages, plus other metadata
    pub fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }

    /// Number of bytes consumed by fragmentation, both in data pages and internal metadata tables
    pub fn fragmented_bytes(&self) -> u64 {
        self.fragmented_bytes
    }

    /// Number of bytes per page
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum Durability {
    /// Commits with this durability level will not be persisted to disk unless followed by a
    /// commit with a higher durability level.
    ///
    /// Note: Pages are only freed during commits with higher durability levels. Exclusively using
    /// this function may result in Error::OutOfSpace.
    None,
    /// Commits with this durability level have been queued for persitance to disk, and should be
    /// persistent some time after [WriteTransaction::commit] returns.
    Eventual,
    /// Commits with this durability level are guaranteed to be persistent as soon as
    /// [WriteTransaction::commit] returns.
    ///
    /// Data is written with checksums, with the following commit algorithm:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Flip the god byte primary bit to activate the newly updated commit slot
    /// 3. Call `fsync` to ensure all writes have been persisted to disk
    ///
    /// When opening the database after a crash, the most recent of the two commit slots with a
    /// valid checksum is used.
    ///
    /// Security considerations: The checksum used is xxhash, a fast, non-cryptographic hash
    /// function with close to perfect collision resistance when used with non-malicious input. An
    /// attacker with an extremely high degree of control over the database's workload, including
    /// the ability to cause the database process to crash, can cause invalid data to be written
    /// with a valid checksum, leaving the database in an invalid, attacker-controlled state.
    Immediate,
    /// Commits with this durability level have the same gaurantees as [Durability::Immediate]
    ///
    /// Additionally, aata is written with the following 2-phase commit algorithm:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Call `fsync` to ensure the database slate and commit slot update have been persisted
    /// 3. Flip the god byte primary bit to activate the newly updated commit slot
    /// 4. Call `fsync` to ensure the write to the god byte has been persisted
    ///
    /// This mitigates a theoretical attack where an attacker who
    /// 1. can control the order in which pages are flushed to disk
    /// 2. can introduce crashes during fsync(),
    /// 3. has knowledge of the database file contents, and
    /// 4. can include arbitrary data in a write transaction
    /// could cause a transaction to partially commit (some but not all of the data is written).
    /// This is described in the design doc in futher detail.
    ///
    /// Security considerations: Many hard disk drives and SSDs do not actually guarantee that data
    /// has been persisted to disk after calling `fsync`. Even with this commit level, an attacker
    /// with a high degree of control over the database's workload, including the ability to cause
    /// the database process to crash, can cause the database to crash with the god byte primary bit
    /// pointing to an invalid commit slot, leaving the database in an invalid, potentially attacker-controlled state.
    Paranoid,
}

/// A read/write transaction
///
/// Only a single [`WriteTransaction`] may exist at a time
pub struct WriteTransaction<'db> {
    db: &'db Database,
    transaction_tracker: Arc<Mutex<TransactionTracker>>,
    mem: &'db TransactionalMemory,
    transaction_id: TransactionId,
    table_tree: RwLock<TableTree<'db>>,
    system_table_tree: RwLock<TableTree<'db>>,
    // The table of freed pages by transaction. FreedTableKey -> binary.
    // The binary blob is a length-prefixed array of PageNumber
    freed_tree: Mutex<BtreeMut<'db, FreedTableKey, FreedPageList<'static>>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    // Pages that were freed from the freed-tree. These can be freed immediately after commit(),
    // since read transactions do not access the freed-tree
    post_commit_frees: Arc<Mutex<Vec<PageNumber>>>,
    open_tables: Mutex<HashMap<String, &'static panic::Location<'static>>>,
    open_system_tables: Mutex<HashMap<String, &'static panic::Location<'static>>>,
    completed: bool,
    dirty: AtomicBool,
    durability: Durability,
    // Persistent savepoints created during this transaction
    created_persistent_savepoints: Mutex<HashSet<SavepointId>>,
    deleted_persistent_savepoints: Mutex<Vec<(SavepointId, TransactionId)>>,
    live_write_transaction: MutexGuard<'db, Option<TransactionId>>,
}

impl<'db> WriteTransaction<'db> {
    pub(crate) fn new(
        db: &'db Database,
        transaction_tracker: Arc<Mutex<TransactionTracker>>,
    ) -> Result<Self> {
        let mut live_write_transaction = db.live_write_transaction.lock().unwrap();
        assert!(live_write_transaction.is_none());
        let transaction_id = db.increment_transaction_id();
        #[cfg(feature = "logging")]
        info!("Beginning write transaction id={:?}", transaction_id);
        *live_write_transaction = Some(transaction_id);

        let root_page = db.get_memory().get_data_root();
        let system_page = db.get_memory().get_system_root();
        let freed_root = db.get_memory().get_freed_root();
        let freed_pages = Arc::new(Mutex::new(vec![]));
        let post_commit_frees = Arc::new(Mutex::new(vec![]));
        Ok(Self {
            db,
            transaction_tracker,
            mem: db.get_memory(),
            transaction_id,
            table_tree: RwLock::new(TableTree::new(
                root_page,
                db.get_memory(),
                freed_pages.clone(),
            )),
            system_table_tree: RwLock::new(TableTree::new(
                system_page,
                db.get_memory(),
                freed_pages.clone(),
            )),
            freed_tree: Mutex::new(BtreeMut::new(
                freed_root,
                db.get_memory(),
                post_commit_frees.clone(),
            )),
            freed_pages,
            post_commit_frees,
            open_tables: Mutex::new(Default::default()),
            open_system_tables: Mutex::new(Default::default()),
            completed: false,
            dirty: AtomicBool::new(false),
            durability: Durability::Immediate,
            created_persistent_savepoints: Mutex::new(Default::default()),
            deleted_persistent_savepoints: Mutex::new(vec![]),
            live_write_transaction,
        })
    }

    #[track_caller]
    fn open_system_table<'txn, K: RedbKey + 'static, V: RedbValue + 'static>(
        &'txn self,
        definition: SystemTableDefinition<K, V>,
    ) -> Result<Table<'db, 'txn, K, V>> {
        #[cfg(feature = "logging")]
        info!("Opening system table: {}", definition);
        if let Some(location) = self
            .open_system_tables
            .lock()
            .unwrap()
            .get(definition.name())
        {
            panic!(
                "System table {} is already open at {}",
                definition.name(),
                location
            );
        }
        self.dirty.store(true, Ordering::Release);

        let internal_table = self
            .system_table_tree
            .write()
            .unwrap()
            .get_or_create_table::<K, V>(definition.name(), TableType::Normal)
            .map_err(|e| {
                e.into_storage_error_or_corrupted("Internal error. System table is corrupted")
            })?;
        self.open_system_tables
            .lock()
            .unwrap()
            .insert(definition.name().to_string(), panic::Location::caller());

        Ok(Table::new(
            definition.name(),
            true,
            internal_table.get_root(),
            self.freed_pages.clone(),
            self.mem,
            self,
        ))
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database.
    /// This savepoint will exist until it is deleted with `[delete_savepoint()]`.
    ///
    /// Note that while a savepoint exists, pages that become unused after it was created are not freed.
    /// Therefore, the lifetime of a savepoint should be minimized.
    ///
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened)
    /// or if the transaction's durability is less than `[Durability::Immediate]`
    pub fn persistent_savepoint(&self) -> Result<u64, SavepointError> {
        if !matches!(
            self.durability,
            Durability::Immediate | Durability::Paranoid
        ) {
            return Err(SavepointError::InvalidSavepoint);
        }

        let mut savepoint = self.ephemeral_savepoint()?;

        let mut next_table = self.open_system_table(NEXT_SAVEPOINT_TABLE)?;
        let mut savepoint_table = self.open_system_table(SAVEPOINT_TABLE)?;
        next_table.insert((), savepoint.get_id().next())?;

        savepoint_table.insert(
            savepoint.get_id(),
            SerializedSavepoint::from_savepoint(&savepoint),
        )?;

        savepoint.set_persistent();

        self.created_persistent_savepoints
            .lock()
            .unwrap()
            .insert(savepoint.get_id());

        Ok(savepoint.get_id().0)
    }

    pub(crate) fn next_persistent_savepoint_id(&self) -> Result<Option<SavepointId>> {
        let next_table = self.open_system_table(NEXT_SAVEPOINT_TABLE)?;
        let value = next_table.get(())?;
        if let Some(next_id) = value {
            Ok(Some(next_id.value()))
        } else {
            Ok(None)
        }
    }

    /// Get a persistent savepoint given its id
    pub fn get_persistent_savepoint(&self, id: u64) -> Result<Savepoint, SavepointError> {
        let table = self.open_system_table(SAVEPOINT_TABLE)?;
        let value = table.get(SavepointId(id))?;

        value
            .map(|x| x.value().to_savepoint(self.transaction_tracker.clone()))
            .ok_or(SavepointError::InvalidSavepoint)
    }

    /// Delete the given persistent savepoint.
    ///
    /// Note that if the transaction is abort()'ed this deletion will be rolled back.
    ///
    /// Returns `true` if the savepoint existed
    /// Returns `[SavepointError::InvalidSavepoint`] if the transaction's durability is less than `[Durability::Immediate]`
    pub fn delete_persistent_savepoint(&self, id: u64) -> Result<bool, SavepointError> {
        if !matches!(
            self.durability,
            Durability::Immediate | Durability::Paranoid
        ) {
            return Err(SavepointError::InvalidSavepoint);
        }
        let mut table = self.open_system_table(SAVEPOINT_TABLE)?;
        let savepoint = table.remove(SavepointId(id))?;
        if let Some(serialized) = savepoint {
            let savepoint = serialized
                .value()
                .to_savepoint(self.transaction_tracker.clone());
            self.deleted_persistent_savepoints
                .lock()
                .unwrap()
                .push((savepoint.get_id(), savepoint.get_transaction_id()));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all persistent savepoints
    pub fn list_persistent_savepoints(&self) -> Result<impl Iterator<Item = u64>> {
        let table = self.open_system_table(SAVEPOINT_TABLE)?;
        let mut savepoints = vec![];
        for savepoint in table.range::<SavepointId>(..)? {
            savepoints.push(savepoint?.0.value().0);
        }
        Ok(savepoints.into_iter())
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database
    ///
    /// This savepoint will be freed as soon as the returned `[Savepoint]` is dropped.
    ///
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened)
    pub fn ephemeral_savepoint(&self) -> Result<Savepoint, SavepointError> {
        if self.dirty.load(Ordering::Acquire) {
            return Err(SavepointError::InvalidSavepoint);
        }

        let (id, transaction_id) = self.db.allocate_savepoint()?;
        #[cfg(feature = "logging")]
        info!(
            "Creating savepoint id={:?}, txn_id={:?}",
            id, transaction_id
        );

        let regional_allocators = self.mem.get_raw_allocator_states();
        let root = self.mem.get_data_root();
        let system_root = self.mem.get_system_root();
        let freed_root = self.mem.get_freed_root();
        let savepoint = Savepoint::new_ephemeral(
            self.db.get_memory(),
            self.transaction_tracker.clone(),
            id,
            transaction_id,
            root,
            system_root,
            freed_root,
            regional_allocators,
        );

        Ok(savepoint)
    }

    /// Restore the state of the database to the given [`Savepoint`]
    ///
    /// Calling this method invalidates all [`Savepoint`]s created after savepoint
    pub fn restore_savepoint(&mut self, savepoint: &Savepoint) -> Result<(), SavepointError> {
        // Ensure that user does not try to restore a Savepoint that is from a different Database
        assert_eq!(
            self.transaction_tracker.as_ref() as *const _,
            savepoint.db_address()
        );

        if !self
            .transaction_tracker
            .lock()
            .unwrap()
            .is_valid_savepoint(savepoint.get_id())
        {
            return Err(SavepointError::InvalidSavepoint);
        }
        #[cfg(feature = "logging")]
        info!(
            "Beginning savepoint restore (id={:?}) in transaction id={:?}",
            savepoint.get_id(),
            self.transaction_id
        );
        // Restoring a savepoint that reverted a file format or checksum type change could corrupt
        // the database
        assert_eq!(self.db.get_memory().get_version(), savepoint.get_version());
        self.dirty.store(true, Ordering::Release);

        let allocated_since_savepoint = self
            .mem
            .pages_allocated_since_raw_state(savepoint.get_regional_allocator_states());

        // We don't want to rollback the system tree, so keep any pages it references
        let referenced_by_system_tree = self
            .system_table_tree
            .read()
            .unwrap()
            .all_referenced_pages()?;

        let mut freed_pages = vec![];
        for page in allocated_since_savepoint {
            if referenced_by_system_tree.contains(&page) {
                continue;
            }
            if self.mem.uncommitted(page) {
                self.mem.free(page);
            } else {
                freed_pages.push(page);
            }
        }
        *self.freed_pages.lock().unwrap() = freed_pages;
        self.table_tree = RwLock::new(TableTree::new(
            savepoint.get_user_root(),
            self.mem,
            self.freed_pages.clone(),
        ));

        // Remove any freed pages that have already been processed. Otherwise this would result in a double free
        // We assume below that PageNumber is length 8
        let oldest_unprocessed_transaction = if let Some(entry) = self
            .freed_tree
            .lock()
            .unwrap()
            .range::<RangeFull, FreedTableKey>(&(..))?
            .next()
        {
            entry?.key().transaction_id
        } else {
            self.transaction_id.0
        };

        let mut freed_tree = BtreeMut::new(
            savepoint.get_freed_root(),
            self.mem,
            self.post_commit_frees.clone(),
        );
        let lookup_key = FreedTableKey {
            transaction_id: oldest_unprocessed_transaction,
            pagination_id: 0,
        };
        let mut to_remove = vec![];
        for entry in freed_tree.range(&(..lookup_key))? {
            to_remove.push(entry?.key());
        }
        for key in to_remove {
            freed_tree.remove(&key)?;
        }

        *self.freed_tree.lock().unwrap() = freed_tree;

        // Invalidate all savepoints that are newer than the one being applied to prevent the user
        // from later trying to restore a savepoint "on another timeline"
        self.transaction_tracker
            .lock()
            .unwrap()
            .invalidate_savepoints_after(savepoint.get_id());

        for persistent_savepoint in self.list_persistent_savepoints()? {
            if persistent_savepoint > savepoint.get_id().0 {
                self.delete_persistent_savepoint(persistent_savepoint)?;
            }
        }

        Ok(())
    }

    /// Set the desired durability level for writes made in this transaction
    /// Defaults to [`Durability::Immediate`]
    ///
    /// Will panic if the durability is reduced below `[Durability::Immediate]` after a persistent savepoint has been created or deleted.
    pub fn set_durability(&mut self, durability: Durability) {
        let no_created = self
            .created_persistent_savepoints
            .lock()
            .unwrap()
            .is_empty();
        let no_deleted = self
            .deleted_persistent_savepoints
            .lock()
            .unwrap()
            .is_empty();
        assert!(no_created && no_deleted);
        self.durability = durability;
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_table<'txn, K: RedbKey + 'static, V: RedbValue + 'static>(
        &'txn self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'db, 'txn, K, V>, TableError> {
        #[cfg(feature = "logging")]
        info!("Opening table: {}", definition);
        if let Some(location) = self.open_tables.lock().unwrap().get(definition.name()) {
            return Err(TableError::TableAlreadyOpen(
                definition.name().to_string(),
                location,
            ));
        }
        self.dirty.store(true, Ordering::Release);

        let internal_table = self
            .table_tree
            .write()
            .unwrap()
            .get_or_create_table::<K, V>(definition.name(), TableType::Normal)?;
        self.open_tables
            .lock()
            .unwrap()
            .insert(definition.name().to_string(), panic::Location::caller());

        Ok(Table::new(
            definition.name(),
            false,
            internal_table.get_root(),
            self.freed_pages.clone(),
            self.mem,
            self,
        ))
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_multimap_table<'txn, K: RedbKey + 'static, V: RedbKey + 'static>(
        &'txn self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'db, 'txn, K, V>, TableError> {
        #[cfg(feature = "logging")]
        info!("Opening multimap table: {}", definition);
        if let Some(location) = self.open_tables.lock().unwrap().get(definition.name()) {
            return Err(TableError::TableAlreadyOpen(
                definition.name().to_string(),
                location,
            ));
        }
        self.dirty.store(true, Ordering::Release);

        let internal_table = self
            .table_tree
            .write()
            .unwrap()
            .get_or_create_table::<K, V>(definition.name(), TableType::Multimap)?;
        self.open_tables
            .lock()
            .unwrap()
            .insert(definition.name().to_string(), panic::Location::caller());

        Ok(MultimapTable::new(
            definition.name(),
            false,
            internal_table.get_root(),
            self.freed_pages.clone(),
            self.mem,
            self,
        ))
    }

    pub(crate) fn close_table<K: RedbKey + 'static, V: RedbValue + 'static>(
        &self,
        name: &str,
        system: bool,
        table: &mut BtreeMut<K, V>,
    ) {
        if system {
            self.open_system_tables
                .lock()
                .unwrap()
                .remove(name)
                .unwrap();
            self.system_table_tree
                .write()
                .unwrap()
                .stage_update_table_root(name, table.get_root());
        } else {
            self.open_tables.lock().unwrap().remove(name).unwrap();
            self.table_tree
                .write()
                .unwrap()
                .stage_update_table_root(name, table.get_root());
        }
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table(&self, definition: impl TableHandle) -> Result<bool, TableError> {
        #[cfg(feature = "logging")]
        info!("Deleting table: {}", definition.name());
        self.dirty.store(true, Ordering::Release);
        self.table_tree
            .write()
            .unwrap()
            .delete_table(definition.name(), TableType::Normal)
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_multimap_table(
        &self,
        definition: impl MultimapTableHandle,
    ) -> Result<bool, TableError> {
        #[cfg(feature = "logging")]
        info!("Deleting multimap table: {}", definition.name());
        self.dirty.store(true, Ordering::Release);
        self.table_tree
            .write()
            .unwrap()
            .delete_table(definition.name(), TableType::Multimap)
    }

    /// List all the tables
    pub fn list_tables(&self) -> Result<impl Iterator<Item = UntypedTableHandle> + '_> {
        self.table_tree
            .read()
            .unwrap()
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter().map(UntypedTableHandle::new))
    }

    /// List all the multimap tables
    pub fn list_multimap_tables(
        &self,
    ) -> Result<impl Iterator<Item = UntypedMultimapTableHandle> + '_> {
        self.table_tree
            .read()
            .unwrap()
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter().map(UntypedMultimapTableHandle::new))
    }

    /// Commit the transaction
    ///
    /// All writes performed in this transaction will be visible to future transactions, and are
    /// durable as consistent with the [`Durability`] level set by [`Self::set_durability`]
    pub fn commit(mut self) -> Result<(), CommitError> {
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        self.commit_inner()
    }

    fn commit_inner(&mut self) -> Result<(), CommitError> {
        #[cfg(feature = "logging")]
        info!(
            "Committing transaction id={:?} with durability={:?}",
            self.transaction_id, self.durability
        );
        match self.durability {
            Durability::None => self.non_durable_commit()?,
            Durability::Eventual => self.durable_commit(true, false)?,
            Durability::Immediate => self.durable_commit(false, false)?,
            Durability::Paranoid => self.durable_commit(false, true)?,
        }

        for (savepoint, transaction) in self.deleted_persistent_savepoints.lock().unwrap().iter() {
            self.transaction_tracker
                .lock()
                .unwrap()
                .deallocate_savepoint(*savepoint, *transaction);
        }

        #[cfg(feature = "logging")]
        info!(
            "Finished commit of transaction id={:?}",
            self.transaction_id
        );

        Ok(())
    }

    /// Abort the transaction
    ///
    /// All writes performed in this transaction will be rolled back
    pub fn abort(mut self) -> Result {
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        self.abort_inner()
    }

    fn abort_inner(&mut self) -> Result {
        #[cfg(feature = "logging")]
        info!("Aborting transaction id={:?}", self.transaction_id);
        for savepoint in self.created_persistent_savepoints.lock().unwrap().iter() {
            match self.delete_persistent_savepoint(savepoint.0) {
                Ok(_) => {}
                Err(err) => match err {
                    SavepointError::InvalidSavepoint => {
                        unreachable!();
                    }
                    SavepointError::Storage(storage_err) => {
                        return Err(storage_err);
                    }
                },
            }
        }
        self.table_tree.write().unwrap().clear_table_root_updates();
        self.mem.rollback_uncommitted_writes()?;
        #[cfg(feature = "logging")]
        info!("Finished abort of transaction id={:?}", self.transaction_id);
        Ok(())
    }

    pub(crate) fn durable_commit(&mut self, eventual: bool, two_phase: bool) -> Result {
        let oldest_live_read = self
            .transaction_tracker
            .lock()
            .unwrap()
            .oldest_live_read_transaction()
            .unwrap_or(self.transaction_id);

        let user_root = self
            .table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;

        let system_root = self
            .system_table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;

        self.process_freed_pages(oldest_live_read)?;
        // If a savepoint exists it might reference the freed-tree, since it holds a reference to the
        // root of the freed-tree. Therefore, we must use the transactional free mechanism to free
        // those pages. If there are no save points then these can be immediately freed, which is
        // done at the end of this function.
        let savepoint_exists = self
            .transaction_tracker
            .lock()
            .unwrap()
            .any_savepoint_exists();
        self.store_freed_pages(savepoint_exists)?;

        // Finalize freed table checksums, before doing the final commit
        // user & system table trees were already finalized when we flushed the pending roots above
        self.freed_tree.lock().unwrap().finalize_dirty_checksums()?;

        let freed_root = self.freed_tree.lock().unwrap().get_root();

        self.mem.commit(
            user_root,
            system_root,
            freed_root,
            self.transaction_id,
            eventual,
            two_phase,
        )?;

        // Mark any pending non-durable commits as fully committed.
        self.transaction_tracker
            .lock()
            .unwrap()
            .clear_pending_non_durable_commits();

        // Immediately free the pages that were freed from the freed-tree itself. These are only
        // accessed by write transactions, so it's safe to free them as soon as the commit is done.
        for page in self.post_commit_frees.lock().unwrap().drain(..) {
            self.mem.free(page);
        }

        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&mut self) -> Result {
        let user_root = self
            .table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;

        let system_root = self
            .system_table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;

        // Store all freed pages for a future commit(), since we can't free pages during a
        // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
        self.store_freed_pages(true)?;

        // Finalize all checksums, before doing the final commit
        self.freed_tree.lock().unwrap().finalize_dirty_checksums()?;

        let freed_root = self.freed_tree.lock().unwrap().get_root();

        self.mem
            .non_durable_commit(user_root, system_root, freed_root, self.transaction_id)?;
        // Register this as a non-durable transaction to ensure that the freed pages we just pushed
        // are only processed after this has been persisted
        self.transaction_tracker
            .lock()
            .unwrap()
            .register_non_durable_commit(self.transaction_id);
        Ok(())
    }

    // Relocate pages to lower number regions/pages
    // Returns true if a page(s) was moved
    pub(crate) fn compact_pages(&mut self) -> Result<bool> {
        let mut progress = false;
        // Relocate the region tracker page
        if self.mem.relocate_region_tracker()? {
            progress = true;
        }

        // Relocate the btree pages
        let mut table_tree = self.table_tree.write().unwrap();
        if table_tree.compact_tables()? {
            progress = true;
        }

        Ok(progress)
    }

    // NOTE: must be called before store_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(&mut self, oldest_live_read: TransactionId) -> Result {
        // We assume below that PageNumber is length 8
        assert_eq!(PageNumber::serialized_size(), 8);
        let lookup_key = FreedTableKey {
            transaction_id: oldest_live_read.0,
            pagination_id: 0,
        };

        let mut to_remove = vec![];
        let mut freed_tree = self.freed_tree.lock().unwrap();
        for entry in freed_tree.range(&(..lookup_key))? {
            let entry = entry?;
            to_remove.push(entry.key());
            let value = entry.value();
            for i in 0..value.len() {
                self.mem.free(value.get(i));
            }
        }

        // Remove all the old transactions
        for key in to_remove {
            freed_tree.remove(&key)?;
        }

        Ok(())
    }

    fn store_freed_pages(&mut self, include_post_commit_free: bool) -> Result {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        let mut freed_tree = self.freed_tree.lock().unwrap();
        if include_post_commit_free {
            // Move all the post-commit pages that came from the freed-tree. These need to be stored
            // since we can't free pages until a durable commit
            self.freed_pages
                .lock()
                .unwrap()
                .extend(self.post_commit_frees.lock().unwrap().drain(..));
        }
        while !self.freed_pages.lock().unwrap().is_empty() {
            let chunk_size = 100;
            let buffer_size = FreedPageList::required_bytes(chunk_size);
            let key = FreedTableKey {
                transaction_id: self.transaction_id.0,
                pagination_id: pagination_counter,
            };
            let mut access_guard =
                freed_tree.insert_reserve(&key, buffer_size.try_into().unwrap())?;

            let mut freed_pages = self.freed_pages.lock().unwrap();
            let len = freed_pages.len();
            access_guard.as_mut().clear();
            for page in freed_pages.drain(len - min(len, chunk_size)..) {
                access_guard.as_mut().push_back(page);
            }
            drop(access_guard);

            pagination_counter += 1;

            if include_post_commit_free {
                // Move all the post-commit pages that came from the freed-tree. These need to be stored
                // since we can't free pages until a durable commit
                freed_pages.extend(self.post_commit_frees.lock().unwrap().drain(..));
            }
        }

        Ok(())
    }

    /// Retrieves information about storage usage in the database
    pub fn stats(&self) -> Result<DatabaseStats> {
        let table_tree = self.table_tree.read().unwrap();
        let data_tree_stats = table_tree.stats()?;
        let freed_tree_stats = self.freed_tree.lock().unwrap().stats()?;
        let total_metadata_bytes = data_tree_stats.metadata_bytes()
            + freed_tree_stats.metadata_bytes
            + freed_tree_stats.stored_leaf_bytes;
        let total_fragmented =
            data_tree_stats.fragmented_bytes() + freed_tree_stats.fragmented_bytes;

        Ok(DatabaseStats {
            tree_height: data_tree_stats.tree_height(),
            allocated_pages: self.mem.count_allocated_pages()?,
            leaf_pages: data_tree_stats.leaf_pages(),
            branch_pages: data_tree_stats.branch_pages(),
            stored_leaf_bytes: data_tree_stats.stored_bytes(),
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) -> Result {
        // Flush any pending updates to make sure we get the latest root
        if let Some(page) = self
            .table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()
            .unwrap()
        {
            eprintln!("Master tree:");
            let master_tree: Btree<&str, InternalTableDefinition> =
                Btree::new(Some(page), PageHint::None, self.mem)?;
            master_tree.print_debug(true)?;
        }

        Ok(())
    }
}

impl<'a> Drop for WriteTransaction<'a> {
    fn drop(&mut self) {
        *self.live_write_transaction = None;
        if !self.completed && !thread::panicking() {
            #[allow(unused_variables)]
            if let Err(error) = self.abort_inner() {
                #[cfg(feature = "logging")]
                warn!("Failure automatically aborting transaction: {}", error);
            }
        }
    }
}

/// A read-only transaction
///
/// Read-only transactions may exist concurrently with writes
pub struct ReadTransaction<'a> {
    transaction_tracker: Arc<Mutex<TransactionTracker>>,
    mem: &'a TransactionalMemory,
    tree: TableTree<'a>,
    transaction_id: TransactionId,
}

impl<'db> ReadTransaction<'db> {
    pub(crate) fn new(
        mem: &'db TransactionalMemory,
        transaction_tracker: Arc<Mutex<TransactionTracker>>,
        transaction_id: TransactionId,
    ) -> Self {
        let root_page = mem.get_data_root();
        Self {
            transaction_tracker,
            mem,
            tree: TableTree::new(root_page, mem, Default::default()),
            transaction_id,
        }
    }

    /// Open the given table
    pub fn open_table<K: RedbKey + 'static, V: RedbValue + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>, TableError> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;

        Ok(ReadOnlyTable::new(
            header.get_root(),
            PageHint::Clean,
            self.mem,
        )?)
    }

    /// Open the given table
    pub fn open_multimap_table<K: RedbKey + 'static, V: RedbKey + 'static>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<ReadOnlyMultimapTable<K, V>, TableError> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Multimap)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;

        Ok(ReadOnlyMultimapTable::new(
            header.get_root(),
            PageHint::Clean,
            self.mem,
        )?)
    }

    /// List all the tables
    pub fn list_tables(&self) -> Result<impl Iterator<Item = UntypedTableHandle>> {
        self.tree
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter().map(UntypedTableHandle::new))
    }

    /// List all the multimap tables
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = UntypedMultimapTableHandle>> {
        self.tree
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter().map(UntypedMultimapTableHandle::new))
    }
}

impl<'a> Drop for ReadTransaction<'a> {
    fn drop(&mut self) {
        self.transaction_tracker
            .lock()
            .unwrap()
            .deallocate_read_transaction(self.transaction_id);
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, TableDefinition};

    const X: TableDefinition<&str, &str> = TableDefinition::new("x");

    #[test]
    fn transaction_id_persistence() {
        let tmpfile = crate::create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert("hello", "world").unwrap();
        }
        let first_txn_id = write_txn.transaction_id;
        write_txn.commit().unwrap();
        drop(db);

        let db2 = Database::create(tmpfile.path()).unwrap();
        let write_txn = db2.begin_write().unwrap();
        assert!(write_txn.transaction_id > first_txn_id);
    }
}
