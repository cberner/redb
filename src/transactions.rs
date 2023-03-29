use crate::transaction_tracker::{TransactionId, TransactionTracker};
use crate::tree_store::{
    Btree, BtreeMut, FreedPageList, FreedTableKey, InternalTableDefinition, PageHint, PageNumber,
    TableTree, TableType, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue};
use crate::{
    Database, Error, MultimapTable, MultimapTableDefinition, MultimapTableHandle,
    ReadOnlyMultimapTable, ReadOnlyTable, Result, Savepoint, Table, TableDefinition, TableHandle,
    UntypedMultimapTableHandle, UntypedTableHandle,
};
#[cfg(feature = "logging")]
use log::{info, warn};
use std::cmp::min;
use std::collections::HashMap;
use std::ops::RangeFull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::{panic, thread};

/// Informational storage stats about the database
#[derive(Debug)]
pub struct DatabaseStats {
    pub(crate) tree_height: usize,
    pub(crate) allocated_pages: usize,
    pub(crate) leaf_pages: usize,
    pub(crate) branch_pages: usize,
    pub(crate) stored_leaf_bytes: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) fragmented_bytes: usize,
    pub(crate) page_size: usize,
}

impl DatabaseStats {
    /// Maximum traversal distance to reach the deepest (key, value) pair, across all tables
    pub fn tree_height(&self) -> usize {
        self.tree_height
    }

    /// Number of pages allocated
    pub fn allocated_pages(&self) -> usize {
        self.allocated_pages
    }

    /// Number of leaf pages that store user data
    pub fn leaf_pages(&self) -> usize {
        self.leaf_pages
    }

    /// Number of branch pages in btrees that store user data
    pub fn branch_pages(&self) -> usize {
        self.branch_pages
    }

    /// Number of bytes consumed by keys and values that have been inserted.
    /// Does not include indexing overhead
    pub fn stored_bytes(&self) -> usize {
        self.stored_leaf_bytes
    }

    /// Number of bytes consumed by keys in internal branch pages, plus other metadata
    pub fn metadata_bytes(&self) -> usize {
        self.metadata_bytes
    }

    /// Number of bytes consumed by fragmentation, both in data pages and internal metadata tables
    pub fn fragmented_bytes(&self) -> usize {
        self.fragmented_bytes
    }

    /// Number of bytes per page
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[derive(Copy, Clone, Debug)]
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
    Immediate,
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
    // The table of freed pages by transaction. FreedTableKey -> binary.
    // The binary blob is a length-prefixed array of PageNumber
    freed_tree: Mutex<BtreeMut<'db, FreedTableKey, FreedPageList<'static>>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    open_tables: Mutex<HashMap<String, &'static panic::Location<'static>>>,
    completed: bool,
    dirty: AtomicBool,
    durability: Durability,
    live_write_transaction: MutexGuard<'db, Option<TransactionId>>,
}

impl<'db> WriteTransaction<'db> {
    pub(crate) fn new(db: &'db Database) -> Result<Self> {
        let mut live_write_transaction = db.live_write_transaction.lock().unwrap();
        assert!(live_write_transaction.is_none());
        let transaction_id = db.increment_transaction_id();
        #[cfg(feature = "logging")]
        info!("Beginning write transaction id={:?}", transaction_id);
        *live_write_transaction = Some(transaction_id);

        let root_page = db.get_memory().get_data_root();
        let freed_root = db.get_memory().get_freed_root();
        let freed_pages = Arc::new(Mutex::new(vec![]));
        Ok(Self {
            db,
            transaction_tracker: db.transaction_tracker(),
            mem: db.get_memory(),
            transaction_id,
            table_tree: RwLock::new(TableTree::new(
                root_page,
                db.get_memory(),
                freed_pages.clone(),
            )),
            freed_tree: Mutex::new(BtreeMut::new(
                freed_root,
                db.get_memory(),
                freed_pages.clone(),
            )),
            freed_pages,
            open_tables: Mutex::new(Default::default()),
            completed: false,
            dirty: AtomicBool::new(false),
            durability: Durability::Immediate,
            live_write_transaction,
        })
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database
    ///
    /// Returns `[Error::InvalidSavepoint`], if the transaction is "dirty" (any tables have been openned)
    pub fn savepoint(&self) -> Result<Savepoint> {
        if self.dirty.load(Ordering::Acquire) {
            return Err(Error::InvalidSavepoint);
        }

        let (id, transaction_id) = self.db.allocate_savepoint()?;
        #[cfg(feature = "logging")]
        info!(
            "Creating savepoint id={:?}, txn_id={:?}",
            id, transaction_id
        );

        let regional_allocators = self.mem.get_raw_allocator_states();
        let root = self.mem.get_data_root();
        let freed_root = self.mem.get_freed_root();
        let savepoint = Savepoint::new(
            self.db,
            id,
            transaction_id,
            root,
            freed_root,
            regional_allocators,
        );

        Ok(savepoint)
    }

    /// Restore the state of the database to the given [`Savepoint`]
    ///
    /// Calling this method invalidates all [`Savepoint`]s created after savepoint
    pub fn restore_savepoint(&mut self, savepoint: &Savepoint) -> Result {
        // Ensure that user does not try to restore a Savepoint that is from a different Database
        assert_eq!(
            self.db.transaction_tracker().as_ref() as *const _,
            savepoint.db_address()
        );

        if !self
            .transaction_tracker
            .lock()
            .unwrap()
            .is_valid_savepoint(savepoint.get_id())
        {
            return Err(Error::InvalidSavepoint);
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
        assert_eq!(
            self.db.get_memory().checksum_type(),
            savepoint.get_checksum_type()
        );
        self.dirty.store(true, Ordering::Release);

        let allocated_since_savepoint = self
            .mem
            .pages_allocated_since_raw_state(savepoint.get_regional_allocator_states());

        let mut freed_pages = vec![];
        for page in allocated_since_savepoint {
            if self.mem.uncommitted(page) {
                self.mem.free(page);
            } else {
                freed_pages.push(page);
            }
        }
        *self.freed_pages.lock().unwrap() = freed_pages;
        self.table_tree = RwLock::new(TableTree::new(
            savepoint.get_root(),
            self.mem,
            self.freed_pages.clone(),
        ));

        // Remove any freed pages that have already been processed. Otherwise this would result in a double free
        // We assume below that PageNumber is length 8
        let oldest_unprocessed_transaction = if let Some(entry) = self
            .freed_tree
            .lock()
            .unwrap()
            .range::<RangeFull, FreedTableKey>(..)?
            .next()
        {
            entry.key().transaction_id
        } else {
            self.transaction_id.0
        };

        let mut freed_tree = BtreeMut::new(
            savepoint.get_freed_root(),
            self.mem,
            self.freed_pages.clone(),
        );
        let lookup_key = FreedTableKey {
            transaction_id: oldest_unprocessed_transaction,
            pagination_id: 0,
        };
        let mut to_remove = vec![];
        for entry in freed_tree.range(..lookup_key)? {
            to_remove.push(entry.key());
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

        Ok(())
    }

    /// Set the desired durability level for writes made in this transaction
    /// Defaults to [`Durability::Immediate`]
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    pub fn open_table<'txn, K: RedbKey + 'static, V: RedbValue + 'static>(
        &'txn self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'db, 'txn, K, V>> {
        #[cfg(feature = "logging")]
        info!("Opening table: {}", definition);
        if let Some(location) = self.open_tables.lock().unwrap().get(definition.name()) {
            return Err(Error::TableAlreadyOpen(
                definition.name().to_string(),
                location,
            ));
        }
        self.dirty.store(true, Ordering::Release);
        self.open_tables
            .lock()
            .unwrap()
            .insert(definition.name().to_string(), panic::Location::caller());

        let internal_table = self
            .table_tree
            .write()
            .unwrap()
            .get_or_create_table::<K, V>(definition.name(), TableType::Normal)?;

        Ok(Table::new(
            definition.name(),
            internal_table.get_root(),
            self.freed_pages.clone(),
            self.mem,
            self,
        ))
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    pub fn open_multimap_table<'txn, K: RedbKey + 'static, V: RedbKey + 'static>(
        &'txn self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'db, 'txn, K, V>> {
        #[cfg(feature = "logging")]
        info!("Opening multimap table: {}", definition);
        if let Some(location) = self.open_tables.lock().unwrap().get(definition.name()) {
            return Err(Error::TableAlreadyOpen(
                definition.name().to_string(),
                location,
            ));
        }
        self.dirty.store(true, Ordering::Release);
        self.open_tables
            .lock()
            .unwrap()
            .insert(definition.name().to_string(), panic::Location::caller());

        let internal_table = self
            .table_tree
            .write()
            .unwrap()
            .get_or_create_table::<K, V>(definition.name(), TableType::Multimap)?;

        Ok(MultimapTable::new(
            definition.name(),
            internal_table.get_root(),
            self.freed_pages.clone(),
            self.mem,
            self,
        ))
    }

    pub(crate) fn close_table<K: RedbKey + 'static, V: RedbValue + 'static>(
        &self,
        name: &str,
        table: &mut BtreeMut<K, V>,
    ) {
        self.open_tables.lock().unwrap().remove(name).unwrap();
        self.table_tree
            .write()
            .unwrap()
            .stage_update_table_root(name, table.get_root());
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table(&self, definition: impl TableHandle) -> Result<bool> {
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
    pub fn delete_multimap_table(&self, definition: impl MultimapTableHandle) -> Result<bool> {
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
    pub fn commit(mut self) -> Result {
        self.table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;
        self.commit_inner()
    }

    fn commit_inner(&mut self) -> Result {
        #[cfg(feature = "logging")]
        info!(
            "Committing transaction id={:?} with durability={:?}",
            self.transaction_id, self.durability
        );
        match self.durability {
            Durability::None => self.non_durable_commit()?,
            Durability::Eventual => self.durable_commit(true)?,
            Durability::Immediate => self.durable_commit(false)?,
        }

        self.completed = true;
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
        self.abort_inner()
    }

    fn abort_inner(&mut self) -> Result {
        #[cfg(feature = "logging")]
        info!("Aborting transaction id={:?}", self.transaction_id);
        self.table_tree.write().unwrap().clear_table_root_updates();
        self.mem.rollback_uncommitted_writes()?;
        self.completed = true;
        #[cfg(feature = "logging")]
        info!("Finished abort of transaction id={:?}", self.transaction_id);
        Ok(())
    }

    pub(crate) fn durable_commit(&mut self, eventual: bool) -> Result {
        let oldest_live_read = self
            .transaction_tracker
            .lock()
            .unwrap()
            .oldest_live_read_transaction()
            .unwrap_or(self.transaction_id);

        let root = self
            .table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;

        self.process_freed_pages(oldest_live_read)?;
        self.store_freed_pages()?;

        let freed_root = self.freed_tree.lock().unwrap().get_root();

        self.mem
            .commit(root, freed_root, self.transaction_id, eventual, None)?;
        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&mut self) -> Result {
        let root = self
            .table_tree
            .write()
            .unwrap()
            .flush_table_root_updates()?;

        // Store all freed pages for a future commit(), since we can't free pages during a
        // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
        self.store_freed_pages()?;

        let freed_root = self.freed_tree.lock().unwrap().get_root();

        self.mem
            .non_durable_commit(root, freed_root, self.transaction_id)?;
        Ok(())
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
        for entry in freed_tree.range(..lookup_key)? {
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

    fn store_freed_pages(&mut self) -> Result {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        let mut freed_tree = self.freed_tree.lock().unwrap();
        while !self.freed_pages.lock().unwrap().is_empty() {
            let chunk_size = 100;
            let buffer_size = FreedPageList::required_bytes(chunk_size);
            let key = FreedTableKey {
                transaction_id: self.transaction_id.0,
                pagination_id: pagination_counter,
            };
            let mut access_guard = freed_tree.insert_reserve(&key, buffer_size)?;

            let mut freed_pages = self.freed_pages.lock().unwrap();
            let len = freed_pages.len();
            access_guard.as_mut().clear();
            for page in freed_pages.drain(len - min(len, chunk_size)..) {
                access_guard.as_mut().push_back(page);
            }
            drop(access_guard);

            pagination_counter += 1;
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
    db: &'a Database,
    tree: TableTree<'a>,
    transaction_id: TransactionId,
}

impl<'db> ReadTransaction<'db> {
    pub(crate) fn new(db: &'db Database, transaction_id: TransactionId) -> Self {
        let root_page = db.get_memory().get_data_root();
        Self {
            db,
            tree: TableTree::new(root_page, db.get_memory(), Default::default()),
            transaction_id,
        }
    }

    /// Open the given table
    pub fn open_table<K: RedbKey + 'static, V: RedbValue + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?
            .ok_or_else(|| Error::TableDoesNotExist(definition.name().to_string()))?;

        ReadOnlyTable::new(header.get_root(), PageHint::Clean, self.db.get_memory())
    }

    /// Open the given table
    pub fn open_multimap_table<K: RedbKey + 'static, V: RedbKey + 'static>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<ReadOnlyMultimapTable<K, V>> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Multimap)?
            .ok_or_else(|| Error::TableDoesNotExist(definition.name().to_string()))?;

        ReadOnlyMultimapTable::new(header.get_root(), PageHint::Clean, self.db.get_memory())
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
        self.db
            .transaction_tracker()
            .lock()
            .unwrap()
            .deallocate_read_transaction(self.transaction_id);
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, TableDefinition};
    use tempfile::NamedTempFile;

    const X: TableDefinition<&str, &str> = TableDefinition::new("x");

    #[test]
    fn transaction_id_persistence() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
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
