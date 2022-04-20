use crate::db::TransactionId;
use crate::tree_store::{
    Btree, BtreeMut, FreedTableKey, InternalTableDefinition, PageNumber, TableTree, TableType,
    TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue};
use crate::{
    Database, Error, MultimapTable, MultimapTableDefinition, ReadOnlyMultimapTable, ReadOnlyTable,
    Result, Table, TableDefinition,
};
use std::cell::RefCell;
use std::cmp::min;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::RangeFull;
use std::panic;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct DatabaseStats {
    pub(crate) tree_height: usize,
    pub(crate) free_pages: usize,
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

    /// Number of free pages remaining
    pub fn free_pages(&self) -> usize {
        self.free_pages
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

pub struct WriteTransaction<'db> {
    db: &'db Database,
    mem: &'db TransactionalMemory,
    transaction_id: TransactionId,
    table_tree: RefCell<TableTree<'db>>,
    // TODO: change the value type to Vec<PageNumber>
    // The table of freed pages by transaction. FreedTableKey -> binary.
    // The binary blob is a length-prefixed array of PageNumber
    freed_tree: BtreeMut<'db, FreedTableKey, [u8]>,
    freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    open_tables: RefCell<HashMap<String, &'static panic::Location<'static>>>,
    pending_table_updates: RefCell<HashMap<String, Option<PageNumber>>>,
    completed: AtomicBool,
    durability: Durability,
}

impl<'db> WriteTransaction<'db> {
    // Safety: caller must guarantee that there is only a single WriteTransaction in existence
    // at a time
    pub(crate) unsafe fn new(db: &'db Database, transaction_id: TransactionId) -> Result<Self> {
        let root_page = db.get_memory().get_data_root();
        let freed_root = db.get_memory().get_freed_root();
        let freed_pages = Rc::new(RefCell::new(vec![]));
        Ok(Self {
            db,
            mem: db.get_memory(),
            transaction_id,
            table_tree: RefCell::new(TableTree::new(
                root_page,
                db.get_memory(),
                freed_pages.clone(),
            )),
            freed_tree: BtreeMut::new(freed_root, db.get_memory(), freed_pages.clone()),
            freed_pages,
            open_tables: RefCell::new(Default::default()),
            pending_table_updates: RefCell::new(Default::default()),
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
    pub fn open_table<'t, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &'t self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'db, 't, K, V>> {
        if let Some(location) = self.open_tables.borrow().get(definition.name()) {
            return Err(Error::TableAlreadyOpen(
                definition.name().to_string(),
                location,
            ));
        }
        self.open_tables
            .borrow_mut()
            .insert(definition.name().to_string(), panic::Location::caller());

        let internal_table = self
            .table_tree
            .borrow_mut()
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
    pub fn open_multimap_table<'t, K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &'t self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'db, 't, K, V>> {
        if let Some(location) = self.open_tables.borrow().get(definition.name()) {
            return Err(Error::TableAlreadyOpen(
                definition.name().to_string(),
                location,
            ));
        }
        self.open_tables
            .borrow_mut()
            .insert(definition.name().to_string(), panic::Location::caller());

        let internal_table = self
            .table_tree
            .borrow_mut()
            .get_or_create_table::<K, V>(definition.name(), TableType::Multimap)?;

        Ok(MultimapTable::new(
            definition.name(),
            internal_table.get_root(),
            self.freed_pages.clone(),
            self.mem,
            self,
        ))
    }

    pub(crate) fn close_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: &str,
        table: &mut BtreeMut<K, V>,
    ) {
        self.open_tables.borrow_mut().remove(name).unwrap();
        self.pending_table_updates
            .borrow_mut()
            .insert(name.to_string(), table.get_root());
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<bool> {
        self.table_tree
            .borrow_mut()
            .delete_table::<K, V>(definition.name(), TableType::Normal)
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_multimap_table<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<bool> {
        self.table_tree
            .borrow_mut()
            .delete_table::<K, V>(definition.name(), TableType::Multimap)
    }

    /// List all the tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_tables(&self) -> Result<impl Iterator<Item = String> + '_> {
        self.table_tree
            .borrow()
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter())
    }

    /// List all the multimap tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = String> + '_> {
        self.table_tree
            .borrow()
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter())
    }

    pub fn commit(mut self) -> Result {
        for (name, root) in self.pending_table_updates.borrow_mut().drain() {
            self.table_tree
                .borrow_mut()
                .update_table_root(&name, root)?;
        }
        match self.commit_inner() {
            Ok(_) => {
                self.db.deallocate_write_transaction(self.transaction_id);
                Ok(())
            }
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

    fn commit_inner(&mut self) -> Result {
        match self.durability {
            Durability::None => self.non_durable_commit()?,
            Durability::Eventual => self.durable_commit(true)?,
            Durability::Immediate => self.durable_commit(false)?,
        }

        self.completed.store(true, Ordering::Release);

        Ok(())
    }

    pub fn abort(self) -> Result {
        self.mem.rollback_uncommited_writes()?;
        self.db.deallocate_write_transaction(self.transaction_id);
        self.completed.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn durable_commit(&mut self, eventual: bool) -> Result {
        let oldest_live_read = self
            .db
            .oldest_live_read_transaction()
            .unwrap_or(self.transaction_id);

        self.process_freed_pages(oldest_live_read)?;
        if oldest_live_read < self.transaction_id {
            self.store_freed_pages()?;
        } else {
            for page in self.freed_pages.borrow_mut().drain(..) {
                // Safety: The oldest live read started after this transactions, so it can't
                // have a references to this page, since we freed it in this transaction
                unsafe {
                    self.mem.free(page)?;
                }
            }
        }

        let root = self.table_tree.borrow().get_root();
        let freed_root = self.freed_tree.get_root();

        self.mem
            .commit(root, freed_root, self.transaction_id, eventual)?;
        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&mut self) -> Result {
        // Store all freed pages for a future commit(), since we can't free pages during a
        // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
        self.store_freed_pages()?;

        let root = self.table_tree.borrow().get_root();
        let freed_root = self.freed_tree.get_root();

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
            transaction_id: oldest_live_read,
            pagination_id: 0,
        };

        let mut to_remove = vec![];
        let mut iter = self.freed_tree.range(..lookup_key)?;
        while let Some(entry) = iter.next() {
            to_remove.push(FreedTableKey::from_bytes(entry.key()));
            let value = entry.value();
            let length = u64::from_le_bytes(value[..size_of::<u64>()].try_into().unwrap()) as usize;
            // 1..=length because the array is length prefixed
            for i in 1..=length {
                let page = PageNumber::from_le_bytes(value[i * 8..(i + 1) * 8].try_into().unwrap());
                // Safety: we free only pages that were marked to be freed before the oldest live transaction,
                // therefore no one can have a reference to this page still
                unsafe {
                    self.mem.free(page)?;
                }
            }
        }
        drop(iter);

        // Remove all the old transactions
        for key in to_remove {
            // Safety: all references to the freed table above have already been dropped.
            unsafe { self.freed_tree.remove(&key)? };
        }

        Ok(())
    }

    fn store_freed_pages(&mut self) -> Result {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        while !self.freed_pages.borrow().is_empty() {
            let chunk_size = 100;
            let buffer_size = size_of::<u64>() + 8 * chunk_size;
            let key = FreedTableKey {
                transaction_id: self.transaction_id,
                pagination_id: pagination_counter,
            };
            // Safety: The freed table is only accessed from the writer, so only this function
            // is using it. The only reference retrieved, access_guard, is dropped before the next call
            // to this method
            let mut access_guard = unsafe { self.freed_tree.insert_reserve(&key, buffer_size)? };

            let len = self.freed_pages.borrow().len();
            access_guard.as_mut()[..8]
                .copy_from_slice(&min(len as u64, chunk_size as u64).to_le_bytes());
            for (i, page) in self
                .freed_pages
                .borrow_mut()
                .drain(len - min(len, chunk_size)..)
                .enumerate()
            {
                access_guard.as_mut()[(i + 1) * 8..(i + 2) * 8]
                    .copy_from_slice(&page.to_le_bytes());
            }
            drop(access_guard);

            pagination_counter += 1;
        }

        Ok(())
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        let table_tree = self.table_tree.borrow();
        let data_tree_stats = table_tree.stats()?;
        let total_metadata_bytes = data_tree_stats.metadata_bytes()
            + self.freed_tree.overhead_bytes()
            + self.freed_tree.stored_leaf_bytes();
        let total_fragmented =
            data_tree_stats.fragmented_bytes() + self.freed_tree.fragmented_bytes();

        Ok(DatabaseStats {
            tree_height: data_tree_stats.tree_height(),
            free_pages: self.mem.count_free_pages()?,
            leaf_pages: data_tree_stats.leaf_pages(),
            branch_pages: data_tree_stats.branch_pages(),
            stored_leaf_bytes: data_tree_stats.stored_bytes(),
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        if let Some(page) = self.table_tree.borrow().get_root() {
            eprintln!("Master tree:");

            let master_tree: Btree<str, InternalTableDefinition> = Btree::new(Some(page), self.mem);
            master_tree.print_debug(true);
            let mut iter = master_tree.range::<RangeFull, &str>(..).unwrap();

            while let Some(entry) = iter.next() {
                eprintln!("{} tree:", String::from_utf8_lossy(entry.key()));
                let definition = InternalTableDefinition::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    // Print as &[u8], since we don't know the types at compile time
                    let tree: Btree<[u8], [u8]> = Btree::new(Some(table_root), self.mem);
                    tree.print_debug(false);
                }
            }
        }
    }
}

impl<'a> Drop for WriteTransaction<'a> {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::Acquire) {
            self.db.record_leaked_write_transaction(self.transaction_id);
        }
    }
}

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
    pub fn open_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?
            .ok_or_else(|| Error::TableDoesNotExist(definition.name().to_string()))?;

        Ok(ReadOnlyTable::new(header.get_root(), self.db.get_memory()))
    }

    /// Open the given table
    pub fn open_multimap_table<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<ReadOnlyMultimapTable<K, V>> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Multimap)?
            .ok_or_else(|| Error::TableDoesNotExist(definition.name().to_string()))?;

        Ok(ReadOnlyMultimapTable::new(
            header.get_root(),
            self.db.get_memory(),
        ))
    }

    /// List all the tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_tables(&self) -> Result<impl Iterator<Item = String>> {
        self.tree
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter())
    }

    /// List all the multimap tables
    // TODO: should return an iterator of &str, once GATs are available
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = String>> {
        self.tree
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter())
    }
}

impl<'a> Drop for ReadTransaction<'a> {
    fn drop(&mut self) {
        self.db.deallocate_read_transaction(self.transaction_id);
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
