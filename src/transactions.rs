use crate::db::TransactionId;
use crate::tree_store::{
    Btree, BtreeMut, FreedTableKey, InternalTableDefinition, PageNumber, TableTree, TableType,
    TransactionalMemory, FREED_TABLE,
};
use crate::types::{RedbKey, RedbValue};
use crate::{
    Database, Error, MultimapTable, MultimapTableDefinition, ReadOnlyMultimapTable, ReadOnlyTable,
    Result, Table, TableDefinition,
};
use std::cell::{Cell, RefCell};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::RangeFull;
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct DatabaseStats {
    tree_height: usize,
    free_pages: usize,
    stored_leaf_bytes: usize,
    metadata_bytes: usize,
    fragmented_bytes: usize,
    page_size: usize,
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

    /// Number of bytes consumed by keys and values that have been inserted.
    /// Does not include indexing overhead
    pub fn stored_bytes(&self) -> usize {
        self.stored_leaf_bytes
    }

    /// Number of bytes consumed by keys in internal index pages, plus other metadata
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
    root_page: Cell<Option<PageNumber>>,
    table_tree: RefCell<TableTree<'db>>,
    freed_pages: RefCell<Vec<PageNumber>>,
    open_tables: RefCell<HashMap<String, &'static panic::Location<'static>>>,
    pending_table_updates: RefCell<HashMap<String, Option<PageNumber>>>,
    completed: AtomicBool,
    durability: Durability,
}

impl<'db> WriteTransaction<'db> {
    // Safety: caller must guarantee that there is only a single WriteTransaction in existence
    // at a time
    pub(crate) unsafe fn new(db: &'db Database, transaction_id: TransactionId) -> Result<Self> {
        let root_page = db.get_memory().get_primary_root_page();
        Ok(Self {
            db,
            mem: db.get_memory(),
            transaction_id,
            root_page: Cell::new(root_page),
            table_tree: RefCell::new(TableTree::new(root_page, db.get_memory())),
            freed_pages: RefCell::new(vec![]),
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
    // TODO: simplify the number of lifetimes here. Also why is it different from multimap_table?
    pub fn open_table<'s: 't, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &'s self,
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
        self.freed_pages.borrow_mut().append(&mut table.freed_pages);
        self.pending_table_updates
            .borrow_mut()
            .insert(name.to_string(), table.root);
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

    pub fn commit(self) -> Result {
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

    fn commit_inner(&self) -> Result {
        self.root_page.set(self.table_tree.borrow().tree.root);

        match self.durability {
            Durability::None => self.non_durable_commit()?,
            Durability::Eventual => self.durable_commit(true)?,
            Durability::Immediate => self.durable_commit(false)?,
        }

        self.completed.store(true, Ordering::Release);

        Ok(())
    }

    pub fn abort(self) -> Result {
        // No-op, just to avoid triggering the leak detection in BtreeMut
        self.take_all_freed_pages();
        self.mem.rollback_uncommited_writes()?;
        self.db.deallocate_write_transaction(self.transaction_id);
        self.completed.store(true, Ordering::Release);
        Ok(())
    }

    // TODO: this method is kind of a hack
    fn take_all_freed_pages(&self) -> Vec<PageNumber> {
        let mut result = vec![];
        result.append(self.freed_pages.borrow_mut().as_mut());
        result.append(&mut self.table_tree.borrow_mut().extra_freed_pages);
        result.append(&mut self.table_tree.borrow_mut().tree.freed_pages);
        result
    }

    pub(crate) fn durable_commit(&self, eventual: bool) -> Result {
        let oldest_live_read = self
            .db
            .oldest_live_read_transaction()
            .unwrap_or(self.transaction_id);

        self.process_freed_pages(oldest_live_read)?;
        if oldest_live_read < self.transaction_id {
            self.store_freed_pages()?;
        } else {
            for page in self.take_all_freed_pages() {
                // Safety: The oldest live read started after this transactions, so it can't
                // have a references to this page, since we freed it in this transaction
                unsafe {
                    self.mem.free(page)?;
                }
            }
        }

        if let Some(root) = self.table_tree.borrow().tree.root {
            self.mem.set_secondary_root_page(root)?;
        } else {
            self.mem.set_secondary_root_page(PageNumber::null())?;
        }

        self.mem.commit(self.transaction_id, eventual)?;
        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&self) -> Result {
        // Store all freed pages for a future commit(), since we can't free pages during a
        // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
        self.store_freed_pages()?;

        if let Some(root) = self.table_tree.borrow().tree.root {
            self.mem.set_secondary_root_page(root)?;
        } else {
            self.mem.set_secondary_root_page(PageNumber::null())?;
        }

        self.mem.non_durable_commit(self.transaction_id)?;
        Ok(())
    }

    // NOTE: must be called before store_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(&self, oldest_live_read: TransactionId) -> Result {
        // We assume below that PageNumber is length 8
        assert_eq!(PageNumber::null().to_le_bytes().len(), 8);
        let lookup_key = FreedTableKey {
            transaction_id: oldest_live_read,
            pagination_id: 0,
        };

        let mut to_remove = vec![];
        let freed_table = self
            .table_tree
            .borrow()
            .get_table::<FreedTableKey, [u8]>(FREED_TABLE, TableType::Normal)?
            .unwrap();
        let mut freed_tree: BtreeMut<FreedTableKey, [u8]> =
            BtreeMut::new(freed_table.get_root(), self.mem);
        let mut iter = freed_tree.range(..lookup_key)?;
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

        // Remove all the old transactions. Note: this may create new pages that need to be freed
        for key in to_remove {
            // Safety: all references to the freed table above have already been dropped
            unsafe {
                freed_tree.remove(&key)?;
            }
        }
        self.table_tree
            .borrow_mut()
            .update_table_root(FREED_TABLE, freed_tree.root)?;
        self.freed_pages
            .borrow_mut()
            .append(&mut freed_tree.freed_pages);

        Ok(())
    }

    fn store_freed_pages(&self) -> Result {
        assert_eq!(PageNumber::null().to_le_bytes().len(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        let freed_table = self
            .table_tree
            .borrow()
            .get_table::<FreedTableKey, [u8]>(FREED_TABLE, TableType::Normal)?
            .unwrap();
        let mut freed_tree: BtreeMut<FreedTableKey, [u8]> =
            BtreeMut::new(freed_table.get_root(), self.mem);
        // TODO: change all master_tree usages to have TableHeader as their value type
        let mut freed_pages = self.take_all_freed_pages();
        while !freed_pages.is_empty() {
            let chunk_size = 100;
            let buffer_size = size_of::<u64>() + 8 * chunk_size;
            let key = FreedTableKey {
                transaction_id: self.transaction_id,
                pagination_id: pagination_counter,
            };
            // Safety: The freed table is only accessed from the writer, so only this function
            // is using it. The only reference retrieved, access_guard, is dropped before the next call
            // to this method
            let (mut access_guard, new_root, mut freed_pages2) =
                unsafe { freed_tree.insert_reserve_special(&key, buffer_size)? };
            freed_pages.append(&mut freed_pages2);

            if freed_pages.len() <= chunk_size {
                // Update the master root, only on the last loop iteration (this may cause another
                // iteration, but that's ok since it would have very few pages to process)
                self.table_tree
                    .borrow_mut()
                    .update_table_root(FREED_TABLE, new_root)?;
                freed_pages.append(&mut self.table_tree.borrow_mut().tree.freed_pages);
                freed_pages.append(&mut self.table_tree.borrow_mut().extra_freed_pages);
            }

            let len = freed_pages.len();
            access_guard.as_mut()[..8]
                .copy_from_slice(&min(len as u64, chunk_size as u64).to_le_bytes());
            for (i, page) in freed_pages.drain(len - min(len, chunk_size)..).enumerate() {
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
        let master_tree_height = table_tree.tree.height();
        let mut max_subtree_height = 0;
        let mut total_stored_bytes = 0;
        // Include the master table in the overhead
        let mut total_metadata_bytes =
            table_tree.tree.overhead_bytes() + table_tree.tree.stored_leaf_bytes();
        let mut total_fragmented = table_tree.tree.fragmented_bytes();

        let mut iter = table_tree.tree.range::<RangeFull, &str>(..)?;
        while let Some(entry) = iter.next() {
            let definition = InternalTableDefinition::from_bytes(entry.value());
            let subtree: Btree<[u8], [u8]> = Btree::new(definition.get_root(), self.mem);
            if std::str::from_utf8(entry.key()).unwrap() == FREED_TABLE {
                // Count the stored bytes of the freed table as metadata overhead
                total_metadata_bytes += subtree.stored_leaf_bytes();
                total_metadata_bytes += subtree.overhead_bytes();
                total_fragmented += subtree.fragmented_bytes();
            } else {
                max_subtree_height = max(max_subtree_height, subtree.height());
                total_stored_bytes += subtree.stored_leaf_bytes();
                total_metadata_bytes += subtree.overhead_bytes();
                total_fragmented += subtree.fragmented_bytes();
            }
        }
        Ok(DatabaseStats {
            tree_height: master_tree_height + max_subtree_height,
            free_pages: self.mem.count_free_pages()?,
            stored_leaf_bytes: total_stored_bytes,
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        if let Some(page) = self.root_page.get() {
            eprintln!("Master tree:");

            let master_tree: Btree<str, [u8]> = Btree::new(Some(page), self.mem);
            master_tree.print_debug();
            let mut iter = master_tree.range::<RangeFull, &str>(..).unwrap();

            while let Some(entry) = iter.next() {
                eprintln!("{} tree:", String::from_utf8_lossy(entry.key()));
                let definition = InternalTableDefinition::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    // Print as &[u8], since we don't know the types at compile time
                    let tree: Btree<[u8], [u8]> = Btree::new(Some(table_root), self.mem);
                    tree.print_debug();
                }
            }
        }
    }
}

impl<'a> Drop for WriteTransaction<'a> {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::Acquire) {
            self.db.record_leaked_write_transaction(self.transaction_id);
            // No-op, just to avoid triggering the leak detection in BtreeMut
            self.take_all_freed_pages();
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
        let root_page = db.get_memory().get_primary_root_page();
        Self {
            db,
            tree: TableTree::new(root_page, db.get_memory()),
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
