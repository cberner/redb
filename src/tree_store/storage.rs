use crate::tree_store::btree_base::{AccessGuard, InternalAccessor, LeafAccessor};
use crate::tree_store::btree_iters::{page_numbers_iter_start_state, AllPageNumbersBtreeIter};
use crate::tree_store::btree_utils::{
    find_key, fragmented_bytes, make_mut_single_leaf, overhead_bytes, print_tree, stored_bytes,
    tree_delete, tree_height, tree_insert,
};
use crate::tree_store::page_store::{Page, PageMut, PageNumber, TransactionalMemory};
use crate::tree_store::{btree_base, AccessGuardMut, BtreeRangeIter};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Error;
use std::borrow::Borrow;
use std::cmp::{max, min};
use std::collections::BTreeSet;
use std::convert::TryInto;
use std::fs::File;
use std::mem::size_of;
use std::ops::{RangeBounds, RangeFull};
use std::panic;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

// The table of freed pages by transaction. (transaction id, pagination counter) -> binary.
// The binary blob is a length-prefixed array of big endian PageNumber
pub(crate) const FREED_TABLE: &str = "$$internal$$freed";

pub(crate) type TransactionId = u64;
type AtomicTransactionId = AtomicU64;

#[derive(Debug)]
pub struct DatabaseStats {
    pub(crate) tree_height: usize,
    pub(crate) free_pages: usize,
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

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) enum TableType {
    Normal,
    Multimap,
}

#[allow(clippy::from_over_into)]
impl Into<u8> for TableType {
    fn into(self) -> u8 {
        match self {
            TableType::Normal => 1,
            TableType::Multimap => 2,
        }
    }
}

impl From<u8> for TableType {
    fn from(value: u8) -> Self {
        match value {
            1 => TableType::Normal,
            2 => TableType::Multimap,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TableHeader {
    table_root: Option<PageNumber>,
    table_type: TableType,
    key_type: String,
    value_type: String,
}

impl TableHeader {
    pub(crate) fn get_root(&self) -> Option<PageNumber> {
        self.table_root
    }

    pub(crate) fn get_type(&self) -> TableType {
        self.table_type
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = vec![self.table_type.into()];
        result.extend_from_slice(
            &self
                .table_root
                .unwrap_or_else(PageNumber::null)
                .to_be_bytes(),
        );
        result.extend_from_slice(&(self.key_type.as_bytes().len() as u32).to_be_bytes());
        result.extend_from_slice(self.key_type.as_bytes());
        result.extend_from_slice(self.value_type.as_bytes());

        result
    }

    fn from_bytes(value: &[u8]) -> Self {
        debug_assert!(value.len() > 14);
        let table_type = TableType::from(value[0]);
        let table_root = PageNumber::from_be_bytes(value[1..9].try_into().unwrap());
        let table_root = if table_root == PageNumber::null() {
            None
        } else {
            Some(table_root)
        };
        let key_type_len = u32::from_be_bytes(value[9..13].try_into().unwrap()) as usize;
        let key_type = std::str::from_utf8(&value[13..(13 + key_type_len)])
            .unwrap()
            .to_string();
        let value_type = std::str::from_utf8(&value[(13 + key_type_len)..])
            .unwrap()
            .to_string();

        TableHeader {
            table_root,
            table_type,
            key_type,
            value_type,
        }
    }
}

pub struct TableNameIter<'a> {
    inner: BtreeRangeIter<'a, str, [u8]>,
    table_type: TableType,
}

impl<'a> Iterator for TableNameIter<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.inner.next() {
            if str::from_bytes(entry.key()) == FREED_TABLE {
                continue;
            }
            if TableHeader::from_bytes(entry.value()).table_type == self.table_type {
                return Some(str::from_bytes(entry.key()).to_string());
            }
        }
        None
    }
}

// Returns the mutable value for the queried key, if present
// Safety: caller must ensure that not other references to the page storing the queried key exist
unsafe fn get_mut_value<'a, K: RedbKey + ?Sized>(
    page: PageMut<'a>,
    query: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<AccessGuardMut<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        btree_base::LEAF => {
            let accessor = LeafAccessor::new(&page);
            let entry_index = accessor.find_key::<K>(query)?;
            let (start, end) = accessor.value_range(entry_index).unwrap();
            let guard = AccessGuardMut::new(page, start, end - start);
            Some(guard)
        }
        btree_base::INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (_, child_page) = accessor.child_for_key::<K>(query);
            return get_mut_value::<K>(manager.get_page_mut(child_page), query, manager);
        }
        _ => unreachable!(),
    }
}

pub(crate) struct Storage {
    mem: TransactionalMemory,
    next_transaction_id: AtomicTransactionId,
    live_read_transactions: Mutex<BTreeSet<TransactionId>>,
    live_write_transaction: Mutex<Option<TransactionId>>,
    pending_freed_pages: Mutex<Vec<PageNumber>>,
    leaked_write_transaction: Mutex<Option<&'static panic::Location<'static>>>,
}

impl Storage {
    pub(crate) fn new(file: File, page_size: Option<usize>) -> Result<Storage, Error> {
        let mut mem = TransactionalMemory::new(file, page_size)?;
        if mem.needs_repair()? {
            let root = mem
                .get_primary_root_page()
                .expect("Tried to repair an empty database");
            // Clear the freed table. We're about to rebuild the allocator state by walking
            // all the reachable pages, which will implicitly free them

            // Safety: we own the mem object, and just created it, so there can't be other references.
            let mut bytes = unsafe {
                get_mut_value::<str>(mem.get_page_mut(root), FREED_TABLE.as_bytes(), &mem).unwrap()
            };
            let mut header = TableHeader::from_bytes(bytes.as_mut());
            header.table_root = None;
            assert_eq!(bytes.as_mut().len(), header.to_bytes().len());
            bytes.as_mut().copy_from_slice(&header.to_bytes());
        }
        while mem.needs_repair()? {
            let root = mem
                .get_primary_root_page()
                .expect("Tried to repair an empty database");
            let root_page = mem.get_page(root);
            let start = page_numbers_iter_start_state(root_page);

            // All pages in the master table
            let mut all_pages_iter: Box<dyn Iterator<Item = PageNumber>> =
                Box::new(AllPageNumbersBtreeIter::new(start, &mem));

            // Iterate over all other tables
            let mut iter: BtreeRangeIter<[u8], [u8]> =
                BtreeRangeIter::new::<RangeFull, [u8]>(.., Some(root), &mem);

            // Chain all the other tables to the master table iter
            while let Some(entry) = iter.next() {
                let definition = TableHeader::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    let page = mem.get_page(table_root);
                    let table_start = page_numbers_iter_start_state(page);
                    let table_pages_iter = AllPageNumbersBtreeIter::new(table_start, &mem);
                    all_pages_iter = Box::new(all_pages_iter.chain(table_pages_iter));
                }
            }

            mem.repair_allocator(all_pages_iter)?;
        }
        mem.finalize_repair_allocator()?;

        let mut next_transaction_id = mem.get_last_committed_transaction_id()? + 1;
        if mem.get_primary_root_page().is_none() {
            assert_eq!(next_transaction_id, 1);
            // Empty database, so insert the freed table.
            let freed_table = TableHeader {
                table_root: None,
                table_type: TableType::Normal,
                key_type: <[u8]>::redb_type_name().to_string(),
                value_type: <[u8]>::redb_type_name().to_string(),
            };
            let (new_root, _) =
                make_mut_single_leaf(FREED_TABLE.as_bytes(), &freed_table.to_bytes(), &mem)?;
            mem.set_secondary_root_page(new_root)?;
            mem.commit(next_transaction_id)?;
            next_transaction_id += 1;
        }

        Ok(Storage {
            mem,
            next_transaction_id: AtomicTransactionId::new(next_transaction_id),
            live_write_transaction: Mutex::new(None),
            live_read_transactions: Mutex::new(Default::default()),
            pending_freed_pages: Mutex::new(Default::default()),
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

    pub(crate) fn allocate_write_transaction(&self) -> Result<TransactionId, Error> {
        let guard = self.leaked_write_transaction.lock().unwrap();
        if let Some(leaked) = *guard {
            return Err(Error::LeakedWriteTransaction(leaked));
        }
        drop(guard);

        assert!(self.live_write_transaction.lock().unwrap().is_none());
        assert!(self.pending_freed_pages.lock().unwrap().is_empty());
        let id = self.next_transaction_id.fetch_add(1, Ordering::SeqCst);
        *self.live_write_transaction.lock().unwrap() = Some(id);
        Ok(id)
    }

    pub(crate) fn allocate_read_transaction(&self) -> TransactionId {
        let id = self.next_transaction_id.fetch_add(1, Ordering::SeqCst);
        self.live_read_transactions.lock().unwrap().insert(id);
        id
    }

    pub(crate) fn deallocate_read_transaction(&self, id: TransactionId) {
        self.live_read_transactions.lock().unwrap().remove(&id);
    }

    pub(crate) fn update_table_root(
        &self,
        name: &str,
        table_root: Option<PageNumber>,
        transaction_id: TransactionId,
        master_root: Option<PageNumber>,
    ) -> Result<PageNumber, Error> {
        // Bypass .get_table() since the table types are dynamic
        // TODO: optimize way this get()
        let bytes = self
            .get::<str, [u8]>(name.as_bytes(), master_root)
            .unwrap()
            .unwrap();
        let mut definition = TableHeader::from_bytes(bytes);
        definition.table_root = table_root;
        // Safety: References into the master table are never returned to the user
        unsafe {
            self.insert::<str>(
                name.as_bytes(),
                &definition.to_bytes(),
                transaction_id,
                master_root,
            )
        }
    }

    // root_page: the root of the master table
    pub(crate) fn list_tables(
        &self,
        table_type: TableType,
        master_root_page: Option<PageNumber>,
    ) -> Result<TableNameIter, Error> {
        let iter = self.get_range::<RangeFull, str, str, [u8]>(.., master_root_page)?;
        Ok(TableNameIter {
            inner: iter,
            table_type,
        })
    }

    // root_page: the root of the master table
    pub(crate) fn get_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: &str,
        table_type: TableType,
        root_page: Option<PageNumber>,
    ) -> Result<Option<TableHeader>, Error> {
        if let Some(found) = self.get::<str, [u8]>(name.as_bytes(), root_page)? {
            let definition = TableHeader::from_bytes(found);
            if definition.get_type() != table_type {
                return Err(Error::TableTypeMismatch(format!(
                    "{:?} is not of type {:?}",
                    name, table_type
                )));
            }
            if definition.key_type != K::redb_type_name()
                || definition.value_type != V::redb_type_name()
            {
                return Err(Error::TableTypeMismatch(format!(
                    "{} is of type Table<{}, {}> not Table<{}, {}>",
                    name,
                    &definition.key_type,
                    &definition.value_type,
                    K::redb_type_name(),
                    V::redb_type_name()
                )));
            }

            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    // root_page: the root of the master table
    pub(crate) fn delete_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: &str,
        table_type: TableType,
        transaction_id: TransactionId,
        root_page: Option<PageNumber>,
    ) -> Result<(Option<PageNumber>, bool), Error> {
        if let Some(definition) = self.get_table::<K, V>(name, table_type, root_page)? {
            if let Some(table_root) = definition.get_root() {
                let page = self.mem.get_page(table_root);
                let start = page_numbers_iter_start_state(page);
                let iter = AllPageNumbersBtreeIter::new(start, &self.mem);
                let mut guard = self.pending_freed_pages.lock().unwrap();
                for page_number in iter {
                    guard.push(page_number);
                }
            }

            // Safety: References into the master table are never returned to the user
            let (new_root, found) = unsafe {
                self.remove::<str, [u8]>(name.as_bytes(), transaction_id, true, root_page)?
            };
            return Ok((new_root, found.is_some()));
        }

        Ok((root_page, false))
    }

    // Returns a tuple of the table id and the new root page
    // root_page: the root of the master table
    pub(crate) fn get_or_create_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: &str,
        table_type: TableType,
        transaction_id: TransactionId,
        root_page: Option<PageNumber>,
    ) -> Result<(TableHeader, PageNumber), Error> {
        if let Some(found) = self.get_table::<K, V>(name, table_type, root_page)? {
            return Ok((found, root_page.unwrap()));
        }

        let header = TableHeader {
            table_root: None,
            table_type,
            key_type: K::redb_type_name().to_string(),
            value_type: V::redb_type_name().to_string(),
        };
        // Safety: References into the master table are never returned to the user
        let new_root = unsafe {
            self.insert::<str>(
                name.as_bytes(),
                &header.to_bytes(),
                transaction_id,
                root_page,
            )?
        };
        Ok((header, new_root))
    }

    // Returns the new root page number
    // Safety: caller must ensure that no references to uncommitted data in this transaction exist
    // TODO: this method could be made safe, if the transaction_id was not copy and was borrowed mut
    pub(crate) unsafe fn insert<K: RedbKey + ?Sized>(
        &self,
        key: &[u8],
        value: &[u8],
        transaction_id: TransactionId,
        root_page: Option<PageNumber>,
    ) -> Result<PageNumber, Error> {
        assert_eq!(
            transaction_id,
            self.live_write_transaction.lock().unwrap().unwrap()
        );
        if let Some(handle) = root_page {
            let root = self.mem.get_page(handle);
            let (new_root, _, freed) = tree_insert::<K>(root, key, value, &self.mem)?;
            self.pending_freed_pages
                .lock()
                .unwrap()
                .extend_from_slice(&freed);
            Ok(new_root)
        } else {
            let (new_root, _) = make_mut_single_leaf(key, value, &self.mem)?;
            Ok(new_root)
        }
    }

    // Returns the new root page number, and accessor for writing the value
    // Safety: caller must ensure that no references to uncommitted data in this transaction exist
    // TODO: this method could be made safe, if the transaction_id was not copy and was borrowed mut
    pub(crate) unsafe fn insert_reserve<K: RedbKey + ?Sized>(
        &self,
        key: &[u8],
        value_len: usize,
        transaction_id: TransactionId,
        root_page: Option<PageNumber>,
    ) -> Result<(PageNumber, AccessGuardMut), Error> {
        assert_eq!(
            transaction_id,
            self.live_write_transaction.lock().unwrap().unwrap()
        );
        let value = vec![0u8; value_len];
        let (new_root, guard) = if let Some(handle) = root_page {
            let root = self.mem.get_page(handle);
            let (new_root, guard, freed) = tree_insert::<K>(root, key, &value, &self.mem)?;
            self.pending_freed_pages
                .lock()
                .unwrap()
                .extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key, &value, &self.mem)?
        };
        Ok((new_root, guard))
    }

    pub(crate) fn len(&self, root_page: Option<PageNumber>) -> Result<usize, Error> {
        let mut iter: BtreeRangeIter<[u8], [u8]> =
            self.get_range::<RangeFull, [u8], [u8], [u8]>(.., root_page)?;
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub(crate) fn get_root_page_number(&self) -> Option<PageNumber> {
        self.mem.get_primary_root_page()
    }

    pub(crate) fn commit(
        &self,
        mut new_master_root: Option<PageNumber>,
        transaction_id: TransactionId,
    ) -> Result<(), Error> {
        let oldest_live_read = self
            .live_read_transactions
            .lock()
            .unwrap()
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| self.next_transaction_id.load(Ordering::SeqCst));

        new_master_root =
            self.process_freed_pages(oldest_live_read, transaction_id, new_master_root)?;
        if oldest_live_read < transaction_id {
            new_master_root = self.store_freed_pages(transaction_id, new_master_root)?;
        } else {
            for page in self.pending_freed_pages.lock().unwrap().drain(..) {
                // Safety: The oldest live read started after this transactions, so it can't
                // have a references to this page, since we freed it in this transaction
                unsafe {
                    self.mem.free(page)?;
                }
            }
        }

        if let Some(root) = new_master_root {
            self.mem.set_secondary_root_page(root)?;
        } else {
            self.mem.set_secondary_root_page(PageNumber::null())?;
        }

        self.mem.commit(transaction_id)?;
        assert_eq!(
            Some(transaction_id),
            self.live_write_transaction.lock().unwrap().take()
        );
        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(
        &self,
        mut new_master_root: Option<PageNumber>,
        transaction_id: TransactionId,
    ) -> Result<(), Error> {
        // Store all freed pages for a future commit(), since we can't free pages during a
        // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
        new_master_root = self.store_freed_pages(transaction_id, new_master_root)?;

        if let Some(root) = new_master_root {
            self.mem.set_secondary_root_page(root)?;
        } else {
            self.mem.set_secondary_root_page(PageNumber::null())?;
        }

        self.mem.non_durable_commit(transaction_id)?;
        assert_eq!(
            Some(transaction_id),
            self.live_write_transaction.lock().unwrap().take()
        );
        Ok(())
    }

    // NOTE: must be called before store_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(
        &self,
        oldest_live_read: TransactionId,
        transaction_id: TransactionId,
        mut master_root: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        assert_eq!(
            transaction_id,
            self.live_write_transaction.lock().unwrap().unwrap()
        );
        assert_eq!(PageNumber::null().to_be_bytes().len(), 8); // We assume below that PageNumber is length 8
        let mut lookup_key = [0u8; size_of::<TransactionId>() + size_of::<u64>()]; // (oldest_live_read, 0)
        lookup_key[0..size_of::<TransactionId>()].copy_from_slice(&oldest_live_read.to_be_bytes());
        // second element of pair is already zero

        let mut to_remove = vec![];
        let mut freed_table = self
            .get_table::<[u8], [u8]>(FREED_TABLE, TableType::Normal, master_root)?
            .unwrap();
        #[allow(clippy::type_complexity)]
        let mut iter: BtreeRangeIter<'_, [u8], [u8]> =
            self.get_range(..lookup_key.as_ref(), freed_table.get_root())?;
        while let Some(entry) = iter.next() {
            to_remove.push(entry.key().to_vec());
            let value = entry.value();
            let length = u64::from_be_bytes(value[..size_of::<u64>()].try_into().unwrap()) as usize;
            // 1..=length because the array is length prefixed
            for i in 1..=length {
                let page = PageNumber::from_be_bytes(value[i * 8..(i + 1) * 8].try_into().unwrap());
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
            let (new_root, _) = unsafe {
                self.remove::<[u8], [u8]>(&key, transaction_id, true, freed_table.table_root)?
            };
            freed_table.table_root = new_root;
        }
        // Safety: References into the master table are never returned to the user
        unsafe {
            master_root = Some(self.insert::<str>(
                FREED_TABLE.as_bytes(),
                &freed_table.to_bytes(),
                transaction_id,
                master_root,
            )?);
        }

        Ok(master_root)
    }

    fn store_freed_pages(
        &self,
        transaction_id: TransactionId,
        mut master_root: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        assert_eq!(
            transaction_id,
            self.live_write_transaction.lock().unwrap().unwrap()
        );
        assert_eq!(PageNumber::null().to_be_bytes().len(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        let mut freed_table = self
            .get_table::<[u8], [u8]>(FREED_TABLE, TableType::Normal, master_root)?
            .unwrap();
        while !self.pending_freed_pages.lock().unwrap().is_empty() {
            let chunk_size = 100;
            let buffer_size = size_of::<u64>() + 8 * chunk_size;
            let mut key = [0u8; size_of::<TransactionId>() + size_of::<u64>()];
            key[0..size_of::<TransactionId>()].copy_from_slice(&transaction_id.to_be_bytes());
            key[size_of::<u64>()..].copy_from_slice(&pagination_counter.to_be_bytes());
            // Safety: The freed table is only accessed from the writer, so only this function
            // is using it. The only reference retrieved, access_guard, is dropped before the next call
            // to this method
            let (r, mut access_guard) = unsafe {
                self.insert_reserve::<[u8]>(
                    key.as_ref(),
                    buffer_size,
                    transaction_id,
                    freed_table.table_root,
                )?
            };
            freed_table.table_root = Some(r);

            if self.pending_freed_pages.lock().unwrap().len() <= chunk_size {
                // Update the master root, only on the last loop iteration (this may cause another
                // iteration, but that's ok since it would have very few pages to process)
                // Safety: References into the master table are never returned to the user
                unsafe {
                    master_root = Some(self.insert::<str>(
                        FREED_TABLE.as_bytes(),
                        &freed_table.to_bytes(),
                        transaction_id,
                        master_root,
                    )?);
                }
            }

            let len = self.pending_freed_pages.lock().unwrap().len();
            access_guard.as_mut()[..8]
                .copy_from_slice(&min(len as u64, chunk_size as u64).to_be_bytes());
            for (i, page) in self
                .pending_freed_pages
                .lock()
                .unwrap()
                .drain(len - min(len, chunk_size)..)
                .enumerate()
            {
                access_guard.as_mut()[(i + 1) * 8..(i + 2) * 8]
                    .copy_from_slice(&page.to_be_bytes());
            }
            drop(access_guard);

            pagination_counter += 1;
        }

        Ok(master_root)
    }

    pub(crate) fn rollback_uncommited_writes(
        &self,
        transaction_id: TransactionId,
    ) -> Result<(), Error> {
        self.pending_freed_pages.lock().unwrap().clear();
        let result = self.mem.rollback_uncommited_writes();
        assert_eq!(
            Some(transaction_id),
            self.live_write_transaction.lock().unwrap().take()
        );

        result
    }

    pub(crate) fn storage_stats(&self) -> Result<DatabaseStats, Error> {
        let master_tree_height = self
            .get_root_page_number()
            .map(|p| tree_height(self.mem.get_page(p), &self.mem))
            .unwrap_or(0);
        let mut max_subtree_height = 0;
        let mut total_stored_bytes = 0;
        // Include the master table in the overhead
        let mut total_metadata_bytes = self
            .get_root_page_number()
            .map(|p| {
                overhead_bytes(self.mem.get_page(p), &self.mem)
                    + stored_bytes(self.mem.get_page(p), &self.mem)
            })
            .unwrap_or(0);
        let mut total_fragmented = self
            .get_root_page_number()
            .map(|p| fragmented_bytes(self.mem.get_page(p), &self.mem))
            .unwrap_or(0);
        let mut iter: BtreeRangeIter<[u8], [u8]> =
            self.get_range::<RangeFull, [u8], [u8], [u8]>(.., self.get_root_page_number())?;
        while let Some(entry) = iter.next() {
            let definition = TableHeader::from_bytes(entry.value());
            if let Some(table_root) = definition.get_root() {
                if std::str::from_utf8(entry.key()).unwrap() == FREED_TABLE {
                    // Count the stored bytes of the freed table as metadata overhead
                    total_metadata_bytes += stored_bytes(self.mem.get_page(table_root), &self.mem);
                    total_metadata_bytes +=
                        overhead_bytes(self.mem.get_page(table_root), &self.mem);
                    total_fragmented += fragmented_bytes(self.mem.get_page(table_root), &self.mem);
                } else {
                    let height = tree_height(self.mem.get_page(table_root), &self.mem);
                    max_subtree_height = max(max_subtree_height, height);
                    total_stored_bytes += stored_bytes(self.mem.get_page(table_root), &self.mem);
                    total_metadata_bytes +=
                        overhead_bytes(self.mem.get_page(table_root), &self.mem);
                    total_fragmented += fragmented_bytes(self.mem.get_page(table_root), &self.mem);
                }
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
        if let Some(page) = self.get_root_page_number() {
            eprintln!("Master tree:");
            print_tree(self.mem.get_page(page), &self.mem);

            let mut iter: BtreeRangeIter<[u8], [u8]> = self
                .get_range::<RangeFull, [u8], [u8], [u8]>(.., Some(page))
                .unwrap();

            while let Some(entry) = iter.next() {
                eprintln!("{} tree:", String::from_utf8_lossy(entry.key()));
                let definition = TableHeader::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    self.print_dirty_tree_debug(table_root);
                }
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_dirty_tree_debug(&self, root_page: PageNumber) {
        print_tree(self.mem.get_page(root_page), &self.mem);
    }

    pub(crate) fn get<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        key: &[u8],
        root_page_handle: Option<PageNumber>,
    ) -> Result<Option<<<V as RedbValue>::View as WithLifetime>::Out>, Error> {
        if let Some(handle) = root_page_handle {
            let root_page = self.mem.get_page(handle);
            return Ok(find_key::<K, V>(root_page, key, &self.mem));
        }
        Ok(None)
    }

    pub(crate) fn get_range<
        'a,
        T: RangeBounds<KR>,
        KR: Borrow<K> + ?Sized + 'a,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    >(
        &'a self,
        range: T,
        root_page: Option<PageNumber>,
    ) -> Result<BtreeRangeIter<K, V>, Error> {
        Ok(BtreeRangeIter::new(range, root_page, &self.mem))
    }

    // Returns the new root page, and a bool indicating whether the entry existed
    // Safety: caller must ensure that no references to uncommitted data in this transaction exist,
    // if free_uncommitted = true
    // TODO: this method could be made safe, if the transaction_id was not copy and was borrowed mut
    pub(crate) unsafe fn remove<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        key: &[u8],
        transaction_id: TransactionId,
        free_uncommitted: bool,
        root_handle: Option<PageNumber>,
    ) -> Result<(Option<PageNumber>, Option<AccessGuard<V>>), Error> {
        assert_eq!(
            transaction_id,
            self.live_write_transaction.lock().unwrap().unwrap()
        );
        if let Some(handle) = root_handle {
            let root_page = self.mem.get_page(handle);
            let (new_root, found, freed) =
                tree_delete::<K, V>(root_page, key, free_uncommitted, &self.mem)?;
            self.pending_freed_pages
                .lock()
                .unwrap()
                .extend_from_slice(&freed);
            return Ok((new_root, found));
        }
        Ok((root_handle, None))
    }
}
