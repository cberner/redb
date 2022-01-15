use crate::tree_store::btree_utils::{
    find_iter_start, find_iter_start_reversed, find_iter_unbounded_reversed,
    find_iter_unbounded_start, lookup_in_raw, make_mut_single_leaf, page_numbers_iter_start_state,
    print_tree, tree_delete, tree_height, tree_insert, AccessGuardMut, AllPageNumbersBtreeIter,
    BtreeEntry, BtreeRangeIter,
};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Error;
use memmap2::MmapRaw;
use std::cell::{Cell, RefCell};
use std::cmp::{max, min};
use std::collections::{BTreeSet, Bound};
use std::convert::TryInto;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{RangeBounds, RangeFull, RangeTo};
use std::panic;

// The table of freed pages by transaction. (transaction id, pagination counter) -> binary.
// The binary blob is a length-prefixed array of big endian PageNumber
// TODO: prevent user from opening and modifying this table
const FREED_TABLE: &[u8] = b"$$internal$$freed";

#[derive(Debug)]
pub struct DbStats {
    tree_height: usize,
    free_pages: usize,
}

impl DbStats {
    fn new(tree_height: usize, free_pages: usize) -> Self {
        DbStats {
            tree_height,
            free_pages,
        }
    }

    pub fn tree_height(&self) -> usize {
        self.tree_height
    }

    pub fn free_pages(&self) -> usize {
        self.free_pages
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
pub(crate) struct TableDefinition {
    table_root: Option<PageNumber>,
    table_type: TableType,
}

impl TableDefinition {
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

        result
    }

    fn from_bytes(value: &[u8]) -> Self {
        assert_eq!(9, value.len());
        let table_root = PageNumber::from_be_bytes(value[1..9].try_into().unwrap());
        let table_root = if table_root == PageNumber::null() {
            None
        } else {
            Some(table_root)
        };
        let table_type = TableType::from(value[0]);

        TableDefinition {
            table_root,
            table_type,
        }
    }
}

pub(in crate) struct Storage {
    mem: TransactionalMemory,
    next_transaction_id: Cell<u128>,
    live_read_transactions: RefCell<BTreeSet<u128>>,
    live_write_transaction: Cell<Option<u128>>,
    pending_freed_pages: RefCell<Vec<PageNumber>>,
    leaked_write_transaction: Cell<Option<&'static panic::Location<'static>>>,
}

impl Storage {
    pub(in crate) fn new(mmap: MmapRaw, page_size: Option<usize>) -> Result<Storage, Error> {
        let mut mem = TransactionalMemory::new(mmap, page_size)?;
        while mem.needs_repair()? {
            let root = mem
                .get_primary_root_page()
                .expect("Tried to repair an empty database");
            let root_page = mem.get_page(root);
            let start = page_numbers_iter_start_state(root_page);
            let mut all_pages_iter: Box<dyn Iterator<Item = PageNumber>> =
                Box::new(AllPageNumbersBtreeIter::new(start, &mem));

            let start_state = find_iter_unbounded_start(mem.get_page(root), None, &mem);
            let mut iter: BtreeRangeIter<RangeFull, [u8], [u8], [u8]> =
                BtreeRangeIter::new(start_state, .., &mem);

            while let Some(entry) = iter.next() {
                let definition = TableDefinition::from_bytes(entry.value());
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
        // TODO: make this detection more reliable
        if mem.get_primary_root_page().is_none() {
            // Empty database, so insert the freed table.
            let freed_table = TableDefinition {
                table_root: None,
                table_type: TableType::Normal,
            };
            let (new_root, _) = make_mut_single_leaf(FREED_TABLE, &freed_table.to_bytes(), &mem)?;
            mem.set_secondary_root_page(new_root)?;
            mem.commit(next_transaction_id)?;
            next_transaction_id += 1;
        }

        Ok(Storage {
            mem,
            next_transaction_id: Cell::new(next_transaction_id),
            live_write_transaction: Cell::new(None),
            live_read_transactions: RefCell::new(Default::default()),
            pending_freed_pages: RefCell::new(Default::default()),
            leaked_write_transaction: Cell::new(Default::default()),
        })
    }

    pub(crate) fn record_leaked_write_transaction(&self, transaction_id: u128) {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        self.leaked_write_transaction
            .set(Some(panic::Location::caller()));
    }

    pub(crate) fn allocate_write_transaction(&self) -> Result<u128, Error> {
        if let Some(leaked) = self.leaked_write_transaction.get() {
            return Err(Error::LeakedWriteTransaction(leaked));
        }

        assert!(self.live_write_transaction.get().is_none());
        assert!(self.pending_freed_pages.borrow().is_empty());
        let id = self.next_transaction_id.get();
        self.live_write_transaction.set(Some(id));
        self.next_transaction_id.set(id + 1);
        Ok(id)
    }

    pub(crate) fn allocate_read_transaction(&self) -> u128 {
        let id = self.next_transaction_id.get();
        self.live_read_transactions.borrow_mut().insert(id);
        self.next_transaction_id.set(id + 1);
        id
    }

    pub(crate) fn deallocate_read_transaction(&self, id: u128) {
        self.live_read_transactions.borrow_mut().remove(&id);
    }

    pub(in crate) fn update_table_root(
        &self,
        name: &[u8],
        table_type: TableType,
        table_root: Option<PageNumber>,
        transaction_id: u128,
        master_root: Option<PageNumber>,
    ) -> Result<PageNumber, Error> {
        let definition = TableDefinition {
            table_root,
            table_type,
        };
        self.insert::<[u8]>(name, &definition.to_bytes(), transaction_id, master_root)
    }

    // root_page: the root of the master table
    pub(in crate) fn get_table(
        &self,
        name: &[u8],
        table_type: TableType,
        root_page: Option<PageNumber>,
    ) -> Result<Option<TableDefinition>, Error> {
        if let Some(found) = self.get::<[u8], [u8]>(name, root_page)? {
            let definition = TableDefinition::from_bytes(found.as_ref());
            if definition.get_type() != table_type {
                return Err(Error::TableTypeMismatch(format!(
                    "{:?} is not of type {:?}",
                    name, table_type
                )));
            }

            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    // root_page: the root of the master table
    pub(in crate) fn delete_table(
        &self,
        name: &[u8],
        table_type: TableType,
        transaction_id: u128,
        mut root_page: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        if let Some(definition) = self.get_table(name, table_type, root_page)? {
            if let Some(table_root) = definition.get_root() {
                let page = self.mem.get_page(table_root);
                let start = page_numbers_iter_start_state(page);
                let iter = AllPageNumbersBtreeIter::new(start, &self.mem);
                for page_number in iter {
                    self.pending_freed_pages.borrow_mut().push(page_number);
                }
            }

            root_page = self.remove::<[u8]>(name, transaction_id, root_page)?;
        }

        Ok(root_page)
    }

    // Returns a tuple of the table id and the new root page
    // root_page: the root of the master table
    pub(in crate) fn get_or_create_table(
        &self,
        name: &[u8],
        table_type: TableType,
        transaction_id: u128,
        root_page: Option<PageNumber>,
    ) -> Result<(TableDefinition, PageNumber), Error> {
        if let Some(found) = self.get_table(name, table_type, root_page)? {
            return Ok((found, root_page.unwrap()));
        }

        let definition = TableDefinition {
            table_root: None,
            table_type,
        };
        let new_root =
            self.insert::<[u8]>(name, &definition.to_bytes(), transaction_id, root_page)?;
        Ok((definition, new_root))
    }

    // Returns the new root page number
    pub(in crate) fn insert<K: RedbKey + ?Sized>(
        &self,
        key: &[u8],
        value: &[u8],
        transaction_id: u128,
        root_page: Option<PageNumber>,
    ) -> Result<PageNumber, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        if let Some(handle) = root_page {
            let root = self.mem.get_page(handle);
            let (new_root, _, freed) = tree_insert::<K>(root, key, value, &self.mem)?;
            self.pending_freed_pages
                .borrow_mut()
                .extend_from_slice(&freed);
            Ok(new_root)
        } else {
            let (new_root, _) = make_mut_single_leaf(key, value, &self.mem)?;
            Ok(new_root)
        }
    }

    // Returns the new root page number, and accessor for writing the value
    pub(in crate) fn insert_reserve<K: RedbKey + ?Sized>(
        &self,
        key: &[u8],
        value_len: usize,
        transaction_id: u128,
        root_page: Option<PageNumber>,
    ) -> Result<(PageNumber, AccessGuardMut), Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        let value = vec![0u8; value_len];
        let (new_root, guard) = if let Some(handle) = root_page {
            let root = self.mem.get_page(handle);
            let (new_root, guard, freed) = tree_insert::<K>(root, key, &value, &self.mem)?;
            self.pending_freed_pages
                .borrow_mut()
                .extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key, &value, &self.mem)?
        };
        Ok((new_root, guard))
    }

    pub(in crate) fn len(&self, root_page: Option<PageNumber>) -> Result<usize, Error> {
        let mut iter: BtreeRangeIter<RangeFull, [u8], [u8], [u8]> =
            self.get_range(.., root_page)?;
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub(in crate) fn get_root_page_number(&self) -> Option<PageNumber> {
        self.mem.get_primary_root_page()
    }

    pub(in crate) fn commit(
        &self,
        mut new_master_root: Option<PageNumber>,
        transaction_id: u128,
    ) -> Result<(), Error> {
        let oldest_live_read = self
            .live_read_transactions
            .borrow()
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| self.next_transaction_id.get());

        new_master_root =
            self.process_freed_pages(oldest_live_read, transaction_id, new_master_root)?;
        if oldest_live_read < transaction_id {
            new_master_root = self.store_freed_pages(transaction_id, new_master_root)?;
        } else {
            for page in self.pending_freed_pages.borrow_mut().drain(..) {
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
        assert_eq!(Some(transaction_id), self.live_write_transaction.take());
        Ok(())
    }

    // Commit without a durability guarantee
    pub(in crate) fn non_durable_commit(
        &self,
        mut new_master_root: Option<PageNumber>,
        transaction_id: u128,
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
        assert_eq!(Some(transaction_id), self.live_write_transaction.take());
        Ok(())
    }

    // NOTE: must be called before store_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(
        &self,
        oldest_live_read: u128,
        transaction_id: u128,
        mut master_root: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        assert_eq!(PageNumber::null().to_be_bytes().len(), 8); // We assume below that PageNumber is length 8
        let mut lookup_key = [0u8; 24]; // (oldest_live_read, 0)
        lookup_key[0..size_of::<u128>()].copy_from_slice(&oldest_live_read.to_be_bytes());
        // second element of pair is already zero

        let mut to_remove = vec![];
        let mut freed_table = self
            .get_table(FREED_TABLE, TableType::Normal, master_root)?
            .unwrap();
        #[allow(clippy::type_complexity)]
        let mut iter: BtreeRangeIter<'_, RangeTo<&[u8]>, &[u8], [u8], [u8]> =
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
            freed_table.table_root =
                self.remove::<[u8]>(&key, transaction_id, freed_table.table_root)?;
        }
        master_root = Some(self.insert::<[u8]>(
            FREED_TABLE,
            &freed_table.to_bytes(),
            transaction_id,
            master_root,
        )?);

        Ok(master_root)
    }

    fn store_freed_pages(
        &self,
        transaction_id: u128,
        mut master_root: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        assert_eq!(PageNumber::null().to_be_bytes().len(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        let mut freed_table = self
            .get_table(FREED_TABLE, TableType::Normal, master_root)?
            .unwrap();
        while !self.pending_freed_pages.borrow().is_empty() {
            let chunk_size = 100;
            let buffer_size = size_of::<u64>() + 8 * chunk_size;
            let mut key = [0u8; 24];
            key[0..size_of::<u128>()].copy_from_slice(&transaction_id.to_be_bytes());
            key[size_of::<u128>()..].copy_from_slice(&pagination_counter.to_be_bytes());
            let (r, mut access_guard) = self.insert_reserve::<[u8]>(
                key.as_ref(),
                buffer_size,
                transaction_id,
                freed_table.table_root,
            )?;
            freed_table.table_root = Some(r);

            if self.pending_freed_pages.borrow().len() <= chunk_size {
                // Update the master root, only on the last loop iteration (this may cause another
                // iteration, but that's ok since it would have very few pages to process)
                master_root = Some(self.insert::<[u8]>(
                    FREED_TABLE,
                    &freed_table.to_bytes(),
                    transaction_id,
                    master_root,
                )?);
            }

            let len = self.pending_freed_pages.borrow().len();
            access_guard.as_mut()[..8]
                .copy_from_slice(&min(len as u64, chunk_size as u64).to_be_bytes());
            for (i, page) in self
                .pending_freed_pages
                .borrow_mut()
                .drain(len - min(len, chunk_size)..)
                .enumerate()
            {
                access_guard.as_mut()[(i + 1) * 8..(i + 2) * 8]
                    .copy_from_slice(&page.to_be_bytes());
            }

            pagination_counter += 1;
        }

        Ok(master_root)
    }

    pub(in crate) fn rollback_uncommited_writes(&self, transaction_id: u128) -> Result<(), Error> {
        self.pending_freed_pages.borrow_mut().clear();
        let result = self.mem.rollback_uncommited_writes();
        assert_eq!(Some(transaction_id), self.live_write_transaction.take());

        result
    }

    pub(in crate) fn storage_stats(&self) -> Result<DbStats, Error> {
        let master_tree_height = self
            .get_root_page_number()
            .map(|p| tree_height(self.mem.get_page(p), &self.mem))
            .unwrap_or(0);
        let mut max_subtree_height = 0;
        let mut iter: BtreeRangeIter<RangeFull, [u8], [u8], [u8]> =
            self.get_range(.., self.get_root_page_number())?;
        while let Some(entry) = iter.next() {
            let definition = TableDefinition::from_bytes(entry.value());
            if let Some(table_root) = definition.get_root() {
                let height = tree_height(self.mem.get_page(table_root), &self.mem);
                max_subtree_height = max(max_subtree_height, height);
            }
        }
        Ok(DbStats::new(
            master_tree_height + max_subtree_height,
            self.mem.count_free_pages()?,
        ))
    }

    #[allow(dead_code)]
    pub(in crate) fn print_debug(&self) {
        if let Some(page) = self.get_root_page_number() {
            eprintln!("Master tree:");
            print_tree(self.mem.get_page(page), &self.mem);

            let mut iter: BtreeRangeIter<RangeFull, [u8], [u8], [u8]> =
                self.get_range(.., Some(page)).unwrap();

            while let Some(entry) = iter.next() {
                eprintln!("{} tree:", String::from_utf8_lossy(entry.key()));
                let definition = TableDefinition::from_bytes(entry.value());
                if let Some(table_root) = definition.get_root() {
                    self.print_dirty_tree_debug(table_root);
                }
            }
        }
    }

    #[allow(dead_code)]
    pub(in crate) fn print_dirty_tree_debug(&self, root_page: PageNumber) {
        print_tree(self.mem.get_page(root_page), &self.mem);
    }

    pub(in crate) fn get<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        key: &[u8],
        root_page_handle: Option<PageNumber>,
    ) -> Result<Option<AccessGuard<V>>, Error> {
        if let Some(handle) = root_page_handle {
            let root_page = self.mem.get_page(handle);
            if let Some((page, offset, len)) = lookup_in_raw::<K>(root_page, key, &self.mem) {
                return Ok(Some(AccessGuard::new(page, offset, len)));
            }
        }
        Ok(None)
    }

    pub(in crate) fn get_range<
        'a,
        T: RangeBounds<KR>,
        KR: AsRef<K> + ?Sized + 'a,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    >(
        &'a self,
        range: T,
        root_page: Option<PageNumber>,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        if let Some(root) = root_page {
            match range.start_bound() {
                Bound::Included(k) | Bound::Excluded(k) => {
                    let start_state = find_iter_start::<K>(
                        self.mem.get_page(root),
                        None,
                        k.as_ref().as_bytes().as_ref(),
                        &self.mem,
                    );
                    Ok(BtreeRangeIter::new(start_state, range, &self.mem))
                }
                Bound::Unbounded => {
                    let start_state =
                        find_iter_unbounded_start(self.mem.get_page(root), None, &self.mem);
                    Ok(BtreeRangeIter::new(start_state, range, &self.mem))
                }
            }
        } else {
            Ok(BtreeRangeIter::new(None, range, &self.mem))
        }
    }

    pub(in crate) fn get_range_reversed<
        'a,
        T: RangeBounds<KR>,
        KR: AsRef<K> + ?Sized + 'a,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    >(
        &'a self,
        range: T,
        root_page: Option<PageNumber>,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        if let Some(root) = root_page {
            match range.end_bound() {
                Bound::Included(k) | Bound::Excluded(k) => {
                    let start_state = find_iter_start_reversed::<K>(
                        self.mem.get_page(root),
                        None,
                        k.as_ref().as_bytes().as_ref(),
                        &self.mem,
                    );
                    Ok(BtreeRangeIter::new_reversed(start_state, range, &self.mem))
                }
                Bound::Unbounded => {
                    let start_state =
                        find_iter_unbounded_reversed(self.mem.get_page(root), None, &self.mem);
                    Ok(BtreeRangeIter::new_reversed(start_state, range, &self.mem))
                }
            }
        } else {
            Ok(BtreeRangeIter::new_reversed(None, range, &self.mem))
        }
    }

    // Returns the new root page. To determine if an entry was remove test whether equal to root_page
    pub(in crate) fn remove<K: RedbKey + ?Sized>(
        &self,
        key: &[u8],
        transaction_id: u128,
        root_handle: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        if let Some(handle) = root_handle {
            let root_page = self.mem.get_page(handle);
            let (new_root, freed) = tree_delete::<K>(root_page, key, &self.mem)?;
            self.pending_freed_pages
                .borrow_mut()
                .extend_from_slice(&freed);
            return Ok(new_root);
        }
        Ok(root_handle)
    }
}

pub struct AccessGuard<'a, V: RedbValue + ?Sized> {
    page: PageImpl<'a>,
    offset: usize,
    len: usize,
    _value_type: PhantomData<V>,
}

impl<'a, V: RedbValue + ?Sized> AccessGuard<'a, V> {
    fn new(page: PageImpl<'a>, offset: usize, len: usize) -> Self {
        Self {
            page,
            offset,
            len,
            _value_type: Default::default(),
        }
    }

    pub fn to_value(&self) -> <<V as RedbValue>::View as WithLifetime>::Out {
        V::from_bytes(&self.page.memory()[self.offset..(self.offset + self.len)])
    }
}

impl<'a> AsRef<[u8]> for AccessGuard<'a, [u8]> {
    fn as_ref(&self) -> &[u8] {
        &self.page.memory()[self.offset..(self.offset + self.len)]
    }
}
