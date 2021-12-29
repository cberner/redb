use crate::tree_store::base_types::NodeHandle;
use crate::tree_store::btree_utils::{
    lookup_in_raw, make_mut_single_leaf, print_tree, tree_delete, tree_height, tree_insert,
    AccessGuardMut, BtreeEntry, BtreeRangeIter,
};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Error;
use memmap2::MmapMut;
use std::cell::{Cell, RefCell};
use std::cmp::min;
use std::collections::BTreeSet;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{RangeBounds, RangeFull, RangeTo};

// The table of name -> table_id mappings
const TABLE_TABLE_ID: u64 = 0;
// The table of freed pages by transaction. (transaction id, pagination counter) -> binary.
// The binary blob is a length-prefixed array of big endian PageNumber
const FREED_TABLE_ID: u64 = TABLE_TABLE_ID + 1;
// Table with a single entry indicating the largest valid table id. u64 -> u64, with a key of LAST_TABLE_ID_KEY
const LAST_TABLE_ID_TABLE: u64 = FREED_TABLE_ID + 1;
const LAST_SYSTEM_TABLE: u64 = LAST_TABLE_ID_TABLE;

const LAST_TABLE_ID_KEY: u64 = 0;

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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum TableType {
    Normal,
    MultiMap,
}

#[allow(clippy::from_over_into)]
impl Into<u8> for TableType {
    fn into(self) -> u8 {
        match self {
            TableType::Normal => 1,
            TableType::MultiMap => 2,
        }
    }
}

impl From<u8> for TableType {
    fn from(value: u8) -> Self {
        match value {
            1 => TableType::Normal,
            2 => TableType::MultiMap,
            _ => unreachable!(),
        }
    }
}

pub(crate) struct TableDefinition {
    table_id: u64,
    table_type: TableType,
}

impl TableDefinition {
    pub(crate) fn get_id(&self) -> u64 {
        self.table_id
    }

    pub(crate) fn get_type(&self) -> TableType {
        self.table_type
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = vec![self.table_type.into()];
        result.extend_from_slice(&self.table_id.to_be_bytes());

        result
    }

    fn from_bytes(value: &[u8]) -> Self {
        assert_eq!(9, value.len());
        let table_id = u64::from_be_bytes(value[1..9].try_into().unwrap());
        let table_type = TableType::from(value[0]);

        TableDefinition {
            table_id,
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
}

impl Storage {
    pub(in crate) fn new(mmap: MmapMut, page_size: Option<usize>) -> Result<Storage, Error> {
        let mem = TransactionalMemory::new(mmap, page_size)?;
        let next_transaction_id = mem.get_last_committed_transaction_id() + 1;

        Ok(Storage {
            mem,
            next_transaction_id: Cell::new(next_transaction_id),
            live_write_transaction: Cell::new(None),
            live_read_transactions: RefCell::new(Default::default()),
            pending_freed_pages: RefCell::new(Default::default()),
        })
    }

    pub(crate) fn allocate_write_transaction(&self) -> u128 {
        assert!(self.live_write_transaction.get().is_none());
        assert!(self.pending_freed_pages.borrow().is_empty());
        let id = self.next_transaction_id.get();
        self.live_write_transaction.set(Some(id));
        self.next_transaction_id.set(id + 1);
        id
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

    pub(in crate) fn get_table(
        &self,
        name: &[u8],
        table_type: TableType,
        root_page: Option<NodeHandle>,
    ) -> Result<Option<TableDefinition>, Error> {
        if let Some(found) = self.get::<[u8], [u8]>(TABLE_TABLE_ID, name, root_page)? {
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

    // Returns a tuple of the table id and the new root page
    pub(in crate) fn get_or_create_table(
        &self,
        name: &[u8],
        table_type: TableType,
        transaction_id: u128,
        root_page: Option<NodeHandle>,
    ) -> Result<(TableDefinition, NodeHandle), Error> {
        if let Some(found) = self.get::<[u8], [u8]>(TABLE_TABLE_ID, name, root_page)? {
            let definition = TableDefinition::from_bytes(found.as_ref());
            if definition.get_type() != table_type {
                return Err(Error::TableTypeMismatch(format!(
                    "{:?} is not of type {:?}",
                    name, table_type
                )));
            }
            return Ok((definition, root_page.unwrap()));
        }

        let largest_id = self
            .get::<u64, u64>(
                LAST_TABLE_ID_TABLE,
                &LAST_TABLE_ID_KEY.to_be_bytes(),
                root_page,
            )?
            .map(|x| x.to_value())
            .unwrap_or(LAST_SYSTEM_TABLE);
        let new_id = largest_id + 1;
        let definition = TableDefinition {
            table_id: new_id,
            table_type,
        };
        let new_root = self.insert::<u64>(
            LAST_TABLE_ID_TABLE,
            &LAST_TABLE_ID_KEY.to_be_bytes(),
            &new_id.to_be_bytes(),
            transaction_id,
            root_page,
        )?;
        let new_root = self.insert::<[u8]>(
            TABLE_TABLE_ID,
            name,
            &definition.to_bytes(),
            transaction_id,
            Some(new_root),
        )?;
        Ok((definition, new_root))
    }

    // Returns the new root page number
    pub(in crate) fn insert<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        value: &[u8],
        transaction_id: u128,
        root_page: Option<NodeHandle>,
    ) -> Result<NodeHandle, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        if let Some(handle) = root_page {
            let root = self.mem.get_page(handle.get_page_number());
            let (new_root, _, freed) = tree_insert::<K>(
                root,
                handle.get_valid_messages(),
                table_id,
                key,
                value,
                &self.mem,
            );
            self.pending_freed_pages
                .borrow_mut()
                .extend_from_slice(&freed);
            Ok(new_root)
        } else {
            let (new_root, _) = make_mut_single_leaf(table_id, key, value, &self.mem);
            Ok(new_root)
        }
    }

    // Returns the new root page number, and accessor for writing the value
    pub(in crate) fn insert_reserve<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        value_len: usize,
        transaction_id: u128,
        root_page: Option<NodeHandle>,
    ) -> Result<(NodeHandle, AccessGuardMut), Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        let value = vec![0u8; value_len];
        let (new_root, guard) = if let Some(handle) = root_page {
            let root = self.mem.get_page(handle.get_page_number());
            let (new_root, guard, freed) = tree_insert::<K>(
                root,
                handle.get_valid_messages(),
                table_id,
                key,
                &value,
                &self.mem,
            );
            self.pending_freed_pages
                .borrow_mut()
                .extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(table_id, key, &value, &self.mem)
        };
        Ok((new_root, guard))
    }

    pub(in crate) fn len(&self, table: u64, root_page: Option<NodeHandle>) -> Result<usize, Error> {
        let mut iter = BtreeRangeIter::<RangeFull, [u8], [u8], [u8]>::new(
            root_page.map(|p| {
                (
                    self.mem.get_page(p.get_page_number()),
                    p.get_valid_messages(),
                )
            }),
            table,
            ..,
            &self.mem,
        );
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub(in crate) fn get_root_page_number(&self) -> Option<NodeHandle> {
        self.mem
            .get_primary_root_page()
            .map(|(page, message_bytes)| NodeHandle::new(page, message_bytes))
    }

    pub(in crate) fn commit(
        &self,
        mut new_root: Option<NodeHandle>,
        transaction_id: u128,
    ) -> Result<(), Error> {
        let oldest_live_read = self
            .live_read_transactions
            .borrow()
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| self.next_transaction_id.get());

        new_root = self.process_freed_pages(oldest_live_read, transaction_id, new_root)?;
        if oldest_live_read < transaction_id {
            new_root = self.store_freed_pages(transaction_id, new_root)?;
        } else {
            for page in self.pending_freed_pages.borrow_mut().drain(..) {
                self.mem.free(page);
            }
        }

        if let Some(ptr) = new_root {
            self.mem
                .set_secondary_root_page(ptr.get_page_number(), ptr.get_valid_messages());
        } else {
            self.mem.set_secondary_root_page(PageNumber::null(), 0);
        }

        self.mem.commit(transaction_id)?;
        assert_eq!(Some(transaction_id), self.live_write_transaction.take());
        Ok(())
    }

    // Commit without a durability guarantee
    pub(in crate) fn non_durable_commit(
        &self,
        mut new_root: Option<NodeHandle>,
        transaction_id: u128,
    ) -> Result<(), Error> {
        // Store all freed pages for a future commit(), since we can't free pages during a
        // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
        new_root = self.store_freed_pages(transaction_id, new_root)?;

        if let Some(ptr) = new_root {
            self.mem
                .set_secondary_root_page(ptr.get_page_number(), ptr.get_valid_messages());
        } else {
            self.mem.set_secondary_root_page(PageNumber::null(), 0);
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
        mut root: Option<NodeHandle>,
    ) -> Result<Option<NodeHandle>, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        assert_eq!(PageNumber::null().to_be_bytes().len(), 8); // We assume below that PageNumber is length 8
        let mut lookup_key = [0u8; 24]; // (oldest_live_read, 0)
        lookup_key[0..size_of::<u128>()].copy_from_slice(&oldest_live_read.to_be_bytes());
        // second element of pair is already zero

        let mut to_remove = vec![];
        #[allow(clippy::type_complexity)]
        let mut iter: BtreeRangeIter<'_, RangeTo<&[u8]>, &[u8], [u8], [u8]> =
            self.get_range(FREED_TABLE_ID, ..lookup_key.as_ref(), root)?;
        while let Some(entry) = iter.next() {
            to_remove.push(entry.key().to_vec());
            let value = entry.value();
            let length = u64::from_be_bytes(value[..size_of::<u64>()].try_into().unwrap()) as usize;
            // 1..=length because the array is length prefixed
            for i in 1..=length {
                let page = PageNumber::from_be_bytes(value[i * 8..(i + 1) * 8].try_into().unwrap());
                self.mem.free(page);
            }
        }
        drop(iter);

        // Remove all the old transactions. Note: this may create new pages that need to be freed
        for key in to_remove {
            root = self.remove::<[u8]>(FREED_TABLE_ID, &key, transaction_id, root)?;
        }

        Ok(root)
    }

    fn store_freed_pages(
        &self,
        transaction_id: u128,
        mut root: Option<NodeHandle>,
    ) -> Result<Option<NodeHandle>, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        assert_eq!(PageNumber::null().to_be_bytes().len(), 8); // We assume below that PageNumber is length 8

        let mut pagination_counter = 0u64;
        while !self.pending_freed_pages.borrow().is_empty() {
            // TODO: dynamically size this
            let chunk_size = 100;
            let buffer_size = size_of::<u64>() + 8 * chunk_size;
            let mut key = [0u8; 24];
            key[0..size_of::<u128>()].copy_from_slice(&transaction_id.to_be_bytes());
            key[size_of::<u128>()..].copy_from_slice(&pagination_counter.to_be_bytes());
            let (r, mut access_guard) = self.insert_reserve::<[u8]>(
                FREED_TABLE_ID,
                key.as_ref(),
                buffer_size,
                transaction_id,
                root,
            )?;
            root = Some(r);

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

        Ok(root)
    }

    pub(in crate) fn rollback_uncommited_writes(&self, transaction_id: u128) -> Result<(), Error> {
        self.pending_freed_pages.borrow_mut().clear();
        let result = self.mem.rollback_uncommited_writes();
        assert_eq!(Some(transaction_id), self.live_write_transaction.take());

        result
    }

    pub(in crate) fn storage_stats(&self) -> Result<DbStats, Error> {
        let tree_height = self
            .get_root_page_number()
            .map(|p| {
                tree_height(
                    self.mem.get_page(p.get_page_number()),
                    p.get_valid_messages(),
                    &self.mem,
                )
            })
            .unwrap_or(0);
        Ok(DbStats::new(tree_height, self.mem.count_free_pages()))
    }

    #[allow(dead_code)]
    pub(in crate) fn print_debug(&self) {
        if let Some(page) = self.get_root_page_number() {
            print_tree(
                self.mem.get_page(page.get_page_number()),
                page.get_valid_messages(),
                &self.mem,
            );
        }
    }

    #[allow(dead_code)]
    pub(in crate) fn print_dirty_debug(&self, root_page: NodeHandle) {
        print_tree(
            self.mem.get_page(root_page.get_page_number()),
            root_page.get_valid_messages(),
            &self.mem,
        );
    }

    pub(in crate) fn get<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        root_page_handle: Option<NodeHandle>,
    ) -> Result<Option<AccessGuard<V>>, Error> {
        if let Some(handle) = root_page_handle {
            let root_page = self.mem.get_page(handle.get_page_number());
            if let Some((page, offset, len)) = lookup_in_raw::<K>(
                root_page,
                handle.get_valid_messages(),
                table_id,
                key,
                &self.mem,
            ) {
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
        table_id: u64,
        range: T,
        root_page: Option<NodeHandle>,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        Ok(BtreeRangeIter::new(
            root_page.map(|p| {
                (
                    self.mem.get_page(p.get_page_number()),
                    p.get_valid_messages(),
                )
            }),
            table_id,
            range,
            &self.mem,
        ))
    }

    pub(in crate) fn get_range_reversed<
        'a,
        T: RangeBounds<KR>,
        KR: AsRef<K> + ?Sized + 'a,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    >(
        &'a self,
        table_id: u64,
        range: T,
        root_page: Option<NodeHandle>,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        Ok(BtreeRangeIter::new_reversed(
            root_page.map(|p| {
                (
                    self.mem.get_page(p.get_page_number()),
                    p.get_valid_messages(),
                )
            }),
            table_id,
            range,
            &self.mem,
        ))
    }

    // Returns the new root page. To determine if an entry was remove test whether equal to root_page
    pub(in crate) fn remove<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        transaction_id: u128,
        root_handle: Option<NodeHandle>,
    ) -> Result<Option<NodeHandle>, Error> {
        assert_eq!(transaction_id, self.live_write_transaction.get().unwrap());
        if let Some(handle) = root_handle {
            let root_page = self.mem.get_page(handle.get_page_number());
            let (new_root, freed) = tree_delete::<K>(
                root_page,
                handle.get_valid_messages(),
                table_id,
                key,
                &self.mem,
            );
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
