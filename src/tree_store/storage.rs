use crate::tree_store::btree_utils::{
    lookup_in_raw, make_mut_single_leaf, print_tree, tree_delete, tree_height, tree_insert,
    AccessGuardMut, BtreeEntry, BtreeRangeIter,
};
use crate::tree_store::page_store::page_manager::{
    Page, PageImpl, PageNumber, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Error;
use memmap2::MmapMut;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};

// The table of name -> table_id mappings
const TABLE_TABLE_ID: u64 = 0;

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
    // TODO: these need to be persisted in the mmap'ed file, so that they don't leak
    // if we crash before freeing pages
    pending_freed_pages: RefCell<BTreeMap<u128, Vec<PageNumber>>>,
}

impl Storage {
    pub(in crate) fn new(mmap: MmapMut) -> Result<Storage, Error> {
        let mem = TransactionalMemory::new(mmap)?;

        Ok(Storage {
            mem,
            next_transaction_id: Cell::new(1),
            live_read_transactions: RefCell::new(Default::default()),
            pending_freed_pages: RefCell::new(Default::default()),
        })
    }

    pub(crate) fn allocate_write_transaction(&self) -> u128 {
        let id = self.next_transaction_id.get();
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

    // Returns a tuple of the table id and the new root page
    pub(in crate) fn get_or_create_table(
        &self,
        name: &[u8],
        table_type: TableType,
        transaction_id: u128,
        root_page: Option<PageNumber>,
    ) -> Result<(TableDefinition, PageNumber), Error> {
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

        let mut iter =
            self.get_range_reversed::<RangeFull, [u8], [u8], [u8]>(TABLE_TABLE_ID, .., root_page)?;
        let largest_id = iter
            .next()
            .map(|x| TableDefinition::from_bytes(x.value()).get_id())
            .unwrap_or(TABLE_TABLE_ID);
        drop(iter);
        let new_id = largest_id + 1;
        let definition = TableDefinition {
            table_id: new_id,
            table_type,
        };
        let new_root = self.insert::<[u8]>(
            TABLE_TABLE_ID,
            name,
            &definition.to_bytes(),
            transaction_id,
            root_page,
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
        root_page: Option<PageNumber>,
    ) -> Result<PageNumber, Error> {
        if let Some(root) = root_page.map(|p| self.mem.get_page(p)) {
            let (new_root, _, freed) = tree_insert::<K>(root, table_id, key, value, &self.mem);
            self.pending_freed_pages
                .borrow_mut()
                .entry(transaction_id)
                .or_default()
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
        root_page: Option<PageNumber>,
    ) -> Result<(PageNumber, AccessGuardMut), Error> {
        let value = vec![0u8; value_len];
        let (new_root, guard) = if let Some(root) = root_page.map(|p| self.mem.get_page(p)) {
            let (new_root, guard, freed) = tree_insert::<K>(root, table_id, key, &value, &self.mem);
            self.pending_freed_pages
                .borrow_mut()
                .entry(transaction_id)
                .or_default()
                .extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(table_id, key, &value, &self.mem)
        };
        Ok((new_root, guard))
    }

    pub(in crate) fn len(&self, table: u64, root_page: Option<PageNumber>) -> Result<usize, Error> {
        let mut iter = BtreeRangeIter::<RangeFull, [u8], [u8], [u8]>::new(
            root_page.map(|p| self.mem.get_page(p)),
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

    pub(in crate) fn get_root_page_number(&self) -> Option<PageNumber> {
        self.mem.get_primary_root_page()
    }

    pub(in crate) fn commit(&self, new_root: Option<PageNumber>) -> Result<(), Error> {
        self.mem
            .set_secondary_root_page(new_root.unwrap_or(PageNumber(0)));
        let oldest_live_read = self
            .live_read_transactions
            .borrow()
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| self.next_transaction_id.get());
        // TODO: replace this loop with BtreeMap.drain_filter when it's stable
        #[allow(clippy::while_let_loop)]
        loop {
            let to_remove =
                if let Some((key, freed)) = self.pending_freed_pages.borrow_mut().iter().next() {
                    if *key < oldest_live_read {
                        for page in freed {
                            self.mem.free(*page);
                        }
                        *key
                    } else {
                        break;
                    }
                } else {
                    break;
                };
            self.pending_freed_pages.borrow_mut().remove(&to_remove);
        }
        self.mem.commit()?;
        Ok(())
    }

    pub(in crate) fn rollback_uncommited_writes(&self, transaction_id: u128) -> Result<(), Error> {
        self.pending_freed_pages
            .borrow_mut()
            .remove(&transaction_id);
        self.mem.rollback_uncommited_writes()
    }

    pub(in crate) fn storage_stats(&self) -> Result<DbStats, Error> {
        let tree_height = self
            .get_root_page_number()
            .map(|p| tree_height(self.mem.get_page(p), &self.mem))
            .unwrap_or(0);
        Ok(DbStats::new(tree_height, self.mem.count_free_pages()))
    }

    #[allow(dead_code)]
    pub(in crate) fn print_debug(&self) {
        if let Some(page) = self.get_root_page_number() {
            print_tree(self.mem.get_page(page), &self.mem);
        }
    }

    #[allow(dead_code)]
    pub(in crate) fn print_dirty_debug(&self, root_page: PageNumber) {
        print_tree(self.mem.get_page(root_page), &self.mem);
    }

    pub(in crate) fn get<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        root_page_number: Option<PageNumber>,
    ) -> Result<Option<AccessGuard<V>>, Error> {
        if let Some(root_page) = root_page_number.map(|p| self.mem.get_page(p)) {
            if let Some((page, offset, len)) =
                lookup_in_raw::<K>(root_page, table_id, key, &self.mem)
            {
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
        root_page: Option<PageNumber>,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        Ok(BtreeRangeIter::new(
            root_page.map(|p| self.mem.get_page(p)),
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
        root_page: Option<PageNumber>,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        Ok(BtreeRangeIter::new_reversed(
            root_page.map(|p| self.mem.get_page(p)),
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
        page_number: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        if let Some(root_page) = page_number.map(|p| self.mem.get_page(p)) {
            let (new_root, freed) = tree_delete::<K>(root_page, table_id, key, &self.mem);
            self.pending_freed_pages
                .borrow_mut()
                .entry(transaction_id)
                .or_default()
                .extend_from_slice(&freed);
            return Ok(new_root);
        }
        Ok(page_number)
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
