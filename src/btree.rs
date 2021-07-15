use crate::btree::Node::{Internal, Leaf};
use crate::btree::RangeIterState::{
    InitialState, InternalLeft, InternalRight, LeafLeft, LeafRight,
};
use crate::page_manager::{Page, PageManager, PageMut};
use crate::types::RedbKey;
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

enum RangeIterState<'a> {
    InitialState(Page<'a>, bool),
    LeafLeft {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    LeafRight {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    InternalLeft {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    InternalRight {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
}

impl<'a> RangeIterState<'a> {
    fn forward_next(self, manager: &'a PageManager) -> Option<RangeIterState> {
        match self {
            RangeIterState::InitialState(root_page, ..) => match root_page.memory()[0] {
                LEAF => Some(LeafLeft {
                    page: root_page,
                    parent: None,
                    reversed: false,
                }),
                INTERNAL => Some(InternalLeft {
                    page: root_page,
                    parent: None,
                    reversed: false,
                }),
                _ => unreachable!(),
            },
            RangeIterState::LeafLeft { page, parent, .. } => Some(LeafRight {
                page,
                parent,
                reversed: false,
            }),
            RangeIterState::LeafRight { parent, .. } => parent.map(|x| *x),
            RangeIterState::InternalLeft { page, parent, .. } => {
                let child = InternalAccessor::new(&page).lte_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafLeft {
                        page: child_page,
                        parent: Some(Box::new(InternalRight {
                            page,
                            parent,
                            reversed: false,
                        })),
                        reversed: false,
                    }),
                    INTERNAL => Some(InternalLeft {
                        page: child_page,
                        parent: Some(Box::new(InternalRight {
                            page,
                            parent,
                            reversed: false,
                        })),
                        reversed: false,
                    }),
                    _ => unreachable!(),
                }
            }
            RangeIterState::InternalRight { page, parent, .. } => {
                let child = InternalAccessor::new(&page).gt_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafLeft {
                        page: child_page,
                        parent,
                        reversed: false,
                    }),
                    INTERNAL => Some(InternalLeft {
                        page: child_page,
                        parent,
                        reversed: false,
                    }),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn backward_next(self, manager: &'a PageManager) -> Option<RangeIterState> {
        match self {
            RangeIterState::InitialState(root_page, ..) => match root_page.memory()[0] {
                LEAF => Some(LeafRight {
                    page: root_page,
                    parent: None,
                    reversed: true,
                }),
                INTERNAL => Some(InternalRight {
                    page: root_page,
                    parent: None,
                    reversed: true,
                }),
                _ => unreachable!(),
            },
            RangeIterState::LeafLeft { parent, .. } => parent.map(|x| *x),
            RangeIterState::LeafRight { page, parent, .. } => Some(LeafLeft {
                page,
                parent,
                reversed: true,
            }),
            RangeIterState::InternalLeft { page, parent, .. } => {
                let child = InternalAccessor::new(&page).lte_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafRight {
                        page: child_page,
                        parent,
                        reversed: true,
                    }),
                    INTERNAL => Some(InternalRight {
                        page: child_page,
                        parent,
                        reversed: true,
                    }),
                    _ => unreachable!(),
                }
            }
            RangeIterState::InternalRight { page, parent, .. } => {
                let child = InternalAccessor::new(&page).gt_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafRight {
                        page: child_page,
                        parent: Some(Box::new(InternalLeft {
                            page,
                            parent,
                            reversed: true,
                        })),
                        reversed: true,
                    }),
                    INTERNAL => Some(InternalRight {
                        page: child_page,
                        parent: Some(Box::new(InternalLeft {
                            page,
                            parent,
                            reversed: true,
                        })),
                        reversed: true,
                    }),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn next(self, manager: &'a PageManager) -> Option<RangeIterState> {
        match &self {
            InitialState(_, reversed) => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::LeafLeft { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::LeafRight { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::InternalLeft { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::InternalRight { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
        }
    }

    fn get_entry(&self) -> Option<EntryAccessor> {
        match self {
            RangeIterState::LeafLeft { page, .. } => Some(LeafAccessor::new(&page).lesser()),
            RangeIterState::LeafRight { page, .. } => LeafAccessor::new(&page).greater(),
            _ => None,
        }
    }
}

// TODO: T should be a RangeBound<&'a K>
pub struct BtreeRangeIter<'a, T: RangeBounds<&'a [u8]>, K: RedbKey + ?Sized> {
    last: Option<RangeIterState<'a>>,
    table_id: u64,
    query_range: T,
    reversed: bool,
    manager: &'a PageManager,
    _key_type: PhantomData<K>,
}

impl<'a, T: RangeBounds<&'a [u8]>, K: RedbKey + ?Sized> BtreeRangeIter<'a, T, K> {
    pub(in crate) fn new(
        root_page: Option<Page<'a>>,
        table_id: u64,
        query_range: T,
        manager: &'a PageManager,
    ) -> Self {
        Self {
            last: root_page.map(|p| InitialState(p, false)),
            table_id,
            query_range,
            reversed: false,
            manager,
            _key_type: Default::default(),
        }
    }

    pub(in crate) fn new_reversed(
        root_page: Option<Page<'a>>,
        table_id: u64,
        query_range: T,
        manager: &'a PageManager,
    ) -> Self {
        Self {
            last: root_page.map(|p| InitialState(p, true)),
            table_id,
            query_range,
            reversed: true,
            manager,
            _key_type: Default::default(),
        }
    }

    // TODO: we need generic-associated-types to implement Iterator
    pub fn next(&mut self) -> Option<EntryAccessor> {
        if let Some(mut state) = self.last.take() {
            loop {
                if let Some(new_state) = state.next(self.manager) {
                    if let Some(entry) = new_state.get_entry() {
                        // TODO: optimize. This is very inefficient to retrieve and then ignore the values
                        if self.table_id == entry.table_id()
                            && bound_contains_key::<T, K>(&self.query_range, entry.key())
                        {
                            self.last = Some(new_state);
                            return self.last.as_ref().map(|s| s.get_entry().unwrap());
                        } else {
                            #[allow(clippy::collapsible_else_if)]
                            if self.reversed {
                                if let Bound::Included(start) = self.query_range.start_bound() {
                                    if entry.compare::<K>(self.table_id, *start).is_lt() {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(start) =
                                    self.query_range.start_bound()
                                {
                                    if entry.compare::<K>(self.table_id, *start).is_le() {
                                        self.last = None;
                                        return None;
                                    }
                                }
                            } else {
                                if let Bound::Included(end) = self.query_range.end_bound() {
                                    if entry.compare::<K>(self.table_id, *end).is_gt() {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(end) = self.query_range.end_bound() {
                                    if entry.compare::<K>(self.table_id, *end).is_ge() {
                                        self.last = None;
                                        return None;
                                    }
                                }
                            };

                            state = new_state;
                        }
                    } else {
                        state = new_state;
                    }
                } else {
                    self.last = None;
                    return None;
                }
            }
        }
        None
    }
}

pub trait BtreeEntry<'a: 'b, 'b> {
    fn key(&'b self) -> &'a [u8];
    fn value(&'b self) -> &'a [u8];
}

fn cmp_keys<K: RedbKey + ?Sized>(table1: u64, key1: &[u8], table2: u64, key2: &[u8]) -> Ordering {
    match table1.cmp(&table2) {
        Ordering::Less => Ordering::Less,
        Ordering::Equal => K::compare(key1, key2),
        Ordering::Greater => Ordering::Greater,
    }
}

fn bound_contains_key<'a, T: RangeBounds<&'a [u8]>, K: RedbKey + ?Sized>(
    range: &T,
    key: &[u8],
) -> bool {
    if let Bound::Included(start) = range.start_bound() {
        if K::compare(key, *start).is_lt() {
            return false;
        }
    } else if let Bound::Excluded(start) = range.start_bound() {
        if K::compare(key, *start).is_le() {
            return false;
        }
    }
    if let Bound::Included(end) = range.end_bound() {
        if K::compare(key, *end).is_gt() {
            return false;
        }
    } else if let Bound::Excluded(end) = range.end_bound() {
        if K::compare(key, *end).is_ge() {
            return false;
        }
    }

    true
}

// Provides a simple zero-copy way to access entries
//
// Entry format is:
// * (8 bytes) key_size
// * (8 bytes) table_id, 64-bit big endian unsigned. Stored between key_size & key_data, so that
//   it can be read with key_data as a single key_size + 8 length unique key for the entire db
// * (key_size bytes) key_data
// * (8 bytes) value_size
// * (value_size bytes) value_data
pub struct EntryAccessor<'a> {
    raw: &'a [u8],
}

impl<'a> EntryAccessor<'a> {
    fn new(raw: &'a [u8]) -> Self {
        EntryAccessor { raw }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.raw[0..8].try_into().unwrap()) as usize
    }

    pub(in crate) fn table_id(&self) -> u64 {
        u64::from_be_bytes(self.raw[8..16].try_into().unwrap())
    }

    fn value_offset(&self) -> usize {
        16 + self.key_len() + 8
    }

    fn value_len(&self) -> usize {
        let key_len = self.key_len();
        u64::from_be_bytes(
            self.raw[(16 + key_len)..(16 + key_len + 8)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn raw_len(&self) -> usize {
        16 + self.key_len() + 8 + self.value_len()
    }

    fn compare<K: RedbKey + ?Sized>(&self, table: u64, key: &[u8]) -> Ordering {
        cmp_keys::<K>(self.table_id(), self.key(), table, key)
    }
}

impl<'a: 'b, 'b> BtreeEntry<'a, 'b> for EntryAccessor<'a> {
    fn key(&'b self) -> &'a [u8] {
        &self.raw[16..(16 + self.key_len())]
    }

    fn value(&'b self) -> &'a [u8] {
        &self.raw[self.value_offset()..(self.value_offset() + self.value_len())]
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct EntryMutator<'a> {
    raw: &'a mut [u8],
}

impl<'a> EntryMutator<'a> {
    fn new(raw: &'a mut [u8]) -> Self {
        EntryMutator { raw }
    }

    fn write_table_id(&mut self, table_id: u64) {
        self.raw[8..16].copy_from_slice(&table_id.to_be_bytes());
    }

    fn write_key(&mut self, key: &[u8]) {
        self.raw[0..8].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.raw[16..(16 + key.len())].copy_from_slice(key);
    }

    fn write_value(&mut self, value: &[u8]) {
        let value_offset = EntryAccessor::new(self.raw).value_offset();
        self.raw[(value_offset - 8)..value_offset]
            .copy_from_slice(&(value.len() as u64).to_be_bytes());
        self.raw[value_offset..(value_offset + value.len())].copy_from_slice(value);
    }
}

// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 1 = LEAF
// * (n bytes) lesser_entry
// * (n bytes) greater_entry: optional
struct LeafAccessor<'a: 'b, 'b> {
    page: &'b Page<'a>,
}

impl<'a: 'b, 'b> LeafAccessor<'a, 'b> {
    fn new(page: &'b Page<'a>) -> Self {
        LeafAccessor { page }
    }

    fn offset_of_lesser(&self) -> usize {
        1
    }

    fn offset_of_greater(&self) -> usize {
        1 + self.lesser().raw_len()
    }

    fn lesser(&self) -> EntryAccessor<'b> {
        EntryAccessor::new(&self.page.memory()[self.offset_of_lesser()..])
    }

    fn greater(&self) -> Option<EntryAccessor<'b>> {
        let entry = EntryAccessor::new(&self.page.memory()[self.offset_of_greater()..]);
        if entry.key_len() == 0 {
            None
        } else {
            Some(entry)
        }
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct LeafBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> LeafBuilder<'a, 'b> {
    fn new(page: &'b mut PageMut<'a>) -> Self {
        page.memory_mut()[0] = LEAF;
        LeafBuilder { page }
    }

    fn write_lesser(&mut self, table_id: u64, key: &[u8], value: &[u8]) {
        let mut entry = EntryMutator::new(&mut self.page.memory_mut()[1..]);
        entry.write_table_id(table_id);
        entry.write_key(key);
        entry.write_value(value);
    }

    fn write_greater(&mut self, entry: Option<(u64, &[u8], &[u8])>) {
        let offset = 1 + EntryAccessor::new(&self.page.memory()[1..]).raw_len();
        let mut writer = EntryMutator::new(&mut self.page.memory_mut()[offset..]);
        if let Some((table_id, key, value)) = entry {
            writer.write_table_id(table_id);
            writer.write_key(key);
            writer.write_value(value);
        } else {
            writer.write_key(&[]);
        }
    }
}

// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 2 = INTERNAL
// * (8 bytes) key_len
// * (8 bytes) table_id 64-bit big-endian unsigned
// * (key_len bytes) key_data
// * (8 bytes) lte_page: page number for keys <= key_data
// * (8 bytes) gt_page: page number for keys > key_data
struct InternalAccessor<'a: 'b, 'b> {
    page: &'b Page<'a>,
}

impl<'a: 'b, 'b> InternalAccessor<'a, 'b> {
    fn new(page: &'b Page<'a>) -> Self {
        InternalAccessor { page }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.page.memory()[1..9].try_into().unwrap()) as usize
    }

    fn table_id(&self) -> u64 {
        u64::from_be_bytes(self.page.memory()[9..17].try_into().unwrap())
    }

    fn key(&self) -> &[u8] {
        &self.page.memory()[17..(17 + self.key_len())]
    }

    fn lte_page(&self) -> u64 {
        let offset = 17 + self.key_len();
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap())
    }

    fn gt_page(&self) -> u64 {
        let offset = 17 + self.key_len() + 8;
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap())
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct InternalBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> InternalBuilder<'a, 'b> {
    fn new(page: &'b mut PageMut<'a>) -> Self {
        page.memory_mut()[0] = INTERNAL;
        InternalBuilder { page }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.page.memory()[1..9].try_into().unwrap()) as usize
    }

    fn write_table_and_key(&mut self, table_id: u64, key: &[u8]) {
        self.page.memory_mut()[1..9].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.page.memory_mut()[9..17].copy_from_slice(&table_id.to_be_bytes());
        self.page.memory_mut()[17..(17 + key.len())].copy_from_slice(key);
    }

    fn write_lte_page(&mut self, page_number: u64) {
        let offset = 17 + self.key_len();
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }

    fn write_gt_page(&mut self, page_number: u64) {
        let offset = 17 + self.key_len() + 8;
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
pub(in crate) fn tree_delete<'a, K: RedbKey + ?Sized>(
    page: Page<'a>,
    table: u64,
    key: &[u8],
    manager: &'a PageManager,
) -> Option<u64> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            #[allow(clippy::collapsible_else_if)]
            if let Some(greater) = accessor.greater() {
                if accessor.lesser().compare::<K>(table, key).is_ne()
                    && greater.compare::<K>(table, key).is_ne()
                {
                    // Not found
                    return Some(page.get_page_number());
                }
                let new_leaf = if accessor.lesser().compare::<K>(table, key).is_eq() {
                    Leaf(
                        (
                            greater.table_id(),
                            greater.key().to_vec(),
                            greater.value().to_vec(),
                        ),
                        None,
                    )
                } else {
                    Leaf(
                        (
                            accessor.lesser().table_id(),
                            accessor.lesser().key().to_vec(),
                            accessor.lesser().value().to_vec(),
                        ),
                        None,
                    )
                };

                // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
                drop(page);
                Some(new_leaf.to_bytes(manager))
            } else {
                if accessor.lesser().compare::<K>(table, key).is_eq() {
                    // Deleted the entire left
                    None
                } else {
                    // Not found
                    Some(page.get_page_number())
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let original_left_page = accessor.lte_page();
            let original_right_page = accessor.gt_page();
            let original_page_number = page.get_page_number();
            let mut left_page = accessor.lte_page();
            let mut right_page = accessor.gt_page();
            // TODO: we should recompute our key, since it may now be smaller (if the largest key in the left tree was deleted)
            let our_table = accessor.table_id();
            let our_key = accessor.key().to_vec();
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            #[allow(clippy::collapsible_else_if)]
            if cmp_keys::<K>(table, key, our_table, our_key.as_slice()).is_le() {
                if let Some(page_number) =
                    tree_delete::<K>(manager.get_page(left_page), table, key, manager)
                {
                    left_page = page_number;
                } else {
                    // The entire left sub-tree was deleted, replace ourself with the right tree
                    return Some(right_page);
                }
            } else {
                if let Some(page_number) =
                    tree_delete::<K>(manager.get_page(right_page), table, key, manager)
                {
                    right_page = page_number;
                } else {
                    return Some(left_page);
                }
            }

            // The key was not found, since neither sub-tree changed
            if left_page == original_left_page && right_page == original_right_page {
                return Some(original_page_number);
            }

            let mut page = manager.allocate();
            let mut builder = InternalBuilder::new(&mut page);
            builder.write_table_and_key(our_table, &our_key);
            builder.write_lte_page(left_page);
            builder.write_gt_page(right_page);

            Some(page.get_page_number())
        }
        _ => unreachable!(),
    }
}

// Returns the page number of the sub-tree into which the key was inserted
pub(in crate) fn tree_insert<'a, K: RedbKey + ?Sized>(
    page: Page<'a>,
    table: u64,
    key: &[u8],
    value: &[u8],
    manager: &'a PageManager,
) -> u64 {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            // TODO: this is suboptimal, because it may rebuild the leaf page even if it's not necessary:
            // e.g. when we insert a second leaf adjacent without modifying this one
            let mut builder = BtreeBuilder::new();
            builder.add(table, key, value);
            if accessor.lesser().compare::<K>(table, key).is_ne() {
                builder.add(
                    accessor.lesser().table_id(),
                    accessor.lesser().key(),
                    accessor.lesser().value(),
                );
            }
            if let Some(entry) = accessor.greater() {
                if entry.compare::<K>(table, key).is_ne() {
                    builder.add(entry.table_id(), entry.key(), entry.value());
                }
            }

            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            builder.build::<K>().to_bytes(manager)
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut left_page = accessor.lte_page();
            let mut right_page = accessor.gt_page();
            let our_table = accessor.table_id();
            let our_key = accessor.key().to_vec();
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            if (table, key) <= (our_table, our_key.as_slice()) {
                left_page =
                    tree_insert::<K>(manager.get_page(left_page), table, key, value, manager);
            } else {
                right_page =
                    tree_insert::<K>(manager.get_page(right_page), table, key, value, manager);
            }

            let mut page = manager.allocate();
            let mut builder = InternalBuilder::new(&mut page);
            builder.write_table_and_key(our_table, &our_key);
            builder.write_lte_page(left_page);
            builder.write_gt_page(right_page);

            page.get_page_number()
        }
        _ => unreachable!(),
    }
}

// Returns the (offset, len) of the value for the queried key, if present
pub(in crate) fn lookup_in_raw<'a, K: RedbKey + ?Sized>(
    page: Page<'a>,
    table: u64,
    query: &[u8],
    manager: &'a PageManager,
) -> Option<(Page<'a>, usize, usize)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            match cmp_keys::<K>(
                table,
                query,
                accessor.lesser().table_id(),
                accessor.lesser().key(),
            ) {
                Ordering::Less => None,
                Ordering::Equal => {
                    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();
                    let value_len = accessor.lesser().value().len();
                    Some((page, offset, value_len))
                }
                Ordering::Greater => {
                    if let Some(entry) = accessor.greater() {
                        if entry.compare::<K>(table, query).is_eq() {
                            let offset = accessor.offset_of_greater() + entry.value_offset();
                            let value_len = entry.value().len();
                            Some((page, offset, value_len))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let left_page = accessor.lte_page();
            let right_page = accessor.gt_page();
            if cmp_keys::<K>(table, query, accessor.table_id(), accessor.key()).is_le() {
                lookup_in_raw::<K>(manager.get_page(left_page), table, query, manager)
            } else {
                lookup_in_raw::<K>(manager.get_page(right_page), table, query, manager)
            }
        }
        _ => unreachable!(),
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(in crate) enum Node {
    Leaf((u64, Vec<u8>, Vec<u8>), Option<(u64, Vec<u8>, Vec<u8>)>),
    Internal(Box<Node>, u64, Vec<u8>, Box<Node>),
}

impl Node {
    // Returns the page number that the node was written to
    pub(in crate) fn to_bytes(&self, page_manager: &PageManager) -> u64 {
        match self {
            Node::Leaf(left_val, right_val) => {
                let mut page = page_manager.allocate();
                let mut builder = LeafBuilder::new(&mut page);
                builder.write_lesser(left_val.0, &left_val.1, &left_val.2);
                builder.write_greater(
                    right_val
                        .as_ref()
                        .map(|(table, key, value)| (*table, key.as_slice(), value.as_slice())),
                );

                page.get_page_number()
            }
            Node::Internal(left, table, key, right) => {
                let left_page = left.to_bytes(page_manager);
                let right_page = right.to_bytes(page_manager);
                let mut page = page_manager.allocate();
                let mut builder = InternalBuilder::new(&mut page);
                builder.write_table_and_key(*table, key);
                builder.write_lte_page(left_page);
                builder.write_gt_page(right_page);

                page.get_page_number()
            }
        }
    }

    fn get_max_key(&self) -> (u64, Vec<u8>) {
        match self {
            Node::Leaf((left_table, left_key, _), right_val) => {
                if let Some((right_table, right_key, _)) = right_val {
                    (*right_table, right_key.to_vec())
                } else {
                    (*left_table, left_key.to_vec())
                }
            }
            Node::Internal(_left, _table, _key, right) => right.get_max_key(),
        }
    }
}

pub(in crate) struct BtreeBuilder {
    pairs: Vec<(u64, Vec<u8>, Vec<u8>)>,
}

impl BtreeBuilder {
    pub(in crate) fn new() -> BtreeBuilder {
        BtreeBuilder { pairs: vec![] }
    }

    pub(in crate) fn add(&mut self, table: u64, key: &[u8], value: &[u8]) {
        self.pairs.push((table, key.to_vec(), value.to_vec()));
    }

    pub(in crate) fn build<K: RedbKey + ?Sized>(mut self) -> Node {
        assert!(!self.pairs.is_empty());
        self.pairs.sort_by(|(table1, key1, _), (table2, key2, _)| {
            cmp_keys::<K>(*table1, key1, *table2, key2)
        });
        let mut leaves = vec![];
        for group in self.pairs.chunks(2) {
            let leaf = if group.len() == 1 {
                Leaf((group[0].0, group[0].1.to_vec(), group[0].2.to_vec()), None)
            } else {
                assert_eq!(group.len(), 2);
                if (group[0].0, &group[0].1) == (group[1].0, &group[1].1) {
                    todo!("support overwriting existing keys");
                }
                Leaf(
                    (group[0].0, group[0].1.to_vec(), group[0].2.to_vec()),
                    Some((group[1].0, group[1].1.to_vec(), group[1].2.to_vec())),
                )
            };
            leaves.push(leaf);
        }

        let mut bottom = leaves;
        let maybe_previous_node: Cell<Option<Node>> = Cell::new(None);
        while bottom.len() > 1 {
            let mut internals = vec![];
            for node in bottom.drain(..) {
                if let Some(previous_node) = maybe_previous_node.take() {
                    let (table, key) = previous_node.get_max_key();
                    let internal = Internal(Box::new(previous_node), table, key, Box::new(node));
                    internals.push(internal)
                } else {
                    maybe_previous_node.set(Some(node));
                }
            }
            if let Some(previous_node) = maybe_previous_node.take() {
                internals.push(previous_node);
            }

            bottom = internals
        }

        bottom.pop().unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::btree::Node::{Internal, Leaf};
    use crate::btree::{BtreeBuilder, Node};

    fn gen_tree() -> Node {
        let left = Leaf(
            (1, b"hello".to_vec(), b"world".to_vec()),
            Some((1, b"hello2".to_vec(), b"world2".to_vec())),
        );
        let right = Leaf((1, b"hello3".to_vec(), b"world3".to_vec()), None);
        Internal(Box::new(left), 1, b"hello2".to_vec(), Box::new(right))
    }

    #[test]
    fn builder() {
        let expected = gen_tree();
        let mut builder = BtreeBuilder::new();
        builder.add(1, b"hello2", b"world2");
        builder.add(1, b"hello3", b"world3");
        builder.add(1, b"hello", b"world");

        assert_eq!(expected, builder.build::<[u8]>());
    }
}
