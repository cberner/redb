use crate::btree::RangeIterState::{
    InitialState, InternalLeft, InternalRight, LeafLeft, LeafRight,
};
use crate::page_manager::{Page, PageMut, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue};
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
    fn forward_next(self, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
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

    fn backward_next(self, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
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

    fn next(self, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
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

pub struct BtreeRangeIter<
    'a,
    T: RangeBounds<&'a K>,
    K: RedbKey + ?Sized + 'a,
    V: RedbValue + ?Sized + 'a,
> {
    last: Option<RangeIterState<'a>>,
    table_id: u64,
    query_range: T,
    reversed: bool,
    manager: &'a TransactionalMemory,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, T: RangeBounds<&'a K>, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a>
    BtreeRangeIter<'a, T, K, V>
{
    pub(in crate) fn new(
        root_page: Option<Page<'a>>,
        table_id: u64,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            last: root_page.map(|p| InitialState(p, false)),
            table_id,
            query_range,
            reversed: false,
            manager,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(in crate) fn new_reversed(
        root_page: Option<Page<'a>>,
        table_id: u64,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            last: root_page.map(|p| InitialState(p, true)),
            table_id,
            query_range,
            reversed: true,
            manager,
            _key_type: Default::default(),
            _value_type: Default::default(),
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
                                    if entry
                                        .compare::<K>(self.table_id, start.as_bytes().as_ref())
                                        .is_lt()
                                    {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(start) =
                                    self.query_range.start_bound()
                                {
                                    if entry
                                        .compare::<K>(self.table_id, start.as_bytes().as_ref())
                                        .is_le()
                                    {
                                        self.last = None;
                                        return None;
                                    }
                                }
                            } else {
                                if let Bound::Included(end) = self.query_range.end_bound() {
                                    if entry
                                        .compare::<K>(self.table_id, end.as_bytes().as_ref())
                                        .is_gt()
                                    {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(end) = self.query_range.end_bound() {
                                    if entry
                                        .compare::<K>(self.table_id, end.as_bytes().as_ref())
                                        .is_ge()
                                    {
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

fn bound_contains_key<'a, T: RangeBounds<&'a K>, K: RedbKey + ?Sized + 'a>(
    range: &T,
    key: &[u8],
) -> bool {
    if let Bound::Included(start) = range.start_bound() {
        if K::compare(key, start.as_bytes().as_ref()).is_lt() {
            return false;
        }
    } else if let Bound::Excluded(start) = range.start_bound() {
        if K::compare(key, start.as_bytes().as_ref()).is_le() {
            return false;
        }
    }
    if let Bound::Included(end) = range.end_bound() {
        if K::compare(key, end.as_bytes().as_ref()).is_gt() {
            return false;
        }
    } else if let Bound::Excluded(end) = range.end_bound() {
        if K::compare(key, end.as_bytes().as_ref()).is_ge() {
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

    fn max_key(&self) -> (u64, &'b [u8]) {
        if let Some(greater) = self.greater() {
            (greater.table_id(), greater.key())
        } else {
            (self.lesser().table_id(), self.lesser().key())
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

    fn lte_page(&self) -> PageNumber {
        let offset = 17 + self.key_len();
        PageNumber(u64::from_be_bytes(
            self.page.memory()[offset..(offset + 8)].try_into().unwrap(),
        ))
    }

    fn gt_page(&self) -> PageNumber {
        let offset = 17 + self.key_len() + 8;
        PageNumber(u64::from_be_bytes(
            self.page.memory()[offset..(offset + 8)].try_into().unwrap(),
        ))
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

    fn write_lte_page(&mut self, page_number: PageNumber) {
        let offset = 17 + self.key_len();
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }

    fn write_gt_page(&mut self, page_number: PageNumber) {
        let offset = 17 + self.key_len() + 8;
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
#[allow(clippy::needless_return)]
pub(in crate) fn tree_delete<'a, K: RedbKey + ?Sized>(
    page: Page<'a>,
    table: u64,
    key: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<PageNumber> {
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
                    (greater.table_id(), greater.key(), greater.value())
                } else {
                    (
                        accessor.lesser().table_id(),
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                    )
                };

                Some(make_single_leaf(
                    new_leaf.0, new_leaf.1, new_leaf.2, manager,
                ))
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
            let original_page_number = page.get_page_number();
            let left_page = accessor.lte_page();
            let right_page = accessor.gt_page();
            #[allow(clippy::collapsible_else_if)]
            if cmp_keys::<K>(table, key, accessor.table_id(), accessor.key()).is_le() {
                if let Some(page_number) =
                    tree_delete::<K>(manager.get_page(left_page), table, key, manager)
                {
                    // The key was not found, since the sub-tree did not change
                    if page_number == left_page {
                        return Some(original_page_number);
                    }
                    let left = manager.get_page(page_number);
                    let new_key = LeafAccessor::new(&left).max_key();
                    return Some(make_index(
                        new_key.0,
                        new_key.1,
                        page_number,
                        right_page,
                        manager,
                    ));
                } else {
                    // The entire left sub-tree was deleted, replace ourself with the right tree
                    return Some(right_page);
                }
            } else {
                if let Some(page_number) =
                    tree_delete::<K>(manager.get_page(right_page), table, key, manager)
                {
                    // The key was not found, since the sub-tree did not change
                    if page_number == right_page {
                        return Some(original_page_number);
                    }
                    return Some(make_index(
                        accessor.table_id(),
                        accessor.key(),
                        left_page,
                        page_number,
                        manager,
                    ));
                } else {
                    return Some(left_page);
                }
            }
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn make_single_leaf<'a>(
    table: u64,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> PageNumber {
    let mut page = manager.allocate();
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(table, key, value);
    builder.write_greater(None);
    page.get_page_number()
}

pub(in crate) fn make_double_leaf<'a, K: RedbKey + ?Sized>(
    table1: u64,
    key1: &[u8],
    value1: &[u8],
    table2: u64,
    key2: &[u8],
    value2: &[u8],
    manager: &'a TransactionalMemory,
) -> PageNumber {
    debug_assert!(cmp_keys::<K>(table1, key1, table2, key2).is_lt());
    let mut page = manager.allocate();
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(table1, key1, value1);
    builder.write_greater(Some((table2, key2, value2)));
    page.get_page_number()
}

pub(in crate) fn make_index(
    table: u64,
    key: &[u8],
    lte_page: PageNumber,
    gt_page: PageNumber,
    manager: &TransactionalMemory,
) -> PageNumber {
    let mut page = manager.allocate();
    let mut builder = InternalBuilder::new(&mut page);
    builder.write_table_and_key(table, &key);
    builder.write_lte_page(lte_page);
    builder.write_gt_page(gt_page);
    page.get_page_number()
}

// Returns the page number of the sub-tree into which the key was inserted
pub(in crate) fn tree_insert<'a, K: RedbKey + ?Sized>(
    page: Page<'a>,
    table: u64,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> PageNumber {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            if let Some(entry) = accessor.greater() {
                match entry.compare::<K>(table, key) {
                    Ordering::Less => {
                        // New entry goes in a new page to the right, so leave this page untouched
                        let left_page = page.get_page_number();

                        let right_page = make_single_leaf(table, key, value, manager);
                        make_index(
                            entry.table_id(),
                            entry.key(),
                            left_page,
                            right_page,
                            manager,
                        )
                    }
                    Ordering::Equal => make_double_leaf::<K>(
                        accessor.lesser().table_id(),
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        table,
                        key,
                        value,
                        manager,
                    ),
                    Ordering::Greater => {
                        let right_table = entry.table_id();
                        let right_key = entry.key();
                        let right_value = entry.value();

                        let left_table = accessor.lesser().table_id();
                        let left_key = accessor.lesser().key();
                        let left_value = accessor.lesser().value();

                        match accessor.lesser().compare::<K>(table, key) {
                            Ordering::Less => {
                                let left = make_double_leaf::<K>(
                                    left_table, left_key, left_value, table, key, value, manager,
                                );
                                let right =
                                    make_single_leaf(right_table, right_key, right_value, manager);
                                make_index(table, key, left, right, manager)
                            }
                            Ordering::Equal => make_double_leaf::<K>(
                                table,
                                key,
                                value,
                                right_table,
                                right_key,
                                right_value,
                                manager,
                            ),
                            Ordering::Greater => {
                                let left = make_double_leaf::<K>(
                                    table, key, value, left_table, left_key, left_value, manager,
                                );
                                let right =
                                    make_single_leaf(right_table, right_key, right_value, manager);
                                make_index(left_table, &left_key, left, right, manager)
                            }
                        }
                    }
                }
            } else {
                match cmp_keys::<K>(
                    accessor.lesser().table_id(),
                    accessor.lesser().key(),
                    table,
                    key,
                ) {
                    Ordering::Less => make_double_leaf::<K>(
                        accessor.lesser().table_id(),
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        table,
                        key,
                        value,
                        manager,
                    ),
                    Ordering::Equal => make_single_leaf(table, key, value, manager),
                    Ordering::Greater => make_double_leaf::<K>(
                        table,
                        key,
                        value,
                        accessor.lesser().table_id(),
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        manager,
                    ),
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut left_page = accessor.lte_page();
            let mut right_page = accessor.gt_page();
            if cmp_keys::<K>(table, key, accessor.table_id(), accessor.key()).is_le() {
                left_page =
                    tree_insert::<K>(manager.get_page(left_page), table, key, value, manager);
            } else {
                right_page =
                    tree_insert::<K>(manager.get_page(right_page), table, key, value, manager);
            }

            let mut page = manager.allocate();
            let mut builder = InternalBuilder::new(&mut page);
            builder.write_table_and_key(accessor.table_id(), accessor.key());
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
    manager: &'a TransactionalMemory,
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
