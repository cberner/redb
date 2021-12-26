use crate::tree_store::btree_utils::DeletionResult::{PartialInternal, PartialLeaf, Subtree};
use crate::tree_store::btree_utils::RangeIterState::{InitialState, Internal, LeafLeft, LeafRight};
use crate::tree_store::page_store::{Page, PageImpl, PageMut, PageNumber, TransactionalMemory};
use crate::tree_store::NodeHandle;
use crate::types::{RedbKey, RedbValue};
use std::cmp::{max, Ordering};
use std::convert::TryInto;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Bound, RangeBounds};

const BTREE_ORDER: usize = 40;
const MESSAGE_BUFFER: usize = 32;
// TODO: dynamically calculate this based on the actual page size
const MAX_KEY_SPACE_PER_PAGE: usize = 4096
    - NodeHandle::serialized_size() * BTREE_ORDER
    - 24 * BTREE_ORDER
    - (1 + NodeHandle::serialized_size()) * MESSAGE_BUFFER;

pub struct AccessGuardMut<'a> {
    page: PageMut<'a>,
    offset: usize,
    len: usize,
}

impl<'a> AccessGuardMut<'a> {
    fn new(page: PageMut<'a>, offset: usize, len: usize) -> Self {
        AccessGuardMut { page, offset, len }
    }
}

impl<'a> AsMut<[u8]> for AccessGuardMut<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.page.memory_mut()[self.offset..(self.offset + self.len)]
    }
}

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

#[derive(Debug)]
enum RangeIterState<'a> {
    InitialState(PageImpl<'a>, u32, bool),
    LeafLeft {
        page: PageImpl<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    LeafRight {
        page: PageImpl<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    Internal {
        page: PageImpl<'a>,
        valid_message_bytes: u32,
        child: usize,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
}

impl<'a> RangeIterState<'a> {
    fn forward_next(self, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
        match self {
            RangeIterState::InitialState(root_page, valid_message_bytes, ..) => {
                match root_page.memory()[0] {
                    LEAF => Some(LeafLeft {
                        page: root_page,
                        parent: None,
                        reversed: false,
                    }),
                    INTERNAL => Some(Internal {
                        page: root_page,
                        valid_message_bytes,
                        child: 0,
                        parent: None,
                        reversed: false,
                    }),
                    _ => unreachable!(),
                }
            }
            RangeIterState::LeafLeft { page, parent, .. } => Some(LeafRight {
                page,
                parent,
                reversed: false,
            }),
            RangeIterState::LeafRight { parent, .. } => parent.map(|x| *x),
            RangeIterState::Internal {
                page,
                valid_message_bytes,
                child,
                mut parent,
                ..
            } => {
                let accessor = InternalAccessor::new(&page, valid_message_bytes);
                let child_page = accessor.child_page(child).unwrap();
                let child_message_bytes = child_page.get_valid_messages();
                let child_page = manager.get_page(child_page.get_page_number());
                if child < BTREE_ORDER - 1 && accessor.child_page(child + 1).is_some() {
                    parent = Some(Box::new(Internal {
                        page,
                        valid_message_bytes,
                        child: child + 1,
                        parent,
                        reversed: false,
                    }));
                }
                match child_page.memory()[0] {
                    LEAF => Some(LeafLeft {
                        page: child_page,
                        parent,
                        reversed: false,
                    }),
                    INTERNAL => Some(Internal {
                        page: child_page,
                        valid_message_bytes: child_message_bytes,
                        child: 0,
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
            RangeIterState::InitialState(root_page, valid_message_bytes, ..) => {
                match root_page.memory()[0] {
                    LEAF => Some(LeafRight {
                        page: root_page,
                        parent: None,
                        reversed: true,
                    }),
                    INTERNAL => {
                        let accessor = InternalAccessor::new(&root_page, valid_message_bytes);
                        let mut index = 0;
                        for i in (0..BTREE_ORDER).rev() {
                            if accessor.child_page(i).is_some() {
                                index = i;
                                break;
                            }
                        }
                        assert!(index > 0);
                        Some(Internal {
                            page: root_page,
                            valid_message_bytes,
                            child: index,
                            parent: None,
                            reversed: true,
                        })
                    }
                    _ => unreachable!(),
                }
            }
            RangeIterState::LeafLeft { parent, .. } => parent.map(|x| *x),
            RangeIterState::LeafRight { page, parent, .. } => Some(LeafLeft {
                page,
                parent,
                reversed: true,
            }),
            RangeIterState::Internal {
                page,
                valid_message_bytes,
                child,
                mut parent,
                ..
            } => {
                let child_page = InternalAccessor::new(&page, valid_message_bytes)
                    .child_page(child)
                    .unwrap();
                let child_message_bytes = child_page.get_valid_messages();
                let child_page = manager.get_page(child_page.get_page_number());
                if child > 0 {
                    parent = Some(Box::new(Internal {
                        page,
                        valid_message_bytes,
                        child: child - 1,
                        parent,
                        reversed: true,
                    }));
                }
                match child_page.memory()[0] {
                    LEAF => Some(LeafRight {
                        page: child_page,
                        parent,
                        reversed: true,
                    }),
                    INTERNAL => {
                        let accessor = InternalAccessor::new(&child_page, child_message_bytes);
                        let mut index = 0;
                        for i in (0..BTREE_ORDER).rev() {
                            if accessor.child_page(i).is_some() {
                                index = i;
                                break;
                            }
                        }
                        assert!(index > 0);
                        Some(Internal {
                            page: child_page,
                            valid_message_bytes: child_message_bytes,
                            child: index,
                            parent,
                            reversed: true,
                        })
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn next(self, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
        match &self {
            InitialState(_, _, reversed) => {
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
            RangeIterState::Internal { reversed, .. } => {
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
            RangeIterState::LeafLeft { page, .. } => Some(LeafAccessor::new(page).lesser()),
            RangeIterState::LeafRight { page, .. } => LeafAccessor::new(page).greater(),
            _ => None,
        }
    }
}

pub struct BtreeRangeIter<
    'a,
    T: RangeBounds<KR>,
    KR: AsRef<K> + ?Sized + 'a,
    K: RedbKey + ?Sized + 'a,
    V: RedbValue + ?Sized + 'a,
> {
    last: Option<RangeIterState<'a>>,
    table_id: u64,
    query_range: T,
    reversed: bool,
    manager: &'a TransactionalMemory,
    _key_type: PhantomData<K>,
    _key_ref_type: PhantomData<KR>,
    _value_type: PhantomData<V>,
}

impl<
        'a,
        T: RangeBounds<KR>,
        KR: AsRef<K> + ?Sized + 'a,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    > BtreeRangeIter<'a, T, KR, K, V>
{
    pub(in crate) fn new(
        root_page: Option<(PageImpl<'a>, u32)>,
        table_id: u64,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            last: root_page.map(|(p, message_bytes)| InitialState(p, message_bytes, false)),
            table_id,
            query_range,
            reversed: false,
            manager,
            _key_type: Default::default(),
            _key_ref_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(in crate) fn new_reversed(
        root_page: Option<(PageImpl<'a>, u32)>,
        table_id: u64,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            last: root_page.map(|(p, message_bytes)| InitialState(p, message_bytes, true)),
            table_id,
            query_range,
            reversed: true,
            manager,
            _key_type: Default::default(),
            _key_ref_type: Default::default(),
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
                            && bound_contains_key::<T, KR, K>(&self.query_range, entry.key())
                        {
                            self.last = Some(new_state);
                            return self.last.as_ref().map(|s| s.get_entry().unwrap());
                        } else {
                            #[allow(clippy::collapsible_else_if)]
                            if self.reversed {
                                if let Bound::Included(start) = self.query_range.start_bound() {
                                    if entry
                                        .compare::<K>(
                                            self.table_id,
                                            start.as_ref().as_bytes().as_ref(),
                                        )
                                        .is_lt()
                                    {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(start) =
                                    self.query_range.start_bound()
                                {
                                    if entry
                                        .compare::<K>(
                                            self.table_id,
                                            start.as_ref().as_bytes().as_ref(),
                                        )
                                        .is_le()
                                    {
                                        self.last = None;
                                        return None;
                                    }
                                }
                            } else {
                                if let Bound::Included(end) = self.query_range.end_bound() {
                                    if entry
                                        .compare::<K>(
                                            self.table_id,
                                            end.as_ref().as_bytes().as_ref(),
                                        )
                                        .is_gt()
                                    {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(end) = self.query_range.end_bound() {
                                    if entry
                                        .compare::<K>(
                                            self.table_id,
                                            end.as_ref().as_bytes().as_ref(),
                                        )
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

fn bound_contains_key<
    'a,
    T: RangeBounds<KR>,
    KR: AsRef<K> + ?Sized + 'a,
    K: RedbKey + ?Sized + 'a,
>(
    range: &T,
    key: &[u8],
) -> bool {
    if let Bound::Included(start) = range.start_bound() {
        if K::compare(key, start.as_ref().as_bytes().as_ref()).is_lt() {
            return false;
        }
    } else if let Bound::Excluded(start) = range.start_bound() {
        if K::compare(key, start.as_ref().as_bytes().as_ref()).is_le() {
            return false;
        }
    }
    if let Bound::Included(end) = range.end_bound() {
        if K::compare(key, end.as_ref().as_bytes().as_ref()).is_gt() {
            return false;
        }
    } else if let Bound::Excluded(end) = range.end_bound() {
        if K::compare(key, end.as_ref().as_bytes().as_ref()).is_ge() {
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

// TODO: support more than 2 entries in a leaf
// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 1 = LEAF
// * (n bytes) lesser_entry
// * (n bytes) greater_entry: optional
struct LeafAccessor<'a: 'b, 'b, T: Page + 'a> {
    page: &'b T,
    _page_lifetime: PhantomData<&'a ()>,
}

impl<'a: 'b, 'b, T: Page + 'a> LeafAccessor<'a, 'b, T> {
    fn new(page: &'b T) -> Self {
        LeafAccessor {
            page,
            _page_lifetime: Default::default(),
        }
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
    fn required_bytes(keys_values: &[&[u8]]) -> usize {
        assert_eq!(keys_values.len() % 2, 0);
        // Page id;
        let mut result = 1;
        // Table ids
        result += keys_values.len() / 2 * size_of::<u64>();
        // key & value lengths
        result += keys_values.len() * size_of::<u64>();
        result += keys_values.iter().map(|x| x.len()).sum::<usize>();

        result
    }

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

// Provides a simple zero-copy way to access an index page
struct InternalAccessor<'a: 'b, 'b, T: Page + 'a> {
    page: &'b T,
    valid_messages: u32,
    _page_lifetime: PhantomData<&'a ()>,
}

impl<'a: 'b, 'b, T: Page + 'a> InternalAccessor<'a, 'b, T> {
    fn new(page: &'b T, message_bytes: u32) -> Self {
        debug_assert_eq!(page.memory()[0], INTERNAL);
        InternalAccessor {
            page,
            valid_messages: message_bytes,
            _page_lifetime: Default::default(),
        }
    }

    fn child_for_key<K: RedbKey + ?Sized>(&self, table: u64, query: &[u8]) -> (usize, NodeHandle) {
        let mut min_child = 0; // inclusive
        let mut max_child = BTREE_ORDER - 1; // inclusive
        while min_child < max_child {
            let mid = (min_child + max_child) / 2;
            if let Some((table_id, key)) = self.table_and_key(mid) {
                match cmp_keys::<K>(table, query, table_id, key) {
                    Ordering::Less => {
                        max_child = mid;
                    }
                    Ordering::Equal => {
                        return (mid, self.child_page(mid).unwrap());
                    }
                    Ordering::Greater => {
                        min_child = mid + 1;
                    }
                }
            } else {
                max_child = mid;
            }
        }
        debug_assert_eq!(min_child, max_child);

        (min_child, self.child_page(min_child).unwrap())
    }

    fn key_offset(&self, n: usize) -> usize {
        let offset =
            1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) * 2 + 8 * n;
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap()) as usize
    }

    fn key_len(&self, n: usize) -> usize {
        let offset =
            1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) + 8 * n;
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap()) as usize
    }

    fn table_id(&self, n: usize) -> Option<u64> {
        debug_assert!(n < BTREE_ORDER - 1);
        let len = self.key_len(n);
        if len == 0 {
            return None;
        }
        let offset = 1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * n;
        Some(u64::from_be_bytes(
            self.page.memory()[offset..(offset + 8)].try_into().unwrap(),
        ))
    }

    fn table_and_key(&self, n: usize) -> Option<(u64, &[u8])> {
        debug_assert!(n < BTREE_ORDER - 1);
        let len = self.key_len(n);
        if len == 0 {
            return None;
        }
        let offset = 1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * n;
        let table =
            u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap());
        let offset = self.key_offset(n);
        Some((table, &self.page.memory()[offset..(offset + len)]))
    }

    fn key(&self, n: usize) -> Option<&[u8]> {
        debug_assert!(n < BTREE_ORDER - 1);
        let offset = self.key_offset(n);
        let len = self.key_len(n);
        if len == 0 {
            return None;
        }
        Some(&self.page.memory()[offset..(offset + len)])
    }

    fn count_children(&self) -> usize {
        let mut count = 1;
        for i in 0..(BTREE_ORDER - 1) {
            let length = self.key_len(i);
            if length == 0 {
                break;
            }
            count += 1;
        }
        count
    }

    fn child_page(&self, n: usize) -> Option<NodeHandle> {
        debug_assert!(n < BTREE_ORDER);
        if n > 0 && self.key_len(n - 1) == 0 {
            return None;
        }

        // Search the delta messages
        let base = 1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) * 3;
        // TODO: this rposition call could maybe be optimized with SIMD
        if let Some(index) = self.page.memory()[base..(base + self.valid_messages as usize)]
            .iter()
            .rposition(|x| *x == n as u8)
        {
            let offset = 1
                + NodeHandle::serialized_size() * BTREE_ORDER
                + 8 * (BTREE_ORDER - 1) * 3
                + MESSAGE_BUFFER
                + NodeHandle::serialized_size() * index;
            return Some(NodeHandle::from_be_bytes(
                self.page.memory()[offset..(offset + NodeHandle::serialized_size())]
                    .try_into()
                    .unwrap(),
            ));
        }

        let offset = 1 + NodeHandle::serialized_size() * n;
        Some(NodeHandle::from_be_bytes(
            self.page.memory()[offset..(offset + NodeHandle::serialized_size())]
                .try_into()
                .unwrap(),
        ))
    }

    fn total_key_length(&self) -> usize {
        let mut len = 0;
        for i in 0..(BTREE_ORDER - 1) {
            len += self.key_len(i);
        }

        len
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
// TODO: change layout to include a length field, instead of always allocating enough fixed size
// slots for BTREE_ORDER entries. This will free up extra space for long keys
// Layout is:
// 1 byte: type
// repeating (BTREE_ORDER times):
// 8 bytes: node handle
// repeating (BTREE_ORDER - 1 times):
// * 8 bytes: table id
// repeating (BTREE_ORDER - 1 times):
// * 8 bytes: key len. Zero length indicates no key, or following page
// repeating (BTREE_ORDER - 1 times):
// * 8 bytes: key offset. Offset to the key data
// TODO: re-assess whether these delta messages are worthwhile. They hurt read performance,
// in the current implementation
// repeating (MESSAGE_BUFFER times):
// 1 byte: child index. Replacement messages, should be read last to first
// repeating (MESSAGE_BUFFER times):
// 8 bytes: node handle. Replacement messages, should be read last to first
// repeating (BTREE_ORDER - 1 times):
// * n bytes: key data
struct InternalBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> InternalBuilder<'a, 'b> {
    fn required_bytes(size_of_keys: usize) -> usize {
        let fixed_size = 1
            + NodeHandle::serialized_size() * BTREE_ORDER
            + 8 * (BTREE_ORDER - 1) * 3
            + (1 + NodeHandle::serialized_size()) * MESSAGE_BUFFER;
        size_of_keys + fixed_size
    }

    fn new(page: &'b mut PageMut<'a>) -> Self {
        page.memory_mut()[0] = INTERNAL;
        //  ensure all the key lengths are zeroed, since we use those to indicate missing keys
        let start = 1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1);
        for i in 0..(BTREE_ORDER - 1) {
            let offset = start + 8 * i;
            page.memory_mut()[offset..(offset + 8)].copy_from_slice(&(0u64).to_be_bytes());
        }
        InternalBuilder { page }
    }

    fn write_first_page(&mut self, node_handle: NodeHandle) {
        let offset = 1;
        self.page.memory_mut()[offset..(offset + NodeHandle::serialized_size())]
            .copy_from_slice(&node_handle.to_be_bytes());
    }

    fn key_offset(&self, n: usize) -> usize {
        let offset =
            1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) * 2 + 8 * n;
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap()) as usize
    }

    fn key_len(&self, n: usize) -> usize {
        let offset =
            1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) + 8 * n;
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap()) as usize
    }

    // Write the nth key and page of values greater than this key, but less than or equal to the next
    // Caller must write keys & pages in increasing order
    fn write_nth_key(&mut self, table_id: u64, key: &[u8], handle: NodeHandle, n: usize) {
        assert!(n < BTREE_ORDER - 1);
        let offset = 1 + NodeHandle::serialized_size() * (n + 1);
        self.page.memory_mut()[offset..(offset + NodeHandle::serialized_size())]
            .copy_from_slice(&handle.to_be_bytes());

        let offset = 1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * n;
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&table_id.to_be_bytes());

        let offset =
            1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) + 8 * n;
        self.page.memory_mut()[offset..(offset + 8)]
            .copy_from_slice(&(key.len() as u64).to_be_bytes());

        let offset =
            1 + NodeHandle::serialized_size() * BTREE_ORDER + 8 * (BTREE_ORDER - 1) * 2 + 8 * n;
        let data_offset = if n > 0 {
            self.key_offset(n - 1) + self.key_len(n - 1)
        } else {
            1 + NodeHandle::serialized_size() * BTREE_ORDER
                + 8 * (BTREE_ORDER - 1) * 3
                + (1 + NodeHandle::serialized_size()) * MESSAGE_BUFFER
        };
        self.page.memory_mut()[offset..(offset + 8)]
            .copy_from_slice(&(data_offset as u64).to_be_bytes());

        self.page.memory_mut()[data_offset..(data_offset + key.len())].copy_from_slice(key);
    }
}

struct InternalMutator<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> InternalMutator<'a, 'b> {
    fn new(page: &'b mut PageMut<'a>) -> Self {
        assert_eq!(page.memory()[0], INTERNAL);
        Self { page }
    }

    fn can_write_delta_message(&self, message_byte_offset: u32) -> bool {
        message_byte_offset < MESSAGE_BUFFER as u32
    }

    // Returns the new valid message offset
    fn write_delta_message(
        &mut self,
        existing_messages: u32,
        child_index: u8,
        handle: NodeHandle,
    ) -> u32 {
        assert!(child_index < BTREE_ORDER as u8);
        assert!(existing_messages < MESSAGE_BUFFER as u32);
        let offset = 1
            + NodeHandle::serialized_size() * BTREE_ORDER
            + 8 * (BTREE_ORDER - 1) * 3
            + existing_messages as usize;
        self.page.memory_mut()[offset] = child_index;
        let offset = 1
            + NodeHandle::serialized_size() * BTREE_ORDER
            + 8 * (BTREE_ORDER - 1) * 3
            + MESSAGE_BUFFER
            + NodeHandle::serialized_size() * existing_messages as usize;
        self.page.memory_mut()[offset..(offset + NodeHandle::serialized_size())]
            .copy_from_slice(&handle.to_be_bytes());

        existing_messages + 1
    }

    fn write_child_page(&mut self, i: usize, node_handle: NodeHandle) {
        let offset = 1 + NodeHandle::serialized_size() * i;
        self.page.memory_mut()[offset..(offset + NodeHandle::serialized_size())]
            .copy_from_slice(&node_handle.to_be_bytes());
    }
}

pub(in crate) fn tree_height<'a>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    manager: &'a TransactionalMemory,
) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => 1,
        INTERNAL => {
            let accessor = InternalAccessor::new(&page, valid_message_bytes);
            let mut max_child_height = 0;
            for i in 0..BTREE_ORDER {
                if let Some(child) = accessor.child_page(i) {
                    let height = tree_height(
                        manager.get_page(child.get_page_number()),
                        child.get_valid_messages(),
                        manager,
                    );
                    max_child_height = max(max_child_height, height);
                }
            }

            max_child_height + 1
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn print_node(page: &impl Page, valid_message_bytes: u32) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page);
            eprint!(
                "Leaf[ (page={:?}), lt_table={} lt_key={:?}",
                page.get_page_number(),
                accessor.lesser().table_id(),
                accessor.lesser().key()
            );
            if let Some(greater) = accessor.greater() {
                eprint!(
                    " gt_table={} gt_key={:?}",
                    greater.table_id(),
                    greater.key()
                );
            }
            eprint!("]");
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(page, valid_message_bytes);
            eprint!(
                "Internal[ (page={:?}/{}), child_0={:?}",
                page.get_page_number(),
                valid_message_bytes,
                accessor.child_page(0).unwrap().get_page_number()
            );
            for i in 0..(BTREE_ORDER - 1) {
                if let Some(child) = accessor.child_page(i + 1) {
                    let table = accessor.table_id(i).unwrap();
                    let key = accessor.key(i).unwrap();
                    eprint!(" table_{}={}", i, table);
                    eprint!(" key_{}={:?}", i, key);
                    eprint!(" child_{}={:?}", i + 1, child.get_page_number());
                }
            }
            eprint!("]");
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn node_children<'a>(
    page: &PageImpl<'a>,
    valid_message_bytes: u32,
    manager: &'a TransactionalMemory,
) -> Vec<(PageImpl<'a>, u32)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            vec![]
        }
        INTERNAL => {
            let mut children = vec![];
            let accessor = InternalAccessor::new(page, valid_message_bytes);
            for i in 0..BTREE_ORDER {
                if let Some(child) = accessor.child_page(i) {
                    children.push((
                        manager.get_page(child.get_page_number()),
                        child.get_valid_messages(),
                    ));
                }
            }
            children
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn print_tree<'a>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    manager: &'a TransactionalMemory,
) {
    let mut pages = vec![(page, valid_message_bytes)];
    while !pages.is_empty() {
        let mut next_children = vec![];
        for (page, message_bytes) in pages.drain(..) {
            next_children.extend(node_children(&page, message_bytes, manager));
            print_node(&page, message_bytes);
            eprint!("  ");
        }
        eprintln!();

        pages = next_children;
    }
}

// Returns the new root, and a list of freed pages
pub(in crate) fn tree_delete<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    table: u64,
    key: &[u8],
    manager: &'a TransactionalMemory,
) -> (Option<NodeHandle>, Vec<PageNumber>) {
    let mut freed = vec![];
    let result =
        match tree_delete_helper::<K>(page, valid_message_bytes, table, key, &mut freed, manager) {
            DeletionResult::Subtree(page) => Some(page),
            DeletionResult::PartialLeaf(entries) => {
                assert!(entries.is_empty());
                None
            }
            DeletionResult::PartialInternal(pages) => {
                assert_eq!(pages.len(), 1);
                Some(pages[0])
            }
        };
    (result, freed)
}

#[derive(Debug)]
enum DeletionResult {
    // A proper subtree
    Subtree(NodeHandle),
    // A leaf subtree with too few entries
    PartialLeaf(Vec<(u64, Vec<u8>, Vec<u8>)>),
    // A index page subtree with too few children
    PartialInternal(Vec<NodeHandle>),
}

// Must return the pages in order
fn split_leaf(
    leaf: NodeHandle,
    partial: &[(u64, Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
) -> Option<(NodeHandle, NodeHandle)> {
    assert!(partial.is_empty());
    let page = manager.get_page(leaf.get_page_number());
    let accessor = LeafAccessor::new(&page);
    if let Some(greater) = accessor.greater() {
        let lesser = accessor.lesser();
        let page1 = make_single_leaf(lesser.table_id(), lesser.key(), lesser.value(), manager);
        let page2 = make_single_leaf(greater.table_id(), greater.key(), greater.value(), manager);
        Some((page1, page2))
    } else {
        None
    }
}

fn merge_leaf(
    leaf: NodeHandle,
    partial: &[(u64, Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
) -> NodeHandle {
    let page = manager.get_page(leaf.get_page_number());
    let accessor = LeafAccessor::new(&page);
    assert!(accessor.greater().is_none());
    assert!(partial.is_empty());
    leaf
}

// Splits the page, if necessary, to fit the additional pages in `partial`
// Returns the pages in order
fn split_index(
    index: NodeHandle,
    partial: &[NodeHandle],
    manager: &TransactionalMemory,
) -> Option<(NodeHandle, NodeHandle)> {
    let page = manager.get_page(index.get_page_number());
    let accessor = InternalAccessor::new(&page, index.get_valid_messages());

    let has_enough_slots = accessor.child_page(BTREE_ORDER - partial.len()).is_none();
    let required_key_space: usize = partial
        .iter()
        .map(|p| {
            max_table_key(
                manager.get_page(p.get_page_number()),
                p.get_valid_messages(),
                manager,
            )
            .1
            .len()
        })
        .sum();
    // TODO: Note we could get a false negative here, since we don't need to store the last key
    // The total_key_length calculation below does it correctly
    let has_space = accessor.total_key_length() + required_key_space < MAX_KEY_SPACE_PER_PAGE;
    if has_space && has_enough_slots {
        return None;
    }

    let mut pages = vec![];
    pages.extend_from_slice(partial);
    for i in 0..BTREE_ORDER {
        if let Some(child) = accessor.child_page(i) {
            pages.push(child);
        }
    }

    pages.sort_by_key(|p| {
        max_table_key(
            manager.get_page(p.get_page_number()),
            p.get_valid_messages(),
            manager,
        )
    });

    let total_key_length: usize = pages
        .iter()
        .map(|p| {
            max_table_key(
                manager.get_page(p.get_page_number()),
                p.get_valid_messages(),
                manager,
            )
            .1
            .len()
        })
        .sum();
    let division = if total_key_length < MAX_KEY_SPACE_PER_PAGE {
        // Use tree order if we did not run out of space
        pages.len() / 2
    } else {
        // Otherwise balance the nodes based on the key size
        let mut index = pages.len() - 2;
        let mut cumulative = 0;
        for (i, p) in pages.iter().enumerate() {
            cumulative += max_table_key(
                manager.get_page(p.get_page_number()),
                p.get_valid_messages(),
                manager,
            )
            .1
            .len();
            if cumulative > total_key_length / 2 {
                index = i;
                break;
            }
        }
        index
    };

    let page1 = make_index_many_pages(&pages[0..division], manager);
    let page2 = make_index_many_pages(&pages[division..], manager);

    Some((page1, page2))
}

// Pages must be in sorted order
fn make_index_many_pages(children: &[NodeHandle], manager: &TransactionalMemory) -> NodeHandle {
    let mut tables_and_keys = vec![];
    let mut key_size = 0;
    for i in 1..children.len() {
        let entry = max_table_key(
            manager.get_page(children[i - 1].get_page_number()),
            children[i - 1].get_valid_messages(),
            manager,
        );
        key_size += entry.1.len();
        tables_and_keys.push(entry);
    }
    let mut page = manager.allocate(InternalBuilder::required_bytes(key_size));
    let mut builder = InternalBuilder::new(&mut page);
    builder.write_first_page(children[0]);
    for i in 1..children.len() {
        let (table, key) = &tables_and_keys[i - 1];
        builder.write_nth_key(*table, key, children[i], i - 1);
    }
    NodeHandle::new(page.get_page_number(), 0)
}

fn merge_index(
    index: NodeHandle,
    partial: &[NodeHandle],
    manager: &TransactionalMemory,
) -> NodeHandle {
    let page = manager.get_page(index.get_page_number());
    let accessor = InternalAccessor::new(&page, index.get_valid_messages());
    assert!(accessor.child_page(BTREE_ORDER - partial.len()).is_none());

    let mut pages = vec![];
    pages.extend_from_slice(partial);
    for i in 0..BTREE_ORDER {
        if let Some(page_number) = accessor.child_page(i) {
            pages.push(page_number);
        }
    }

    pages.sort_by_key(|p| {
        max_table_key(
            manager.get_page(p.get_page_number()),
            p.get_valid_messages(),
            manager,
        )
    });
    assert!(pages.len() <= BTREE_ORDER);

    make_index_many_pages(&pages, manager)
}

fn repair_children(
    children: Vec<DeletionResult>,
    manager: &TransactionalMemory,
) -> Vec<NodeHandle> {
    if children.iter().all(|x| matches!(x, Subtree(_))) {
        children
            .iter()
            .map(|x| match x {
                Subtree(page_number) => *page_number,
                _ => unreachable!(),
            })
            .collect()
    } else if children.iter().any(|x| matches!(x, PartialLeaf(_))) {
        let mut result = vec![];
        let mut repaired = false;
        // For each whole subtree, try to merge it with a partial left to repair it, if one is neighboring
        for i in 0..children.len() {
            if let Subtree(handle) = &children[i] {
                if repaired {
                    result.push(*handle);
                    continue;
                }
                let offset = if i > 0 { i - 1 } else { i + 1 };
                if let Some(PartialLeaf(partials)) = children.get(offset) {
                    if let Some((page1, page2)) = split_leaf(*handle, partials, manager) {
                        result.push(page1);
                        result.push(page2);
                    } else {
                        result.push(merge_leaf(*handle, partials, manager));
                    }
                    repaired = true;
                } else {
                    // No adjacent partial
                    result.push(*handle);
                }
            }
        }
        result
    } else if children.iter().any(|x| matches!(x, PartialInternal(_))) {
        let mut result = vec![];
        let mut repaired = false;
        // For each whole subtree, try to merge it with a partial left to repair it, if one is neighboring
        for i in 0..children.len() {
            if let Subtree(page_number) = &children[i] {
                if repaired {
                    result.push(*page_number);
                    continue;
                }
                let offset = if i > 0 { i - 1 } else { i + 1 };
                if let Some(PartialInternal(partials)) = children.get(offset) {
                    if let Some((page1, page2)) = split_index(*page_number, partials, manager) {
                        result.push(page1);
                        result.push(page2);
                    } else {
                        result.push(merge_index(*page_number, partials, manager));
                    }
                    repaired = true;
                } else {
                    // No adjacent partial
                    result.push(*page_number);
                }
            }
        }
        result
    } else {
        unreachable!()
    }
}

fn max_table_key(
    page: PageImpl,
    valid_message_bytes: u32,
    manager: &TransactionalMemory,
) -> (u64, Vec<u8>) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            if let Some(greater) = accessor.greater() {
                (greater.table_id(), greater.key().to_vec())
            } else {
                (
                    accessor.lesser().table_id(),
                    accessor.lesser().key().to_vec(),
                )
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page, valid_message_bytes);
            for i in (0..BTREE_ORDER).rev() {
                if let Some(child) = accessor.child_page(i) {
                    return max_table_key(
                        manager.get_page(child.get_page_number()),
                        child.get_valid_messages(),
                        manager,
                    );
                }
            }
            unreachable!();
        }
        _ => unreachable!(),
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
#[allow(clippy::needless_return)]
fn tree_delete_helper<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    table: u64,
    key: &[u8],
    freed: &mut Vec<PageNumber>,
    manager: &'a TransactionalMemory,
) -> DeletionResult {
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
                    return Subtree(NodeHandle::new(page.get_page_number(), valid_message_bytes));
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

                freed.push(page.get_page_number());
                Subtree(make_single_leaf(
                    new_leaf.0, new_leaf.1, new_leaf.2, manager,
                ))
            } else {
                if accessor.lesser().compare::<K>(table, key).is_eq() {
                    // Deleted the entire left
                    freed.push(page.get_page_number());
                    PartialLeaf(vec![])
                } else {
                    // Not found
                    Subtree(NodeHandle::new(page.get_page_number(), valid_message_bytes))
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page, valid_message_bytes);
            let original_page_number = page.get_page_number();
            let mut children = vec![];
            let mut found = false;
            let mut last_valid_child = BTREE_ORDER - 1;
            for i in 0..(BTREE_ORDER - 1) {
                if let Some(index_table) = accessor.table_id(i) {
                    let index_key = accessor.key(i).unwrap();
                    let child_page = accessor.child_page(i).unwrap();
                    if cmp_keys::<K>(table, key, index_table, index_key).is_le() && !found {
                        found = true;
                        let result = tree_delete_helper::<K>(
                            manager.get_page(child_page.get_page_number()),
                            child_page.get_valid_messages(),
                            table,
                            key,
                            freed,
                            manager,
                        );
                        // The key must not have been found, since the subtree didn't change
                        if let Subtree(page_number) = result {
                            if page_number == child_page {
                                return Subtree(NodeHandle::new(
                                    original_page_number,
                                    valid_message_bytes,
                                ));
                            }
                        }
                        children.push(result);
                    } else {
                        children.push(Subtree(child_page));
                    }
                } else {
                    last_valid_child = i;
                    break;
                }
            }
            let last_page = accessor.child_page(last_valid_child).unwrap();
            if found {
                // Already found the insertion place, so just copy
                children.push(Subtree(last_page));
            } else {
                let result = tree_delete_helper::<K>(
                    manager.get_page(last_page.get_page_number()),
                    last_page.get_valid_messages(),
                    table,
                    key,
                    freed,
                    manager,
                );
                found = true;
                // The key must not have been found, since the subtree didn't change
                if let Subtree(page_number) = result {
                    if page_number == last_page {
                        return Subtree(NodeHandle::new(original_page_number, valid_message_bytes));
                    }
                }
                children.push(result);
            }
            assert!(found);
            assert!(children.len() > 1);
            freed.push(original_page_number);
            let children = repair_children(children, manager);
            if children.len() == 1 {
                return PartialInternal(children);
            }

            Subtree(make_index_many_pages(&children, manager))
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn make_mut_single_leaf<'a>(
    table: u64,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> (NodeHandle, AccessGuardMut<'a>) {
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key, value]));
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(table, key, value);
    builder.write_greater(None);

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value.len());

    (NodeHandle::new(page_num, 0), guard)
}

pub(in crate) fn make_mut_double_leaf_right<'a, K: RedbKey + ?Sized>(
    table1: u64,
    key1: &[u8],
    value1: &[u8],
    table2: u64,
    key2: &[u8],
    value2: &[u8],
    manager: &'a TransactionalMemory,
) -> (NodeHandle, AccessGuardMut<'a>) {
    debug_assert!(cmp_keys::<K>(table1, key1, table2, key2).is_lt());
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key1, value1, key2, value2]));
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(table1, key1, value1);
    builder.write_greater(Some((table2, key2, value2)));

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_greater() + accessor.greater().unwrap().value_offset();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value2.len());

    (NodeHandle::new(page_num, 0), guard)
}

pub(in crate) fn make_mut_double_leaf_left<'a, K: RedbKey + ?Sized>(
    table1: u64,
    key1: &[u8],
    value1: &[u8],
    table2: u64,
    key2: &[u8],
    value2: &[u8],
    manager: &'a TransactionalMemory,
) -> (NodeHandle, AccessGuardMut<'a>) {
    debug_assert!(cmp_keys::<K>(table1, key1, table2, key2).is_lt());
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key1, value1, key2, value2]));
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(table1, key1, value1);
    builder.write_greater(Some((table2, key2, value2)));

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value1.len());

    (NodeHandle::new(page_num, 0), guard)
}

pub(in crate) fn make_single_leaf<'a>(
    table: u64,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> NodeHandle {
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key, value]));
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(table, key, value);
    builder.write_greater(None);
    NodeHandle::new(page.get_page_number(), 0)
}

pub(in crate) fn make_index(
    table: u64,
    key: &[u8],
    lte_page: NodeHandle,
    gt_page: NodeHandle,
    manager: &TransactionalMemory,
) -> NodeHandle {
    let mut page = manager.allocate(InternalBuilder::required_bytes(key.len()));
    let mut builder = InternalBuilder::new(&mut page);
    builder.write_first_page(lte_page);
    builder.write_nth_key(table, key, gt_page, 0);
    NodeHandle::new(page.get_page_number(), 0)
}

// Returns the page number of the sub-tree into which the key was inserted,
// and the guard which can be used to access the value, and a list of freed pages
pub(in crate) fn tree_insert<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    table: u64,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> (NodeHandle, AccessGuardMut<'a>, Vec<PageNumber>) {
    let mut freed = vec![];
    let (page1, more, guard) = tree_insert_helper::<K>(
        page,
        valid_message_bytes,
        table,
        key,
        value,
        &mut freed,
        manager,
    );

    if let Some((table, key, page2)) = more {
        let index_page = make_index(table, &key, page1, page2, manager);
        (index_page, guard, freed)
    } else {
        (page1, guard, freed)
    }
}

// Patch is applied at patch_index of the accessor children, using patch_handle to replace the child,
// and inserting patch_extension after it
// copies [start_child, end_child)
fn copy_to_builder_and_patch<'a>(
    accessor: &InternalAccessor<PageImpl<'a>>,
    start_child: usize,
    end_child: usize,
    builder: &mut InternalBuilder,
    patch_index: u8,
    patch_handle: NodeHandle,
    patch_extension: Option<(u64, &[u8], NodeHandle)>,
) {
    let mut dest = 0;
    if patch_index as usize == start_child {
        builder.write_first_page(patch_handle);
        if let Some((extra_table, extra_key, extra_handle)) = patch_extension {
            builder.write_nth_key(extra_table, extra_key, extra_handle, dest);
            dest += 1;
        }
    } else {
        builder.write_first_page(accessor.child_page(start_child).unwrap());
    }

    for i in (start_child + 1)..end_child {
        if let Some((table, key)) = accessor.table_and_key(i - 1) {
            let handle = if i == patch_index as usize {
                patch_handle
            } else {
                accessor.child_page(i).unwrap()
            };
            builder.write_nth_key(table, key, handle, dest);
            dest += 1;
            if i == patch_index as usize {
                if let Some((extra_table, extra_key, extra_handle)) = patch_extension {
                    builder.write_nth_key(extra_table, extra_key, extra_handle, dest);
                    dest += 1;
                };
            }
        } else {
            break;
        }
    }
}

#[allow(clippy::type_complexity)]
fn tree_insert_helper<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    table: u64,
    key: &[u8],
    value: &[u8],
    freed: &mut Vec<PageNumber>,
    manager: &'a TransactionalMemory,
) -> (
    NodeHandle,
    Option<(u64, Vec<u8>, NodeHandle)>,
    AccessGuardMut<'a>,
) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            if let Some(entry) = accessor.greater() {
                match entry.compare::<K>(table, key) {
                    Ordering::Less => {
                        // New entry goes in a new page to the right, so leave this page untouched
                        let left_page = page.get_page_number();

                        let (right_page, guard) = make_mut_single_leaf(table, key, value, manager);

                        (
                            NodeHandle::new(left_page, valid_message_bytes),
                            Some((entry.table_id(), entry.key().to_vec(), right_page)),
                            guard,
                        )
                    }
                    Ordering::Equal => {
                        let (new_page, guard) = make_mut_double_leaf_right::<K>(
                            accessor.lesser().table_id(),
                            accessor.lesser().key(),
                            accessor.lesser().value(),
                            table,
                            key,
                            value,
                            manager,
                        );

                        if !manager.free_if_uncommitted(page.get_page_number()) {
                            freed.push(page.get_page_number());
                        }

                        (new_page, None, guard)
                    }
                    Ordering::Greater => {
                        let right_table = entry.table_id();
                        let right_key = entry.key();
                        let right_value = entry.value();

                        let left_table = accessor.lesser().table_id();
                        let left_key = accessor.lesser().key();
                        let left_value = accessor.lesser().value();

                        match accessor.lesser().compare::<K>(table, key) {
                            Ordering::Less => {
                                let (left, guard) = make_mut_double_leaf_right::<K>(
                                    left_table, left_key, left_value, table, key, value, manager,
                                );
                                let right =
                                    make_single_leaf(right_table, right_key, right_value, manager);

                                if !manager.free_if_uncommitted(page.get_page_number()) {
                                    freed.push(page.get_page_number());
                                }

                                (left, Some((table, key.to_vec(), right)), guard)
                            }
                            Ordering::Equal => {
                                let (new_page, guard) = make_mut_double_leaf_left::<K>(
                                    table,
                                    key,
                                    value,
                                    right_table,
                                    right_key,
                                    right_value,
                                    manager,
                                );

                                if !manager.free_if_uncommitted(page.get_page_number()) {
                                    freed.push(page.get_page_number());
                                }

                                (new_page, None, guard)
                            }
                            Ordering::Greater => {
                                let (left, guard) = make_mut_double_leaf_left::<K>(
                                    table, key, value, left_table, left_key, left_value, manager,
                                );
                                let right =
                                    make_single_leaf(right_table, right_key, right_value, manager);

                                if !manager.free_if_uncommitted(page.get_page_number()) {
                                    freed.push(page.get_page_number());
                                }

                                (left, Some((left_table, left_key.to_vec(), right)), guard)
                            }
                        }
                    }
                }
            } else {
                let (new_page, guard) = match cmp_keys::<K>(
                    accessor.lesser().table_id(),
                    accessor.lesser().key(),
                    table,
                    key,
                ) {
                    Ordering::Less => make_mut_double_leaf_right::<K>(
                        accessor.lesser().table_id(),
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        table,
                        key,
                        value,
                        manager,
                    ),
                    Ordering::Equal => make_mut_single_leaf(table, key, value, manager),
                    Ordering::Greater => make_mut_double_leaf_left::<K>(
                        table,
                        key,
                        value,
                        accessor.lesser().table_id(),
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        manager,
                    ),
                };

                if !manager.free_if_uncommitted(page.get_page_number()) {
                    freed.push(page.get_page_number());
                }

                (new_page, None, guard)
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page, valid_message_bytes);
            // Delta message can only be used if the keys did not change
            let (child_index, child_page) = accessor.child_for_key::<K>(table, key);
            let (page1, more, guard) = tree_insert_helper::<K>(
                manager.get_page(child_page.get_page_number()),
                child_page.get_valid_messages(),
                table,
                key,
                value,
                freed,
                manager,
            );

            if let Some((index_table2, index_key2, page2)) = more {
                let new_children_count = 1 + accessor.count_children();
                let new_total_key_size = index_key2.len() + accessor.total_key_length();

                if new_children_count <= BTREE_ORDER && new_total_key_size < MAX_KEY_SPACE_PER_PAGE
                {
                    // Rewrite page since we're splitting a child
                    let mut new_page =
                        manager.allocate(accessor.total_key_length() + index_key2.len());
                    let mut builder = InternalBuilder::new(&mut new_page);

                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        BTREE_ORDER,
                        &mut builder,
                        child_index as u8,
                        page1,
                        Some((index_table2, &index_key2, page2)),
                    );
                    // Free the original page, since we've replaced it
                    if !manager.free_if_uncommitted(page.get_page_number()) {
                        freed.push(page.get_page_number());
                    }
                    (NodeHandle::new(new_page.get_page_number(), 0), None, guard)
                } else {
                    // TODO: optimize to remove these Vecs
                    let mut children = vec![];
                    let mut index_table_keys: Vec<(u64, &[u8])> = vec![];

                    if child_index == 0 {
                        children.push(page1);
                        index_table_keys.push((index_table2, &index_key2));
                        children.push(page2);
                    } else {
                        children.push(accessor.child_page(0).unwrap());
                    };
                    for i in 1..BTREE_ORDER {
                        if let Some((temp_table, temp_key)) = accessor.table_and_key(i - 1) {
                            index_table_keys.push((temp_table, temp_key));
                            if i == child_index as usize {
                                children.push(page1);
                                index_table_keys.push((index_table2, &index_key2));
                                children.push(page2);
                            } else {
                                children.push(accessor.child_page(i).unwrap());
                            };
                        } else {
                            break;
                        }
                    }

                    let division = if new_total_key_size < MAX_KEY_SPACE_PER_PAGE {
                        // Use tree order if we did not run out of space
                        BTREE_ORDER / 2
                    } else {
                        // Otherwise balance the nodes based on the key size
                        let mut index = index_table_keys.len() - 1;
                        let mut cumulative = 0;
                        for (i, (_, key)) in index_table_keys.iter().enumerate() {
                            cumulative += key.len();
                            if cumulative > new_total_key_size / 2 {
                                index = i;
                                break;
                            }
                        }
                        index
                    };

                    // Rewrite page since we're splitting a child
                    let key_size = index_table_keys[0..division]
                        .iter()
                        .map(|(_, k)| k.len())
                        .sum();
                    let mut new_page = manager.allocate(InternalBuilder::required_bytes(key_size));
                    let mut builder = InternalBuilder::new(&mut new_page);

                    builder.write_first_page(children[0]);
                    for i in 0..division {
                        let (table, key) = &index_table_keys[i];
                        builder.write_nth_key(*table, key, children[i + 1], i);
                    }

                    let (index_table, index_key) = &index_table_keys[division];

                    let key_size = index_table_keys[(division + 1)..]
                        .iter()
                        .map(|(_, k)| k.len())
                        .sum();
                    let mut new_page2 = manager.allocate(InternalBuilder::required_bytes(key_size));
                    let mut builder2 = InternalBuilder::new(&mut new_page2);
                    builder2.write_first_page(children[division + 1]);
                    for i in (division + 1)..index_table_keys.len() {
                        let (table, key) = &index_table_keys[i];
                        builder2.write_nth_key(*table, key, children[i + 1], i - (division + 1));
                    }

                    // Free the original page, since we've replaced it
                    if !manager.free_if_uncommitted(page.get_page_number()) {
                        freed.push(page.get_page_number());
                    }
                    (
                        NodeHandle::new(new_page.get_page_number(), 0),
                        Some((
                            *index_table,
                            index_key.to_vec(),
                            NodeHandle::new(new_page2.get_page_number(), 0),
                        )),
                        guard,
                    )
                }
            } else {
                let mut mutpage = manager.get_page_mut(page.get_page_number());
                let mut mutator = InternalMutator::new(&mut mutpage);
                if page1 == child_page {
                    // NO-OP. One of our descendants is uncommitted, so there was no change
                    (
                        NodeHandle::new(mutpage.get_page_number(), valid_message_bytes),
                        None,
                        guard,
                    )
                } else if manager.uncommitted(page.get_page_number()) {
                    assert_eq!(valid_message_bytes, 0);
                    mutator.write_child_page(child_index, page1);
                    (
                        NodeHandle::new(mutpage.get_page_number(), valid_message_bytes),
                        None,
                        guard,
                    )
                } else if mutator.can_write_delta_message(valid_message_bytes) {
                    let new_messages =
                        mutator.write_delta_message(valid_message_bytes, child_index as u8, page1);
                    (
                        NodeHandle::new(mutpage.get_page_number(), new_messages),
                        None,
                        guard,
                    )
                } else {
                    // Page is full of delta messages, so rewrite it
                    let mut new_page = manager
                        .allocate(InternalBuilder::required_bytes(accessor.total_key_length()));
                    let mut builder = InternalBuilder::new(&mut new_page);
                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        BTREE_ORDER,
                        &mut builder,
                        child_index as u8,
                        page1,
                        None,
                    );

                    // Free the original page, since we've replaced it
                    if !manager.free_if_uncommitted(page.get_page_number()) {
                        freed.push(page.get_page_number());
                    }
                    (NodeHandle::new(new_page.get_page_number(), 0), None, guard)
                }
            }
        }
        _ => unreachable!(),
    }
}

// Returns the (offset, len) of the value for the queried key, if present
pub(in crate) fn lookup_in_raw<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    valid_message_bytes: u32,
    table: u64,
    query: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<(PageImpl<'a>, usize, usize)> {
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
            let accessor = InternalAccessor::new(&page, valid_message_bytes);
            let (_, child_page) = accessor.child_for_key::<K>(table, query);
            return lookup_in_raw::<K>(
                manager.get_page(child_page.get_page_number()),
                child_page.get_valid_messages(),
                table,
                query,
                manager,
            );
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::btree_utils::BTREE_ORDER;
    use crate::{Database, Table};
    use tempfile::NamedTempFile;

    #[test]
    fn tree_balance() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        let db = unsafe { Database::open(tmpfile.path(), 16 * 1024 * 1024).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

        let elements = (BTREE_ORDER / 2).pow(2) as usize;

        let mut txn = table.begin_write().unwrap();
        for i in (0..elements).rev() {
            txn.insert(&i.to_be_bytes(), b"").unwrap();
        }
        txn.commit().unwrap();

        let expected_height = (elements as f32).log((BTREE_ORDER / 2) as f32) as usize + 1;
        let height = db.stats().unwrap().tree_height();
        assert!(
            height <= expected_height,
            "height={} expected={}",
            height,
            expected_height
        );

        let reduce_to = BTREE_ORDER / 2;

        let mut txn = table.begin_write().unwrap();
        for i in 0..(elements - reduce_to) {
            txn.remove(&i.to_be_bytes()).unwrap();
        }
        txn.commit().unwrap();

        let expected_height = (reduce_to as f32).log((BTREE_ORDER / 2) as f32) as usize + 1;
        let height = db.stats().unwrap().tree_height();
        assert!(
            height <= expected_height,
            "height={} expected={}",
            height,
            expected_height
        );
    }
}
