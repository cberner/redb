use crate::tree_store::btree_base::{EntryAccessor, InternalAccessor, LeafAccessor};
use crate::tree_store::btree_base::{BTREE_ORDER, INTERNAL, LEAF};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::{BtreeEntry, PageNumber};
use crate::types::{RedbKey, RedbValue};
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::RangeBounds;

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

#[derive(Debug)]
pub enum RangeIterState<'a> {
    Leaf {
        page: PageImpl<'a>,
        entry: usize,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    Internal {
        page: PageImpl<'a>,
        child: usize,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
}

impl<'a> RangeIterState<'a> {
    fn page_number(&self) -> PageNumber {
        match self {
            Leaf { page, .. } => page.get_page_number(),
            Internal { page, .. } => page.get_page_number(),
        }
    }

    fn next(self, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
        match self {
            Leaf {
                page,
                entry,
                parent,
                reversed,
            } => {
                let accessor = LeafAccessor::new(&page);
                let direction = if reversed { -1 } else { 1 };
                let next_entry = entry as isize + direction;
                if 0 <= next_entry && next_entry < accessor.num_pairs() as isize {
                    Some(Leaf {
                        page,
                        entry: next_entry as usize,
                        parent,
                        reversed,
                    })
                } else {
                    parent.map(|x| *x)
                }
            }
            Internal {
                page,
                child,
                mut parent,
                reversed,
            } => {
                let accessor = InternalAccessor::new(&page);
                let child_page = accessor.child_page(child).unwrap();
                let child_page = manager.get_page(child_page);
                let direction = if reversed { -1 } else { 1 };
                let next_child = child as isize + direction;
                if 0 <= next_child && next_child < accessor.count_children() as isize {
                    parent = Some(Box::new(Internal {
                        page,
                        child: next_child as usize,
                        parent,
                        reversed,
                    }));
                }
                match child_page.memory()[0] {
                    LEAF => {
                        let child_accessor = LeafAccessor::new(&child_page);
                        let entry = if reversed {
                            child_accessor.num_pairs() - 1
                        } else {
                            0
                        };
                        Some(Leaf {
                            page: child_page,
                            entry,
                            parent,
                            reversed,
                        })
                    }
                    INTERNAL => {
                        let child_accessor = InternalAccessor::new(&child_page);
                        let child = if reversed {
                            child_accessor.count_children() - 1
                        } else {
                            0
                        };
                        Some(Internal {
                            page: child_page,
                            child,
                            parent,
                            reversed,
                        })
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn get_entry(&self) -> Option<EntryAccessor> {
        match self {
            Leaf { page, entry, .. } => LeafAccessor::new(page).entry(*entry),
            _ => None,
        }
    }
}

pub(crate) struct AllPageNumbersBtreeIter<'a> {
    next: Option<RangeIterState<'a>>,
    manager: &'a TransactionalMemory,
}

impl<'a> AllPageNumbersBtreeIter<'a> {
    pub(crate) fn new(start: RangeIterState<'a>, manager: &'a TransactionalMemory) -> Self {
        match start {
            Leaf { entry, .. } => {
                assert_eq!(entry, 0)
            }
            Internal { child, .. } => {
                assert_eq!(child, 0)
            }
        }
        Self {
            next: Some(start),
            manager,
        }
    }
}

impl<'a> Iterator for AllPageNumbersBtreeIter<'a> {
    type Item = PageNumber;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = self.next.take()?;
        let value = state.page_number();
        // Only return each page number the first time we visit it
        loop {
            if let Some(next_state) = state.next(self.manager) {
                state = next_state;
            } else {
                self.next = None;
                return Some(value);
            }

            match state {
                Leaf { entry, .. } => {
                    if entry == 0 {
                        self.next = Some(state);
                        return Some(value);
                    }
                }
                Internal { child, .. } => {
                    if child == 0 {
                        self.next = Some(state);
                        return Some(value);
                    }
                }
            }
        }
    }
}

pub(in crate) fn page_numbers_iter_start_state(page: PageImpl) -> RangeIterState {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => Leaf {
            page,
            entry: 0,
            parent: None,
            reversed: false,
        },
        INTERNAL => Internal {
            page,
            child: 0,
            parent: None,
            reversed: false,
        },
        _ => unreachable!(),
    }
}

pub struct BtreeRangeIter<
    'a,
    T: RangeBounds<KR>,
    KR: AsRef<K> + ?Sized + 'a,
    K: RedbKey + ?Sized + 'a,
    V: RedbValue + ?Sized + 'a,
> {
    // whether we've returned the value for the self.next state. We don't want to advance the initial
    // state, until it has been returned
    consumed: bool,
    next: Option<RangeIterState<'a>>,
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
        state: Option<RangeIterState<'a>>,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            consumed: false,
            next: state,
            query_range,
            reversed: false,
            manager,
            _key_type: Default::default(),
            _key_ref_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(in crate) fn new_reversed(
        state: Option<RangeIterState<'a>>,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            consumed: false,
            next: state,
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
        loop {
            if self.consumed {
                let state = self.next.take()?;
                self.next = state.next(self.manager);
                // Return None if the next state is None
                self.next.as_ref()?;
            }

            self.next.as_ref()?;

            self.consumed = true;
            if let Some(entry) = self.next.as_ref().unwrap().get_entry() {
                if bound_contains_key::<T, KR, K>(&self.query_range, entry.key()) {
                    return self.next.as_ref().map(|s| s.get_entry().unwrap());
                } else {
                    #[allow(clippy::collapsible_else_if)]
                    if self.reversed {
                        if let Bound::Included(start) = self.query_range.start_bound() {
                            if K::compare(entry.key(), start.as_ref().as_bytes().as_ref()).is_lt() {
                                self.next = None;
                            }
                        } else if let Bound::Excluded(start) = self.query_range.start_bound() {
                            if K::compare(entry.key(), start.as_ref().as_bytes().as_ref()).is_le() {
                                self.next = None;
                            }
                        }
                    } else {
                        if let Bound::Included(end) = self.query_range.end_bound() {
                            if K::compare(entry.key(), end.as_ref().as_bytes().as_ref()).is_gt() {
                                self.next = None;
                            }
                        } else if let Bound::Excluded(end) = self.query_range.end_bound() {
                            if K::compare(entry.key(), end.as_ref().as_bytes().as_ref()).is_ge() {
                                self.next = None;
                            }
                        }
                    };
                }
            }
        }
    }
}

pub(in crate) fn find_iter_unbounded_start<'a>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => Some(Leaf {
            page,
            entry: 0,
            parent,
            reversed: false,
        }),
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let child_page_number = accessor.child_page(0).unwrap();
            let child_page = manager.get_page(child_page_number);
            parent = Some(Box::new(Internal {
                page,
                child: 1,
                parent,
                reversed: false,
            }));
            find_iter_unbounded_start(child_page, parent, manager)
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn find_iter_unbounded_reversed<'a>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let entry = accessor.num_pairs() - 1;
            Some(Leaf {
                page,
                entry,
                parent,
                reversed: true,
            })
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let child_index = accessor.count_children() - 1;
            let child_page_number = accessor.child_page(child_index).unwrap();
            let child_page = manager.get_page(child_page_number);
            if child_index > 0 && accessor.child_page(child_index - 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    child: child_index - 1,
                    parent,
                    reversed: true,
                }));
            }
            find_iter_unbounded_reversed(child_page, parent, manager)
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn find_iter_start<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    query: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let (position, _) = accessor.position::<K>(query);
            Some(Leaf {
                page,
                entry: position,
                parent,
                reversed: false,
            })
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number);
            if child_index < BTREE_ORDER - 1 && accessor.child_page(child_index + 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    child: child_index + 1,
                    parent,
                    reversed: false,
                }));
            }
            find_iter_start::<K>(child_page, parent, query, manager)
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn find_iter_start_reversed<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    query: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let (position, _) = accessor.position::<K>(query);
            Some(Leaf {
                page,
                entry: position,
                parent,
                reversed: true,
            })
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number);
            if child_index > 0 && accessor.child_page(child_index - 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    child: child_index - 1,
                    parent,
                    reversed: true,
                }));
            }
            find_iter_start_reversed::<K>(child_page, parent, query, manager)
        }
        _ => unreachable!(),
    }
}
