use crate::tree_store::btree_base::{EntryAccessor, InternalAccessor, LeafAccessor};
use crate::tree_store::btree_base::{BTREE_ORDER, INTERNAL, LEAF};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::PageNumber;
use crate::types::{RedbKey, RedbValue};
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use RangeIterState::Repeat;

fn bound_contains_key<
    'a,
    T: RangeBounds<KR>,
    KR: Borrow<K> + ?Sized + 'a,
    K: RedbKey + ?Sized + 'a,
>(
    range: &T,
    key: &[u8],
) -> bool {
    if let Bound::Included(start) = range.start_bound() {
        if K::compare(key, start.borrow().as_bytes().as_ref()).is_lt() {
            return false;
        }
    } else if let Bound::Excluded(start) = range.start_bound() {
        if K::compare(key, start.borrow().as_bytes().as_ref()).is_le() {
            return false;
        }
    }
    if let Bound::Included(end) = range.end_bound() {
        if K::compare(key, end.borrow().as_bytes().as_ref()).is_gt() {
            return false;
        }
    } else if let Bound::Excluded(end) = range.end_bound() {
        if K::compare(key, end.borrow().as_bytes().as_ref()).is_ge() {
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
    },
    Internal {
        page: PageImpl<'a>,
        child: usize,
        parent: Option<Box<RangeIterState<'a>>>,
    },
    Repeat {
        inner: Box<RangeIterState<'a>>,
    },
}

impl<'a> RangeIterState<'a> {
    fn page_number(&self) -> PageNumber {
        match self {
            Leaf { page, .. } => page.get_page_number(),
            Internal { page, .. } => page.get_page_number(),
            Repeat { .. } => unreachable!(),
        }
    }

    fn next(self, reverse: bool, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
        match self {
            Leaf {
                page,
                entry,
                parent,
            } => {
                let accessor = LeafAccessor::new(&page);
                let direction = if reverse { -1 } else { 1 };
                let next_entry = entry as isize + direction;
                if 0 <= next_entry && next_entry < accessor.num_pairs() as isize {
                    Some(Leaf {
                        page,
                        entry: next_entry as usize,
                        parent,
                    })
                } else {
                    parent.map(|x| *x)
                }
            }
            Internal {
                page,
                child,
                mut parent,
            } => {
                let accessor = InternalAccessor::new(&page);
                let child_page = accessor.child_page(child).unwrap();
                let child_page = manager.get_page(child_page);
                let direction = if reverse { -1 } else { 1 };
                let next_child = child as isize + direction;
                if 0 <= next_child && next_child < accessor.count_children() as isize {
                    parent = Some(Box::new(Internal {
                        page,
                        child: next_child as usize,
                        parent,
                    }));
                }
                match child_page.memory()[0] {
                    LEAF => {
                        let child_accessor = LeafAccessor::new(&child_page);
                        let entry = if reverse {
                            child_accessor.num_pairs() - 1
                        } else {
                            0
                        };
                        Some(Leaf {
                            page: child_page,
                            entry,
                            parent,
                        })
                    }
                    INTERNAL => {
                        let child_accessor = InternalAccessor::new(&child_page);
                        let child = if reverse {
                            child_accessor.count_children() - 1
                        } else {
                            0
                        };
                        Some(Internal {
                            page: child_page,
                            child,
                            parent,
                        })
                    }
                    _ => unreachable!(),
                }
            }
            Repeat { inner } => Some(*inner),
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
            Repeat { .. } => unreachable!(),
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
            if let Some(next_state) = state.next(false, self.manager) {
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
                Repeat { .. } => unreachable!(),
            }
        }
    }
}

pub(crate) fn page_numbers_iter_start_state(page: PageImpl) -> RangeIterState {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => Leaf {
            page,
            entry: 0,
            parent: None,
        },
        INTERNAL => Internal {
            page,
            child: 0,
            parent: None,
        },
        _ => unreachable!(),
    }
}

pub struct BtreeRangeIter<
    'a,
    T: RangeBounds<KR>,
    KR: Borrow<K> + ?Sized + 'a,
    K: RedbKey + ?Sized + 'a,
    V: RedbValue + ?Sized + 'a,
> {
    left: Option<RangeIterState<'a>>, // Exclusive. The previous element returned
    right: Option<RangeIterState<'a>>, // Exclusive. The previous element returned
    // TODO: refactor away this query_range field
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
        KR: Borrow<K> + ?Sized + 'a,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    > BtreeRangeIter<'a, T, KR, K, V>
{
    pub(crate) fn new(
        left: Option<RangeIterState<'a>>,
        right: Option<RangeIterState<'a>>,
        query_range: T,
        manager: &'a TransactionalMemory,
    ) -> Self {
        Self {
            left: left.map(|s| Repeat { inner: Box::new(s) }),
            right: right.map(|s| Repeat { inner: Box::new(s) }),
            query_range,
            reversed: false,
            manager,
            _key_type: Default::default(),
            _key_ref_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn reverse(self) -> Self {
        Self {
            left: self.left,
            right: self.right,
            query_range: self.query_range,
            reversed: !self.reversed,
            manager: self.manager,
            _key_type: Default::default(),
            _key_ref_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    // TODO: we need generic-associated-types to implement Iterator
    pub fn next(&mut self) -> Option<EntryAccessor> {
        loop {
            if !self.reversed {
                self.left = self.left.take()?.next(self.reversed, self.manager);
                // Return None if the next state is None
                self.left.as_ref()?;

                if let (
                    Some(Leaf {
                        page: left_page,
                        entry: left_entry,
                        parent: _,
                    }),
                    Some(Leaf {
                        page: right_page,
                        entry: right_entry,
                        parent: _,
                    }),
                ) = (&self.left, &self.right)
                {
                    if left_page.get_page_number() == right_page.get_page_number()
                        && left_entry >= right_entry
                    {
                        return None;
                    }
                }

                if let Some(entry) = self.left.as_ref().unwrap().get_entry() {
                    if bound_contains_key::<T, KR, K>(&self.query_range, entry.key()) {
                        return self.left.as_ref().map(|s| s.get_entry().unwrap());
                    } else {
                        #[allow(clippy::collapsible_else_if)]
                        if let Bound::Included(end) = self.query_range.end_bound() {
                            if K::compare(entry.key(), end.borrow().as_bytes().as_ref()).is_gt() {
                                self.left = None;
                            }
                        } else if let Bound::Excluded(end) = self.query_range.end_bound() {
                            if K::compare(entry.key(), end.borrow().as_bytes().as_ref()).is_ge() {
                                self.left = None;
                            }
                        }
                    }
                }
            } else {
                self.right = self.right.take()?.next(self.reversed, self.manager);
                // Return None if the next state is None
                self.right.as_ref()?;

                if let (
                    Some(Leaf {
                        page: left_page,
                        entry: left_entry,
                        parent: _,
                    }),
                    Some(Leaf {
                        page: right_page,
                        entry: right_entry,
                        parent: _,
                    }),
                ) = (&self.left, &self.right)
                {
                    if left_page.get_page_number() == right_page.get_page_number()
                        && left_entry >= right_entry
                    {
                        return None;
                    }
                }

                if let Some(entry) = self.right.as_ref().unwrap().get_entry() {
                    if bound_contains_key::<T, KR, K>(&self.query_range, entry.key()) {
                        return self.right.as_ref().map(|s| s.get_entry().unwrap());
                    } else {
                        #[allow(clippy::collapsible_else_if)]
                        if let Bound::Included(start) = self.query_range.start_bound() {
                            if K::compare(entry.key(), start.borrow().as_bytes().as_ref()).is_lt() {
                                self.right = None;
                            }
                        } else if let Bound::Excluded(start) = self.query_range.start_bound() {
                            if K::compare(entry.key(), start.borrow().as_bytes().as_ref()).is_le() {
                                self.right = None;
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn find_iter_unbounded_left<'a>(
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
        }),
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let child_page_number = accessor.child_page(0).unwrap();
            let child_page = manager.get_page(child_page_number);
            parent = Some(Box::new(Internal {
                page,
                child: 1,
                parent,
            }));
            find_iter_unbounded_left(child_page, parent, manager)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn find_iter_unbounded_right<'a>(
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
                }));
            }
            find_iter_unbounded_right(child_page, parent, manager)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn find_iter_left<'a, K: RedbKey + ?Sized>(
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
                }));
            }
            find_iter_left::<K>(child_page, parent, query, manager)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn find_iter_right<'a, K: RedbKey + ?Sized>(
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
                }));
            }
            find_iter_right::<K>(child_page, parent, query, manager)
        }
        _ => unreachable!(),
    }
}
