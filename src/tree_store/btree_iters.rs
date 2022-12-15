use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::btree_base::{BRANCH, LEAF};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::PageNumber;
use crate::types::{RedbKey, RedbValue};
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};

#[derive(Debug)]
pub enum RangeIterState<'a> {
    Leaf {
        page: PageImpl<'a>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        entry: usize,
        parent: Option<Box<RangeIterState<'a>>>,
    },
    Internal {
        page: PageImpl<'a>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        child: usize,
        parent: Option<Box<RangeIterState<'a>>>,
    },
}

impl<'a> RangeIterState<'a> {
    fn page_number(&self) -> PageNumber {
        match self {
            Leaf { page, .. } => page.get_page_number(),
            Internal { page, .. } => page.get_page_number(),
        }
    }

    fn next(self, reverse: bool, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
        match self {
            Leaf {
                page,
                fixed_key_size,
                fixed_value_size,
                entry,
                parent,
            } => {
                let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
                let direction = if reverse { -1 } else { 1 };
                let next_entry = isize::try_from(entry).unwrap() + direction;
                if 0 <= next_entry && next_entry < accessor.num_pairs().try_into().unwrap() {
                    Some(Leaf {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        entry: next_entry.try_into().unwrap(),
                        parent,
                    })
                } else {
                    parent.map(|x| *x)
                }
            }
            Internal {
                page,
                fixed_key_size,
                fixed_value_size,
                child,
                mut parent,
            } => {
                let accessor = BranchAccessor::new(&page, fixed_key_size);
                let child_page = accessor.child_page(child).unwrap();
                let child_page = manager.get_page(child_page);
                let direction = if reverse { -1 } else { 1 };
                let next_child = isize::try_from(child).unwrap() + direction;
                if 0 <= next_child && next_child < accessor.count_children().try_into().unwrap() {
                    parent = Some(Box::new(Internal {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        child: next_child.try_into().unwrap(),
                        parent,
                    }));
                }
                match child_page.memory()[0] {
                    LEAF => {
                        let child_accessor = LeafAccessor::new(
                            child_page.memory(),
                            fixed_key_size,
                            fixed_value_size,
                        );
                        let entry = if reverse {
                            child_accessor.num_pairs() - 1
                        } else {
                            0
                        };
                        Some(Leaf {
                            page: child_page,
                            fixed_key_size,
                            fixed_value_size,
                            entry,
                            parent,
                        })
                    }
                    BRANCH => {
                        let child_accessor = BranchAccessor::new(&child_page, fixed_key_size);
                        let child = if reverse {
                            child_accessor.count_children() - 1
                        } else {
                            0
                        };
                        Some(Internal {
                            page: child_page,
                            fixed_key_size,
                            fixed_value_size,
                            child,
                            parent,
                        })
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn get_entry(&self) -> Option<EntryGuard<'a>> {
        match self {
            Leaf {
                page,
                fixed_key_size,
                fixed_value_size,
                entry,
                ..
            } => {
                let (key, value) =
                    LeafAccessor::new(page.memory(), *fixed_key_size, *fixed_value_size)
                        .entry_ranges(*entry)?;
                Some(EntryGuard::new(page.clone(), key, value))
            }
            _ => None,
        }
    }
}

pub(crate) struct EntryGuard<'a> {
    page: PageImpl<'a>,
    key_range: Range<usize>,
    value_range: Range<usize>,
}

impl<'a> EntryGuard<'a> {
    fn new(page: PageImpl<'a>, key_range: Range<usize>, value_range: Range<usize>) -> Self {
        Self {
            page,
            key_range,
            value_range,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.page.memory()[self.key_range.clone()]
    }

    pub(crate) fn value(&self) -> &[u8] {
        &self.page.memory()[self.value_range.clone()]
    }
}

pub(crate) struct AllPageNumbersBtreeIter<'a> {
    next: Option<RangeIterState<'a>>,
    manager: &'a TransactionalMemory,
}

impl<'a> AllPageNumbersBtreeIter<'a> {
    pub(crate) fn new(
        root: PageNumber,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        manager: &'a TransactionalMemory,
    ) -> Self {
        let root_page = manager.get_page(root);
        let node_mem = root_page.memory();
        let start = match node_mem[0] {
            LEAF => Leaf {
                page: root_page,
                fixed_key_size,
                fixed_value_size,
                entry: 0,
                parent: None,
            },
            BRANCH => Internal {
                page: root_page,
                fixed_key_size,
                fixed_value_size,
                child: 0,
                parent: None,
            },
            _ => unreachable!(),
        };
        Self {
            next: Some(start),
            manager,
        }
    }
}

impl<'a> Iterator for AllPageNumbersBtreeIter<'a> {
    type Item = PageNumber;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let state = self.next.take()?;
            let value = state.page_number();
            // Only return each page number once
            let once = match state {
                Leaf { entry, .. } => entry == 0,
                Internal { child, .. } => child == 0,
            };
            self.next = state.next(false, self.manager);
            if once {
                return Some(value);
            }
        }
    }
}

pub(crate) struct BtreeRangeIter<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> {
    left: Option<RangeIterState<'a>>, // Exclusive. The previous element returned
    right: Option<RangeIterState<'a>>, // Exclusive. The previous element returned
    include_left: bool,               // left is inclusive, instead of exclusive
    include_right: bool,              // right is inclusive, instead of exclusive
    manager: &'a TransactionalMemory,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> BtreeRangeIter<'a, K, V> {
    pub(crate) fn new<
        'a0,
        T: RangeBounds<KR> + 'a0,
        KR: Borrow<K::RefBaseType<'a0>> + ?Sized + 'a0,
    >(
        query_range: T,
        table_root: Option<PageNumber>,
        manager: &'a TransactionalMemory,
    ) -> Self
    where
        'a: 'a0,
    {
        if let Some(root) = table_root {
            let (include_left, left) = match query_range.start_bound() {
                Bound::Included(k) => find_iter_left::<K, V>(
                    manager.get_page(root),
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    true,
                    manager,
                ),
                Bound::Excluded(k) => find_iter_left::<K, V>(
                    manager.get_page(root),
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    false,
                    manager,
                ),
                Bound::Unbounded => {
                    let state =
                        find_iter_unbounded::<K, V>(manager.get_page(root), None, false, manager);
                    (true, state)
                }
            };
            let (include_right, right) = match query_range.end_bound() {
                Bound::Included(k) => find_iter_right::<K, V>(
                    manager.get_page(root),
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    true,
                    manager,
                ),
                Bound::Excluded(k) => find_iter_right::<K, V>(
                    manager.get_page(root),
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    false,
                    manager,
                ),
                Bound::Unbounded => {
                    let state =
                        find_iter_unbounded::<K, V>(manager.get_page(root), None, true, manager);
                    (true, state)
                }
            };
            Self {
                left,
                right,
                include_left,
                include_right,
                manager,
                _key_type: Default::default(),
                _value_type: Default::default(),
            }
        } else {
            Self {
                left: None,
                right: None,
                include_left: false,
                include_right: false,
                manager,
                _key_type: Default::default(),
                _value_type: Default::default(),
            }
        }
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> Iterator
    for BtreeRangeIter<'a, K, V>
{
    type Item = EntryGuard<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let (
            Some(Leaf {
                page: left_page,
                entry: left_entry,
                ..
            }),
            Some(Leaf {
                page: right_page,
                entry: right_entry,
                ..
            }),
        ) = (&self.left, &self.right)
        {
            if left_page.get_page_number() == right_page.get_page_number()
                && (left_entry > right_entry
                    || (left_entry == right_entry && (!self.include_left || !self.include_right)))
            {
                return None;
            }
        }

        loop {
            if !self.include_left {
                self.left = self.left.take()?.next(false, self.manager);
            }
            // Return None if the next state is None
            self.left.as_ref()?;

            if let (
                Some(Leaf {
                    page: left_page,
                    entry: left_entry,
                    ..
                }),
                Some(Leaf {
                    page: right_page,
                    entry: right_entry,
                    ..
                }),
            ) = (&self.left, &self.right)
            {
                if left_page.get_page_number() == right_page.get_page_number()
                    && (left_entry > right_entry
                        || (left_entry == right_entry && !self.include_right))
                {
                    return None;
                }
            }

            self.include_left = false;
            if self.left.as_ref().unwrap().get_entry().is_some() {
                return self.left.as_ref().map(|s| s.get_entry().unwrap());
            }
        }
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> DoubleEndedIterator
    for BtreeRangeIter<'a, K, V>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let (
            Some(Leaf {
                page: left_page,
                entry: left_entry,
                ..
            }),
            Some(Leaf {
                page: right_page,
                entry: right_entry,
                ..
            }),
        ) = (&self.left, &self.right)
        {
            if left_page.get_page_number() == right_page.get_page_number()
                && (left_entry > right_entry
                    || (left_entry == right_entry && (!self.include_left || !self.include_right)))
            {
                return None;
            }
        }

        loop {
            if !self.include_right {
                self.right = self.right.take()?.next(true, self.manager);
            }
            // Return None if the next state is None
            self.right.as_ref()?;

            if let (
                Some(Leaf {
                    page: left_page,
                    entry: left_entry,
                    ..
                }),
                Some(Leaf {
                    page: right_page,
                    entry: right_entry,
                    ..
                }),
            ) = (&self.left, &self.right)
            {
                if left_page.get_page_number() == right_page.get_page_number()
                    && (left_entry > right_entry
                        || (left_entry == right_entry && !self.include_left))
                {
                    return None;
                }
            }

            self.include_right = false;
            if self.right.as_ref().unwrap().get_entry().is_some() {
                return self.right.as_ref().map(|s| s.get_entry().unwrap());
            }
        }
    }
}

fn find_iter_unbounded<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    reverse: bool,
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let entry = if reverse { accessor.num_pairs() - 1 } else { 0 };
            Some(Leaf {
                page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                entry,
                parent,
            })
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_index = if reverse {
                accessor.count_children() - 1
            } else {
                0
            };
            let child_page_number = accessor.child_page(child_index).unwrap();
            let child_page = manager.get_page(child_page_number);
            let direction = if reverse { -1isize } else { 1 };
            parent = Some(Box::new(Internal {
                page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                child: (isize::try_from(child_index).unwrap() + direction)
                    .try_into()
                    .unwrap(),
                parent,
            }));
            find_iter_unbounded::<K, V>(child_page, parent, reverse, manager)
        }
        _ => unreachable!(),
    }
}

// Returns a bool indicating whether the first entry pointed to by the state is included in the
// queried range
fn find_iter_left<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    query: &[u8],
    include_query: bool,
    manager: &'a TransactionalMemory,
) -> (bool, Option<RangeIterState<'a>>) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let (mut position, found) = accessor.position::<K>(query);
            let include = if position < accessor.num_pairs() {
                include_query || !found
            } else {
                // Back up to the last valid position
                position -= 1;
                // and exclude it
                false
            };
            let result = Leaf {
                page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                entry: position,
                parent,
            };
            (include, Some(result))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number);
            if child_index < accessor.count_children() - 1 {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    child: child_index + 1,
                    parent,
                }));
            }
            find_iter_left::<K, V>(child_page, parent, query, include_query, manager)
        }
        _ => unreachable!(),
    }
}

fn find_iter_right<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    query: &[u8],
    include_query: bool,
    manager: &'a TransactionalMemory,
) -> (bool, Option<RangeIterState<'a>>) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let (mut position, found) = accessor.position::<K>(query);
            let include = if position < accessor.num_pairs() {
                include_query && found
            } else {
                // Back up to the last valid position
                position -= 1;
                // and include it
                true
            };
            let result = Leaf {
                page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                entry: position,
                parent,
            };
            (include, Some(result))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number);
            if child_index > 0 && accessor.child_page(child_index - 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    child: child_index - 1,
                    parent,
                }));
            }
            find_iter_right::<K, V>(child_page, parent, query, include_query, manager)
        }
        _ => unreachable!(),
    }
}
