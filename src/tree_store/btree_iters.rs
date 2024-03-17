use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::btree_base::{BRANCH, LEAF};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::{BtreeHeader, PageNumber};
use crate::types::{Key, Value};
use crate::Result;
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub enum RangeIterState {
    Leaf {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        entry: usize,
        parent: Option<Box<RangeIterState>>,
    },
    Internal {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        child: usize,
        parent: Option<Box<RangeIterState>>,
    },
}

impl RangeIterState {
    fn page_number(&self) -> PageNumber {
        match self {
            Leaf { page, .. } => page.get_page_number(),
            Internal { page, .. } => page.get_page_number(),
        }
    }

    fn next(self, reverse: bool, manager: &TransactionalMemory) -> Result<Option<RangeIterState>> {
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
                    Ok(Some(Leaf {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        entry: next_entry.try_into().unwrap(),
                        parent,
                    }))
                } else {
                    Ok(parent.map(|x| *x))
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
                let child_page = manager.get_page(child_page)?;
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
                        Ok(Some(Leaf {
                            page: child_page,
                            fixed_key_size,
                            fixed_value_size,
                            entry,
                            parent,
                        }))
                    }
                    BRANCH => {
                        let child_accessor = BranchAccessor::new(&child_page, fixed_key_size);
                        let child = if reverse {
                            child_accessor.count_children() - 1
                        } else {
                            0
                        };
                        Ok(Some(Internal {
                            page: child_page,
                            fixed_key_size,
                            fixed_value_size,
                            child,
                            parent,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn get_entry<K: Key, V: Value>(&self) -> Option<EntryGuard<K, V>> {
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

pub(crate) struct EntryGuard<K: Key, V: Value> {
    page: PageImpl,
    key_range: Range<usize>,
    value_range: Range<usize>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key, V: Value> EntryGuard<K, V> {
    fn new(page: PageImpl, key_range: Range<usize>, value_range: Range<usize>) -> Self {
        Self {
            page,
            key_range,
            value_range,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn key_data(&self) -> Vec<u8> {
        self.page.memory()[self.key_range.clone()].to_vec()
    }

    pub(crate) fn key(&self) -> K::SelfType<'_> {
        K::from_bytes(&self.page.memory()[self.key_range.clone()])
    }

    pub(crate) fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.page.memory()[self.value_range.clone()])
    }

    pub(crate) fn into_raw(self) -> (PageImpl, Range<usize>, Range<usize>) {
        (self.page, self.key_range, self.value_range)
    }
}

pub(crate) struct AllPageNumbersBtreeIter {
    next: Option<RangeIterState>,
    manager: Arc<TransactionalMemory>,
}

impl AllPageNumbersBtreeIter {
    pub(crate) fn new(
        root: PageNumber,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        manager: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let root_page = manager.get_page(root)?;
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
        Ok(Self {
            next: Some(start),
            manager,
        })
    }
}

impl Iterator for AllPageNumbersBtreeIter {
    type Item = Result<PageNumber>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let state = self.next.take()?;
            let value = state.page_number();
            // Only return each page number once
            let once = match state {
                Leaf { entry, .. } => entry == 0,
                Internal { child, .. } => child == 0,
            };
            match state.next(false, &self.manager) {
                Ok(next) => {
                    self.next = next;
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
            if once {
                return Some(Ok(value));
            }
        }
    }
}

pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    root: &'a mut Option<BtreeHeader>,
    inner: BtreeRangeIter<K, V>,
    predicate: F,
    free_on_drop: Vec<PageNumber>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    mem: Arc<TransactionalMemory>,
}

impl<'a, K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    BtreeExtractIf<'a, K, V, F>
{
    pub(crate) fn new(
        root: &'a mut Option<BtreeHeader>,
        inner: BtreeRangeIter<K, V>,
        predicate: F,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            root,
            inner,
            predicate,
            free_on_drop: vec![],
            master_free_list,
            mem,
        }
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Iterator
    for BtreeExtractIf<'_, K, V, F>
{
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item = self.inner.next();
        while let Some(Ok(ref entry)) = item {
            if (self.predicate)(entry.key(), entry.value()) {
                let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new_do_not_modify(
                    self.root,
                    self.mem.clone(),
                    &mut self.free_on_drop,
                );
                match operation.delete(&entry.key()) {
                    Ok(x) => {
                        assert!(x.is_some());
                    }
                    Err(x) => {
                        return Some(Err(x));
                    }
                }
                break;
            }
            item = self.inner.next();
        }
        item
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut item = self.inner.next_back();
        while let Some(Ok(ref entry)) = item {
            if (self.predicate)(entry.key(), entry.value()) {
                let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new_do_not_modify(
                    self.root,
                    self.mem.clone(),
                    &mut self.free_on_drop,
                );
                match operation.delete(&entry.key()) {
                    Ok(x) => {
                        assert!(x.is_some());
                    }
                    Err(x) => {
                        return Some(Err(x));
                    }
                }
                break;
            }
            item = self.inner.next_back();
        }
        item
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Drop
    for BtreeExtractIf<'_, K, V, F>
{
    fn drop(&mut self) {
        let mut master_free_list = self.master_free_list.lock().unwrap();
        for page in self.free_on_drop.drain(..) {
            if !self.mem.free_if_uncommitted(page) {
                master_free_list.push(page);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct BtreeRangeIter<K: Key + 'static, V: Value + 'static> {
    left: Option<RangeIterState>, // Exclusive. The previous element returned
    right: Option<RangeIterState>, // Exclusive. The previous element returned
    include_left: bool,           // left is inclusive, instead of exclusive
    include_right: bool,          // right is inclusive, instead of exclusive
    manager: Arc<TransactionalMemory>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> BtreeRangeIter<K, V> {
    pub(crate) fn new<'a, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a>>>(
        query_range: &'_ T,
        table_root: Option<PageNumber>,
        manager: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        if let Some(root) = table_root {
            let (include_left, left) = match query_range.start_bound() {
                Bound::Included(k) => find_iter_left::<K, V>(
                    manager.get_page(root)?,
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    true,
                    &manager,
                )?,
                Bound::Excluded(k) => find_iter_left::<K, V>(
                    manager.get_page(root)?,
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    false,
                    &manager,
                )?,
                Bound::Unbounded => {
                    let state = find_iter_unbounded::<K, V>(
                        manager.get_page(root)?,
                        None,
                        false,
                        &manager,
                    )?;
                    (true, state)
                }
            };
            let (include_right, right) = match query_range.end_bound() {
                Bound::Included(k) => find_iter_right::<K, V>(
                    manager.get_page(root)?,
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    true,
                    &manager,
                )?,
                Bound::Excluded(k) => find_iter_right::<K, V>(
                    manager.get_page(root)?,
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    false,
                    &manager,
                )?,
                Bound::Unbounded => {
                    let state =
                        find_iter_unbounded::<K, V>(manager.get_page(root)?, None, true, &manager)?;
                    (true, state)
                }
            };
            Ok(Self {
                left,
                right,
                include_left,
                include_right,
                manager,
                _key_type: Default::default(),
                _value_type: Default::default(),
            })
        } else {
            Ok(Self {
                left: None,
                right: None,
                include_left: false,
                include_right: false,
                manager,
                _key_type: Default::default(),
                _value_type: Default::default(),
            })
        }
    }
}

impl<K: Key, V: Value> Iterator for BtreeRangeIter<K, V> {
    type Item = Result<EntryGuard<K, V>>;

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
                match self.left.take()?.next(false, &self.manager) {
                    Ok(left) => {
                        self.left = left;
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
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
            if self.left.as_ref().unwrap().get_entry::<K, V>().is_some() {
                return self.left.as_ref().map(|s| s.get_entry().unwrap()).map(Ok);
            }
        }
    }
}

impl<K: Key, V: Value> DoubleEndedIterator for BtreeRangeIter<K, V> {
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
                match self.right.take()?.next(true, &self.manager) {
                    Ok(right) => {
                        self.right = right;
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
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
            if self.right.as_ref().unwrap().get_entry::<K, V>().is_some() {
                return self.right.as_ref().map(|s| s.get_entry().unwrap()).map(Ok);
            }
        }
    }
}

fn find_iter_unbounded<K: Key, V: Value>(
    page: PageImpl,
    mut parent: Option<Box<RangeIterState>>,
    reverse: bool,
    manager: &TransactionalMemory,
) -> Result<Option<RangeIterState>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let entry = if reverse { accessor.num_pairs() - 1 } else { 0 };
            Ok(Some(Leaf {
                page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                entry,
                parent,
            }))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_index = if reverse {
                accessor.count_children() - 1
            } else {
                0
            };
            let child_page_number = accessor.child_page(child_index).unwrap();
            let child_page = manager.get_page(child_page_number)?;
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
fn find_iter_left<K: Key, V: Value>(
    page: PageImpl,
    mut parent: Option<Box<RangeIterState>>,
    query: &[u8],
    include_query: bool,
    manager: &TransactionalMemory,
) -> Result<(bool, Option<RangeIterState>)> {
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
            Ok((include, Some(result)))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number)?;
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

fn find_iter_right<K: Key, V: Value>(
    page: PageImpl,
    mut parent: Option<Box<RangeIterState>>,
    query: &[u8],
    include_query: bool,
    manager: &TransactionalMemory,
) -> Result<(bool, Option<RangeIterState>)> {
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
            Ok((include, Some(result)))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number)?;
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
