use crate::Result;
use crate::tree_store::btree_base::{BRANCH, LEAF};
use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{PageNumber, PageResolver};
use crate::types::{Key, Value};
use Bound::{Excluded, Included, Unbounded};
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};

#[derive(Debug, Clone)]
enum RangeIterState {
    Leaf {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        entry: usize,
        entry_count: usize,
        parent: Option<Box<RangeIterState>>,
    },
    Internal {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        branch_state: BranchIterState,
        parent: Option<Box<RangeIterState>>,
    },
}

#[derive(Debug, Clone, Copy)]
enum BranchIterState {
    // The branch page itself has not been yielded yet. The next step descends
    // into `child`, so page-number iteration should still report this branch.
    Descend { child: usize },
    // A child has already been visited. The next step descends into the next
    // sibling; when no sibling exists this state is not stored.
    AfterChild { next_child: usize },
}

impl BranchIterState {
    fn descend(child: usize) -> Self {
        Self::Descend { child }
    }

    fn after_child(child: usize, child_count: usize, reverse: bool) -> Option<Self> {
        let next_child = if reverse {
            child.checked_sub(1)?
        } else {
            let next_child = child + 1;
            if next_child >= child_count {
                return None;
            }
            next_child
        };

        Some(Self::AfterChild { next_child })
    }

    fn child_to_visit(self) -> usize {
        match self {
            Self::Descend { child } => child,
            Self::AfterChild { next_child } => next_child,
        }
    }

    fn is_entering(self) -> bool {
        matches!(self, Self::Descend { .. })
    }
}

impl RangeIterState {
    fn page_number(&self) -> PageNumber {
        match self {
            Leaf { page, .. } | Internal { page, .. } => page.get_page_number(),
        }
    }

    fn is_leaf(&self) -> bool {
        matches!(self, Leaf { .. })
    }

    fn next(
        self,
        reverse: bool,
        manager: &PageResolver,
        hint: PageHint,
    ) -> Result<Option<RangeIterState>> {
        match self {
            Leaf {
                page,
                fixed_key_size,
                fixed_value_size,
                entry,
                entry_count,
                parent,
            } => {
                let direction = if reverse { -1 } else { 1 };
                let next_entry = isize::try_from(entry).unwrap() + direction;
                if 0 <= next_entry && next_entry < entry_count.try_into().unwrap() {
                    Ok(Some(Leaf {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        entry: next_entry.try_into().unwrap(),
                        entry_count,
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
                branch_state,
                mut parent,
            } => {
                let child = branch_state.child_to_visit();
                let accessor = BranchAccessor::new(&page, fixed_key_size);
                let child_count = accessor.count_children();
                let child_page = accessor.child_page(child).unwrap();
                let child_page = manager.get_page(child_page, hint)?;
                if let Some(branch_state) =
                    BranchIterState::after_child(child, child_count, reverse)
                {
                    parent = Some(Box::new(Internal {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        branch_state,
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
                        let entry_count = child_accessor.num_pairs();
                        Ok(Some(Leaf {
                            page: child_page,
                            fixed_key_size,
                            fixed_value_size,
                            entry,
                            entry_count,
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
                            branch_state: BranchIterState::descend(child),
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
            Internal { .. } => None,
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
            _key_type: PhantomData,
            _value_type: PhantomData,
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
    manager: PageResolver,
    hint: PageHint,
}

impl AllPageNumbersBtreeIter {
    pub(crate) fn new(
        root: PageNumber,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Result<Self> {
        let root_page = manager.get_page(root, hint)?;
        let node_mem = root_page.memory();
        let start = match node_mem[0] {
            LEAF => {
                let entry_count =
                    LeafAccessor::new(root_page.memory(), fixed_key_size, fixed_value_size)
                        .num_pairs();
                Leaf {
                    page: root_page,
                    fixed_key_size,
                    fixed_value_size,
                    entry: 0,
                    entry_count,
                    parent: None,
                }
            }
            BRANCH => Internal {
                page: root_page,
                fixed_key_size,
                fixed_value_size,
                branch_state: BranchIterState::descend(0),
                parent: None,
            },
            _ => unreachable!(),
        };
        Ok(Self {
            next: Some(start),
            manager,
            hint,
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
                Internal { branch_state, .. } => branch_state.is_entering(),
            };
            match state.next(false, &self.manager, self.hint) {
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

#[derive(Clone)]
pub(crate) struct BtreeRangeIter<K: Key + 'static, V: Value + 'static> {
    left: Option<RangeIterState>, // Exclusive. The previous element returned
    right: Option<RangeIterState>, // Exclusive. The previous element returned
    include_left: bool,           // left is inclusive, instead of exclusive
    include_right: bool,          // right is inclusive, instead of exclusive
    manager: PageResolver,
    hint: PageHint,
    // When Some, the right boundary is Unbounded and not yet computed.
    // Stores the tree root for lazy initialization on first next_back() call.
    uninit_right_root: Option<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

fn range_is_empty<'a, K: Key + 'static, KR: Borrow<K::SelfType<'a>>, T: RangeBounds<KR>>(
    range: &T,
) -> bool {
    match (range.start_bound(), range.end_bound()) {
        (Unbounded, _) | (_, Unbounded) => false,
        (Included(start), Excluded(end)) | (Excluded(start), Included(end) | Excluded(end)) => {
            let start_tmp = K::as_bytes(start.borrow());
            let start_value = start_tmp.as_ref();
            let end_tmp = K::as_bytes(end.borrow());
            let end_value = end_tmp.as_ref();
            K::compare(start_value, end_value).is_ge()
        }
        (Included(start), Included(end)) => {
            let start_tmp = K::as_bytes(start.borrow());
            let start_value = start_tmp.as_ref();
            let end_tmp = K::as_bytes(end.borrow());
            let end_value = end_tmp.as_ref();
            K::compare(start_value, end_value).is_gt()
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> BtreeRangeIter<K, V> {
    pub(crate) fn new<'a, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a>>>(
        query_range: &'_ T,
        table_root: Option<PageNumber>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Result<Self> {
        if range_is_empty::<K, KR, T>(query_range) {
            return Ok(Self {
                left: None,
                right: None,
                include_left: false,
                include_right: false,
                manager,
                hint,
                uninit_right_root: None,
                _key_type: PhantomData,
                _value_type: PhantomData,
            });
        }
        if let Some(root) = table_root {
            let (include_left, left) = match query_range.start_bound() {
                Included(k) => find_iter_left::<K, V>(
                    manager.get_page(root, hint)?,
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    true,
                    &manager,
                    hint,
                )?,
                Excluded(k) => find_iter_left::<K, V>(
                    manager.get_page(root, hint)?,
                    None,
                    K::as_bytes(k.borrow()).as_ref(),
                    false,
                    &manager,
                    hint,
                )?,
                Unbounded => {
                    let state = find_iter_unbounded::<K, V>(
                        manager.get_page(root, hint)?,
                        None,
                        false,
                        &manager,
                        hint,
                    )?;
                    (true, state)
                }
            };
            // For an unbounded right end, skip the expensive tree traversal to the rightmost
            // leaf. The right boundary will be lazily computed on the first next_back() call.
            // For forward iteration (next()), right=None correctly means "no upper bound".
            let (include_right, right, uninit_right_root) = match query_range.end_bound() {
                Included(k) => {
                    let (inc, state) = find_iter_right::<K, V>(
                        manager.get_page(root, hint)?,
                        None,
                        K::as_bytes(k.borrow()).as_ref(),
                        true,
                        &manager,
                        hint,
                    )?;
                    (inc, state, None)
                }
                Excluded(k) => {
                    let (inc, state) = find_iter_right::<K, V>(
                        manager.get_page(root, hint)?,
                        None,
                        K::as_bytes(k.borrow()).as_ref(),
                        false,
                        &manager,
                        hint,
                    )?;
                    (inc, state, None)
                }
                Unbounded => (true, None, Some(root)),
            };
            Ok(Self {
                left,
                right,
                include_left,
                include_right,
                manager,
                hint,
                uninit_right_root,
                _key_type: PhantomData,
                _value_type: PhantomData,
            })
        } else {
            Ok(Self {
                left: None,
                right: None,
                include_left: false,
                include_right: false,
                manager,
                hint,
                uninit_right_root: None,
                _key_type: PhantomData,
                _value_type: PhantomData,
            })
        }
    }

    pub(super) fn close(&mut self) {
        self.left = None;
        self.right = None;
        self.uninit_right_root = None;
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
            && left_page.get_page_number() == right_page.get_page_number()
            && (left_entry > right_entry
                || (left_entry == right_entry && (!self.include_left || !self.include_right)))
        {
            self.close();
            return None;
        }

        loop {
            if !self.include_left {
                let Some(current) = self.left.take() else {
                    self.close();
                    return None;
                };
                match current.next(false, &self.manager, self.hint) {
                    Ok(left) => {
                        self.left = left;
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
            }
            // Return None if the next state is None
            if self.left.is_none() {
                self.close();
                return None;
            }

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
                && left_page.get_page_number() == right_page.get_page_number()
                && (left_entry > right_entry || (left_entry == right_entry && !self.include_right))
            {
                self.close();
                return None;
            }

            self.include_left = false;
            let state = self.left.as_ref().unwrap();
            if state.is_leaf() {
                return Some(Ok(state.get_entry().unwrap()));
            }
        }
    }
}

impl<K: Key, V: Value> DoubleEndedIterator for BtreeRangeIter<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Lazily initialize the unbounded right boundary on first next_back() call.
        if let Some(root) = self.uninit_right_root.take() {
            let page = match self.manager.get_page(root, self.hint) {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };
            match find_iter_unbounded::<K, V>(page, None, true, &self.manager, self.hint) {
                Ok(state) => self.right = state,
                Err(e) => return Some(Err(e)),
            }
        }
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
            && left_page.get_page_number() == right_page.get_page_number()
            && (left_entry > right_entry
                || (left_entry == right_entry && (!self.include_left || !self.include_right)))
        {
            self.close();
            return None;
        }

        loop {
            if !self.include_right {
                let Some(current) = self.right.take() else {
                    self.close();
                    return None;
                };
                match current.next(true, &self.manager, self.hint) {
                    Ok(right) => {
                        self.right = right;
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
            }
            // Return None if the next state is None
            if self.right.is_none() {
                self.close();
                return None;
            }

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
                && left_page.get_page_number() == right_page.get_page_number()
                && (left_entry > right_entry || (left_entry == right_entry && !self.include_left))
            {
                self.close();
                return None;
            }

            self.include_right = false;
            let state = self.right.as_ref().unwrap();
            if state.is_leaf() {
                return Some(Ok(state.get_entry().unwrap()));
            }
        }
    }
}

fn find_iter_unbounded<K: Key, V: Value>(
    page: PageImpl,
    mut parent: Option<Box<RangeIterState>>,
    reverse: bool,
    manager: &PageResolver,
    hint: PageHint,
) -> Result<Option<RangeIterState>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let entry_count = accessor.num_pairs();
            let entry = if reverse { entry_count - 1 } else { 0 };
            Ok(Some(Leaf {
                page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                entry,
                entry_count,
                parent,
            }))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_count = accessor.count_children();
            let child_index = if reverse { child_count - 1 } else { 0 };
            let child_page_number = accessor.child_page(child_index).unwrap();
            let child_page = manager.get_page(child_page_number, hint)?;
            if let Some(branch_state) =
                BranchIterState::after_child(child_index, child_count, reverse)
            {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    branch_state,
                    parent,
                }));
            }
            find_iter_unbounded::<K, V>(child_page, parent, reverse, manager, hint)
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
    manager: &PageResolver,
    hint: PageHint,
) -> Result<(bool, Option<RangeIterState>)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let (mut position, found) = accessor.position::<K>(query);
            let entry_count = accessor.num_pairs();
            let include = if position < entry_count {
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
                entry_count,
                parent,
            };
            Ok((include, Some(result)))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_count = accessor.count_children();
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number, hint)?;
            if let Some(branch_state) =
                BranchIterState::after_child(child_index, child_count, false)
            {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    branch_state,
                    parent,
                }));
            }
            find_iter_left::<K, V>(child_page, parent, query, include_query, manager, hint)
        }
        _ => unreachable!(),
    }
}

fn find_iter_right<K: Key, V: Value>(
    page: PageImpl,
    mut parent: Option<Box<RangeIterState>>,
    query: &[u8],
    include_query: bool,
    manager: &PageResolver,
    hint: PageHint,
) -> Result<(bool, Option<RangeIterState>)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let (mut position, found) = accessor.position::<K>(query);
            let entry_count = accessor.num_pairs();
            let include = if position < entry_count {
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
                entry_count,
                parent,
            };
            Ok((include, Some(result)))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_count = accessor.count_children();
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number, hint)?;
            if let Some(branch_state) = BranchIterState::after_child(child_index, child_count, true)
            {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    branch_state,
                    parent,
                }));
            }
            find_iter_right::<K, V>(child_page, parent, query, include_query, manager, hint)
        }
        _ => unreachable!(),
    }
}
