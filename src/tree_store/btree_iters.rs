use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BtreeHeader, Checksum, EntryAccessor, LEAF};
use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::btree_iters::RangeIterState::{BranchChild, Enter, Exit, Leaf};
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
    Enter {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        subtree: Option<RangeSubtree>,
        parent: Option<Box<RangeIterState>>,
    },
    Leaf {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        entry: usize,
        start: usize,
        end: usize,
        subtree: Option<RangeSubtree>,
        parent: Option<Box<RangeIterState>>,
    },
    BranchChild {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        child: usize,
        first_range_child: usize,
        last_range_child: usize,
        subtree: Option<RangeSubtree>,
        parent: Option<Box<RangeIterState>>,
    },
    Exit {
        subtree: RangeSubtree,
        parent: Option<Box<RangeIterState>>,
    },
}

fn lower_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, bound: Bound<&[u8]>) -> usize {
    match bound {
        Included(query) | Excluded(query) => {
            let (mut position, found) = accessor.position::<K>(query);
            if matches!(bound, Excluded(_)) && found {
                position += 1;
            }
            position
        }
        Unbounded => 0,
    }
}

fn upper_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, bound: Bound<&[u8]>) -> usize {
    match bound {
        Included(query) | Excluded(query) => {
            let (mut position, found) = accessor.position::<K>(query);
            if matches!(bound, Included(_)) && found {
                position += 1;
            }
            position
        }
        Unbounded => accessor.num_pairs(),
    }
}

fn child_to_visit<K: Key>(
    accessor: &BranchAccessor<'_, '_, PageImpl>,
    bound: Bound<&[u8]>,
    reverse: bool,
) -> usize {
    match bound {
        Included(query) | Excluded(query) => accessor.child_for_key::<K>(query).0,
        Unbounded => {
            if reverse {
                accessor.count_children() - 1
            } else {
                0
            }
        }
    }
}

fn leaf_entries<K: Key>(
    accessor: &LeafAccessor<'_>,
    left_bound: Bound<&[u8]>,
    right_bound: Bound<&[u8]>,
) -> Range<usize> {
    let start = lower_bound_entry::<K>(accessor, left_bound);
    let end = upper_bound_entry::<K>(accessor, right_bound);
    start..end
}

#[derive(Debug, Clone)]
pub(crate) enum RangeVisit<'a> {
    BranchEnter { branch: &'a RangeSubtree },
    // A whole subtree outside the requested entry range, emitted in traversal order.
    SkippedSubtree { subtree: &'a RangeSubtree },
    LeafEntry { entry: RangeLeafEntry<'a> },
    LeafExit { subtree: &'a RangeSubtree },
    BranchExit { branch: &'a RangeSubtree },
}

#[derive(Debug, Clone)]
pub(crate) struct RangeSubtree {
    page: PageNumber,
    checksum: Checksum,
    upper_key: Option<Vec<u8>>,
    root_distance: u32,
}

impl RangeSubtree {
    pub(crate) fn root(header: BtreeHeader) -> Self {
        Self {
            page: header.root,
            checksum: header.checksum,
            upper_key: None,
            root_distance: 0,
        }
    }

    pub(super) fn child(&self, accessor: &BranchAccessor<'_, '_, PageImpl>, index: usize) -> Self {
        let upper_key = if index + 1 < accessor.count_children() {
            Some(accessor.key(index).unwrap().to_vec())
        } else {
            self.upper_key.clone()
        };
        Self {
            page: accessor.child_page(index).unwrap(),
            checksum: accessor.child_checksum(index).unwrap(),
            upper_key,
            root_distance: self.root_distance + 1,
        }
    }

    pub(crate) fn page_number(&self) -> PageNumber {
        self.page
    }

    pub(crate) fn root_distance(&self) -> u32 {
        self.root_distance
    }

    pub(crate) fn into_parts(self) -> (PageNumber, Checksum, Option<Vec<u8>>, u32) {
        (self.page, self.checksum, self.upper_key, self.root_distance)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RangeLeafEntry<'a> {
    page: &'a PageImpl,
    subtree: &'a RangeSubtree,
    entry_index: usize,
}

impl RangeLeafEntry<'_> {
    pub(crate) fn page_number(&self) -> PageNumber {
        self.subtree.page_number()
    }

    pub(crate) fn page(&self) -> &PageImpl {
        self.page
    }

    pub(crate) fn subtree(&self) -> &RangeSubtree {
        self.subtree
    }

    pub(crate) fn entry_index(&self) -> usize {
        self.entry_index
    }

    pub(crate) fn entry<K: Key, V: Value>(&self) -> EntryAccessor<'_> {
        LeafAccessor::new(self.page.memory(), K::fixed_width(), V::fixed_width())
            .entry(self.entry_index)
            .expect("range iterator entry must exist")
    }
}

fn ignore_range_event(_event: RangeVisit<'_>) -> Result {
    Ok(())
}

impl RangeIterState {
    fn page_number(&self) -> PageNumber {
        match self {
            Enter { page, .. } | Leaf { page, .. } | BranchChild { page, .. } => {
                page.get_page_number()
            }
            Exit { subtree, .. } => subtree.page_number(),
        }
    }

    fn is_leaf(&self) -> bool {
        matches!(self, Leaf { .. })
    }

    fn next<K: Key>(
        self,
        left_bound: Bound<&[u8]>,
        right_bound: Bound<&[u8]>,
        reverse: bool,
        manager: &PageResolver,
        hint: PageHint,
        visitor: &mut impl for<'a> FnMut(RangeVisit<'a>) -> Result,
    ) -> Result<Option<RangeIterState>> {
        match self {
            Enter {
                page,
                fixed_key_size,
                fixed_value_size,
                subtree,
                parent,
            } => match page.memory()[0] {
                LEAF => {
                    let accessor =
                        LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
                    let entry_count = accessor.num_pairs();
                    // TODO: Track when a descended subtree is fully inside the
                    // range, so interior leaves can skip these bound searches.
                    let entries = leaf_entries::<K>(&accessor, left_bound, right_bound);
                    Ok(if entries.start < entries.end {
                        let entry = if reverse {
                            entries.end - 1
                        } else {
                            entries.start
                        };
                        Some(Leaf {
                            page,
                            fixed_key_size,
                            fixed_value_size,
                            entry,
                            start: entries.start,
                            end: entries.end,
                            subtree,
                            parent,
                        })
                    } else if (!reverse && !matches!(right_bound, Unbounded) && entries.end == 0)
                        || (reverse
                            && !matches!(left_bound, Unbounded)
                            && entries.start == entry_count)
                    {
                        if let Some(subtree) = subtree.as_ref() {
                            visitor(RangeVisit::SkippedSubtree { subtree })?;
                            parent.map(|x| *x)
                        } else {
                            None
                        }
                    } else {
                        if let Some(subtree) = subtree.as_ref() {
                            visitor(RangeVisit::SkippedSubtree { subtree })?;
                        }
                        parent.map(|x| *x)
                    })
                }
                BRANCH => {
                    let accessor = BranchAccessor::new(&page, fixed_key_size);
                    if let Some(subtree) = subtree.as_ref() {
                        visitor(RangeVisit::BranchEnter { branch: subtree })?;
                    }
                    let seek_bound = if reverse { right_bound } else { left_bound };
                    let child_count = accessor.count_children();
                    let (child, first_range_child, last_range_child) = if subtree.is_some() {
                        let first_range_child = child_to_visit::<K>(&accessor, left_bound, false);
                        let last_range_child = child_to_visit::<K>(&accessor, right_bound, true);
                        let child = if reverse { child_count - 1 } else { 0 };
                        (child, first_range_child, last_range_child)
                    } else {
                        (
                            child_to_visit::<K>(&accessor, seek_bound, reverse),
                            0,
                            child_count - 1,
                        )
                    };
                    Ok(Some(BranchChild {
                        child,
                        first_range_child,
                        last_range_child,
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        subtree,
                        parent,
                    }))
                }
                _ => unreachable!(),
            },
            Leaf {
                page,
                fixed_key_size,
                fixed_value_size,
                entry,
                start,
                end,
                subtree,
                parent,
            } => {
                let next_entry = if reverse {
                    entry.checked_sub(1).filter(|entry| *entry >= start)
                } else {
                    let next_entry = entry + 1;
                    (next_entry < end).then_some(next_entry)
                };
                if let Some(entry) = next_entry {
                    Ok(Some(Leaf {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        entry,
                        start,
                        end,
                        subtree,
                        parent,
                    }))
                } else {
                    if let Some(subtree) = subtree {
                        let page_number = page.get_page_number();
                        drop(page);
                        debug_assert_eq!(page_number, subtree.page_number());
                        visitor(RangeVisit::LeafExit { subtree: &subtree })?;
                    }
                    Ok(parent.map(|x| *x))
                }
            }
            BranchChild {
                page,
                fixed_key_size,
                fixed_value_size,
                child,
                first_range_child,
                last_range_child,
                subtree,
                mut parent,
            } => {
                let (child_page, child_subtree, child_count) = {
                    let accessor = BranchAccessor::new(&page, fixed_key_size);
                    let child_count = accessor.count_children();
                    let child_subtree = subtree
                        .as_ref()
                        .map(|subtree| subtree.child(&accessor, child));
                    if let Some(child_subtree) = child_subtree.as_ref()
                        && (child < first_range_child || child > last_range_child)
                    {
                        visitor(RangeVisit::SkippedSubtree {
                            subtree: child_subtree,
                        })?;
                        return Ok(Self::next_branch_child(
                            BranchChild {
                                page,
                                fixed_key_size,
                                fixed_value_size,
                                child,
                                first_range_child,
                                last_range_child,
                                subtree,
                                parent,
                            },
                            child_count,
                            reverse,
                        )
                        .map(|state| *state));
                    }
                    let child_page = manager.get_page(accessor.child_page(child).unwrap(), hint)?;
                    (child_page, child_subtree, child_count)
                };
                parent = Self::next_branch_child(
                    BranchChild {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        child,
                        first_range_child,
                        last_range_child,
                        subtree,
                        parent,
                    },
                    child_count,
                    reverse,
                );
                Ok(Some(Enter {
                    page: child_page,
                    fixed_key_size,
                    fixed_value_size,
                    subtree: child_subtree,
                    parent,
                }))
            }
            Exit { subtree, parent } => {
                visitor(RangeVisit::BranchExit { branch: &subtree })?;
                Ok(parent.map(|x| *x))
            }
        }
    }

    fn next_branch_child(
        state: RangeIterState,
        child_count: usize,
        reverse: bool,
    ) -> Option<Box<RangeIterState>> {
        let BranchChild {
            page,
            fixed_key_size,
            fixed_value_size,
            child,
            first_range_child,
            last_range_child,
            subtree,
            parent,
        } = state
        else {
            unreachable!("next branch child requires a branch child state");
        };
        let next_child = if reverse {
            child.checked_sub(1)
        } else {
            let next_child = child + 1;
            (next_child < child_count).then_some(next_child)
        };
        if let Some(child) = next_child {
            Some(Box::new(BranchChild {
                page,
                fixed_key_size,
                fixed_value_size,
                child,
                first_range_child,
                last_range_child,
                subtree,
                parent,
            }))
        } else if let Some(subtree) = subtree {
            Some(Box::new(Exit { subtree, parent }))
        } else {
            parent
        }
    }

    fn visit_leaf_entry(
        &self,
        visitor: &mut impl for<'a> FnMut(RangeVisit<'a>) -> Result,
    ) -> Result {
        if let Leaf {
            page,
            entry,
            subtree: Some(subtree),
            ..
        } = self
        {
            visitor(RangeVisit::LeafEntry {
                entry: RangeLeafEntry {
                    page,
                    subtree,
                    entry_index: *entry,
                },
            })?;
        }
        Ok(())
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
            Enter { .. } | BranchChild { .. } | Exit { .. } => None,
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
        let start = Enter {
            page: root_page,
            fixed_key_size,
            fixed_value_size,
            subtree: None,
            parent: None,
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
            let once = match &state {
                Enter {
                    page,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => match page.memory()[0] {
                    BRANCH => true,
                    LEAF => {
                        LeafAccessor::new(page.memory(), *fixed_key_size, *fixed_value_size)
                            .num_pairs()
                            == 0
                    }
                    _ => unreachable!(),
                },
                Leaf { entry, .. } => *entry == 0,
                BranchChild { .. } | Exit { .. } => false,
            };
            let mut ignore_events = ignore_range_event;
            match state.next::<()>(
                Unbounded,
                Unbounded,
                false,
                &self.manager,
                self.hint,
                &mut ignore_events,
            ) {
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
    left_bound: Bound<Vec<u8>>,
    right_bound: Bound<Vec<u8>>,
    // Cursors start inclusive so short scans can drop the iterator without forcing an
    // extra state-machine step past the last yielded entry.
    include_left: bool,  // left is inclusive, instead of exclusive
    include_right: bool, // right is inclusive, instead of exclusive
    manager: PageResolver,
    hint: PageHint,
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
        Self::new_inner(
            query_range,
            table_root.map(|root| (root, None)),
            manager,
            hint,
        )
    }

    pub(crate) fn new_with_subtree_metadata<'a, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a>>>(
        query_range: &'_ T,
        table_root: Option<BtreeHeader>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Result<Self> {
        Self::new_inner(
            query_range,
            table_root.map(|header| (header.root, Some(RangeSubtree::root(header)))),
            manager,
            hint,
        )
    }

    fn new_inner<'a, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a>>>(
        query_range: &'_ T,
        table_root: Option<(PageNumber, Option<RangeSubtree>)>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Result<Self> {
        if range_is_empty::<K, KR, T>(query_range) {
            return Ok(Self {
                left: None,
                right: None,
                left_bound: Unbounded,
                right_bound: Unbounded,
                include_left: false,
                include_right: false,
                manager,
                hint,
                _key_type: PhantomData,
                _value_type: PhantomData,
            });
        }
        if let Some((root, root_subtree)) = table_root {
            let root_page = manager.get_page(root, hint)?;
            let left_bound = query_range
                .start_bound()
                .map(|k| K::as_bytes(k.borrow()).as_ref().to_vec());
            let right_bound = query_range
                .end_bound()
                .map(|k| K::as_bytes(k.borrow()).as_ref().to_vec());
            let left = Some(Enter {
                page: root_page.clone(),
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                subtree: root_subtree.clone(),
                parent: None,
            });
            let right = Some(Enter {
                page: root_page,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                subtree: root_subtree,
                parent: None,
            });
            Ok(Self {
                left,
                right,
                left_bound,
                right_bound,
                include_left: true,
                include_right: true,
                manager,
                hint,
                _key_type: PhantomData,
                _value_type: PhantomData,
            })
        } else {
            Ok(Self {
                left: None,
                right: None,
                left_bound: Unbounded,
                right_bound: Unbounded,
                include_left: false,
                include_right: false,
                manager,
                hint,
                _key_type: PhantomData,
                _value_type: PhantomData,
            })
        }
    }

    pub(crate) fn close(&mut self) {
        self.left = None;
        self.right = None;
    }

    pub(crate) fn next_with_visitor(
        &mut self,
        mut visitor: impl for<'a> FnMut(RangeVisit<'a>) -> Result,
    ) -> Option<Result> {
        self.right = None;
        self.include_right = false;
        self.next_state(&mut visitor)
    }

    fn advance(
        &self,
        current: RangeIterState,
        reverse: bool,
        visitor: &mut impl for<'a> FnMut(RangeVisit<'a>) -> Result,
    ) -> Result<Option<RangeIterState>> {
        current.next::<K>(
            self.left_bound.as_ref().map(Vec::as_slice),
            self.right_bound.as_ref().map(Vec::as_slice),
            reverse,
            &self.manager,
            self.hint,
            visitor,
        )
    }

    fn limit_left_to_right_cursor(&mut self) {
        let (
            Some(Leaf {
                page: left_page,
                end: left_end,
                parent: left_parent,
                ..
            }),
            Some(Leaf {
                page: right_page,
                entry: right_entry,
                ..
            }),
        ) = (&mut self.left, &self.right)
        else {
            return;
        };
        if left_page.get_page_number() == right_page.get_page_number() {
            let end = right_entry + usize::from(self.include_right);
            *left_end = (*left_end).min(end);
            // Entries after this leaf boundary belong to the right cursor.
            *left_parent = None;
        }
    }

    fn limit_right_to_left_cursor(&mut self) {
        let (
            Some(Leaf {
                page: left_page,
                entry: left_entry,
                ..
            }),
            Some(Leaf {
                page: right_page,
                start: right_start,
                parent: right_parent,
                ..
            }),
        ) = (&self.left, &mut self.right)
        else {
            return;
        };
        if left_page.get_page_number() == right_page.get_page_number() {
            let start = left_entry + usize::from(!self.include_left);
            *right_start = (*right_start).max(start);
            // Entries before this leaf boundary belong to the left cursor.
            *right_parent = None;
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> BtreeRangeIter<K, V> {
    fn next_state(
        &mut self,
        visitor: &mut impl for<'a> FnMut(RangeVisit<'a>) -> Result,
    ) -> Option<Result> {
        loop {
            if !self.include_left || self.left.as_ref().is_some_and(|state| !state.is_leaf()) {
                let Some(current) = self.left.take() else {
                    self.close();
                    return None;
                };
                match self.advance(current, false, visitor) {
                    Ok(left) => {
                        self.left = left;
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
            }
            if self.left.is_none() {
                self.close();
                return None;
            }

            self.limit_left_to_right_cursor();
            let state = self.left.as_ref().unwrap();
            if state.is_leaf() {
                let Leaf {
                    entry, start, end, ..
                } = state
                else {
                    unreachable!();
                };
                if *entry < *start || *entry >= *end {
                    self.close();
                    return None;
                }
                self.include_left = false;
                if let Err(err) = state.visit_leaf_entry(visitor) {
                    return Some(Err(err));
                }
                return Some(Ok(()));
            }
        }
    }

    fn next_back_state(
        &mut self,
        visitor: &mut impl for<'a> FnMut(RangeVisit<'a>) -> Result,
    ) -> Option<Result> {
        loop {
            if !self.include_right || self.right.as_ref().is_some_and(|state| !state.is_leaf()) {
                let Some(current) = self.right.take() else {
                    self.close();
                    return None;
                };
                match self.advance(current, true, visitor) {
                    Ok(right) => {
                        self.right = right;
                    }
                    Err(err) => {
                        return Some(Err(err));
                    }
                }
            }
            if self.right.is_none() {
                self.close();
                return None;
            }

            self.limit_right_to_left_cursor();
            let state = self.right.as_ref().unwrap();
            if state.is_leaf() {
                let Leaf {
                    entry, start, end, ..
                } = state
                else {
                    unreachable!();
                };
                if *entry < *start || *entry >= *end {
                    self.close();
                    return None;
                }
                self.include_right = false;
                if let Err(err) = state.visit_leaf_entry(visitor) {
                    return Some(Err(err));
                }
                return Some(Ok(()));
            }
        }
    }
}

impl<K: Key, V: Value> Iterator for BtreeRangeIter<K, V> {
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut ignore_events = ignore_range_event;
        self.next_state(&mut ignore_events)
            .map(|result| result.map(|()| self.left.as_ref().unwrap().get_entry().unwrap()))
    }
}

impl<K: Key, V: Value> DoubleEndedIterator for BtreeRangeIter<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut ignore_events = ignore_range_event;
        self.next_back_state(&mut ignore_events)
            .map(|result| result.map(|()| self.right.as_ref().unwrap().get_entry().unwrap()))
    }
}
