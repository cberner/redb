use crate::AccessGuard;
use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, LEAF, LeafAccessor};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::cmp::Ordering;
use std::collections::Bound;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Branch {
    page: PageImpl,
    child_index: usize,
}

impl Branch {
    fn new(page: PageImpl, child_index: usize) -> Self {
        Self { page, child_index }
    }

    fn into_parts(self) -> (PageImpl, usize) {
        (self.page, self.child_index)
    }
}

#[derive(Copy, Clone)]
pub(super) enum Position<'a> {
    // Gap before the first key.
    Start,
    // Gap after the last key.
    End,
    // Gap before `key`, or where `key` would be inserted.
    Before(&'a [u8]),
    // Gap after `key`, or where `key` would be inserted.
    After(&'a [u8]),
}

#[derive(Copy, Clone, PartialEq)]
enum Direction {
    Next,
    Previous,
}

impl Direction {
    fn is_next(self) -> bool {
        matches!(self, Self::Next)
    }

    fn opposite(self) -> Self {
        match self {
            Self::Next => Self::Previous,
            Self::Previous => Self::Next,
        }
    }
}

fn lower_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, position: Position<'_>) -> usize {
    match position {
        Position::Start => 0,
        Position::End => accessor.num_pairs(),
        Position::Before(query) | Position::After(query) => {
            let (mut position_index, found) = accessor.position::<K>(query);
            if matches!(position, Position::After(_)) && found {
                position_index += 1;
            }
            position_index
        }
    }
}

fn child_to_visit<K: Key>(
    accessor: &BranchAccessor<'_, '_, PageImpl>,
    position: Position<'_>,
) -> usize {
    match position {
        Position::Start => 0,
        Position::End => accessor.count_children() - 1,
        Position::Before(query) | Position::After(query) => accessor.child_for_key::<K>(query).0,
    }
}

fn descend_to_position<K: Key + 'static, V: Value + 'static, F>(
    page: PageImpl,
    position: Position<'_>,
    path: &mut Vec<Branch>,
    get_page: &mut F,
) -> Result<Leaf>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    match page.memory()[0] {
        LEAF => {
            let (position, len) = {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                (
                    lower_bound_entry::<K>(&accessor, position),
                    accessor.num_pairs(),
                )
            };
            Ok(Leaf {
                page,
                position,
                len,
            })
        }
        BRANCH => {
            let (child_index, child_page) = {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_index = child_to_visit::<K>(&accessor, position);
                (child_index, accessor.child_page(child_index).unwrap())
            };
            path.push(Branch::new(page, child_index));
            let child = get_page(child_page)?;
            descend_to_position::<K, V, F>(child, position, path, get_page)
        }
        _ => unreachable!(),
    }
}

fn move_to_adjacent_leaf<K: Key + 'static, V: Value + 'static, F>(
    path: &mut Vec<Branch>,
    direction: Direction,
    get_page: &mut F,
) -> Result<Option<Leaf>>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    for index in (0..path.len()).rev() {
        let next_child = {
            let frame = &path[index];
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            if direction.is_next() {
                let child_index = frame.child_index + 1;
                (child_index < accessor.count_children())
                    .then(|| (child_index, accessor.child_page(child_index).unwrap()))
            } else {
                frame
                    .child_index
                    .checked_sub(1)
                    .map(|child_index| (child_index, accessor.child_page(child_index).unwrap()))
            }
        };

        if let Some((child_index, child_page)) = next_child {
            path[index].child_index = child_index;
            path.truncate(index + 1);
            let page = get_page(child_page)?;
            let edge = if direction.is_next() {
                Position::Start
            } else {
                Position::End
            };
            return descend_to_position::<K, V, F>(page, edge, path, get_page).map(Some);
        }
    }

    Ok(None)
}

fn prepare_leaf<K: Key + 'static, V: Value + 'static, F>(
    leaf: &mut Option<Leaf>,
    path: &mut Vec<Branch>,
    direction: Direction,
    get_page: &mut F,
) -> Result<bool>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    while let Some(current) = leaf.as_ref() {
        if (direction.is_next() && current.position < current.len)
            || (!direction.is_next() && current.position > 0)
        {
            return Ok(true);
        }
        let Some(next_leaf) = move_to_adjacent_leaf::<K, V, F>(path, direction, get_page)? else {
            return Ok(false);
        };
        *leaf = Some(next_leaf);
    }

    Ok(false)
}

fn entry<K: Key + 'static, V: Value + 'static>(leaf: &Leaf, position: usize) -> EntryGuard<K, V> {
    let (key, value) = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
        .entry_ranges(position)
        .expect("cursor entry must exist");
    EntryGuard::new(leaf.page.clone(), key, value)
}

fn entry_ref<K: Key + 'static, V: Value + 'static>(
    leaf: &Leaf,
    position: usize,
) -> EntryRef<'_, K, V> {
    let (key_range, value_range) =
        LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
            .entry_ranges(position)
            .expect("cursor entry must exist");
    EntryRef {
        page: &leaf.page,
        key_range,
        value_range,
        _key_type: PhantomData,
        _value_type: PhantomData,
    }
}

fn key_data<K: Key + 'static, V: Value + 'static>(leaf: &Leaf, position: usize) -> Vec<u8> {
    LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
        .entry(position)
        .expect("cursor entry must exist")
        .key()
        .to_vec()
}

#[derive(Clone)]
struct Leaf {
    page: PageImpl,
    position: usize,
    len: usize,
}

pub(super) struct EntryRef<'a, K: Key + 'static, V: Value + 'static> {
    page: &'a PageImpl,
    key_range: Range<usize>,
    value_range: Range<usize>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> EntryRef<'_, K, V> {
    pub(super) fn key_bytes(&self) -> &[u8] {
        &self.page.memory()[self.key_range.clone()]
    }

    pub(super) fn key(&self) -> K::SelfType<'_> {
        K::from_bytes(&self.page.memory()[self.key_range.clone()])
    }

    pub(super) fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.page.memory()[self.value_range.clone()])
    }
}

#[derive(Clone)]
pub(super) struct Cursor<K: Key + 'static, V: Value + 'static> {
    root: PageNumber,
    path: Vec<Branch>,
    // Gap cursor position: next() returns the entry at position, and prev()
    // returns the entry before position.
    leaf: Option<Leaf>,
    manager: PageResolver,
    hint: PageHint,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> Cursor<K, V> {
    pub(super) fn new(root: PageNumber, manager: PageResolver, hint: PageHint) -> Self {
        Self {
            root,
            path: vec![],
            leaf: None,
            manager,
            hint,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub(super) fn seek_to(&mut self, position: Position<'_>) -> Result {
        self.path.clear();
        let root_page = self.manager.get_page(self.root, self.hint)?;
        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        *leaf = Some(descend_to_position::<K, V, _>(
            root_page,
            position,
            path,
            &mut get_page,
        )?);
        Ok(())
    }

    fn ensure_has_entry(&mut self, direction: Direction) -> Result<bool> {
        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        prepare_leaf::<K, V, _>(leaf, path, direction, &mut get_page)
    }

    pub(super) fn normalize_forward_gap(&mut self) -> Result {
        if self
            .leaf
            .as_ref()
            .is_none_or(|leaf| leaf.position != leaf.len)
        {
            return Ok(());
        }

        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        if let Some(next_leaf) =
            move_to_adjacent_leaf::<K, V, _>(path, Direction::Next, &mut get_page)?
        {
            *leaf = Some(next_leaf);
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }

        let leaf = self.leaf.as_mut().expect("cursor must be positioned");
        let position = leaf.position;
        leaf.position += 1;
        Ok(Some(entry(leaf, position)))
    }

    pub(super) fn prev(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }

        let leaf = self.leaf.as_mut().expect("cursor must be positioned");
        leaf.position -= 1;
        Ok(Some(entry(leaf, leaf.position)))
    }

    fn page_number(&self) -> PageNumber {
        self.leaf
            .as_ref()
            .expect("cursor must be positioned")
            .page
            .get_page_number()
    }

    fn position(&self) -> usize {
        self.leaf
            .as_ref()
            .expect("cursor must be positioned")
            .position
    }

    pub(super) fn compare_position(&self, other: &Self) -> Ordering {
        let self_page = self.page_number();
        let other_page = other.page_number();
        if self_page == other_page {
            return self.position().cmp(&other.position());
        }

        assert_eq!(self.path.len(), other.path.len());
        for (self_frame, other_frame) in self.path.iter().zip(&other.path) {
            match self_frame.child_index.cmp(&other_frame.child_index) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        unreachable!("distinct cursor pages must diverge in their branch path")
    }
}

struct CursorPosition {
    path: Vec<Branch>,
    // Gap cursor position: next operations use `position`, and previous
    // operations use the entry before `position`.
    leaf: Leaf,
}

// Owned cursor state, detachable from the tree borrows so that callers like
// `RangeMut` can persist a cursor position across calls.
#[derive(Default)]
pub(super) struct CursorState {
    // None means the cursor has not been positioned. Otherwise the ancestor
    // path and current leaf are kept together as one valid gap cursor.
    position: Option<CursorPosition>,
    // Pending removals from the current leaf. A batch is recorded in strictly
    // increasing order by forward scans, or strictly decreasing order by
    // backward scans; the two must not be mixed within a batch.
    removed_indexes: Vec<usize>,
    // True when guards backed by the current leaf's memory were handed out for
    // pending removals: the flush must not modify the leaf memory in place.
    detached_guards: bool,
    // True when a flush consumed pending removals but failed to apply them.
    // The caller must not let the transaction commit, since entries already
    // reported as removed may remain in the tree.
    lost_removals: bool,
}

pub(super) struct CursorMut<'a, 'b, K: Key + 'static, V: Value + 'static> {
    // Table header state, separate from traversal position.
    root: &'b mut Option<BtreeHeader>,
    page_allocator: &'b PageAllocator,
    freed: &'b mut Vec<PageNumber>,
    allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
    state: CursorState,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl CursorPosition {
    fn into_parts(self) -> (Vec<(PageImpl, usize)>, Leaf) {
        (
            self.path.into_iter().map(Branch::into_parts).collect(),
            self.leaf,
        )
    }

    fn has_entry(&self, direction: Direction) -> bool {
        match direction {
            Direction::Next => self.leaf.position < self.leaf.len,
            Direction::Previous => self.leaf.position > 0,
        }
    }

    fn entry_index(&self, direction: Direction) -> usize {
        match direction {
            Direction::Next => self.leaf.position,
            Direction::Previous => self.leaf.position - 1,
        }
    }

    fn move_once(&mut self, direction: Direction) {
        match direction {
            Direction::Next => self.leaf.position += 1,
            Direction::Previous => self.leaf.position -= 1,
        }
    }
}

impl<'a, 'b, K: Key + 'static, V: Value + 'static> CursorMut<'a, 'b, K, V> {
    pub(super) fn new(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: &'b PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self::with_state(
            root,
            page_allocator,
            freed,
            allocated,
            CursorState::default(),
        )
    }

    pub(super) fn with_state(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: &'b PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
        state: CursorState,
    ) -> Self {
        Self {
            root,
            page_allocator,
            freed,
            allocated,
            state,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn into_state(self) -> CursorState {
        self.state
    }

    pub(super) fn seek_to(&mut self, target: Position<'_>) -> Result {
        assert!(self.state.removed_indexes.is_empty());
        self.state.position = None;
        let Some(header) = *self.root else {
            return Ok(());
        };
        let root_page = self.page_allocator.get_page(header.root, PageHint::None)?;
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        let mut path = vec![];
        let leaf = descend_to_position::<K, V, _>(root_page, target, &mut path, &mut get_page)?;
        self.state.position = Some(CursorPosition { path, leaf });
        Ok(())
    }

    // Pending removals may only be carried in the direction that recorded
    // them; moving the gap back across a pending removal would expose it.
    fn check_pending_removals(&self, direction: Direction) {
        let Some(position) = self.state.position.as_ref() else {
            return;
        };
        let gap = position.leaf.position;
        let valid = match direction {
            Direction::Next => self
                .state
                .removed_indexes
                .last()
                .is_none_or(|last| *last < gap),
            Direction::Previous => self
                .state
                .removed_indexes
                .last()
                .is_none_or(|last| *last >= gap),
        };
        assert!(valid, "pending removals must match the scan direction");
    }

    // The cursor can be positioned at the edge of a leaf. Before peeking in a
    // direction, move across empty leaf edges until the current leaf has an
    // entry on that side or the cursor reaches the tree edge. Leaving a leaf
    // flushes its pending removals and reseeks past it in the updated tree.
    fn ensure_has_entry(&mut self, direction: Direction) -> Result<bool> {
        self.check_pending_removals(direction);
        loop {
            let Some(position) = self.state.position.as_ref() else {
                return Ok(false);
            };
            if position.has_entry(direction) {
                return Ok(true);
            }
            if !self.state.removed_indexes.is_empty() {
                let resume_key = self
                    .flush_removed_entries(direction)?
                    .expect("pending removals exist");
                let resume = match direction {
                    Direction::Next => Position::After(resume_key.as_slice()),
                    Direction::Previous => Position::Before(resume_key.as_slice()),
                };
                self.seek_to(resume)?;
                continue;
            }
            return self.step_to_adjacent_leaf(direction);
        }
    }

    fn step_to_adjacent_leaf(&mut self, direction: Direction) -> Result<bool> {
        let Some(position) = self.state.position.as_mut() else {
            return Ok(false);
        };
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        if let Some(next_leaf) =
            move_to_adjacent_leaf::<K, V, _>(&mut position.path, direction, &mut get_page)?
        {
            position.leaf = next_leaf;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(super) fn peek_next(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }
        let position = self
            .state
            .position
            .as_ref()
            .expect("cursor must be positioned");
        Ok(Some(entry_ref(
            &position.leaf,
            position.entry_index(Direction::Next),
        )))
    }

    pub(super) fn peek_prev(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }
        let position = self
            .state
            .position
            .as_ref()
            .expect("cursor must be positioned");
        Ok(Some(entry_ref(
            &position.leaf,
            position.entry_index(Direction::Previous),
        )))
    }

    pub(super) fn next(&mut self) -> Result<bool> {
        if self.peek_next()?.is_none() {
            return Ok(false);
        }
        self.state
            .position
            .as_mut()
            .expect("cursor must be positioned")
            .move_once(Direction::Next);
        Ok(true)
    }

    /// Removes and returns the next entry.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    pub(super) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.state.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }
        let position = self
            .state
            .position
            .take()
            .expect("cursor must be positioned");
        let index = position.leaf.position;
        self.remove_leaf_entry(position.leaf.page, position.path, index)
    }

    // Note: discard batches may be compacted in place at flush, so they must
    // not be parked by a RangeMut; park() asserts that pending batches carry
    // detached guards.
    pub(super) fn remove_next_discard(&mut self) -> Result<bool> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(false);
        }
        self.record_removal(Direction::Next);
        Ok(true)
    }

    /// Removes and returns the next entry, deferring the leaf rewrite until the
    /// cursor leaves the leaf.
    ///
    /// The returned guards are backed by the leaf's current memory and remain
    /// valid after the deferred rewrite happens.
    pub(super) fn remove_next_deferred(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }
        Ok(Some(self.record_removal_deferred(Direction::Next)))
    }

    /// Removes and returns the previous entry.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    pub(super) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.state.removed_indexes.is_empty());
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }
        let position = self
            .state
            .position
            .take()
            .expect("cursor must be positioned");
        let index = position.leaf.position - 1;
        self.remove_leaf_entry(position.leaf.page, position.path, index)
    }

    /// Removes and returns the previous entry, deferring the leaf rewrite until
    /// the cursor leaves the leaf.
    ///
    /// The returned guards are backed by the leaf's current memory and remain
    /// valid after the deferred rewrite happens.
    pub(super) fn remove_prev_deferred(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }
        Ok(Some(self.record_removal_deferred(Direction::Previous)))
    }

    fn record_removal(&mut self, direction: Direction) -> usize {
        let position = self
            .state
            .position
            .as_mut()
            .expect("cursor must be positioned");
        let index = position.entry_index(direction);
        let monotonic = match direction {
            Direction::Next => self
                .state
                .removed_indexes
                .last()
                .is_none_or(|last| *last < index),
            Direction::Previous => self
                .state
                .removed_indexes
                .last()
                .is_none_or(|last| *last > index),
        };
        assert!(
            monotonic,
            "removed indexes must be recorded monotonically in the scan direction"
        );
        self.state.removed_indexes.push(index);
        position.move_once(direction);
        index
    }

    fn record_removal_deferred(
        &mut self,
        direction: Direction,
    ) -> (AccessGuard<'a, K>, AccessGuard<'a, V>) {
        // The Arc-backed guards stay valid because the page store hands out a
        // fresh buffer whenever a freed page number is reused, and the
        // detached flush below never mutates this leaf's bytes in place.
        let index = self.record_removal(direction);
        self.state.detached_guards = true;
        let leaf = &self
            .state
            .position
            .as_ref()
            .expect("cursor must be positioned")
            .leaf;
        let (key_range, value_range) =
            LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
                .entry_ranges(index)
                .expect("removed cursor entry must exist");
        let page = leaf.page.to_arc();
        (
            AccessGuard::with_arc_page(page.clone(), key_range),
            AccessGuard::with_arc_page(page, value_range),
        )
    }

    pub(super) fn finish_pending_removals(&mut self) -> Result {
        // The flush direction only affects the resume key, which is unused here.
        let _ = self.flush_removed_entries(Direction::Next)?;
        Ok(())
    }

    /// True if a failed flush may have left entries in the tree that the
    /// caller was told were removed. The transaction must not commit.
    pub(super) fn lost_removals(&self) -> bool {
        self.state.lost_removals
    }

    fn flush_removed_entries(&mut self, direction: Direction) -> Result<Option<Vec<u8>>> {
        if self.state.removed_indexes.is_empty() {
            return Ok(None);
        }

        let position = self
            .state
            .position
            .take()
            .expect("cursor must be positioned");
        // Tree mutation invalidates the cursor path. Callers that continue
        // iteration reseek to the first entry past the original leaf in the
        // scan direction.
        let resume_key = match direction {
            Direction::Next => key_data::<K, V>(&position.leaf, position.leaf.len - 1),
            Direction::Previous => key_data::<K, V>(&position.leaf, 0),
        };
        let (path, leaf) = position.into_parts();
        let mut removed_indexes = std::mem::take(&mut self.state.removed_indexes);
        // Backward scans record indexes in decreasing order.
        if removed_indexes.first() > removed_indexes.last() {
            removed_indexes.reverse();
        }
        let allow_in_place = !self.state.detached_guards;
        self.state.detached_guards = false;
        let mut helper: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        let result = helper.delete_leaf_entries(leaf.page, path, &removed_indexes, allow_in_place);
        if result.is_err() {
            // The batch was consumed, so the removals can no longer be applied.
            self.state.lost_removals = true;
        }
        result?;
        removed_indexes.clear();
        self.state.removed_indexes = removed_indexes;

        Ok(Some(resume_key))
    }

    fn remove_leaf_entry(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.state.removed_indexes.is_empty());
        let path = path.into_iter().map(Branch::into_parts).collect();
        let mut helper = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        let entry = helper.pop_leaf_entry(leaf, path, index)?;
        Ok(Some(entry))
    }
}

// One end of a `RangeMut`. The ends share the tree, and a mutation through
// one end invalidates the other end's cursor path, so at most one end keeps a
// live cursor at a time. The other end is parked as the key bound describing
// its gap position, which doubles as the scan limit for the live end: bounds
// only ever name keys the parked end has not consumed, so they stay valid no
// matter how the live end restructures the tree.
//
// Parking does not mutate the tree: a dirty end's pending removals are
// snapshotted with the leaf's bytes and reattached (or applied by key) when
// the end is next activated, so alternating between the ends keeps the
// per-leaf removal batching.
enum EndState {
    Parked(Bound<Vec<u8>>),
    Pending(ParkedBatch),
    Live(CursorState),
}

// Pending removals of a parked end. The snapshot holds no page references,
// so the other end is free to restructure the tree, including this leaf.
struct ParkedBatch {
    bound: Bound<Vec<u8>>,
    leaf_bytes: Arc<[u8]>,
    removed_indexes: Vec<usize>,
}

// A pair of mutable gap cursors converging over a key range, the engine
// behind extract_if. `Direction::Next` operations consume entries from the
// front of the range and `Direction::Previous` from the back; the two ends
// never yield the same entry. Removals are batched per leaf by `CursorMut`
// and flushed when the scan leaves the leaf, when the other end is activated,
// or on close.
pub(super) struct RangeMut<'a, K: Key + 'static, V: Value + 'static> {
    root: &'a mut Option<BtreeHeader>,
    page_allocator: PageAllocator,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    // Pages freed by cursor mutations, drained after every operation so the
    // master list's lock is never held while control returns to the caller.
    freed: Vec<PageNumber>,
    front: EndState,
    back: EndState,
    // Which end, if any, is known to be live and positioned at an in-range
    // entry. Cleared whenever the gap moves or the tree is mutated.
    settled: Option<Direction>,
    // Set when a flush consumed pending removals but failed to apply them:
    // entries already yielded as removed may remain in the tree, so the
    // caller must not let the transaction commit.
    lost_removals: bool,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> RangeMut<'a, K, V> {
    pub(super) fn new(
        root: &'a mut Option<BtreeHeader>,
        lower_bound: Bound<Vec<u8>>,
        upper_bound: Bound<Vec<u8>>,
        page_allocator: PageAllocator,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            root,
            page_allocator,
            allocated,
            master_free_list,
            freed: vec![],
            front: EndState::Parked(lower_bound),
            back: EndState::Parked(upper_bound),
            settled: None,
            lost_removals: false,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub(super) fn peek_next(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        self.peek(Direction::Next)
    }

    pub(super) fn peek_prev(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        self.peek(Direction::Previous)
    }

    pub(super) fn next(&mut self) -> Result<bool> {
        self.advance(Direction::Next)
    }

    pub(super) fn prev(&mut self) -> Result<bool> {
        self.advance(Direction::Previous)
    }

    /// Removes and returns the next entry, if the range is not exhausted.
    ///
    /// The returned guards remain valid for the life of the transaction.
    pub(super) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove(Direction::Next)
    }

    /// Removes and returns the previous entry, if the range is not exhausted.
    ///
    /// The returned guards remain valid for the life of the transaction.
    pub(super) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove(Direction::Previous)
    }

    /// Flushes pending removals on both ends. Must be called before the tree
    /// root is read or the tree is mutated through another path.
    pub(super) fn close(&mut self) -> Result {
        let front = self.flush_end(Direction::Next);
        let back = self.flush_end(Direction::Previous);
        front.and(back)
    }

    /// True if a failed flush may have left entries in the tree that were
    /// already yielded as removed. The transaction must not commit.
    pub(super) fn lost_removals(&self) -> bool {
        self.lost_removals
    }

    // Applies an end's pending removals to the tree, leaving it parked.
    fn flush_end(&mut self, direction: Direction) -> Result {
        if matches!(self.end_ref(direction), EndState::Live(_)) {
            let result =
                self.with_live_cursor(direction, |cursor| cursor.finish_pending_removals());
            self.park(direction);
            result
        } else {
            self.apply_pending(direction)
        }
    }

    fn peek(&mut self, direction: Direction) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.settle(direction)? {
            return Ok(None);
        }
        let EndState::Live(state) = self.end_ref(direction) else {
            unreachable!("settled end must be live");
        };
        let position = state.position.as_ref().expect("settled end is positioned");
        Ok(Some(entry_ref(
            &position.leaf,
            position.entry_index(direction),
        )))
    }

    fn advance(&mut self, direction: Direction) -> Result<bool> {
        if !self.settle(direction)? {
            return Ok(false);
        }
        self.settled = None;
        let EndState::Live(state) = self.end_mut(direction) else {
            unreachable!("settled end must be live");
        };
        state
            .position
            .as_mut()
            .expect("settled end is positioned")
            .move_once(direction);
        Ok(true)
    }

    fn remove(
        &mut self,
        direction: Direction,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        if !self.settle(direction)? {
            return Ok(None);
        }
        self.settled = None;
        let result = self.with_live_cursor(direction, |cursor| match direction {
            Direction::Next => cursor.remove_next_deferred(),
            Direction::Previous => cursor.remove_prev_deferred(),
        })?;
        Ok(Some(result.expect("settled entry must be removable")))
    }

    // Ensures the cursor for `direction` is live and positioned at an entry,
    // and that the entry is within the range left between the two ends.
    fn settle(&mut self, direction: Direction) -> Result<bool> {
        if self.settled == Some(direction) {
            return Ok(true);
        }
        self.activate(direction)?;
        let has_entry = self.with_live_cursor(direction, |cursor| match direction {
            Direction::Next => Ok(cursor.peek_next()?.is_some()),
            Direction::Previous => Ok(cursor.peek_prev()?.is_some()),
        })?;
        if !has_entry {
            return Ok(false);
        }
        if !self.entry_in_range(direction) {
            return Ok(false);
        }
        self.settled = Some(direction);
        Ok(true)
    }

    // Makes `direction`'s end live, parking the other end first so that only
    // one end ever holds page references across a mutation.
    fn activate(&mut self, direction: Direction) -> Result {
        if matches!(self.end_ref(direction), EndState::Live(_)) {
            return Ok(());
        }
        self.park(direction.opposite());
        let mut state = self.seek_end(direction)?;
        if matches!(self.end_ref(direction), EndState::Pending(_)) {
            let EndState::Pending(batch) =
                std::mem::replace(self.end_mut(direction), EndState::Parked(Unbounded))
            else {
                unreachable!();
            };
            // Identical leaf bytes mean an identical layout, so the snapshot's
            // indexes are valid against the landed leaf no matter how it got there.
            if snapshot_matches(&state, &batch.leaf_bytes) {
                state.removed_indexes = batch.removed_indexes;
                state.detached_guards = true;
            } else {
                // The other end rewrote the leaf: apply the snapshot by key
                // and reseek. Both steps require this end to hold no pages.
                drop(state);
                *self.end_mut(direction) = EndState::Parked(batch.bound.clone());
                self.resolve_batch(direction, batch)?;
                state = self.seek_end(direction)?;
            }
        }
        *self.end_mut(direction) = EndState::Live(state);
        Ok(())
    }

    // Descends to the gap described by `direction`'s parked bound.
    fn seek_end(&mut self, direction: Direction) -> Result<CursorState> {
        let bound = match self.end_ref(direction) {
            EndState::Parked(bound) => bound.clone(),
            EndState::Pending(batch) => batch.bound.clone(),
            EndState::Live(_) => unreachable!("end must be parked"),
        };
        let target = match direction {
            Direction::Next => match &bound {
                Included(key) => Position::Before(key),
                Excluded(key) => Position::After(key),
                Unbounded => Position::Start,
            },
            Direction::Previous => match &bound {
                Included(key) => Position::After(key),
                Excluded(key) => Position::Before(key),
                Unbounded => Position::End,
            },
        };
        let mut cursor = self.cursor(CursorState::default());
        let result = cursor.seek_to(target);
        let state = cursor.into_state();
        // On error the end stays parked, preserving any pending batch for close().
        result?;
        Ok(state)
    }

    // Reduces a live end to a parked form. This never mutates the tree:
    // pending removals are snapshotted and applied later.
    fn park(&mut self, direction: Direction) {
        self.settled = None;
        let end = self.end_mut(direction);
        let EndState::Live(state) = end else {
            return;
        };
        let bound = park_bound::<K, V>(state, direction);
        let parked = if state.removed_indexes.is_empty() {
            EndState::Parked(bound)
        } else {
            debug_assert!(state.detached_guards);
            let position = state
                .position
                .as_ref()
                .expect("pending removals require a position");
            EndState::Pending(ParkedBatch {
                bound,
                leaf_bytes: position.leaf.page.to_arc(),
                removed_indexes: std::mem::take(&mut state.removed_indexes),
            })
        };
        *end = parked;
    }

    // Applies a parked end's pending removals to the tree.
    fn apply_pending(&mut self, direction: Direction) -> Result {
        if !matches!(self.end_ref(direction), EndState::Pending(_)) {
            return Ok(());
        }
        self.activate(direction)?;
        self.with_live_cursor(direction, |cursor| cursor.finish_pending_removals())?;
        self.park(direction);
        Ok(())
    }

    // Applies a snapshotted batch whose leaf was rewritten by the other end:
    // the pending entries are deleted by key, recovered from the snapshot.
    fn resolve_batch(&mut self, direction: Direction, mut batch: ParkedBatch) -> Result {
        while let Some(index) = batch.removed_indexes.pop() {
            let key = LeafAccessor::new(&batch.leaf_bytes, K::fixed_width(), V::fixed_width())
                .entry(index)
                .expect("snapshot entry must exist")
                .key();
            let mut helper: MutateHelper<'_, '_, K, V> = MutateHelper::new(
                &mut *self.root,
                self.page_allocator.clone(),
                &mut self.freed,
                Arc::clone(&self.allocated),
            );
            let result = helper.delete_key(key);
            self.drain_freed();
            match result {
                Ok(removed) => debug_assert!(removed.is_some()),
                Err(err) => {
                    // Keep the unapplied removals so that close() retries them.
                    batch.removed_indexes.push(index);
                    *self.end_mut(direction) = EndState::Pending(batch);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn with_live_cursor<R>(
        &mut self,
        direction: Direction,
        operation: impl FnOnce(&mut CursorMut<'a, '_, K, V>) -> Result<R>,
    ) -> Result<R> {
        let end = self.end_mut(direction);
        let EndState::Live(state) = std::mem::replace(end, EndState::Parked(Unbounded)) else {
            unreachable!("end must be live");
        };
        let mut cursor = self.cursor(state);
        let result = operation(&mut cursor);
        let state = cursor.into_state();
        if state.lost_removals {
            self.lost_removals = true;
        }
        *self.end_mut(direction) = EndState::Live(state);
        self.drain_freed();
        result
    }

    fn cursor(&mut self, state: CursorState) -> CursorMut<'a, '_, K, V> {
        CursorMut::with_state(
            &mut *self.root,
            &self.page_allocator,
            &mut self.freed,
            &self.allocated,
            state,
        )
    }

    // Whether the live end's current entry is still inside the range bounded
    // by the parked end.
    fn entry_in_range(&self, direction: Direction) -> bool {
        let EndState::Live(state) = self.end_ref(direction) else {
            unreachable!("end must be live");
        };
        let position = state.position.as_ref().expect("end is positioned");
        let entry = entry_ref::<K, V>(&position.leaf, position.entry_index(direction));
        let key = entry.key_bytes();
        let bound = match self.end_ref(direction.opposite()) {
            EndState::Parked(bound) => bound,
            EndState::Pending(batch) => &batch.bound,
            EndState::Live(_) => unreachable!("peer end must be parked while this end is live"),
        };
        match direction {
            Direction::Next => match bound {
                Included(bound) => K::compare(key, bound).is_le(),
                Excluded(bound) => K::compare(key, bound).is_lt(),
                Unbounded => true,
            },
            Direction::Previous => match bound {
                Included(bound) => K::compare(key, bound).is_ge(),
                Excluded(bound) => K::compare(key, bound).is_gt(),
                Unbounded => true,
            },
        }
    }

    fn end_ref(&self, direction: Direction) -> &EndState {
        match direction {
            Direction::Next => &self.front,
            Direction::Previous => &self.back,
        }
    }

    fn end_mut(&mut self, direction: Direction) -> &mut EndState {
        match direction {
            Direction::Next => &mut self.front,
            Direction::Previous => &mut self.back,
        }
    }

    fn drain_freed(&mut self) {
        if self.freed.is_empty() {
            return;
        }
        let mut master_free_list = self.master_free_list.lock().unwrap();
        let mut allocated = self.allocated.lock().unwrap();
        for page in self.freed.drain(..) {
            if !self
                .page_allocator
                .free_if_uncommitted(page, &mut allocated)
            {
                master_free_list.push(page);
            }
        }
    }
}

// Whether the cursor landed on a leaf whose memory is byte-identical to the
// snapshot. Identical bytes imply an identical entry layout.
fn snapshot_matches(state: &CursorState, snapshot: &[u8]) -> bool {
    state
        .position
        .as_ref()
        .is_some_and(|position| position.leaf.page.memory() == snapshot)
}

// The bound form of a gap cursor's logical position, used to park one end of
// a `RangeMut` and later reseek it. Bounds name a key adjacent to the gap in
// the leaf's pre-flush memory; seeks resolve them by comparison, so they stay
// correct even if the named key is itself a pending removal.
fn park_bound<K: Key + 'static, V: Value + 'static>(
    state: &CursorState,
    direction: Direction,
) -> Bound<Vec<u8>> {
    // The cursor was never positioned, which only happens when the tree is
    // empty; any bound is equivalent.
    let Some(position) = state.position.as_ref() else {
        return Unbounded;
    };
    let leaf = &position.leaf;
    match direction {
        Direction::Next => {
            if leaf.position < leaf.len {
                Included(key_data::<K, V>(leaf, leaf.position))
            } else {
                Excluded(key_data::<K, V>(leaf, leaf.len - 1))
            }
        }
        Direction::Previous => {
            if leaf.position > 0 {
                Included(key_data::<K, V>(leaf, leaf.position - 1))
            } else {
                Excluded(key_data::<K, V>(leaf, 0))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree_store::btree_base::LeafBuilder;
    use crate::tree_store::{
        AllocationPolicy, InMemoryBackend, PAGE_SIZE, PageTrackerPolicy, TransactionalMemory,
    };

    fn cursor_with_entries(entries: &[u64]) -> Cursor<u64, u64> {
        let mem = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
        )
        .unwrap();
        mem.begin_repair().unwrap();
        let mem = Arc::new(mem);
        let page_allocator = PageAllocator::new(mem, AllocationPolicy::Default);
        let allocated_pages = Mutex::new(PageTrackerPolicy::new_tracking());
        let keys_and_values: Vec<_> = entries
            .iter()
            .map(|entry| {
                (
                    u64::as_bytes(entry).as_ref().to_vec(),
                    u64::as_bytes(entry).as_ref().to_vec(),
                )
            })
            .collect();
        let mut builder = LeafBuilder::new(
            &page_allocator,
            &allocated_pages,
            entries.len(),
            u64::fixed_width(),
            u64::fixed_width(),
        );
        for (key, value) in &keys_and_values {
            builder.push(key, value);
        }
        let page = builder.build().unwrap();
        let root = page.get_page_number();
        drop(page);

        let mut cursor = Cursor::<u64, u64>::new(root, page_allocator.resolver(), PageHint::None);
        cursor.seek_to(Position::Start).unwrap();
        cursor
    }

    #[test]
    fn cursor_preserves_boundary_gap_after_failed_next() {
        let mut cursor = cursor_with_entries(&[1, 2, 3]);

        for expected in [1, 2, 3] {
            assert_eq!(cursor.next().unwrap().unwrap().key(), expected);
        }
        assert!(cursor.next().unwrap().is_none());

        assert_eq!(cursor.prev().unwrap().unwrap().key(), 3);
        assert_eq!(cursor.prev().unwrap().unwrap().key(), 2);
    }

    #[test]
    fn cursor_preserves_boundary_gap_after_failed_prev() {
        let mut cursor = cursor_with_entries(&[1, 2, 3]);

        assert!(cursor.prev().unwrap().is_none());

        assert_eq!(cursor.next().unwrap().unwrap().key(), 1);
        assert_eq!(cursor.next().unwrap().unwrap().key(), 2);
    }
}
