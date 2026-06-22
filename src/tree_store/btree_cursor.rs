use crate::AccessGuard;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, LEAF, LeafAccessor, OwnedEntryBuffer, leaf_below_merge_threshold,
    retained_after_removals,
};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTrackerPolicy};
use crate::types::{Key, Value};
use crate::{Result, StorageError};
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

    // The index and page of the child adjacent to `child_index` in `direction`,
    // if the branch has one. Shared by the stepping code and the run machinery,
    // whose "the step stays under this parent" invariant requires they agree.
    fn adjacent_child(
        &self,
        direction: Direction,
        fixed_key_width: Option<usize>,
    ) -> Option<(usize, PageNumber)> {
        let accessor = BranchAccessor::new(&self.page, fixed_key_width);
        let child_index = match direction {
            Direction::Next => {
                let next = self.child_index + 1;
                (next < accessor.count_children()).then_some(next)?
            }
            Direction::Previous => self.child_index.checked_sub(1)?,
        };
        Some((child_index, accessor.child_page(child_index).unwrap()))
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
        if let Some((child_index, child_page)) =
            path[index].adjacent_child(direction, K::fixed_width())
        {
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

// The key a scan reseeks past after this leaf is flushed or spliced away: the
// leaf's key furthest in the scan direction.
fn scan_boundary_key<K: Key + 'static, V: Value + 'static>(
    leaf: &Leaf,
    direction: Direction,
) -> Vec<u8> {
    match direction {
        Direction::Next => key_data::<K, V>(leaf, leaf.len - 1),
        Direction::Previous => key_data::<K, V>(leaf, 0),
    }
}

#[derive(Clone)]
struct Leaf {
    page: PageImpl,
    position: usize,
    len: usize,
}

// A contiguous run of leaf children under one parent branch. When deleting
// from a leaf would leave it sparse, the cursor buffers the run's retained
// entries and replaces the originals with packed leaves in one parent update,
// instead of merging each sparse leaf into its neighbor one rewrite at a time.
//
// While a run is open the tree is never mutated, so cursor paths stay valid.
// The buffered entries hold no page references, so an open run also survives
// foreign mutations; only the parent path captured at splice time must be
// current. Runs grow in either scan direction.
struct LeafRunRewrite {
    parent_page: PageNumber,
    direction: Direction,
    // The parent's children consumed so far: a contiguous range extended
    // upward by forward scans and downward by backward scans.
    replaced_children: Range<usize>,
    // All retained entries of the run, kept in ascending key order: forward
    // scans append at the back, backward scans prepend at the front. Only
    // leaves left below the merge threshold accumulate (each retaining less
    // than a third of a page) plus at most one run-ending leaf, so the buffer
    // is bounded by roughly fanout x page_size / 3.
    entries: OwnedEntryBuffer,
    removed_pairs: u64,
}

impl LeafRunRewrite {
    fn new(parent_page: PageNumber, child_index: usize, direction: Direction) -> Self {
        let origin = match direction {
            Direction::Next => child_index,
            Direction::Previous => child_index + 1,
        };
        let replaced_children = origin..origin;
        Self {
            parent_page,
            direction,
            replaced_children,
            entries: OwnedEntryBuffer::default(),
            removed_pairs: 0,
        }
    }

    fn append_entries_from<K: Key, V: Value>(
        &mut self,
        page: PageImpl,
        child_index: usize,
        removed_indexes: &[usize],
    ) {
        debug_assert!(removed_indexes.windows(2).all(|pair| pair[0] < pair[1]));
        // Hard asserts: a violation would splice the wrong children out of
        // the parent, so it must not be compiled out of release builds.
        match self.direction {
            Direction::Next => {
                assert_eq!(child_index, self.replaced_children.end);
                self.replaced_children.end += 1;
            }
            Direction::Previous => {
                assert_eq!(child_index + 1, self.replaced_children.start);
                self.replaced_children.start -= 1;
            }
        }
        self.removed_pairs += removed_indexes.len() as u64;

        // A forward scan's leaf is entirely greater than the run so far, and a
        // backward scan's entirely smaller.
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        self.entries
            .extend_from_leaf(&accessor, removed_indexes, self.direction.is_next());
    }
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
    // Set when an error interrupted removals that were already reported to
    // the caller: they may remain in the tree, so the transaction must not
    // commit. Every cursor operation re-raises instead of touching the tree,
    // since the position and run were discarded at the point of failure.
    poisoned: bool,
    // An in-progress sparse-leaf coalescing run. It holds no page references,
    // so it may outlive the position; it is spliced before the tree is
    // observed.
    leaf_run_rewrite: Option<LeafRunRewrite>,
}

enum LeafCloseOutcome {
    // The leaf had no pending removals and remains the current cursor leaf.
    Unchanged,
    // The tree was rewritten and the cursor position consumed; resume the
    // scan from `resume_key`.
    Flushed { resume_key: Vec<u8> },
    // The leaf was absorbed into the open run, whose parent has more children
    // to consume. The tree is untouched, so the cursor position (still on the
    // absorbed leaf) remains valid.
    AbsorbedIntoRun,
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
        self.check_not_poisoned()?;
        assert!(self.state.leaf_run_rewrite.is_none());
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
        self.check_not_poisoned()?;
        self.check_pending_removals(direction);
        loop {
            let Some(position) = self.state.position.as_ref() else {
                return Ok(false);
            };
            if position.has_entry(direction) {
                return Ok(true);
            }
            if !self.advance_past_closed_leaf(direction)? {
                return Ok(false);
            }
        }
    }

    // Closes the exhausted leaf (flushing its pending removals directly or
    // into a coalescing run) and positions the cursor at the adjacent leaf.
    // Returns false at the edge of the tree, leaving the cursor parked on the
    // edge leaf so that `park_bound` still describes the consumed position.
    fn advance_past_closed_leaf(&mut self, direction: Direction) -> Result<bool> {
        match self.close_current_leaf(direction)? {
            LeafCloseOutcome::Unchanged => self.step_to_adjacent_leaf(direction),
            LeafCloseOutcome::Flushed { resume_key } => {
                self.resume_after_rewrite(direction, &resume_key)?;
                Ok(self.state.position.is_some())
            }
            LeafCloseOutcome::AbsorbedIntoRun => {
                // The immediate parent has an adjacent child (else the run
                // would have been spliced), so the ordinary step is guaranteed
                // to stay under it. A failed step may leave the path partially
                // updated, so the open run's removals can no longer be
                // applied.
                let stepped = self
                    .step_to_adjacent_leaf(direction)
                    .inspect_err(|_| self.poison())?;
                assert!(stepped);
                Ok(stepped)
            }
        }
    }

    // The parent frame of the open run: while a run is open, the position
    // stays under the run's parent.
    fn run_parent_frame(&self) -> &Branch {
        let position = self
            .state
            .position
            .as_ref()
            .expect("cursor must be positioned");
        position
            .path
            .last()
            .expect("leaf runs require a parent branch")
    }

    // Whether the run's parent branch has another child to consume in the
    // scan direction.
    fn run_parent_has_more_children(&self, direction: Direction) -> bool {
        self.run_parent_frame()
            .adjacent_child(direction, K::fixed_width())
            .is_some()
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
        // fresh buffer whenever a freed page number is reused, and no flush
        // that can run while these guards exist mutates leaf bytes in place:
        // the detached flush below and `RangeMut`'s batch resolution by key
        // both rewrite leaves instead.
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
        self.check_not_poisoned()?;
        let direction = self
            .state
            .leaf_run_rewrite
            .as_ref()
            .map_or(Direction::Next, |run| run.direction);
        // A clean current leaf need not be closed: the splice leaves it in
        // place, and an empty batch has nothing to flush or absorb.
        if self.state.position.is_some() && !self.state.removed_indexes.is_empty() {
            self.close_current_leaf(direction)?;
        }
        // Splices the run left open when the scan stopped partway through
        // its parent.
        self.splice_open_run()
    }

    /// True if an error interrupted removals that were already reported to
    /// the caller: they may remain in the tree, so the transaction must not
    /// commit.
    pub(super) fn poisoned(&self) -> bool {
        self.state.poisoned
    }

    // Marks the removals already reported to the caller as unappliable, after
    // an error consumed them or broke the paths needed to apply them. Safety
    // comes from `check_not_poisoned` re-raising at every entry point; the
    // discards below just release page references and buffers early.
    fn poison(&mut self) {
        self.state.poisoned = true;
        self.state.position = None;
        self.state.removed_indexes.clear();
        self.state.leaf_run_rewrite = None;
    }

    fn check_not_poisoned(&self) -> Result {
        if self.state.poisoned {
            return Err(StorageError::PreviousIo);
        }
        Ok(())
    }

    // Applies the pending removals, returning the key past which the scan
    // resumes.
    fn flush_removed_entries(&mut self, direction: Direction) -> Result<Vec<u8>> {
        assert!(!self.state.removed_indexes.is_empty());

        let position = self
            .state
            .position
            .take()
            .expect("cursor must be positioned");
        // Tree mutation invalidates the cursor path. Callers that continue
        // iteration reseek to the first entry past the original leaf in the
        // scan direction.
        let resume_key = scan_boundary_key::<K, V>(&position.leaf, direction);
        let (path, leaf) = position.into_parts();
        let allow_in_place = !self.state.detached_guards;
        let removed_indexes = self.take_removals_ascending();
        let result = self.mutate_helper().delete_leaf_entries(
            leaf.page,
            path,
            &removed_indexes,
            allow_in_place,
        );
        if result.is_err() {
            // The batch was consumed, so the removals can no longer be applied.
            self.poison();
        }
        result?;

        Ok(resume_key)
    }

    // Flushes the current leaf's pending removals, either directly or into a
    // coalescing run. Only a direct flush mutates the tree and consumes the
    // cursor position; absorbing into a run leaves both untouched.
    fn close_current_leaf(&mut self, direction: Direction) -> Result<LeafCloseOutcome> {
        assert!(self.state.position.is_some(), "cursor must be positioned");

        // The short-circuit order matters: `leaf_would_underfill` may only be
        // consulted for a leaf with pending removals, so a merely-iterated
        // sparse leaf cannot open a run.
        let has_removals = !self.state.removed_indexes.is_empty();
        let underfilling = has_removals && {
            let position = self.state.position.as_ref().unwrap();
            self.leaf_would_underfill(&position.leaf) && !position.path.is_empty()
        };
        if underfilling {
            let removed_indexes = self.take_removals_ascending();
            self.append_leaf_to_run(direction, &removed_indexes);
            if self.run_parent_has_more_children(direction) {
                return Ok(LeafCloseOutcome::AbsorbedIntoRun);
            }
            // The run consumed its parent's last child in the scan direction,
            // so nothing more can join it; fall through to the splice.
        } else if self.state.leaf_run_rewrite.is_some() {
            // A leaf that stays healthy on its own ends the run, so a run's
            // rewrites stay proportional to the sparse region. Its pending
            // removals ride the splice; a fully retained leaf is left in
            // place, though a run that packs below the merge threshold may
            // still merge with it when the splice absorbs an adjacent child.
            if has_removals {
                let removed_indexes = self.take_removals_ascending();
                self.append_leaf_to_run(direction, &removed_indexes);
            }
        } else if has_removals {
            let resume_key = self.flush_removed_entries(direction)?;
            return Ok(LeafCloseOutcome::Flushed { resume_key });
        } else {
            return Ok(LeafCloseOutcome::Unchanged);
        }
        // Every remaining path ends the run: splice it and resume past this
        // leaf, whose furthest key bounds everything the run consumed.
        let position = self.state.position.as_ref().unwrap();
        let resume_key = scan_boundary_key::<K, V>(&position.leaf, direction);
        self.splice_open_run()?;
        Ok(LeafCloseOutcome::Flushed { resume_key })
    }

    // Takes the pending batch in ascending order; backward scans record their
    // batch in decreasing order.
    fn take_removals_ascending(&mut self) -> Vec<usize> {
        let mut removed_indexes = std::mem::take(&mut self.state.removed_indexes);
        if removed_indexes.first() > removed_indexes.last() {
            removed_indexes.reverse();
        }
        // The batch is consumed, so it no longer constrains flushes.
        self.state.detached_guards = false;
        removed_indexes
    }

    fn resume_after_rewrite(&mut self, direction: Direction, key: &[u8]) -> Result {
        match direction {
            Direction::Next => self.seek_to(Position::After(key)),
            Direction::Previous => self.seek_to(Position::Before(key)),
        }
    }

    // Absorbs the current leaf into the run, opening one if necessary. The
    // tree and the cursor position are left untouched.
    fn append_leaf_to_run(&mut self, direction: Direction, removed_indexes: &[usize]) {
        let (page, parent_page, child_index) = {
            let frame = self.run_parent_frame();
            let position = self.state.position.as_ref().unwrap();
            (
                position.leaf.page.clone(),
                frame.page.get_page_number(),
                frame.child_index,
            )
        };
        let run = self
            .state
            .leaf_run_rewrite
            .get_or_insert_with(|| LeafRunRewrite::new(parent_page, child_index, direction));
        // Hard asserts: a stale parent or mixed scan direction would splice
        // the wrong children, so they must hold in release builds too.
        assert_eq!(run.parent_page, parent_page);
        assert!(run.direction == direction);
        run.append_entries_from::<K, V>(page, child_index, removed_indexes);
    }

    // Replaces the run's children in the parent with packed leaves built from
    // the buffered entries, while the cursor path is still valid. The cursor
    // position is consumed; callers reseek from the run's boundary key. On
    // error the cursor is poisoned: the run's removals were lost.
    pub(super) fn splice_open_run(&mut self) -> Result {
        self.check_not_poisoned()?;
        let Some(run) = self.state.leaf_run_rewrite.take() else {
            return Ok(());
        };
        let position = self
            .state
            .position
            .take()
            .expect("open run requires a position");
        let result = self.splice_run(run, position);
        if result.is_err() {
            // The removals the run carried can no longer be applied.
            self.poison();
        }
        result
    }

    fn splice_run(&mut self, run: LeafRunRewrite, position: CursorPosition) -> Result {
        // The leaf is one of the pages the splice frees, so its page reference
        // must be released first; only the parent path is needed below.
        let CursorPosition { path, leaf } = position;
        drop(leaf);
        // Splicing against a stale parent would replace the wrong children.
        assert_eq!(
            path.last()
                .expect("leaf runs require a parent branch")
                .page
                .get_page_number(),
            run.parent_page
        );
        self.mutate_helper().replace_leaf_children(
            path.into_iter().map(Branch::into_parts).collect(),
            run.replaced_children,
            run.entries,
            run.removed_pairs,
        )
    }

    // Whether removing the pending entries would leave the leaf below the merge
    // threshold, matching `MutateHelper::plan_leaf_delete`'s Merge disposition.
    fn leaf_would_underfill(&self, leaf: &Leaf) -> bool {
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        let (retained_pairs, retained_bytes) =
            retained_after_removals(&accessor, &self.state.removed_indexes);
        retained_pairs == 0
            || leaf_below_merge_threshold(
                retained_pairs,
                retained_bytes,
                K::fixed_width(),
                V::fixed_width(),
                self.page_allocator.get_page_size(),
            )
    }

    // All tree mutations flow through here. Mutating invalidates saved paths,
    // so any open run must have been spliced first; a hard assert because a
    // violation would later splice through a stale path.
    fn mutate_helper<'c>(&'c mut self) -> MutateHelper<'a, 'c, K, V> {
        assert!(self.state.leaf_run_rewrite.is_none());
        MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        )
    }

    fn remove_leaf_entry(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.state.removed_indexes.is_empty());
        let path = path.into_iter().map(Branch::into_parts).collect();
        let entry = self.mutate_helper().pop_leaf_entry(leaf, path, index)?;
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
// However, `leaf_bytes` may share the buffer of a live dirty leaf, so tree
// mutations that can run while the batch exists must not modify leaf memory
// in place.
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
    // Set when an error interrupted removals that were already yielded to the
    // caller: they may remain in the tree, so the transaction must not
    // commit. Every range operation re-raises instead of touching the tree.
    poisoned: bool,
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
            poisoned: false,
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
    ///
    /// Consumers must treat iteration errors as terminal: an end whose
    /// position was lost may park a bound that un-consumes entries it already
    /// yielded, so continuing past an error can re-yield them. Call `close()`
    /// and stop, like `BtreeExtractIf`'s latch.
    pub(super) fn close(&mut self) -> Result {
        let front = self.flush_end(Direction::Next);
        let back = self.flush_end(Direction::Previous);
        front.and(back)
    }

    /// True if an error interrupted removals that were already yielded to the
    /// caller: they may remain in the tree, so the transaction must not
    /// commit.
    pub(super) fn poisoned(&self) -> bool {
        self.poisoned
    }

    fn check_not_poisoned(&self) -> Result {
        if self.poisoned {
            return Err(StorageError::PreviousIo);
        }
        Ok(())
    }

    // Applies an end's pending removals to the tree, leaving it parked.
    fn flush_end(&mut self, direction: Direction) -> Result {
        self.check_not_poisoned()?;
        if matches!(self.end_ref(direction), EndState::Live(_)) {
            let result =
                self.with_live_cursor(direction, |cursor| cursor.finish_pending_removals());
            result.and(self.park(direction))
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
        self.check_not_poisoned()?;
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
        self.park(direction.opposite())?;
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
                self.resolve_batch(batch)?;
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

    // Reduces a live end to a parked form. An open coalescing run is spliced
    // first, while the cursor path is still valid; mid-leaf pending removals
    // are snapshotted and applied later.
    fn park(&mut self, direction: Direction) -> Result {
        self.settled = None;
        let end = self.end_mut(direction);
        let EndState::Live(state) = end else {
            return Ok(());
        };
        // Capture the parked form before the splice consumes the position.
        // The splice rewrites the parent, but not this leaf's memory, so the
        // snapshot stays valid.
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
        let result = if state.leaf_run_rewrite.is_some() {
            self.with_live_cursor(direction, |cursor| cursor.splice_open_run())
        } else {
            Ok(())
        };
        *self.end_mut(direction) = parked;
        self.drain_freed();
        result
    }

    // Applies a parked end's pending removals to the tree.
    fn apply_pending(&mut self, direction: Direction) -> Result {
        if !matches!(self.end_ref(direction), EndState::Pending(_)) {
            return Ok(());
        }
        self.activate(direction)?;
        self.with_live_cursor(direction, |cursor| cursor.finish_pending_removals())?;
        self.park(direction)
    }

    // Applies a snapshotted batch whose leaf was rewritten by the other end:
    // the pending entries are deleted by key, recovered from the snapshot. On
    // error the range is poisoned: the batch was consumed, so its removals
    // can no longer be applied.
    fn resolve_batch(&mut self, batch: ParkedBatch) -> Result {
        // Mutating would invalidate an open run's saved parent path; this
        // mirrors CursorMut::mutate_helper's chokepoint assert, which the raw
        // MutateHelper below bypasses.
        for direction in [Direction::Next, Direction::Previous] {
            if let EndState::Live(state) = self.end_ref(direction) {
                assert!(state.leaf_run_rewrite.is_none());
            }
        }
        for &index in &batch.removed_indexes {
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
            // In-place deletion must be disabled: the other end's pending
            // snapshot and any deferred-removal guards handed to the caller
            // may share the buffer of the live leaf containing this key.
            let result = helper.delete_key(key, false);
            self.drain_freed();
            match result {
                Ok(removed) => debug_assert!(removed.is_some()),
                Err(err) => {
                    self.poisoned = true;
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
        if state.poisoned {
            self.poisoned = true;
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
    // No position: the tree was empty, or a flush/seek error consumed it.
    // Unbounded is safe only because those paths are terminal (the flush
    // paths run from close(), and errors poison or latch); parking a live
    // end Unbounded would un-consume everything it already yielded.
    let Some(position) = state.position.as_ref() else {
        return Unbounded;
    };
    let leaf = &position.leaf;
    // A consumed leaf edge parks as the same exclusive boundary that flushes
    // and splices resume from; the two must agree so that neither end of a
    // range re-yields or skips an entry.
    match direction {
        Direction::Next if leaf.position < leaf.len => {
            Included(key_data::<K, V>(leaf, leaf.position))
        }
        Direction::Previous if leaf.position > 0 => {
            Included(key_data::<K, V>(leaf, leaf.position - 1))
        }
        _ => Excluded(scan_boundary_key::<K, V>(leaf, direction)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree_store::btree_base::{DEFERRED, LeafBuilder};
    use crate::tree_store::{
        AllocationPolicy, InMemoryBackend, PAGE_SIZE, PageTrackerPolicy, TransactionalMemory,
    };

    fn leaf_root_with_entries(entries: &[u64]) -> (PageAllocator, PageNumber) {
        let mem = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();
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

        (page_allocator, root)
    }

    fn cursor_with_entries(entries: &[u64]) -> Cursor<u64, u64> {
        let (page_allocator, root) = leaf_root_with_entries(entries);
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

    // An exhausted mutable cursor must stay parked on the edge leaf: park_bound
    // relies on the position to bound the other end of a RangeMut, and an
    // unpositioned cursor would park as Unbounded, un-consuming the entries.
    #[test]
    fn cursor_mut_stays_parked_at_tree_edge() {
        let (page_allocator, root_page) = leaf_root_with_entries(&[1, 2, 3]);
        let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
        let mut freed = vec![];
        let allocated = Arc::new(Mutex::new(PageTrackerPolicy::new_tracking()));
        let mut cursor: CursorMut<'_, '_, u64, u64> =
            CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);

        cursor.seek_to(Position::Start).unwrap();
        for _ in 0..3 {
            assert!(cursor.next().unwrap());
        }
        assert!(cursor.peek_next().unwrap().is_none());
        assert!(cursor.state.position.is_some());
        assert!(matches!(
            park_bound::<u64, u64>(&cursor.state, Direction::Next),
            Excluded(_)
        ));

        cursor.seek_to(Position::End).unwrap();
        for _ in 0..3 {
            assert!(cursor.peek_prev().unwrap().is_some());
            cursor
                .state
                .position
                .as_mut()
                .unwrap()
                .move_once(Direction::Previous);
        }
        assert!(cursor.peek_prev().unwrap().is_none());
        assert!(cursor.state.position.is_some());
        assert!(matches!(
            park_bound::<u64, u64>(&cursor.state, Direction::Previous),
            Excluded(_)
        ));
    }

    // Once poisoned, every cursor operation re-raises instead of touching the
    // tree, so removals stranded by the original error can never be observed
    // as applied.
    #[test]
    fn poisoned_cursor_mut_re_raises() {
        let (page_allocator, root_page) = leaf_root_with_entries(&[1, 2, 3]);
        let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
        let mut freed = vec![];
        let allocated = Arc::new(Mutex::new(PageTrackerPolicy::new_tracking()));
        let mut cursor: CursorMut<'_, '_, u64, u64> =
            CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);

        cursor.seek_to(Position::Start).unwrap();
        cursor.poison();
        assert!(cursor.poisoned());
        assert!(matches!(cursor.peek_next(), Err(StorageError::PreviousIo)));
        assert!(matches!(
            cursor.seek_to(Position::Start),
            Err(StorageError::PreviousIo)
        ));
        assert!(matches!(
            cursor.finish_pending_removals(),
            Err(StorageError::PreviousIo)
        ));
        assert!(matches!(
            cursor.splice_open_run(),
            Err(StorageError::PreviousIo)
        ));
    }
}
