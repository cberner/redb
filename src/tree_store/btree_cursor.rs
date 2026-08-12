use crate::AccessGuard;
use crate::sync::Mutex;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, LEAF, LeafAccessor, MAX_BTREE_DEPTH, OwnedEntryBuffer,
    leaf_below_merge_threshold, leaf_fits_one_page, retained_after_removals,
};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTracker};
use crate::types::{Key, Value};
use crate::{Result, StorageError};
#[cfg(feature = "experimental_cursor")]
use alloc::boxed::Box;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::marker::PhantomData;
use core::ops::Bound;
use core::ops::Bound::{Excluded, Included, Unbounded};
use core::ops::Range;

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

impl<'a> Position<'a> {
    // The gap before the first key the bound admits.
    pub(super) fn from_lower_bound(bound: Bound<&'a [u8]>) -> Self {
        match bound {
            Included(key) => Self::Before(key),
            Excluded(key) => Self::After(key),
            Unbounded => Self::Start,
        }
    }

    // The gap after the last key the bound admits.
    pub(super) fn from_upper_bound(bound: Bound<&'a [u8]>) -> Self {
        match bound {
            Included(key) => Self::After(key),
            Excluded(key) => Self::Before(key),
            Unbounded => Self::End,
        }
    }
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
    let mut page = page;
    loop {
        let (child_index, child_page) = match page.memory()[0] {
            LEAF => {
                let (leaf_position, len) = {
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    (
                        lower_bound_entry::<K>(&accessor, position),
                        accessor.num_pairs(),
                    )
                };
                return Ok(Leaf {
                    page,
                    position: leaf_position,
                    len,
                });
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_index = child_to_visit::<K>(&accessor, position);
                (child_index, accessor.child_page(child_index).unwrap())
            }
            _ => unreachable!(),
        };
        if path.len() >= MAX_BTREE_DEPTH {
            return Err(StorageError::Corrupted(
                "Btree exceeded maximum depth".to_string(),
            ));
        }
        path.push(Branch::new(page, child_index));
        page = get_page(child_page)?;
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
    // scans append at the back, backward scans prepend at the front. A run
    // opens at a leaf left below the merge threshold and extends across every
    // later leaf with removals under the same parent, so the buffer is
    // bounded by fanout x page_size.
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

// The threshold at which a cursor's pending inserts are spliced into the
// tree. The splice cost is dominated by rebuilding the ancestor path, so what
// matters is the flush count; measurements flatten out around 1MiB.
#[cfg(feature = "experimental_cursor")]
const INSERT_FLUSH_BYTES: usize = 1024 * 1024;

// Which side of the gap a run's pending inserts fall on: `insert_before`
// buffers ascending arrivals behind the gap, `insert_after` descending
// arrivals in front of it. The two cannot share the ends-only buffer, so
// switching direction splices the pending run first.
#[cfg(feature = "experimental_cursor")]
#[derive(Copy, Clone, PartialEq)]
enum RunDirection {
    Ascending,
    Descending,
}

// Pending inserts at the cursor's gap, in ascending key order throughout. An
// ascending run's buffer holds the current leaf's entries before the gap and
// then the inserts, appended in arrival order; a descending run's holds only
// the inserts, prepended in arrival order, and the leaf's entries join at
// the flush. Either way the splice packs the buffer plus the rest of the
// leaf into full leaves in one parent update per flush instead of descending
// the tree per insert.
//
// While a run is open the tree is never mutated and the cursor position never
// moves, so the position's path stays valid until the splice.
#[cfg(feature = "experimental_cursor")]
struct InsertRun {
    direction: RunDirection,
    // Key of the entry after the gap when the run opened; None when the gap
    // is at the end of the tree.
    opening_next_key: Option<Vec<u8>>,
    // Greatest key at or before the gap: the last insert or, before any
    // insert, the entry preceding the gap; None at the start of the tree.
    // Inserts must sort strictly above it.
    previous_key: Option<Vec<u8>>,
    entries: OwnedEntryBuffer,
    // Buffered pairs that are new inserts; the rest were copied from the
    // current leaf.
    inserted_pairs: u64,
}

#[cfg(feature = "experimental_cursor")]
impl InsertRun {
    // The buffered entry nearest the gap on the entry-before side, if any:
    // a pending insert or an entry copied from the leaf's head. A descending
    // run buffers nothing on that side; the predecessor stays in the leaf.
    fn buffered_previous(&self) -> Option<(&[u8], &[u8])> {
        match self.direction {
            RunDirection::Ascending => self.entries.back(),
            RunDirection::Descending => None,
        }
    }

    // The buffered entry nearest the gap on the entry-after side, if any:
    // the most recent pending insert. An ascending run buffers nothing on
    // that side; the successor stays in the leaf.
    fn buffered_next(&self) -> Option<(&[u8], &[u8])> {
        match self.direction {
            RunDirection::Ascending => None,
            RunDirection::Descending => self.entries.front(),
        }
    }

    // Smallest key at or after the gap: the last `insert_after` or, before
    // any, the entry following the gap. Inserts must sort strictly below it.
    fn next_key(&self) -> Option<&[u8]> {
        self.buffered_next()
            .map(|(key, _)| key)
            .or(self.opening_next_key.as_deref())
    }

    // True unless `key` sorts strictly between the gap's live bounds.
    fn rejects<K: Key>(&self, key: &[u8]) -> bool {
        if let Some(previous) = &self.previous_key
            && K::compare(key, previous).is_le()
        {
            return true;
        }
        if let Some(next) = self.next_key()
            && K::compare(key, next).is_ge()
        {
            return true;
        }
        false
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

    // Guards over the entry that outlive this borrow of the cursor. They stay
    // valid as long as the tree is not mutated, which the compiler enforces
    // once the caller ties them to a borrow of the cursor's owner.
    #[cfg(feature = "experimental_cursor")]
    pub(super) fn to_guards<'g>(&self) -> (AccessGuard<'g, K>, AccessGuard<'g, V>) {
        (
            AccessGuard::with_page(self.page.clone(), self.key_range.clone()),
            AccessGuard::with_page(self.page.clone(), self.value_range.clone()),
        )
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

    // The entry after the gap, without moving the gap. The position may still
    // settle onto an adjacent leaf sharing the gap.
    #[cfg(feature = "experimental_cursor")]
    pub(super) fn peek_next(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(None);
        }

        let leaf = self.leaf.as_ref().expect("cursor must be positioned");
        Ok(Some(entry(leaf, leaf.position)))
    }

    // The entry before the gap, without moving the gap.
    #[cfg(feature = "experimental_cursor")]
    pub(super) fn peek_prev(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(None);
        }

        let leaf = self.leaf.as_ref().expect("cursor must be positioned");
        Ok(Some(entry(leaf, leaf.position - 1)))
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
    // Pending inserts at the gap, spliced when the buffer fills or the cursor
    // moves on. Never set by the removal-oriented cursor users. Boxed to keep
    // the state small for those users.
    #[cfg(feature = "experimental_cursor")]
    insert_run: Option<Box<InsertRun>>,
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
    allocated: &'b Arc<PageTracker>,
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
        allocated: &'b Arc<PageTracker>,
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
        allocated: &'b Arc<PageTracker>,
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
        #[cfg(feature = "experimental_cursor")]
        assert!(self.state.insert_run.is_none());
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
        // Moving across a leaf edge here would invalidate an open insert
        // run's captured position; its owner peeks the buffer instead.
        #[cfg(feature = "experimental_cursor")]
        assert!(self.state.insert_run.is_none());
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

    // Steps the gap past the entry after it without reading the entry, for
    // callers that already peeked it.
    pub(super) fn move_next(&mut self) -> Result<bool> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(false);
        }
        self.state
            .position
            .as_mut()
            .expect("cursor must be positioned")
            .move_once(Direction::Next);
        Ok(true)
    }

    // Steps the gap before the entry preceding it without reading the entry.
    #[cfg(feature = "experimental_cursor")]
    pub(super) fn move_prev(&mut self) -> Result<bool> {
        if !self.ensure_has_entry(Direction::Previous)? {
            return Ok(false);
        }
        self.state
            .position
            .as_mut()
            .expect("cursor must be positioned")
            .move_once(Direction::Previous);
        Ok(true)
    }

    #[cfg(feature = "experimental_cursor")]
    pub(super) fn next(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.move_next()? {
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

    #[cfg(feature = "experimental_cursor")]
    pub(super) fn prev(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.move_prev()? {
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

    /// Removes the entry after the gap, returning it with a copy of its key.
    /// The mutation consumes the position: once the returned guards have
    /// dropped, seeking to `Position::Before` the key restores the gap
    /// between the removed entry's old neighbors.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    #[cfg(feature = "experimental_cursor")]
    #[allow(clippy::type_complexity)]
    pub(super) fn remove_next_taking_key(
        &mut self,
    ) -> Result<Option<(Vec<u8>, AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_taking_key(Direction::Next)
    }

    /// Removes the entry before the gap; see
    /// [`remove_next_taking_key`](Self::remove_next_taking_key).
    #[cfg(feature = "experimental_cursor")]
    #[allow(clippy::type_complexity)]
    pub(super) fn remove_prev_taking_key(
        &mut self,
    ) -> Result<Option<(Vec<u8>, AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_taking_key(Direction::Previous)
    }

    #[cfg(feature = "experimental_cursor")]
    #[allow(clippy::type_complexity)]
    fn remove_taking_key(
        &mut self,
        direction: Direction,
    ) -> Result<Option<(Vec<u8>, AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.state.removed_indexes.is_empty());
        if !self.ensure_has_entry(direction)? {
            return Ok(None);
        }
        let position = self
            .state
            .position
            .take()
            .expect("cursor must be positioned");
        let index = position.entry_index(direction);
        let key = key_data::<K, V>(&position.leaf, index);
        let entry = self.remove_leaf_entry(position.leaf.page, position.path, index)?;
        Ok(entry.map(|(key_guard, value_guard)| (key, key_guard, value_guard)))
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
        #[cfg(feature = "experimental_cursor")]
        {
            self.state.insert_run = None;
        }
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
    //
    // Run policy: only an underfilling leaf opens a run, but once one is open
    // any leaf that is being rewritten anyway extends it, so a stretch of
    // moderate removals packs into full replacements instead of one
    // half-empty leaf per original. A leaf with no removals ends the run and
    // is left in place, so rewrites never spread past the region the scan is
    // already dirtying. One whose survivors exceed a page (a large value with
    // small neighbors) rides the splice but also ends the run, keeping the
    // buffer bounded by fanout x page_size plus one such leaf.
    fn close_current_leaf(&mut self, direction: Direction) -> Result<LeafCloseOutcome> {
        assert!(self.state.position.is_some(), "cursor must be positioned");
        let run_open = self.state.leaf_run_rewrite.is_some();

        if self.state.removed_indexes.is_empty() {
            if !run_open {
                return Ok(LeafCloseOutcome::Unchanged);
            }
            // A leaf with no removals is not one of the run's replaced
            // children: the splice below leaves it in place, so its entries
            // must not join the buffer or they would be duplicated. If the
            // packed run falls below the merge threshold, the splice's
            // neighbor absorption consumes this leaf's page and extends the
            // replaced range over it instead.
        } else {
            // The survivor accounting must precede `take_removals_ascending`,
            // which drains the pending batch it reads. A merely-iterated
            // sparse leaf never gets here, so it cannot open a run.
            let (underfilling, packs, has_parent) = {
                let position = self.state.position.as_ref().unwrap();
                let accessor = LeafAccessor::new(
                    position.leaf.page.memory(),
                    K::fixed_width(),
                    V::fixed_width(),
                );
                let (retained_pairs, retained_bytes) =
                    retained_after_removals(&accessor, &self.state.removed_indexes);
                let page_size = self.page_allocator.get_page_size();
                // Matches `MutateHelper::plan_leaf_delete`'s Merge disposition.
                let underfilling = retained_pairs == 0
                    || leaf_below_merge_threshold(
                        retained_pairs,
                        retained_bytes,
                        K::fixed_width(),
                        V::fixed_width(),
                        page_size,
                    );
                // An underfilling leaf's survivors trivially share a page.
                let packs = underfilling
                    || leaf_fits_one_page(
                        retained_pairs,
                        retained_bytes,
                        K::fixed_width(),
                        V::fixed_width(),
                        page_size,
                    );
                (underfilling, packs, !position.path.is_empty())
            };
            if !(underfilling || run_open) || !has_parent {
                let resume_key = self.flush_removed_entries(direction)?;
                return Ok(LeafCloseOutcome::Flushed { resume_key });
            }
            let keeps_run_open = packs && self.run_parent_has_more_children(direction);
            let removed_indexes = self.take_removals_ascending();
            self.append_leaf_to_run(direction, &removed_indexes);
            if keeps_run_open {
                return Ok(LeafCloseOutcome::AbsorbedIntoRun);
            }
            // Either the run consumed its parent's last child in the scan
            // direction or this leaf cannot pack; fall through to the splice.
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
        let mut removed_indexes = core::mem::take(&mut self.state.removed_indexes);
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

    // All tree mutations flow through here. Mutating invalidates saved paths,
    // so any open run must have been spliced first; a hard assert because a
    // violation would later splice through a stale path.
    fn mutate_helper<'c>(&'c mut self) -> MutateHelper<'a, 'c, K, V> {
        assert!(self.state.leaf_run_rewrite.is_none());
        #[cfg(feature = "experimental_cursor")]
        assert!(self.state.insert_run.is_none());
        MutateHelper::new(
            &mut *self.root,
            self.page_allocator,
            &mut *self.freed,
            self.allocated,
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

#[cfg(feature = "experimental_cursor")]
impl<K: Key + 'static, V: Value + 'static> CursorMut<'_, '_, K, V> {
    /// Buffers `key`/`value` for insertion into the gap, leaving the gap
    /// after the new entry. Returns false, leaving the tree and any pending
    /// inserts unchanged, unless `key` sorts strictly between the entries
    /// adjacent to the gap.
    pub(super) fn insert_before(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.check_not_poisoned()?;
        assert!(self.state.removed_indexes.is_empty());
        assert!(self.state.leaf_run_rewrite.is_none());
        self.ensure_insert_run(RunDirection::Ascending)?;
        let run = self.state.insert_run.as_mut().unwrap();
        if run.rejects::<K>(key) {
            // A run opened just for a rejected key holds nothing; drop it
            // again, so rejection never leaves an open run behind to trip
            // operations that require no pending inserts.
            if run.inserted_pairs == 0 {
                self.state.insert_run = None;
            }
            return Ok(false);
        }
        run.entries.push_back(key, value);
        run.previous_key = Some(key.to_vec());
        run.inserted_pairs += 1;
        if run.entries.total_bytes() >= INSERT_FLUSH_BYTES {
            self.flush_insert_run(true)?;
        }
        Ok(true)
    }

    /// Buffers `key`/`value` for insertion into the gap, leaving the gap
    /// before the new entry. Returns false, leaving the tree and any pending
    /// inserts unchanged, unless `key` sorts strictly between the entries
    /// adjacent to the gap.
    pub(super) fn insert_after(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.check_not_poisoned()?;
        assert!(self.state.removed_indexes.is_empty());
        assert!(self.state.leaf_run_rewrite.is_none());
        self.ensure_insert_run(RunDirection::Descending)?;
        let run = self.state.insert_run.as_mut().unwrap();
        if run.rejects::<K>(key) {
            // A run opened just for a rejected key holds nothing; drop it
            // again, so rejection never leaves an open run behind to trip
            // operations that require no pending inserts.
            if run.inserted_pairs == 0 {
                self.state.insert_run = None;
            }
            return Ok(false);
        }
        run.entries.push_front(key, value);
        run.inserted_pairs += 1;
        if run.entries.total_bytes() >= INSERT_FLUSH_BYTES {
            self.flush_insert_run(true)?;
        }
        Ok(true)
    }

    // Opens a run in `direction` if none is open, splicing a pending run on
    // the gap's other side first: the two directions cannot share the
    // ends-only buffer.
    fn ensure_insert_run(&mut self, direction: RunDirection) -> Result {
        if let Some(run) = &self.state.insert_run {
            if run.direction == direction {
                return Ok(());
            }
            self.flush_insert_run(true)?;
        }
        if self.state.insert_run.is_none() {
            self.open_insert_run(direction)?;
        }
        Ok(())
    }

    // Captures the gap's neighbor keys and the current leaf's entries before
    // the gap. The peeks first settle a gap at the edge of two leaves on the
    // later leaf and then back on the earlier one, so afterward the gap's
    // remaining predecessors all sit in the current leaf before `position`,
    // and its successors are `position..` of the current leaf plus every
    // later leaf.
    fn open_insert_run(&mut self, direction: RunDirection) -> Result {
        assert!(self.state.insert_run.is_none());
        let opening_next_key = self.peek_next()?.map(|entry| entry.key_bytes().to_vec());
        let previous_key = self.peek_prev()?.map(|entry| entry.key_bytes().to_vec());
        let mut entries = OwnedEntryBuffer::default();
        // A descending run's buffer only ever grows at the front, so the
        // leaf's entries before the gap join at the flush instead of here.
        if direction == RunDirection::Ascending
            && let Some(position) = &self.state.position
        {
            let accessor = LeafAccessor::new(
                position.leaf.page.memory(),
                K::fixed_width(),
                V::fixed_width(),
            );
            entries.extend_from_leaf_range(&accessor, 0..position.leaf.position, true);
        }
        self.state.insert_run = Some(Box::new(InsertRun {
            direction,
            opening_next_key,
            previous_key,
            entries,
            inserted_pairs: 0,
        }));
        Ok(())
    }

    /// Splices the pending inserts into the tree. With `reseek` the cursor is
    /// repositioned at the gap after the last insert; without it the position
    /// is consumed and the cursor must not be used again.
    ///
    /// On error the cursor is poisoned: inserts already reported to the
    /// caller were lost, so the transaction must not commit.
    pub(super) fn flush_insert_run(&mut self, reseek: bool) -> Result {
        self.check_not_poisoned()?;
        let Some(run) = self.state.insert_run.take() else {
            return Ok(());
        };
        if run.inserted_pairs == 0 {
            // No pending inserts: the tree is untouched and the position
            // remains valid.
            return Ok(());
        }
        // The gap sits between the pending inserts on its two sides; resume
        // on whichever side has one, at the same gap.
        let resume_before = run.buffered_next().map(|(key, _)| key.to_vec());
        let resume_after = run.previous_key;
        let descending = run.direction == RunDirection::Descending;
        let mut entries = run.entries;
        let replaced = if let Some(position) = self.state.position.take() {
            let accessor = LeafAccessor::new(
                position.leaf.page.memory(),
                K::fixed_width(),
                V::fixed_width(),
            );
            // A descending run's buffer holds only the pending inserts; the
            // leaf's entries before the gap join at the front here. The rest
            // of the leaf follows every pending insert either way.
            if descending {
                entries.extend_from_leaf_range(&accessor, 0..position.leaf.position, false);
            }
            entries.extend_from_leaf_range(
                &accessor,
                position.leaf.position..position.leaf.len,
                true,
            );
            let CursorPosition { path, leaf } = position;
            let replaced_leaf = leaf.page.get_page_number();
            // The leaf is one of the pages the splice frees, so its page
            // reference must be released first.
            drop(leaf);
            Some((
                path.into_iter().map(Branch::into_parts).collect(),
                replaced_leaf,
            ))
        } else {
            None
        };
        let result = self
            .mutate_helper()
            .splice_insert_run(replaced, &entries, run.inserted_pairs);
        if result.is_err() {
            // The buffered inserts were consumed and can no longer be applied.
            self.poison();
        }
        result?;
        if reseek {
            if let Some(key) = &resume_before {
                self.seek_to(Position::Before(key))?;
            } else {
                let key = resume_after.expect("a run with inserts bounds its gap on some side");
                self.seek_to(Position::After(&key))?;
            }
        }
        Ok(())
    }
}

// The tree borrows shared by every owner of detached cursor state: the root,
// the plumbing needed to build `CursorMut`s over it, and the pages their
// mutations free, drained after every operation so the master list's lock is
// never held while control returns to the caller.
pub(super) struct CursorTree<'a, K: Key + 'static, V: Value + 'static> {
    root: &'a mut Option<BtreeHeader>,
    page_allocator: PageAllocator,
    allocated: Arc<PageTracker>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    freed: Vec<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> CursorTree<'a, K, V> {
    fn new(
        root: &'a mut Option<BtreeHeader>,
        page_allocator: PageAllocator,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<PageTracker>,
    ) -> Self {
        Self {
            root,
            page_allocator,
            allocated,
            master_free_list,
            freed: vec![],
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
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

    fn drain_freed(&mut self) {
        if self.freed.is_empty() {
            return;
        }
        let mut master_free_list = self.master_free_list.lock().unwrap();
        for page in self.freed.drain(..) {
            if !self
                .page_allocator
                .free_if_uncommitted(page, &self.allocated)
            {
                master_free_list.push(page);
            }
        }
    }
}

// The tree-level cursor behind the public read-only `Cursor`. The caller
// positions it with its seek methods before use; over an empty tree there is
// no position and every operation reports no entry.
#[cfg(feature = "experimental-api-5")]
pub(crate) struct BtreeCursor<K: Key + 'static, V: Value + 'static> {
    inner: Option<Cursor<K, V>>,
}

#[cfg(feature = "experimental-api-5")]
impl<K: Key + 'static, V: Value + 'static> BtreeCursor<K, V> {
    pub(crate) fn new(root: Option<BtreeHeader>, resolver: PageResolver, hint: PageHint) -> Self {
        Self {
            inner: root.map(|header| Cursor::new(header.root, resolver, hint)),
        }
    }

    /// Positions the cursor at the gap before the first key `bound` admits.
    pub(crate) fn seek_lower_bound(&mut self, bound: Bound<&[u8]>) -> Result {
        if let Some(cursor) = &mut self.inner {
            cursor.seek_to(Position::from_lower_bound(bound))?;
        }
        Ok(())
    }

    /// Positions the cursor at the gap after the last key `bound` admits.
    pub(crate) fn seek_upper_bound(&mut self, bound: Bound<&[u8]>) -> Result {
        if let Some(cursor) = &mut self.inner {
            cursor.seek_to(Position::from_upper_bound(bound))?;
        }
        Ok(())
    }
}

#[cfg(feature = "experimental_cursor")]
impl<K: Key + 'static, V: Value + 'static> BtreeCursor<K, V> {
    /// The entry after the gap, without moving the gap.
    #[allow(clippy::type_complexity)]
    pub(crate) fn peek_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let Some(cursor) = &mut self.inner else {
            return Ok(None);
        };
        Ok(cursor.peek_next()?.map(entry_guards))
    }

    /// The entry before the gap, without moving the gap.
    #[allow(clippy::type_complexity)]
    pub(crate) fn peek_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let Some(cursor) = &mut self.inner else {
            return Ok(None);
        };
        Ok(cursor.peek_prev()?.map(entry_guards))
    }

    /// Moves the gap past the entry after it, returning that entry.
    #[allow(clippy::type_complexity)]
    pub(crate) fn next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let Some(cursor) = &mut self.inner else {
            return Ok(None);
        };
        Ok(cursor.next()?.map(entry_guards))
    }

    /// Moves the gap before the entry preceding it, returning that entry.
    #[allow(clippy::type_complexity)]
    pub(crate) fn prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let Some(cursor) = &mut self.inner else {
            return Ok(None);
        };
        Ok(cursor.prev()?.map(entry_guards))
    }
}

#[cfg(feature = "experimental_cursor")]
fn entry_guards<K: Key + 'static, V: Value + 'static>(
    entry: EntryGuard<K, V>,
) -> (AccessGuard<'static, K>, AccessGuard<'static, V>) {
    let (page, key_range, value_range) = entry.into_raw();
    (
        AccessGuard::with_page(page.clone(), key_range),
        AccessGuard::with_page(page, value_range),
    )
}

// The tree-level cursor behind the public `CursorMut`: one gap cursor that
// owns its state across calls, in the style of `RangeMut` but with a single
// end and buffered insertion instead of removal batching.
#[cfg(feature = "experimental_cursor")]
pub(crate) struct BtreeCursorMut<'a, K: Key + 'static, V: Value + 'static> {
    tree: CursorTree<'a, K, V>,
    state: CursorState,
    // Key of the last removed entry, whose removal consumed the position.
    // The next operation reseeks to the gap the key names; the reseek must
    // wait, since an in-place removal is applied only when the returned
    // value guard drops.
    pending_reseek: Option<Vec<u8>>,
}

#[cfg(feature = "experimental_cursor")]
impl<'a, K: Key + 'static, V: Value + 'static> BtreeCursorMut<'a, K, V> {
    pub(crate) fn new(
        root: &'a mut Option<BtreeHeader>,
        page_allocator: PageAllocator,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<PageTracker>,
    ) -> Self {
        Self {
            tree: CursorTree::new(root, page_allocator, master_free_list, allocated),
            state: CursorState::default(),
            pending_reseek: None,
        }
    }

    /// Positions the cursor at the gap before the first key `bound` admits.
    pub(crate) fn seek_lower_bound(&mut self, bound: Bound<&[u8]>) -> Result {
        self.pending_reseek = None;
        self.with_cursor(|cursor| cursor.seek_to(Position::from_lower_bound(bound)))
    }

    /// Positions the cursor at the gap after the last key `bound` admits.
    pub(crate) fn seek_upper_bound(&mut self, bound: Bound<&[u8]>) -> Result {
        self.pending_reseek = None;
        self.with_cursor(|cursor| cursor.seek_to(Position::from_upper_bound(bound)))
    }

    // Restores the gap a removal consumed: the removed key, now absent from
    // the tree, names the gap between its old neighbors. Every operation
    // except the seeks, which position anew, settles this first.
    fn settle_reseek(&mut self) -> Result {
        let Some(key) = self.pending_reseek.take() else {
            return Ok(());
        };
        self.with_cursor(|cursor| cursor.seek_to(Position::Before(&key)))
    }

    /// The entry after the gap, including while inserts are pending: the
    /// most recent `insert_after` is returned without splicing.
    #[allow(clippy::type_complexity)]
    pub(crate) fn peek_next(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.settle_reseek()?;
        if let Some(run) = &self.state.insert_run {
            if let Some((key, value)) = run.buffered_next() {
                return Ok(Some((
                    AccessGuard::with_owned_value(key.to_vec()),
                    AccessGuard::with_owned_value(value.to_vec()),
                )));
            }
            // The position must not move while a run is open, so the entry
            // after the gap (the run's opening next key, unchanged since the
            // tree is not mutated during a run) is re-read with a detached
            // read-only cursor.
            let Some(next_key) = &run.opening_next_key else {
                return Ok(None);
            };
            let header = self.tree.root.expect("a captured next key implies a root");
            let mut cursor: Cursor<K, V> = Cursor::new(
                header.root,
                self.tree.page_allocator.resolver(),
                PageHint::None,
            );
            cursor.seek_to(Position::Before(next_key))?;
            let entry = cursor
                .next()?
                .expect("the captured next key is in the tree");
            return Ok(Some(entry_guards(entry)));
        }
        self.with_cursor(|cursor| Ok(cursor.peek_next()?.map(|entry| entry.to_guards())))
    }

    /// The entry before the gap, including while inserts are pending: the
    /// most recent insert is returned without splicing.
    ///
    /// While a run is open, the gap's predecessors in the current leaf are
    /// either buffered (ascending runs) or untouched at the position
    /// (descending runs); the normalizing peeks settled the position on the
    /// earlier of two leaves sharing the gap, so finding none on either path
    /// means the gap is at the start of the tree.
    #[allow(clippy::type_complexity)]
    pub(crate) fn peek_prev(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.settle_reseek()?;
        if let Some(run) = &self.state.insert_run {
            if let Some((key, value)) = run.buffered_previous() {
                return Ok(Some((
                    AccessGuard::with_owned_value(key.to_vec()),
                    AccessGuard::with_owned_value(value.to_vec()),
                )));
            }
            // A descending run's buffer holds only entries after the gap;
            // the predecessor still sits in the current leaf, untouched
            // while the run is open. No such entry means the gap is at the
            // start of the tree.
            if run.direction == RunDirection::Descending
                && let Some(position) = &self.state.position
                && position.leaf.position > 0
            {
                let entry = entry_ref::<K, V>(&position.leaf, position.leaf.position - 1);
                return Ok(Some(entry.to_guards()));
            }
            return Ok(None);
        }
        self.with_cursor(|cursor| Ok(cursor.peek_prev()?.map(|entry| entry.to_guards())))
    }

    /// Moves the gap past the entry after it, returning that entry. Any
    /// pending inserts are spliced first: moving the cursor closes the run.
    #[allow(clippy::type_complexity)]
    pub(crate) fn next(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.settle_reseek()?;
        self.with_cursor(|cursor| {
            cursor.flush_insert_run(true)?;
            Ok(cursor.next()?.map(|entry| entry.to_guards()))
        })
    }

    /// Moves the gap before the entry preceding it, returning that entry.
    /// Any pending inserts are spliced first: moving the cursor closes the
    /// run.
    #[allow(clippy::type_complexity)]
    pub(crate) fn prev(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.settle_reseek()?;
        self.with_cursor(|cursor| {
            cursor.flush_insert_run(true)?;
            Ok(cursor.prev()?.map(|entry| entry.to_guards()))
        })
    }

    /// See [`CursorMut::insert_before`].
    pub(crate) fn insert_before(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.settle_reseek()?;
        self.with_cursor(|cursor| cursor.insert_before(key, value))
    }

    /// See [`CursorMut::insert_after`].
    pub(crate) fn insert_after(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.settle_reseek()?;
        self.with_cursor(|cursor| cursor.insert_after(key, value))
    }

    /// Removes the entry after the gap, returning it. The gap does not move:
    /// it ends up between the removed entry's old neighbors. Any pending
    /// inserts are spliced first: removing through the cursor closes the run.
    #[allow(clippy::type_complexity)]
    pub(crate) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.settle_reseek()?;
        let removed = self.with_cursor(|cursor| {
            cursor.flush_insert_run(true)?;
            cursor.remove_next_taking_key()
        })?;
        Ok(removed.map(|(key, key_guard, value_guard)| {
            // The mutation consumed the position, and an in-place removal is
            // applied only when the value guard drops; the reseek waits for
            // the next operation, by which time the guards are gone.
            self.pending_reseek = Some(key);
            (key_guard, value_guard)
        }))
    }

    /// Removes the entry before the gap, returning it; the counterpart of
    /// [`remove_next`](Self::remove_next).
    #[allow(clippy::type_complexity)]
    pub(crate) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.settle_reseek()?;
        let removed = self.with_cursor(|cursor| {
            cursor.flush_insert_run(true)?;
            cursor.remove_prev_taking_key()
        })?;
        Ok(removed.map(|(key, key_guard, value_guard)| {
            self.pending_reseek = Some(key);
            (key_guard, value_guard)
        }))
    }

    /// Splices any pending inserts into the tree, keeping the cursor at its
    /// gap.
    pub(crate) fn apply_pending_inserts(&mut self) -> Result {
        self.with_cursor(|cursor| cursor.flush_insert_run(true))
    }

    /// Splices any pending inserts into the tree, consuming the position.
    pub(crate) fn finish(&mut self) -> Result {
        self.with_cursor(|cursor| cursor.flush_insert_run(false))
    }

    /// True if an error interrupted inserts that were already reported to the
    /// caller: they may be missing from the tree, so the transaction must not
    /// commit.
    pub(crate) fn poisoned(&self) -> bool {
        self.state.poisoned
    }

    fn with_cursor<R>(
        &mut self,
        operation: impl FnOnce(&mut CursorMut<'a, '_, K, V>) -> Result<R>,
    ) -> Result<R> {
        let mut cursor = self.tree.cursor(core::mem::take(&mut self.state));
        let result = operation(&mut cursor);
        self.state = cursor.into_state();
        self.tree.drain_freed();
        result
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
    tree: CursorTree<'a, K, V>,
    front: EndState,
    back: EndState,
    // Which end, if any, is known to be live and positioned at an in-range
    // entry. Cleared whenever the gap moves or the tree is mutated.
    settled: Option<Direction>,
    // Set when an error interrupted removals that were already yielded to the
    // caller: they may remain in the tree, so the transaction must not
    // commit. Every range operation re-raises instead of touching the tree.
    poisoned: bool,
}

impl<'a, K: Key + 'static, V: Value + 'static> RangeMut<'a, K, V> {
    pub(super) fn new(
        root: &'a mut Option<BtreeHeader>,
        lower_bound: Bound<Vec<u8>>,
        upper_bound: Bound<Vec<u8>>,
        page_allocator: PageAllocator,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<PageTracker>,
    ) -> Self {
        Self {
            tree: CursorTree::new(root, page_allocator, master_free_list, allocated),
            front: EndState::Parked(lower_bound),
            back: EndState::Parked(upper_bound),
            settled: None,
            poisoned: false,
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
                core::mem::replace(self.end_mut(direction), EndState::Parked(Unbounded))
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
        let bound = bound.as_ref().map(Vec::as_slice);
        let target = match direction {
            Direction::Next => Position::from_lower_bound(bound),
            Direction::Previous => Position::from_upper_bound(bound),
        };
        let mut cursor = self.tree.cursor(CursorState::default());
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
                removed_indexes: core::mem::take(&mut state.removed_indexes),
            })
        };
        let result = if state.leaf_run_rewrite.is_some() {
            self.with_live_cursor(direction, |cursor| cursor.splice_open_run())
        } else {
            Ok(())
        };
        *self.end_mut(direction) = parked;
        self.tree.drain_freed();
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
                &mut *self.tree.root,
                &self.tree.page_allocator,
                &mut self.tree.freed,
                &self.tree.allocated,
            );
            // In-place deletion must be disabled: the other end's pending
            // snapshot and any deferred-removal guards handed to the caller
            // may share the buffer of the live leaf containing this key.
            let result = helper.delete_key(key, false);
            self.tree.drain_freed();
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
        let EndState::Live(state) = core::mem::replace(end, EndState::Parked(Unbounded)) else {
            unreachable!("end must be live");
        };
        let mut cursor = self.tree.cursor(state);
        let result = operation(&mut cursor);
        let state = cursor.into_state();
        if state.poisoned {
            self.poisoned = true;
        }
        *self.end_mut(direction) = EndState::Live(state);
        self.tree.drain_freed();
        result
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
    use crate::tree_store::{AllocationPolicy, InMemoryBackend, PAGE_SIZE, TransactionalMemory};

    fn test_page_allocator() -> PageAllocator {
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
        PageAllocator::new(Arc::new(mem), AllocationPolicy::Default)
    }

    // The returned tracker records the built page: mutations through a cursor
    // must share it, so the pages they free are found tracked.
    fn leaf_root_with_entries(entries: &[u64]) -> (PageAllocator, PageNumber, Arc<PageTracker>) {
        let page_allocator = test_page_allocator();
        let allocated_pages = Arc::new(PageTracker::new_tracking());
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

        (page_allocator, root, allocated_pages)
    }

    fn cursor_with_entries(entries: &[u64]) -> Cursor<u64, u64> {
        let (page_allocator, root, _) = leaf_root_with_entries(entries);
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
        let (page_allocator, root_page, allocated) = leaf_root_with_entries(&[1, 2, 3]);
        let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
        let mut freed = vec![];
        let mut cursor: CursorMut<'_, '_, u64, u64> =
            CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);

        cursor.seek_to(Position::Start).unwrap();
        for _ in 0..3 {
            assert!(cursor.move_next().unwrap());
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

    #[cfg(feature = "experimental_cursor")]
    mod insert_tests {
        use super::*;
        use crate::tree_store::RawBtree;
        use crate::tree_store::btree::UntypedBtreeMut;

        fn insert(cursor: &mut CursorMut<'_, '_, u64, u64>, key: u64, value: u64) -> bool {
            cursor
                .insert_before(u64::as_bytes(&key).as_ref(), u64::as_bytes(&value).as_ref())
                .unwrap()
        }

        fn insert_after(cursor: &mut CursorMut<'_, '_, u64, u64>, key: u64, value: u64) -> bool {
            cursor
                .insert_after(u64::as_bytes(&key).as_ref(), u64::as_bytes(&value).as_ref())
                .unwrap()
        }

        fn scan(root: Option<BtreeHeader>, page_allocator: &PageAllocator) -> Vec<(u64, u64)> {
            let Some(header) = root else {
                return vec![];
            };
            let mut cursor =
                Cursor::<u64, u64>::new(header.root, page_allocator.resolver(), PageHint::None);
            cursor.seek_to(Position::Start).unwrap();
            let mut entries = vec![];
            while let Some(entry) = cursor.next().unwrap() {
                entries.push((entry.key(), entry.value()));
            }
            entries
        }

        // Full validation of a spliced tree: contents and order via a scan,
        // stored length, per-key branch routing (which depends on the
        // separators the splice wrote), and checksum-tree consistency.
        fn assert_tree(
            root: Option<BtreeHeader>,
            page_allocator: &PageAllocator,
            expected: &[(u64, u64)],
        ) {
            assert_eq!(scan(root, page_allocator), expected);
            assert_eq!(
                root.map_or(0, |header| header.length),
                expected.len() as u64
            );
            for (key, value) in expected {
                let mut cursor = Cursor::<u64, u64>::new(
                    root.unwrap().root,
                    page_allocator.resolver(),
                    PageHint::None,
                );
                cursor
                    .seek_to(Position::Before(u64::as_bytes(key).as_ref()))
                    .unwrap();
                let entry = cursor.next().unwrap().expect("key must route to its leaf");
                assert_eq!(entry.key(), *key);
                assert_eq!(entry.value(), *value);
            }
            let mut untyped = UntypedBtreeMut::new(
                root,
                page_allocator.clone(),
                Arc::new(Mutex::new(vec![])),
                u64::fixed_width(),
                u64::fixed_width(),
            );
            let finalized = untyped.finalize_dirty_checksums().unwrap();
            let raw = RawBtree::new(
                finalized,
                u64::fixed_width(),
                u64::fixed_width(),
                page_allocator.resolver(),
                PageHint::None,
            );
            assert!(raw.verify_checksum().unwrap());
        }

        #[test]
        fn insert_run_builds_tree_from_empty() {
            for flush_every in [1, 3, 64] {
                let page_allocator = test_page_allocator();
                let mut root = None;
                let mut freed = vec![];
                let allocated = Arc::new(PageTracker::new_tracking());
                let mut cursor: CursorMut<'_, '_, u64, u64> =
                    CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
                cursor.seek_to(Position::End).unwrap();
                for key in 0..2000 {
                    assert!(insert(&mut cursor, key, key * 3));
                    if key % flush_every == 0 {
                        cursor.flush_insert_run(true).unwrap();
                    }
                }
                cursor.flush_insert_run(true).unwrap();
                drop(cursor);

                let expected: Vec<_> = (0..2000).map(|key| (key, key * 3)).collect();
                assert_tree(root, &page_allocator, &expected);
            }
        }

        #[test]
        fn insert_run_grows_multiple_levels() {
            let page_allocator = test_page_allocator();
            let mut root = None;
            let mut freed = vec![];
            let allocated = Arc::new(PageTracker::new_tracking());
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor.seek_to(Position::End).unwrap();
            for key in 0..60_000 {
                assert!(insert(&mut cursor, key, key));
                if key % 1000 == 999 {
                    cursor.flush_insert_run(true).unwrap();
                }
            }
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);

            let stats = crate::tree_store::btree::btree_stats(
                root.map(|header| header.root),
                &page_allocator.resolver(),
                u64::fixed_width(),
                u64::fixed_width(),
                PageHint::None,
            )
            .unwrap();
            assert!(stats.tree_height >= 3, "height {}", stats.tree_height);

            let expected: Vec<_> = (0..60_000).map(|key| (key, key)).collect();
            assert_tree(root, &page_allocator, &expected);
        }

        #[test]
        fn insert_run_into_leaf_middle() {
            let existing: Vec<u64> = (0..100).map(|i| i * 10).collect();
            let (page_allocator, root_page, allocated) = leaf_root_with_entries(&existing);
            let mut root = Some(BtreeHeader::new(root_page, DEFERRED, existing.len() as u64));
            let mut freed = vec![];
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor
                .seek_to(Position::Before(u64::as_bytes(&500).as_ref()))
                .unwrap();
            for key in 491..500 {
                assert!(insert(&mut cursor, key, key));
            }
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);

            let mut expected: Vec<_> = existing.iter().map(|&key| (key, key)).collect();
            expected.extend((491..500).map(|key| (key, key)));
            expected.sort_unstable();
            assert_tree(root, &page_allocator, &expected);
        }

        // Splicing many entries into a single-leaf root exercises replacing
        // the root leaf itself and growing new levels above the replacements.
        #[test]
        fn insert_run_splits_root_leaf() {
            let (page_allocator, root_page, allocated) = leaf_root_with_entries(&[0, 1_000_000]);
            let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 2));
            let mut freed = vec![];
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor
                .seek_to(Position::After(u64::as_bytes(&0).as_ref()))
                .unwrap();
            for key in 1..=10_000 {
                assert!(insert(&mut cursor, key, key));
            }
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);

            let mut expected = vec![(0, 0), (1_000_000, 1_000_000)];
            expected.extend((1..=10_000).map(|key| (key, key)));
            expected.sort_unstable();
            assert_tree(root, &page_allocator, &expected);
        }

        // A splice under a non-rightmost branch of a height-3 tree: the
        // rebuilt branch is not its parent's last child, so its subtree's
        // unchanged greatest key must be recovered from the parent's
        // separator rather than propagated as unknown.
        #[test]
        fn insert_run_into_middle_of_tall_tree() {
            let page_allocator = test_page_allocator();
            let mut root = None;
            let mut freed = vec![];
            let allocated = Arc::new(PageTracker::new_tracking());
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor.seek_to(Position::End).unwrap();
            // Even keys, leaving gaps to insert into; enough for three levels
            for key in 0..60_000 {
                assert!(insert(&mut cursor, key * 2, key * 2));
                if key % 1000 == 999 {
                    cursor.flush_insert_run(true).unwrap();
                }
            }
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);
            let stats = crate::tree_store::btree::btree_stats(
                root.map(|header| header.root),
                &page_allocator.resolver(),
                u64::fixed_width(),
                u64::fixed_width(),
                PageHint::None,
            )
            .unwrap();
            assert!(stats.tree_height >= 3, "height {}", stats.tree_height);

            // Gaps chosen to land under leaves in the interior of the tree
            let mut expected: Vec<(u64, u64)> = (0..60_000).map(|key| (key * 2, key * 2)).collect();
            for target in [1001u64, 30_001, 60_001, 90_001, 119_001] {
                let mut cursor: CursorMut<'_, '_, u64, u64> =
                    CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
                cursor
                    .seek_to(Position::Before(u64::as_bytes(&target).as_ref()))
                    .unwrap();
                assert!(insert(&mut cursor, target, target));
                cursor.flush_insert_run(false).unwrap();
                drop(cursor);
                expected.push((target, target));
            }
            expected.sort_unstable();
            assert_tree(root, &page_allocator, &expected);
        }

        // Once a splice's replacement collapses to a single node, the
        // ancestors take the deletion path's child-pointer swap, writing
        // uncommitted pages in place instead of rebuilding the spine. The
        // second splice below lands in a leaf rebalanced by the first, so it
        // replaces one leaf with one leaf and must leave the root page --
        // and everything else above the leaf's parent -- untouched.
        #[test]
        fn insert_run_swaps_ancestors_in_place() {
            let page_allocator = test_page_allocator();
            let mut root = None;
            let mut freed = vec![];
            let allocated = Arc::new(PageTracker::new_tracking());
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor.seek_to(Position::End).unwrap();
            // Even keys, leaving gaps to insert into; enough for three levels
            for key in 0..60_000 {
                assert!(insert(&mut cursor, key * 2, key * 2));
                if key % 1000 == 999 {
                    cursor.flush_insert_run(true).unwrap();
                }
            }
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);
            let stats = crate::tree_store::btree::btree_stats(
                root.map(|header| header.root),
                &page_allocator.resolver(),
                u64::fixed_width(),
                u64::fixed_width(),
                PageHint::None,
            )
            .unwrap();
            assert!(stats.tree_height >= 3, "height {}", stats.tree_height);

            // The first splice splits a packed-full leaf, rebalancing the
            // pieces; root stability is not guaranteed here, since the
            // parent may split as well.
            for target in [30_001u64, 30_003] {
                let root_before = root.map(|header| header.root);
                let mut cursor: CursorMut<'_, '_, u64, u64> =
                    CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
                cursor
                    .seek_to(Position::Before(u64::as_bytes(&target).as_ref()))
                    .unwrap();
                assert!(insert(&mut cursor, target, target));
                cursor.flush_insert_run(false).unwrap();
                drop(cursor);
                if target == 30_003 {
                    assert_eq!(root.map(|header| header.root), root_before);
                }
            }

            let mut expected: Vec<(u64, u64)> = (0..60_000).map(|key| (key * 2, key * 2)).collect();
            expected.push((30_001, 30_001));
            expected.push((30_003, 30_003));
            expected.sort_unstable();
            assert_tree(root, &page_allocator, &expected);
        }

        #[test]
        fn insert_run_rejects_unordered_keys() {
            let (page_allocator, root_page, allocated) = leaf_root_with_entries(&[10, 20, 30]);
            let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
            let mut freed = vec![];
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor
                .seek_to(Position::After(u64::as_bytes(&20).as_ref()))
                .unwrap();
            // Equal to the previous entry, below it, equal to the next entry,
            // and above it are all rejected.
            assert!(!insert(&mut cursor, 20, 20));
            assert!(!insert(&mut cursor, 15, 15));
            assert!(!insert(&mut cursor, 30, 30));
            assert!(!insert(&mut cursor, 35, 35));
            // Rejections that leave no pending inserts also drop the run they
            // opened, keeping operations that require no pending inserts
            // available; seek_to asserts exactly that.
            assert!(cursor.state.insert_run.is_none());
            cursor
                .seek_to(Position::After(u64::as_bytes(&20).as_ref()))
                .unwrap();
            assert!(insert(&mut cursor, 25, 25));
            // Inserts must also stay above the pending insert.
            assert!(!insert(&mut cursor, 25, 25));
            assert!(!insert(&mut cursor, 24, 24));
            // A rejection with inserts pending must keep the run open.
            assert!(cursor.state.insert_run.is_some());
            assert!(insert(&mut cursor, 26, 26));
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);

            let expected = [10, 20, 25, 26, 30].map(|key| (key, key));
            assert_tree(root, &page_allocator, &expected);
        }

        // A run of insert_after calls arrives descending: each insert becomes
        // the gap's new successor, mirroring insert_before's ascending runs.
        #[test]
        fn insert_run_descending_from_empty() {
            for flush_every in [1, 3, 64] {
                let page_allocator = test_page_allocator();
                let mut root = None;
                let mut freed = vec![];
                let allocated = Arc::new(PageTracker::new_tracking());
                let mut cursor: CursorMut<'_, '_, u64, u64> =
                    CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
                cursor.seek_to(Position::Start).unwrap();
                for key in (0..2000).rev() {
                    assert!(insert_after(&mut cursor, key, key * 3));
                    if key % flush_every == 0 {
                        cursor.flush_insert_run(true).unwrap();
                    }
                }
                cursor.flush_insert_run(true).unwrap();
                drop(cursor);

                let expected: Vec<_> = (0..2000).map(|key| (key, key * 3)).collect();
                assert_tree(root, &page_allocator, &expected);
            }
        }

        // Both piles fill in one run: insert_before advances the gap's lower
        // bound while insert_after lowers its upper bound, and the flush
        // splices them around the same gap.
        #[test]
        fn insert_run_mixed_directions() {
            let (page_allocator, root_page, allocated) = leaf_root_with_entries(&[10, 20, 30]);
            let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
            let mut freed = vec![];
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor
                .seek_to(Position::After(u64::as_bytes(&20).as_ref()))
                .unwrap();
            assert!(insert(&mut cursor, 21, 21));
            assert!(insert_after(&mut cursor, 29, 29));
            assert!(insert_after(&mut cursor, 25, 25));
            assert!(insert(&mut cursor, 22, 22));
            // The live bounds are the piles' innermost pending inserts.
            assert!(!insert(&mut cursor, 25, 25));
            assert!(!insert_after(&mut cursor, 22, 22));
            assert!(!insert_after(&mut cursor, 25, 25));
            assert!(insert(&mut cursor, 24, 24));
            // A mid-run flush resumes at the same gap, between the piles.
            cursor.flush_insert_run(true).unwrap();
            assert_eq!(cursor.peek_prev().unwrap().unwrap().key(), 24);
            assert_eq!(cursor.peek_next().unwrap().unwrap().key(), 25);
            drop(cursor);

            let expected = [10, 20, 21, 22, 24, 25, 29, 30].map(|key| (key, key));
            assert_tree(root, &page_allocator, &expected);
        }

        #[test]
        fn insert_run_after_rejects_unordered_keys() {
            let (page_allocator, root_page, allocated) = leaf_root_with_entries(&[10, 20, 30]);
            let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
            let mut freed = vec![];
            let mut cursor: CursorMut<'_, '_, u64, u64> =
                CursorMut::new(&mut root, &page_allocator, &mut freed, &allocated);
            cursor
                .seek_to(Position::After(u64::as_bytes(&20).as_ref()))
                .unwrap();
            // Equal to the previous entry, below it, equal to the next entry,
            // and above it are all rejected.
            assert!(!insert_after(&mut cursor, 20, 20));
            assert!(!insert_after(&mut cursor, 15, 15));
            assert!(!insert_after(&mut cursor, 30, 30));
            assert!(!insert_after(&mut cursor, 35, 35));
            // Rejections that leave no pending inserts also drop the run they
            // opened; seek_to asserts exactly that.
            assert!(cursor.state.insert_run.is_none());
            cursor
                .seek_to(Position::After(u64::as_bytes(&20).as_ref()))
                .unwrap();
            assert!(insert_after(&mut cursor, 25, 25));
            // Inserts must also stay below the pending insert.
            assert!(!insert_after(&mut cursor, 25, 25));
            assert!(!insert_after(&mut cursor, 26, 26));
            assert!(cursor.state.insert_run.is_some());
            // Switching direction splices the pending insert first, so this
            // rejection sees it as the gap's upper bound in the tree, and
            // the rejected run is dropped again.
            assert!(!insert(&mut cursor, 25, 25));
            assert!(cursor.state.insert_run.is_none());
            assert!(insert(&mut cursor, 21, 21));
            cursor.flush_insert_run(false).unwrap();
            drop(cursor);

            let expected = [10, 20, 21, 25, 30].map(|key| (key, key));
            assert_tree(root, &page_allocator, &expected);
        }
    }

    // Once poisoned, every cursor operation re-raises instead of touching the
    // tree, so removals stranded by the original error can never be observed
    // as applied.
    #[test]
    fn poisoned_cursor_mut_re_raises() {
        let (page_allocator, root_page, allocated) = leaf_root_with_entries(&[1, 2, 3]);
        let mut root = Some(BtreeHeader::new(root_page, DEFERRED, 3));
        let mut freed = vec![];
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
