use crate::AccessGuard;
use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, LEAF, LeafAccessor};
use crate::tree_store::btree_iters::EntryGuard;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::cmp::Ordering;
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

#[derive(Copy, Clone)]
enum Direction {
    Next,
    Previous,
}

impl Direction {
    fn is_next(self) -> bool {
        matches!(self, Self::Next)
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

// Owned cursor state, kept separate from the tree borrows so that it can
// outlive a single operation.
#[derive(Default)]
pub(super) struct CursorState {
    // None means the cursor has not been positioned. Otherwise the ancestor
    // path and current leaf are kept together as one valid gap cursor.
    position: Option<CursorPosition>,
    // Pending removals from the current leaf, recorded in strictly increasing order.
    removed_indexes: Vec<usize>,
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
        Self {
            root,
            page_allocator,
            freed,
            allocated,
            state: CursorState::default(),
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
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

    // The cursor can be positioned at the edge of a leaf. Before peeking in a
    // direction, move across empty leaf edges until the current leaf has an
    // entry on that side or the cursor reaches the tree edge.
    fn ensure_has_entry(&mut self, direction: Direction) -> Result<bool> {
        loop {
            let Some(position) = self.state.position.as_ref() else {
                return Ok(false);
            };
            if position.has_entry(direction) {
                return Ok(true);
            }
            if direction.is_next() && !self.state.removed_indexes.is_empty() {
                let resume_key = self
                    .flush_removed_entries()?
                    .expect("pending removals exist");
                self.seek_to(Position::After(&resume_key))?;
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
        assert!(self.state.removed_indexes.is_empty());
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
        self.move_cursor(Direction::Next);
        Ok(true)
    }

    pub(super) fn prev(&mut self) -> Result<bool> {
        if self.peek_prev()?.is_none() {
            return Ok(false);
        }
        self.move_cursor(Direction::Previous);
        Ok(true)
    }

    fn move_cursor(&mut self, direction: Direction) {
        self.state
            .position
            .as_mut()
            .expect("cursor must be positioned")
            .move_once(direction);
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

    /// Removes and returns the next entry.
    ///
    /// The caller may continue mutating the tree while the returned guards are live.
    pub(super) fn remove_next_detached(
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
        self.remove_leaf_entry_detached(position.leaf.page, position.path, index)
    }

    pub(super) fn remove_next_discard(&mut self) -> Result<bool> {
        if !self.ensure_has_entry(Direction::Next)? {
            return Ok(false);
        }
        let leaf = &mut self
            .state
            .position
            .as_mut()
            .expect("cursor must be positioned")
            .leaf;
        assert!(
            self.state
                .removed_indexes
                .last()
                .is_none_or(|last| *last < leaf.position),
            "removed indexes must be recorded in strictly increasing order"
        );
        self.state.removed_indexes.push(leaf.position);
        leaf.position += 1;
        Ok(true)
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

    /// Removes and returns the previous entry.
    ///
    /// The caller may continue mutating the tree while the returned guards are live.
    pub(super) fn remove_prev_detached(
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
        self.remove_leaf_entry_detached(position.leaf.page, position.path, index)
    }

    pub(super) fn finish_pending_removals(&mut self) -> Result {
        let _ = self.flush_removed_entries()?;
        Ok(())
    }

    /// True if a failed flush may have left entries in the tree that the
    /// caller was told were removed. The transaction must not commit.
    pub(super) fn lost_removals(&self) -> bool {
        self.state.lost_removals
    }

    fn flush_removed_entries(&mut self) -> Result<Option<Vec<u8>>> {
        if self.state.removed_indexes.is_empty() {
            return Ok(None);
        }

        let position = self
            .state
            .position
            .take()
            .expect("cursor must be positioned");
        // Tree mutation invalidates the cursor path. Callers that continue
        // iteration reseek to the first entry after the original leaf.
        let resume_key = key_data::<K, V>(&position.leaf, position.leaf.len - 1);
        let (path, leaf) = position.into_parts();
        let mut removed_indexes = std::mem::take(&mut self.state.removed_indexes);
        let mut helper: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        let result = helper.delete_leaf_entries(leaf.page, path, &removed_indexes);
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
        self.remove_leaf_entry_inner(leaf, path, index, true)
    }

    fn remove_leaf_entry_detached(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_leaf_entry_inner(leaf, path, index, false)
    }

    fn remove_leaf_entry_inner(
        &mut self,
        leaf: PageImpl,
        path: Vec<Branch>,
        index: usize,
        allow_in_place: bool,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.state.removed_indexes.is_empty());
        let path = path.into_iter().map(Branch::into_parts).collect();
        let mut helper = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        let entry = if allow_in_place {
            helper.pop_leaf_entry(leaf, path, index)?
        } else {
            helper.pop_leaf_entry_detached(leaf, path, index)?
        };
        Ok(Some(entry))
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
