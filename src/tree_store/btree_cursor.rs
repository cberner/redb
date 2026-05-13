use crate::AccessGuard;
use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, LEAF, LeafAccessor};
use crate::tree_store::btree_iters::{EntryGuard, child_to_visit, lower_bound_entry};
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageResolver, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::cmp::Ordering;
use std::collections::Bound;
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

fn descend_edge<K: Key + 'static, V: Value + 'static, F>(
    page: PageImpl,
    high_edge: bool,
    path: &mut Vec<Branch>,
    get_page: &mut F,
) -> Result<(PageImpl, usize)>
where
    // TODO: introduce a trait for this that PageResolver can implement too.
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    match page.memory()[0] {
        LEAF => {
            let len = {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                accessor.num_pairs()
            };
            Ok((page, len))
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, K::fixed_width());
            let child_index = if high_edge {
                accessor.count_children() - 1
            } else {
                0
            };
            let child_page = accessor.child_page(child_index).unwrap();
            path.push(Branch::new(page, child_index));
            let child = get_page(child_page)?;
            descend_edge::<K, V, F>(child, high_edge, path, get_page)
        }
        _ => unreachable!(),
    }
}

fn edge_leaf<K: Key + 'static, V: Value + 'static, F>(
    page: PageImpl,
    high_edge: bool,
    path: &mut Vec<Branch>,
    get_page: &mut F,
) -> Result<Leaf>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    let (page, len) = descend_edge::<K, V, F>(page, high_edge, path, get_page)?;
    Ok(Leaf {
        page,
        position: if high_edge { len } else { 0 },
        len,
    })
}

fn descend_to_bound<K: Key + 'static, V: Value + 'static, F>(
    page: PageImpl,
    bound: Bound<&[u8]>,
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
                    lower_bound_entry::<K>(&accessor, bound),
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
                let child_index = child_to_visit::<K>(&accessor, bound, false);
                (child_index, accessor.child_page(child_index).unwrap())
            };
            path.push(Branch::new(page, child_index));
            let child = get_page(child_page)?;
            descend_to_bound::<K, V, F>(child, bound, path, get_page)
        }
        _ => unreachable!(),
    }
}

fn move_to_adjacent_leaf<K: Key + 'static, V: Value + 'static, F>(
    path: &mut Vec<Branch>,
    forward: bool,
    get_page: &mut F,
) -> Result<Option<Leaf>>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    for index in (0..path.len()).rev() {
        let next_child = {
            let frame = &path[index];
            let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
            if forward {
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
            return edge_leaf::<K, V, F>(page, !forward, path, get_page).map(Some);
        }
    }

    Ok(None)
}

fn prepare_leaf<K: Key + 'static, V: Value + 'static, F>(
    leaf: &mut Option<Leaf>,
    path: &mut Vec<Branch>,
    forward: bool,
    get_page: &mut F,
) -> Result<bool>
where
    F: FnMut(PageNumber) -> Result<PageImpl>,
{
    while let Some(current) = leaf.as_ref() {
        if (forward && current.position < current.len) || (!forward && current.position > 0) {
            return Ok(true);
        }
        let Some(next_leaf) = move_to_adjacent_leaf::<K, V, F>(path, forward, get_page)? else {
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

    pub(super) fn seek_to(&mut self, bound: Bound<&[u8]>) -> Result {
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
        *leaf = Some(descend_to_bound::<K, V, _>(
            root_page,
            bound,
            path,
            &mut get_page,
        )?);
        Ok(())
    }

    pub(super) fn seek_to_end(&mut self) -> Result {
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
        *leaf = Some(edge_leaf::<K, V, _>(root_page, true, path, &mut get_page)?);
        Ok(())
    }

    fn prepare_next(&mut self) -> Result<bool> {
        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        prepare_leaf::<K, V, _>(leaf, path, true, &mut get_page)
    }

    fn prepare_prev(&mut self) -> Result<bool> {
        let Self {
            manager,
            hint,
            path,
            leaf,
            ..
        } = self;
        let mut get_page = |page| manager.get_page(page, *hint);
        prepare_leaf::<K, V, _>(leaf, path, false, &mut get_page)
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
        if let Some(next_leaf) = move_to_adjacent_leaf::<K, V, _>(path, true, &mut get_page)? {
            *leaf = Some(next_leaf);
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.prepare_next()? {
            return Ok(None);
        }

        let leaf = self.leaf.as_mut().expect("cursor must be positioned");
        let position = leaf.position;
        leaf.position += 1;
        Ok(Some(entry(leaf, position)))
    }

    pub(super) fn prev(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        if !self.prepare_prev()? {
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

#[derive(Copy, Clone)]
pub(super) enum CursorMutPosition {
    Start,
    End,
}

pub(super) struct CursorMut<'a, 'b, K: Key + 'static, V: Value + 'static> {
    root: &'b mut Option<BtreeHeader>,
    page_allocator: &'b PageAllocator,
    freed: &'b mut Vec<PageNumber>,
    allocated: &'b Arc<Mutex<PageTrackerPolicy>>,
    path: Vec<Branch>,
    leaf: Option<Leaf>,
    removed_indexes: Vec<usize>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
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
            path: vec![],
            leaf: None,
            removed_indexes: vec![],
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn seek_to(&mut self, position: CursorMutPosition) -> Result {
        assert!(self.removed_indexes.is_empty());
        self.path.clear();
        self.leaf = None;
        let Some(header) = *self.root else {
            return Ok(());
        };
        let root = self.page_allocator.get_page(header.root, PageHint::None)?;
        let high_edge = matches!(position, CursorMutPosition::End);
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        self.leaf = Some(edge_leaf::<K, V, _>(
            root,
            high_edge,
            &mut self.path,
            &mut get_page,
        )?);
        Ok(())
    }

    pub(super) fn seek_to_bound(&mut self, bound: Bound<&[u8]>) -> Result {
        assert!(self.removed_indexes.is_empty());
        self.path.clear();
        self.leaf = None;
        let Some(header) = *self.root else {
            return Ok(());
        };
        let root_page = self.page_allocator.get_page(header.root, PageHint::None)?;
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        self.leaf = Some(descend_to_bound::<K, V, _>(
            root_page,
            bound,
            &mut self.path,
            &mut get_page,
        )?);
        Ok(())
    }

    fn prepare_next(&mut self) -> Result<bool> {
        loop {
            let Some(leaf) = self.leaf.as_ref() else {
                return Ok(false);
            };
            if leaf.position < leaf.len {
                return Ok(true);
            }
            if self.removed_indexes.is_empty() {
                if !self.advance_to_next_leaf()? {
                    return Ok(false);
                }
            } else {
                self.flush_removed_entries(false)?;
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool> {
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        if let Some(next_leaf) =
            move_to_adjacent_leaf::<K, V, _>(&mut self.path, true, &mut get_page)?
        {
            self.leaf = Some(next_leaf);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn prepare_prev(&mut self) -> Result<bool> {
        assert!(self.removed_indexes.is_empty());
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        prepare_leaf::<K, V, _>(&mut self.leaf, &mut self.path, false, &mut get_page)
    }

    pub(super) fn peek_next(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.prepare_next()? {
            return Ok(None);
        }
        let leaf = self.leaf.as_ref().expect("cursor must be positioned");
        Ok(Some(entry_ref(leaf, leaf.position)))
    }

    pub(super) fn peek_prev(&mut self) -> Result<Option<EntryRef<'_, K, V>>> {
        if !self.prepare_prev()? {
            return Ok(None);
        }
        let leaf = self.leaf.as_ref().expect("cursor must be positioned");
        Ok(Some(entry_ref(leaf, leaf.position - 1)))
    }

    pub(super) fn next(&mut self) -> Result<bool> {
        if !self.prepare_next()? {
            return Ok(false);
        }
        self.leaf
            .as_mut()
            .expect("cursor must be positioned")
            .position += 1;
        Ok(true)
    }

    pub(super) fn prev(&mut self) -> Result<bool> {
        if !self.prepare_prev()? {
            return Ok(false);
        }
        self.leaf
            .as_mut()
            .expect("cursor must be positioned")
            .position -= 1;
        Ok(true)
    }

    /// Removes and returns the next entry.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    pub(super) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.removed_indexes.is_empty());
        if !self.prepare_next()? {
            return Ok(None);
        }
        let leaf = self.leaf.take().expect("cursor must be positioned");
        let position = leaf.position;
        self.remove_leaf_entry(leaf.page, position)
    }

    /// Removes and returns the next entry.
    ///
    /// The caller may continue mutating the tree while the returned guards are live.
    pub(super) fn remove_next_detached(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.removed_indexes.is_empty());
        if !self.prepare_next()? {
            return Ok(None);
        }
        let leaf = self.leaf.take().expect("cursor must be positioned");
        let position = leaf.position;
        self.remove_leaf_entry_detached(leaf.page, position)
    }

    pub(super) fn remove_next_discard(&mut self) -> Result<bool> {
        if !self.prepare_next()? {
            return Ok(false);
        }
        let leaf = self.leaf.as_mut().expect("cursor must be positioned");
        self.removed_indexes.push(leaf.position);
        leaf.position += 1;
        Ok(true)
    }

    /// Removes and returns the previous entry.
    ///
    /// The returned guards must be dropped before mutating the tree again.
    pub(super) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.removed_indexes.is_empty());
        if !self.prepare_prev()? {
            return Ok(None);
        }
        let leaf = self.leaf.take().expect("cursor must be positioned");
        let position = leaf.position - 1;
        self.remove_leaf_entry(leaf.page, position)
    }

    /// Removes and returns the previous entry.
    ///
    /// The caller may continue mutating the tree while the returned guards are live.
    pub(super) fn remove_prev_detached(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.removed_indexes.is_empty());
        if !self.prepare_prev()? {
            return Ok(None);
        }
        let leaf = self.leaf.take().expect("cursor must be positioned");
        let position = leaf.position - 1;
        self.remove_leaf_entry_detached(leaf.page, position)
    }

    pub(super) fn finish_pending_removals(&mut self) -> Result {
        self.flush_removed_entries(true)
    }

    fn flush_removed_entries(&mut self, close: bool) -> Result {
        if self.removed_indexes.is_empty() {
            return Ok(());
        }

        let leaf = self.leaf.take().expect("cursor must be positioned");
        // If the cursor is closing, no later operation needs a valid position.
        // Otherwise the tree mutation invalidates the current path, so reseek
        // to the first entry after the original leaf.
        let next_bound = (!close).then(|| key_data::<K, V>(&leaf, leaf.len - 1));
        let path = std::mem::take(&mut self.path)
            .into_iter()
            .map(Branch::into_parts)
            .collect();
        let mut removed_indexes = std::mem::take(&mut self.removed_indexes);
        let mut helper: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        helper.delete_leaf_entries(leaf.page, path, &removed_indexes)?;
        removed_indexes.clear();
        self.removed_indexes = removed_indexes;

        if let Some(next_bound) = next_bound {
            // TODO: preserve enough cursor state to advance without reseeking from the root.
            self.seek_to_bound(Bound::Excluded(&next_bound))?;
        }
        Ok(())
    }

    fn remove_leaf_entry(
        &mut self,
        leaf: PageImpl,
        position: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_leaf_entry_inner(leaf, position, true)
    }

    fn remove_leaf_entry_detached(
        &mut self,
        leaf: PageImpl,
        position: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.remove_leaf_entry_inner(leaf, position, false)
    }

    fn remove_leaf_entry_inner(
        &mut self,
        leaf: PageImpl,
        position: usize,
        allow_in_place: bool,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        assert!(self.removed_indexes.is_empty());
        let path = std::mem::take(&mut self.path)
            .into_iter()
            .map(Branch::into_parts)
            .collect();
        let mut helper = MutateHelper::new(
            &mut *self.root,
            (*self.page_allocator).clone(),
            &mut *self.freed,
            Arc::clone(self.allocated),
        );
        let entry = if allow_in_place {
            helper.pop_leaf_entry(leaf, path, position)?
        } else {
            helper.pop_leaf_entry_detached(leaf, path, position)?
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
        cursor.seek_to(Bound::Unbounded).unwrap();
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
