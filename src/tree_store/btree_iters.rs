use crate::Result;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, BranchBuilder, DEFERRED, LEAF, LeafAccessor, LeafBuilder,
};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::page_store::{Page, PageHint, PageImpl, TransactionalMemory};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use Bound::{Excluded, Included, Unbounded};
use std::borrow::Borrow;
use std::cmp::Ordering;
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
            Leaf { page, .. } | Internal { page, .. } => page.get_page_number(),
        }
    }

    fn next(
        self,
        reverse: bool,
        manager: &TransactionalMemory,
        hint: PageHint,
    ) -> Result<Option<RangeIterState>> {
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
                let child_page = manager.get_page(child_page, hint)?;
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
    manager: Arc<TransactionalMemory>,
    hint: PageHint,
}

impl AllPageNumbersBtreeIter {
    pub(crate) fn new(
        root: PageNumber,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        manager: Arc<TransactionalMemory>,
        hint: PageHint,
    ) -> Result<Self> {
        let root_page = manager.get_page(root, hint)?;
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
                Internal { child, .. } => child == 0,
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

// Walks the tree in-order, yielding entries for which `predicate` returns true and
// removing those yielded entries from the tree. Semantics match the old iterator-based
// implementation: the user predicate is only invoked on entries reached by the iterator,
// and only yielded entries are removed -- if the iterator is dropped mid-leaf, any
// later entries in that leaf (including matches that were never yielded) stay.
//
// The speedup over the previous `MutateHelper::delete` per entry comes from batching
// at page boundaries: when the iterator moves off a leaf, the leaf is rebuilt in a
// single LeafBuilder pass (or dropped entirely if all entries were yielded), and branch
// updates are accumulated as we walk so each ancestor branch is touched once, not once
// per yielded entry.
pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    root: &'a mut Option<BtreeHeader>,
    page_allocator: PageAllocator,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    predicate: F,
    // Range bounds snapshotted as owned bytes so they're stable across calls.
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    // Walk state machine.
    state: ExtractState,
    // Pages queued to free at drop (committed pages). Uncommitted pages are freed
    // immediately via page_allocator.free_if_uncommitted.
    freed: Vec<PageNumber>,
    entries_removed: u64,
    initial_length: u64,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

enum ExtractState {
    NotStarted,
    Forward {
        path: Vec<BranchFrame>,
        leaf: Option<LeafFrame>,
    },
    Backward {
        path: Vec<BranchFrame>,
        leaf: Option<LeafFrame>,
    },
    Done,
}

struct BranchFrame {
    page: PageImpl,
    page_number: PageNumber,
    // Child index this branch occupies in its parent (0 for root, ignored).
    parent_child_idx: usize,
    // The next child index to descend into. None = exhausted in the current
    // direction. The semantics of "next" depend on the walker direction:
    // Forward uses higher indices, Backward uses lower.
    next_child: Option<usize>,
    // Accumulated updates for this branch's children. Insertion order does not
    // matter; we sort at finalize time.
    updates: Vec<(usize, ChildChange)>,
}

struct LeafFrame {
    page: PageImpl,
    page_number: PageNumber,
    parent_child_idx: usize,
    // Next entry index to examine. Entries [0..next_entry) have either been skipped
    // (out of range, or predicate returned false) or yielded.
    next_entry: usize,
    // Entry indices that were yielded (must be removed on finalize). Always ascending.
    yielded: Vec<usize>,
}

#[derive(Debug)]
enum ChildChange {
    Drop,
    Replace(PageNumber),
}

impl<'a, K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    BtreeExtractIf<'a, K, V, F>
{
    pub(crate) fn new<'r, KR, R>(
        root: &'a mut Option<BtreeHeader>,
        range: &'_ R,
        predicate: F,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
    ) -> Self
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
        R: RangeBounds<KR>,
    {
        let start_bound = match range.start_bound() {
            Included(k) => Included(K::as_bytes(k.borrow()).as_ref().to_vec()),
            Excluded(k) => Excluded(K::as_bytes(k.borrow()).as_ref().to_vec()),
            Unbounded => Unbounded,
        };
        let end_bound = match range.end_bound() {
            Included(k) => Included(K::as_bytes(k.borrow()).as_ref().to_vec()),
            Excluded(k) => Excluded(K::as_bytes(k.borrow()).as_ref().to_vec()),
            Unbounded => Unbounded,
        };
        let initial_length = root.as_ref().map_or(0, |h| h.length);
        Self {
            root,
            page_allocator,
            allocated,
            master_free_list,
            predicate,
            start_bound,
            end_bound,
            state: ExtractState::NotStarted,
            freed: Vec::new(),
            entries_removed: 0,
            initial_length,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    // Init forward walk: navigate to the first leaf in the tree.
    fn init_forward(&mut self) -> Result<()> {
        let Some(header) = *self.root else {
            self.state = ExtractState::Done;
            return Ok(());
        };
        self.state = ExtractState::Forward {
            path: Vec::new(),
            leaf: None,
        };
        self.descend_to_edge_leaf(header.root, 0, true)
    }

    // Init backward walk: navigate to the rightmost leaf in the tree.
    fn init_backward(&mut self) -> Result<()> {
        let Some(header) = *self.root else {
            self.state = ExtractState::Done;
            return Ok(());
        };
        self.state = ExtractState::Backward {
            path: Vec::new(),
            leaf: None,
        };
        self.descend_to_edge_leaf(header.root, 0, false)
    }

    // Descend from the subtree root `start_pn` toward the leftmost (forward) or
    // rightmost (backward) leaf, pushing each traversed branch onto the current state's
    // path. Sets state's leaf to the final LeafFrame.
    fn descend_to_edge_leaf(
        &mut self,
        start_pn: PageNumber,
        start_parent_child_idx: usize,
        is_forward: bool,
    ) -> Result<()> {
        let mut current_pn = start_pn;
        let mut parent_child_idx = start_parent_child_idx;
        loop {
            let page = self.page_allocator.get_page(current_pn, PageHint::None)?;
            match page.memory()[0] {
                LEAF => {
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    let num = accessor.num_pairs();
                    let new_leaf = LeafFrame {
                        page,
                        page_number: current_pn,
                        parent_child_idx,
                        next_entry: if is_forward { 0 } else { num },
                        yielded: Vec::new(),
                    };
                    match &mut self.state {
                        ExtractState::Forward { leaf, .. }
                        | ExtractState::Backward { leaf, .. } => {
                            *leaf = Some(new_leaf);
                        }
                        _ => unreachable!(),
                    }
                    return Ok(());
                }
                BRANCH => {
                    let accessor = BranchAccessor::new(&page, K::fixed_width());
                    let count = accessor.count_children();
                    let (enter_idx, next_child) = if is_forward {
                        (0usize, if count > 1 { Some(1) } else { None })
                    } else {
                        let idx = count - 1;
                        (idx, if idx > 0 { Some(idx - 1) } else { None })
                    };
                    let next_pn = accessor.child_page(enter_idx).unwrap();
                    match &mut self.state {
                        ExtractState::Forward { path, .. }
                        | ExtractState::Backward { path, .. } => {
                            path.push(BranchFrame {
                                page,
                                page_number: current_pn,
                                parent_child_idx,
                                next_child,
                                updates: Vec::new(),
                            });
                        }
                        _ => unreachable!(),
                    }
                    current_pn = next_pn;
                    parent_child_idx = enter_idx;
                }
                _ => unreachable!(),
            }
        }
    }

    // Finalize the current leaf. If we yielded anything from it, rebuild it (or drop
    // it, if we yielded every entry). Record the update in the innermost branch frame
    // so the branch rewrite at pop time knows about it.
    fn finalize_leaf(&mut self, leaf: LeafFrame) -> Result<()> {
        let yielded = &leaf.yielded;
        let yielded_count = yielded.len();
        self.entries_removed += yielded_count as u64;
        if yielded_count == 0 {
            drop(leaf.page);
            return Ok(());
        }
        let accessor = LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width());
        let num = accessor.num_pairs();
        let change = if yielded_count == num {
            drop(leaf.page);
            self.queue_free(leaf.page_number);
            ChildChange::Drop
        } else {
            let mut builder = LeafBuilder::new(
                &self.page_allocator,
                &self.allocated,
                num - yielded_count,
                K::fixed_width(),
                V::fixed_width(),
            );
            let mut yielded_iter = yielded.iter().copied().peekable();
            for i in 0..num {
                if yielded_iter.peek() == Some(&i) {
                    yielded_iter.next();
                    continue;
                }
                let entry = accessor.entry(i).unwrap();
                builder.push(entry.key(), entry.value());
            }
            let new_page = builder.build()?;
            let new_page_number = new_page.get_page_number();
            drop(new_page);
            drop(leaf.page);
            self.queue_free(leaf.page_number);
            ChildChange::Replace(new_page_number)
        };
        match &mut self.state {
            ExtractState::Forward { path, .. } | ExtractState::Backward { path, .. } => {
                if let Some(frame) = path.last_mut() {
                    frame.updates.push((leaf.parent_child_idx, change));
                } else {
                    self.apply_root_change(change);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    // Apply a child change at the tree root (no parent branch exists).
    fn apply_root_change(&mut self, change: ChildChange) {
        match change {
            ChildChange::Drop => {
                *self.root = None;
            }
            ChildChange::Replace(new_page) => {
                let new_length = self.initial_length - self.entries_removed;
                *self.root = Some(BtreeHeader::new(new_page, DEFERRED, new_length));
            }
        }
    }

    // Finalize a branch frame: apply its accumulated child updates. Returns the branch's
    // change to propagate up (Drop / Replace / no-op).
    fn finalize_branch(&mut self, mut frame: BranchFrame) -> Result<Option<ChildChange>> {
        if frame.updates.is_empty() {
            drop(frame.page);
            return Ok(None);
        }
        frame.updates.sort_by_key(|(i, _)| *i);
        let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
        let count = accessor.count_children();
        let mut kept_children: Vec<(PageNumber, crate::tree_store::btree_base::Checksum)> =
            Vec::with_capacity(count);
        let mut kept_indices: Vec<usize> = Vec::with_capacity(count);
        let mut updates_iter = frame.updates.iter().peekable();
        for i in 0..count {
            let change = if updates_iter.peek().map(|(idx, _)| *idx) == Some(i) {
                Some(&updates_iter.next().unwrap().1)
            } else {
                None
            };
            match change {
                Some(ChildChange::Drop) => {}
                Some(ChildChange::Replace(new_page)) => {
                    kept_children.push((*new_page, DEFERRED));
                    kept_indices.push(i);
                }
                None => {
                    kept_children.push((
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    ));
                    kept_indices.push(i);
                }
            }
        }
        if kept_children.is_empty() {
            drop(frame.page);
            self.queue_free(frame.page_number);
            return Ok(Some(ChildChange::Drop));
        }
        if kept_children.len() == 1 {
            let (single_page, _checksum) = kept_children[0];
            drop(frame.page);
            self.queue_free(frame.page_number);
            return Ok(Some(ChildChange::Replace(single_page)));
        }
        let mut builder = BranchBuilder::new(
            &self.page_allocator,
            &self.allocated,
            kept_children.len(),
            K::fixed_width(),
        );
        for (k, (child_pn, checksum)) in kept_children.iter().enumerate() {
            if k > 0 {
                builder.push_key(accessor.key(kept_indices[k - 1]).unwrap());
            }
            builder.push_child(*child_pn, *checksum);
        }
        let new_page = builder.build()?;
        let new_page_number = new_page.get_page_number();
        drop(new_page);
        drop(frame.page);
        self.queue_free(frame.page_number);
        Ok(Some(ChildChange::Replace(new_page_number)))
    }

    // Free a page now (if uncommitted) or queue it for the master free list.
    fn queue_free(&mut self, page: PageNumber) {
        let mut allocated = self.allocated.lock().unwrap();
        if !self
            .page_allocator
            .free_if_uncommitted(page, &mut allocated)
        {
            self.freed.push(page);
        }
    }

    // Advance the walker to the next leaf in the current direction. Pops and finalizes
    // branch frames as their children are exhausted.
    fn advance_to_next_leaf(&mut self) -> Result<()> {
        loop {
            let is_forward = matches!(self.state, ExtractState::Forward { .. });
            let path = match &mut self.state {
                ExtractState::Forward { path, leaf } | ExtractState::Backward { path, leaf } => {
                    debug_assert!(leaf.is_none());
                    path
                }
                _ => return Ok(()),
            };
            let Some(frame) = path.last_mut() else {
                self.state = ExtractState::Done;
                return Ok(());
            };
            let Some(child_idx) = frame.next_child else {
                let popped = path.pop().unwrap();
                let parent_child_idx = popped.parent_child_idx;
                let change = self.finalize_branch(popped)?;
                match &mut self.state {
                    ExtractState::Forward { path, .. } | ExtractState::Backward { path, .. } => {
                        if let Some(parent) = path.last_mut() {
                            if let Some(c) = change {
                                parent.updates.push((parent_child_idx, c));
                            }
                        } else if let Some(c) = change {
                            self.apply_root_change(c);
                        }
                    }
                    _ => unreachable!(),
                }
                continue;
            };
            let child_pn = {
                let accessor = BranchAccessor::new(&frame.page, K::fixed_width());
                let count = accessor.count_children();
                frame.next_child = if is_forward {
                    if child_idx + 1 < count {
                        Some(child_idx + 1)
                    } else {
                        None
                    }
                } else if child_idx > 0 {
                    Some(child_idx - 1)
                } else {
                    None
                };
                accessor.child_page(child_idx).unwrap()
            };
            self.descend_to_edge_leaf(child_pn, child_idx, is_forward)?;
            return Ok(());
        }
    }

    fn finalize_on_drop(&mut self) -> Result<()> {
        let leaf_to_finalize = match &mut self.state {
            ExtractState::Forward { leaf, .. } | ExtractState::Backward { leaf, .. } => leaf.take(),
            _ => return Ok(()),
        };
        if let Some(leaf) = leaf_to_finalize {
            self.finalize_leaf(leaf)?;
        }
        loop {
            let popped = match &mut self.state {
                ExtractState::Forward { path, .. } | ExtractState::Backward { path, .. } => {
                    path.pop()
                }
                _ => None,
            };
            let Some(frame) = popped else {
                break;
            };
            let parent_child_idx = frame.parent_child_idx;
            let change = self.finalize_branch(frame)?;
            match &mut self.state {
                ExtractState::Forward { path, .. } | ExtractState::Backward { path, .. } => {
                    if let Some(parent_frame) = path.last_mut() {
                        if let Some(c) = change {
                            parent_frame.updates.push((parent_child_idx, c));
                        }
                    } else if let Some(c) = change {
                        self.apply_root_change(c);
                    }
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    }
}

enum RangeRelation {
    Before,
    Inside,
    After,
}

fn key_range_relation<K: Key>(
    start_bound: &Bound<Vec<u8>>,
    end_bound: &Bound<Vec<u8>>,
    key_bytes: &[u8],
) -> RangeRelation {
    match start_bound {
        Included(s) => {
            if matches!(K::compare(key_bytes, s), Ordering::Less) {
                return RangeRelation::Before;
            }
        }
        Excluded(s) => {
            if matches!(K::compare(key_bytes, s), Ordering::Less | Ordering::Equal) {
                return RangeRelation::Before;
            }
        }
        Unbounded => {}
    }
    match end_bound {
        Included(e) => {
            if matches!(K::compare(key_bytes, e), Ordering::Greater) {
                return RangeRelation::After;
            }
        }
        Excluded(e) => {
            if matches!(
                K::compare(key_bytes, e),
                Ordering::Greater | Ordering::Equal
            ) {
                return RangeRelation::After;
            }
        }
        Unbounded => {}
    }
    RangeRelation::Inside
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    BtreeExtractIf<'_, K, V, F>
{
    // Shared body for next() and next_back(). `is_forward` determines iteration direction.
    fn step(&mut self, is_forward: bool) -> Option<Result<EntryGuard<K, V>>> {
        loop {
            let (yielded_entry, leaf_exhausted, past_end) = {
                let leaf_ref = match &mut self.state {
                    ExtractState::Forward { leaf, .. } if is_forward => leaf,
                    ExtractState::Backward { leaf, .. } if !is_forward => leaf,
                    _ => return None,
                };
                let Some(leaf_frame) = leaf_ref else {
                    self.state = ExtractState::Done;
                    return None;
                };
                let accessor =
                    LeafAccessor::new(leaf_frame.page.memory(), K::fixed_width(), V::fixed_width());
                let num = accessor.num_pairs();
                let mut result: Option<EntryGuard<K, V>> = None;
                let mut past_end = false;
                loop {
                    let idx = if is_forward {
                        if leaf_frame.next_entry >= num {
                            break;
                        }
                        let i = leaf_frame.next_entry;
                        leaf_frame.next_entry += 1;
                        i
                    } else {
                        if leaf_frame.next_entry == 0 {
                            break;
                        }
                        leaf_frame.next_entry -= 1;
                        leaf_frame.next_entry
                    };
                    let entry = accessor.entry(idx).unwrap();
                    let key_bytes = entry.key();
                    match key_range_relation::<K>(&self.start_bound, &self.end_bound, key_bytes) {
                        RangeRelation::Before => {
                            if is_forward {
                                continue;
                            }
                            // Moving backward through sorted entries -- we've
                            // gone past the range's start; no more matches here.
                            past_end = true;
                            leaf_frame.next_entry = 0;
                            break;
                        }
                        RangeRelation::After => {
                            if !is_forward {
                                continue;
                            }
                            past_end = true;
                            leaf_frame.next_entry = num;
                            break;
                        }
                        RangeRelation::Inside => {}
                    }
                    let matched =
                        (self.predicate)(K::from_bytes(key_bytes), V::from_bytes(entry.value()));
                    if matched {
                        let (k_range, v_range) = accessor.entry_ranges(idx).unwrap();
                        // Yielded list stays sorted ascending: forward pushes at the
                        // end, backward inserts at the front.
                        if is_forward {
                            leaf_frame.yielded.push(idx);
                        } else {
                            let pos = leaf_frame
                                .yielded
                                .binary_search(&idx)
                                .expect_err("idx shouldn't already be present");
                            leaf_frame.yielded.insert(pos, idx);
                        }
                        let page_clone = leaf_frame.page.clone();
                        result = Some(EntryGuard::new(page_clone, k_range, v_range));
                        break;
                    }
                }
                let exhausted = if is_forward {
                    leaf_frame.next_entry >= num
                } else {
                    leaf_frame.next_entry == 0
                };
                (result, exhausted, past_end)
            };
            if let Some(entry) = yielded_entry {
                return Some(Ok(entry));
            }
            debug_assert!(leaf_exhausted || past_end);
            let leaf_owned = match &mut self.state {
                ExtractState::Forward { leaf, .. } | ExtractState::Backward { leaf, .. } => {
                    leaf.take().unwrap()
                }
                _ => unreachable!(),
            };
            if let Err(e) = self.finalize_leaf(leaf_owned) {
                self.state = ExtractState::Done;
                return Some(Err(e));
            }
            if past_end {
                if let Err(e) = self.finalize_on_drop() {
                    self.state = ExtractState::Done;
                    return Some(Err(e));
                }
                self.state = ExtractState::Done;
                return None;
            }
            if let Err(e) = self.advance_to_next_leaf() {
                self.state = ExtractState::Done;
                return Some(Err(e));
            }
        }
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Iterator
    for BtreeExtractIf<'_, K, V, F>
{
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            ExtractState::NotStarted => {
                if let Err(e) = self.init_forward() {
                    self.state = ExtractState::Done;
                    return Some(Err(e));
                }
            }
            ExtractState::Backward { .. } | ExtractState::Done => return None,
            ExtractState::Forward { .. } => {}
        }
        self.step(true)
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.state {
            ExtractState::NotStarted => {
                if let Err(e) = self.init_backward() {
                    self.state = ExtractState::Done;
                    return Some(Err(e));
                }
            }
            // Mixing directions on a single iterator is not supported; once forward
            // iteration has begun, subsequent next_back returns None rather than
            // panicking or yielding. This preserves the public "no panic on mixing"
            // behavior asserted in extract_if_next_then_next_back_panic.
            ExtractState::Forward { .. } | ExtractState::Done => return None,
            ExtractState::Backward { .. } => {}
        }
        self.step(false)
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Drop
    for BtreeExtractIf<'_, K, V, F>
{
    fn drop(&mut self) {
        let _ = self.finalize_on_drop();
        let mut master = self.master_free_list.lock().unwrap();
        master.extend(self.freed.drain(..));
    }
}

#[derive(Clone)]
pub(crate) struct BtreeRangeIter<K: Key + 'static, V: Value + 'static> {
    left: Option<RangeIterState>, // Exclusive. The previous element returned
    right: Option<RangeIterState>, // Exclusive. The previous element returned
    include_left: bool,           // left is inclusive, instead of exclusive
    include_right: bool,          // right is inclusive, instead of exclusive
    manager: Arc<TransactionalMemory>,
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
        manager: Arc<TransactionalMemory>,
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

    fn close(&mut self) {
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
            if self.left.as_ref().unwrap().get_entry::<K, V>().is_some() {
                return self.left.as_ref().map(|s| s.get_entry().unwrap()).map(Ok);
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
    hint: PageHint,
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
            let child_page = manager.get_page(child_page_number, hint)?;
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
    manager: &TransactionalMemory,
    hint: PageHint,
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
            let child_page = manager.get_page(child_page_number, hint)?;
            if child_index < accessor.count_children() - 1 {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    child: child_index + 1,
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
    manager: &TransactionalMemory,
    hint: PageHint,
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
            let child_page = manager.get_page(child_page_number, hint)?;
            if child_index > 0 && accessor.child_page(child_index - 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    fixed_key_size: K::fixed_width(),
                    fixed_value_size: V::fixed_width(),
                    child: child_index - 1,
                    parent,
                }));
            }
            find_iter_right::<K, V>(child_page, parent, query, include_query, manager, hint)
        }
        _ => unreachable!(),
    }
}
