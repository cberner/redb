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

#[derive(Clone)]
struct Leaf {
    page: PageImpl,
    position: usize,
    len: usize,
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
        self.descend_to_bound(root_page, bound)
    }

    pub(super) fn seek_to_end(&mut self) -> Result {
        self.path.clear();
        let root_page = self.manager.get_page(self.root, self.hint)?;
        self.descend_edge(root_page, true)
    }

    fn descend_to_bound(&mut self, page: PageImpl, bound: Bound<&[u8]>) -> Result {
        match page.memory()[0] {
            LEAF => {
                let (position, len) = {
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    (
                        lower_bound_entry::<K>(&accessor, bound),
                        accessor.num_pairs(),
                    )
                };
                self.set_leaf(page, position, len);
                Ok(())
            }
            BRANCH => {
                let (child_index, child_page) = {
                    let accessor = BranchAccessor::new(&page, K::fixed_width());
                    let child_index = child_to_visit::<K>(&accessor, bound, false);
                    (child_index, accessor.child_page(child_index).unwrap())
                };
                self.path.push(Branch::new(page, child_index));
                let page = self.manager.get_page(child_page, self.hint)?;
                self.descend_to_bound(page, bound)
            }
            _ => unreachable!(),
        }
    }

    fn descend_edge(&mut self, page: PageImpl, high_edge: bool) -> Result {
        let manager = &self.manager;
        let hint = self.hint;
        let mut get_page = |page| manager.get_page(page, hint);
        let (page, len) = descend_edge::<K, V, _>(page, high_edge, &mut self.path, &mut get_page)?;
        let position = if high_edge { len } else { 0 };
        self.set_leaf(page, position, len);
        Ok(())
    }

    fn set_leaf(&mut self, page: PageImpl, position: usize, len: usize) {
        self.leaf = Some(Leaf {
            page,
            position,
            len,
        });
    }

    fn move_to_adjacent_leaf(&mut self, forward: bool) -> Result<bool> {
        for index in (0..self.path.len()).rev() {
            let next_child = {
                let frame = &self.path[index];
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
                self.path[index].child_index = child_index;
                self.path.truncate(index + 1);
                let page = self.manager.get_page(child_page, self.hint)?;
                self.descend_edge(page, !forward)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn normalize_forward_gap(&mut self) -> Result {
        let Some(leaf) = self.leaf.as_ref() else {
            return Ok(());
        };
        if leaf.position == leaf.len {
            self.move_to_adjacent_leaf(true)?;
        }
        Ok(())
    }

    pub(super) fn next(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        loop {
            let Some(leaf) = self.leaf.as_ref() else {
                return Ok(None);
            };
            if leaf.position < leaf.len {
                let entry = self.get_entry(leaf.position);
                self.leaf
                    .as_mut()
                    .expect("cursor must be positioned")
                    .position += 1;
                return Ok(Some(entry));
            }

            if !self.move_to_adjacent_leaf(true)? {
                return Ok(None);
            }
        }
    }

    pub(super) fn prev(&mut self) -> Result<Option<EntryGuard<K, V>>> {
        loop {
            let Some(leaf) = self.leaf.as_ref() else {
                return Ok(None);
            };
            if leaf.position > 0 {
                let entry = leaf.position - 1;
                self.leaf
                    .as_mut()
                    .expect("cursor must be positioned")
                    .position = entry;
                return Ok(Some(self.get_entry(entry)));
            }

            if !self.move_to_adjacent_leaf(false)? {
                return Ok(None);
            }
        }
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

    fn get_entry(&self, entry: usize) -> EntryGuard<K, V> {
        let leaf = self.leaf.as_ref().expect("cursor must be positioned");
        let (key, value) =
            LeafAccessor::new(leaf.page.memory(), K::fixed_width(), V::fixed_width())
                .entry_ranges(entry)
                .expect("cursor entry must exist");
        EntryGuard::new(leaf.page.clone(), key, value)
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
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    pub(super) fn seek_to(&mut self, position: CursorMutPosition) -> Result {
        self.path.clear();
        self.leaf = None;
        let Some(header) = *self.root else {
            return Ok(());
        };
        let root = self.page_allocator.get_page(header.root, PageHint::None)?;
        let high_edge = matches!(position, CursorMutPosition::End);
        let page_allocator = self.page_allocator;
        let mut get_page = |page| page_allocator.get_page(page, PageHint::None);
        let (page, len) = descend_edge::<K, V, _>(root, high_edge, &mut self.path, &mut get_page)?;
        let position = if high_edge { len } else { 0 };
        self.leaf = Some(Leaf {
            page,
            position,
            len,
        });
        Ok(())
    }

    pub(super) fn remove_next(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        let Some(leaf) = self.leaf.take() else {
            self.path.clear();
            return Ok(None);
        };
        if leaf.position == leaf.len {
            self.path.clear();
            return Ok(None);
        }
        self.remove_leaf_entry(leaf.page, leaf.position)
    }

    pub(super) fn remove_prev(
        &mut self,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        let Some(leaf) = self.leaf.take() else {
            self.path.clear();
            return Ok(None);
        };
        let Some(position) = leaf.position.checked_sub(1) else {
            self.path.clear();
            return Ok(None);
        };
        self.remove_leaf_entry(leaf.page, position)
    }

    fn remove_leaf_entry(
        &mut self,
        leaf: PageImpl,
        position: usize,
    ) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
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
        Ok(Some(helper.pop_leaf_entry(leaf, path, position)?))
    }
}
