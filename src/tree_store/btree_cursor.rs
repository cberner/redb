use crate::Result;
use crate::tree_store::btree_base::{BRANCH, BranchAccessor, LEAF, LeafAccessor};
use crate::tree_store::btree_iters::{EntryGuard, child_to_visit, lower_bound_entry};
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{PageNumber, PageResolver};
use crate::types::{Key, Value};
use std::cmp::Ordering;
use std::collections::Bound;
use std::marker::PhantomData;

#[derive(Clone)]
struct Branch {
    page: PageImpl,
    child_index: usize,
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
                self.path.push(Branch { page, child_index });
                let page = self.manager.get_page(child_page, self.hint)?;
                self.descend_to_bound(page, bound)
            }
            _ => unreachable!(),
        }
    }

    fn descend_edge(&mut self, page: PageImpl, high_edge: bool) -> Result {
        match page.memory()[0] {
            LEAF => {
                let (position, len) = {
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    let len = accessor.num_pairs();
                    (if high_edge { len } else { 0 }, len)
                };
                self.set_leaf(page, position, len);
                Ok(())
            }
            BRANCH => {
                let (child_index, child_page) = {
                    let accessor = BranchAccessor::new(&page, K::fixed_width());
                    let child_index = if high_edge {
                        accessor.count_children() - 1
                    } else {
                        0
                    };
                    (child_index, accessor.child_page(child_index).unwrap())
                };
                self.path.push(Branch { page, child_index });
                let page = self.manager.get_page(child_page, self.hint)?;
                self.descend_edge(page, high_edge)
            }
            _ => unreachable!(),
        }
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
