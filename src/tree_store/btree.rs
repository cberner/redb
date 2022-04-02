use crate::tree_store::btree_base::{InternalAccessor, LeafAccessor, INTERNAL, LEAF};
use crate::tree_store::btree_utils::{
    find_key, make_mut_single_leaf, node_children, print_node, tree_delete, tree_insert,
};
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::{AccessGuardMut, BtreeRangeIter, PageNumber};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::{AccessGuard, Result};
use std::borrow::Borrow;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};

pub(crate) struct BtreeMut<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    // TODO: make these private?
    pub(crate) mem: &'a TransactionalMemory,
    pub(crate) root: Option<PageNumber>,
    // TODO: storing these freed_pages is very error prone, because they need to be copied out into
    // the Storage object before the BtreeMut is destroyed
    pub(crate) freed_pages: Vec<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> BtreeMut<'a, K, V> {
    pub(crate) fn new(root: Option<PageNumber>, mem: &'a TransactionalMemory) -> Self {
        Self {
            mem,
            root,
            freed_pages: vec![],
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert(&mut self, key: &K, value: &V) -> Result {
        if let Some(p) = self.root {
            let root = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, _, freed) = tree_insert::<K>(
                root,
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
                self.mem,
            )?;
            self.freed_pages.extend_from_slice(&freed);
            self.root = Some(new_root);
        } else {
            let (new_root, _) =
                make_mut_single_leaf(key.as_bytes().as_ref(), value.as_bytes().as_ref(), self.mem)?;
            self.root = Some(new_root);
        }
        Ok(())
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert_reserve(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<AccessGuardMut> {
        let value = vec![0u8; value_length];
        let (new_root, guard) = if let Some(p) = self.root {
            let root = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, guard, freed) =
                tree_insert::<K>(root, key.as_bytes().as_ref(), &value, self.mem)?;
            self.freed_pages.extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key.as_bytes().as_ref(), &value, self.mem)?
        };
        self.root = Some(new_root);
        Ok(guard)
    }

    // Special version of insert_reserve for usage in the freed table
    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert_reserve_special(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<(AccessGuardMut, Option<PageNumber>, Vec<PageNumber>)> {
        let value = vec![0u8; value_length];
        let (new_root, guard) = if let Some(p) = self.root {
            let root = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, guard, freed) =
                tree_insert::<K>(root, key.as_bytes().as_ref(), &value, self.mem)?;
            self.freed_pages.extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key.as_bytes().as_ref(), &value, self.mem)?
        };
        self.root = Some(new_root);
        Ok((guard, self.root, std::mem::take(&mut self.freed_pages)))
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn remove(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        if let Some(p) = self.root {
            let root_page = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, found, freed) =
                tree_delete::<K, V>(root_page, key.as_bytes().as_ref(), true, self.mem)?;
            self.freed_pages.extend_from_slice(&freed);
            self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    // Like remove(), but does not free uncommitted data
    pub(crate) fn remove_retain_uncommitted(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        if let Some(p) = self.root {
            let root_page = self.mem.get_page(p);
            // Safety: free_uncommitted is false
            let (new_root, found, freed) = unsafe {
                tree_delete::<K, V>(root_page, key.as_bytes().as_ref(), false, self.mem)?
            };
            self.freed_pages.extend_from_slice(&freed);
            self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        self.read_tree().print_debug()
    }

    pub(crate) fn stored_leaf_bytes(&self) -> usize {
        self.read_tree().stored_leaf_bytes()
    }

    pub(crate) fn overhead_bytes(&self) -> usize {
        self.read_tree().overhead_bytes()
    }

    pub(crate) fn fragmented_bytes(&self) -> usize {
        self.read_tree().fragmented_bytes()
    }

    pub(crate) fn height(&self) -> usize {
        self.read_tree().height()
    }

    fn read_tree(&self) -> Btree<K, V> {
        Btree::new(self.root, self.mem)
    }

    pub(crate) fn get(
        &self,
        key: &K,
    ) -> Result<Option<<<V as RedbValue>::View as WithLifetime>::Out>> {
        self.read_tree().get(key)
    }

    pub(crate) fn range<T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<K, V>> {
        self.read_tree().range(range)
    }

    pub(crate) fn len(&self) -> Result<usize> {
        self.read_tree().len()
    }
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Drop for BtreeMut<'a, K, V> {
    fn drop(&mut self) {
        debug_assert!(self.freed_pages.is_empty());
        if !self.freed_pages.is_empty() {
            eprintln!("{} pages leaked by BtreeMut", self.freed_pages.len());
        }
    }
}

pub(crate) struct Btree<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    mem: &'a TransactionalMemory,
    root: Option<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Btree<'a, K, V> {
    pub(crate) fn new(root: Option<PageNumber>, mem: &'a TransactionalMemory) -> Self {
        Self {
            mem,
            root,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn get(
        &self,
        key: &K,
    ) -> Result<Option<<<V as RedbValue>::View as WithLifetime<'a>>::Out>> {
        if let Some(p) = self.root {
            let root_page = self.mem.get_page(p);
            return Ok(find_key::<K, V>(
                root_page,
                key.as_bytes().as_ref(),
                self.mem,
            ));
        }
        Ok(None)
    }

    pub(crate) fn range<T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<'a, K, V>> {
        Ok(BtreeRangeIter::new(range, self.root, self.mem))
    }

    pub(crate) fn len(&self) -> Result<usize> {
        let mut iter: BtreeRangeIter<[u8], [u8]> =
            BtreeRangeIter::new::<RangeFull, [u8]>(.., self.root, self.mem);
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        if let Some(p) = self.root {
            let mut pages = vec![self.mem.get_page(p)];
            while !pages.is_empty() {
                let mut next_children = vec![];
                for page in pages.drain(..) {
                    next_children.extend(node_children(&page, self.mem));
                    print_node::<K, PageImpl<'a>>(&page);
                    eprint!("  ");
                }
                eprintln!();

                pages = next_children;
            }
        }
    }

    pub(crate) fn stored_leaf_bytes(&self) -> usize {
        if let Some(root) = self.root {
            self.stored_bytes_helper(root)
        } else {
            0
        }
    }

    fn stored_bytes_helper(&self, page_number: PageNumber) -> usize {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                accessor.length_of_pairs(0, accessor.num_pairs())
            }
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let mut bytes = 0;
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        bytes += self.stored_bytes_helper(child);
                    }
                }

                bytes
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn overhead_bytes(&self) -> usize {
        if let Some(root) = self.root {
            self.overhead_bytes_helper(root)
        } else {
            0
        }
    }

    fn overhead_bytes_helper(&self, page_number: PageNumber) -> usize {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                accessor.total_length() - accessor.length_of_pairs(0, accessor.num_pairs())
            }
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                // Internal pages are all "overhead"
                let mut bytes = accessor.total_length();
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        bytes += self.overhead_bytes_helper(child);
                    }
                }

                bytes
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn fragmented_bytes(&self) -> usize {
        if let Some(root) = self.root {
            self.fragmented_bytes_helper(root)
        } else {
            0
        }
    }

    fn fragmented_bytes_helper(&self, page_number: PageNumber) -> usize {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                page.memory().len() - accessor.total_length()
            }
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                // Internal pages are all "overhead"
                let mut bytes = page.memory().len() - accessor.total_length();
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        bytes += self.fragmented_bytes_helper(child);
                    }
                }

                bytes
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn height(&self) -> usize {
        if let Some(root) = self.root {
            self.height_helper(root)
        } else {
            0
        }
    }

    fn height_helper(&self, page_number: PageNumber) -> usize {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => 1,
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let mut max_child_height = 0;
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        let height = self.height_helper(child);
                        max_child_height = max(max_child_height, height);
                    }
                }

                max_child_height + 1
            }
            _ => unreachable!(),
        }
    }
}
