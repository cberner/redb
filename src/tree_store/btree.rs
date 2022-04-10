use crate::tree_store::btree_base::{FreePolicy, InternalAccessor, LeafAccessor, INTERNAL, LEAF};
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::{AccessGuardMut, BtreeRangeIter, PageNumber};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::{AccessGuard, Result};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};
use std::rc::Rc;

pub(crate) struct BtreeMut<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    mem: &'a TransactionalMemory,
    root: Option<PageNumber>,
    freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> BtreeMut<'a, K, V> {
    pub(crate) fn new(
        root: Option<PageNumber>,
        mem: &'a TransactionalMemory,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            mem,
            root,
            freed_pages,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn get_root(&self) -> Option<PageNumber> {
        self.root
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert(&mut self, key: &K, value: &V) -> Result {
        let mut freed_pages = self.freed_pages.borrow_mut();
        let mut operation = MutateHelper::new(
            &mut self.root,
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        operation.insert(key, value)?;
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
        let mut freed_pages = self.freed_pages.borrow_mut();
        let value = vec![0u8; value_length];
        let mut operation = MutateHelper::new(
            &mut self.root,
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let guard = operation.insert(key, value.as_slice())?;
        Ok(guard)
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn remove(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        let mut freed_pages = self.freed_pages.borrow_mut();
        let mut operation = MutateHelper::new(
            &mut self.root,
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let result = operation.delete(key)?;
        Ok(result)
    }

    // Like remove(), but does not free uncommitted data
    pub(crate) fn remove_retain_uncommitted(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        let mut freed_pages = self.freed_pages.borrow_mut();
        let mut operation = MutateHelper::new(
            &mut self.root,
            FreePolicy::Never,
            self.mem,
            freed_pages.as_mut(),
        );
        let result = operation.safe_delete(key)?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) {
        self.read_tree().print_debug(include_values)
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

    pub(crate) fn branch_pages(&self) -> usize {
        self.read_tree().branch_pages()
    }

    pub(crate) fn leaf_pages(&self) -> usize {
        self.read_tree().leaf_pages()
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
            return Ok(self.get_helper(root_page, key.as_bytes().as_ref()));
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(
        &self,
        page: PageImpl<'a>,
        query: &[u8],
    ) -> Option<<<V as RedbValue>::View as WithLifetime<'a>>::Out> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                let entry_index = accessor.find_key::<K>(query)?;
                let (start, end) = accessor.value_range(entry_index).unwrap();
                Some(V::from_bytes(&page.into_memory()[start..end]))
            }
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let (_, child_page) = accessor.child_for_key::<K>(query);
                self.get_helper(self.mem.get_page(child_page), query)
            }
            _ => unreachable!(),
        }
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
    pub(crate) fn print_debug(&self, include_values: bool) {
        if let Some(p) = self.root {
            let mut pages = vec![self.mem.get_page(p)];
            while !pages.is_empty() {
                let mut next_children = vec![];
                for page in pages.drain(..) {
                    let node_mem = page.memory();
                    match node_mem[0] {
                        LEAF => {
                            LeafAccessor::new(&page).print_node::<K, V>(include_values);
                        }
                        INTERNAL => {
                            let accessor = InternalAccessor::new(&page);
                            for i in 0..accessor.count_children() {
                                let child = accessor.child_page(i).unwrap();
                                next_children.push(self.mem.get_page(child));
                            }
                            accessor.print_node::<K>();
                        }
                        _ => unreachable!(),
                    }
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

    pub(crate) fn branch_pages(&self) -> usize {
        if let Some(root) = self.root {
            self.branch_pages_helper(root)
        } else {
            0
        }
    }

    // TODO: merge all these stats helpers together
    fn branch_pages_helper(&self, page_number: PageNumber) -> usize {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => 0,
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let mut count = 1;
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        count += self.branch_pages_helper(child);
                    }
                }

                count
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn leaf_pages(&self) -> usize {
        if let Some(root) = self.root {
            self.leaf_pages_helper(root)
        } else {
            0
        }
    }

    fn leaf_pages_helper(&self, page_number: PageNumber) -> usize {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => 1,
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let mut count = 0;
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        count += self.leaf_pages_helper(child);
                    }
                }

                count
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
