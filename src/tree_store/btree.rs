use crate::tree_store::btree_base::{
    branch_checksum, leaf_checksum, BranchAccessor, Checksum, FreePolicy, LeafAccessor, BRANCH,
    LEAF,
};
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::{AccessGuardMut, BtreeRangeIter, PageNumber};
use crate::types::{RedbKey, RedbValue};
use crate::{AccessGuard, Result};
#[cfg(feature = "logging")]
use log::trace;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};
use std::rc::Rc;

pub(crate) struct BtreeStats {
    pub(crate) tree_height: usize,
    pub(crate) leaf_pages: usize,
    pub(crate) branch_pages: usize,
    pub(crate) stored_leaf_bytes: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) fragmented_bytes: usize,
}

pub(crate) struct BtreeMut<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    mem: &'a TransactionalMemory,
    root: Rc<RefCell<Option<(PageNumber, Checksum)>>>,
    freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> BtreeMut<'a, K, V> {
    pub(crate) fn new(
        root: Option<(PageNumber, Checksum)>,
        mem: &'a TransactionalMemory,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            mem,
            root: Rc::new(RefCell::new(root)),
            freed_pages,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn get_root(&self) -> Option<(PageNumber, Checksum)> {
        *(*self.root).borrow()
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert(
        &mut self,
        key: &K::RefBaseType<'_>,
        value: &V::RefBaseType<'_>,
    ) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with value of length {}",
            &self.root,
            key,
            V::as_bytes(value).as_ref().len()
        );
        let mut freed_pages = self.freed_pages.borrow_mut();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            self.root.clone(),
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let (old_value, _) = operation.insert(key, value)?;
        Ok(old_value)
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    // TODO: return type should be V, not [u8]
    pub(crate) unsafe fn insert_reserve(
        &mut self,
        key: &K::RefBaseType<'_>,
        value_length: usize,
    ) -> Result<AccessGuardMut<'a, K, &[u8]>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with {} reserved bytes for the value",
            &self.root,
            key,
            value_length
        );
        let mut freed_pages = self.freed_pages.borrow_mut();
        let value = vec![0u8; value_length];
        let mut operation = MutateHelper::<K, &[u8]>::new(
            self.root.clone(),
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let (_, mut guard) = operation.insert(key, &value)?;
        guard.set_root_for_drop(self.root.clone());
        Ok(guard)
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn remove(
        &mut self,
        key: &K::RefBaseType<'_>,
    ) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let mut freed_pages = self.freed_pages.borrow_mut();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            self.root.clone(),
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let result = operation.delete(key)?;
        Ok(result)
    }

    // TODO: remove after verifying that this isn't needed to optimize multimap tables
    // Like remove(), but does not free uncommitted data
    #[allow(dead_code)]
    pub(crate) fn remove_retain_uncommitted(
        &mut self,
        key: &K::RefBaseType<'_>,
    ) -> Result<Option<AccessGuard<V>>> {
        let mut freed_pages = self.freed_pages.borrow_mut();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            self.root.clone(),
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

    pub(crate) fn stats(&self) -> BtreeStats {
        btree_stats(
            self.get_root().map(|(p, _)| p),
            self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    fn read_tree(&self) -> Btree<K, V> {
        Btree::new(self.get_root(), self.mem)
    }

    pub(crate) fn get(&self, key: &K::RefBaseType<'_>) -> Result<Option<V::SelfType<'_>>> {
        self.read_tree().get(key)
    }

    pub(crate) fn range<
        'a0,
        T: RangeBounds<KR> + 'a0,
        KR: Borrow<K::RefBaseType<'a0>> + ?Sized + 'a0,
    >(
        &'a0 self,
        range: T,
    ) -> Result<BtreeRangeIter<K, V>> {
        self.read_tree().range(range)
    }

    pub(crate) fn len(&self) -> Result<usize> {
        self.read_tree().len()
    }
}

pub(crate) struct RawBtree<'a> {
    mem: &'a TransactionalMemory,
    root: Option<(PageNumber, Checksum)>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl<'a> RawBtree<'a> {
    pub(crate) fn new(
        root: Option<(PageNumber, Checksum)>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: &'a TransactionalMemory,
    ) -> Self {
        Self {
            mem,
            root,
            fixed_key_size,
            fixed_value_size,
        }
    }

    pub(crate) fn verify_checksum(&self) -> bool {
        if let Some((root, checksum)) = self.root {
            self.verify_checksum_helper(root, checksum)
        } else {
            true
        }
    }

    fn verify_checksum_helper(&self, page_number: PageNumber, expected_checksum: Checksum) -> bool {
        let page = self.mem.get_page(page_number);
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                expected_checksum
                    == leaf_checksum(
                        &page,
                        self.fixed_key_size,
                        self.fixed_value_size,
                        self.mem.checksum_type(),
                    )
            }
            BRANCH => {
                if expected_checksum
                    != branch_checksum(&page, self.fixed_key_size, self.mem.checksum_type())
                {
                    return false;
                }
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                for i in 0..accessor.count_children() {
                    if !self.verify_checksum_helper(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    ) {
                        return false;
                    }
                }
                true
            }
            _ => unreachable!(),
        }
    }
}

pub(crate) struct Btree<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    mem: &'a TransactionalMemory,
    root: Option<(PageNumber, Checksum)>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Btree<'a, K, V> {
    pub(crate) fn new(root: Option<(PageNumber, Checksum)>, mem: &'a TransactionalMemory) -> Self {
        Self {
            mem,
            root,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn get(&self, key: &K::RefBaseType<'_>) -> Result<Option<V::SelfType<'a>>> {
        if let Some((p, _)) = self.root {
            let root_page = self.mem.get_page(p);
            return Ok(self.get_helper(root_page, K::as_bytes(key).as_ref()));
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(&self, page: PageImpl<'a>, query: &[u8]) -> Option<V::SelfType<'a>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let entry_index = accessor.find_key::<K>(query)?;
                let (start, end) = accessor.value_range(entry_index).unwrap();
                Some(V::from_bytes(&page.into_memory()[start..end]))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (_, child_page) = accessor.child_for_key::<K>(query);
                self.get_helper(self.mem.get_page(child_page), query)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn range<
        'a0,
        T: RangeBounds<KR> + 'a0,
        KR: Borrow<K::RefBaseType<'a0>> + ?Sized + 'a0,
    >(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<'a, K, V>>
    where
        'a: 'a0,
    {
        Ok(BtreeRangeIter::new(
            range,
            self.root.map(|(p, _)| p),
            self.mem,
        ))
    }

    pub(crate) fn len(&self) -> Result<usize> {
        let mut iter: BtreeRangeIter<K, V> = BtreeRangeIter::new::<RangeFull, K::RefBaseType<'_>>(
            ..,
            self.root.map(|(p, _)| p),
            self.mem,
        );
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) {
        if let Some((p, _)) = self.root {
            let mut pages = vec![self.mem.get_page(p)];
            while !pages.is_empty() {
                let mut next_children = vec![];
                for page in pages.drain(..) {
                    let node_mem = page.memory();
                    match node_mem[0] {
                        LEAF => {
                            eprint!("Leaf[ (page={:?})", page.get_page_number());
                            LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width())
                                .print_node::<K, V>(include_values);
                            eprint!("]");
                        }
                        BRANCH => {
                            let accessor = BranchAccessor::new(&page, K::fixed_width());
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
}

pub(crate) fn btree_stats(
    root: Option<PageNumber>,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> BtreeStats {
    if let Some(root) = root {
        stats_helper(root, mem, fixed_key_size, fixed_value_size)
    } else {
        BtreeStats {
            tree_height: 0,
            leaf_pages: 0,
            branch_pages: 0,
            stored_leaf_bytes: 0,
            metadata_bytes: 0,
            fragmented_bytes: 0,
        }
    }
}

fn stats_helper(
    page_number: PageNumber,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> BtreeStats {
    let page = mem.get_page(page_number);
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
            let leaf_bytes = accessor.length_of_pairs(0, accessor.num_pairs());
            let overhead_bytes = accessor.total_length() - leaf_bytes;
            let fragmented_bytes = page.memory().len() - accessor.total_length();
            BtreeStats {
                tree_height: 1,
                leaf_pages: 1,
                branch_pages: 0,
                stored_leaf_bytes: leaf_bytes,
                metadata_bytes: overhead_bytes,
                fragmented_bytes,
            }
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, fixed_key_size);
            let mut max_child_height = 0;
            let mut leaf_pages = 0;
            let mut branch_pages = 1;
            let mut stored_leaf_bytes = 0;
            let mut metadata_bytes = accessor.total_length();
            let mut fragmented_bytes = page.memory().len() - accessor.total_length();
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    let stats = stats_helper(child, mem, fixed_key_size, fixed_value_size);
                    max_child_height = max(max_child_height, stats.tree_height);
                    leaf_pages += stats.leaf_pages;
                    branch_pages += stats.branch_pages;
                    stored_leaf_bytes += stats.stored_leaf_bytes;
                    metadata_bytes += stats.metadata_bytes;
                    fragmented_bytes += stats.fragmented_bytes;
                }
            }

            BtreeStats {
                tree_height: max_child_height + 1,
                leaf_pages,
                branch_pages,
                stored_leaf_bytes,
                metadata_bytes,
                fragmented_bytes,
            }
        }
        _ => unreachable!(),
    }
}
