use crate::tree_store::btree_base::{
    branch_checksum, leaf_checksum, BranchAccessor, BranchMutator, Checksum, FreePolicy,
    LeafAccessor, BRANCH, LEAF,
};
use crate::tree_store::btree_iters::BtreeDrain;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::{AccessGuardMut, BtreeDrainFilter, BtreeRangeIter, PageHint, PageNumber};
use crate::types::{RedbKey, RedbValue, RedbValueMutInPlace};
use crate::{AccessGuard, Result};
#[cfg(feature = "logging")]
use log::trace;
use std::borrow::Borrow;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};
use std::sync::{Arc, Mutex};

pub(crate) struct BtreeStats {
    pub(crate) tree_height: usize,
    pub(crate) leaf_pages: usize,
    pub(crate) branch_pages: usize,
    pub(crate) stored_leaf_bytes: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) fragmented_bytes: usize,
}

pub(crate) struct UntypedBtreeMut<'a> {
    mem: &'a TransactionalMemory,
    root: Arc<Mutex<Option<(PageNumber, Checksum)>>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    key_width: Option<usize>,
    value_width: Option<usize>,
}

impl<'a> UntypedBtreeMut<'a> {
    pub(crate) fn new(
        root: Option<(PageNumber, Checksum)>,
        mem: &'a TransactionalMemory,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root: Arc::new(Mutex::new(root)),
            freed_pages,
            key_width,
            value_width,
        }
    }

    pub(crate) fn get_root(&self) -> Option<(PageNumber, Checksum)> {
        *(*self.root).lock().unwrap()
    }

    // Relocate the btree to lower pages
    pub(crate) fn relocate(&mut self) -> Result<bool> {
        if let Some(root) = self.get_root() {
            if let Some(new_root) = self.relocate_helper(root.0)? {
                *self.root.lock().unwrap() = Some(new_root);
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Relocates the given page to a lower page if possible, and returns the new page number
    fn relocate_helper(
        &mut self,
        page_number: PageNumber,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        let old_page = self.mem.get_page(page_number)?;
        let mut new_page = self.mem.allocate_lowest(old_page.memory().len())?;
        let new_page_number = new_page.get_page_number();
        if !new_page_number.is_before(page_number) {
            drop(new_page);
            self.mem.free(new_page_number);
            return Ok(None);
        }

        new_page.memory_mut().copy_from_slice(old_page.memory());

        let node_mem = old_page.memory();
        let new_checksum = match node_mem[0] {
            LEAF => leaf_checksum(&new_page, self.key_width, self.value_width),
            BRANCH => {
                let accessor = BranchAccessor::new(&old_page, self.key_width);
                let mut mutator = BranchMutator::new(&mut new_page);
                for i in 0..accessor.count_children() {
                    let child = accessor.child_page(i).unwrap();
                    if let Some((new_child, new_checksum)) = self.relocate_helper(child)? {
                        mutator.write_child_page(i, new_child, new_checksum);
                    }
                }
                branch_checksum(&new_page, self.key_width)
            }
            _ => unreachable!(),
        };

        let mut freed_pages = self.freed_pages.lock().unwrap();
        FreePolicy::Uncommitted.conditional_free(page_number, &mut freed_pages, self.mem);

        Ok(Some((new_page_number, new_checksum)))
    }
}

pub(crate) struct BtreeMut<'a, K: RedbKey, V: RedbValue> {
    mem: &'a TransactionalMemory,
    root: Arc<Mutex<Option<(PageNumber, Checksum)>>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + 'a, V: RedbValue + 'a> BtreeMut<'a, K, V> {
    pub(crate) fn new(
        root: Option<(PageNumber, Checksum)>,
        mem: &'a TransactionalMemory,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            mem,
            root: Arc::new(Mutex::new(root)),
            freed_pages,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn get_root(&self) -> Option<(PageNumber, Checksum)> {
        *(*self.root).lock().unwrap()
    }

    pub(crate) fn relocate(&mut self) -> Result<bool> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem,
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        if tree.relocate()? {
            *self.root.lock().unwrap() = tree.get_root();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn insert(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with value of length {}",
            &self.root,
            key,
            V::as_bytes(value).as_ref().len()
        );
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut root = self.root.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut root,
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let (old_value, _) = operation.insert(key, value)?;
        Ok(old_value)
    }

    pub(crate) fn remove(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let mut root = self.root.lock().unwrap();
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut root,
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let result = operation.delete(key)?;
        Ok(result)
    }

    // Like remove(), but does not free uncommitted data
    pub(crate) fn remove_retain_uncommitted(
        &mut self,
        key: &K::SelfType<'_>,
    ) -> Result<Option<(AccessGuard<V>, Vec<PageNumber>)>> {
        let mut freed_pages = vec![];
        let mut root = self.root.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut root, FreePolicy::Never, self.mem, &mut freed_pages);
        let result = operation.safe_delete(key)?;
        Ok(result.map(|x| (x, freed_pages)))
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.read_tree()?.print_debug(include_values)
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.get_root().map(|(p, _)| p),
            self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    fn read_tree(&self) -> Result<Btree<'a, K, V>> {
        Btree::new(self.get_root(), PageHint::None, self.mem)
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        self.read_tree()?.get(key)
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR> + 'a0, KR: Borrow<K::SelfType<'a0>> + 'a0>(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<'a, K, V>>
    where
        K: 'a0,
    {
        self.read_tree()?.range(range)
    }

    pub(crate) fn drain<
        'a0,
        T: RangeBounds<KR> + Clone + 'a0,
        // TODO: we shouldn't require Clone
        KR: Borrow<K::SelfType<'a0>> + Clone + 'a0,
    >(
        &mut self,
        range: T,
    ) -> Result<BtreeDrain<'a, K, V>>
    where
        K: 'a0,
    {
        let iter = self.range(range.clone())?;
        let return_iter = self.range(range)?;
        let mut free_on_drop = vec![];
        let mut root = self.root.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut root, FreePolicy::Never, self.mem, &mut free_on_drop);
        for entry in iter {
            // TODO: optimize so that we don't have to call safe_delete in a loop
            assert!(operation.safe_delete(entry?.key().borrow())?.is_some());
        }

        let result = BtreeDrain::new(
            return_iter,
            free_on_drop,
            self.freed_pages.clone(),
            self.mem,
        );

        Ok(result)
    }

    pub(crate) fn drain_filter<
        'a0,
        T: RangeBounds<KR> + Clone + 'a0,
        // TODO: we shouldn't require Clone
        KR: Borrow<K::SelfType<'a0>> + Clone + 'a0,
        F: for<'f> Fn(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    >(
        &mut self,
        range: T,
        predicate: F,
    ) -> Result<BtreeDrainFilter<'a, K, V, F>>
    where
        K: 'a0,
    {
        let iter = self.range(range.clone())?;
        let return_iter = self.range(range)?;
        let mut free_on_drop = vec![];
        let mut root = self.root.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut root, FreePolicy::Never, self.mem, &mut free_on_drop);
        for entry in iter {
            // TODO: optimize so that we don't have to call safe_delete in a loop
            let entry = entry?;
            if predicate(entry.key(), entry.value()) {
                assert!(operation.safe_delete(entry.key().borrow())?.is_some());
            }
        }

        let result = BtreeDrainFilter::new(
            return_iter,
            predicate,
            free_on_drop,
            self.freed_pages.clone(),
            self.mem,
        );

        Ok(result)
    }

    pub(crate) fn len(&self) -> Result<usize> {
        self.read_tree()?.len()
    }
}

impl<'a, K: RedbKey + 'a, V: RedbValueMutInPlace + 'a> BtreeMut<'a, K, V> {
    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    pub(crate) fn insert_reserve(
        &mut self,
        key: &K::SelfType<'_>,
        value_length: usize,
    ) -> Result<AccessGuardMut<'a, K, V>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with {} reserved bytes for the value",
            &self.root,
            key,
            value_length
        );
        let mut root = self.root.lock().unwrap();
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut value = vec![0u8; value_length];
        V::initialize(&mut value);
        let mut operation = MutateHelper::<K, V>::new(
            &mut root,
            FreePolicy::Uncommitted,
            self.mem,
            freed_pages.as_mut(),
        );
        let (_, mut guard) = operation.insert(key, &V::from_bytes(&value))?;
        guard.set_root_for_drop(self.root.clone());
        Ok(guard)
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

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        if let Some((root, checksum)) = self.root {
            self.verify_checksum_helper(root, checksum)
        } else {
            Ok(true)
        }
    }

    fn verify_checksum_helper(
        &self,
        page_number: PageNumber,
        expected_checksum: Checksum,
    ) -> Result<bool> {
        let page = self.mem.get_page(page_number)?;
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                expected_checksum
                    == leaf_checksum(&page, self.fixed_key_size, self.fixed_value_size)
            }
            BRANCH => {
                if expected_checksum != branch_checksum(&page, self.fixed_key_size) {
                    return Ok(false);
                }
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                for i in 0..accessor.count_children() {
                    if !self.verify_checksum_helper(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    )? {
                        return Ok(false);
                    }
                }
                true
            }
            _ => unreachable!(),
        })
    }
}

pub(crate) struct Btree<'a, K: RedbKey, V: RedbValue> {
    mem: &'a TransactionalMemory,
    // Cache of the root page to avoid repeated lookups
    cached_root: Option<PageImpl<'a>>,
    root: Option<(PageNumber, Checksum)>,
    hint: PageHint,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey, V: RedbValue> Btree<'a, K, V> {
    pub(crate) fn new(
        root: Option<(PageNumber, Checksum)>,
        hint: PageHint,
        mem: &'a TransactionalMemory,
    ) -> Result<Self> {
        let cached_root = if let Some((r, _)) = root {
            Some(mem.get_page_extended(r, hint)?)
        } else {
            None
        };
        Ok(Self {
            mem,
            cached_root,
            root,
            hint,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some(ref root_page) = self.cached_root {
            self.get_helper(root_page.clone(), K::as_bytes(key).as_ref())
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(&self, page: PageImpl<'a>, query: &[u8]) -> Result<Option<AccessGuard<'a, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    // Safety: free_on_drop is false
                    let guard = AccessGuard::new(page, start, end - start, false, self.mem);
                    Ok(Some(guard))
                } else {
                    Ok(None)
                }
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (_, child_page) = accessor.child_for_key::<K>(query);
                self.get_helper(self.mem.get_page_extended(child_page, self.hint)?, query)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR> + 'a0, KR: Borrow<K::SelfType<'a0>> + 'a0>(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<'a, K, V>>
    where
        K: 'a0,
    {
        BtreeRangeIter::new(range, self.root.map(|(p, _)| p), self.mem)
    }

    pub(crate) fn len(&self) -> Result<usize> {
        let iter: BtreeRangeIter<K, V> = BtreeRangeIter::new::<RangeFull, K::SelfType<'_>>(
            ..,
            self.root.map(|(p, _)| p),
            self.mem,
        )?;
        let mut count = 0;
        for v in iter {
            v?;
            count += 1;
        }
        Ok(count)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        if let Some((p, _)) = self.root {
            let mut pages = vec![self.mem.get_page(p)?];
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
                                next_children.push(self.mem.get_page(child)?);
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

        Ok(())
    }
}

pub(crate) fn btree_stats(
    root: Option<PageNumber>,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<BtreeStats> {
    if let Some(root) = root {
        stats_helper(root, mem, fixed_key_size, fixed_value_size)
    } else {
        Ok(BtreeStats {
            tree_height: 0,
            leaf_pages: 0,
            branch_pages: 0,
            stored_leaf_bytes: 0,
            metadata_bytes: 0,
            fragmented_bytes: 0,
        })
    }
}

fn stats_helper(
    page_number: PageNumber,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<BtreeStats> {
    let page = mem.get_page(page_number)?;
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
            let leaf_bytes = accessor.length_of_pairs(0, accessor.num_pairs());
            let overhead_bytes = accessor.total_length() - leaf_bytes;
            let fragmented_bytes = page.memory().len() - accessor.total_length();
            Ok(BtreeStats {
                tree_height: 1,
                leaf_pages: 1,
                branch_pages: 0,
                stored_leaf_bytes: leaf_bytes,
                metadata_bytes: overhead_bytes,
                fragmented_bytes,
            })
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
                    let stats = stats_helper(child, mem, fixed_key_size, fixed_value_size)?;
                    max_child_height = max(max_child_height, stats.tree_height);
                    leaf_pages += stats.leaf_pages;
                    branch_pages += stats.branch_pages;
                    stored_leaf_bytes += stats.stored_leaf_bytes;
                    metadata_bytes += stats.metadata_bytes;
                    fragmented_bytes += stats.fragmented_bytes;
                }
            }

            Ok(BtreeStats {
                tree_height: max_child_height + 1,
                leaf_pages,
                branch_pages,
                stored_leaf_bytes,
                metadata_bytes,
                fragmented_bytes,
            })
        }
        _ => unreachable!(),
    }
}
