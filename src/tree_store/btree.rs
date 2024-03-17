use crate::db::TransactionGuard;
use crate::tree_store::btree_base::{
    branch_checksum, leaf_checksum, BranchAccessor, BranchMutator, BtreeHeader, Checksum,
    LeafAccessor, BRANCH, DEFERRED, LEAF,
};
use crate::tree_store::btree_iters::BtreeExtractIf;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{CachePriority, Page, PageImpl, PageMut, TransactionalMemory};
use crate::tree_store::{
    AccessGuardMut, AllPageNumbersBtreeIter, BtreeRangeIter, PageHint, PageNumber,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, Result};
#[cfg(feature = "logging")]
use log::trace;
use std::borrow::Borrow;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

pub(crate) struct BtreeStats {
    pub(crate) tree_height: u32,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
}

pub(crate) struct UntypedBtreeMut {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    key_width: Option<usize>,
    value_width: Option<usize>,
}

impl UntypedBtreeMut {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            freed_pages,
            key_width,
            value_width,
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    // Recomputes the checksum for all pages that are uncommitted
    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result {
        let mut root = self.root;
        if let Some(BtreeHeader {
            root: ref p,
            ref mut checksum,
            length: _,
        }) = root
        {
            if !self.mem.uncommitted(*p) {
                // root page is clean
                return Ok(());
            }

            *checksum = self.finalize_dirty_checksums_helper(*p)?;
            self.root = root;
        }

        Ok(())
    }

    fn finalize_dirty_checksums_helper(&mut self, page_number: PageNumber) -> Result<Checksum> {
        assert!(self.mem.uncommitted(page_number));
        let mut page = self.mem.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => leaf_checksum(&page, self.key_width, self.value_width),
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                let mut new_children = vec![];
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.mem.uncommitted(child_page) {
                        let new_checksum = self.finalize_dirty_checksums_helper(child_page)?;
                        new_children.push(Some((i, child_page, new_checksum)));
                    } else {
                        // Child is clean, skip it
                        new_children.push(None);
                    }
                }

                drop(accessor);
                let mut mutator = BranchMutator::new(&mut page);
                for (child_index, child_page, child_checksum) in new_children.into_iter().flatten()
                {
                    mutator.write_child_page(child_index, child_page, child_checksum);
                }
                drop(mutator);

                branch_checksum(&page, self.key_width)
            }
            _ => unreachable!(),
        }
    }

    // Applies visitor to all dirty leaf pages in the tree
    pub(crate) fn dirty_leaf_visitor<F>(&mut self, visitor: F) -> Result
    where
        F: Fn(PageMut) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            if !self.mem.uncommitted(page_number) {
                // root page is clean
                return Ok(());
            }

            let page = self.mem.get_page_mut(page_number)?;
            match page.memory()[0] {
                LEAF => {
                    visitor(page)?;
                }
                BRANCH => {
                    drop(page);
                    self.dirty_leaf_visitor_helper(page_number, &visitor)?;
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    fn dirty_leaf_visitor_helper<F>(&mut self, page_number: PageNumber, visitor: &F) -> Result
    where
        F: Fn(PageMut) -> Result,
    {
        assert!(self.mem.uncommitted(page_number));
        let page = self.mem.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => {
                visitor(page)?;
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.mem.uncommitted(child_page) {
                        self.dirty_leaf_visitor_helper(child_page, visitor)?;
                    }
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    // Relocate the btree to lower pages
    pub(crate) fn relocate(&mut self) -> Result<bool> {
        if let Some(root) = self.get_root() {
            if let Some((new_root, new_checksum)) = self.relocate_helper(root.root)? {
                self.root = Some(BtreeHeader::new(new_root, new_checksum, root.length));
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
        let mut new_page = self.mem.allocate_lowest(
            old_page.memory().len(),
            CachePriority::default_btree(old_page.memory()),
        )?;
        let new_page_number = new_page.get_page_number();
        if !new_page_number.is_before(page_number) {
            drop(new_page);
            self.mem.free(new_page_number);
            return Ok(None);
        }

        new_page.memory_mut().copy_from_slice(old_page.memory());

        let node_mem = old_page.memory();
        match node_mem[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&old_page, self.key_width);
                let mut mutator = BranchMutator::new(&mut new_page);
                for i in 0..accessor.count_children() {
                    let child = accessor.child_page(i).unwrap();
                    if let Some((new_child, new_checksum)) = self.relocate_helper(child)? {
                        mutator.write_child_page(i, new_child, new_checksum);
                    }
                }
            }
            _ => unreachable!(),
        }

        let mut freed_pages = self.freed_pages.lock().unwrap();
        if !self.mem.free_if_uncommitted(page_number) {
            freed_pages.push(page_number);
        }

        Ok(Some((new_page_number, DEFERRED)))
    }
}

pub(crate) struct BtreeMut<'a, K: Key + 'static, V: Value + 'static> {
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<K: Key + 'static, V: Value + 'static> BtreeMut<'_, K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            mem,
            transaction_guard: guard,
            root,
            freed_pages,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _lifetime: Default::default(),
        }
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            V::fixed_width(),
            self.mem.clone(),
        )
        .verify_checksum()
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        tree.finalize_dirty_checksums()?;
        self.root = tree.get_root();
        Ok(())
    }

    pub(crate) fn all_pages_iter(&self) -> Result<Option<AllPageNumbersBtreeIter>> {
        if let Some(root) = self.root.map(|x| x.root) {
            Ok(Some(AllPageNumbersBtreeIter::new(
                root,
                K::fixed_width(),
                V::fixed_width(),
                self.mem.clone(),
            )?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn relocate(&mut self) -> Result<bool> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        if tree.relocate()? {
            self.root = tree.get_root();
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
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut self.root, self.mem.clone(), freed_pages.as_mut());
        let (old_value, _) = operation.insert(key, value)?;
        Ok(old_value)
    }

    pub(crate) fn remove(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new(&mut self.root, self.mem.clone(), freed_pages.as_mut());
        let result = operation.delete(key)?;
        Ok(result)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.read_tree()?.print_debug(include_values)
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.get_root().map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    fn read_tree(&self) -> Result<Btree<K, V>> {
        Btree::new(
            self.get_root(),
            PageHint::None,
            self.transaction_guard.clone(),
            self.mem.clone(),
        )
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        self.read_tree()?.get(key)
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR> + 'a0, KR: Borrow<K::SelfType<'a0>> + 'a0>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>>
    where
        K: 'a0,
    {
        self.read_tree()?.range(range)
    }

    pub(crate) fn extract_from_if<
        'a,
        'a0,
        T: RangeBounds<KR> + 'a0,
        KR: Borrow<K::SelfType<'a0>> + 'a0,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    >(
        &'a mut self,
        range: &'_ T,
        predicate: F,
    ) -> Result<BtreeExtractIf<'a, K, V, F>>
    where
        K: 'a0,
    {
        let iter = self.range(range)?;

        let result = BtreeExtractIf::new(
            &mut self.root,
            iter,
            predicate,
            self.freed_pages.clone(),
            self.mem.clone(),
        );

        Ok(result)
    }

    pub(crate) fn retain_in<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        mut predicate: F,
        range: impl RangeBounds<KR> + 'a,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let iter = self.range(&range)?;
        let mut freed = vec![];
        let mut operation: MutateHelper<'_, '_, K, V> =
            MutateHelper::new_do_not_modify(&mut self.root, self.mem.clone(), &mut freed);
        for entry in iter {
            let entry = entry?;
            if !predicate(entry.key(), entry.value()) {
                assert!(operation.delete(&entry.key())?.is_some());
            }
        }
        self.freed_pages.lock().unwrap().extend_from_slice(&freed);

        Ok(())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        self.read_tree()?.len()
    }
}

impl<'a, K: Key + 'a, V: MutInPlaceValue + 'a> BtreeMut<'a, K, V> {
    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    // Return type has the same lifetime as &self, because the tree must not be modified until the mutable guard is dropped
    pub(crate) fn insert_reserve(
        &mut self,
        key: &K::SelfType<'_>,
        value_length: u32,
    ) -> Result<AccessGuardMut<V>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with {} reserved bytes for the value",
            &self.root,
            key,
            value_length
        );
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut value = vec![0u8; value_length as usize];
        V::initialize(&mut value);
        let mut operation =
            MutateHelper::<K, V>::new(&mut self.root, self.mem.clone(), freed_pages.as_mut());
        let (_, guard) = operation.insert(key, &V::from_bytes(&value))?;
        Ok(guard)
    }
}

pub(crate) struct RawBtree {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl RawBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            mem,
            root,
            fixed_key_size,
            fixed_value_size,
        }
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            self.fixed_key_size,
            self.fixed_value_size,
        )
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map(|x| x.length).unwrap_or(0))
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        if let Some(header) = self.root {
            self.verify_checksum_helper(header.root, header.checksum)
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
                if let Ok(computed) =
                    leaf_checksum(&page, self.fixed_key_size, self.fixed_value_size)
                {
                    expected_checksum == computed
                } else {
                    false
                }
            }
            BRANCH => {
                if let Ok(computed) = branch_checksum(&page, self.fixed_key_size) {
                    if expected_checksum != computed {
                        return Ok(false);
                    }
                } else {
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
            _ => false,
        })
    }
}

pub(crate) struct Btree<K: Key + 'static, V: Value + 'static> {
    mem: Arc<TransactionalMemory>,
    _transaction_guard: Arc<TransactionGuard>,
    // Cache of the root page to avoid repeated lookups
    cached_root: Option<PageImpl>,
    root: Option<BtreeHeader>,
    hint: PageHint,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key, V: Value> Btree<K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let cached_root = if let Some(header) = root {
            Some(mem.get_page_extended(header.root, hint)?)
        } else {
            None
        };
        Ok(Self {
            mem,
            _transaction_guard: guard,
            cached_root,
            root,
            hint,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'static, V>>> {
        if let Some(ref root_page) = self.cached_root {
            self.get_helper(root_page.clone(), K::as_bytes(key).as_ref())
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(&self, page: PageImpl, query: &[u8]) -> Result<Option<AccessGuard<'static, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuard::with_page(page, start..end);
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

    pub(crate) fn range<'a0, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a0>>>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>> {
        BtreeRangeIter::new(range, self.root.map(|x| x.root), self.mem.clone())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map(|x| x.length).unwrap_or(0))
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        if let Some(p) = self.root.map(|x| x.root) {
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
            let fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            Ok(BtreeStats {
                tree_height: 1,
                leaf_pages: 1,
                branch_pages: 0,
                stored_leaf_bytes: leaf_bytes.try_into().unwrap(),
                metadata_bytes: overhead_bytes.try_into().unwrap(),
                fragmented_bytes,
            })
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, fixed_key_size);
            let mut max_child_height = 0;
            let mut leaf_pages = 0;
            let mut branch_pages = 1;
            let mut stored_leaf_bytes = 0;
            let mut metadata_bytes = accessor.total_length() as u64;
            let mut fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
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
