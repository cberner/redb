use crate::compat::{HashMap, Mutex};
#[cfg(feature = "std")]
use crate::db::CorruptPageInfo;
use crate::db::TransactionGuard;
use crate::tree_store::btree_base::{
    AccessGuardMut, BRANCH, BranchAccessor, BranchMutator, BtreeHeader, Checksum, DEFERRED, LEAF,
    LeafAccessor, branch_checksum, leaf_checksum,
};
use crate::tree_store::btree_iters::BtreeExtractIf;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::compression::CompressionConfig;
use crate::tree_store::page_store::{Page, PageImpl, PageMut, TransactionalMemory};
use crate::tree_store::{
    AccessGuardMutInPlace, AllPageNumbersBtreeIter, BtreeRangeIter, PageHint, PageNumber,
    PageTrackerPolicy,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, Result};
#[cfg(feature = "std")]
use alloc::format;
#[cfg(feature = "std")]
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::cmp::max;
use core::marker::PhantomData;
use core::ops::RangeBounds;
#[cfg(feature = "logging")]
use log::trace;

pub(crate) struct BtreeStats {
    pub(crate) tree_height: u32,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct PagePath {
    path: Vec<PageNumber>,
}

impl PagePath {
    pub(crate) fn new_root(page_number: PageNumber) -> Self {
        Self {
            path: vec![page_number],
        }
    }

    pub(crate) fn with_child(&self, page_number: PageNumber) -> Self {
        let mut path = self.path.clone();
        path.push(page_number);
        Self { path }
    }

    pub(crate) fn with_subpath(&self, other: &Self) -> Self {
        let mut path = self.path.clone();
        path.extend(&other.path);
        Self { path }
    }

    pub(crate) fn parents(&self) -> &[PageNumber] {
        &self.path[..self.path.len() - 1]
    }

    pub(crate) fn page_number(&self) -> PageNumber {
        self.path[self.path.len() - 1]
    }
}

pub(crate) struct UntypedBtree {
    mem: Arc<TransactionalMemory>,
    root: Option<BtreeHeader>,
    key_width: Option<usize>,
    _value_width: Option<usize>,
}

impl UntypedBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        mem: Arc<TransactionalMemory>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            key_width,
            _value_width: value_width,
        }
    }

    // Applies visitor to pages in the tree
    pub(crate) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            self.visit_pages_helper(PagePath::new_root(page_number), &mut visitor)?;
        }

        Ok(())
    }

    fn visit_pages_helper<F>(&self, path: PagePath, visitor: &mut F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        visitor(&path)?;
        let page = self.mem.get_page(path.page_number())?;

        match page.memory()[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    let child_path = path.with_child(child_page);
                    self.visit_pages_helper(child_path, visitor)?;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
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
    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut root = self.root;
        if let Some(BtreeHeader {
            root: ref p,
            ref mut checksum,
            length: _,
        }) = root
        {
            if !self.mem.uncommitted(*p) {
                // root page is clean
                return Ok(root);
            }

            *checksum = self.finalize_dirty_checksums_helper(*p)?;
            self.root = root;
        }

        Ok(root)
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

                let mut mutator = BranchMutator::new(page.memory_mut());
                for (child_index, child_page, child_checksum) in new_children.into_iter().flatten()
                {
                    mutator.write_child_page(child_index, child_page, child_checksum);
                }

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

    pub(crate) fn relocate(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<bool> {
        if let Some(root) = self.get_root()
            && let Some((new_root, new_checksum)) =
                self.relocate_helper(root.root, relocation_map)?
        {
            self.root = Some(BtreeHeader::new(new_root, new_checksum, root.length));
            return Ok(true);
        }
        Ok(false)
    }

    // Relocates the given subtree to the pages specified in relocation_map
    fn relocate_helper(
        &mut self,
        page_number: PageNumber,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        let old_page = self.mem.get_page(page_number)?;
        let mut new_page = if let Some(new_page_number) = relocation_map.get(&page_number) {
            self.mem.get_page_mut(*new_page_number)?
        } else {
            return Ok(None);
        };
        new_page.memory_mut().copy_from_slice(old_page.memory());

        let node_mem = old_page.memory();
        match node_mem[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&old_page, self.key_width);
                let mut mutator = BranchMutator::new(new_page.memory_mut());
                for i in 0..accessor.count_children() {
                    let child = accessor.child_page(i).unwrap();
                    if let Some((new_child, new_checksum)) =
                        self.relocate_helper(child, relocation_map)?
                    {
                        mutator.write_child_page(i, new_child, new_checksum);
                    }
                }
            }
            _ => unreachable!(),
        }

        let mut freed_pages = self.freed_pages.lock();
        // No need to track allocations, because this method is only called during compaction when
        // there can't be any savepoints
        let mut ignore = PageTrackerPolicy::Ignore;
        if !self.mem.free_if_uncommitted(page_number, &mut ignore) {
            freed_pages.push(page_number);
        }

        Ok(Some((new_page.get_page_number(), DEFERRED)))
    }
}

pub(crate) struct BtreeMut<'a, K: Key + 'static, V: Value + 'static> {
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    compression_override: Option<CompressionConfig>,
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
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            mem,
            transaction_guard: guard,
            root,
            freed_pages,
            allocated_pages,
            compression_override: None,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _lifetime: Default::default(),
        }
    }

    /// Create a `BtreeMut` that never compresses values, regardless of database config.
    /// Used for internal metadata trees (table tree, allocator state).
    pub(crate) fn new_uncompressed(
        root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            mem,
            transaction_guard: guard,
            root,
            freed_pages,
            allocated_pages,
            compression_override: Some(CompressionConfig::None),
            _key_type: Default::default(),
            _value_type: Default::default(),
            _lifetime: Default::default(),
        }
    }

    fn compression(&self) -> CompressionConfig {
        self.compression_override
            .unwrap_or_else(|| self.mem.compression())
    }

    fn value_width(&self) -> Option<usize> {
        if self.compression().is_enabled() {
            None
        } else {
            V::fixed_width()
        }
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            self.value_width(),
        );
        self.root = tree.finalize_dirty_checksums()?;
        Ok(self.root)
    }

    #[allow(dead_code)]
    pub(crate) fn all_pages_iter(&self) -> Result<Option<AllPageNumbersBtreeIter>> {
        if let Some(root) = self.root.map(|x| x.root) {
            Ok(Some(AllPageNumbersBtreeIter::new(
                root,
                K::fixed_width(),
                self.value_width(),
                self.mem.clone(),
            )?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        self.read_tree()?.visit_all_pages(visitor)
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn set_root(&mut self, root: Option<BtreeHeader>) {
        self.root = root;
    }

    pub(crate) fn relocate(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<bool> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.mem.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            self.value_width(),
        );
        if tree.relocate(relocation_map)? {
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
    ) -> Result<Option<AccessGuard<'_, V>>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with value of length {}",
            &self.root,
            key,
            V::as_bytes(value).as_ref().len()
        );
        let compression = self.compression();
        let mut freed_pages = self.freed_pages.lock();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
            compression,
        );
        let (old_value, _) = operation.insert(key, value)?;
        Ok(old_value)
    }

    // Insert without allocating or freeing any pages. This requires that you've previously
    // inserted the same key, with a value of at least the same serialized length, earlier
    // in the same transaction. If those preconditions aren't satisfied, insert_inplace()
    // will panic; it won't allocate under any circumstances
    pub(crate) fn insert_inplace(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<()> {
        let compression = self.compression();
        let mut fake_freed_pages = vec![];
        let fake_allocated_pages = Arc::new(Mutex::new(PageTrackerPolicy::Closed));
        let mut operation = MutateHelper::<K, V>::new(
            &mut self.root,
            self.mem.clone(),
            fake_freed_pages.as_mut(),
            fake_allocated_pages,
            compression,
        );
        operation.insert_inplace(key, value)?;
        assert!(fake_freed_pages.is_empty());
        Ok(())
    }

    pub(crate) fn remove(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let compression = self.compression();
        let mut freed_pages = self.freed_pages.lock();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
            compression,
        );
        let result = operation.delete(key)?;
        Ok(result)
    }

    #[allow(dead_code)]
    #[cfg(feature = "std")]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.read_tree()?.print_debug(include_values)
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.get_root().map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            self.value_width(),
        )
    }

    fn read_tree(&self) -> Result<Btree<K, V>> {
        if self.compression_override == Some(CompressionConfig::None) {
            Btree::new_uncompressed(
                self.get_root(),
                PageHint::None,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )
        } else {
            Btree::new(
                self.get_root(),
                PageHint::None,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )
        }
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        self.read_tree()?.get(key)
    }

    pub(crate) fn get_mut(
        &mut self,
        key: &K::SelfType<'_>,
    ) -> Result<Option<AccessGuardMut<'_, V>>> {
        if let Some(ref mut root) = self.root {
            let key_bytes = K::as_bytes(key);
            let query = key_bytes.as_ref();
            let page_mut = if self.mem.uncommitted(root.root) {
                self.mem.get_page_mut(root.root)?
            } else {
                let mut freed_pages = self.freed_pages.lock();
                let mut allocated = self.allocated_pages.lock();
                let required: usize = root
                    .root
                    .page_size_bytes(self.mem.get_page_size().try_into().unwrap())
                    .try_into()
                    .unwrap();
                let mut new_page = self.mem.allocate(required, &mut allocated)?;
                let old_page = self.mem.get_page(root.root)?;
                new_page.memory_mut().copy_from_slice(old_page.memory());
                drop(old_page);
                freed_pages.push(root.root);

                root.root = new_page.get_page_number();
                root.checksum = DEFERRED;
                new_page
            };
            self.get_mut_helper(None, page_mut, query)
        } else {
            Ok(None)
        }
    }

    fn get_mut_helper(
        &mut self,
        parent: Option<(PageMut, usize)>,
        mut page: PageMut,
        query: &[u8],
    ) -> Result<Option<AccessGuardMut<'_, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let value_width = self.value_width();
                let compression = self.compression();
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), value_width);
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuardMut::new(
                        page,
                        start,
                        end - start,
                        entry_index,
                        parent,
                        self.mem.clone(),
                        self.allocated_pages.clone(),
                        self.root.as_mut().unwrap(),
                        K::fixed_width(),
                        value_width,
                        compression,
                    )?;
                    Ok(Some(guard))
                } else {
                    Ok(None)
                }
            }
            BRANCH => {
                let (child_index, child_page) = {
                    let accessor = BranchAccessor::new(&page, K::fixed_width());
                    accessor.child_for_key::<K>(query)
                };
                let child_page_mut = if self.mem.uncommitted(child_page) {
                    self.mem.get_page_mut(child_page)?
                } else {
                    let mut freed_pages = self.freed_pages.lock();
                    let mut allocated = self.allocated_pages.lock();
                    let required: usize = child_page
                        .page_size_bytes(self.mem.get_page_size().try_into().unwrap())
                        .try_into()
                        .unwrap();
                    let mut new_page = self.mem.allocate(required, &mut allocated)?;
                    let old_child_page = self.mem.get_page(child_page)?;
                    new_page
                        .memory_mut()
                        .copy_from_slice(old_child_page.memory());
                    drop(old_child_page);
                    freed_pages.push(child_page);

                    let mut mutator = BranchMutator::new(page.memory_mut());
                    mutator.write_child_page(child_index, new_page.get_page_number(), DEFERRED);
                    new_page
                };
                self.get_mut_helper(Some((page, child_index)), child_page_mut, query)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        self.read_tree()?.first()
    }

    pub(crate) fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        self.read_tree()?.last()
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
        let compression = self.compression();

        let result = BtreeExtractIf::new(
            &mut self.root,
            iter,
            predicate,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            self.mem.clone(),
            compression,
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
        let compression = self.compression();
        // Do not modify the existing tree, because we're iterating over it concurrently with the removals
        // TODO: optimize this to iterate and remove at the same time
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new_do_not_modify(
            &mut self.root,
            self.mem.clone(),
            &mut freed,
            self.allocated_pages.clone(),
            compression,
        );
        for entry in iter {
            let entry = entry?;
            if !predicate(entry.key(), entry.value()) {
                assert!(operation.delete(&entry.key())?.is_some());
            }
        }
        let mut freed_pages = self.freed_pages.lock();
        let mut allocated_pages = self.allocated_pages.lock();
        for page in freed {
            if !self.mem.free_if_uncommitted(page, &mut allocated_pages) {
                freed_pages.push(page);
            }
        }

        Ok(())
    }

    pub(crate) fn len(&self) -> Result<u64> {
        self.read_tree()?.len()
    }
}

impl<'a, K: Key + 'a, V: MutInPlaceValue + 'a> BtreeMut<'a, K, V> {
    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to `value_length`
    // Return type has the same lifetime as &self, because the tree must not be modified until the mutable guard is dropped
    pub(crate) fn insert_reserve(
        &mut self,
        key: &K::SelfType<'_>,
        value_length: usize,
    ) -> Result<AccessGuardMutInPlace<'_, V>> {
        #[cfg(feature = "logging")]
        trace!(
            "Btree(root={:?}): Inserting {:?} with {} reserved bytes for the value",
            &self.root, key, value_length
        );
        let compression = self.compression();
        let mut freed_pages = self.freed_pages.lock();
        let mut value = vec![0u8; value_length];
        V::initialize(&mut value);
        let mut operation = MutateHelper::<K, V>::new(
            &mut self.root,
            self.mem.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
            compression,
        );
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
        Ok(self.root.map_or(0, |x| x.length))
    }

    /// Iterate all entries as raw key/value byte slices.
    pub(crate) fn raw_iter(&self) -> Result<crate::tree_store::btree_iters::RawEntryIter> {
        crate::tree_store::btree_iters::RawEntryIter::new(
            self.root.map(|h| h.root),
            self.fixed_key_size,
            self.fixed_value_size,
            self.mem.clone(),
        )
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

    /// Verifies all page checksums, collecting details for every corrupt page found.
    /// Returns a `(pages_checked, corrupt_pages)` tuple.
    #[cfg(feature = "std")]
    pub(crate) fn verify_checksum_detailed(&self) -> Result<(u64, Vec<CorruptPageInfo>)> {
        let mut pages_checked = 0u64;
        let mut corruptions = Vec::new();
        if let Some(header) = self.root {
            self.verify_checksum_detailed_helper(
                header.root,
                header.checksum,
                &mut pages_checked,
                &mut corruptions,
            )?;
        }
        Ok((pages_checked, corruptions))
    }

    #[cfg(feature = "std")]
    fn verify_checksum_detailed_helper(
        &self,
        page_number: PageNumber,
        expected_checksum: Checksum,
        pages_checked: &mut u64,
        corruptions: &mut Vec<CorruptPageInfo>,
    ) -> Result {
        let page = self.mem.get_page(page_number)?;
        let node_mem = page.memory();
        *pages_checked += 1;

        match node_mem[0] {
            LEAF => {
                let valid = if let Ok(computed) =
                    leaf_checksum(&page, self.fixed_key_size, self.fixed_value_size)
                {
                    expected_checksum == computed
                } else {
                    false
                };
                if !valid {
                    corruptions.push(CorruptPageInfo {
                        page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                        table_name: None,
                        description: "leaf page checksum mismatch".to_string(),
                    });
                }
            }
            BRANCH => {
                let valid = if let Ok(computed) = branch_checksum(&page, self.fixed_key_size) {
                    expected_checksum == computed
                } else {
                    false
                };
                if !valid {
                    corruptions.push(CorruptPageInfo {
                        page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                        table_name: None,
                        description: "branch page checksum mismatch".to_string(),
                    });
                }
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                for i in 0..accessor.count_children() {
                    self.verify_checksum_detailed_helper(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                        pages_checked,
                        corruptions,
                    )?;
                }
            }
            other => {
                corruptions.push(CorruptPageInfo {
                    page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                    table_name: None,
                    description: format!("unknown page type tag: {other}"),
                });
            }
        }

        Ok(())
    }

    /// Verifies B-tree structural invariants:
    /// - Keys in each leaf are sorted (byte-order comparison)
    /// - Branch child pointers reference valid page types (LEAF or BRANCH)
    /// - All paths from root to leaf have the same depth
    ///
    /// Returns corruption details for any violations found.
    #[cfg(feature = "std")]
    pub(crate) fn verify_structure(&self) -> Result<Vec<CorruptPageInfo>> {
        let mut corruptions = Vec::new();
        if let Some(header) = self.root {
            let expected_depth = self.measure_depth(header.root)?;
            self.verify_structure_helper(header.root, 0, expected_depth, &mut corruptions)?;
        }
        Ok(corruptions)
    }

    /// Measures the depth of the leftmost path from root to leaf.
    #[cfg(feature = "std")]
    fn measure_depth(&self, page_number: PageNumber) -> Result<u32> {
        let page = self.mem.get_page(page_number)?;
        let node_mem = page.memory();
        if node_mem[0] == BRANCH {
            let accessor = BranchAccessor::new(&page, self.fixed_key_size);
            if let Some(child) = accessor.child_page(0) {
                return Ok(1 + self.measure_depth(child)?);
            }
        }
        Ok(0)
    }

    #[cfg(feature = "std")]
    fn verify_structure_helper(
        &self,
        page_number: PageNumber,
        current_depth: u32,
        expected_depth: u32,
        corruptions: &mut Vec<CorruptPageInfo>,
    ) -> Result {
        let page = self.mem.get_page(page_number)?;
        let node_mem = page.memory();

        match node_mem[0] {
            LEAF => {
                if current_depth != expected_depth {
                    corruptions.push(CorruptPageInfo {
                        page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                        table_name: None,
                        description: format!(
                            "leaf at depth {current_depth}, expected {expected_depth}"
                        ),
                    });
                }
                let accessor =
                    LeafAccessor::new(node_mem, self.fixed_key_size, self.fixed_value_size);
                let num = accessor.num_pairs();
                for i in 1..num {
                    if let (Some(prev), Some(curr)) = (accessor.entry(i - 1), accessor.entry(i))
                        && prev.key() >= curr.key()
                    {
                        corruptions.push(CorruptPageInfo {
                            page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                            table_name: None,
                            description: format!(
                                "leaf keys not sorted at index {i} (key[{}] >= key[{i}])",
                                i - 1
                            ),
                        });
                        break;
                    }
                }
            }
            BRANCH => {
                if current_depth >= expected_depth {
                    corruptions.push(CorruptPageInfo {
                        page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                        table_name: None,
                        description: format!(
                            "branch at depth {current_depth}, expected leaf at depth {expected_depth}"
                        ),
                    });
                    return Ok(());
                }
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                for i in 0..accessor.count_children() {
                    if let Some(child) = accessor.child_page(i) {
                        self.verify_structure_helper(
                            child,
                            current_depth + 1,
                            expected_depth,
                            corruptions,
                        )?;
                    } else {
                        corruptions.push(CorruptPageInfo {
                            page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                            table_name: None,
                            description: format!("branch child {i} has invalid page pointer"),
                        });
                    }
                }
            }
            other => {
                corruptions.push(CorruptPageInfo {
                    page_number: u64::from_le_bytes(page_number.to_le_bytes()),
                    table_name: None,
                    description: format!("unknown page type tag: {other}"),
                });
            }
        }

        Ok(())
    }
}

pub(crate) struct Btree<K: Key + 'static, V: Value + 'static> {
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    // Cache of the root page to avoid repeated lookups
    cached_root: Option<PageImpl>,
    root: Option<BtreeHeader>,
    hint: PageHint,
    compression_override: Option<CompressionConfig>,
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
            transaction_guard: guard,
            cached_root,
            root,
            hint,
            compression_override: None,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    pub(crate) fn new_uncompressed(
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
            transaction_guard: guard,
            cached_root,
            root,
            hint,
            compression_override: Some(CompressionConfig::None),
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    fn compression(&self) -> CompressionConfig {
        self.compression_override
            .unwrap_or_else(|| self.mem.compression())
    }

    fn value_width(&self) -> Option<usize> {
        if self.compression().is_enabled() {
            None
        } else {
            V::fixed_width()
        }
    }

    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        &self.transaction_guard
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            self.value_width(),
            self.mem.clone(),
        )
        .verify_checksum()
    }

    #[cfg(feature = "std")]
    pub(crate) fn verify_checksum_detailed(&self) -> Result<(u64, Vec<CorruptPageInfo>)> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            self.value_width(),
            self.mem.clone(),
        )
        .verify_checksum_detailed()
    }

    #[cfg(feature = "std")]
    pub(crate) fn verify_structure(&self) -> Result<Vec<CorruptPageInfo>> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            self.value_width(),
            self.mem.clone(),
        )
        .verify_structure()
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        let tree = UntypedBtree::new(
            self.root,
            self.mem.clone(),
            K::fixed_width(),
            self.value_width(),
        );
        tree.visit_all_pages(visitor)
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
                let accessor =
                    LeafAccessor::new(page.memory(), K::fixed_width(), self.value_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = if self.compression().is_enabled() {
                        AccessGuard::with_page_decompress(page, start..end)?
                    } else {
                        AccessGuard::with_page(page, start..end)
                    };
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

    pub(crate) fn first(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(ref root) = self.cached_root {
            self.first_helper(root.clone())
        } else {
            Ok(None)
        }
    }

    fn first_helper(
        &self,
        page: PageImpl,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor =
                    LeafAccessor::new(page.memory(), K::fixed_width(), self.value_width());
                let (key_range, value_range) = accessor.entry_ranges(0).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = if self.compression().is_enabled() {
                    AccessGuard::with_page_decompress(page, value_range)?
                } else {
                    AccessGuard::with_page(page, value_range)
                };
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(0).unwrap();
                self.first_helper(self.mem.get_page_extended(child_page, self.hint)?)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn last(
        &self,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        if let Some(ref root) = self.cached_root {
            self.last_helper(root.clone())
        } else {
            Ok(None)
        }
    }

    fn last_helper(
        &self,
        page: PageImpl,
    ) -> Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor =
                    LeafAccessor::new(page.memory(), K::fixed_width(), self.value_width());
                let (key_range, value_range) =
                    accessor.entry_ranges(accessor.num_pairs() - 1).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = if self.compression().is_enabled() {
                    AccessGuard::with_page_decompress(page, value_range)?
                } else {
                    AccessGuard::with_page(page, value_range)
                };
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(accessor.count_children() - 1).unwrap();
                self.last_helper(self.mem.get_page_extended(child_page, self.hint)?)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a0>>>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeRangeIter<K, V>> {
        BtreeRangeIter::new(
            range,
            self.root.map(|x| x.root),
            self.value_width(),
            self.mem.clone(),
            self.compression().is_enabled(),
        )
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map_or(0, |x| x.length))
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.root.map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            self.value_width(),
        )
    }

    #[allow(dead_code)]
    #[cfg(feature = "std")]
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
                            LeafAccessor::new(page.memory(), K::fixed_width(), self.value_width())
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

impl<K: Key, V: Value> Drop for Btree<K, V> {
    fn drop(&mut self) {
        // Make sure that we clear our reference to the root page, before the transaction guard goes out of scope
        self.cached_root = None;
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
