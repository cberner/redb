use crate::db::TransactionGuard;
use crate::tree_store::btree_base::{
    AccessGuardMut, BRANCH, BranchAccessor, BranchMutator, BtreeHeader, Checksum, DEFERRED, LEAF,
    LeafAccessor, LeafPageMut, branch_checksum, leaf_checksum,
};
use crate::tree_store::btree_cursor::{CursorMut, Position};
use crate::tree_store::btree_iters::range_is_empty;
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::page_store::{Page, PageImpl, PageMut};
use crate::tree_store::{
    AccessGuardMutInPlace, AllPageNumbersBtreeIter, BtreeCursorRange, BtreeExtractIf,
    PageAllocator, PageHint, PageNumber, PageResolver, PageTrackerPolicy,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, Result};
#[cfg(feature = "logging")]
use log::trace;
use std::borrow::Borrow;
use std::cmp::max;
use std::collections::{Bound, HashMap};
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

pub(super) struct UntypedBtree {
    mem: PageResolver,
    root: Option<BtreeHeader>,
    hint: PageHint,
    key_width: Option<usize>,
    _value_width: Option<usize>,
}

impl UntypedBtree {
    pub(super) fn new(
        root: Option<BtreeHeader>,
        mem: PageResolver,
        hint: PageHint,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            mem,
            root,
            hint,
            key_width,
            _value_width: value_width,
        }
    }

    // Applies visitor to pages in the tree
    pub(super) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
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
        let page = self.mem.get_page(path.page_number(), self.hint)?;

        match page.memory()[0] {
            LEAF => {
                // No-op
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if path.parents().contains(&child_page) || path.page_number() == child_page {
                        return Err(crate::StorageError::Corrupted(
                            "Cycle detected in Btree pages".to_string(),
                        ));
                    }
                    let child_path = path.with_child(child_page);
                    self.visit_pages_helper(child_path, visitor)?;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

pub(super) struct UntypedBtreeMut {
    page_allocator: PageAllocator,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    key_width: Option<usize>,
    value_width: Option<usize>,
}

impl UntypedBtreeMut {
    pub(super) fn new(
        root: Option<BtreeHeader>,
        page_allocator: PageAllocator,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        key_width: Option<usize>,
        value_width: Option<usize>,
    ) -> Self {
        Self {
            page_allocator,
            root,
            freed_pages,
            key_width,
            value_width,
        }
    }

    pub(super) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    // Recomputes the checksum for all pages that are uncommitted
    pub(super) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut root = self.root;
        if let Some(BtreeHeader {
            root: ref p,
            ref mut checksum,
            length: _,
        }) = root
        {
            if !self.page_allocator.uncommitted(*p) {
                // root page is clean
                return Ok(root);
            }

            *checksum = self.finalize_dirty_checksums_helper(*p)?;
            self.root = root;
        }

        Ok(root)
    }

    fn finalize_dirty_checksums_helper(&mut self, page_number: PageNumber) -> Result<Checksum> {
        assert!(self.page_allocator.uncommitted(page_number));
        let mut page = self.page_allocator.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => leaf_checksum(&page, self.key_width, self.value_width),
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                let mut new_children = vec![];
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.page_allocator.uncommitted(child_page) {
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
    pub(super) fn dirty_leaf_visitor<F>(&mut self, visitor: F) -> Result
    where
        F: for<'a> Fn(LeafPageMut<'a>) -> Result,
    {
        if let Some(page_number) = self.root.map(|x| x.root) {
            if !self.page_allocator.uncommitted(page_number) {
                // root page is clean
                return Ok(());
            }

            let page = self.page_allocator.get_page_mut(page_number)?;
            match page.memory()[0] {
                LEAF => {
                    visitor(LeafPageMut::new(page, self.key_width, self.value_width))?;
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
        F: for<'a> Fn(LeafPageMut<'a>) -> Result,
    {
        assert!(self.page_allocator.uncommitted(page_number));
        let page = self.page_allocator.get_page_mut(page_number)?;

        match page.memory()[0] {
            LEAF => {
                visitor(LeafPageMut::new(page, self.key_width, self.value_width))?;
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.key_width);
                for i in 0..accessor.count_children() {
                    let child_page = accessor.child_page(i).unwrap();
                    if self.page_allocator.uncommitted(child_page) {
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
        let old_page = self.page_allocator.get_page(page_number, PageHint::None)?;
        let mut new_page = if let Some(new_page_number) = relocation_map.get(&page_number) {
            self.page_allocator.get_page_mut(*new_page_number)?
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

        let mut freed_pages = self.freed_pages.lock().unwrap();
        // No need to track allocations, because this method is only called during compaction when
        // there can't be any savepoints
        let mut ignore = PageTrackerPolicy::Ignore;
        if !self
            .page_allocator
            .free_if_uncommitted(page_number, &mut ignore)
        {
            freed_pages.push(page_number);
        }

        Ok(Some((new_page.get_page_number(), DEFERRED)))
    }
}

pub(crate) struct BtreeMut<K: Key + 'static, V: Value + 'static> {
    page_allocator: PageAllocator,
    transaction_guard: Arc<TransactionGuard>,
    root: Option<BtreeHeader>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> BtreeMut<K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        page_allocator: PageAllocator,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            page_allocator,
            transaction_guard: guard,
            root,
            freed_pages,
            allocated_pages,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        let mut tree = UntypedBtreeMut::new(
            self.get_root(),
            self.page_allocator.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
        );
        self.root = tree.finalize_dirty_checksums()?;
        Ok(self.root)
    }

    #[allow(dead_code)]
    pub(crate) fn all_pages_iter(&self) -> Option<AllPageNumbersBtreeIter> {
        self.root.map(|x| {
            AllPageNumbersBtreeIter::new(
                x.root,
                K::fixed_width(),
                self.page_allocator.resolver(),
                PageHint::None,
            )
        })
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
            self.page_allocator.clone(),
            self.freed_pages.clone(),
            K::fixed_width(),
            V::fixed_width(),
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
        // The cursor updates the root incrementally, so pages queued before an
        // error still need to be recorded for commit.
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut self.root,
            self.page_allocator.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
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
        let mut fake_freed_pages = vec![];
        let fake_allocated_pages = Arc::new(Mutex::new(PageTrackerPolicy::Closed));
        let mut operation = MutateHelper::<K, V>::new(
            &mut self.root,
            self.page_allocator.clone(),
            fake_freed_pages.as_mut(),
            fake_allocated_pages,
        );
        operation.insert_inplace(key, value)?;
        assert!(fake_freed_pages.is_empty());
        Ok(())
    }

    // Reserves `key` (inserting `placeholder` if absent, preserving its value otherwise)
    // and CoWs the path to it, so a subsequent insert_inplace() with a value of
    // equal-or-smaller serialized size won't allocate or free pages. Returns the
    // serialized bytes currently stored at `key`.
    pub(crate) fn force_uncommitted(
        &mut self,
        key: &K::SelfType<'_>,
        placeholder: &V::SelfType<'_>,
    ) -> Result<Vec<u8>> {
        let existing_bytes = self
            .get(key)?
            .map(|guard| V::as_bytes(&guard.value()).as_ref().to_vec());
        let bytes = existing_bytes.unwrap_or_else(|| V::as_bytes(placeholder).as_ref().to_vec());
        {
            let value = V::from_bytes(&bytes);
            self.insert(key, &value)?;
        }
        Ok(bytes)
    }

    pub(crate) fn remove(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'_, V>>> {
        #[cfg(feature = "logging")]
        trace!("Btree(root={:?}): Deleting {:?}", &self.root, key);
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new(
            &mut self.root,
            self.page_allocator.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
        );
        let result = operation.delete(key)?;
        Ok(result)
    }

    // Removes and returns the leftmost entry in the tree, if any, in a single tree descent.
    pub(crate) fn pop_first(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut cursor: CursorMut<'_, '_, K, V> = CursorMut::new(
            &mut self.root,
            &self.page_allocator,
            freed_pages.as_mut(),
            &self.allocated_pages,
        );
        cursor.seek_to(Position::Start)?;
        cursor.remove_next()
    }

    // Removes and returns the rightmost entry in the tree, if any, in a single tree descent.
    pub(crate) fn pop_last(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut cursor: CursorMut<'_, '_, K, V> = CursorMut::new(
            &mut self.root,
            &self.page_allocator,
            freed_pages.as_mut(),
            &self.allocated_pages,
        );
        cursor.seek_to(Position::End)?;
        cursor.remove_prev()
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.read_tree()?.print_debug(include_values)
    }

    pub(crate) fn stats(&self) -> Result<BtreeStats> {
        btree_stats(
            self.get_root().map(|x| x.root),
            &self.page_allocator.resolver(),
            K::fixed_width(),
            V::fixed_width(),
            PageHint::None,
        )
    }

    fn read_tree(&self) -> Result<Btree<K, V>> {
        Btree::new(
            self.get_root(),
            PageHint::None,
            self.transaction_guard.clone(),
            self.page_allocator.resolver(),
        )
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
            let page_mut = if self.page_allocator.uncommitted(root.root) {
                self.page_allocator.get_page_mut(root.root)?
            } else {
                let mut freed_pages = self.freed_pages.lock().unwrap();
                let mut allocated = self.allocated_pages.lock().unwrap();
                let required: usize = root
                    .root
                    .page_size_bytes(self.page_allocator.get_page_size().try_into().unwrap())
                    .try_into()
                    .unwrap();
                let mut new_page = self.page_allocator.allocate(required, &mut allocated)?;
                let old_page = self.page_allocator.get_page(root.root, PageHint::None)?;
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

    fn get_mut_helper<'txn>(
        &'txn mut self,
        parent: Option<(PageMut<'txn>, usize)>,
        mut page: PageMut<'txn>,
        query: &[u8],
    ) -> Result<Option<AccessGuardMut<'txn, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuardMut::new(
                        page,
                        start,
                        end - start,
                        entry_index,
                        parent,
                        self.page_allocator.clone(),
                        self.allocated_pages.clone(),
                        self.root.as_mut().unwrap(),
                        K::fixed_width(),
                    );
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
                let child_page_mut = if self.page_allocator.uncommitted(child_page) {
                    self.page_allocator.get_page_mut(child_page)?
                } else {
                    let mut freed_pages = self.freed_pages.lock().unwrap();
                    let mut allocated = self.allocated_pages.lock().unwrap();
                    let required: usize = child_page
                        .page_size_bytes(self.page_allocator.get_page_size().try_into().unwrap())
                        .try_into()
                        .unwrap();
                    let mut new_page = self.page_allocator.allocate(required, &mut allocated)?;
                    let old_child_page =
                        self.page_allocator.get_page(child_page, PageHint::None)?;
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
    ) -> Result<BtreeCursorRange<K, V>>
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
        let lower_bound = range
            .start_bound()
            .map(|key| K::as_bytes(key.borrow()).as_ref().to_vec());
        let upper_bound = range
            .end_bound()
            .map(|key| K::as_bytes(key.borrow()).as_ref().to_vec());

        let result = BtreeExtractIf::new(
            &mut self.root,
            lower_bound,
            upper_bound,
            predicate,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            self.page_allocator.clone(),
        );

        Ok(result)
    }

    // Sets `poisoned` if an error left entries in the tree that the
    // predicate had already rejected; the caller must then poison the
    // transaction so they cannot be committed.
    pub(crate) fn retain_in<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        mut predicate: F,
        range: impl RangeBounds<KR> + 'a,
        poisoned: &mut bool,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        if self.root.is_none() || range_is_empty::<K, KR, _>(&range) {
            return Ok(());
        }

        let lower_bound = range
            .start_bound()
            .map(|key| K::as_bytes(key.borrow()).as_ref().to_vec());
        let upper_bound = range
            .end_bound()
            .map(|key| K::as_bytes(key.borrow()).as_ref().to_vec());
        let mut freed = vec![];
        let result = self.retain_in_helper(
            lower_bound.as_ref().map(Vec::as_slice),
            upper_bound.as_ref().map(Vec::as_slice),
            &mut predicate,
            &mut freed,
            poisoned,
        );

        // The cursor updates the root incrementally, so pages queued before an
        // error still need to be recorded for commit.
        let mut freed_pages = self.freed_pages.lock().unwrap();
        freed_pages.extend(freed);

        result
    }

    fn retain_in_helper<F>(
        &mut self,
        lower_bound: Bound<&[u8]>,
        upper_bound: Bound<&[u8]>,
        predicate: &mut F,
        freed: &mut Vec<PageNumber>,
        poisoned: &mut bool,
    ) -> Result
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        let mut cursor: CursorMut<'_, '_, K, V> = CursorMut::new(
            &mut self.root,
            &self.page_allocator,
            freed,
            &self.allocated_pages,
        );
        let result = Self::retain_scan(&mut cursor, lower_bound, upper_bound, predicate);
        if result.is_err() {
            // Best effort: apply any pending removals so the failure does not
            // silently drop entries the predicate already rejected. If the
            // cursor is poisoned this re-raises without touching the tree.
            let _ = cursor.finish_pending_removals();
        }
        *poisoned = cursor.poisoned();
        result
    }

    fn retain_scan<F>(
        cursor: &mut CursorMut<'_, '_, K, V>,
        lower_bound: Bound<&[u8]>,
        upper_bound: Bound<&[u8]>,
        predicate: &mut F,
    ) -> Result
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        match lower_bound {
            Bound::Included(key) => cursor.seek_to(Position::Before(key))?,
            Bound::Excluded(key) => cursor.seek_to(Position::After(key))?,
            Bound::Unbounded => cursor.seek_to(Position::Start)?,
        }
        while let Some(entry) = cursor.peek_next()? {
            if !Self::before_upper_bound(upper_bound, entry.key_bytes()) {
                break;
            }
            if predicate(entry.key(), entry.value()) {
                cursor.next()?;
            } else {
                cursor.remove_next_discard()?;
            }
        }
        cursor.finish_pending_removals()
    }

    fn before_upper_bound(bound: Bound<&[u8]>, key: &[u8]) -> bool {
        match bound {
            Bound::Included(bound) => K::compare(key, bound).is_le(),
            Bound::Excluded(bound) => K::compare(key, bound).is_lt(),
            Bound::Unbounded => true,
        }
    }

    pub(crate) fn len(&self) -> Result<u64> {
        self.read_tree()?.len()
    }
}

impl<K: Key + 'static, V: MutInPlaceValue + 'static> BtreeMut<K, V> {
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
        let mut freed_pages = self.freed_pages.lock().unwrap();
        let mut value = vec![0u8; value_length];
        V::initialize(&mut value);
        let mut operation = MutateHelper::<K, V>::new(
            &mut self.root,
            self.page_allocator.clone(),
            freed_pages.as_mut(),
            self.allocated_pages.clone(),
        );
        let (_, guard) = operation.insert(key, &V::from_bytes(&value))?;
        Ok(guard)
    }
}

pub(crate) struct RawBtree {
    mem: PageResolver,
    root: Option<BtreeHeader>,
    hint: PageHint,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl RawBtree {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: PageResolver,
        hint: PageHint,
    ) -> Self {
        Self {
            mem,
            root,
            hint,
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
            self.hint,
        )
    }

    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.root.map_or(0, |x| x.length))
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        if let Some(header) = self.root {
            let mut visited = Vec::new();
            self.verify_checksum_helper(header.root, header.checksum, &mut visited)
        } else {
            Ok(true)
        }
    }

    fn verify_checksum_helper(
        &self,
        page_number: PageNumber,
        expected_checksum: Checksum,
        visited: &mut Vec<PageNumber>,
    ) -> Result<bool> {
        if visited.contains(&page_number) {
            return Ok(false);
        }
        visited.push(page_number);

        let page = self.mem.get_page(page_number, self.hint)?;
        let node_mem = page.memory();
        let result = Ok(match node_mem[0] {
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
                        visited,
                    )? {
                        return Ok(false);
                    }
                }
                true
            }
            _ => false,
        });

        visited.pop();
        result
    }
}

pub(crate) struct Btree<K: Key + 'static, V: Value + 'static> {
    mem: PageResolver,
    transaction_guard: Arc<TransactionGuard>,
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
        mem: PageResolver,
    ) -> Result<Self> {
        let cached_root = if let Some(header) = root {
            Some(mem.get_page(header.root, hint)?)
        } else {
            None
        };
        Ok(Self {
            mem,
            transaction_guard: guard,
            cached_root,
            root,
            hint,
            _key_type: PhantomData,
            _value_type: PhantomData,
        })
    }

    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        &self.transaction_guard
    }

    pub(crate) fn hint(&self) -> PageHint {
        self.hint
    }

    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.root
    }

    pub(crate) fn verify_checksum(&self) -> Result<bool> {
        RawBtree::new(
            self.get_root(),
            K::fixed_width(),
            V::fixed_width(),
            self.mem.clone(),
            self.hint,
        )
        .verify_checksum()
    }

    pub(crate) fn visit_all_pages<F>(&self, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        let tree = UntypedBtree::new(
            self.root,
            self.mem.clone(),
            self.hint,
            K::fixed_width(),
            V::fixed_width(),
        );
        tree.visit_all_pages(visitor)
    }

    pub(crate) fn get(&self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'static, V>>> {
        if let Some(ref root_page) = self.cached_root {
            self.get_helper(root_page, K::as_bytes(key).as_ref())
        } else {
            Ok(None)
        }
    }

    // Returns the value for the queried key, if present
    fn get_helper(&self, page: &PageImpl, query: &[u8]) -> Result<Option<AccessGuard<'static, V>>> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                if let Some(entry_index) = accessor.find_key::<K>(query) {
                    let (start, end) = accessor.value_range(entry_index).unwrap();
                    let guard = AccessGuard::with_page(page.clone(), start..end);
                    Ok(Some(guard))
                } else {
                    Ok(None)
                }
            }
            BRANCH => {
                let accessor = BranchAccessor::new(page, K::fixed_width());
                let (_, child_page) = accessor.child_for_key::<K>(query);
                let child_page = self.mem.get_page(child_page, self.hint)?;
                self.get_helper(&child_page, query)
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
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (key_range, value_range) = accessor.entry_ranges(0).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = AccessGuard::with_page(page, value_range);
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(0).unwrap();
                self.first_helper(self.mem.get_page(child_page, self.hint)?)
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
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (key_range, value_range) =
                    accessor.entry_ranges(accessor.num_pairs() - 1).unwrap();
                let key_guard = AccessGuard::with_page(page.clone(), key_range);
                let value_guard = AccessGuard::with_page(page, value_range);
                Ok(Some((key_guard, value_guard)))
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_page = accessor.child_page(accessor.count_children() - 1).unwrap();
                self.last_helper(self.mem.get_page(child_page, self.hint)?)
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn range<'a0, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a0>>>(
        &self,
        range: &'_ T,
    ) -> Result<BtreeCursorRange<K, V>> {
        BtreeCursorRange::new(
            range,
            self.root.map(|x| x.root),
            self.mem.clone(),
            self.hint,
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
            V::fixed_width(),
            self.hint,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        if let Some(p) = self.root.map(|x| x.root) {
            let mut pages = vec![self.mem.get_page(p, self.hint)?];
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
                                next_children.push(self.mem.get_page(child, self.hint)?);
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

pub(super) fn btree_stats(
    root: Option<PageNumber>,
    mem: &PageResolver,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    hint: PageHint,
) -> Result<BtreeStats> {
    if let Some(root) = root {
        stats_helper(root, mem, fixed_key_size, fixed_value_size, hint)
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
    mem: &PageResolver,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    hint: PageHint,
) -> Result<BtreeStats> {
    let page = mem.get_page(page_number, hint)?;
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
                    let stats = stats_helper(child, mem, fixed_key_size, fixed_value_size, hint)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_path_cycle_detection() {
        let p1 = PageNumber {
            region: 0,
            page_index: 1,
            page_order: 0,
        };
        let p2 = PageNumber {
            region: 0,
            page_index: 2,
            page_order: 0,
        };
        let p3 = PageNumber {
            region: 0,
            page_index: 3,
            page_order: 0,
        };

        let path = PagePath::new_root(p1);
        // No cycle yet: children p2 and p3 are clean
        assert!(!(path.parents().contains(&p2) || path.page_number() == p2));

        let path = path.with_child(p2);
        assert!(!(path.parents().contains(&p3) || path.page_number() == p3));

        // Self-loop on current page (p2)
        assert!(path.parents().contains(&p2) || path.page_number() == p2);

        // Loop back to ancestor (p1)
        assert!(path.parents().contains(&p1) || path.page_number() == p1);
    }

    #[test]
    fn test_verify_checksum_cycle_detection() {
        let p1 = PageNumber {
            region: 0,
            page_index: 1,
            page_order: 0,
        };
        let p2 = PageNumber {
            region: 0,
            page_index: 2,
            page_order: 0,
        };

        let visited = [p1, p2];

        // p1 and p2 are in the active path, so they should be detected as cycles
        assert!(visited.contains(&p1));
        assert!(visited.contains(&p2));

        // p3 is not in the active path, so no cycle
        let p3 = PageNumber {
            region: 0,
            page_index: 3,
            page_order: 0,
        };
        assert!(!visited.contains(&p3));
    }

    #[test]
    fn test_cycle_detection_in_btree() {
        use crate::tree_store::btree_base::RawBranchBuilder;
        use crate::tree_store::{
            AllocationPolicy, InMemoryBackend, PAGE_SIZE, TransactionalMemory,
        };

        let mem = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();
        let mem = Arc::new(mem);
        let page_allocator = PageAllocator::new(mem.clone(), AllocationPolicy::Default);
        let allocated_pages = Mutex::new(PageTrackerPolicy::new_tracking());
        let mut allocated_guard = allocated_pages.lock().unwrap();

        let mut p1_mut = page_allocator
            .allocate(PAGE_SIZE, &mut allocated_guard)
            .unwrap();
        let mut p2_mut = page_allocator
            .allocate(PAGE_SIZE, &mut allocated_guard)
            .unwrap();

        let p1 = p1_mut.get_page_number();
        let p2 = p2_mut.get_page_number();

        // Write p2 first, pointing to p1 with a dummy checksum (e.g. 0)
        {
            let mut builder2 = RawBranchBuilder::new(p2_mut.memory_mut(), 1, Some(8));
            builder2.write_first_page(p1, 0);
            builder2.write_nth_key(b"key12345", p1, 0, 0);
        }

        // Compute checksum of p2
        let p2_checksum = branch_checksum(&p2_mut, Some(8)).unwrap();

        // Write p1 pointing to p2 with p2_checksum
        {
            let mut builder1 = RawBranchBuilder::new(p1_mut.memory_mut(), 1, Some(8));
            builder1.write_first_page(p2, p2_checksum);
            builder1.write_nth_key(b"key12345", p2, p2_checksum, 0);
        }

        // Compute checksum of p1
        let p1_checksum = branch_checksum(&p1_mut, Some(8)).unwrap();

        drop(p1_mut);
        drop(p2_mut);
        drop(allocated_guard);

        // Construct the UntypedBtree and RawBtree
        let header = BtreeHeader {
            root: p1,
            checksum: p1_checksum,
            length: 1,
        };

        let resolver = PageResolver::new(mem);
        let raw_btree = RawBtree::new(
            Some(header),
            Some(8),
            Some(8),
            resolver.clone(),
            PageHint::None,
        );
        let untyped_btree =
            UntypedBtree::new(Some(header), resolver, PageHint::None, Some(8), Some(8));

        // Test RawBtree verify_checksum cycle detection: should return Ok(false) due to cycle
        let verify_res = raw_btree.verify_checksum();
        assert!(verify_res.is_ok());
        assert!(!verify_res.unwrap());

        // Test UntypedBtree visit_all_pages cycle detection: should return Err containing "Cycle detected"
        let visit_res = untyped_btree.visit_all_pages(|_| Ok(()));
        assert!(visit_res.is_err());
        let err_str = format!("{:?}", visit_res.err().unwrap());
        assert!(
            err_str.contains("Cycle detected in Btree pages"),
            "Expected cycle error, got: {err_str}"
        );
    }
}
