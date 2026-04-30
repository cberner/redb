use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, BranchBuilder, BranchMutator, Checksum, DEFERRED, LEAF, LeafAccessor,
    LeafBuilder, LeafMutator, RawBranchBuilder, RawLeafBuilder,
};
use crate::tree_store::btree_mutator::DeletionResult::{
    DeletedBranch, DeletedLeaf, PartialBranch, PartialLeaf, Subtree,
};
use crate::tree_store::page_store::{Page, PageImpl, PageMut};
use crate::tree_store::{
    AccessGuardMutInPlace, BtreeHeader, PageAllocator, PageHint, PageNumber, PageTrackerPolicy,
};
use crate::types::{Key, Value};
use crate::{AccessGuard, Result};
use std::borrow::Borrow;
use std::cmp::{Ordering, max, min};
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

// Describes which entry to delete. `Key` navigates via key comparison; `First`
// and `Last` navigate to the leftmost or rightmost entry, and also cause the
// deleted key bytes to be captured so the caller can return them alongside the
// value (used by pop_first / pop_last).
#[derive(Copy, Clone)]
enum DeleteTarget<'a> {
    Key(&'a [u8]),
    First,
    Last,
}

#[derive(Debug)]
enum DeletionResult {
    // A proper subtree
    Subtree(PageNumber),
    // A leaf with zero children
    DeletedLeaf,
    // A leaf with fewer entries than desired
    PartialLeaf {
        page: Arc<[u8]>,
        deleted_pair: usize,
    },
    // A branch page subtree with fewer children than desired.
    // Held in unbuilt form: the caller will merge it with a sibling and build a new page,
    // so allocating a page here just to free it again would be wasteful.
    // Checksums are retained because preserved children may be clean pages whose real
    // checksums must be propagated (finalize only recomputes uncommitted pages).
    PartialBranch {
        children: Vec<(PageNumber, Checksum)>,
        keys: Vec<Vec<u8>>,
    },
    // Indicates that the branch node was deleted, and includes the only remaining child.
    // Checksum is retained for the same reason as `PartialBranch`.
    DeletedBranch(PageNumber, Checksum),
}

struct RetainNode {
    page: PageNumber,
    checksum: Checksum,
    // Upper bound for this subtree. Parent branch separators are rebuilt from
    // it. Only the final subtree in an ordered list may use None; separator
    // keys on older branch pages can be stale upper bounds, so retain must not
    // rely on this being the exact maximum key unless the node was just rebuilt.
    upper_key: Option<Vec<u8>>,
}

#[derive(Copy, Clone, Debug)]
enum RetainMergeSide {
    Left,
    Right,
}

#[derive(Copy, Clone, Debug)]
struct RetainPartial {
    // An underfilled rebuilt node only retries adjacent sides that have not
    // already been repacked with this node. This prevents normalization from
    // cycling on variable-sized entries that cannot be balanced further.
    try_left: bool,
    try_right: bool,
}

#[derive(Copy, Clone)]
struct RetainMergeContext {
    left_partial: Option<RetainPartial>,
    right_partial: Option<RetainPartial>,
}

impl RetainPartial {
    fn new() -> Self {
        Self {
            try_left: true,
            try_right: true,
        }
    }

    fn after_trying(mut self, side: RetainMergeSide) -> Self {
        match side {
            RetainMergeSide::Left => self.try_left = false,
            RetainMergeSide::Right => self.try_right = false,
        }
        self
    }

    fn left_only(mut self) -> Option<Self> {
        self.try_right = false;
        self.try_left.then_some(self)
    }

    fn right_only(mut self) -> Option<Self> {
        self.try_left = false;
        self.try_right.then_some(self)
    }
}

impl RetainMergeContext {
    fn new(left: &RetainSubtree, right: &RetainSubtree) -> Self {
        Self {
            left_partial: left.partial,
            right_partial: right.partial,
        }
    }
}

// A sealed B-tree subtree, annotated with its distance from the original retain
// walk root. This lets retain stitch unchanged boundary subtrees without
// measuring their distance to leaves.
struct RetainSubtree {
    node: RetainNode,
    depth: u32,
    // Some if this page is below the merge threshold and still has an adjacent
    // side worth trying.
    partial: Option<RetainPartial>,
}

struct RetainSubtreeBuilder {
    // In-progress left-to-right replacement run. Entries are sealed subtrees;
    // the builder owns deciding which adjacent pair must be grafted or merged
    // before the run can be used as branch children or a root replacement.
    subtrees: Vec<RetainSubtree>,
}

impl RetainSubtreeBuilder {
    fn new() -> Self {
        Self { subtrees: vec![] }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            subtrees: Vec::with_capacity(capacity),
        }
    }

    fn from_subtrees(subtrees: Vec<RetainSubtree>) -> Self {
        Self { subtrees }
    }

    fn is_empty(&self) -> bool {
        self.subtrees.is_empty()
    }

    fn len(&self) -> usize {
        self.subtrees.len()
    }

    fn push(&mut self, subtree: RetainSubtree) {
        self.subtrees.push(subtree);
    }

    fn extend(&mut self, subtrees: impl IntoIterator<Item = RetainSubtree>) {
        for subtree in subtrees {
            self.push(subtree);
        }
    }

    fn debug_state(&self) -> Vec<(u32, Option<RetainPartial>)> {
        self.subtrees
            .iter()
            .map(|subtree| (subtree.depth, subtree.partial))
            .collect()
    }

    fn first_pair_needing_merge(&mut self) -> Option<usize> {
        loop {
            if self.subtrees.len() <= 1 {
                return None;
            }

            if let Some(index) = self
                .subtrees
                .windows(2)
                .position(|pair| pair[0].depth != pair[1].depth)
            {
                return Some(index);
            }

            let index = self
                .subtrees
                .iter()
                .position(|subtree| subtree.partial.is_some())?;

            let partial = self.subtrees[index].partial.unwrap();
            if partial.try_right && index + 1 < self.subtrees.len() {
                return Some(index);
            }
            if partial.try_left && index > 0 {
                return Some(index - 1);
            }
            self.subtrees[index].partial = None;
        }
    }

    fn remove_pair(&mut self, index: usize) -> (RetainSubtree, RetainSubtree) {
        let left = self.subtrees.remove(index);
        let right = self.subtrees.remove(index);
        (left, right)
    }

    fn splice(&mut self, index: usize, replacement: Vec<RetainSubtree>) {
        self.subtrees.splice(index..index, replacement);
    }

    fn into_vec(self) -> Vec<RetainSubtree> {
        self.subtrees
    }
}

struct RetainResult {
    // Some means retain left the walked subtree untouched. None means retain
    // rewrote or removed it, and `replacement` holds the new ordered subtrees.
    unchanged: Option<RetainNode>,
    replacement: RetainSubtreeBuilder,
}

impl RetainResult {
    fn unchanged(node: RetainNode) -> Self {
        Self {
            unchanged: Some(node),
            replacement: RetainSubtreeBuilder::new(),
        }
    }

    fn changed(replacement: Vec<RetainSubtree>) -> Self {
        Self {
            unchanged: None,
            replacement: RetainSubtreeBuilder::from_subtrees(replacement),
        }
    }

    fn is_changed(&self) -> bool {
        self.unchanged.is_none()
    }

    fn into_subtrees(self, depth: u32) -> Vec<RetainSubtree> {
        if let Some(node) = self.unchanged {
            vec![RetainSubtree {
                node,
                depth,
                partial: None,
            }]
        } else {
            self.replacement.into_vec()
        }
    }

    fn into_changed_subtrees(self) -> Vec<RetainSubtree> {
        debug_assert!(self.unchanged.is_none());
        self.replacement.into_vec()
    }
}

struct RetainWalkContext {
    checksum: Checksum,
    upper_key: Option<Vec<u8>>,
    depth: u32,
}

struct KeyRange<'a, K: Key + ?Sized> {
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,
    _key: PhantomData<K>,
}

impl<K: Key + ?Sized> Copy for KeyRange<'_, K> {}

impl<K: Key + ?Sized> Clone for KeyRange<'_, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: Key + ?Sized> KeyRange<'a, K> {
    fn new(start: Bound<&'a [u8]>, end: Bound<&'a [u8]>) -> Self {
        Self {
            start,
            end,
            _key: PhantomData,
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        !self.less_than_start(key) && !self.greater_than_end(key)
    }

    fn less_than_start(&self, key: &[u8]) -> bool {
        match self.start {
            Bound::Included(start) => K::compare(key, start) == Ordering::Less,
            Bound::Excluded(start) => K::compare(key, start) != Ordering::Greater,
            Bound::Unbounded => false,
        }
    }

    fn greater_than_end(&self, key: &[u8]) -> bool {
        match self.end {
            Bound::Included(end) => K::compare(key, end) == Ordering::Greater,
            Bound::Excluded(end) => K::compare(key, end) != Ordering::Less,
            Bound::Unbounded => false,
        }
    }

    fn child_lower_bound_is_past_end(&self, lower_bound: &[u8]) -> bool {
        match self.end {
            Bound::Included(end) | Bound::Excluded(end) => {
                K::compare(lower_bound, end) != Ordering::Less
            }
            Bound::Unbounded => false,
        }
    }
}

struct InsertionResult<'a, V: Value + 'static> {
    // the new root page
    new_root: PageNumber,
    // checksum of the root page
    root_checksum: Checksum,
    // Following sibling, if the root had to be split
    additional_sibling: Option<(Vec<u8>, PageNumber, Checksum)>,
    // The inserted value for .insert_reserve() to use
    inserted_value: AccessGuardMutInPlace<'a, V>,
    // The previous value, if any
    old_value: Option<AccessGuard<'a, V>>,
}

pub(crate) struct MutateHelper<'a, 'b, K: Key, V: Value> {
    root: &'b mut Option<BtreeHeader>,
    modify_uncommitted: bool,
    page_allocator: PageAllocator,
    freed: &'b mut Vec<PageNumber>,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, 'b, K: Key + 'static, V: Value + 'static> MutateHelper<'a, 'b, K, V> {
    pub(crate) fn new(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            root,
            modify_uncommitted: true,
            page_allocator,
            freed,
            allocated,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    // Creates a new mutator which will not modify any existing uncommitted pages, or free any existing pages.
    // It will still queue pages for future freeing in the freed vec
    pub(crate) fn new_do_not_modify(
        root: &'b mut Option<BtreeHeader>,
        page_allocator: PageAllocator,
        freed: &'b mut Vec<PageNumber>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            root,
            modify_uncommitted: false,
            page_allocator,
            freed,
            allocated,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    fn conditional_free(&mut self, page_number: PageNumber) {
        if self.modify_uncommitted {
            let mut allocated = self.allocated.lock().unwrap();
            if !self
                .page_allocator
                .free_if_uncommitted(page_number, &mut allocated)
            {
                self.freed.push(page_number);
            }
        } else {
            self.freed.push(page_number);
        }
    }

    pub(crate) fn delete(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'a, V>>> {
        let mut found_key = None;
        self.delete_target(DeleteTarget::Key(K::as_bytes(key).as_ref()), &mut found_key)
    }

    // Deletes the leftmost entry in the tree and returns (key, value), or None if empty.
    pub(crate) fn pop_first(&mut self) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        let mut found_key = None;
        let value = self.delete_target(DeleteTarget::First, &mut found_key)?;
        Ok(value.map(|v| (found_key.take().unwrap(), v)))
    }

    // Deletes the rightmost entry in the tree and returns (key, value), or None if empty.
    pub(crate) fn pop_last(&mut self) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        let mut found_key = None;
        let value = self.delete_target(DeleteTarget::Last, &mut found_key)?;
        Ok(value.map(|v| (found_key.take().unwrap(), v)))
    }

    pub(crate) fn retain_in_range<'r, KR, F>(
        &mut self,
        range: &'_ impl RangeBounds<KR>,
        mut predicate: F,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'r>> + 'r,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        assert!(self.modify_uncommitted);
        let Some(header) = *self.root else {
            return Ok(());
        };

        let start_tmp = match range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => Some(K::as_bytes(key.borrow())),
            Bound::Unbounded => None,
        };
        let end_tmp = match range.end_bound() {
            Bound::Included(key) | Bound::Excluded(key) => Some(K::as_bytes(key.borrow())),
            Bound::Unbounded => None,
        };
        let start_bound = match (range.start_bound(), start_tmp.as_ref()) {
            (Bound::Included(_), Some(bytes)) => Bound::Included(bytes.as_ref()),
            (Bound::Excluded(_), Some(bytes)) => Bound::Excluded(bytes.as_ref()),
            _ => Bound::Unbounded,
        };
        let end_bound = match (range.end_bound(), end_tmp.as_ref()) {
            (Bound::Included(_), Some(bytes)) => Bound::Included(bytes.as_ref()),
            (Bound::Excluded(_), Some(bytes)) => Bound::Excluded(bytes.as_ref()),
            _ => Bound::Unbounded,
        };
        let bounds = KeyRange::<K>::new(start_bound, end_bound);

        let mut removed = 0;
        let root_page = self.page_allocator.get_page(header.root, PageHint::None)?;
        let retain_result = self.retain_walk(
            root_page,
            RetainWalkContext {
                checksum: header.checksum,
                upper_key: None,
                depth: 0,
            },
            bounds,
            &mut predicate,
            &mut removed,
        )?;

        *self.root = if !retain_result.is_changed() {
            Some(header)
        } else {
            let subtrees = retain_result.into_changed_subtrees();
            if subtrees.is_empty() {
                None
            } else {
                let root = self.build_root_from_subtrees(subtrees)?;
                Some(BtreeHeader::new(
                    root.node.page,
                    root.node.checksum,
                    header.length - removed,
                ))
            }
        };

        Ok(())
    }

    fn retain_walk<F>(
        &mut self,
        page: PageImpl,
        context: RetainWalkContext,
        bounds: KeyRange<'_, K>,
        predicate: &mut F,
        removed: &mut u64,
    ) -> Result<RetainResult>
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        match page.memory()[0] {
            LEAF => self.retain_walk_leaf(page, context, bounds, predicate, removed),
            BRANCH => self.retain_walk_branch(page, context, bounds, predicate, removed),
            _ => unreachable!(),
        }
    }

    fn retain_walk_leaf<F>(
        &mut self,
        page: PageImpl,
        context: RetainWalkContext,
        bounds: KeyRange<'_, K>,
        predicate: &mut F,
        removed: &mut u64,
    ) -> Result<RetainResult>
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        let page_number = page.get_page_number();
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        let num_pairs = accessor.num_pairs();
        let mut keep = Vec::with_capacity(num_pairs);
        let mut kept_count = 0;
        for i in 0..num_pairs {
            let entry = accessor.entry(i).unwrap();
            let keep_entry = if bounds.contains(entry.key()) {
                predicate(K::from_bytes(entry.key()), V::from_bytes(entry.value()))
            } else {
                true
            };
            keep.push(keep_entry);
            if keep_entry {
                kept_count += 1;
            } else {
                *removed += 1;
            }
        }

        if kept_count == num_pairs {
            return Ok(RetainResult::unchanged(RetainNode {
                page: page_number,
                checksum: context.checksum,
                upper_key: context.upper_key,
            }));
        }

        if kept_count == 0 {
            drop(page);
            self.conditional_free(page_number);
            return Ok(RetainResult::changed(vec![]));
        }

        let mut builder = LeafBuilder::new(
            &self.page_allocator,
            &self.allocated,
            kept_count,
            K::fixed_width(),
            V::fixed_width(),
        );
        let new_upper_key = accessor
            .entry(keep.iter().rposition(|kept| *kept).unwrap())
            .unwrap()
            .key()
            .to_vec();
        let mut kept_bytes = 0;
        for (i, kept) in keep.into_iter().enumerate() {
            if kept {
                let entry = accessor.entry(i).unwrap();
                kept_bytes += entry.key().len() + entry.value().len();
                builder.push(entry.key(), entry.value());
            }
        }
        let required_bytes = RawLeafBuilder::required_bytes(
            kept_count,
            kept_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        let partial = (required_bytes < self.page_allocator.get_page_size() / 3)
            .then_some(RetainPartial::new());
        let new_page = builder.build()?;
        let new_page_number = new_page.get_page_number();
        drop(new_page);
        drop(page);
        self.conditional_free(page_number);
        Ok(RetainResult::changed(vec![RetainSubtree {
            node: RetainNode {
                page: new_page_number,
                checksum: DEFERRED,
                upper_key: Some(new_upper_key),
            },
            depth: context.depth,
            partial,
        }]))
    }

    fn retain_walk_branch<F>(
        &mut self,
        page: PageImpl,
        context: RetainWalkContext,
        bounds: KeyRange<'_, K>,
        predicate: &mut F,
        removed: &mut u64,
    ) -> Result<RetainResult>
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        let page_number = page.get_page_number();
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let child_count = accessor.count_children();
        let mut child_results = Vec::with_capacity(child_count);
        let child_depth = context.depth + 1;
        let mut changed = false;
        let page_resolver = self.page_allocator.resolver();

        for i in 0..child_count {
            let child_upper_key =
                Self::branch_child_upper_key(&accessor, context.upper_key.as_deref(), i);
            let below_start = child_upper_key
                .as_ref()
                .is_some_and(|key| bounds.less_than_start(key));
            let above_end =
                i > 0 && bounds.child_lower_bound_is_past_end(accessor.key(i - 1).unwrap());
            if below_start || above_end {
                child_results.push(RetainResult::unchanged(Self::branch_child_node(
                    &accessor,
                    context.upper_key.as_deref(),
                    i,
                )));
                continue;
            }

            let child_page_number = accessor.child_page(i).unwrap();
            let child_checksum = accessor.child_checksum(i).unwrap();
            let child_page = page_resolver.get_page(child_page_number, PageHint::None)?;
            let child = self.retain_walk(
                child_page,
                RetainWalkContext {
                    checksum: child_checksum,
                    upper_key: child_upper_key,
                    depth: child_depth,
                },
                bounds,
                predicate,
                removed,
            )?;
            changed |= child.is_changed();
            child_results.push(child);
        }

        if !changed {
            return Ok(RetainResult::unchanged(RetainNode {
                page: page_number,
                checksum: context.checksum,
                upper_key: context.upper_key,
            }));
        }

        let mut children = RetainSubtreeBuilder::with_capacity(child_count);
        for child in child_results {
            children.extend(child.into_subtrees(child_depth));
        }

        if children.is_empty() {
            drop(page);
            self.conditional_free(page_number);
            return Ok(RetainResult::changed(vec![]));
        }

        drop(page);
        self.conditional_free(page_number);

        let subtrees = self.build_branch_from_subtree_builder(children)?;
        Ok(RetainResult::changed(subtrees))
    }

    fn leaf_entries_required_bytes(entries: &[(Vec<u8>, Vec<u8>)]) -> usize {
        let entries_bytes = entries
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        RawLeafBuilder::required_bytes(
            entries.len(),
            entries_bytes,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    fn build_leaf_subtree(
        &mut self,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        partial: Option<RetainPartial>,
        depth: u32,
    ) -> Result<RetainSubtree> {
        debug_assert!(!entries.is_empty());
        let upper_key = entries.last().unwrap().0.clone();
        let mut builder = LeafBuilder::new(
            &self.page_allocator,
            &self.allocated,
            entries.len(),
            K::fixed_width(),
            V::fixed_width(),
        );
        for (key, value) in &entries {
            builder.push(key, value);
        }
        let page = builder.build()?;
        Ok(RetainSubtree {
            node: RetainNode {
                page: page.get_page_number(),
                checksum: DEFERRED,
                upper_key: Some(upper_key),
            },
            depth,
            partial,
        })
    }

    fn build_leaf_subtrees_from_entries(
        &mut self,
        mut entries: Vec<(Vec<u8>, Vec<u8>)>,
        merge_context: Option<RetainMergeContext>,
        depth: u32,
    ) -> Result<Vec<RetainSubtree>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let page_size = self.page_allocator.get_page_size();
        let mut groups = vec![];
        let mut current = vec![];
        for entry in entries.drain(..) {
            current.push(entry);
            if current.len() > 1 && Self::leaf_entries_required_bytes(&current) > page_size {
                let overflow = current.pop().unwrap();
                groups.push(std::mem::take(&mut current));
                current.push(overflow);
            }
        }
        if !current.is_empty() {
            groups.push(current);
        }

        let min_size = page_size / 3;
        for i in (1..groups.len()).rev() {
            while Self::leaf_entries_required_bytes(&groups[i]) < min_size
                && groups[i - 1].len() > 1
            {
                let moved = groups[i - 1].pop().unwrap();
                groups[i].insert(0, moved);
                if Self::leaf_entries_required_bytes(&groups[i]) > page_size
                    || Self::leaf_entries_required_bytes(&groups[i - 1]) < min_size
                {
                    let moved = groups[i].remove(0);
                    groups[i - 1].push(moved);
                    break;
                }
            }
        }

        let mut result = Vec::with_capacity(groups.len());
        let group_count = groups.len();
        for (i, group) in groups.into_iter().enumerate() {
            let partial = if Self::leaf_entries_required_bytes(&group) < min_size {
                Self::partial_for_rebuilt_group(i, group_count, merge_context)
            } else {
                None
            };
            let subtree = self.build_leaf_subtree(group, partial, depth)?;
            result.push(subtree);
        }
        Ok(result)
    }

    fn partial_for_rebuilt_group(
        index: usize,
        group_count: usize,
        merge_context: Option<RetainMergeContext>,
    ) -> Option<RetainPartial> {
        debug_assert!(index < group_count);
        if group_count == 1 {
            return Some(RetainPartial::new());
        }

        if index == 0 {
            let partial = merge_context
                .and_then(|context| context.left_partial)
                .unwrap_or_else(RetainPartial::new)
                .after_trying(RetainMergeSide::Right);
            partial.left_only()
        } else if index + 1 == group_count {
            let partial = merge_context
                .and_then(|context| context.right_partial)
                .unwrap_or_else(RetainPartial::new)
                .after_trying(RetainMergeSide::Left);
            partial.right_only()
        } else {
            Some(RetainPartial::new())
        }
    }

    fn branch_child_upper_key(
        accessor: &BranchAccessor<'_, '_, PageImpl>,
        parent_upper_key: Option<&[u8]>,
        index: usize,
    ) -> Option<Vec<u8>> {
        if index + 1 < accessor.count_children() {
            Some(accessor.key(index).unwrap().to_vec())
        } else {
            parent_upper_key.map(<[u8]>::to_vec)
        }
    }

    fn branch_child_node(
        accessor: &BranchAccessor<'_, '_, PageImpl>,
        parent_upper_key: Option<&[u8]>,
        index: usize,
    ) -> RetainNode {
        RetainNode {
            page: accessor.child_page(index).unwrap(),
            checksum: accessor.child_checksum(index).unwrap(),
            upper_key: Self::branch_child_upper_key(accessor, parent_upper_key, index),
        }
    }

    fn branch_child_subtree(
        accessor: &BranchAccessor<'_, '_, PageImpl>,
        parent_upper_key: Option<&[u8]>,
        child_depth: u32,
        index: usize,
    ) -> RetainSubtree {
        RetainSubtree {
            node: Self::branch_child_node(accessor, parent_upper_key, index),
            depth: child_depth,
            partial: None,
        }
    }

    fn normalize_retain_subtrees(
        &mut self,
        subtrees: Vec<RetainSubtree>,
    ) -> Result<Vec<RetainSubtree>> {
        self.finish_subtree_builder(RetainSubtreeBuilder::from_subtrees(subtrees))
    }

    fn finish_subtree_builder(
        &mut self,
        mut builder: RetainSubtreeBuilder,
    ) -> Result<Vec<RetainSubtree>> {
        let mut iterations = 0;
        loop {
            iterations += 1;
            debug_assert!(
                iterations < 10_000,
                "retain normalization did not converge: {:?}",
                builder.debug_state()
            );
            if builder.len() <= 1 {
                return Ok(builder.into_vec());
            }

            if let Some(index) = builder.first_pair_needing_merge() {
                self.merge_subtree_builder_pair(&mut builder, index)?;
                continue;
            }

            return Ok(builder.into_vec());
        }
    }

    fn merge_subtree_builder_pair(
        &mut self,
        builder: &mut RetainSubtreeBuilder,
        index: usize,
    ) -> Result<()> {
        let (left, right) = builder.remove_pair(index);
        let merged = self.merge_ordered_retain_subtrees(left, right)?;
        builder.splice(index, merged);
        Ok(())
    }

    fn merge_ordered_retain_subtrees(
        &mut self,
        left: RetainSubtree,
        right: RetainSubtree,
    ) -> Result<Vec<RetainSubtree>> {
        match left.depth.cmp(&right.depth) {
            Ordering::Equal => self.merge_adjacent_retain_subtrees(left, right),
            Ordering::Greater => self.merge_deeper_subtree_with_shallower(left, right, true),
            Ordering::Less => self.merge_deeper_subtree_with_shallower(right, left, false),
        }
    }

    fn merge_deeper_subtree_with_shallower(
        &mut self,
        deeper: RetainSubtree,
        shallower: RetainSubtree,
        deeper_before_shallower: bool,
    ) -> Result<Vec<RetainSubtree>> {
        debug_assert!(deeper.depth > shallower.depth);
        debug_assert!(deeper_before_shallower || shallower.node.upper_key.is_some());
        let page = self
            .page_allocator
            .get_page(shallower.node.page, PageHint::None)?;
        debug_assert_eq!(page.memory()[0], BRANCH);
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let child_depth = shallower.depth + 1;
        let edge_index = if deeper_before_shallower {
            0
        } else {
            accessor.count_children() - 1
        };
        let edge = Self::branch_child_subtree(
            &accessor,
            shallower.node.upper_key.as_deref(),
            child_depth,
            edge_index,
        );
        let mut children = Vec::with_capacity(accessor.count_children() + 1);

        if deeper_before_shallower {
            children.extend(self.merge_ordered_retain_subtrees(deeper, edge)?);
            for i in 1..accessor.count_children() {
                children.push(Self::branch_child_subtree(
                    &accessor,
                    shallower.node.upper_key.as_deref(),
                    child_depth,
                    i,
                ));
            }
        } else {
            for i in 0..edge_index {
                children.push(Self::branch_child_subtree(
                    &accessor,
                    shallower.node.upper_key.as_deref(),
                    child_depth,
                    i,
                ));
            }
            children.extend(self.merge_ordered_retain_subtrees(edge, deeper)?);
        }

        drop(page);
        self.conditional_free(shallower.node.page);
        self.build_branch_from_children(children)
    }

    fn merge_adjacent_retain_subtrees(
        &mut self,
        left: RetainSubtree,
        right: RetainSubtree,
    ) -> Result<Vec<RetainSubtree>> {
        debug_assert_eq!(left.depth, right.depth);
        debug_assert!(left.node.upper_key.is_some());
        let page = self
            .page_allocator
            .get_page(left.node.page, PageHint::None)?;
        let left_page_type = page.memory()[0];
        drop(page);
        let page = self
            .page_allocator
            .get_page(right.node.page, PageHint::None)?;
        let right_page_type = page.memory()[0];
        drop(page);
        match (left_page_type, right_page_type) {
            (LEAF, LEAF) => self.merge_leaf_subtrees(left, right),
            (BRANCH, BRANCH) => self.merge_branch_subtrees(left, right),
            _ => unreachable!(),
        }
    }

    fn merge_leaf_subtrees(
        &mut self,
        left: RetainSubtree,
        right: RetainSubtree,
    ) -> Result<Vec<RetainSubtree>> {
        let merge_context = RetainMergeContext::new(&left, &right);
        let mut entries = vec![];
        for subtree in [&left, &right] {
            let page = self
                .page_allocator
                .get_page(subtree.node.page, PageHint::None)?;
            debug_assert_eq!(page.memory()[0], LEAF);
            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            for i in 0..accessor.num_pairs() {
                let entry = accessor.entry(i).unwrap();
                entries.push((entry.key().to_vec(), entry.value().to_vec()));
            }
        }
        self.conditional_free(left.node.page);
        self.conditional_free(right.node.page);
        self.build_leaf_subtrees_from_entries(entries, Some(merge_context), left.depth)
    }

    fn merge_branch_subtrees(
        &mut self,
        left: RetainSubtree,
        right: RetainSubtree,
    ) -> Result<Vec<RetainSubtree>> {
        let merge_context = RetainMergeContext::new(&left, &right);
        let left_page = self
            .page_allocator
            .get_page(left.node.page, PageHint::None)?;
        let right_page = self
            .page_allocator
            .get_page(right.node.page, PageHint::None)?;
        debug_assert_eq!(left_page.memory()[0], BRANCH);
        debug_assert_eq!(right_page.memory()[0], BRANCH);
        let left_accessor = BranchAccessor::new(&left_page, K::fixed_width());
        let right_accessor = BranchAccessor::new(&right_page, K::fixed_width());
        let child_depth = left.depth + 1;
        let mut children =
            Vec::with_capacity(left_accessor.count_children() + right_accessor.count_children());
        for i in 0..left_accessor.count_children() {
            children.push(Self::branch_child_subtree(
                &left_accessor,
                left.node.upper_key.as_deref(),
                child_depth,
                i,
            ));
        }
        for i in 0..right_accessor.count_children() {
            children.push(Self::branch_child_subtree(
                &right_accessor,
                right.node.upper_key.as_deref(),
                child_depth,
                i,
            ));
        }
        drop(left_page);
        drop(right_page);
        self.conditional_free(left.node.page);
        self.conditional_free(right.node.page);
        self.build_branch_from_children_with_context(children, Some(merge_context))
    }

    fn build_root_from_subtrees(
        &mut self,
        mut subtrees: Vec<RetainSubtree>,
    ) -> Result<RetainSubtree> {
        subtrees = self.normalize_retain_subtrees(subtrees)?;
        while subtrees.len() > 1 {
            // Multiple subtrees at root depth need a new root above them. Bump
            // their relative depths before building that parent level.
            if subtrees.iter().all(|subtree| subtree.depth == 0) {
                for subtree in &mut subtrees {
                    subtree.depth += 1;
                }
            }
            subtrees = self.build_parent_level(subtrees, None)?;
            subtrees = self.normalize_retain_subtrees(subtrees)?;
        }
        Ok(subtrees.pop().unwrap())
    }

    fn build_branch_from_children(
        &mut self,
        children: Vec<RetainSubtree>,
    ) -> Result<Vec<RetainSubtree>> {
        self.build_branch_from_subtree_builder(RetainSubtreeBuilder::from_subtrees(children))
    }

    fn build_branch_from_subtree_builder(
        &mut self,
        children: RetainSubtreeBuilder,
    ) -> Result<Vec<RetainSubtree>> {
        self.build_branch_from_subtree_builder_with_context(children, None)
    }

    fn build_branch_from_children_with_context(
        &mut self,
        children: Vec<RetainSubtree>,
        merge_context: Option<RetainMergeContext>,
    ) -> Result<Vec<RetainSubtree>> {
        self.build_branch_from_subtree_builder_with_context(
            RetainSubtreeBuilder::from_subtrees(children),
            merge_context,
        )
    }

    fn build_branch_from_subtree_builder_with_context(
        &mut self,
        children: RetainSubtreeBuilder,
        merge_context: Option<RetainMergeContext>,
    ) -> Result<Vec<RetainSubtree>> {
        let children = self.finish_subtree_builder(children)?;
        if children.len() <= 1 {
            return Ok(children);
        }
        self.build_parent_level(children, merge_context)
    }

    fn build_parent_level(
        &mut self,
        children: Vec<RetainSubtree>,
        merge_context: Option<RetainMergeContext>,
    ) -> Result<Vec<RetainSubtree>> {
        debug_assert!(children.len() > 1);
        let child_depth = children[0].depth;
        debug_assert!(child_depth > 0);
        debug_assert!(children.iter().all(|child| child.depth == child_depth));

        let page_size = self.page_allocator.get_page_size();
        let mut groups: Vec<Vec<RetainSubtree>> = vec![];
        let mut current = vec![];
        for child in children {
            current.push(child);
            if current.len() > 2 && Self::branch_group_required_bytes(&current) > page_size {
                let overflow = current.pop().unwrap();
                groups.push(std::mem::take(&mut current));
                current.push(overflow);
            }
        }
        if !current.is_empty() {
            groups.push(current);
        }

        if groups.len() > 1 && groups.last().unwrap().len() == 1 {
            let last = groups.pop().unwrap().pop().unwrap();
            let previous = groups.last_mut().unwrap();
            previous.push(last);
        }

        let min_size = page_size / 3;
        for i in (1..groups.len()).rev() {
            while groups[i].len() > 1
                && Self::branch_group_required_bytes(&groups[i]) < min_size
                && groups[i - 1].len() > 2
            {
                let moved = groups[i - 1].pop().unwrap();
                groups[i].insert(0, moved);
                if Self::branch_group_required_bytes(&groups[i]) > page_size
                    || Self::branch_group_required_bytes(&groups[i - 1]) < min_size
                {
                    let moved = groups[i].remove(0);
                    groups[i - 1].push(moved);
                    break;
                }
            }
        }

        let mut parents = vec![];
        let group_count = groups.len();
        let multiple_groups = group_count > 1;
        for (i, group) in groups.into_iter().enumerate() {
            if group.len() == 1 {
                parents.extend(group);
                continue;
            }
            let force_full = multiple_groups
                && Self::branch_group_required_bytes(&group) < min_size
                && group.len() == 2;
            let partial = if !force_full && Self::branch_group_required_bytes(&group) < min_size {
                Self::partial_for_rebuilt_group(i, group_count, merge_context)
            } else {
                None
            };
            let built = self.build_branch_nodes(&group, child_depth, partial)?;
            parents.extend(built);
        }
        Ok(parents)
    }

    fn branch_group_required_bytes(children: &[RetainSubtree]) -> usize {
        debug_assert!(children.len() > 1);
        let total_key_bytes = children
            .iter()
            .take(children.len() - 1)
            .map(|child| {
                child
                    .node
                    .upper_key
                    .as_ref()
                    .expect("non-final retain child must have an upper bound")
                    .len()
            })
            .sum();
        RawBranchBuilder::required_bytes(children.len() - 1, total_key_bytes, K::fixed_width())
    }

    fn build_branch_nodes(
        &mut self,
        children: &[RetainSubtree],
        child_depth: u32,
        partial: Option<RetainPartial>,
    ) -> Result<Vec<RetainSubtree>> {
        debug_assert!(children.len() > 1);
        let depth = child_depth
            .checked_sub(1)
            .expect("retain branch children must be below their parent");
        let mut builder = BranchBuilder::new(
            &self.page_allocator,
            &self.allocated,
            children.len(),
            K::fixed_width(),
        );
        for (i, child) in children.iter().enumerate() {
            builder.push_child(child.node.page, child.node.checksum);
            if i + 1 < children.len() {
                builder.push_key(
                    child
                        .node
                        .upper_key
                        .as_ref()
                        .expect("non-final retain child must have an upper bound"),
                );
            }
        }

        if builder.should_split() {
            let (left, separator, right) = builder.build_split()?;
            let separator = separator.to_vec();
            let left_page = left.get_page_number();
            let right_page = right.get_page_number();
            Ok(vec![
                RetainSubtree {
                    node: RetainNode {
                        page: left_page,
                        checksum: DEFERRED,
                        upper_key: Some(separator),
                    },
                    depth,
                    partial: None,
                },
                RetainSubtree {
                    node: RetainNode {
                        page: right_page,
                        checksum: DEFERRED,
                        upper_key: children.last().unwrap().node.upper_key.clone(),
                    },
                    depth,
                    partial: None,
                },
            ])
        } else {
            let page = builder.build()?;
            Ok(vec![RetainSubtree {
                node: RetainNode {
                    page: page.get_page_number(),
                    checksum: DEFERRED,
                    upper_key: children.last().unwrap().node.upper_key.clone(),
                },
                depth,
                partial,
            }])
        }
    }

    fn delete_target(
        &mut self,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some(BtreeHeader {
            root: p, length, ..
        }) = *self.root
        {
            let (deletion_result, found) = self.delete_helper(
                self.page_allocator.get_page(p, PageHint::None)?,
                target,
                found_key,
            )?;
            if found.is_none() {
                // The tree was not modified; leave *self.root untouched so that any clean
                // root page keeps its already-valid checksum.
                return Ok(None);
            }
            let new_length = length - 1;
            let new_root = match deletion_result {
                Subtree(page) => Some(BtreeHeader::new(page, DEFERRED, new_length)),
                DeletedLeaf => None,
                PartialLeaf { page, deleted_pair } => {
                    let accessor = LeafAccessor::new(&page, K::fixed_width(), V::fixed_width());
                    let mut builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        accessor.num_pairs() - 1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    builder.push_all_except(&accessor, Some(deleted_pair));
                    let page = builder.build()?;
                    assert_eq!(new_length, accessor.num_pairs() as u64 - 1);
                    Some(BtreeHeader::new(
                        page.get_page_number(),
                        DEFERRED,
                        new_length,
                    ))
                }
                PartialBranch { children, keys } => {
                    let mut builder = BranchBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        children.len(),
                        K::fixed_width(),
                    );
                    for (child, child_checksum) in children {
                        builder.push_child(child, child_checksum);
                    }
                    for key in &keys {
                        builder.push_key(key);
                    }
                    let page = builder.build()?;
                    Some(BtreeHeader::new(
                        page.get_page_number(),
                        DEFERRED,
                        new_length,
                    ))
                }
                DeletedBranch(remaining_child, checksum) => {
                    Some(BtreeHeader::new(remaining_child, checksum, new_length))
                }
            };
            *self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn insert(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<(Option<AccessGuard<'a, V>>, AccessGuardMutInPlace<'a, V>)> {
        let (new_root, old_value, guard) = if let Some(BtreeHeader {
            root: p,
            checksum,
            length,
        }) = *self.root
        {
            let result = self.insert_helper(
                self.page_allocator.get_page(p, PageHint::None)?,
                checksum,
                K::as_bytes(key).as_ref(),
                V::as_bytes(value).as_ref(),
            )?;

            let new_length = if result.old_value.is_some() {
                length
            } else {
                length + 1
            };

            let new_root = if let Some((key, page2, page2_checksum)) = result.additional_sibling {
                let mut builder =
                    BranchBuilder::new(&self.page_allocator, &self.allocated, 2, K::fixed_width());
                builder.push_child(result.new_root, result.root_checksum);
                builder.push_key(&key);
                builder.push_child(page2, page2_checksum);
                let new_page = builder.build()?;
                BtreeHeader::new(new_page.get_page_number(), DEFERRED, new_length)
            } else {
                BtreeHeader::new(result.new_root, result.root_checksum, new_length)
            };
            (new_root, result.old_value, result.inserted_value)
        } else {
            let key_bytes = K::as_bytes(key);
            let value_bytes = V::as_bytes(value);
            let key_bytes = key_bytes.as_ref();
            let value_bytes = value_bytes.as_ref();
            let mut builder = LeafBuilder::new(
                &self.page_allocator,
                &self.allocated,
                1,
                K::fixed_width(),
                V::fixed_width(),
            );
            builder.push(key_bytes, value_bytes);
            let page = builder.build()?;

            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let offset = accessor.offset_of_first_value();
            let page_num = page.get_page_number();
            let guard = AccessGuardMutInPlace::new(page, offset, value_bytes.len());

            (BtreeHeader::new(page_num, DEFERRED, 1), None, guard)
        };
        *self.root = Some(new_root);
        Ok((old_value, guard))
    }

    fn insert_helper(
        &mut self,
        page: PageImpl,
        page_checksum: Checksum,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertionResult<'a, V>> {
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (position, found) = accessor.position::<K>(key);

                // Fast-path to avoid re-building and splitting pages with a single large value
                let single_large_value = accessor.num_pairs() == 1
                    && accessor.total_length() >= self.page_allocator.get_page_size();
                if !found && single_large_value {
                    let mut builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    builder.push(key, value);
                    let new_page = builder.build()?;
                    let new_page_number = new_page.get_page_number();
                    let new_page_accessor =
                        LeafAccessor::new(new_page.memory(), K::fixed_width(), V::fixed_width());
                    let offset = new_page_accessor.offset_of_first_value();
                    let guard = AccessGuardMutInPlace::new(new_page, offset, value.len());
                    return if position == 0 {
                        Ok(InsertionResult {
                            new_root: new_page_number,
                            root_checksum: DEFERRED,
                            additional_sibling: Some((
                                key.to_vec(),
                                page.get_page_number(),
                                page_checksum,
                            )),
                            inserted_value: guard,
                            old_value: None,
                        })
                    } else {
                        let split_key = accessor.last_entry().key().to_vec();
                        Ok(InsertionResult {
                            new_root: page.get_page_number(),
                            root_checksum: page_checksum,
                            additional_sibling: Some((split_key, new_page_number, DEFERRED)),
                            inserted_value: guard,
                            old_value: None,
                        })
                    };
                }

                // Fast-path for uncommitted pages, that can be modified in-place
                let has_inplace_space = || -> bool {
                    if found {
                        LeafMutator::sufficient_replace_inplace_space(
                            &page,
                            position,
                            K::fixed_width(),
                            V::fixed_width(),
                            value,
                        )
                    } else {
                        LeafMutator::sufficient_insert_inplace_space(
                            &page,
                            position,
                            K::fixed_width(),
                            V::fixed_width(),
                            key,
                            value,
                        )
                    }
                };
                if self.page_allocator.uncommitted(page.get_page_number())
                    && self.modify_uncommitted
                    && has_inplace_space()
                {
                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let copied_value = accessor.entry(position).unwrap().value().to_vec();
                        Some(AccessGuard::with_owned_value(copied_value))
                    } else {
                        None
                    };
                    drop(page);
                    let mut page_mut = self.page_allocator.get_page_mut(page_number)?;
                    let mut mutator =
                        LeafMutator::new(page_mut.memory_mut(), K::fixed_width(), V::fixed_width());
                    if found {
                        mutator.replace(position, value);
                    } else {
                        mutator.insert(position, key, value);
                    }
                    let new_page_accessor =
                        LeafAccessor::new(page_mut.memory(), K::fixed_width(), V::fixed_width());
                    let offset = new_page_accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMutInPlace::new(page_mut, offset, value.len());
                    return Ok(InsertionResult {
                        new_root: page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: guard,
                        old_value: existing_value,
                    });
                }

                let mut builder = LeafBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    accessor.num_pairs() + 1,
                    K::fixed_width(),
                    V::fixed_width(),
                );
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        builder.push(key, value);
                    }
                    if !found || i != position {
                        let entry = accessor.entry(i).unwrap();
                        builder.push(entry.key(), entry.value());
                    }
                }
                if accessor.num_pairs() == position {
                    builder.push(key, value);
                }
                if !builder.should_split() {
                    let new_page = builder.build()?;

                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let (start, end) = accessor.value_range(position).unwrap();
                        if self.modify_uncommitted && self.page_allocator.uncommitted(page_number) {
                            let arc = page.to_arc();
                            drop(page);
                            let mut allocated = self.allocated.lock().unwrap();
                            self.page_allocator.free(page_number, &mut allocated);
                            Some(AccessGuard::with_arc_page(arc, start..end))
                        } else {
                            self.freed.push(page_number);
                            Some(AccessGuard::with_page(page, start..end))
                        }
                    } else {
                        drop(page);
                        self.conditional_free(page_number);
                        None
                    };

                    let new_page_number = new_page.get_page_number();
                    let accessor =
                        LeafAccessor::new(new_page.memory(), K::fixed_width(), V::fixed_width());
                    let offset = accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMutInPlace::new(new_page, offset, value.len());

                    InsertionResult {
                        new_root: new_page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: guard,
                        old_value: existing_value,
                    }
                } else {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    let split_key = split_key.to_vec();
                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let (start, end) = accessor.value_range(position).unwrap();
                        if self.modify_uncommitted && self.page_allocator.uncommitted(page_number) {
                            let arc = page.to_arc();
                            drop(page);
                            let mut allocated = self.allocated.lock().unwrap();
                            self.page_allocator.free(page_number, &mut allocated);
                            Some(AccessGuard::with_arc_page(arc, start..end))
                        } else {
                            self.freed.push(page_number);
                            Some(AccessGuard::with_page(page, start..end))
                        }
                    } else {
                        drop(page);
                        self.conditional_free(page_number);
                        None
                    };

                    let new_page_number = new_page1.get_page_number();
                    let new_page_number2 = new_page2.get_page_number();
                    let accessor =
                        LeafAccessor::new(new_page1.memory(), K::fixed_width(), V::fixed_width());
                    let division = accessor.num_pairs();
                    let guard = if position < division {
                        let accessor = LeafAccessor::new(
                            new_page1.memory(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        let offset = accessor.offset_of_value(position).unwrap();
                        AccessGuardMutInPlace::new(new_page1, offset, value.len())
                    } else {
                        let accessor = LeafAccessor::new(
                            new_page2.memory(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        let offset = accessor.offset_of_value(position - division).unwrap();
                        AccessGuardMutInPlace::new(new_page2, offset, value.len())
                    };

                    InsertionResult {
                        new_root: new_page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: Some((split_key, new_page_number2, DEFERRED)),
                        inserted_value: guard,
                        old_value: existing_value,
                    }
                }
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (child_index, child_page) = accessor.child_for_key::<K>(key);
                let child_checksum = accessor.child_checksum(child_index).unwrap();
                let sub_result = self.insert_helper(
                    self.page_allocator.get_page(child_page, PageHint::None)?,
                    child_checksum,
                    key,
                    value,
                )?;

                // Skip-path: if child page number and checksum haven't changed,
                // no branch update is needed. This avoids redundant get_page_mut +
                // write_child_page calls on repeat visits to the same subtree
                // within a transaction.
                if sub_result.additional_sibling.is_none()
                    && sub_result.new_root == child_page
                    && sub_result.root_checksum == child_checksum
                {
                    return Ok(InsertionResult {
                        new_root: page.get_page_number(),
                        root_checksum: page_checksum,
                        additional_sibling: None,
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    });
                }

                if sub_result.additional_sibling.is_none()
                    && self.modify_uncommitted
                    && self.page_allocator.uncommitted(page.get_page_number())
                {
                    let page_number = page.get_page_number();
                    drop(page);
                    let mut mutpage = self.page_allocator.get_page_mut(page_number)?;
                    let mut mutator = BranchMutator::new(mutpage.memory_mut());
                    mutator.write_child_page(
                        child_index,
                        sub_result.new_root,
                        sub_result.root_checksum,
                    );
                    return Ok(InsertionResult {
                        new_root: mutpage.get_page_number(),
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    });
                }

                // A child was added, or we couldn't use the fast-path above
                let mut builder = BranchBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    accessor.count_children() + 1,
                    K::fixed_width(),
                );
                if child_index == 0 {
                    builder.push_child(sub_result.new_root, sub_result.root_checksum);
                    if let Some((ref index_key2, page2, page2_checksum)) =
                        sub_result.additional_sibling
                    {
                        builder.push_key(index_key2);
                        builder.push_child(page2, page2_checksum);
                    }
                } else {
                    builder.push_child(
                        accessor.child_page(0).unwrap(),
                        accessor.child_checksum(0).unwrap(),
                    );
                }
                for i in 1..accessor.count_children() {
                    if let Some(key) = accessor.key(i - 1) {
                        builder.push_key(key);
                        if i == child_index {
                            builder.push_child(sub_result.new_root, sub_result.root_checksum);
                            if let Some((ref index_key2, page2, page2_checksum)) =
                                sub_result.additional_sibling
                            {
                                builder.push_key(index_key2);
                                builder.push_child(page2, page2_checksum);
                            }
                        } else {
                            builder.push_child(
                                accessor.child_page(i).unwrap(),
                                accessor.child_checksum(i).unwrap(),
                            );
                        }
                    } else {
                        unreachable!();
                    }
                }

                let result = if builder.should_split() {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    InsertionResult {
                        new_root: new_page1.get_page_number(),
                        root_checksum: DEFERRED,
                        additional_sibling: Some((
                            split_key.to_vec(),
                            new_page2.get_page_number(),
                            DEFERRED,
                        )),
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    }
                } else {
                    let new_page = builder.build()?;
                    InsertionResult {
                        new_root: new_page.get_page_number(),
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: sub_result.inserted_value,
                        old_value: sub_result.old_value,
                    }
                };
                // Free the original page, since we've replaced it
                let page_number = page.get_page_number();
                drop(page);
                self.conditional_free(page_number);

                result
            }
            _ => unreachable!(),
        })
    }

    pub(crate) fn insert_inplace(
        &mut self,
        key: &K::SelfType<'_>,
        value: &V::SelfType<'_>,
    ) -> Result<()> {
        assert!(self.modify_uncommitted);
        let header = self.root.expect("Key not found (tree is empty)");
        self.insert_inplace_helper(
            self.page_allocator.get_page_mut(header.root)?,
            K::as_bytes(key).as_ref(),
            V::as_bytes(value).as_ref(),
        )?;
        *self.root = Some(BtreeHeader::new(header.root, DEFERRED, header.length));
        Ok(())
    }

    fn insert_inplace_helper(&mut self, mut page: PageMut, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(self.page_allocator.uncommitted(page.get_page_number()));

        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                let (position, found) = accessor.position::<K>(key);
                assert!(found);
                let old_len = accessor.entry(position).unwrap().value().len();
                assert!(value.len() <= old_len);
                let mut mutator =
                    LeafMutator::new(page.memory_mut(), K::fixed_width(), V::fixed_width());
                mutator.replace(position, value);
            }
            BRANCH => {
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let (child_index, child_page) = accessor.child_for_key::<K>(key);
                self.insert_inplace_helper(
                    self.page_allocator.get_page_mut(child_page)?,
                    key,
                    value,
                )?;
                let mut mutator = BranchMutator::new(page.memory_mut());
                mutator.write_child_page(child_index, child_page, DEFERRED);
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn delete_leaf_helper(
        &mut self,
        page: PageImpl,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        let (position, found) = match target {
            DeleteTarget::Key(key) => accessor.position::<K>(key),
            DeleteTarget::First => (0, true),
            DeleteTarget::Last => (accessor.num_pairs() - 1, true),
        };
        if !found {
            // Leaf is unchanged; caller short-circuits via `found.is_none()`.
            return Ok((Subtree(page.get_page_number()), None));
        }
        let want_key = matches!(target, DeleteTarget::First | DeleteTarget::Last);
        let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
            - accessor.length_of_pairs(position, position + 1);
        let new_required_bytes = RawLeafBuilder::required_bytes(
            accessor.num_pairs() - 1,
            new_kv_bytes,
            K::fixed_width(),
            V::fixed_width(),
        );
        let uncommitted = self.page_allocator.uncommitted(page.get_page_number());

        // Fast-path for dirty pages: perform in-place removal without allocating a new page.
        // The threshold matches the merge threshold (page_size/3) so that we use in-place
        // removal for all cases where the page won't need merging with a sibling.
        if uncommitted
            && self.modify_uncommitted
            && new_required_bytes >= self.page_allocator.get_page_size() / 3
            && accessor.num_pairs() > 1
        {
            let (start, end) = accessor.value_range(position).unwrap();
            // The returned value guard owns the mutable page and removes the entry on drop,
            // so we can't hand the key back as a borrow into the same page. Copy it instead.
            if want_key {
                *found_key = Some(AccessGuard::with_owned_value(
                    accessor.entry(position).unwrap().key().to_vec(),
                ));
            }
            let page_number = page.get_page_number();
            drop(page);
            let page_mut = self.page_allocator.get_page_mut(page_number)?;

            let guard = AccessGuard::remove_on_drop(
                page_mut,
                start,
                end - start,
                position,
                K::fixed_width(),
            );
            return Ok((Subtree(page_number), Some(guard)));
        }

        let result = if accessor.num_pairs() == 1 {
            DeletedLeaf
        } else if new_required_bytes < self.page_allocator.get_page_size() / 3 {
            // Merge when less than 33% full. Splits occur when a page is full and produce two 50%
            // full pages, so we use 33% instead of 50% to avoid oscillating
            PartialLeaf {
                page: page.to_arc(),
                deleted_pair: position,
            }
        } else {
            let mut builder = LeafBuilder::new(
                &self.page_allocator,
                &self.allocated,
                accessor.num_pairs() - 1,
                K::fixed_width(),
                V::fixed_width(),
            );
            for i in 0..accessor.num_pairs() {
                if i == position {
                    continue;
                }
                let entry = accessor.entry(i).unwrap();
                builder.push(entry.key(), entry.value());
            }
            let new_page = builder.build()?;
            Subtree(new_page.get_page_number())
        };
        let (key_range, value_range) = accessor.entry_ranges(position).unwrap();
        let guard = if uncommitted && self.modify_uncommitted {
            let page_number = page.get_page_number();
            let arc = page.to_arc();
            drop(page);
            let mut allocated = self.allocated.lock().unwrap();
            self.page_allocator.free(page_number, &mut allocated);
            if want_key {
                *found_key = Some(AccessGuard::with_arc_page(arc.clone(), key_range));
            }
            Some(AccessGuard::with_arc_page(arc, value_range))
        } else {
            // Won't be freed until the end of the transaction, so returning the page
            // in the AccessGuard below is still safe
            if want_key {
                *found_key = Some(AccessGuard::with_page(page.clone(), key_range));
            }
            self.freed.push(page.get_page_number());
            Some(AccessGuard::with_page(page, value_range))
        };
        Ok((result, guard))
    }

    fn finalize_branch_builder(
        builder: BranchBuilder<'_, '_>,
        page_size: usize,
    ) -> Result<DeletionResult> {
        let result = if let Some((only_child, checksum)) = builder.to_single_child() {
            DeletedBranch(only_child, checksum)
        } else if builder.required_bytes() < page_size / 3 {
            // Merge when less than 33% full. Splits occur when a page is full and produce two 50%
            // full pages, so we use 33% instead of 50% to avoid oscillating.
            // Skip the page allocation: the caller will immediately merge this with a sibling.
            let (children, keys) = builder.into_parts();
            PartialBranch { children, keys }
        } else {
            let new_page = builder.build()?;
            Subtree(new_page.get_page_number())
        };
        Ok(result)
    }

    fn delete_branch_helper(
        &mut self,
        page: PageImpl,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let original_page_number = page.get_page_number();
        let (child_index, child_page_number) = match target {
            DeleteTarget::Key(key) => accessor.child_for_key::<K>(key),
            DeleteTarget::First => (0, accessor.child_page(0).unwrap()),
            DeleteTarget::Last => {
                let idx = accessor.count_children() - 1;
                (idx, accessor.child_page(idx).unwrap())
            }
        };
        let child_checksum = accessor.child_checksum(child_index).unwrap();
        let (result, found) = self.delete_helper(
            self.page_allocator
                .get_page(child_page_number, PageHint::None)?,
            target,
            found_key,
        )?;
        if found.is_none() {
            // Subtree unchanged; caller identifies this via `found.is_none()`.
            return Ok((Subtree(original_page_number), None));
        }
        if let Subtree(new_child) = result {
            // Skip-path: the child's in-parent entry is already (child_page_number, DEFERRED)
            // and the mutated child kept its page number, so no write to this branch is needed.
            // This preserves the optimization from f8ccc39 without carrying a checksum on
            // `Subtree` (a modified `Subtree` always has checksum DEFERRED).
            if new_child == child_page_number && child_checksum == DEFERRED {
                return Ok((Subtree(original_page_number), found));
            }

            let result_page = if self.page_allocator.uncommitted(original_page_number)
                && self.modify_uncommitted
            {
                drop(page);
                let mut mutpage = self.page_allocator.get_page_mut(original_page_number)?;
                let mut mutator = BranchMutator::new(mutpage.memory_mut());
                mutator.write_child_page(child_index, new_child, DEFERRED);
                original_page_number
            } else {
                let mut builder = BranchBuilder::new(
                    &self.page_allocator,
                    &self.allocated,
                    accessor.count_children(),
                    K::fixed_width(),
                );
                builder.push_all(&accessor);
                builder.replace_child(child_index, new_child, DEFERRED);
                let new_page = builder.build()?;
                self.conditional_free(original_page_number);
                new_page.get_page_number()
            };
            return Ok((Subtree(result_page), found));
        }

        // Child is requesting to be merged with a sibling
        let mut builder = BranchBuilder::new(
            &self.page_allocator,
            &self.allocated,
            accessor.count_children(),
            K::fixed_width(),
        );

        let final_result = match result {
            Subtree(_) => {
                // Handled in the if above
                unreachable!();
            }
            DeletedLeaf => {
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    builder.push_child(
                        accessor.child_page(i).unwrap(),
                        accessor.child_checksum(i).unwrap(),
                    );
                }
                let end = if child_index == accessor.count_children() - 1 {
                    // Skip the last key, which precedes the child
                    accessor.count_children() - 2
                } else {
                    accessor.count_children() - 1
                };
                for i in 0..end {
                    if i == child_index {
                        continue;
                    }
                    builder.push_key(accessor.key(i).unwrap());
                }
                Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?
            }
            PartialLeaf {
                page: partial_child_page,
                deleted_pair,
            } => {
                let partial_child_accessor =
                    LeafAccessor::new(&partial_child_page, K::fixed_width(), V::fixed_width());
                debug_assert!(partial_child_accessor.num_pairs() > 1);

                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                debug_assert!(merge_with < accessor.count_children());
                let merge_with_page = self
                    .page_allocator
                    .get_page(accessor.child_page(merge_with).unwrap(), PageHint::None)?;
                let merge_with_accessor =
                    LeafAccessor::new(merge_with_page.memory(), K::fixed_width(), V::fixed_width());

                let single_large_value = merge_with_accessor.num_pairs() == 1
                    && merge_with_accessor.total_length() >= self.page_allocator.get_page_size();
                // Don't try to merge or rebalance, if the sibling contains a single large value
                if single_large_value {
                    let mut child_builder = LeafBuilder::new(
                        &self.page_allocator,
                        &self.allocated,
                        partial_child_accessor.num_pairs() - 1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    child_builder.push_all_except(&partial_child_accessor, Some(deleted_pair));
                    let new_page = child_builder.build()?;
                    builder.push_all(&accessor);
                    builder.replace_child(child_index, new_page.get_page_number(), DEFERRED);

                    let result = Self::finalize_branch_builder(
                        builder,
                        self.page_allocator.get_page_size(),
                    )?;

                    drop(page);
                    self.conditional_free(original_page_number);
                    // child_page_number does not need to be freed, because it's a leaf and the
                    // MutAccessGuard will free it

                    return Ok((result, found));
                }

                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    let page_number = accessor.child_page(i).unwrap();
                    let page_checksum = accessor.child_checksum(i).unwrap();
                    if i == merge_with {
                        let mut child_builder = LeafBuilder::new(
                            &self.page_allocator,
                            &self.allocated,
                            partial_child_accessor.num_pairs() - 1
                                + merge_with_accessor.num_pairs(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        if child_index < merge_with {
                            child_builder
                                .push_all_except(&partial_child_accessor, Some(deleted_pair));
                        }
                        child_builder.push_all_except(&merge_with_accessor, None);
                        if child_index > merge_with {
                            child_builder
                                .push_all_except(&partial_child_accessor, Some(deleted_pair));
                        }
                        if child_builder.should_split() {
                            let (new_page1, split_key, new_page2) = child_builder.build_split()?;
                            builder.push_key(split_key);
                            builder.push_child(new_page1.get_page_number(), DEFERRED);
                            builder.push_child(new_page2.get_page_number(), DEFERRED);
                        } else {
                            let new_page = child_builder.build()?;
                            builder.push_child(new_page.get_page_number(), DEFERRED);
                        }

                        let merged_key_index = max(child_index, merge_with);
                        if merged_key_index < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(merged_key_index).unwrap());
                        }
                    } else {
                        builder.push_child(page_number, page_checksum);
                        if i < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(i).unwrap());
                        }
                    }
                }

                let result =
                    Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.conditional_free(page_number);
                // child_page_number does not need to be freed, because it's a leaf and the
                // MutAccessGuard will free it

                result
            }
            DeletedBranch(only_grandchild, grandchild_checksum) => {
                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                let merge_with_page = self
                    .page_allocator
                    .get_page(accessor.child_page(merge_with).unwrap(), PageHint::None)?;
                let merge_with_accessor = BranchAccessor::new(&merge_with_page, K::fixed_width());
                debug_assert!(merge_with < accessor.count_children());
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    let page_number = accessor.child_page(i).unwrap();
                    let page_checksum = accessor.child_checksum(i).unwrap();
                    if i == merge_with {
                        let mut child_builder = BranchBuilder::new(
                            &self.page_allocator,
                            &self.allocated,
                            merge_with_accessor.count_children() + 1,
                            K::fixed_width(),
                        );
                        let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                        if child_index < merge_with {
                            child_builder.push_child(only_grandchild, grandchild_checksum);
                            child_builder.push_key(separator_key);
                        }
                        child_builder.push_all(&merge_with_accessor);
                        if child_index > merge_with {
                            child_builder.push_key(separator_key);
                            child_builder.push_child(only_grandchild, grandchild_checksum);
                        }
                        if child_builder.should_split() {
                            let (new_page1, separator, new_page2) = child_builder.build_split()?;
                            builder.push_child(new_page1.get_page_number(), DEFERRED);
                            builder.push_key(separator);
                            builder.push_child(new_page2.get_page_number(), DEFERRED);
                        } else {
                            let new_page = child_builder.build()?;
                            builder.push_child(new_page.get_page_number(), DEFERRED);
                        }

                        let merged_key_index = max(child_index, merge_with);
                        if merged_key_index < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(merged_key_index).unwrap());
                        }
                    } else {
                        builder.push_child(page_number, page_checksum);
                        if i < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(i).unwrap());
                        }
                    }
                }
                let result =
                    Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.conditional_free(page_number);

                result
            }
            PartialBranch {
                children: partial_children,
                keys: partial_keys,
            } => {
                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                let merge_with_page = self
                    .page_allocator
                    .get_page(accessor.child_page(merge_with).unwrap(), PageHint::None)?;
                let merge_with_accessor = BranchAccessor::new(&merge_with_page, K::fixed_width());
                debug_assert!(merge_with < accessor.count_children());
                for i in 0..accessor.count_children() {
                    if i == child_index {
                        continue;
                    }
                    let page_number = accessor.child_page(i).unwrap();
                    let page_checksum = accessor.child_checksum(i).unwrap();
                    if i == merge_with {
                        let mut child_builder = BranchBuilder::new(
                            &self.page_allocator,
                            &self.allocated,
                            merge_with_accessor.count_children() + partial_children.len(),
                            K::fixed_width(),
                        );
                        let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                        if child_index < merge_with {
                            for &(child, child_checksum) in &partial_children {
                                child_builder.push_child(child, child_checksum);
                            }
                            for key in &partial_keys {
                                child_builder.push_key(key);
                            }
                            child_builder.push_key(separator_key);
                        }
                        child_builder.push_all(&merge_with_accessor);
                        if child_index > merge_with {
                            child_builder.push_key(separator_key);
                            for &(child, child_checksum) in &partial_children {
                                child_builder.push_child(child, child_checksum);
                            }
                            for key in &partial_keys {
                                child_builder.push_key(key);
                            }
                        }
                        if child_builder.should_split() {
                            let (new_page1, separator, new_page2) = child_builder.build_split()?;
                            builder.push_child(new_page1.get_page_number(), DEFERRED);
                            builder.push_key(separator);
                            builder.push_child(new_page2.get_page_number(), DEFERRED);
                        } else {
                            let new_page = child_builder.build()?;
                            builder.push_child(new_page.get_page_number(), DEFERRED);
                        }

                        let merged_key_index = max(child_index, merge_with);
                        if merged_key_index < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(merged_key_index).unwrap());
                        }
                    } else {
                        builder.push_child(page_number, page_checksum);
                        if i < accessor.count_children() - 1 {
                            builder.push_key(accessor.key(i).unwrap());
                        }
                    }
                }
                let result =
                    Self::finalize_branch_builder(builder, self.page_allocator.get_page_size())?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.conditional_free(page_number);

                result
            }
        };

        drop(page);
        self.conditional_free(original_page_number);

        Ok((final_result, found))
    }

    // Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
    // If key is not found, guaranteed not to modify the tree.
    // When `target` is DeleteTarget::First or DeleteTarget::Last, `found_key` is populated
    // with an AccessGuard for the deleted key.
    fn delete_helper(
        &mut self,
        page: PageImpl,
        target: DeleteTarget<'_>,
        found_key: &mut Option<AccessGuard<'a, K>>,
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => self.delete_leaf_helper(page, target, found_key),
            BRANCH => self.delete_branch_helper(page, target, found_key),
            _ => unreachable!(),
        }
    }
}
