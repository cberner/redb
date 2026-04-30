use crate::Result;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, BranchBuilder, Checksum, DEFERRED, LEAF, LeafAccessor, LeafBuilder,
    RawBranchBuilder, RawLeafBuilder,
};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{PageAllocator, PageHint, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Mutex;

type RetainLeafEntry = (Vec<u8>, Vec<u8>);

#[derive(Copy, Clone, Eq, PartialEq)]
enum RetainEdge {
    Left,
    Right,
}

impl RetainEdge {
    fn child_index<T: Page>(self, accessor: &BranchAccessor<'_, '_, T>) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => accessor.count_children() - 1,
        }
    }
}

pub(super) struct RetainBuilderContext<'a, K: Key, V: Value> {
    page_allocator: &'a PageAllocator,
    allocated: &'a Mutex<PageTrackerPolicy>,
    freed: &'a mut Vec<PageNumber>,
    modify_uncommitted: bool,
    _types: PhantomData<(K, V)>,
}

impl<'a, K: Key, V: Value> RetainBuilderContext<'a, K, V> {
    pub(super) fn new(
        page_allocator: &'a PageAllocator,
        allocated: &'a Mutex<PageTrackerPolicy>,
        freed: &'a mut Vec<PageNumber>,
        modify_uncommitted: bool,
    ) -> Self {
        Self {
            page_allocator,
            allocated,
            freed,
            modify_uncommitted,
            _types: PhantomData,
        }
    }

    pub(super) fn get_page(&self, page_number: PageNumber) -> Result<PageImpl> {
        self.page_allocator.get_page(page_number, PageHint::None)
    }

    pub(super) fn conditional_free(&mut self, page_number: PageNumber) {
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
}

// A sealed B-tree subtree, annotated with its distance from the original retain walk root.
pub(super) struct RetainSubtree {
    page: PageNumber,
    checksum: Checksum,
    // Upper bound for this subtree. Parent branch separators are rebuilt from
    // it. Only the final subtree in an ordered list may use None; separator
    // keys on older branch pages can be stale upper bounds, so retain must not
    // rely on this being the exact maximum key unless the node was just rebuilt.
    upper_key: Option<Vec<u8>>,
    root_distance: u32,
}

impl RetainSubtree {
    pub(super) fn new(
        page: PageNumber,
        checksum: Checksum,
        upper_key: Option<Vec<u8>>,
        root_distance: u32,
    ) -> Self {
        Self {
            page,
            checksum,
            upper_key,
            root_distance,
        }
    }

    pub(super) fn branch_child(
        accessor: &BranchAccessor<'_, '_, PageImpl>,
        parent_upper_key: Option<&[u8]>,
        root_distance: u32,
        index: usize,
    ) -> Self {
        let upper_key = if index + 1 < accessor.count_children() {
            Some(accessor.key(index).unwrap().to_vec())
        } else {
            parent_upper_key.map(<[u8]>::to_vec)
        };

        Self::new(
            accessor.child_page(index).unwrap(),
            accessor.child_checksum(index).unwrap(),
            upper_key,
            root_distance,
        )
    }

    pub(super) fn root_distance(&self) -> u32 {
        self.root_distance
    }

    fn graft_ordered<K: Key, V: Value>(
        context: &mut RetainBuilderContext<'_, K, V>,
        left: Self,
        right: Self,
    ) -> Result<(Self, Option<Self>)> {
        match left.root_distance.cmp(&right.root_distance) {
            Ordering::Equal => Ok((left, Some(right))),
            Ordering::Greater => {
                Self::graft_deeper_into_shallower(context, left, right, RetainEdge::Left)
            }
            Ordering::Less => {
                Self::graft_deeper_into_shallower(context, right, left, RetainEdge::Right)
            }
        }
    }

    fn graft_deeper_into_shallower<K: Key, V: Value>(
        context: &mut RetainBuilderContext<'_, K, V>,
        deeper: Self,
        shallower: Self,
        edge: RetainEdge,
    ) -> Result<(Self, Option<Self>)> {
        assert!(deeper.root_distance > shallower.root_distance);
        let old_page = shallower.page;
        let branch_root_distance = shallower.root_distance;
        let branch_upper_key = shallower.upper_key;
        assert!(edge == RetainEdge::Left || branch_upper_key.is_some());
        let page = context.get_page(old_page)?;
        assert_eq!(page.memory()[0], BRANCH);
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let child_root_distance = branch_root_distance + 1;
        let edge_index = edge.child_index(&accessor);
        let edge_child = Self::branch_child(
            &accessor,
            branch_upper_key.as_deref(),
            child_root_distance,
            edge_index,
        );
        let replacement = match edge {
            RetainEdge::Left => Self::graft_ordered(context, deeper, edge_child)?,
            RetainEdge::Right => Self::graft_ordered(context, edge_child, deeper)?,
        };

        let rebuilt = Self::replace_branch_child(
            context,
            &accessor,
            branch_root_distance,
            branch_upper_key,
            edge,
            replacement,
        )?;
        drop(page);
        context.conditional_free(old_page);
        Ok(rebuilt)
    }

    fn absorb_leaf_entries<K: Key, V: Value>(
        self,
        context: &mut RetainBuilderContext<'_, K, V>,
        entries: Vec<RetainLeafEntry>,
        entry_root_distance: u32,
        edge: RetainEdge,
    ) -> Result<(Self, Option<Self>)> {
        match self.root_distance.cmp(&entry_root_distance) {
            Ordering::Equal => {
                let page = context.get_page(self.page)?;
                assert_eq!(page.memory()[0], LEAF);
                let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                assert!(!entries.is_empty());
                let upper_key = match edge {
                    RetainEdge::Left => self.upper_key.clone(),
                    RetainEdge::Right => Some(entries.last().unwrap().0.clone()),
                };
                let mut builder = LeafBuilder::new(
                    context.page_allocator,
                    context.allocated,
                    entries.len() + accessor.num_pairs(),
                    K::fixed_width(),
                    V::fixed_width(),
                );
                match edge {
                    RetainEdge::Left => {
                        for (key, value) in &entries {
                            builder.push(key, value);
                        }
                        builder.push_all_except(&accessor, None);
                    }
                    RetainEdge::Right => {
                        builder.push_all_except(&accessor, None);
                        for (key, value) in &entries {
                            builder.push(key, value);
                        }
                    }
                }
                let result = if builder.should_split() {
                    let (left, separator, right) = builder.build_split()?;
                    let separator = separator.to_vec();
                    (
                        Self::new(
                            left.get_page_number(),
                            DEFERRED,
                            Some(separator),
                            entry_root_distance,
                        ),
                        Some(Self::new(
                            right.get_page_number(),
                            DEFERRED,
                            upper_key,
                            entry_root_distance,
                        )),
                    )
                } else {
                    let page = builder.build()?;
                    (
                        Self::new(
                            page.get_page_number(),
                            DEFERRED,
                            upper_key,
                            entry_root_distance,
                        ),
                        None,
                    )
                };
                drop(page);
                context.conditional_free(self.page);
                Ok(result)
            }
            Ordering::Less => {
                let old_page = self.page;
                let branch_root_distance = self.root_distance;
                let branch_upper_key = self.upper_key;
                let page = context.get_page(old_page)?;
                assert_eq!(page.memory()[0], BRANCH);
                let accessor = BranchAccessor::new(&page, K::fixed_width());
                let child_root_distance = branch_root_distance + 1;
                let edge_index = edge.child_index(&accessor);
                let edge_child = Self::branch_child(
                    &accessor,
                    branch_upper_key.as_deref(),
                    child_root_distance,
                    edge_index,
                );
                let replacement =
                    edge_child.absorb_leaf_entries(context, entries, entry_root_distance, edge)?;
                let rebuilt = Self::replace_branch_child(
                    context,
                    &accessor,
                    branch_root_distance,
                    branch_upper_key,
                    edge,
                    replacement,
                )?;
                drop(page);
                context.conditional_free(old_page);
                Ok(rebuilt)
            }
            Ordering::Greater => {
                unreachable!("retain leaf entries cannot be above the subtree they are joining")
            }
        }
    }

    fn replace_branch_child<K: Key, V: Value>(
        context: &mut RetainBuilderContext<'_, K, V>,
        accessor: &BranchAccessor<'_, '_, PageImpl>,
        branch_root_distance: u32,
        branch_upper_key: Option<Vec<u8>>,
        edge: RetainEdge,
        replacement: (Self, Option<Self>),
    ) -> Result<(Self, Option<Self>)> {
        let child_root_distance = branch_root_distance + 1;
        let (left, right) = replacement;
        assert_eq!(left.root_distance, child_root_distance);
        if let Some(ref right) = right {
            assert_eq!(right.root_distance, child_root_distance);
        }
        let child_index = edge.child_index(accessor);

        let replacement_upper_key = right
            .as_ref()
            .map_or_else(|| left.upper_key.clone(), |right| right.upper_key.clone());
        let branch_upper_key = match edge {
            RetainEdge::Left => {
                assert_eq!(replacement_upper_key.as_deref(), accessor.key(child_index));
                branch_upper_key
            }
            RetainEdge::Right => replacement_upper_key,
        };

        let child_capacity = accessor.count_children() + usize::from(right.is_some());
        let mut builder = BranchBuilder::new(
            context.page_allocator,
            context.allocated,
            child_capacity,
            K::fixed_width(),
        );

        match (edge, right.as_ref()) {
            (_, None) => {
                builder.push_all(accessor);
                builder.replace_child(child_index, left.page, left.checksum);
            }
            (RetainEdge::Left, Some(right)) => {
                builder.push_child(left.page, left.checksum);
                builder.push_key(
                    left.upper_key
                        .as_ref()
                        .expect("non-final retain child must have an upper bound"),
                );
                builder.push_all(accessor);
                builder.replace_child(1, right.page, right.checksum);
            }
            (RetainEdge::Right, Some(right)) => {
                builder.push_all(accessor);
                builder.replace_child(child_index, left.page, left.checksum);
                builder.push_key(
                    left.upper_key
                        .as_ref()
                        .expect("non-final retain child must have an upper bound"),
                );
                builder.push_child(right.page, right.checksum);
            }
        }

        Self::build_branch_from_builder(builder, branch_root_distance, branch_upper_key)
    }

    fn build_parent_subtrees<K: Key, V: Value>(
        context: &mut RetainBuilderContext<'_, K, V>,
        children: &[Self],
        child_root_distance: u32,
    ) -> Result<(Self, Option<Self>)> {
        assert!(children.len() > 1);
        let root_distance = child_root_distance
            .checked_sub(1)
            .expect("retain branch children must be below their parent");
        let mut builder = BranchBuilder::new(
            context.page_allocator,
            context.allocated,
            children.len(),
            K::fixed_width(),
        );
        for (i, child) in children.iter().enumerate() {
            builder.push_child(child.page, child.checksum);
            if i + 1 < children.len() {
                builder.push_key(
                    child
                        .upper_key
                        .as_ref()
                        .expect("non-final retain child must have an upper bound"),
                );
            }
        }
        let upper_key = children.last().unwrap().upper_key.clone();
        Self::build_branch_from_builder(builder, root_distance, upper_key)
    }

    fn build_branch_from_builder(
        builder: BranchBuilder<'_, '_>,
        root_distance: u32,
        upper_key: Option<Vec<u8>>,
    ) -> Result<(Self, Option<Self>)> {
        if builder.should_split() {
            let (left, separator, right) = builder.build_split()?;
            let separator = separator.to_vec();
            let left_page = left.get_page_number();
            let right_page = right.get_page_number();
            Ok((
                Self::new(left_page, DEFERRED, Some(separator), root_distance),
                Some(Self::new(right_page, DEFERRED, upper_key, root_distance)),
            ))
        } else {
            let page = builder.build()?;
            Ok((
                Self::new(page.get_page_number(), DEFERRED, upper_key, root_distance),
                None,
            ))
        }
    }
}

// Retained leaf entries that have not yet been sealed into a page.
struct InProgressLeaf {
    entries: Vec<RetainLeafEntry>,
    root_distance: Option<u32>,
}

impl InProgressLeaf {
    fn new() -> Self {
        Self {
            entries: vec![],
            root_distance: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.root_distance.is_none()
    }

    fn required_bytes<K: Key, V: Value>(entries: &[RetainLeafEntry]) -> usize {
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

    fn push<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        key: &[u8],
        value: &[u8],
        root_distance: u32,
    ) -> Result<Option<RetainSubtree>> {
        let mut flushed = None;
        if let Some(leaf_root_distance) = self.root_distance {
            assert_eq!(leaf_root_distance, root_distance);
        } else {
            self.root_distance = Some(root_distance);
        }

        self.entries.push((key.to_vec(), value.to_vec()));
        if self.entries.len() > 1
            && Self::required_bytes::<K, V>(&self.entries) > context.page_allocator.get_page_size()
        {
            let overflow = self.entries.pop().unwrap();
            if self.is_full_enough(context) {
                assert!(flushed.is_none());
                flushed = self.seal_if_sufficient(context)?;
                self.root_distance = Some(root_distance);
                self.entries.push(overflow);
            } else {
                self.entries.push(overflow);
                assert!(flushed.is_none());
                flushed = self.seal_if_sufficient(context)?;
            }
        }

        Ok(flushed)
    }

    fn is_full_enough<K: Key, V: Value>(&self, context: &RetainBuilderContext<'_, K, V>) -> bool {
        Self::required_bytes::<K, V>(&self.entries) >= context.page_allocator.get_page_size() / 3
    }

    fn seal_if_sufficient<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<Option<RetainSubtree>> {
        if self.entries.is_empty() {
            self.root_distance = None;
            return Ok(None);
        }

        if !self.is_full_enough(context) {
            return Ok(None);
        }

        self.seal_unchecked(context)
    }

    fn seal_unchecked<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<Option<RetainSubtree>> {
        if self.entries.is_empty() {
            self.root_distance = None;
            return Ok(None);
        }

        let entries = std::mem::take(&mut self.entries);
        let root_distance = self
            .root_distance
            .take()
            .expect("retain leaf entries must have a root distance");
        let upper_key = entries.last().unwrap().0.clone();
        let mut builder = LeafBuilder::new(
            context.page_allocator,
            context.allocated,
            entries.len(),
            K::fixed_width(),
            V::fixed_width(),
        );
        for (key, value) in &entries {
            builder.push(key, value);
        }
        let page = builder.build()?;
        Ok(Some(RetainSubtree::new(
            page.get_page_number(),
            DEFERRED,
            Some(upper_key),
            root_distance,
        )))
    }

    fn take_entries(&mut self) -> Option<(Vec<RetainLeafEntry>, u32)> {
        if self.entries.is_empty() {
            self.root_distance = None;
            None
        } else {
            let entries = std::mem::take(&mut self.entries);
            let root_distance = self
                .root_distance
                .take()
                .expect("retain leaf entries must have a root distance");
            Some((entries, root_distance))
        }
    }

    fn graft_to_subtree<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        subtree: RetainSubtree,
        edge: RetainEdge,
    ) -> Result<(RetainSubtree, Option<RetainSubtree>)> {
        let (entries, root_distance) = self
            .take_entries()
            .expect("retain leaf entries must exist before subtree grafting");
        subtree.absorb_leaf_entries(context, entries, root_distance, edge)
    }
}

pub(super) struct RetainSubtreeBuilder {
    // In-progress left-to-right replacement stream. The builder keeps the
    // right subtree in each adjacent pair at the same root distance or farther
    // from the root, so the pending frontier is bounded by one accumulation
    // buffer per root distance.
    frontier: Vec<RetainSubtree>,
    leaf: InProgressLeaf,
}

impl RetainSubtreeBuilder {
    pub(super) fn new() -> Self {
        Self {
            frontier: vec![],
            leaf: InProgressLeaf::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.frontier.is_empty() && self.leaf.is_empty()
    }

    fn normalize_frontier<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<()> {
        while let Some(index) = self.rightmost_descent() {
            self.graft_pair_at(context, index)?;
            assert!(
                self.rightmost_descent()
                    .is_none_or(|next_index| next_index < index),
                "retain descent graft did not progress"
            );
        }

        while let Some(parent_range) =
            self.same_root_distance_prefix_ready_for_parent::<K, V>(context)
        {
            let old_len = self.frontier.len();
            let start = parent_range.start;
            let children: Vec<_> = self.frontier.drain(parent_range).collect();
            let child_root_distance = children[0].root_distance;
            debug_assert!(
                children
                    .iter()
                    .all(|child| child.root_distance == child_root_distance)
            );
            let parents =
                RetainSubtree::build_parent_subtrees(context, &children, child_root_distance)?;
            let (parent, right_parent) = parents;
            assert!(right_parent.is_none());
            self.frontier.insert(start, parent);
            assert!(self.frontier.len() < old_len);
            assert!(self.rightmost_descent().is_none());
        }

        Ok(())
    }

    fn rightmost_descent(&self) -> Option<usize> {
        self.frontier
            .windows(2)
            .rposition(|pair| pair[0].root_distance > pair[1].root_distance)
    }

    fn first_root_distance_change(&self) -> Option<usize> {
        self.frontier
            .windows(2)
            .position(|pair| pair[0].root_distance != pair[1].root_distance)
    }

    fn same_root_distance_prefix_ready_for_parent<K: Key, V: Value>(
        &self,
        context: &RetainBuilderContext<'_, K, V>,
    ) -> Option<Range<usize>> {
        if self.frontier.len() <= 1 {
            return None;
        }

        let root_distance = self.frontier.last().unwrap().root_distance;
        let same_distance_start = self
            .frontier
            .iter()
            .rposition(|subtree| subtree.root_distance != root_distance)
            .map_or(0, |index| index + 1);
        if self.frontier[same_distance_start].root_distance == 0 {
            return None;
        }

        let page_size = context.page_allocator.get_page_size();
        let min_size = page_size / 3;
        for right_remainder_start in same_distance_start + 2..self.frontier.len() - 1 {
            let left_parent_with_next = &self.frontier[same_distance_start..=right_remainder_start];
            let right_remainder = &self.frontier[right_remainder_start..];
            if Self::branch_group_required_bytes::<K>(left_parent_with_next) > page_size {
                if Self::branch_group_required_bytes::<K>(right_remainder) >= min_size {
                    return Some(same_distance_start..right_remainder_start);
                }
                return None;
            }
        }
        None
    }

    fn graft_pair_at<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        index: usize,
    ) -> Result<()> {
        let right = self.frontier.remove(index + 1);
        let left = self.frontier.remove(index);
        let (grafted, right_grafted) = RetainSubtree::graft_ordered(context, left, right)?;
        self.frontier.insert(index, grafted);
        if let Some(right_grafted) = right_grafted {
            self.frontier.insert(index + 1, right_grafted);
        }
        Ok(())
    }

    fn branch_group_required_bytes<K: Key>(children: &[RetainSubtree]) -> usize {
        assert!(children.len() > 1);
        let total_key_bytes = children
            .iter()
            .take(children.len() - 1)
            .map(|child| {
                child
                    .upper_key
                    .as_ref()
                    .expect("non-final retain child must have an upper bound")
                    .len()
            })
            .sum();
        RawBranchBuilder::required_bytes(children.len() - 1, total_key_bytes, K::fixed_width())
    }

    pub(super) fn push_leaf_entry<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        key: &[u8],
        value: &[u8],
        root_distance: u32,
    ) -> Result<()> {
        if let Some(leaf) = self.leaf.push(context, key, value, root_distance)? {
            self.frontier.push(leaf);
            self.normalize_frontier(context)?;
        }
        Ok(())
    }

    pub(super) fn push_subtree<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        subtree: RetainSubtree,
    ) -> Result<()> {
        if let Some(leaf) = self.leaf.seal_if_sufficient(context)? {
            self.frontier.push(leaf);
            self.normalize_frontier(context)?;
        }

        if self.leaf.is_empty() {
            self.frontier.push(subtree);
        } else {
            let (subtree, right_subtree) =
                self.leaf
                    .graft_to_subtree(context, subtree, RetainEdge::Left)?;
            self.frontier.push(subtree);
            if let Some(right_subtree) = right_subtree {
                self.frontier.push(right_subtree);
            }
        }
        self.normalize_frontier(context)
    }

    pub(super) fn append<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        child_builder: RetainSubtreeBuilder,
    ) -> Result<()> {
        let RetainSubtreeBuilder { frontier, mut leaf } = child_builder;

        for subtree in frontier {
            self.push_subtree(context, subtree)?;
        }
        if let Some((entries, root_distance)) = leaf.take_entries() {
            for (key, value) in entries {
                self.push_leaf_entry(context, &key, &value, root_distance)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish_root<K: Key, V: Value>(
        mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        if let Some(leaf) = self.leaf.seal_if_sufficient(context)? {
            self.frontier.push(leaf);
            self.normalize_frontier(context)?;
        }
        if !self.leaf.is_empty() {
            if let Some(subtree) = self.frontier.pop() {
                let (subtree, right_subtree) =
                    self.leaf
                        .graft_to_subtree(context, subtree, RetainEdge::Right)?;
                self.frontier.push(subtree);
                if let Some(right_subtree) = right_subtree {
                    self.frontier.push(right_subtree);
                }
            } else if let Some(leaf) = self.leaf.seal_unchecked(context)? {
                self.frontier.push(leaf);
            }
        }

        self.normalize_frontier(context)?;
        while self.frontier.len() > 1 {
            while let Some(index) = self.first_root_distance_change() {
                let old_len = self.frontier.len();
                self.graft_pair_at(context, index)?;
                assert!(
                    self.frontier.len() < old_len
                        || self
                            .first_root_distance_change()
                            .is_none_or(|next_index| next_index > index),
                    "retain root-distance graft did not progress"
                );
            }

            if self.frontier.len() <= 1 {
                break;
            }

            // Multiple subtrees at root distance zero need a new root above
            // them. Bump their relative distances before building that parent.
            if self
                .frontier
                .iter()
                .all(|subtree| subtree.root_distance == 0)
            {
                for subtree in &mut self.frontier {
                    subtree.root_distance += 1;
                }
            }
            let child_root_distance = self.frontier[0].root_distance;
            debug_assert!(
                self.frontier
                    .iter()
                    .all(|subtree| subtree.root_distance == child_root_distance)
            );
            let parents =
                RetainSubtree::build_parent_subtrees(context, &self.frontier, child_root_distance)?;
            let (parent, right_parent) = parents;
            let old_len = self.frontier.len();
            self.frontier.clear();
            self.frontier.push(parent);
            if let Some(right_parent) = right_parent {
                self.frontier.push(right_parent);
            }
            assert!(self.frontier.len() < old_len);
            self.normalize_frontier(context)?;
        }

        Ok(self.frontier.pop().map(|root| (root.page, root.checksum)))
    }
}
