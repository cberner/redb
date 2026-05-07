use crate::Result;
use crate::tree_store::btree_base::{
    BRANCH, BranchAccessor, BranchBuilder, Checksum, DEFERRED, LEAF, LeafAccessor, LeafBuilder,
    RawBranchBuilder, RawLeafBuilder,
};
use crate::tree_store::btree_iters::{BtreeRangeIter, RangeLeafEntry, RangeSubtree, RangeVisit};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{
    BtreeHeader, PageAllocator, PageHint, PageNumber, PageResolver, PageTrackerPolicy,
};
use crate::types::{Key, Value};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};
use std::sync::Mutex;

type RetainLeafEntry = (Vec<u8>, Vec<u8>);
type RetainLeafEntries = VecDeque<RetainLeafEntry>;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum BuildDirection {
    LeftToRight,
    #[allow(dead_code)]
    RightToLeft,
}

impl BuildDirection {
    fn leaf_entries_before_subtree_edge(self) -> RetainEdge {
        match self {
            Self::LeftToRight => RetainEdge::Left,
            Self::RightToLeft => RetainEdge::Right,
        }
    }

    fn leaf_entries_after_subtree_edge(self) -> RetainEdge {
        match self {
            Self::LeftToRight => RetainEdge::Right,
            Self::RightToLeft => RetainEdge::Left,
        }
    }

    fn same_root_distance_edge_run(
        self,
        frontier: &VecDeque<RetainSubtree>,
    ) -> Option<Range<usize>> {
        if frontier.len() <= 1 {
            return None;
        }

        match self {
            Self::LeftToRight => {
                let root_distance = frontier.back()?.root_distance;
                let start = frontier
                    .iter()
                    .rposition(|subtree| subtree.root_distance != root_distance)
                    .map_or(0, |index| index + 1);
                Some(start..frontier.len())
            }
            Self::RightToLeft => {
                let root_distance = frontier.front()?.root_distance;
                let end = frontier
                    .iter()
                    .position(|subtree| subtree.root_distance != root_distance)
                    .unwrap_or(frontier.len());
                Some(0..end)
            }
        }
    }
}

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

pub(super) struct Retain {
    builder: RetainSubtreeBuilder,
    in_progress: InProgressSubtree,
    current_leaf: Option<CurrentRetainLeaf>,
    removed: u64,
}

struct CurrentRetainLeaf {
    page: PageImpl,
    subtree: RangeSubtree,
    removed_indexes: Vec<usize>,
}

impl Retain {
    pub(super) fn new() -> Self {
        Self {
            builder: RetainSubtreeBuilder::left_to_right(),
            in_progress: InProgressSubtree::new(),
            current_leaf: None,
            removed: 0,
        }
    }

    pub(super) fn execute<'r, K, V, KR, F>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        header: BtreeHeader,
        range: &'_ impl RangeBounds<KR>,
        resolver: PageResolver,
        predicate: &mut F,
    ) -> Result
    where
        K: Key + 'static,
        V: Value + 'static,
        KR: Borrow<K::SelfType<'r>> + 'r,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        let mut iter = BtreeRangeIter::<K, V>::new_with_subtree_metadata(
            range,
            Some(header),
            resolver,
            PageHint::None,
        )?;
        while let Some(result) =
            iter.next_with_visitor(|event| self.visit(context, event, predicate))
        {
            result?;
        }
        Ok(())
    }

    fn visit<K: Key, V: Value, F>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        event: RangeVisit<'_>,
        predicate: &mut F,
    ) -> Result
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        match event {
            RangeVisit::BranchEnter { branch } => {
                self.in_progress.enter_branch(branch.clone());
                Ok(())
            }
            RangeVisit::SkippedSubtree { subtree } => {
                self.in_progress
                    .push_subtree(RetainSubtree::from_range(subtree.clone()));
                Ok(())
            }
            RangeVisit::LeafEntry { entry } => self.visit_leaf_entry(context, entry, predicate),
            RangeVisit::LeafExit { subtree } => {
                let page_number = subtree.page_number();
                if self.current_leaf_page() == Some(page_number) {
                    self.complete_current_leaf(context)?;
                }
                Ok(())
            }
            RangeVisit::BranchExit { branch } => {
                if let Some(replaced_page) =
                    self.in_progress
                        .exit_branch_into(context, &mut self.builder, branch)?
                {
                    context.conditional_free(replaced_page);
                }
                Ok(())
            }
        }
    }

    pub(super) fn finish<K: Key, V: Value>(
        mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        header: BtreeHeader,
    ) -> Result<Option<BtreeHeader>> {
        self.complete_current_leaf(context)?;
        if self.removed == 0 {
            return Ok(Some(header));
        }

        let replaced_pages = self.in_progress.finish_into(context, &mut self.builder)?;
        for page in replaced_pages {
            context.conditional_free(page);
        }

        if let Some((root, checksum)) = self.builder.finish_root(context)? {
            Ok(Some(BtreeHeader::new(
                root,
                checksum,
                header.length - self.removed,
            )))
        } else {
            Ok(None)
        }
    }

    fn visit_leaf_entry<K: Key, V: Value, F>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        entry: RangeLeafEntry<'_>,
        predicate: &mut F,
    ) -> Result
    where
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    {
        assert!(
            self.current_leaf_page()
                .is_none_or(|page| page == entry.page_number())
        );
        let new_leaf = self.current_leaf.is_none();
        if new_leaf {
            self.current_leaf = Some(CurrentRetainLeaf::new(entry));
        }

        let entry_accessor = entry.entry::<K, V>();
        let key = entry_accessor.key();

        if !predicate(K::from_bytes(key), V::from_bytes(entry_accessor.value())) {
            self.mark_removed::<K, V>(context, entry.entry_index())?;
        }
        Ok(())
    }

    fn mark_removed<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        entry_index: usize,
    ) -> Result {
        let leaf = self
            .current_leaf
            .as_mut()
            .expect("range visitor must set current leaf before predicate");
        let first_removed_in_leaf = leaf.mark_removed(entry_index);
        if first_removed_in_leaf {
            self.in_progress.mark_changed();
            self.in_progress.flush_into(context, &mut self.builder)?;
        }
        self.removed += 1;
        Ok(())
    }

    fn complete_current_leaf<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result {
        if let Some(leaf) = self.current_leaf.take() {
            leaf.complete_into(context, &mut self.in_progress, &mut self.builder)?;
        }
        Ok(())
    }

    fn current_leaf_page(&self) -> Option<PageNumber> {
        self.current_leaf
            .as_ref()
            .map(CurrentRetainLeaf::page_number)
    }
}

impl CurrentRetainLeaf {
    fn new(entry: RangeLeafEntry<'_>) -> Self {
        Self {
            page: entry.page().clone(),
            subtree: entry.subtree().clone(),
            removed_indexes: vec![],
        }
    }

    fn page_number(&self) -> PageNumber {
        self.subtree.page_number()
    }

    fn root_distance(&self) -> u32 {
        self.subtree.root_distance()
    }

    fn page(&self) -> &PageImpl {
        &self.page
    }

    fn mark_removed(&mut self, index: usize) -> bool {
        let first_removed = self.removed_indexes.is_empty();
        assert!(
            self.removed_indexes
                .last()
                .is_none_or(|last_index| *last_index < index)
        );
        self.removed_indexes.push(index);
        first_removed
    }

    fn complete_into<K: Key, V: Value>(
        self,
        context: &mut RetainBuilderContext<'_, K, V>,
        in_progress: &mut InProgressSubtree,
        builder: &mut RetainSubtreeBuilder,
    ) -> Result {
        if self.removed_indexes.is_empty() {
            in_progress.push_subtree(self.into_subtree());
            return Ok(());
        }

        let old_page = self.page_number();
        let root_distance = self.root_distance();
        {
            let accessor =
                LeafAccessor::new(self.page().memory(), K::fixed_width(), V::fixed_width());
            builder.push_leaf_entries_except(
                context,
                &accessor,
                root_distance,
                &self.removed_indexes,
            )?;
        }
        drop(self);
        context.conditional_free(old_page);
        Ok(())
    }

    fn into_subtree(self) -> RetainSubtree {
        RetainSubtree::from_range(self.subtree)
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

    pub(super) fn from_range(subtree: RangeSubtree) -> Self {
        let (page, checksum, upper_key, root_distance) = subtree.into_parts();
        Self::new(page, checksum, upper_key, root_distance)
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
        entries: RetainLeafEntries,
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
                    RetainEdge::Right => Some(entries.back().unwrap().0.clone()),
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

// Tracks unchanged subtrees completed by the range traversal.
//
// `branch_stack` mirrors the active BranchEnter/BranchExit stack. Completed
// unchanged subtrees are kept in their open parent BranchFrame, or in
// `completed` when no branch is open. On BranchExit, an unchanged frame
// collapses back to its original branch page; a changed frame flushes its
// completed children into the replacement builder.
pub(super) struct InProgressSubtree {
    completed: Vec<RetainSubtree>,
    branch_stack: Vec<BranchFrame>,
}

impl InProgressSubtree {
    pub(super) fn new() -> Self {
        Self {
            completed: vec![],
            branch_stack: vec![],
        }
    }

    pub(super) fn enter_branch(&mut self, branch: RangeSubtree) {
        assert!(
            self.branch_stack
                .last()
                .is_none_or(|current| current.page_number() != branch.page_number()),
            "range iterator emitted duplicate branch enter"
        );
        self.branch_stack.push(BranchFrame::new(branch));
    }

    // Attach a completed unchanged subtree to the nearest open parent branch.
    // If there is no parent, it is ready to be flushed directly to the builder.
    pub(super) fn push_subtree(&mut self, subtree: RetainSubtree) {
        if let Some(branch) = self.branch_stack.last_mut() {
            branch.push_child(subtree);
        } else {
            self.completed.push(subtree);
        }
    }

    pub(super) fn mark_changed(&mut self) {
        if let Some(branch) = self.branch_stack.last_mut() {
            branch.mark_changed();
        }
    }

    pub(super) fn exit_branch_into<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        builder: &mut RetainSubtreeBuilder,
        branch: &RangeSubtree,
    ) -> Result<Option<PageNumber>> {
        let frame = self
            .branch_stack
            .pop()
            .expect("range iterator emitted branch exit without matching enter");
        assert_eq!(
            frame.page_number(),
            branch.page_number(),
            "range iterator emitted branch exit out of order"
        );

        self.finish_branch_frame(context, builder, frame)
    }

    // A changed leaf makes all previously completed unchanged subtrees part of
    // the replacement stream. Keep the branch stack open for later exit events.
    pub(super) fn flush_into<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        builder: &mut RetainSubtreeBuilder,
    ) -> Result<()> {
        for subtree in self.completed.drain(..) {
            builder.push_subtree(context, subtree)?;
        }
        for branch in &mut self.branch_stack {
            branch.flush_children_into(context, builder)?;
        }
        Ok(())
    }

    pub(super) fn finish_into<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        builder: &mut RetainSubtreeBuilder,
    ) -> Result<Vec<PageNumber>> {
        let mut replaced_pages = vec![];
        while let Some(frame) = self.branch_stack.pop() {
            if let Some(page) = self.finish_branch_frame(context, builder, frame)? {
                replaced_pages.push(page);
            }
        }
        self.flush_into(context, builder)?;
        Ok(replaced_pages)
    }

    fn finish_branch_frame<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        builder: &mut RetainSubtreeBuilder,
        mut frame: BranchFrame,
    ) -> Result<Option<PageNumber>> {
        // Unchanged frames collapse to their original branch page. Changed
        // frames flush their unchanged children and cause the parent to rebuild.
        if frame.is_changed() {
            if let Some(parent) = self.branch_stack.last_mut() {
                parent.mark_changed();
            }
            let old_page = frame.page_number();
            frame.flush_children_into(context, builder)?;
            Ok(Some(old_page))
        } else {
            self.push_subtree(frame.into_retain_subtree());
            Ok(None)
        }
    }
}

// One open branch in the range traversal stack. Its `unchanged_children` can
// still collapse back into `branch` if no change is found before BranchExit.
struct BranchFrame {
    branch: RangeSubtree,
    unchanged_children: Vec<RetainSubtree>,
    changed: bool,
}

impl BranchFrame {
    fn new(branch: RangeSubtree) -> Self {
        Self {
            branch,
            unchanged_children: vec![],
            changed: false,
        }
    }

    fn page_number(&self) -> PageNumber {
        self.branch.page_number()
    }

    fn is_changed(&self) -> bool {
        self.changed
    }

    fn mark_changed(&mut self) {
        self.changed = true;
    }

    fn push_child(&mut self, subtree: RetainSubtree) {
        self.unchanged_children.push(subtree);
    }

    fn flush_children_into<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        builder: &mut RetainSubtreeBuilder,
    ) -> Result<()> {
        for subtree in self.unchanged_children.drain(..) {
            builder.push_subtree(context, subtree)?;
        }
        Ok(())
    }

    fn into_retain_subtree(self) -> RetainSubtree {
        RetainSubtree::from_range(self.branch)
    }
}

// Retained leaf entries that have not yet been sealed into a page.
struct InProgressLeaf {
    entries: RetainLeafEntries,
    entries_bytes: usize,
    root_distance: Option<u32>,
}

impl InProgressLeaf {
    fn new() -> Self {
        Self {
            entries: RetainLeafEntries::new(),
            entries_bytes: 0,
            root_distance: None,
        }
    }

    fn is_empty(&self) -> bool {
        assert!(!self.entries.is_empty() || self.entries_bytes == 0);
        self.entries.is_empty() && self.root_distance.is_none()
    }

    fn required_bytes<K: Key, V: Value>(&self) -> usize {
        RawLeafBuilder::required_bytes(
            self.entries.len(),
            self.entries_bytes,
            K::fixed_width(),
            V::fixed_width(),
        )
    }

    fn push_entry(&mut self, entry: RetainLeafEntry, direction: BuildDirection) {
        self.entries_bytes += entry.0.len() + entry.1.len();
        match direction {
            BuildDirection::LeftToRight => self.entries.push_back(entry),
            BuildDirection::RightToLeft => self.entries.push_front(entry),
        }
    }

    fn pop_overflow_entry(&mut self, direction: BuildDirection) -> RetainLeafEntry {
        let entry = match direction {
            BuildDirection::LeftToRight => self.entries.pop_back().unwrap(),
            BuildDirection::RightToLeft => self.entries.pop_front().unwrap(),
        };
        self.entries_bytes -= entry.0.len() + entry.1.len();
        entry
    }

    fn push<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        key: &[u8],
        value: &[u8],
        root_distance: u32,
        direction: BuildDirection,
    ) -> Result<Option<RetainSubtree>> {
        let mut flushed = None;
        if let Some(leaf_root_distance) = self.root_distance {
            assert_eq!(leaf_root_distance, root_distance);
        } else {
            self.root_distance = Some(root_distance);
        }

        self.push_entry((key.to_vec(), value.to_vec()), direction);
        if self.entries.len() > 1
            && self.required_bytes::<K, V>() > context.page_allocator.get_page_size()
        {
            let overflow = self.pop_overflow_entry(direction);
            if self.is_full_enough(context) {
                assert!(flushed.is_none());
                flushed = self.seal_if_sufficient(context)?;
                self.root_distance = Some(root_distance);
                self.push_entry(overflow, direction);
            } else {
                self.push_entry(overflow, direction);
                assert!(flushed.is_none());
                flushed = self.seal_if_sufficient(context)?;
            }
        }

        Ok(flushed)
    }

    fn is_full_enough<K: Key, V: Value>(&self, context: &RetainBuilderContext<'_, K, V>) -> bool {
        self.required_bytes::<K, V>() >= context.page_allocator.get_page_size() / 3
    }

    fn seal_if_sufficient<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<Option<RetainSubtree>> {
        if self.entries.is_empty() {
            self.root_distance = None;
            self.entries_bytes = 0;
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
        self.entries_bytes = 0;
        let root_distance = self
            .root_distance
            .take()
            .expect("retain leaf entries must have a root distance");
        let upper_key = entries.back().unwrap().0.clone();
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

    fn take_entries(&mut self) -> Option<(RetainLeafEntries, u32)> {
        if self.entries.is_empty() {
            self.root_distance = None;
            self.entries_bytes = 0;
            None
        } else {
            let entries = std::mem::take(&mut self.entries);
            self.entries_bytes = 0;
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
    // In-progress replacement stream, always ordered left-to-right by key.
    // Direction only controls which end receives new stream items.
    direction: BuildDirection,
    frontier: VecDeque<RetainSubtree>,
    leaf: InProgressLeaf,
}

impl RetainSubtreeBuilder {
    fn new(direction: BuildDirection) -> Self {
        Self {
            direction,
            frontier: VecDeque::new(),
            leaf: InProgressLeaf::new(),
        }
    }

    pub(super) fn left_to_right() -> Self {
        Self::new(BuildDirection::LeftToRight)
    }

    #[allow(dead_code)]
    pub(super) fn right_to_left() -> Self {
        Self::new(BuildDirection::RightToLeft)
    }

    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.frontier.is_empty() && self.leaf.is_empty()
    }

    fn normalize_frontier<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<()> {
        while let Some(index) = self.active_edge_descent() {
            self.graft_pair_at(context, index)?;
        }

        while let Some(parent_range) =
            self.same_root_distance_edge_ready_for_parent::<K, V>(context)
        {
            let old_len = self.frontier.len();
            let start = parent_range.start;
            let children: Vec<_> = self.frontier.drain(parent_range).collect();
            let child_root_distance = children[0].root_distance;
            assert!(
                children
                    .iter()
                    .all(|child| child.root_distance == child_root_distance)
            );
            let parents =
                RetainSubtree::build_parent_subtrees(context, &children, child_root_distance)?;
            let (parent, right_parent) = parents;
            self.frontier.insert(start, parent);
            if let Some(right_parent) = right_parent {
                self.frontier.insert(start + 1, right_parent);
            }
            assert!(self.frontier.len() < old_len);
            assert!(self.active_edge_descent().is_none());
        }

        Ok(())
    }

    fn active_edge_descent(&self) -> Option<usize> {
        let adjacent_subtrees = || {
            self.frontier
                .iter()
                .zip(self.frontier.iter().skip(1))
                .enumerate()
        };
        match self.direction {
            BuildDirection::LeftToRight => adjacent_subtrees()
                .rev()
                .find(|(_, (left, right))| left.root_distance > right.root_distance)
                .map(|(index, _)| index),
            BuildDirection::RightToLeft => adjacent_subtrees()
                .find(|(_, (left, right))| left.root_distance < right.root_distance)
                .map(|(index, _)| index),
        }
    }

    fn first_root_distance_change(&self) -> Option<usize> {
        let adjacent_subtrees = || {
            self.frontier
                .iter()
                .zip(self.frontier.iter().skip(1))
                .enumerate()
        };
        match self.direction {
            BuildDirection::LeftToRight => adjacent_subtrees()
                .find(|(_, (left, right))| left.root_distance != right.root_distance)
                .map(|(index, _)| index),
            BuildDirection::RightToLeft => adjacent_subtrees()
                .rev()
                .find(|(_, (left, right))| left.root_distance != right.root_distance)
                .map(|(index, _)| index),
        }
    }

    fn root_distance_change_progressed(&self, index: usize) -> bool {
        self.first_root_distance_change()
            .is_none_or(|next_index| match self.direction {
                BuildDirection::LeftToRight => next_index > index,
                BuildDirection::RightToLeft => next_index < index,
            })
    }

    fn same_root_distance_edge_ready_for_parent<K: Key, V: Value>(
        &self,
        context: &RetainBuilderContext<'_, K, V>,
    ) -> Option<Range<usize>> {
        const MIN_CHILDREN_PER_SIDE: usize = 2;

        let run = self.direction.same_root_distance_edge_run(&self.frontier)?;
        if self.frontier[run.start].root_distance == 0 {
            return None;
        }

        if run.end - run.start < MIN_CHILDREN_PER_SIDE * 2 {
            return None;
        }

        let page_size = context.page_allocator.get_page_size();
        let min_size = page_size / 3;

        let first_split = run.start + MIN_CHILDREN_PER_SIDE;
        let last_split = run.end - MIN_CHILDREN_PER_SIDE;
        // Probe one child beyond the candidate parent. If that would overflow,
        // the unextended parent is ready as long as the edge remainder is large enough.
        match self.direction {
            BuildDirection::LeftToRight => {
                for split in first_split..=last_split {
                    let parent = run.start..split;
                    let parent_with_next_child_end = split + 1;
                    let parent_with_next_child = run.start..parent_with_next_child_end;
                    let edge_remainder = split..run.end;
                    if self.branch_group_required_bytes::<K>(parent_with_next_child) > page_size {
                        if self.branch_group_required_bytes::<K>(edge_remainder) >= min_size {
                            return Some(parent);
                        }
                        return None;
                    }
                }
            }
            BuildDirection::RightToLeft => {
                for split in (first_split..=last_split).rev() {
                    let parent = split..run.end;
                    let parent_with_previous_child = split - 1..run.end;
                    let edge_remainder = run.start..split;
                    if self.branch_group_required_bytes::<K>(parent_with_previous_child) > page_size
                    {
                        if self.branch_group_required_bytes::<K>(edge_remainder) >= min_size {
                            return Some(parent);
                        }
                        return None;
                    }
                }
            }
        }
        None
    }

    fn graft_pair_at<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        index: usize,
    ) -> Result<()> {
        let second = self.frontier.remove(index + 1).unwrap();
        let first = self.frontier.remove(index).unwrap();
        let (grafted, right_grafted) = RetainSubtree::graft_ordered(context, first, second)?;
        self.frontier.insert(index, grafted);
        if let Some(right_grafted) = right_grafted {
            self.frontier.insert(index + 1, right_grafted);
        }
        Ok(())
    }

    fn push_subtree_to_active_edge(&mut self, subtree: RetainSubtree) {
        match self.direction {
            BuildDirection::LeftToRight => self.frontier.push_back(subtree),
            BuildDirection::RightToLeft => self.frontier.push_front(subtree),
        }
    }

    fn push_ordered_pair_to_active_edge(
        &mut self,
        left: RetainSubtree,
        right: Option<RetainSubtree>,
    ) {
        match self.direction {
            BuildDirection::LeftToRight => {
                self.frontier.push_back(left);
                if let Some(right) = right {
                    self.frontier.push_back(right);
                }
            }
            BuildDirection::RightToLeft => {
                if let Some(right) = right {
                    self.frontier.push_front(right);
                }
                self.frontier.push_front(left);
            }
        }
    }

    fn pop_subtree_from_active_edge(&mut self) -> Option<RetainSubtree> {
        match self.direction {
            BuildDirection::LeftToRight => self.frontier.pop_back(),
            BuildDirection::RightToLeft => self.frontier.pop_front(),
        }
    }

    fn branch_group_required_bytes<K: Key>(&self, range: Range<usize>) -> usize {
        assert!(range.end - range.start > 1);
        let total_key_bytes: usize = (range.start..range.end - 1)
            .map(|index| {
                self.frontier[index]
                    .upper_key
                    .as_ref()
                    .expect("non-final retain child must have an upper bound")
                    .len()
            })
            .sum();
        RawBranchBuilder::required_bytes(
            range.end - range.start - 1,
            total_key_bytes,
            K::fixed_width(),
        )
    }

    pub(super) fn push_leaf_entry<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        key: &[u8],
        value: &[u8],
        root_distance: u32,
    ) -> Result<()> {
        if let Some(leaf) = self
            .leaf
            .push(context, key, value, root_distance, self.direction)?
        {
            self.push_subtree_to_active_edge(leaf);
            self.normalize_frontier(context)?;
        }
        Ok(())
    }

    pub(super) fn push_leaf_entries_except<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        accessor: &LeafAccessor<'_>,
        root_distance: u32,
        removed_indexes: &[usize],
    ) -> Result<()> {
        match self.direction {
            BuildDirection::LeftToRight => {
                let mut removed_pos = 0;
                for i in 0..accessor.num_pairs() {
                    if removed_indexes
                        .get(removed_pos)
                        .is_some_and(|removed| *removed == i)
                    {
                        removed_pos += 1;
                    } else {
                        let entry = accessor.entry(i).unwrap();
                        self.push_leaf_entry(context, entry.key(), entry.value(), root_distance)?;
                    }
                }
                assert_eq!(removed_pos, removed_indexes.len());
            }
            BuildDirection::RightToLeft => {
                let mut removed_pos = removed_indexes.len();
                for i in (0..accessor.num_pairs()).rev() {
                    if removed_pos > 0
                        && removed_indexes
                            .get(removed_pos - 1)
                            .is_some_and(|removed| *removed == i)
                    {
                        removed_pos -= 1;
                    } else {
                        let entry = accessor.entry(i).unwrap();
                        self.push_leaf_entry(context, entry.key(), entry.value(), root_distance)?;
                    }
                }
                assert_eq!(removed_pos, 0);
            }
        }
        Ok(())
    }

    pub(super) fn push_subtree<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        subtree: RetainSubtree,
    ) -> Result<()> {
        if let Some(leaf) = self.leaf.seal_if_sufficient(context)? {
            self.push_subtree_to_active_edge(leaf);
            self.normalize_frontier(context)?;
        }

        if self.leaf.is_empty() {
            self.push_subtree_to_active_edge(subtree);
        } else {
            let edge = self.direction.leaf_entries_before_subtree_edge();
            let (left, right) = self.leaf.graft_to_subtree(context, subtree, edge)?;
            self.push_ordered_pair_to_active_edge(left, right);
        }
        self.normalize_frontier(context)
    }

    #[allow(dead_code)]
    pub(super) fn append<K: Key, V: Value>(
        &mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
        child_builder: RetainSubtreeBuilder,
    ) -> Result<()> {
        let RetainSubtreeBuilder {
            direction,
            frontier,
            mut leaf,
        } = child_builder;

        match (self.direction, direction) {
            (BuildDirection::LeftToRight, BuildDirection::LeftToRight) => {
                for subtree in frontier {
                    self.push_subtree(context, subtree)?;
                }
                if let Some((entries, root_distance)) = leaf.take_entries() {
                    for (key, value) in entries {
                        self.push_leaf_entry(context, &key, &value, root_distance)?;
                    }
                }
            }
            (BuildDirection::LeftToRight, BuildDirection::RightToLeft) => {
                if let Some((entries, root_distance)) = leaf.take_entries() {
                    for (key, value) in entries {
                        self.push_leaf_entry(context, &key, &value, root_distance)?;
                    }
                }
                for subtree in frontier {
                    self.push_subtree(context, subtree)?;
                }
            }
            (BuildDirection::RightToLeft, BuildDirection::LeftToRight) => {
                if let Some((entries, root_distance)) = leaf.take_entries() {
                    for (key, value) in entries.into_iter().rev() {
                        self.push_leaf_entry(context, &key, &value, root_distance)?;
                    }
                }
                for subtree in frontier.into_iter().rev() {
                    self.push_subtree(context, subtree)?;
                }
            }
            (BuildDirection::RightToLeft, BuildDirection::RightToLeft) => {
                for subtree in frontier.into_iter().rev() {
                    self.push_subtree(context, subtree)?;
                }
                if let Some((entries, root_distance)) = leaf.take_entries() {
                    for (key, value) in entries.into_iter().rev() {
                        self.push_leaf_entry(context, &key, &value, root_distance)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish_root<K: Key, V: Value>(
        mut self,
        context: &mut RetainBuilderContext<'_, K, V>,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        if let Some(leaf) = self.leaf.seal_if_sufficient(context)? {
            self.push_subtree_to_active_edge(leaf);
            self.normalize_frontier(context)?;
        }
        if !self.leaf.is_empty() {
            if let Some(subtree) = self.pop_subtree_from_active_edge() {
                let edge = self.direction.leaf_entries_after_subtree_edge();
                let (left, right) = self.leaf.graft_to_subtree(context, subtree, edge)?;
                self.push_ordered_pair_to_active_edge(left, right);
            } else if let Some(leaf) = self.leaf.seal_unchecked(context)? {
                self.push_subtree_to_active_edge(leaf);
            }
        }

        self.normalize_frontier(context)?;
        while self.frontier.len() > 1 {
            while let Some(index) = self.first_root_distance_change() {
                let old_len = self.frontier.len();
                self.graft_pair_at(context, index)?;
                assert!(
                    self.frontier.len() < old_len || self.root_distance_change_progressed(index),
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
            assert!(
                self.frontier
                    .iter()
                    .all(|subtree| subtree.root_distance == child_root_distance)
            );
            let old_len = self.frontier.len();
            let children: Vec<_> = self.frontier.drain(..).collect();
            let (parent, right_parent) =
                RetainSubtree::build_parent_subtrees(context, &children, child_root_distance)?;
            self.frontier.push_back(parent);
            if let Some(right_parent) = right_parent {
                self.frontier.push_back(right_parent);
            }
            assert!(self.frontier.len() < old_len);
            self.normalize_frontier(context)?;
        }

        Ok(self
            .frontier
            .pop_front()
            .map(|root| (root.page, root.checksum)))
    }
}
