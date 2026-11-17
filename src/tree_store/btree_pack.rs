// Packs the entries of a tree into fewer pages, for the maximum compaction mode.
//
// The tree is walked, and at each branch whose children are leaves, the pairs of those leaves are
// repacked into as few leaves as they fit. The keys under the branch are unchanged by that, so
// the key that its parent holds for it stays valid. That is what lets this run on untyped trees:
// it never compares keys.
//
// Only the children of one branch are held at a time, and a pass stops once it has replaced its
// budget of pages, so memory use does not depend on the size of the tree.
use crate::sync::Mutex;
use crate::tree_store::btree_base::{
    BranchAccessor, BranchBuilder, BtreeHeader, Checksum, DEFERRED, LeafAccessor, LeafBuilder,
    MAX_BTREE_DEPTH, PageType, RawBranchBuilder, pack_leaf_ranges,
};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{PageAllocator, PageHint, PageNumber, PageTracker, RawLeafBuilder};
use crate::{Result, StorageError};
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::ops::Range;

// The order of the page that a branch may grow to when its leaves are packed
const MAX_PACKED_BRANCH_ORDER: u8 = 2;

// A child of a branch, with the key that the branch holds after it. The last child has none
#[derive(Clone)]
struct Child {
    page: PageNumber,
    checksum: Checksum,
    key: Option<Vec<u8>>,
}

// A leaf that is planned to take the place of old ones: the pairs that it takes, and the key
// that the branch holds after it
struct NewLeaf {
    pairs: Range<usize>,
    key: Option<Vec<u8>>,
}

// Packs the tree at `root`, and decrements `budget` by the pages it replaces. It stops once
// `budget` reaches zero, which it may overshoot by the children of one branch. Returns the new
// root, whose checksums are deferred, if the tree changed
pub(super) fn pack_tree(
    page_allocator: &PageAllocator,
    freed_pages: &Mutex<Vec<PageNumber>>,
    root: BtreeHeader,
    key_width: Option<usize>,
    value_width: Option<usize>,
    budget: &mut usize,
) -> Result<Option<BtreeHeader>> {
    let mut packer = Packer::new(page_allocator, key_width, value_width, budget);
    let new_root = packer.pack(root.root, 0);
    // Pages that were freed before an error still need to be recorded
    freed_pages.lock().unwrap().append(&mut packer.freed);
    Ok(new_root?.map(|(page, checksum)| BtreeHeader::new(page, checksum, root.length)))
}

struct Packer<'a> {
    page_allocator: &'a PageAllocator,
    allocated: PageTracker,
    // Committed pages that were replaced, to be freed once nothing can read them
    freed: Vec<PageNumber>,
    key_width: Option<usize>,
    value_width: Option<usize>,
    budget: &'a mut usize,
}

impl<'a> Packer<'a> {
    fn new(
        page_allocator: &'a PageAllocator,
        key_width: Option<usize>,
        value_width: Option<usize>,
        budget: &'a mut usize,
    ) -> Self {
        Self {
            page_allocator,
            // No savepoint can exist during compaction, so allocations need no tracking
            allocated: PageTracker::ignore(),
            freed: vec![],
            key_width,
            value_width,
            budget,
        }
    }

    // Packs the tree at `page_number`, which is `depth` levels below the root. Returns the page
    // that replaces it, if the tree changed
    fn pack(
        &mut self,
        page_number: PageNumber,
        depth: usize,
    ) -> Result<Option<(PageNumber, Checksum)>> {
        if depth > MAX_BTREE_DEPTH {
            return Err(StorageError::Corrupted(
                "Btree exceeded maximum depth".to_string(),
            ));
        }
        let is_root = depth == 0;
        let page = self.page_allocator.get_page(page_number, PageHint::None)?;
        if PageType::from_page(&page)? == PageType::Leaf {
            // A tree that is a single leaf has nothing to pack
            return Ok(None);
        }
        let accessor = BranchAccessor::new(&page, self.key_width);
        let mut children: Vec<Child> = (0..accessor.count_children())
            .map(|i| Child {
                page: accessor.child_page(i).unwrap(),
                checksum: accessor.child_checksum(i).unwrap(),
                key: accessor.key(i).map(<[u8]>::to_vec),
            })
            .collect();
        let mut changed = false;

        // The leaves of a tree are all at the same depth, so one child tells what the others are
        if self.is_leaf(children[0].page)? {
            let packed = self.pack_leaves(&children, page.memory().len(), is_root)?;
            if let Some(packed) = packed {
                children = packed;
                changed = true;
            }
        } else {
            for child in &mut children {
                if *self.budget == 0 {
                    break;
                }
                if let Some((new_page, checksum)) = self.pack(child.page, depth + 1)? {
                    child.page = new_page;
                    child.checksum = checksum;
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(None);
        }

        // A root that is left with a single child is replaced by it
        let replacement = if let [child] = children.as_slice() {
            (child.page, child.checksum)
        } else {
            (self.build_branch(&children)?, DEFERRED)
        };
        drop(page);
        self.free(page_number);
        Ok(Some(replacement))
    }

    // Repacks the pairs of the leaves in `children` into as few leaves as they fit. Returns the
    // new children of their branch, which takes `branch_size` bytes, if that saves a page
    fn pack_leaves(
        &mut self,
        children: &[Child],
        branch_size: usize,
        is_root: bool,
    ) -> Result<Option<Vec<Child>>> {
        if *self.budget == 0 {
            return Ok(None);
        }
        // A leaf larger than a page is not read, so that what is held here is bounded
        let mut leaves = vec![];
        for child in children {
            leaves.push(if child.page.page_order == 0 {
                Some(self.get_page(child.page, PageType::Leaf)?)
            } else {
                None
            });
        }
        // The pairs of the leaves in order, each with the index of its leaf, and the range of
        // them that each leaf holds. A leaf that was not read stands as one empty pair
        let mut pairs: Vec<(usize, &[u8], &[u8])> = vec![];
        let mut ranges: Vec<Range<usize>> = vec![];
        for (i, leaf) in leaves.iter().enumerate() {
            let start = pairs.len();
            if let Some(leaf) = leaf {
                let accessor = LeafAccessor::new(leaf.memory(), self.key_width, self.value_width);
                for j in 0..accessor.num_pairs() {
                    let entry = accessor.entry(j).unwrap();
                    pairs.push((i, entry.key(), entry.value()));
                }
            } else {
                pairs.push((i, &[], &[]));
            }
            ranges.push(start..pairs.len());
        }
        let Some(plan) = self.plan_leaves(children, &pairs, &ranges, branch_size, is_root) else {
            return Ok(None);
        };

        let mut new_children = vec![];
        let mut replaced = vec![true; children.len()];
        for leaf in plan {
            let first = pairs[leaf.pairs.start].0;
            if leaf.pairs == ranges[first] {
                // A leaf that would only copy an old one is that old one
                new_children.push(children[first].clone());
                replaced[first] = false;
            } else {
                new_children.push(Child {
                    page: self.build_leaf(&pairs[leaf.pairs])?,
                    checksum: DEFERRED,
                    key: leaf.key,
                });
            }
        }
        drop(pairs);
        drop(leaves);
        for (child, replaced) in children.iter().zip(replaced) {
            if replaced {
                self.free(child.page);
            }
        }
        Ok(Some(new_children))
    }

    // Plans the leaves that replace those of a branch, each filled with `pairs` for as long as
    // they fit. Returns None unless the leaves and their branch then take fewer pages.
    //
    // Where a leaf ends with an old one, the branch keeps the key that it has for that one.
    // Elsewhere the only key known to be valid is the whole key of the leaf's last pair. A key
    // type may have shortened the keys that the branch holds (see `Key::separator()`), so the new
    // keys can be much longer than those, and the branch is allowed to grow, up to a page of
    // order `MAX_PACKED_BRANCH_ORDER`
    fn plan_leaves(
        &self,
        children: &[Child],
        pairs: &[(usize, &[u8], &[u8])],
        ranges: &[Range<usize>],
        branch_size: usize,
        is_root: bool,
    ) -> Option<Vec<NewLeaf>> {
        let page_size = self.page_allocator.get_page_size();
        // A leaf that was not read counts as a full page, which no pair fits with, so it is left
        // as it is
        let pair_bytes = |(leaf, key, value): &(usize, &[u8], &[u8])| {
            if children[*leaf].page.page_order == 0 {
                key.len() + value.len()
            } else {
                page_size
            }
        };
        let mut plan = pack_leaf_ranges(
            pairs.iter().map(pair_bytes),
            self.key_width,
            self.value_width,
            page_size,
        );
        // A branch other than the root keeps two children, so its last leaf is left as it is
        if !is_root && plan.len() == 1 {
            let last_leaf = ranges[children.len() - 1].clone();
            plan = vec![0..last_leaf.start, last_leaf];
        }

        // The pages that an allocation of this many bytes takes
        let pages = |bytes: usize| bytes.div_ceil(page_size).next_power_of_two();
        let mut new_leaves = vec![];
        let mut new_pages = 0;
        let mut key_bytes = 0;
        for range in plan {
            let first = pairs[range.start].0;
            let last = pairs[range.end - 1].0;
            let key = if range.end == ranges[last].end {
                children[last].key.clone()
            } else {
                Some(pairs[range.end - 1].1.to_vec())
            };
            key_bytes += key.as_ref().map_or(0, Vec::len);
            new_pages += if range == ranges[first] {
                1 << children[first].page.page_order
            } else {
                let bytes = pairs[range.clone()].iter().map(pair_bytes).sum();
                pages(RawLeafBuilder::required_bytes(
                    range.len(),
                    bytes,
                    self.key_width,
                    self.value_width,
                ))
            };
            new_leaves.push(NewLeaf { pairs: range, key });
        }
        let branch_bytes =
            RawBranchBuilder::required_bytes(new_leaves.len() - 1, key_bytes, self.key_width);
        if branch_bytes > branch_size.max(page_size << MAX_PACKED_BRANCH_ORDER) {
            return None;
        }
        new_pages += pages(branch_bytes);
        let old_leaf_pages: usize = children
            .iter()
            .map(|child| 1 << child.page.page_order)
            .sum();
        let old_pages = branch_size / page_size + old_leaf_pages;
        (new_pages < old_pages).then_some(new_leaves)
    }

    fn build_leaf(&self, pairs: &[(usize, &[u8], &[u8])]) -> Result<PageNumber> {
        let mut builder = LeafBuilder::new(
            self.page_allocator,
            &self.allocated,
            pairs.len(),
            self.key_width,
            self.value_width,
        );
        for (_, key, value) in pairs {
            builder.push(key, value);
        }
        Ok(builder.build()?.get_page_number())
    }

    fn build_branch(&self, children: &[Child]) -> Result<PageNumber> {
        let mut builder = BranchBuilder::new(
            self.page_allocator,
            &self.allocated,
            children.len(),
            self.key_width,
        );
        for child in children {
            builder.push_child(child.page, child.checksum);
        }
        for child in &children[..children.len() - 1] {
            builder.push_key(child.key.as_deref().unwrap());
        }
        Ok(builder.build()?.get_page_number())
    }

    fn is_leaf(&self, page_number: PageNumber) -> Result<bool> {
        let page = self.page_allocator.get_page(page_number, PageHint::None)?;
        Ok(PageType::from_page(&page)? == PageType::Leaf)
    }

    // Reads a page that its siblings say is of `page_type`
    fn get_page(&self, page_number: PageNumber, page_type: PageType) -> Result<PageImpl> {
        let page = self.page_allocator.get_page(page_number, PageHint::None)?;
        if PageType::from_page(&page)? != page_type {
            return Err(StorageError::Corrupted(
                "Btree leaves at different depths".to_string(),
            ));
        }
        Ok(page)
    }

    // Frees a page that was replaced, which counts against the budget
    fn free(&mut self, page: PageNumber) {
        self.page_allocator
            .conditional_free(page, &self.allocated, &mut self.freed);
        *self.budget = self.budget.saturating_sub(1);
    }
}

// What `check_tree()` found a tree to be made of
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TreeShape {
    pub(crate) leaves: usize,
    pub(crate) branches: usize,
    // The bytes of the largest branch
    pub(crate) largest_branch: usize,
    pub(crate) bytes: u64,
}

// Checks the tree at `root` for what a lookup relies on and packing must keep: every leaf is at
// the same depth, the keys of every leaf ascend, every branch has at least two children, and each
// branch key sorts at or above every key of the child before it and below every key of the child
// after it. Returns the tree's least and greatest keys, and the levels of branches above its leaves
#[cfg(test)]
pub(crate) fn check_tree(
    resolver: &crate::tree_store::PageResolver,
    root: PageNumber,
    key_width: Option<usize>,
    value_width: Option<usize>,
    compare: fn(&[u8], &[u8]) -> core::cmp::Ordering,
    visit_value: &mut dyn FnMut(&[u8]),
    shape: &mut TreeShape,
) -> (Vec<u8>, Vec<u8>, usize) {
    let page = resolver.get_page(root, PageHint::None).unwrap();
    shape.bytes += page.memory().len() as u64;
    match PageType::from_page(&page).unwrap() {
        PageType::Leaf => {
            shape.leaves += 1;
            let accessor = LeafAccessor::new(page.memory(), key_width, value_width);
            assert!(accessor.num_pairs() > 0);
            for i in 0..accessor.num_pairs() {
                let entry = accessor.entry(i).unwrap();
                if i > 0 {
                    let previous = accessor.entry(i - 1).unwrap();
                    assert!(compare(previous.key(), entry.key()).is_lt());
                }
                visit_value(entry.value());
            }
            (
                accessor.entry(0).unwrap().key().to_vec(),
                accessor.last_entry().key().to_vec(),
                0,
            )
        }
        PageType::Branch => {
            shape.branches += 1;
            let accessor = BranchAccessor::new(&page, key_width);
            let children = accessor.count_children();
            assert!(children >= 2);
            shape.largest_branch = shape.largest_branch.max(page.memory().len());
            let mut bounds: Option<(Vec<u8>, Vec<u8>)> = None;
            let mut height = None;
            for i in 0..children {
                let (least, greatest, child_height) = check_tree(
                    resolver,
                    accessor.child_page(i).unwrap(),
                    key_width,
                    value_width,
                    compare,
                    visit_value,
                    shape,
                );
                assert_eq!(*height.get_or_insert(child_height), child_height);
                if i > 0 {
                    assert!(compare(accessor.key(i - 1).unwrap(), &least).is_lt());
                }
                if i + 1 < children {
                    assert!(compare(&greatest, accessor.key(i).unwrap()).is_le());
                }
                bounds = Some(match bounds {
                    Some((first, _)) => (first, greatest),
                    None => (least, greatest),
                });
            }
            let (least, greatest) = bounds.unwrap();
            (least, greatest, height.unwrap() + 1)
        }
    }
}

#[cfg(test)]
mod test {
    use super::{TreeShape, check_tree, pack_tree};
    use crate::StorageError;
    use crate::sync::Mutex;
    use crate::tree_store::btree_base::{
        BranchBuilder, BranchMutator, BtreeHeader, DEFERRED, LeafBuilder,
    };
    use crate::tree_store::page_store::Page;
    use crate::tree_store::{
        AllocationPolicy, InMemoryBackend, PageAllocator, PageNumber, PageTracker,
        TransactionalMemory,
    };
    use crate::types::{Key, Value};
    use alloc::boxed::Box;
    use alloc::sync::Arc;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::ops::Range;

    fn make_allocator() -> PageAllocator {
        let (mem, _writer_lock) = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            4096,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::ExclusiveWriter,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();
        PageAllocator::new(Arc::new(mem), AllocationPolicy::Default)
    }

    fn leaf(page_allocator: &PageAllocator, keys: Range<u64>) -> PageNumber {
        let tracker = PageTracker::ignore();
        let keys: Vec<[u8; 8]> = keys.map(u64::to_le_bytes).collect();
        let mut builder = LeafBuilder::new(
            page_allocator,
            &tracker,
            keys.len(),
            u64::fixed_width(),
            u64::fixed_width(),
        );
        for key in &keys {
            builder.push(key, key);
        }
        builder.build().unwrap().get_page_number()
    }

    // A branch of `children`, each with the greatest key under it
    fn branch(page_allocator: &PageAllocator, children: &[(PageNumber, u64)]) -> PageNumber {
        let tracker = PageTracker::ignore();
        let mut builder =
            BranchBuilder::new(page_allocator, &tracker, children.len(), u64::fixed_width());
        for (page, _) in children {
            builder.push_child(*page, DEFERRED);
        }
        for (_, key) in &children[..children.len() - 1] {
            builder.push_key(key.to_le_bytes().to_vec());
        }
        builder.build().unwrap().get_page_number()
    }

    fn pack(page_allocator: &PageAllocator, root: PageNumber) -> Option<PageNumber> {
        let mut budget = usize::MAX;
        pack_tree(
            page_allocator,
            &Mutex::new(vec![]),
            BtreeHeader::new(root, DEFERRED, 6),
            u64::fixed_width(),
            u64::fixed_width(),
            &mut budget,
        )
        .unwrap()
        .map(|header| header.root)
    }

    // The leaves, branches and height of the tree at `root`, which holds the keys 0 to 5
    fn shape(page_allocator: &PageAllocator, root: PageNumber) -> (usize, usize, usize) {
        let mut shape = TreeShape::default();
        let mut pairs = 0;
        let (least, greatest, height) = check_tree(
            &page_allocator.resolver(),
            root,
            u64::fixed_width(),
            u64::fixed_width(),
            <u64 as Key>::compare,
            &mut |_| pairs += 1,
            &mut shape,
        );
        assert_eq!(least, 0u64.to_le_bytes());
        assert_eq!(greatest, 5u64.to_le_bytes());
        assert_eq!(pairs, 6);
        (shape.leaves, shape.branches, height)
    }

    #[test]
    fn a_branch_keeps_two_children() {
        let page_allocator = make_allocator();
        // Two branches, of three leaves each that would fit one
        let mut branches = vec![];
        for first in [0, 3] {
            let leaves: Vec<(PageNumber, u64)> = (first..first + 3)
                .map(|key| (leaf(&page_allocator, key..key + 1), key))
                .collect();
            branches.push((branch(&page_allocator, &leaves), first + 2));
        }
        let root = branch(&page_allocator, &branches);
        assert_eq!(shape(&page_allocator, root), (6, 3, 2));

        // Each branch is left with two leaves, though the three would fit one
        let root = pack(&page_allocator, root).unwrap();
        assert_eq!(shape(&page_allocator, root), (4, 3, 2));
        assert!(pack(&page_allocator, root).is_none());
    }

    #[test]
    fn a_cycle_is_reported() {
        let page_allocator = make_allocator();
        let leaves = [
            (leaf(&page_allocator, 0..1), 0),
            (leaf(&page_allocator, 1..2), 1),
        ];
        let root = branch(&page_allocator, &leaves);
        // The branch becomes its own first child
        let mut page = page_allocator.get_page_mut(root).unwrap();
        BranchMutator::new(page.memory_mut()).write_child_page(0, root, DEFERRED);
        drop(page);

        let mut budget = usize::MAX;
        let result = pack_tree(
            &page_allocator,
            &Mutex::new(vec![]),
            BtreeHeader::new(root, DEFERRED, 2),
            u64::fixed_width(),
            u64::fixed_width(),
            &mut budget,
        );
        assert!(matches!(result, Err(StorageError::Corrupted(_))));
    }
}
