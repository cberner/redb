// Packs a tree's entries into as few pages as possible, for the maximum compaction mode.
//
// The tree is walked bottom-up, and at each branch the branch's children are repacked: leaves
// into as few leaves as their pairs fit, or branches into as few branches as their children fit.
// Repacking a branch's children leaves the keys under it unchanged, so the key that its parent
// holds for it stays valid. That is what lets this run on untyped trees: it never compares keys.
//
// Only the children of one branch are held at a time, and a pass stops once it has replaced its
// budget of pages, so memory use does not depend on the size of the tree.
use crate::sync::Mutex;
use crate::tree_store::btree_base::{
    BranchAccessor, BranchMutator, BtreeHeader, Checksum, DEFERRED, LeafAccessor, LeafPageMut,
    MAX_BTREE_DEPTH, PageType, RawBranchBuilder, leaf_split_required,
};
use crate::tree_store::multimap_btree::{collection_subtree, subtree_collection};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{
    DynamicCollection, PageAllocator, PageHint, PageNumber, PageTracker, RawLeafBuilder,
};
use crate::types::Value;
use crate::{Result, StorageError};
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::ops::Range;

// What a tree's values are
#[derive(Copy, Clone)]
pub(super) enum TreeValues {
    // Values of this fixed width, if they have one
    Plain(Option<usize>),
    // The collections of a multimap table, each holding its values inline or in a subtree. The
    // width is that of the values, which are the subtree's keys
    Multimap(Option<usize>),
}

#[derive(Copy, Clone)]
struct Shape {
    key_width: Option<usize>,
    values: TreeValues,
}

impl Shape {
    fn value_width(self) -> Option<usize> {
        match self.values {
            TreeValues::Plain(width) => width,
            TreeValues::Multimap(width) => DynamicCollection::<()>::fixed_width_with(width),
        }
    }
}

// A child of a branch whose children are being repacked
enum NewChild {
    // The existing child at this index
    Kept(usize),
    // A new page, holding this range of the children's pairs or grandchildren
    Packed(Range<usize>),
}

// Packs the tree at `root`, and decrements `budget` by the pages it replaces. It stops once
// `budget` reaches zero, which it may overshoot by the children of one branch. Returns the new
// root, whose checksums are deferred, if the tree changed
pub(super) fn pack_tree(
    page_allocator: &PageAllocator,
    freed_pages: &Mutex<Vec<PageNumber>>,
    root: BtreeHeader,
    key_width: Option<usize>,
    values: TreeValues,
    budget: &mut usize,
) -> Result<Option<BtreeHeader>> {
    let mut packer = Packer {
        page_allocator,
        budget,
        freed: vec![],
    };
    let shape = Shape { key_width, values };
    let new_root = packer
        .height(root.root, shape)
        .and_then(|height| packer.pack(root.root, shape, height, true));
    freed_pages.lock().unwrap().append(&mut packer.freed);
    Ok(new_root?.map(|page| BtreeHeader::new(page, DEFERRED, root.length)))
}

// Splits `items` into consecutive groups, each extended for as long as `fits` allows it, and at
// least `min_len` long
fn greedy_groups(
    items: Range<usize>,
    min_len: usize,
    fits: impl Fn(Range<usize>) -> bool,
) -> Vec<Range<usize>> {
    let mut groups: Vec<Range<usize>> = vec![];
    let mut group = items.start..items.start;
    for item in items {
        if group.len() >= min_len && !fits(group.start..item + 1) {
            groups.push(group.clone());
            group = item..item;
        }
        group.end = item + 1;
    }
    if !group.is_empty() {
        // A trailing group that is too short joins the one before it
        if group.len() < min_len
            && let Some(previous) = groups.pop()
        {
            group.start = previous.start;
        }
        groups.push(group);
    }
    groups
}

// Prefix sums of `lens`, so that the sum over any range takes constant time
fn prefix_sums(lens: impl Iterator<Item = usize>) -> Vec<usize> {
    let mut sums = vec![0];
    for len in lens {
        sums.push(sums.last().unwrap() + len);
    }
    sums
}

struct Packer<'a, 'b> {
    page_allocator: &'a PageAllocator,
    budget: &'b mut usize,
    // Committed pages that were replaced, to be freed once nothing can read them
    freed: Vec<PageNumber>,
}

impl Packer<'_, '_> {
    // The number of levels of branches above the leaves of the tree at `root`
    fn height(&self, root: PageNumber, shape: Shape) -> Result<usize> {
        let mut page_number = root;
        for height in 0..=MAX_BTREE_DEPTH {
            let page = self.page_allocator.get_page(page_number, PageHint::None)?;
            match PageType::from_page(&page)? {
                PageType::Leaf => return Ok(height),
                PageType::Branch => {
                    page_number = BranchAccessor::new(&page, shape.key_width)
                        .child_page(0)
                        .unwrap();
                }
            }
        }
        Err(StorageError::Corrupted(
            "Btree exceeded maximum depth".to_string(),
        ))
    }

    // Packs the tree below `page_number`, which is `level` levels above the leaves, and returns
    // the page that replaces it, if any. A branch's subtrees are packed before its children are,
    // so that it sees how few children they left
    fn pack(
        &mut self,
        page_number: PageNumber,
        shape: Shape,
        level: usize,
        is_root: bool,
    ) -> Result<Option<PageNumber>> {
        let page = self.page_allocator.get_page(page_number, PageHint::None)?;
        let expected = if level == 0 {
            PageType::Leaf
        } else {
            PageType::Branch
        };
        if PageType::from_page(&page)? != expected {
            return Err(StorageError::Corrupted(
                "Btree leaves at different depths".to_string(),
            ));
        }
        let replacement = if level == 0 {
            // A leaf changes only where a multimap table's subtrees do
            match shape.values {
                TreeValues::Multimap(value_width) => {
                    self.pack_subtrees(&page, shape, value_width)?
                }
                TreeValues::Plain(_) => None,
            }
        } else {
            let accessor = BranchAccessor::new(&page, shape.key_width);
            let old_children: Vec<PageNumber> = (0..accessor.count_children())
                .map(|i| accessor.child_page(i).unwrap())
                .collect();
            let mut children = old_children.clone();
            // The leaves of other tables do not change, so they are not read here
            let visit_children = level > 1 || matches!(shape.values, TreeValues::Multimap(_));
            for child in &mut children {
                if !visit_children || *self.budget == 0 {
                    break;
                }
                if let Some(new_child) = self.pack(*child, shape, level - 1, false)? {
                    *child = new_child;
                }
            }
            let packed = if *self.budget == 0 {
                None
            } else if level == 1 {
                self.pack_leaves(&page, &old_children, &children, shape, is_root)?
            } else {
                self.pack_branches(&page, &old_children, &children, shape, is_root)?
            };
            match packed {
                Some(new_page) => Some(new_page),
                None if children != old_children => {
                    Some(self.copy_branch(&page, &old_children, &children)?)
                }
                None => None,
            }
        };
        if replacement.is_some() {
            drop(page);
            self.replace(page_number);
        }
        Ok(replacement)
    }

    // Packs the pairs of the leaves in `children` into as few leaves as they fit. A leaf larger
    // than a page is kept where it is, and not read, so that what this holds is bounded
    fn pack_leaves(
        &mut self,
        branch: &PageImpl,
        old_children: &[PageNumber],
        children: &[PageNumber],
        shape: Shape,
        is_root: bool,
    ) -> Result<Option<PageNumber>> {
        let page_size = self.page_allocator.get_page_size();
        let mut leaves = vec![];
        for child in children {
            leaves.push(if child.page_order == 0 {
                Some(self.page_allocator.get_page(*child, PageHint::None)?)
            } else {
                None
            });
        }
        // Every pair of the leaves that are read, with the index of the child it is in, and the
        // range of the pairs that each of those children holds
        let mut pairs: Vec<(usize, &[u8], &[u8])> = vec![];
        let mut child_pairs: Vec<Option<Range<usize>>> = vec![];
        for (i, leaf) in leaves.iter().enumerate() {
            child_pairs.push(leaf.as_ref().map(|leaf| {
                let accessor =
                    LeafAccessor::new(leaf.memory(), shape.key_width, shape.value_width());
                let start = pairs.len();
                for j in 0..accessor.num_pairs() {
                    let entry = accessor.entry(j).unwrap();
                    pairs.push((i, entry.key(), entry.value()));
                }
                start..pairs.len()
            }));
        }
        let bytes = prefix_sums(pairs.iter().map(|(_, key, value)| key.len() + value.len()));
        let fits = |range: Range<usize>| {
            !leaf_split_required(
                range.len(),
                bytes[range.end] - bytes[range.start],
                shape.key_width,
                shape.value_width(),
                page_size,
            )
        };

        // Each run of leaves between the kept ones is packed greedily
        let mut plan = vec![];
        let mut run: Option<Range<usize>> = None;
        for (i, range) in child_pairs.iter().enumerate() {
            if let Some(range) = range {
                run = Some(run.map_or(range.clone(), |run| run.start..range.end));
            } else {
                if let Some(run) = run.take() {
                    plan.extend(
                        greedy_groups(run, 1, fits)
                            .into_iter()
                            .map(NewChild::Packed),
                    );
                }
                plan.push(NewChild::Kept(i));
            }
        }
        if let Some(run) = run {
            plan.extend(
                greedy_groups(run, 1, fits)
                    .into_iter()
                    .map(NewChild::Packed),
            );
        }
        let Some(plan) = Self::finish_plan(plan, &child_pairs, is_root, 1) else {
            return Ok(None);
        };

        let branch_accessor = BranchAccessor::new(branch, shape.key_width);
        let mut new_children = vec![];
        let mut keys = vec![];
        for (n, new_child) in plan.iter().enumerate() {
            let (child, last_pair) = match new_child {
                NewChild::Kept(i) => (*i, None),
                NewChild::Packed(range) => (pairs[range.end - 1].0, Some(range.end - 1)),
            };
            new_children.push(match new_child {
                NewChild::Kept(i) => Self::kept(&branch_accessor, old_children, children, *i),
                NewChild::Packed(range) => {
                    (self.write_leaf(&pairs[range.clone()], shape)?, DEFERRED)
                }
            });
            if n + 1 < plan.len() {
                // Where the old children ended here, their key in `branch` separates the new ones
                let ends_child = last_pair
                    .is_none_or(|pair| child_pairs[child].as_ref().unwrap().end == pair + 1);
                keys.push(if ends_child {
                    branch_accessor.key(child).unwrap()
                } else {
                    pairs[last_pair.unwrap()].1
                });
            }
        }
        let replacement = if new_children.len() == 1 {
            new_children[0].0
        } else {
            self.write_branch(&new_children, &keys, shape.key_width)?
        };

        let kept = Self::kept_children(&plan, children.len());
        drop(pairs);
        drop(leaves);
        for (i, child) in children.iter().enumerate() {
            if !kept[i] {
                self.replace(*child);
            }
        }
        Ok(Some(replacement))
    }

    // Merges the children of the branches in `children` into as few branches as they fit
    fn pack_branches(
        &mut self,
        branch: &PageImpl,
        old_children: &[PageNumber],
        children: &[PageNumber],
        shape: Shape,
        is_root: bool,
    ) -> Result<Option<PageNumber>> {
        // A branch larger than a page holds keys too large to be worth moving, so that what this
        // holds is bounded
        if children.iter().any(|child| child.page_order > 0) {
            return Ok(None);
        }
        let page_size = self.page_allocator.get_page_size();
        let branch_accessor = BranchAccessor::new(branch, shape.key_width);
        let mut pages = vec![];
        for child in children {
            pages.push(self.page_allocator.get_page(*child, PageHint::None)?);
        }
        let accessors: Vec<BranchAccessor<'_, PageImpl>> = pages
            .iter()
            .map(|page| BranchAccessor::new(page, shape.key_width))
            .collect();
        // Every grandchild, the key that follows each grandchild but the last, and the range of
        // the grandchildren that each child holds. Between the grandchildren of two children, the
        // key is the one in `branch` that separates those children
        let mut grandchildren: Vec<(PageNumber, Checksum)> = vec![];
        let mut keys: Vec<&[u8]> = vec![];
        let mut child_grandchildren = vec![];
        for (i, accessor) in accessors.iter().enumerate() {
            let start = grandchildren.len();
            for j in 0..accessor.count_children() {
                grandchildren.push((
                    accessor.child_page(j).unwrap(),
                    accessor.child_checksum(j).unwrap(),
                ));
                if j + 1 < accessor.count_children() {
                    keys.push(accessor.key(j).unwrap());
                }
            }
            if i + 1 < accessors.len() {
                keys.push(branch_accessor.key(i).unwrap());
            }
            child_grandchildren.push(Some(start..grandchildren.len()));
        }
        let key_bytes = prefix_sums(keys.iter().map(|key| key.len()));
        let fits = |range: Range<usize>| {
            let num_keys = range.len() - 1;
            let bytes = key_bytes[range.end - 1] - key_bytes[range.start];
            RawBranchBuilder::required_bytes(num_keys, bytes, shape.key_width) <= page_size
                && u16::try_from(num_keys).is_ok()
        };
        let plan = greedy_groups(0..grandchildren.len(), 2, fits)
            .into_iter()
            .map(NewChild::Packed)
            .collect();
        let Some(plan) = Self::finish_plan(plan, &child_grandchildren, is_root, 2) else {
            return Ok(None);
        };

        let mut new_children = vec![];
        let mut new_keys = vec![];
        for (n, new_child) in plan.iter().enumerate() {
            let range = match new_child {
                NewChild::Kept(i) => child_grandchildren[*i].clone().unwrap(),
                NewChild::Packed(range) => range.clone(),
            };
            new_children.push(match new_child {
                NewChild::Kept(i) => Self::kept(&branch_accessor, old_children, children, *i),
                NewChild::Packed(range) => (
                    self.write_branch(
                        &grandchildren[range.clone()],
                        &keys[range.start..range.end - 1],
                        shape.key_width,
                    )?,
                    DEFERRED,
                ),
            });
            if n + 1 < plan.len() {
                new_keys.push(keys[range.end - 1]);
            }
        }
        let replacement = if new_children.len() == 1 {
            new_children[0].0
        } else {
            self.write_branch(&new_children, &new_keys, shape.key_width)?
        };

        let kept = Self::kept_children(&plan, children.len());
        drop(accessors);
        drop(pages);
        for (i, child) in children.iter().enumerate() {
            if !kept[i] {
                self.replace(*child);
            }
        }
        Ok(Some(replacement))
    }

    // Keeps each child that a packed page would only copy, and makes sure that a branch other
    // than the root keeps at least two children, of at least `min_len` items each. None if the
    // children are best left as they are, because packing them would not save a page
    fn finish_plan(
        plan: Vec<NewChild>,
        child_items: &[Option<Range<usize>>],
        is_root: bool,
        min_len: usize,
    ) -> Option<Vec<NewChild>> {
        let mut plan: Vec<NewChild> = plan
            .into_iter()
            .map(|new_child| match new_child {
                NewChild::Packed(range) => match child_items
                    .iter()
                    .position(|items| items.as_ref() == Some(&range))
                {
                    Some(i) => NewChild::Kept(i),
                    None => NewChild::Packed(range),
                },
                kept @ NewChild::Kept(_) => kept,
            })
            .collect();
        if plan.len() == 1 && !is_root {
            let Some(NewChild::Packed(range)) = plan.pop() else {
                return None;
            };
            if range.len() < 2 * min_len {
                return None;
            }
            let middle = range.start + range.len() / 2;
            plan = vec![
                NewChild::Packed(range.start..middle),
                NewChild::Packed(middle..range.end),
            ];
        }
        (plan.len() < child_items.len()).then_some(plan)
    }

    fn kept_children(plan: &[NewChild], len: usize) -> Vec<bool> {
        let mut kept = vec![false; len];
        for new_child in plan {
            if let NewChild::Kept(i) = new_child {
                kept[*i] = true;
            }
        }
        kept
    }

    // The page and checksum of an existing child of `branch`. A child replaced below has a
    // deferred checksum
    fn kept(
        branch: &BranchAccessor<'_, PageImpl>,
        old_children: &[PageNumber],
        children: &[PageNumber],
        i: usize,
    ) -> (PageNumber, Checksum) {
        if children[i] == old_children[i] {
            (children[i], branch.child_checksum(i).unwrap())
        } else {
            (children[i], DEFERRED)
        }
    }

    // Packs the subtrees that the values in a multimap table's leaf hold. Returns the page that
    // replaces the leaf, if any of them changed
    fn pack_subtrees(
        &mut self,
        leaf: &PageImpl,
        shape: Shape,
        subtree_key_width: Option<usize>,
    ) -> Result<Option<PageNumber>> {
        let subtree_shape = Shape {
            key_width: subtree_key_width,
            values: TreeValues::Plain(<() as Value>::fixed_width()),
        };
        let accessor = LeafAccessor::new(leaf.memory(), shape.key_width, shape.value_width());
        let mut new_values = vec![];
        for i in 0..accessor.num_pairs() {
            if *self.budget == 0 {
                break;
            }
            if let Some(subtree) = collection_subtree(accessor.entry(i).unwrap().value())
                && let Some(new_root) = self
                    .height(subtree.root, subtree_shape)
                    .and_then(|height| self.pack(subtree.root, subtree_shape, height, true))?
            {
                let new_subtree = BtreeHeader::new(new_root, DEFERRED, subtree.length);
                new_values.push((i, subtree_collection(new_subtree)));
            }
        }
        if new_values.is_empty() {
            return Ok(None);
        }
        let mut page = self
            .page_allocator
            .allocate(leaf.memory().len(), &PageTracker::ignore())?;
        page.memory_mut().copy_from_slice(leaf.memory());
        let page_number = page.get_page_number();
        let mut page = LeafPageMut::new(page, shape.key_width, shape.value_width());
        for (i, value) in new_values {
            page.replace_value(i, &value);
        }
        Ok(Some(page_number))
    }

    // A copy of `branch` that points to `children` instead of `old_children`
    fn copy_branch(
        &mut self,
        branch: &PageImpl,
        old_children: &[PageNumber],
        children: &[PageNumber],
    ) -> Result<PageNumber> {
        let mut page = self
            .page_allocator
            .allocate(branch.memory().len(), &PageTracker::ignore())?;
        page.memory_mut().copy_from_slice(branch.memory());
        let mut mutator = BranchMutator::new(page.memory_mut());
        for (i, (old_child, child)) in old_children.iter().zip(children).enumerate() {
            if old_child != child {
                mutator.write_child_page(i, *child, DEFERRED);
            }
        }
        Ok(page.get_page_number())
    }

    fn write_leaf(&mut self, pairs: &[(usize, &[u8], &[u8])], shape: Shape) -> Result<PageNumber> {
        let key_bytes = pairs.iter().map(|(_, key, _)| key.len()).sum();
        let value_bytes: usize = pairs.iter().map(|(_, _, value)| value.len()).sum();
        let required = RawLeafBuilder::required_bytes(
            pairs.len(),
            key_bytes + value_bytes,
            shape.key_width,
            shape.value_width(),
        );
        let mut page = self
            .page_allocator
            .allocate(required, &PageTracker::ignore())?;
        let mut builder = RawLeafBuilder::new(
            page.memory_mut(),
            pairs.len(),
            shape.key_width,
            shape.value_width(),
            key_bytes,
        );
        for (_, key, value) in pairs {
            builder.append(key, value);
        }
        drop(builder);
        Ok(page.get_page_number())
    }

    fn write_branch(
        &mut self,
        children: &[(PageNumber, Checksum)],
        keys: &[&[u8]],
        key_width: Option<usize>,
    ) -> Result<PageNumber> {
        let key_bytes = keys.iter().map(|key| key.len()).sum();
        let required = RawBranchBuilder::required_bytes(keys.len(), key_bytes, key_width);
        let mut page = self
            .page_allocator
            .allocate(required, &PageTracker::ignore())?;
        let mut builder = RawBranchBuilder::new(page.memory_mut(), keys.len(), key_width);
        builder.write_first_page(children[0].0, children[0].1);
        for (i, key) in keys.iter().enumerate() {
            builder.write_nth_key(key, children[i + 1].0, children[i + 1].1, i);
        }
        drop(builder);
        Ok(page.get_page_number())
    }

    // Frees `page` once nothing can read it, which is immediately if this transaction wrote it.
    // No savepoint can exist during compaction, so the freed page needs no tracking
    fn replace(&mut self, page: PageNumber) {
        self.page_allocator
            .conditional_free(page, &PageTracker::ignore(), &mut self.freed);
        *self.budget = self.budget.saturating_sub(1);
    }
}

#[cfg(test)]
mod test {
    use super::{NewChild, Packer, greedy_groups};
    use alloc::vec;
    use alloc::vec::Vec;
    use core::ops::Range;

    fn describe(plan: Option<Vec<NewChild>>) -> Option<Vec<(bool, Range<usize>)>> {
        plan.map(|plan| {
            plan.into_iter()
                .map(|new_child| match new_child {
                    NewChild::Kept(i) => (true, i..i + 1),
                    NewChild::Packed(range) => (false, range),
                })
                .collect()
        })
    }

    #[test]
    fn greedy_groups_fill_each_group() {
        // Groups of at most three items
        let groups = greedy_groups(0..7, 1, |range| range.len() <= 3);
        assert_eq!(groups, vec![0..3, 3..6, 6..7]);
        // A trailing group shorter than the minimum joins the one before it
        let groups = greedy_groups(0..7, 2, |range| range.len() <= 3);
        assert_eq!(groups, vec![0..3, 3..7]);
        // A group reaches the minimum even where `fits` refuses it
        let groups = greedy_groups(0..4, 2, |_| false);
        assert_eq!(groups, vec![0..2, 2..4]);
        assert!(greedy_groups(3..3, 1, |_| true).is_empty());
    }

    #[test]
    fn finish_plan_keeps_copies_and_two_children() {
        let children = [Some(0..2), Some(2..3), Some(3..5)];
        // A group holding exactly one child's items keeps that child
        let plan = vec![NewChild::Packed(0..2), NewChild::Packed(2..5)];
        assert_eq!(
            describe(Packer::finish_plan(plan, &children, false, 1)),
            Some(vec![(true, 0..1), (false, 2..5)])
        );
        // A plan that saves no page is dropped
        let plan = vec![
            NewChild::Packed(0..1),
            NewChild::Packed(1..3),
            NewChild::Packed(3..5),
        ];
        assert!(Packer::finish_plan(plan, &children, false, 1).is_none());
        // A branch other than the root keeps two children
        let plan = vec![NewChild::Packed(0..5)];
        assert_eq!(
            describe(Packer::finish_plan(plan, &children, false, 1)),
            Some(vec![(false, 0..2), (false, 2..5)])
        );
        // ... unless they would be too short, as a branch child of fewer than two grandchildren
        let plan = vec![NewChild::Packed(0..3)];
        assert!(Packer::finish_plan(plan, &[Some(0..1), Some(1..3)], false, 2).is_none());
        // The root may be left with a single child, which then replaces it
        let plan = vec![NewChild::Packed(0..5)];
        assert_eq!(
            describe(Packer::finish_plan(plan, &children, true, 1)),
            Some(vec![(false, 0..5)])
        );
    }
}
