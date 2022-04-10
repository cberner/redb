use crate::tree_store::btree_base::{
    FreePolicy, IndexBuilder, InternalAccessor, InternalBuilder, InternalMutator, LeafAccessor,
    LeafBuilder, LeafBuilder2, INTERNAL, LEAF,
};
use crate::tree_store::btree_mutator::DeletionResult::{
    DeletedInternal, PartialInternal, PartialLeaf, Subtree,
};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{AccessGuardMut, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue};
use crate::{AccessGuard, Result};
use std::cmp::{max, min};
use std::marker::PhantomData;

#[derive(Debug)]
enum DeletionResult {
    // A proper subtree
    Subtree(PageNumber),
    // A leaf subtree with too few entries
    PartialLeaf { deleted_pair: usize },
    // A index page subtree with fewer children than desired
    PartialInternal(PageNumber),
    // Indicates that the index node was deleted, and includes the only remaining child
    DeletedInternal(PageNumber),
}

pub(crate) struct MutateHelper<'a, 'b, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    root: &'b mut Option<PageNumber>,
    free_policy: FreePolicy,
    mem: &'a TransactionalMemory,
    freed: &'b mut Vec<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, 'b, K: RedbKey + ?Sized, V: RedbValue + ?Sized> MutateHelper<'a, 'b, K, V> {
    pub(crate) fn new(
        root: &'b mut Option<PageNumber>,
        free_policy: FreePolicy,
        mem: &'a TransactionalMemory,
        freed: &'b mut Vec<PageNumber>,
    ) -> Self {
        Self {
            root,
            free_policy,
            mem,
            freed,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn safe_delete(&mut self, key: &K) -> Result<Option<AccessGuard<'a, V>>> {
        assert_eq!(self.free_policy, FreePolicy::Never);
        // Safety: we asserted that the free policy is Never
        unsafe { self.delete(key) }
    }

    // Safety: caller must ensure that no references to uncommitted pages in this table exist
    pub(crate) unsafe fn delete(&mut self, key: &K) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some(p) = *self.root {
            let (deletion_result, found) =
                self.delete_helper(self.mem.get_page(p), key.as_bytes().as_ref())?;
            let new_root = match deletion_result {
                DeletionResult::Subtree(page) => Some(page),
                DeletionResult::PartialLeaf { deleted_pair } => {
                    let page = self.mem.get_page(p);
                    let accessor = LeafAccessor::new(&page);
                    if accessor.num_pairs() == 1 {
                        None
                    } else {
                        let mut builder = LeafBuilder2::new(self.mem, accessor.num_pairs() - 1);
                        for i in 0..accessor.num_pairs() {
                            if i == deleted_pair {
                                continue;
                            }
                            let entry = accessor.entry(i).unwrap();
                            builder.push(entry.key(), entry.value());
                        }
                        Some(builder.build()?.get_page_number())
                    }
                }
                DeletionResult::PartialInternal(page_number) => Some(page_number),
                DeletionResult::DeletedInternal(remaining_child) => Some(remaining_child),
            };
            *self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    // Safety: caller must ensure that no references to uncommitted pages in this tree exist
    pub(crate) unsafe fn insert(&mut self, key: &K, value: &V) -> Result<AccessGuardMut<'a>> {
        let (new_root, guard) = if let Some(p) = *self.root {
            let (page1, more, guard) = self.insert_helper(
                self.mem.get_page(p),
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
            )?;

            let new_root = if let Some((key, page2)) = more {
                let mut builder = IndexBuilder::new(self.mem);
                builder.push_child(page1);
                builder.push_key(&key);
                builder.push_child(page2);
                builder.build()?.get_page_number()
            } else {
                page1
            };
            (new_root, guard)
        } else {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            let key_bytes = key_bytes.as_ref();
            let value_bytes = value_bytes.as_ref();
            let mut page = self.mem.allocate(LeafBuilder::required_bytes(
                1,
                key_bytes.len() + value_bytes.len(),
            ))?;
            let mut builder = LeafBuilder::new(&mut page, 1, key_bytes.len());
            builder.append(key_bytes, value_bytes);
            drop(builder);

            let accessor = LeafAccessor::new(&page);
            let offset = accessor.offset_of_first_value();
            let page_num = page.get_page_number();
            let guard = AccessGuardMut::new(page, offset, value_bytes.len());

            (page_num, guard)
        };
        *self.root = Some(new_root);
        Ok(guard)
    }

    #[allow(clippy::type_complexity)]
    // Safety: caller must ensure that no references to uncommitted pages in this table exist
    unsafe fn insert_helper(
        &mut self,
        page: PageImpl<'a>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(
        PageNumber,
        Option<(Vec<u8>, PageNumber)>,
        AccessGuardMut<'a>,
    )> {
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                let (position, found) = accessor.position::<K>(key);

                // Fast-path to avoid re-building and splitting pages with a single large value
                let single_large_value = accessor.num_pairs() == 1
                    && accessor.total_length() >= self.mem.get_page_size();
                if !found && single_large_value {
                    let mut builder = LeafBuilder2::new(self.mem, 1);
                    builder.push(key, value);
                    let new_page = builder.build()?;
                    let new_page_number = new_page.get_page_number();
                    let new_page_accessor = LeafAccessor::new(&new_page);
                    let offset = new_page_accessor.offset_of_value(position).unwrap();
                    drop(new_page_accessor);
                    let guard = AccessGuardMut::new(new_page, offset, value.len());
                    return if position == 0 {
                        Ok((
                            new_page_number,
                            Some((key.to_vec(), page.get_page_number())),
                            guard,
                        ))
                    } else {
                        let split_key = accessor.last_entry().key().to_vec();
                        Ok((
                            page.get_page_number(),
                            Some((split_key, new_page_number)),
                            guard,
                        ))
                    };
                }

                let mut builder = LeafBuilder2::new(self.mem, accessor.num_pairs() + 1);
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
                    drop(page);
                    self.free_policy
                        .conditional_free(page_number, self.freed, self.mem)?;

                    let new_page_number = new_page.get_page_number();
                    let accessor = LeafAccessor::new(&new_page);
                    let offset = accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMut::new(new_page, offset, value.len());

                    (new_page_number, None, guard)
                } else {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    let page_number = page.get_page_number();
                    let split_key = split_key.to_vec();
                    drop(page);
                    self.free_policy
                        .conditional_free(page_number, self.freed, self.mem)?;

                    let new_page_number = new_page1.get_page_number();
                    let new_page_number2 = new_page2.get_page_number();
                    let accessor = LeafAccessor::new(&new_page1);
                    let division = accessor.num_pairs();
                    let guard = if position < division {
                        let accessor = LeafAccessor::new(&new_page1);
                        let offset = accessor.offset_of_value(position).unwrap();
                        AccessGuardMut::new(new_page1, offset, value.len())
                    } else {
                        let accessor = LeafAccessor::new(&new_page2);
                        let offset = accessor.offset_of_value(position - division).unwrap();
                        AccessGuardMut::new(new_page2, offset, value.len())
                    };

                    (new_page_number, Some((split_key, new_page_number2)), guard)
                }
            }
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let (child_index, child_page) = accessor.child_for_key::<K>(key);
                let (page1, more, guard) =
                    self.insert_helper(self.mem.get_page(child_page), key, value)?;

                if more.is_none() {
                    // Check fast-path if no children were added
                    if page1 == child_page {
                        // NO-OP. One of our descendants is uncommitted, so there was no change
                        return Ok((page.get_page_number(), None, guard));
                    } else if self.mem.uncommitted(page.get_page_number()) {
                        let page_number = page.get_page_number();
                        drop(page);
                        // Safety: Since the page is uncommitted, no other transactions could have it open
                        // and we just dropped our reference to it, on the line above
                        let mut mutpage = self.mem.get_page_mut(page_number);
                        let mut mutator = InternalMutator::new(&mut mutpage);
                        mutator.write_child_page(child_index, page1);
                        return Ok((mutpage.get_page_number(), None, guard));
                    }
                }

                // A child was added, or we couldn't use the fast-path above
                let mut builder = IndexBuilder::new(self.mem);
                if child_index == 0 {
                    builder.push_child(page1);
                    if let Some((ref index_key2, page2)) = more {
                        builder.push_key(index_key2);
                        builder.push_child(page2);
                    }
                } else {
                    builder.push_child(accessor.child_page(0).unwrap());
                }
                for i in 1..accessor.count_children() {
                    if let Some(key) = accessor.key(i - 1) {
                        builder.push_key(key);
                        if i == child_index {
                            builder.push_child(page1);
                            if let Some((ref index_key2, page2)) = more {
                                builder.push_key(index_key2);
                                builder.push_child(page2);
                            }
                        } else {
                            builder.push_child(accessor.child_page(i).unwrap());
                        }
                    } else {
                        unreachable!();
                    }
                }

                let result = if builder.should_split() {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    (
                        new_page1.get_page_number(),
                        Some((split_key.to_vec(), new_page2.get_page_number())),
                        guard,
                    )
                } else {
                    let new_page = builder.build()?;
                    (new_page.get_page_number(), None, guard)
                };
                // Free the original page, since we've replaced it
                let page_number = page.get_page_number();
                drop(page);
                // Safety: If the page is uncommitted, no other transactions can have references to it,
                // and we just dropped ours on the line above
                self.free_policy
                    .conditional_free(page_number, self.freed, self.mem)?;

                result
            }
            _ => unreachable!(),
        })
    }

    // Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
    // If key is not found, guaranteed not to modify the tree
    //
    // Safety: caller must ensure that no references to uncommitted pages in this table exist
    unsafe fn delete_helper(
        &mut self,
        page: PageImpl<'a>,
        key: &[u8],
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                let (position, found) = accessor.position::<K>(key);
                if !found {
                    return Ok((Subtree(page.get_page_number()), None));
                }
                let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
                    - accessor.length_of_pairs(position, position + 1);
                let new_required_bytes =
                    LeafBuilder::required_bytes(accessor.num_pairs() - 1, new_kv_bytes);
                let result = if new_required_bytes < self.mem.get_page_size() / 2 {
                    PartialLeaf {
                        deleted_pair: position,
                    }
                } else {
                    let mut builder = LeafBuilder2::new(self.mem, accessor.num_pairs() - 1);
                    for i in 0..accessor.num_pairs() {
                        if i == position {
                            continue;
                        }
                        let entry = accessor.entry(i).unwrap();
                        builder.push(entry.key(), entry.value());
                    }
                    Subtree(builder.build()?.get_page_number())
                };
                let uncommitted = self.mem.uncommitted(page.get_page_number());
                let free_on_drop = if !uncommitted || matches!(self.free_policy, FreePolicy::Never)
                {
                    // Won't be freed until the end of the transaction, so returning the page
                    // in the AccessGuard below is still safe
                    self.freed.push(page.get_page_number());
                    false
                } else {
                    true
                };
                let (start, end) = accessor.value_range(position).unwrap();
                let guard = Some(AccessGuard::new(
                    page,
                    start,
                    end - start,
                    free_on_drop,
                    self.mem,
                ));
                Ok((result, guard))
            }
            // TODO: cleanup the handling of internal index nodes. This function is insanely long
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let original_page_number = page.get_page_number();
                let (child_index, child_page_number) = accessor.child_for_key::<K>(key);
                let (result, found) =
                    self.delete_helper(self.mem.get_page(child_page_number), key)?;
                if found.is_none() {
                    return Ok((Subtree(original_page_number), None));
                }
                let final_result = match result {
                    Subtree(new_child) => {
                        if new_child == child_page_number {
                            // NO-OP. One of our descendants is uncommitted, so there was no change
                            return Ok((Subtree(original_page_number), found));
                        } else if self.mem.uncommitted(original_page_number) {
                            drop(page);
                            // Safety: Caller guarantees there are no references to uncommitted pages,
                            // and we just dropped our reference to it on the line above
                            let mut mutpage = self.mem.get_page_mut(original_page_number);
                            let mut mutator = InternalMutator::new(&mut mutpage);
                            mutator.write_child_page(child_index, new_child);
                            return Ok((Subtree(original_page_number), found));
                        } else {
                            let mut new_page =
                                self.mem.allocate(InternalBuilder::required_bytes(
                                    accessor.count_children() - 1,
                                    accessor.total_key_length(),
                                ))?;
                            let mut builder =
                                InternalBuilder::new(&mut new_page, accessor.count_children() - 1);
                            if child_index == 0 {
                                builder.write_first_page(new_child);
                            } else {
                                builder.write_first_page(accessor.child_page(0).unwrap());
                            }

                            for i in 1..accessor.count_children() {
                                if let Some(key) = accessor.key(i - 1) {
                                    let page_number = if i == child_index {
                                        new_child
                                    } else {
                                        accessor.child_page(i).unwrap()
                                    };
                                    builder.write_nth_key(key, page_number, i - 1);
                                } else {
                                    unreachable!();
                                }
                            }

                            drop(builder);
                            Subtree(new_page.get_page_number())
                        }
                    }
                    PartialLeaf { deleted_pair } => {
                        let partial_child_page = self.mem.get_page(child_page_number);
                        let partial_child_accessor = LeafAccessor::new(&partial_child_page);

                        let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                        let merge_with_page =
                            self.mem.get_page(accessor.child_page(merge_with).unwrap());
                        let merge_with_accessor = LeafAccessor::new(&merge_with_page);
                        debug_assert!(merge_with < accessor.count_children());
                        let mut builder = IndexBuilder::new(&self.mem);
                        for i in 0..accessor.count_children() {
                            if i == child_index {
                                continue;
                            }
                            let page_number = accessor.child_page(i).unwrap();
                            if i == merge_with {
                                let mut child_builder = LeafBuilder2::new(
                                    self.mem,
                                    partial_child_accessor.num_pairs() - 1
                                        + merge_with_accessor.num_pairs(),
                                );
                                if child_index < merge_with {
                                    for i in 0..partial_child_accessor.num_pairs() {
                                        if i == deleted_pair {
                                            continue;
                                        }
                                        let entry = partial_child_accessor.entry(i).unwrap();
                                        child_builder.push(entry.key(), entry.value());
                                    }
                                }
                                for j in 0..merge_with_accessor.num_pairs() {
                                    let entry = merge_with_accessor.entry(j).unwrap();
                                    child_builder.push(entry.key(), entry.value());
                                }
                                if child_index > merge_with {
                                    for i in 0..partial_child_accessor.num_pairs() {
                                        if i == deleted_pair {
                                            continue;
                                        }
                                        let entry = partial_child_accessor.entry(i).unwrap();
                                        child_builder.push(entry.key(), entry.value());
                                    }
                                }
                                if child_builder.should_split() {
                                    let (new_page1, split_key, new_page2) =
                                        child_builder.build_split()?;
                                    builder.push_key(split_key);
                                    builder.push_child(new_page1.get_page_number());
                                    builder.push_child(new_page2.get_page_number());
                                } else {
                                    let new_page = child_builder.build()?;
                                    builder.push_child(new_page.get_page_number());
                                }

                                let merged_key_index = max(child_index, merge_with);
                                if merged_key_index < accessor.count_children() - 1 {
                                    builder.push_key(accessor.key(merged_key_index).unwrap());
                                }
                            } else {
                                builder.push_child(page_number);
                                if i < accessor.count_children() - 1 {
                                    builder.push_key(accessor.key(i).unwrap());
                                }
                            }
                        }
                        let result = if let Some(only_child) = builder.to_single_child() {
                            DeletedInternal(only_child)
                        } else {
                            // TODO: can we optimize away this page allocation?
                            // The PartialInternal gets returned, and then the caller has to merge it immediately
                            let new_page = builder.build()?;
                            let accessor = InternalAccessor::new(&new_page);
                            if accessor.total_length() < self.mem.get_page_size() / 2 {
                                PartialInternal(new_page.get_page_number())
                            } else {
                                Subtree(new_page.get_page_number())
                            }
                        };

                        let page_number = merge_with_page.get_page_number();
                        drop(merge_with_page);
                        self.free_policy
                            .conditional_free(page_number, self.freed, self.mem)?;

                        result
                    }
                    DeletionResult::DeletedInternal(only_grandchild) => {
                        let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                        let merge_with_page =
                            self.mem.get_page(accessor.child_page(merge_with).unwrap());
                        let merge_with_accessor = InternalAccessor::new(&merge_with_page);
                        debug_assert!(merge_with < accessor.count_children());
                        let mut builder = IndexBuilder::new(self.mem);
                        for i in 0..accessor.count_children() {
                            if i == child_index {
                                continue;
                            }
                            let page_number = accessor.child_page(i).unwrap();
                            if i == merge_with {
                                let mut child_builder = IndexBuilder::new(self.mem);
                                let separator_key =
                                    accessor.key(min(child_index, merge_with)).unwrap();
                                if child_index < merge_with {
                                    child_builder.push_child(only_grandchild);
                                    child_builder.push_key(separator_key);
                                }
                                // TODO: add a method to IndexBuilder to push all children & keys from an InternalAccessor
                                for j in 0..merge_with_accessor.count_children() {
                                    child_builder
                                        .push_child(merge_with_accessor.child_page(j).unwrap());
                                    if j < merge_with_accessor.count_children() - 1 {
                                        child_builder.push_key(merge_with_accessor.key(j).unwrap());
                                    }
                                }
                                if child_index > merge_with {
                                    child_builder.push_key(separator_key);
                                    child_builder.push_child(only_grandchild);
                                }
                                if child_builder.should_split() {
                                    let (new_page1, separator, new_page2) =
                                        child_builder.build_split()?;
                                    builder.push_child(new_page1.get_page_number());
                                    builder.push_key(&separator);
                                    builder.push_child(new_page2.get_page_number());
                                } else {
                                    let new_page = child_builder.build()?;
                                    builder.push_child(new_page.get_page_number());
                                }

                                let merged_key_index = max(child_index, merge_with);
                                if merged_key_index < accessor.count_children() - 1 {
                                    builder.push_key(accessor.key(merged_key_index).unwrap());
                                }
                            } else {
                                builder.push_child(page_number);
                                if i < accessor.count_children() - 1 {
                                    builder.push_key(accessor.key(i).unwrap());
                                }
                            }
                        }
                        let result = if let Some(only_child) = builder.to_single_child() {
                            DeletedInternal(only_child)
                        } else {
                            let new_page = builder.build()?;
                            let accessor = InternalAccessor::new(&new_page);
                            if accessor.total_length() < self.mem.get_page_size() / 2 {
                                PartialInternal(new_page.get_page_number())
                            } else {
                                Subtree(new_page.get_page_number())
                            }
                        };

                        let page_number = merge_with_page.get_page_number();
                        drop(merge_with_page);
                        self.free_policy
                            .conditional_free(page_number, self.freed, self.mem)?;

                        result
                    }
                    PartialInternal(partial_child) => {
                        let partial_child_page = self.mem.get_page(partial_child);
                        let partial_child_accessor = InternalAccessor::new(&partial_child_page);
                        let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                        let merge_with_page =
                            self.mem.get_page(accessor.child_page(merge_with).unwrap());
                        let merge_with_accessor = InternalAccessor::new(&merge_with_page);
                        debug_assert!(merge_with < accessor.count_children());
                        let mut builder = IndexBuilder::new(self.mem);
                        for i in 0..accessor.count_children() {
                            if i == child_index {
                                continue;
                            }
                            let page_number = accessor.child_page(i).unwrap();
                            if i == merge_with {
                                let mut child_builder = IndexBuilder::new(self.mem);
                                let separator_key =
                                    accessor.key(min(child_index, merge_with)).unwrap();
                                if child_index < merge_with {
                                    for j in 0..partial_child_accessor.count_children() {
                                        child_builder.push_child(
                                            partial_child_accessor.child_page(j).unwrap(),
                                        );
                                        if j < partial_child_accessor.count_children() - 1 {
                                            child_builder
                                                .push_key(partial_child_accessor.key(j).unwrap());
                                        }
                                    }
                                    child_builder.push_key(separator_key);
                                }
                                // TODO: add a method to IndexBuilder to push all children & keys from an InternalAccessor
                                for j in 0..merge_with_accessor.count_children() {
                                    child_builder
                                        .push_child(merge_with_accessor.child_page(j).unwrap());
                                    if j < merge_with_accessor.count_children() - 1 {
                                        child_builder.push_key(merge_with_accessor.key(j).unwrap());
                                    }
                                }
                                if child_index > merge_with {
                                    child_builder.push_key(separator_key);
                                    for j in 0..partial_child_accessor.count_children() {
                                        child_builder.push_child(
                                            partial_child_accessor.child_page(j).unwrap(),
                                        );
                                        if j < partial_child_accessor.count_children() - 1 {
                                            child_builder
                                                .push_key(partial_child_accessor.key(j).unwrap());
                                        }
                                    }
                                }
                                if child_builder.should_split() {
                                    let (new_page1, separator, new_page2) =
                                        child_builder.build_split()?;
                                    builder.push_child(new_page1.get_page_number());
                                    builder.push_key(&separator);
                                    builder.push_child(new_page2.get_page_number());
                                } else {
                                    let new_page = child_builder.build()?;
                                    builder.push_child(new_page.get_page_number());
                                }

                                let merged_key_index = max(child_index, merge_with);
                                if merged_key_index < accessor.count_children() - 1 {
                                    builder.push_key(accessor.key(merged_key_index).unwrap());
                                }
                            } else {
                                builder.push_child(page_number);
                                if i < accessor.count_children() - 1 {
                                    builder.push_key(accessor.key(i).unwrap());
                                }
                            }
                        }
                        let result = if let Some(only_child) = builder.to_single_child() {
                            DeletedInternal(only_child)
                        } else {
                            let new_page = builder.build()?;
                            let accessor = InternalAccessor::new(&new_page);
                            if accessor.total_length() < self.mem.get_page_size() / 2 {
                                PartialInternal(new_page.get_page_number())
                            } else {
                                Subtree(new_page.get_page_number())
                            }
                        };

                        let page_number = merge_with_page.get_page_number();
                        drop(merge_with_page);
                        self.free_policy
                            .conditional_free(page_number, self.freed, self.mem)?;
                        drop(partial_child_page);
                        self.free_policy
                            .conditional_free(partial_child, self.freed, self.mem)?;

                        result
                    }
                };

                self.free_policy
                    .conditional_free(original_page_number, self.freed, self.mem)?;

                Ok((final_result, found))
            }
            _ => unreachable!(),
        }
    }
}
