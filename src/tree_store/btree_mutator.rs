use crate::tree_store::btree_base::{
    BranchAccessor, BranchBuilder, BranchMutator, Checksum, FreePolicy, LeafAccessor, LeafBuilder,
    LeafMutator, BRANCH, DEFERRED, LEAF,
};
use crate::tree_store::btree_mutator::DeletionResult::{
    DeletedBranch, DeletedLeaf, PartialBranch, PartialLeaf, Subtree,
};
use crate::tree_store::page_store::{Page, PageImpl};
use crate::tree_store::{AccessGuardMut, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue};
use crate::{AccessGuard, Result};
use std::cmp::{max, min};
use std::marker::PhantomData;

// TODO: it seems like Checksum can be removed from most/all of these, now that we're using deferred checksums
#[derive(Debug)]
enum DeletionResult {
    // A proper subtree
    Subtree(PageNumber, Checksum),
    // A leaf with zero children
    DeletedLeaf,
    // A leaf with fewer entries than desired
    PartialLeaf { deleted_pair: usize },
    // A branch page subtree with fewer children than desired
    PartialBranch(PageNumber, Checksum),
    // Indicates that the branch node was deleted, and includes the only remaining child
    DeletedBranch(PageNumber, Checksum),
}

struct InsertionResult<'a, V: RedbValue> {
    // the new root page
    new_root: PageNumber,
    // checksum of the root page
    root_checksum: Checksum,
    // Following sibling, if the root had to be split
    additional_sibling: Option<(Vec<u8>, PageNumber, Checksum)>,
    // The inserted value for .insert_reserve() to use
    inserted_value: AccessGuardMut<'a, V>,
    // The previous value, if any
    old_value: Option<AccessGuard<'a, V>>,
}

pub(crate) struct MutateHelper<'a, 'b, K: RedbKey, V: RedbValue> {
    root: &'b mut Option<(PageNumber, Checksum)>,
    free_policy: FreePolicy,
    mem: &'a TransactionalMemory,
    freed: &'b mut Vec<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, 'b, K: RedbKey, V: RedbValue> MutateHelper<'a, 'b, K, V> {
    pub(crate) fn new(
        root: &'b mut Option<(PageNumber, Checksum)>,
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

    // TODO: can we remove this method now that delete is safe?
    pub(crate) fn safe_delete(
        &mut self,
        key: &K::SelfType<'_>,
    ) -> Result<Option<AccessGuard<'a, V>>> {
        assert_eq!(self.free_policy, FreePolicy::Never);
        self.delete(key)
    }

    pub(crate) fn delete(&mut self, key: &K::SelfType<'_>) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some((p, checksum)) = *self.root {
            let (deletion_result, found) =
                self.delete_helper(self.mem.get_page(p)?, checksum, K::as_bytes(key).as_ref())?;
            let new_root = match deletion_result {
                Subtree(page, checksum) => Some((page, checksum)),
                DeletedLeaf => None,
                PartialLeaf { deleted_pair } => {
                    let page = self.mem.get_page(p)?;
                    let accessor =
                        LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
                    let mut builder = LeafBuilder::new(
                        self.mem,
                        accessor.num_pairs() - 1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    builder.push_all_except(&accessor, Some(deleted_pair));
                    let page = builder.build()?;
                    Some((page.get_page_number(), DEFERRED))
                }
                PartialBranch(page_number, checksum) => Some((page_number, checksum)),
                DeletedBranch(remaining_child, checksum) => Some((remaining_child, checksum)),
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
    ) -> Result<(Option<AccessGuard<'a, V>>, AccessGuardMut<'a, V>)> {
        let (new_root, old_value, guard) = if let Some((p, checksum)) = *self.root {
            let result = self.insert_helper(
                self.mem.get_page(p)?,
                checksum,
                K::as_bytes(key).as_ref(),
                V::as_bytes(value).as_ref(),
            )?;

            let new_root = if let Some((key, page2, page2_checksum)) = result.additional_sibling {
                let mut builder = BranchBuilder::new(self.mem, 2, K::fixed_width());
                builder.push_child(result.new_root, result.root_checksum);
                builder.push_key(&key);
                builder.push_child(page2, page2_checksum);
                let new_page = builder.build()?;
                (new_page.get_page_number(), DEFERRED)
            } else {
                (result.new_root, result.root_checksum)
            };
            (new_root, result.old_value, result.inserted_value)
        } else {
            let key_bytes = K::as_bytes(key);
            let value_bytes = V::as_bytes(value);
            let key_bytes = key_bytes.as_ref();
            let value_bytes = value_bytes.as_ref();
            let mut builder = LeafBuilder::new(self.mem, 1, K::fixed_width(), V::fixed_width());
            builder.push(key_bytes, value_bytes);
            let page = builder.build()?;

            let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
            let offset = accessor.offset_of_first_value();
            let page_num = page.get_page_number();
            let guard = AccessGuardMut::new(page, offset, value_bytes.len());

            ((page_num, DEFERRED), None, guard)
        };
        *self.root = Some(new_root);
        Ok((old_value, guard))
    }

    fn insert_helper(
        &mut self,
        page: PageImpl<'a>,
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
                    && accessor.total_length() >= self.mem.get_page_size();
                if !found && single_large_value {
                    let mut builder =
                        LeafBuilder::new(self.mem, 1, K::fixed_width(), V::fixed_width());
                    builder.push(key, value);
                    let new_page = builder.build()?;
                    let new_page_number = new_page.get_page_number();
                    let new_page_accessor =
                        LeafAccessor::new(new_page.memory(), K::fixed_width(), V::fixed_width());
                    let offset = new_page_accessor.offset_of_first_value();
                    drop(new_page_accessor);
                    let guard = AccessGuardMut::new(new_page, offset, value.len());
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
                if self.mem.uncommitted(page.get_page_number())
                    && LeafMutator::sufficient_insert_inplace_space(
                        &page,
                        position,
                        found,
                        K::fixed_width(),
                        V::fixed_width(),
                        key,
                        value,
                    )
                {
                    let page_number = page.get_page_number();
                    let existing_value = if found {
                        let copied_value = accessor.entry(position).unwrap().value().to_vec();
                        Some(AccessGuard::with_owned_value(copied_value))
                    } else {
                        None
                    };
                    drop(page);
                    let mut page_mut = self.mem.get_page_mut(page_number)?;
                    let mut mutator =
                        LeafMutator::new(&mut page_mut, K::fixed_width(), V::fixed_width());
                    mutator.insert(position, found, key, value);
                    let new_page_accessor =
                        LeafAccessor::new(page_mut.memory(), K::fixed_width(), V::fixed_width());
                    let offset = new_page_accessor.offset_of_value(position).unwrap();
                    drop(new_page_accessor);
                    let guard = AccessGuardMut::new(page_mut, offset, value.len());
                    return Ok(InsertionResult {
                        new_root: page_number,
                        root_checksum: DEFERRED,
                        additional_sibling: None,
                        inserted_value: guard,
                        old_value: existing_value,
                    });
                }

                let mut builder = LeafBuilder::new(
                    self.mem,
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
                        let free_on_drop = self.free_policy.free_on_drop(page_number, self.mem);
                        if !free_on_drop {
                            self.freed.push(page_number);
                        }
                        Some(AccessGuard::new(
                            page,
                            start,
                            end - start,
                            free_on_drop,
                            self.mem,
                        ))
                    } else {
                        drop(page);
                        self.free_policy
                            .conditional_free(page_number, self.freed, self.mem);
                        None
                    };

                    let new_page_number = new_page.get_page_number();
                    let accessor =
                        LeafAccessor::new(new_page.memory(), K::fixed_width(), V::fixed_width());
                    let offset = accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMut::new(new_page, offset, value.len());

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
                        let free_on_drop = self.free_policy.free_on_drop(page_number, self.mem);
                        if !free_on_drop {
                            self.freed.push(page_number);
                        }
                        Some(AccessGuard::new(
                            page,
                            start,
                            end - start,
                            free_on_drop,
                            self.mem,
                        ))
                    } else {
                        drop(page);
                        self.free_policy
                            .conditional_free(page_number, self.freed, self.mem);
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
                        AccessGuardMut::new(new_page1, offset, value.len())
                    } else {
                        let accessor = LeafAccessor::new(
                            new_page2.memory(),
                            K::fixed_width(),
                            V::fixed_width(),
                        );
                        let offset = accessor.offset_of_value(position - division).unwrap();
                        AccessGuardMut::new(new_page2, offset, value.len())
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
                let sub_result =
                    self.insert_helper(self.mem.get_page(child_page)?, child_checksum, key, value)?;

                if sub_result.additional_sibling.is_none()
                    && self.mem.uncommitted(page.get_page_number())
                {
                    let page_number = page.get_page_number();
                    drop(page);
                    let mut mutpage = self.mem.get_page_mut(page_number)?;
                    let mut mutator = BranchMutator::new(&mut mutpage);
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
                let mut builder =
                    BranchBuilder::new(self.mem, accessor.count_children() + 1, K::fixed_width());
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
                self.free_policy
                    .conditional_free(page_number, self.freed, self.mem);

                result
            }
            _ => unreachable!(),
        })
    }

    fn delete_leaf_helper(
        &mut self,
        page: PageImpl<'a>,
        checksum: Checksum,
        key: &[u8],
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let accessor = LeafAccessor::new(page.memory(), K::fixed_width(), V::fixed_width());
        let (position, found) = accessor.position::<K>(key);
        if !found {
            return Ok((Subtree(page.get_page_number(), checksum), None));
        }
        let new_kv_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
            - accessor.length_of_pairs(position, position + 1);
        let new_required_bytes =
            LeafBuilder::required_bytes(accessor.num_pairs() - 1, new_kv_bytes);
        let uncommitted = self.mem.uncommitted(page.get_page_number());

        // Fast-path for dirty pages
        if uncommitted
            && new_required_bytes >= self.mem.get_page_size() / 2
            && accessor.num_pairs() > 1
        {
            let (start, end) = accessor.value_range(position).unwrap();
            let page_number = page.get_page_number();
            drop(page);
            let page_mut = self.mem.get_page_mut(page_number)?;

            let guard = AccessGuard::remove_on_drop(
                page_mut,
                start,
                end - start,
                position,
                K::fixed_width(),
                self.mem,
            );
            return Ok((Subtree(page_number, DEFERRED), Some(guard)));
        }

        let result = if accessor.num_pairs() == 1 {
            DeletedLeaf
        } else if new_required_bytes < self.mem.get_page_size() / 3 {
            // Merge when less than 33% full. Splits occur when a page is full and produce two 50%
            // full pages, so we use 33% instead of 50% to avoid oscillating
            PartialLeaf {
                deleted_pair: position,
            }
        } else {
            let mut builder = LeafBuilder::new(
                self.mem,
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
            Subtree(new_page.get_page_number(), DEFERRED)
        };
        let free_on_drop = if !uncommitted || matches!(self.free_policy, FreePolicy::Never) {
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

    fn finalize_branch_builder(&self, builder: BranchBuilder<'_, '_>) -> Result<DeletionResult> {
        let result = if let Some((only_child, checksum)) = builder.to_single_child() {
            DeletedBranch(only_child, checksum)
        } else {
            // TODO: can we optimize away this page allocation?
            // The PartialInternal gets returned, and then the caller has to merge it immediately
            let new_page = builder.build()?;
            let accessor = BranchAccessor::new(&new_page, K::fixed_width());
            // Merge when less than 33% full. Splits occur when a page is full and produce two 50%
            // full pages, so we use 33% instead of 50% to avoid oscillating
            if accessor.total_length() < self.mem.get_page_size() / 3 {
                PartialBranch(new_page.get_page_number(), DEFERRED)
            } else {
                Subtree(new_page.get_page_number(), DEFERRED)
            }
        };
        Ok(result)
    }

    fn delete_branch_helper(
        &mut self,
        page: PageImpl<'a>,
        checksum: Checksum,
        key: &[u8],
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let accessor = BranchAccessor::new(&page, K::fixed_width());
        let original_page_number = page.get_page_number();
        let (child_index, child_page_number) = accessor.child_for_key::<K>(key);
        let child_checksum = accessor.child_checksum(child_index).unwrap();
        let (result, found) =
            self.delete_helper(self.mem.get_page(child_page_number)?, child_checksum, key)?;
        if found.is_none() {
            return Ok((Subtree(original_page_number, checksum), None));
        }
        if let Subtree(new_child, new_child_checksum) = result {
            let result_page = if self.mem.uncommitted(original_page_number) {
                drop(page);
                let mut mutpage = self.mem.get_page_mut(original_page_number)?;
                let mut mutator = BranchMutator::new(&mut mutpage);
                mutator.write_child_page(child_index, new_child, new_child_checksum);
                original_page_number
            } else {
                let mut builder =
                    BranchBuilder::new(self.mem, accessor.count_children(), K::fixed_width());
                builder.push_all(&accessor);
                builder.replace_child(child_index, new_child, new_child_checksum);
                let new_page = builder.build()?;
                self.free_policy
                    .conditional_free(original_page_number, self.freed, self.mem);
                new_page.get_page_number()
            };
            return Ok((Subtree(result_page, DEFERRED), found));
        }

        // Child is requesting to be merged with a sibling
        let mut builder = BranchBuilder::new(self.mem, accessor.count_children(), K::fixed_width());

        let final_result = match result {
            Subtree(_, _) => {
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
                self.finalize_branch_builder(builder)?
            }
            PartialLeaf { deleted_pair } => {
                let partial_child_page = self.mem.get_page(child_page_number)?;
                let partial_child_accessor = LeafAccessor::new(
                    partial_child_page.memory(),
                    K::fixed_width(),
                    V::fixed_width(),
                );
                debug_assert!(partial_child_accessor.num_pairs() > 1);

                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                debug_assert!(merge_with < accessor.count_children());
                let merge_with_page = self
                    .mem
                    .get_page(accessor.child_page(merge_with).unwrap())?;
                let merge_with_accessor =
                    LeafAccessor::new(merge_with_page.memory(), K::fixed_width(), V::fixed_width());

                let single_large_value = merge_with_accessor.num_pairs() == 1
                    && merge_with_accessor.total_length() >= self.mem.get_page_size();
                // Don't try to merge or rebalance, if the sibling contains a single large value
                if single_large_value {
                    let mut child_builder = LeafBuilder::new(
                        self.mem,
                        partial_child_accessor.num_pairs() - 1,
                        K::fixed_width(),
                        V::fixed_width(),
                    );
                    child_builder.push_all_except(&partial_child_accessor, Some(deleted_pair));
                    let new_page = child_builder.build()?;
                    builder.push_all(&accessor);
                    builder.replace_child(child_index, new_page.get_page_number(), DEFERRED);

                    let result = self.finalize_branch_builder(builder)?;

                    drop(page);
                    self.free_policy
                        .conditional_free(original_page_number, self.freed, self.mem);
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
                            self.mem,
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

                let result = self.finalize_branch_builder(builder)?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.free_policy
                    .conditional_free(page_number, self.freed, self.mem);
                // child_page_number does not need to be freed, because it's a leaf and the
                // MutAccessGuard will free it

                result
            }
            DeletedBranch(only_grandchild, grandchild_checksum) => {
                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                let merge_with_page = self
                    .mem
                    .get_page(accessor.child_page(merge_with).unwrap())?;
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
                            self.mem,
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
                let result = self.finalize_branch_builder(builder)?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.free_policy
                    .conditional_free(page_number, self.freed, self.mem);

                result
            }
            PartialBranch(partial_child, ..) => {
                let partial_child_page = self.mem.get_page(partial_child)?;
                let partial_child_accessor =
                    BranchAccessor::new(&partial_child_page, K::fixed_width());
                let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                let merge_with_page = self
                    .mem
                    .get_page(accessor.child_page(merge_with).unwrap())?;
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
                            self.mem,
                            merge_with_accessor.count_children()
                                + partial_child_accessor.count_children(),
                            K::fixed_width(),
                        );
                        let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                        if child_index < merge_with {
                            child_builder.push_all(&partial_child_accessor);
                            child_builder.push_key(separator_key);
                        }
                        child_builder.push_all(&merge_with_accessor);
                        if child_index > merge_with {
                            child_builder.push_key(separator_key);
                            child_builder.push_all(&partial_child_accessor);
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
                let result = self.finalize_branch_builder(builder)?;

                let page_number = merge_with_page.get_page_number();
                drop(merge_with_page);
                self.free_policy
                    .conditional_free(page_number, self.freed, self.mem);
                drop(partial_child_page);
                self.free_policy
                    .conditional_free(partial_child, self.freed, self.mem);

                result
            }
        };

        drop(page);
        self.free_policy
            .conditional_free(original_page_number, self.freed, self.mem);

        Ok((final_result, found))
    }

    // Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
    // If key is not found, guaranteed not to modify the tree
    fn delete_helper(
        &mut self,
        page: PageImpl<'a>,
        checksum: Checksum,
        key: &[u8],
    ) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
        let node_mem = page.memory();
        match node_mem[0] {
            LEAF => self.delete_leaf_helper(page, checksum, key),
            BRANCH => self.delete_branch_helper(page, checksum, key),
            _ => unreachable!(),
        }
    }
}
