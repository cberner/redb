use crate::tree_store::btree_base::{
    AccessGuard, FreePolicy, IndexBuilder, InternalAccessor, InternalBuilder, InternalMutator,
    LeafAccessor, LeafBuilder2, BTREE_ORDER, INTERNAL, LEAF,
};
use crate::tree_store::btree_utils::DeletionResult::{PartialInternal, PartialLeaf, Subtree};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue};
use crate::Result;
use std::cmp::{max, min};

#[derive(Debug)]
pub(crate) enum DeletionResult {
    // A proper subtree
    Subtree(PageNumber),
    // A leaf subtree with too few entries
    PartialLeaf(Vec<(Vec<u8>, Vec<u8>)>),
    // A index page subtree with too few children
    PartialInternal(Vec<PageNumber>, Vec<Vec<u8>>),
}

// Pages must be in sorted order
pub(crate) fn make_index(
    children: &[PageNumber],
    keys: &[impl AsRef<[u8]>],
    manager: &TransactionalMemory,
) -> Result<PageNumber> {
    assert_eq!(children.len() - 1, keys.len());
    let key_size = keys.iter().map(|x| x.as_ref().len()).sum();
    let mut page = manager.allocate(InternalBuilder::required_bytes(keys.len(), key_size))?;
    let mut builder = InternalBuilder::new(&mut page, keys.len());
    builder.write_first_page(children[0]);
    for i in 1..children.len() {
        let key = &keys[i - 1];
        builder.write_nth_key(key.as_ref(), children[i], i - 1);
    }
    drop(builder);
    Ok(page.get_page_number())
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
//
// Safety: caller must ensure that no references to uncommitted pages in this table exist
pub(crate) unsafe fn tree_delete_helper<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    free_policy: FreePolicy,
    freed: &mut Vec<PageNumber>,
    manager: &'a TransactionalMemory,
) -> Result<(DeletionResult, Option<AccessGuard<'a, V>>)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let (position, found) = accessor.position::<K>(key);
            if !found {
                return Ok((Subtree(page.get_page_number()), None));
            }
            // TODO: also check if page is large enough
            let result = if accessor.num_pairs() < BTREE_ORDER / 2 {
                let mut partial = Vec::with_capacity(accessor.num_pairs());
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        continue;
                    }
                    let entry = accessor.entry(i).unwrap();
                    partial.push((entry.key().to_vec(), entry.value().to_vec()));
                }
                PartialLeaf(partial)
            } else {
                let mut builder = LeafBuilder2::new(manager);
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        continue;
                    }
                    let entry = accessor.entry(i).unwrap();
                    builder.push(entry.key(), entry.value());
                }
                Subtree(builder.build()?.get_page_number())
            };
            let uncommitted = manager.uncommitted(page.get_page_number());
            let free_on_drop = if !uncommitted || matches!(free_policy, FreePolicy::Never) {
                // Won't be freed until the end of the transaction, so returning the page
                // in the AccessGuard below is still safe
                freed.push(page.get_page_number());
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
                manager,
            ));
            Ok((result, guard))
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let original_page_number = page.get_page_number();
            let (child_index, child_page_number) = accessor.child_for_key::<K>(key);
            let (result, found) = tree_delete_helper::<K, V>(
                manager.get_page(child_page_number),
                key,
                free_policy,
                freed,
                manager,
            )?;
            if found.is_none() {
                return Ok((Subtree(original_page_number), None));
            }
            let final_result = match result {
                Subtree(new_child) => {
                    if new_child == child_page_number {
                        // NO-OP. One of our descendants is uncommitted, so there was no change
                        return Ok((Subtree(original_page_number), found));
                    } else if manager.uncommitted(original_page_number) {
                        drop(page);
                        // Safety: Caller guarantees there are no references to uncommitted pages,
                        // and we just dropped our reference to it on the line above
                        let mut mutpage = manager.get_page_mut(original_page_number);
                        let mut mutator = InternalMutator::new(&mut mutpage);
                        mutator.write_child_page(child_index, new_child);
                        return Ok((Subtree(original_page_number), found));
                    } else {
                        let mut new_page = manager.allocate(InternalBuilder::required_bytes(
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
                PartialLeaf(partials) => {
                    let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                    debug_assert!(merge_with < accessor.count_children());
                    let mut children = vec![];
                    let mut keys = vec![];
                    for i in 0..accessor.count_children() {
                        if i == child_index {
                            continue;
                        }
                        let page_number = accessor.child_page(i).unwrap();
                        if i == merge_with {
                            let mut child_builder = LeafBuilder2::new(manager);
                            if child_index < merge_with {
                                for (key, value) in partials.iter() {
                                    child_builder.push(key, value);
                                }
                            }
                            let merge_with_page = manager.get_page(page_number);
                            let merge_with_accessor = LeafAccessor::new(&merge_with_page);
                            for j in 0..merge_with_accessor.num_pairs() {
                                let entry = merge_with_accessor.entry(j).unwrap();
                                child_builder.push(entry.key(), entry.value());
                            }
                            if child_index > merge_with {
                                for (key, value) in partials.iter() {
                                    child_builder.push(key, value);
                                }
                            }
                            if child_builder.should_split() {
                                let (new_page1, new_page2) = child_builder.build_split()?;
                                let leaf_accessor = LeafAccessor::new(&new_page1);
                                keys.push(leaf_accessor.last_entry().key().to_vec());
                                children.push(new_page1.get_page_number());
                                children.push(new_page2.get_page_number());
                            } else {
                                let new_page = child_builder.build()?;
                                children.push(new_page.get_page_number());
                            }
                            drop(merge_with_page);
                            free_policy.conditional_free(page_number, freed, manager)?;

                            let merged_key_index = max(child_index, merge_with);
                            if merged_key_index < accessor.count_children() - 1 {
                                // TODO: find a way to optimize way this to_vec()?
                                keys.push(accessor.key(merged_key_index).unwrap().to_vec());
                            }
                        } else {
                            children.push(page_number);
                            if i < accessor.count_children() - 1 {
                                // TODO: find a way to optimize way this to_vec()?
                                keys.push(accessor.key(i).unwrap().to_vec());
                            }
                        }
                    }
                    if children.len() == 1 {
                        PartialInternal(children, vec![])
                    } else {
                        Subtree(make_index(&children, &keys, manager)?)
                    }
                }
                PartialInternal(partial_children, partial_keys) => {
                    let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                    debug_assert!(merge_with < accessor.count_children());
                    let mut children = vec![];
                    let mut keys = vec![];
                    for i in 0..accessor.count_children() {
                        if i == child_index {
                            continue;
                        }
                        let page_number = accessor.child_page(i).unwrap();
                        if i == merge_with {
                            let mut child_builder = IndexBuilder::new(manager);
                            let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                            let merge_with_page = manager.get_page(page_number);
                            let merge_with_accessor = InternalAccessor::new(&merge_with_page);
                            if child_index < merge_with {
                                for child in &partial_children {
                                    child_builder.push_child(*child);
                                }
                                for key in partial_keys.iter() {
                                    child_builder.push_key(key)
                                }
                                child_builder.push_key(separator_key);
                            }
                            for j in 0..merge_with_accessor.count_children() {
                                child_builder
                                    .push_child(merge_with_accessor.child_page(j).unwrap());
                                if j < merge_with_accessor.count_children() - 1 {
                                    child_builder.push_key(merge_with_accessor.key(j).unwrap());
                                }
                            }
                            if child_index > merge_with {
                                child_builder.push_key(separator_key);
                                for child in &partial_children {
                                    child_builder.push_child(*child);
                                }
                                for key in partial_keys.iter() {
                                    child_builder.push_key(key)
                                }
                            }
                            if child_builder.should_split() {
                                let (new_page1, separator, new_page2) =
                                    child_builder.build_split()?;
                                children.push(new_page1.get_page_number());
                                keys.push(separator);
                                children.push(new_page2.get_page_number());
                            } else {
                                let new_page = child_builder.build()?;
                                children.push(new_page.get_page_number());
                            }
                            drop(merge_with_page);
                            free_policy.conditional_free(page_number, freed, manager)?;

                            let merged_key_index = max(child_index, merge_with);
                            if merged_key_index < accessor.count_children() - 1 {
                                // TODO: find a way to optimize way this to_vec()?
                                keys.push(accessor.key(merged_key_index).unwrap().to_vec());
                            }
                        } else {
                            children.push(page_number);
                            if i < accessor.count_children() - 1 {
                                // TODO: find a way to optimize way this to_vec()?
                                keys.push(accessor.key(i).unwrap().to_vec());
                            }
                        }
                    }
                    if children.len() == 1 {
                        PartialInternal(children, vec![])
                    } else {
                        Subtree(make_index(&children, &keys, manager)?)
                    }
                }
            };

            free_policy.conditional_free(original_page_number, freed, manager)?;

            Ok((final_result, found))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::btree_utils::BTREE_ORDER;
    use crate::{Database, TableDefinition};
    use tempfile::NamedTempFile;

    const X: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    #[test]
    fn tree_balance() {
        fn expected_height(mut elements: usize) -> usize {
            // Root may have only 2 entries
            let mut height = 1;
            elements /= 2;

            // Leaves may have only a single entry
            height += 1;

            // Each internal node half-full, plus 1 to round up
            height += (elements as f32).log((BTREE_ORDER / 2) as f32) as usize + 1;

            height
        }

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        // One for the last table id counter, and one for the "x" -> TableDefinition entry
        let num_internal_entries = 2;

        let db = unsafe { Database::create(tmpfile.path(), 16 * 1024 * 1024).unwrap() };
        let txn = db.begin_write().unwrap();

        let elements = (BTREE_ORDER / 2).pow(2) as usize - num_internal_entries;

        {
            let mut table = txn.open_table(X).unwrap();
            for i in (0..elements).rev() {
                table.insert(&i.to_le_bytes(), b"").unwrap();
            }
        }
        txn.commit().unwrap();

        let expected = expected_height(elements + num_internal_entries);
        let txn = db.begin_write().unwrap();
        let height = txn.stats().unwrap().tree_height();
        assert!(
            height <= expected,
            "height={} expected={}",
            height,
            expected
        );

        let reduce_to = BTREE_ORDER / 2 - num_internal_entries;
        {
            let mut table = txn.open_table(X).unwrap();
            for i in 0..(elements - reduce_to) {
                table.remove(&i.to_le_bytes()).unwrap();
            }
        }
        txn.commit().unwrap();

        let expected = expected_height(reduce_to + num_internal_entries);
        let txn = db.begin_write().unwrap();
        let height = txn.stats().unwrap().tree_height();
        txn.abort().unwrap();
        assert!(
            height <= expected,
            "height={} expected={}",
            height,
            expected
        );
    }
}
