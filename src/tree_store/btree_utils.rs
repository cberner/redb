use crate::tree_store::btree_base::{
    AccessGuard, FreePolicy, IndexBuilder, InternalAccessor, InternalBuilder, InternalMutator,
    LeafAccessor, LeafBuilder2, BTREE_ORDER, INTERNAL, LEAF,
};
use crate::tree_store::btree_utils::DeletionResult::{PartialInternal, PartialLeaf, Subtree};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::tree_store::AccessGuardMut;
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

// partials must be in sorted order, and be disjoint from the pairs in the leaf
// Must return the pages in order
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn split_leaf<K: RedbKey + ?Sized>(
    leaf: PageNumber,
    partial: &[(Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
    free_policy: FreePolicy,
    freed: &mut Vec<PageNumber>,
) -> Result<(PageNumber, PageNumber)> {
    assert!(!partial.is_empty());
    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    let partials_last = K::compare(&partial[0].0, accessor.last_entry().key()).is_gt();

    let mut builder = LeafBuilder2::new(manager);
    if !partials_last {
        assert!(K::compare(&partial.last().unwrap().0, accessor.first_entry().key()).is_lt());
        for (key, value) in partial {
            builder.push(key, value);
        }
    }
    for i in 0..accessor.num_pairs() {
        let entry = accessor.entry(i).unwrap();
        builder.push(entry.key(), entry.value());
    }
    if partials_last {
        for (key, value) in partial {
            builder.push(key, value);
        }
    }
    let (new_page, new_page2) = builder.build_split()?;

    drop(page);
    free_policy.conditional_free(leaf, freed, manager)?;

    Ok((new_page.get_page_number(), new_page2.get_page_number()))
}

// partials must be in sorted order, and be disjoint from the pairs in the leaf
// returns None if the merged page would be too large
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn merge_leaf<K: RedbKey + ?Sized>(
    leaf: PageNumber,
    partial: &[(Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
    free_policy: FreePolicy,
    freed: &mut Vec<PageNumber>,
) -> Result<Option<PageNumber>> {
    if partial.is_empty() {
        return Ok(Some(leaf));
    }

    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    // TODO: also check that page won't be too large
    if accessor.num_pairs() + partial.len() > BTREE_ORDER {
        return Ok(None);
    }

    let mut builder = LeafBuilder2::new(manager);

    // partials are all strictly lesser or all greater than the entries in the page, since they're
    // being merged from a neighboring leaf
    let partial_after = K::compare(&partial[0].0, accessor.last_entry().key()).is_gt();
    if !partial_after {
        assert!(K::compare(&partial.last().unwrap().0, accessor.first_entry().key()).is_lt());
        for (key, value) in partial {
            builder.push(key, value);
        }
    }
    for i in 0..accessor.num_pairs() {
        let entry = accessor.entry(i).unwrap();
        builder.push(entry.key(), entry.value());
    }
    if partial_after {
        for (key, value) in partial {
            builder.push(key, value);
        }
    }
    let new_page = builder.build()?;

    drop(page);
    free_policy.conditional_free(leaf, freed, manager)?;

    Ok(Some(new_page.get_page_number()))
}

// Splits the page, if necessary, to fit the additional pages in `partial`
// Returns the pages in order along with the key that separates them
// Safety: caller must ensure that no references to uncommitted pages in this table exist
#[allow(clippy::too_many_arguments)]
unsafe fn split_index(
    index: PageNumber,
    partial_children: &[PageNumber],
    partial_keys: &[Vec<u8>],
    separator_key: &[u8],
    partial_last: bool,
    manager: &TransactionalMemory,
    free_policy: FreePolicy,
    freed: &mut Vec<PageNumber>,
) -> Result<Option<(PageNumber, Vec<u8>, PageNumber)>> {
    let page = manager.get_page(index);
    let accessor = InternalAccessor::new(&page);

    if accessor
        .child_page(BTREE_ORDER - partial_children.len())
        .is_none()
    {
        return Ok(None);
    }

    let mut pages = vec![];
    let mut keys = vec![];
    if !partial_last {
        pages.extend_from_slice(partial_children);
        for k in partial_keys {
            keys.push(k.as_slice());
        }
        keys.push(separator_key);
    }
    for i in 0..accessor.count_children() {
        let child = accessor.child_page(i).unwrap();
        pages.push(child);
        if i < accessor.count_children() - 1 {
            keys.push(accessor.key(i).unwrap());
        }
    }
    if partial_last {
        pages.extend_from_slice(partial_children);
        keys.push(separator_key);
        for k in partial_keys {
            keys.push(k.as_slice());
        }
    }

    let division = pages.len() / 2;

    let page1 = make_index(&pages[0..division], &keys[0..division - 1], manager)?;
    let separator = keys[division - 1].to_vec();
    let page2 = make_index(&pages[division..], &keys[division..], manager)?;
    drop(page);
    free_policy.conditional_free(index, freed, manager)?;

    Ok(Some((page1, separator, page2)))
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

// Safety: caller must ensure that no references to uncommitted pages in this table exist
#[allow(clippy::too_many_arguments)]
unsafe fn merge_index(
    index: PageNumber,
    partial_children: &[PageNumber],
    partial_keys: &[Vec<u8>],
    separator_key: &[u8],
    partial_last: bool,
    manager: &TransactionalMemory,
    free_policy: FreePolicy,
    freed: &mut Vec<PageNumber>,
) -> Result<PageNumber> {
    let page = manager.get_page(index);
    let accessor = InternalAccessor::new(&page);
    assert!(accessor
        .child_page(BTREE_ORDER - partial_children.len())
        .is_none());

    let mut builder = IndexBuilder::new(manager);
    if !partial_last {
        for child in partial_children {
            builder.push_child(*child);
        }
        for k in partial_keys.iter() {
            builder.push_key(k);
        }
        builder.push_key(separator_key);
    }
    for i in 0..accessor.count_children() {
        let child = accessor.child_page(i).unwrap();
        builder.push_child(child);
        if i < accessor.count_children() - 1 {
            builder.push_key(accessor.key(i).unwrap());
        }
    }
    if partial_last {
        builder.push_key(separator_key);
        for k in partial_keys.iter() {
            builder.push_key(k);
        }
        for child in partial_children {
            builder.push_child(*child);
        }
    }

    let result = builder.build().map(|p| p.get_page_number());
    drop(page);
    free_policy.conditional_free(index, freed, manager)?;

    result
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
                            if let Some(new_page) = merge_leaf::<K>(
                                page_number,
                                &partials,
                                manager,
                                free_policy,
                                freed,
                            )? {
                                children.push(new_page);
                            } else {
                                let (page1, page2) = split_leaf::<K>(
                                    page_number,
                                    &partials,
                                    manager,
                                    free_policy,
                                    freed,
                                )?;
                                children.push(page1);
                                let leaf1 = manager.get_page(page1);
                                let leaf_accessor = LeafAccessor::new(&leaf1);
                                keys.push(leaf_accessor.last_entry().key().to_vec());
                                children.push(page2);
                            }
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
                    // Whether the partial sorts after the child it is merging with
                    let partial_last = child_index > 0;
                    let separator_key = accessor.key(min(child_index, merge_with)).unwrap();
                    debug_assert!(merge_with < accessor.count_children());
                    let mut children = vec![];
                    let mut keys = vec![];
                    for i in 0..accessor.count_children() {
                        if i == child_index {
                            continue;
                        }
                        let page_number = accessor.child_page(i).unwrap();
                        if i == merge_with {
                            if let Some((page1, separator, page2)) = split_index(
                                page_number,
                                &partial_children,
                                &partial_keys,
                                separator_key,
                                partial_last,
                                manager,
                                free_policy,
                                freed,
                            )? {
                                children.push(page1);
                                keys.push(separator);
                                children.push(page2);
                            } else {
                                children.push(merge_index(
                                    page_number,
                                    &partial_children,
                                    &partial_keys,
                                    separator_key,
                                    partial_last,
                                    manager,
                                    free_policy,
                                    freed,
                                )?);
                            }
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

#[allow(clippy::type_complexity)]
// Safety: caller must ensure that no references to uncommitted pages in this table exist
pub(crate) unsafe fn tree_insert_helper<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    value: &[u8],
    freed: &mut Vec<PageNumber>,
    free_policy: FreePolicy,
    manager: &'a TransactionalMemory,
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
            let mut builder = LeafBuilder2::new(manager);
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
                free_policy.conditional_free(page_number, freed, manager)?;

                let new_page_number = new_page.get_page_number();
                let accessor = LeafAccessor::new(&new_page);
                let offset = accessor.offset_of_value(position).unwrap();
                let guard = AccessGuardMut::new(new_page, offset, value.len());

                (new_page_number, None, guard)
            } else {
                let (new_page1, new_page2) = builder.build_split()?;
                let page_number = page.get_page_number();
                drop(page);
                free_policy.conditional_free(page_number, freed, manager)?;

                let new_page_number = new_page1.get_page_number();
                let new_page_number2 = new_page2.get_page_number();
                let accessor = LeafAccessor::new(&new_page1);
                let division = accessor.num_pairs();
                let split_key = accessor.last_entry().key().to_vec();
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
            let (page1, more, guard) = tree_insert_helper::<K>(
                manager.get_page(child_page),
                key,
                value,
                freed,
                free_policy,
                manager,
            )?;

            if more.is_none() {
                // Check fast-path if no children were added
                if page1 == child_page {
                    // NO-OP. One of our descendants is uncommitted, so there was no change
                    return Ok((page.get_page_number(), None, guard));
                } else if manager.uncommitted(page.get_page_number()) {
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: Since the page is uncommitted, no other transactions could have it open
                    // and we just dropped our reference to it, on the line above
                    let mut mutpage = manager.get_page_mut(page_number);
                    let mut mutator = InternalMutator::new(&mut mutpage);
                    mutator.write_child_page(child_index, page1);
                    return Ok((mutpage.get_page_number(), None, guard));
                }
            }

            // A child was added, or we couldn't use the fast-path above
            let mut builder = IndexBuilder::new(manager);
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

            if builder.should_split() {
                let (new_page1, split_key, new_page2) = builder.build_split()?;
                // Free the original page, since we've replaced it
                let page_number = page.get_page_number();
                drop(page);
                // Safety: If the page is uncommitted, no other transactions can have references to it,
                // and we just dropped ours on the line above
                free_policy.conditional_free(page_number, freed, manager)?;
                (
                    new_page1.get_page_number(),
                    Some((split_key, new_page2.get_page_number())),
                    guard,
                )
            } else {
                let new_page = builder.build()?;
                // Free the original page, since we've replaced it
                let page_number = page.get_page_number();
                drop(page);
                // Safety: If the page is uncommitted, no other transactions can have references to it,
                // and we just dropped ours on the line above
                free_policy.conditional_free(page_number, freed, manager)?;
                (new_page.get_page_number(), None, guard)
            }
        }
        _ => unreachable!(),
    })
}

#[cfg(test)]
mod test {
    use crate::tree_store::btree_base::{FreePolicy, IndexBuilder, InternalAccessor, LeafBuilder};
    use crate::tree_store::btree_utils::{merge_index, BTREE_ORDER};
    use crate::tree_store::page_store::{Page, TransactionalMemory};
    use crate::tree_store::PageNumber;
    use crate::Result;
    use crate::{Database, TableDefinition};
    use tempfile::NamedTempFile;

    const X: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    fn make_leaf<'a>(
        key: &[u8],
        value: &[u8],
        manager: &'a TransactionalMemory,
    ) -> Result<PageNumber> {
        let mut page = manager.allocate(LeafBuilder::required_bytes(1, key.len() + value.len()))?;
        let mut builder = LeafBuilder::new(&mut page, 1, key.len());
        builder.append(key, value);
        drop(builder);

        Ok(page.get_page_number())
    }

    #[test]
    // Test for regression in index merge/split code found by fuzzer, where keys were sorted
    // in their binary order rather than by their RedbKey::compare order
    fn merge_regression() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let file = tmpfile.into_file();
        let mem = TransactionalMemory::new(file, 1024 * 1024, None, true).unwrap();
        let one = make_leaf(&1u64.to_le_bytes(), &[], &mem).unwrap();
        let two = make_leaf(&2u64.to_le_bytes(), &[], &mem).unwrap();
        // a value which has the second byte set, but zero in the first byte
        let next_byte = make_leaf(&256u64.to_le_bytes(), &[], &mem).unwrap();

        let mut builder = IndexBuilder::new(&mem);
        builder.push_child(one);
        builder.push_child(two);
        let key = 1u64.to_le_bytes();
        builder.push_key(&key);
        let index = builder.build().unwrap().get_page_number();

        let mut freed = vec![];
        let merged = unsafe {
            merge_index(
                index,
                &[next_byte],
                &[],
                &2u64.to_le_bytes(),
                true,
                &mem,
                FreePolicy::Never,
                &mut freed,
            )
        }
        .unwrap();
        let merged_page = mem.get_page(merged);
        let accessor = InternalAccessor::new(&merged_page);
        assert_eq!(accessor.count_children(), 3);
        assert_eq!(accessor.child_page(0).unwrap(), one);
        assert_eq!(accessor.child_page(1).unwrap(), two);
        assert_eq!(accessor.child_page(2).unwrap(), next_byte);
    }

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
