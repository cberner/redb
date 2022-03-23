use crate::tree_store::btree_base::{
    AccessGuard, InternalAccessor, InternalBuilder, InternalMutator, LeafAccessor, LeafBuilder,
    BTREE_ORDER, INTERNAL, LEAF,
};
use crate::tree_store::btree_utils::DeletionResult::{PartialInternal, PartialLeaf, Subtree};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::tree_store::AccessGuardMut;
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Error;
use crate::Result;
use std::cmp::max;

pub(crate) fn tree_height<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => 1,
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut max_child_height = 0;
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    let height = tree_height(manager.get_page(child), manager);
                    max_child_height = max(max_child_height, height);
                }
            }

            max_child_height + 1
        }
        _ => unreachable!(),
    }
}

pub(crate) fn stored_bytes<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            accessor.length_of_pairs(0, accessor.num_pairs())
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut bytes = 0;
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    bytes += stored_bytes(manager.get_page(child), manager);
                }
            }

            bytes
        }
        _ => unreachable!(),
    }
}

pub(crate) fn overhead_bytes<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            accessor.total_length() - accessor.length_of_pairs(0, accessor.num_pairs())
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            // Internal pages are all "overhead"
            let mut bytes = accessor.total_length();
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    bytes += overhead_bytes(manager.get_page(child), manager);
                }
            }

            bytes
        }
        _ => unreachable!(),
    }
}

pub(crate) fn fragmented_bytes<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            page.memory().len() - accessor.total_length()
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            // Internal pages are all "overhead"
            let mut bytes = page.memory().len() - accessor.total_length();
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    bytes += fragmented_bytes(manager.get_page(child), manager);
                }
            }

            bytes
        }
        _ => unreachable!(),
    }
}

pub(crate) fn print_node(page: &impl Page) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page);
            eprint!("Leaf[ (page={:?})", page.get_page_number(),);
            let mut i = 0;
            while let Some(entry) = accessor.entry(i) {
                eprint!(" key_{}={:?}", i, entry.key());
                i += 1;
            }
            eprint!("]");
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(page);
            eprint!(
                "Internal[ (page={:?}), child_0={:?}",
                page.get_page_number(),
                accessor.child_page(0).unwrap()
            );
            for i in 0..(accessor.count_children() - 1) {
                if let Some(child) = accessor.child_page(i + 1) {
                    let key = accessor.key(i).unwrap();
                    eprint!(" key_{}={:?}", i, key);
                    eprint!(" child_{}={:?}", i + 1, child);
                }
            }
            eprint!("]");
        }
        _ => unreachable!(),
    }
}

pub(crate) fn node_children<'a>(
    page: &PageImpl<'a>,
    manager: &'a TransactionalMemory,
) -> Vec<PageImpl<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            vec![]
        }
        INTERNAL => {
            let mut children = vec![];
            let accessor = InternalAccessor::new(page);
            for i in 0..accessor.count_children() {
                let child = accessor.child_page(i).unwrap();
                children.push(manager.get_page(child));
            }
            children
        }
        _ => unreachable!(),
    }
}

pub(crate) fn print_tree<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) {
    let mut pages = vec![page];
    while !pages.is_empty() {
        let mut next_children = vec![];
        for page in pages.drain(..) {
            next_children.extend(node_children(&page, manager));
            print_node(&page);
            eprint!("  ");
        }
        eprintln!();

        pages = next_children;
    }
}

// Returns the new root, bool indicating if the key existed, and a list of freed pages
// Safety: see tree_delete_helper()
#[allow(clippy::type_complexity)]
pub(crate) unsafe fn tree_delete<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    free_uncommitted: bool,
    manager: &'a TransactionalMemory,
) -> Result<
    (
        Option<PageNumber>,
        Option<AccessGuard<'a, V>>,
        Vec<PageNumber>,
    ),
    Error,
> {
    let mut freed = vec![];
    let (deletion_result, found) =
        tree_delete_helper::<K, V>(page, key, free_uncommitted, &mut freed, manager)?;
    let result = match deletion_result {
        DeletionResult::Subtree(page) => Some(page),
        DeletionResult::PartialLeaf(entries) => {
            if entries.is_empty() {
                None
            } else {
                let size: usize = entries.iter().map(|(k, v)| k.len() + v.len()).sum();
                let key_size: usize = entries.iter().map(|(k, _)| k.len()).sum();
                let mut page =
                    manager.allocate(LeafBuilder::required_bytes(entries.len(), size))?;
                let mut builder = LeafBuilder::new(&mut page, entries.len(), key_size);
                for (key, value) in entries {
                    builder.append(&key, &value);
                }
                drop(builder);
                Some(page.get_page_number())
            }
        }
        DeletionResult::PartialInternal(pages) => {
            assert_eq!(pages.len(), 1);
            Some(pages[0])
        }
    };
    Ok((result, found, freed))
}

#[derive(Debug)]
enum DeletionResult {
    // A proper subtree
    Subtree(PageNumber),
    // A leaf subtree with too few entries
    PartialLeaf(Vec<(Vec<u8>, Vec<u8>)>),
    // A index page subtree with too few children
    PartialInternal(Vec<PageNumber>),
}

// partials must be in sorted order, and be disjoint from the pairs in the leaf
// Must return the pages in order
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn split_leaf<K: RedbKey + ?Sized>(
    leaf: PageNumber,
    partial: &[(Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
    free_uncommitted: bool,
    freed: &mut Vec<PageNumber>,
) -> Result<(PageNumber, PageNumber)> {
    assert!(!partial.is_empty());
    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    assert!(partial.len() <= accessor.num_pairs());
    let partials_last = K::compare(&partial[0].0, accessor.last_entry().key()).is_gt();
    if !partials_last {
        assert!(K::compare(&partial.last().unwrap().0, accessor.first_entry().key()).is_lt());
    }

    let division = if partials_last {
        (accessor.num_pairs() + partial.len()) / 2
    } else {
        (accessor.num_pairs() + partial.len()) / 2 - partial.len()
    };

    let mut new_size1 = accessor.length_of_pairs(0, division);
    let mut new_key_size1 = accessor.length_of_keys(0, division);
    let mut new_size2 = accessor.length_of_pairs(division, accessor.num_pairs());
    let mut new_key_size2 = accessor.length_of_keys(division, accessor.num_pairs());
    let partial_size = partial
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>();
    let partial_key_size = partial.iter().map(|(k, _)| k.len()).sum::<usize>();
    let (num_pairs1, num_pairs2) = if partials_last {
        new_size2 += partial_size;
        new_key_size2 += partial_key_size;
        (division, accessor.num_pairs() - division + partial.len())
    } else {
        new_size1 += partial_size;
        new_key_size1 += partial_key_size;
        (partial.len() + division, accessor.num_pairs() - division)
    };

    let mut new_page = manager.allocate(LeafBuilder::required_bytes(num_pairs1, new_size1))?;
    let mut builder = LeafBuilder::new(&mut new_page, num_pairs1, new_key_size1);

    if !partials_last {
        for (key, value) in partial {
            builder.append(key, value);
        }
    }
    for i in 0..division {
        let entry = accessor.entry(i).unwrap();
        builder.append(entry.key(), entry.value());
    }
    drop(builder);

    let mut new_page2 = manager.allocate(LeafBuilder::required_bytes(num_pairs2, new_size2))?;
    let mut builder = LeafBuilder::new(&mut new_page2, num_pairs2, new_key_size2);
    for i in division..accessor.num_pairs() {
        let entry = accessor.entry(i).unwrap();
        builder.append(entry.key(), entry.value());
    }
    if partials_last {
        for (key, value) in partial {
            builder.append(key, value);
        }
    }
    drop(builder);

    drop(page);
    if !(free_uncommitted && manager.free_if_uncommitted(leaf)?) {
        freed.push(leaf);
    }

    Ok((new_page.get_page_number(), new_page2.get_page_number()))
}

// partials must be in sorted order, and be disjoint from the pairs in the leaf
// returns None if the merged page would be too large
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn merge_leaf<K: RedbKey + ?Sized>(
    leaf: PageNumber,
    partial: &[(Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
    free_uncommitted: bool,
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

    let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
    let new_size = old_size
        + partial
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
    let new_key_size = accessor.length_of_keys(0, accessor.num_pairs())
        + partial.iter().map(|(k, _)| k.len()).sum::<usize>();
    let mut new_page = manager.allocate(LeafBuilder::required_bytes(
        accessor.num_pairs() + partial.len(),
        new_size,
    ))?;
    let mut builder = LeafBuilder::new(
        &mut new_page,
        accessor.num_pairs() + partial.len(),
        new_key_size,
    );

    // partials are all strictly lesser or all greater than the entries in the page, since they're
    // being merged from a neighboring leaf
    let partial_after = K::compare(&partial[0].0, accessor.last_entry().key()).is_gt();
    if !partial_after {
        assert!(K::compare(&partial.last().unwrap().0, accessor.first_entry().key()).is_lt());
        for (key, value) in partial {
            builder.append(key, value);
        }
    }
    for i in 0..accessor.num_pairs() {
        let entry = accessor.entry(i).unwrap();
        builder.append(entry.key(), entry.value());
    }
    if partial_after {
        for (key, value) in partial {
            builder.append(key, value);
        }
    }
    drop(builder);

    drop(page);
    if !(free_uncommitted && manager.free_if_uncommitted(leaf)?) {
        freed.push(leaf);
    }

    Ok(Some(new_page.get_page_number()))
}

// Splits the page, if necessary, to fit the additional pages in `partial`
// Returns the pages in order
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn split_index(
    index: PageNumber,
    partial: &[PageNumber],
    manager: &TransactionalMemory,
    free_uncommitted: bool,
    freed: &mut Vec<PageNumber>,
) -> Result<Option<(PageNumber, PageNumber)>> {
    let page = manager.get_page(index);
    let accessor = InternalAccessor::new(&page);

    if accessor.child_page(BTREE_ORDER - partial.len()).is_none() {
        return Ok(None);
    }

    let mut pages = vec![];
    pages.extend_from_slice(partial);
    for i in 0..accessor.count_children() {
        let child = accessor.child_page(i).unwrap();
        pages.push(child);
    }

    pages.sort_by_key(|p| max_key(manager.get_page(*p), manager));

    let division = pages.len() / 2;

    let page1 = make_index_many_pages(&pages[0..division], manager)?;
    let page2 = make_index_many_pages(&pages[division..], manager)?;
    drop(page);
    if !(free_uncommitted && manager.free_if_uncommitted(index)?) {
        freed.push(index);
    }

    Ok(Some((page1, page2)))
}

// Pages must be in sorted order
fn make_index_many_pages(
    children: &[PageNumber],
    manager: &TransactionalMemory,
) -> Result<PageNumber> {
    let mut keys = vec![];
    let mut key_size = 0;
    for i in 1..children.len() {
        let key = max_key(manager.get_page(children[i - 1]), manager);
        key_size += key.len();
        keys.push(key);
    }
    let mut page = manager.allocate(InternalBuilder::required_bytes(
        children.len() - 1,
        key_size,
    ))?;
    let mut builder = InternalBuilder::new(&mut page, children.len() - 1);
    builder.write_first_page(children[0]);
    for i in 1..children.len() {
        let key = &keys[i - 1];
        builder.write_nth_key(key, children[i], i - 1);
    }
    drop(builder);
    Ok(page.get_page_number())
}

// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn merge_index(
    index: PageNumber,
    partial: &[PageNumber],
    manager: &TransactionalMemory,
    free_uncommitted: bool,
    freed: &mut Vec<PageNumber>,
) -> Result<PageNumber> {
    let page = manager.get_page(index);
    let accessor = InternalAccessor::new(&page);
    assert!(accessor.child_page(BTREE_ORDER - partial.len()).is_none());

    let mut pages = vec![];
    pages.extend_from_slice(partial);
    for i in 0..accessor.count_children() {
        let child = accessor.child_page(i).unwrap();
        pages.push(child);
    }

    pages.sort_by_key(|p| max_key(manager.get_page(*p), manager));
    assert!(pages.len() <= BTREE_ORDER);

    drop(page);
    if !(free_uncommitted && manager.free_if_uncommitted(index)?) {
        freed.push(index);
    }

    make_index_many_pages(&pages, manager)
}

fn max_key(page: PageImpl, manager: &TransactionalMemory) -> Vec<u8> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            accessor.last_entry().key().to_vec()
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let last_child = accessor.child_page(accessor.count_children() - 1).unwrap();
            max_key(manager.get_page(last_child), manager)
        }
        _ => unreachable!(),
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
//
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn tree_delete_helper<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    free_uncommitted: bool,
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
            if accessor.num_pairs() < BTREE_ORDER / 2 {
                let mut partial = Vec::with_capacity(accessor.num_pairs());
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        continue;
                    }
                    let entry = accessor.entry(i).unwrap();
                    partial.push((entry.key().to_vec(), entry.value().to_vec()));
                }
                let uncommitted = manager.uncommitted(page.get_page_number());
                if !(uncommitted && free_uncommitted) {
                    // Won't be freed until the end of the transaction, so returning the page
                    // in the AccessGuard below is still safe
                    freed.push(page.get_page_number());
                }
                let (start, end) = accessor.value_range(position).unwrap();
                let guard = AccessGuard::new(
                    page,
                    start,
                    end - start,
                    uncommitted && free_uncommitted,
                    manager,
                );
                Ok((PartialLeaf(partial), Some(guard)))
            } else {
                let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
                let new_size =
                    old_size - key.len() - accessor.entry(position).unwrap().value().len();
                let new_key_size = accessor.length_of_keys(0, accessor.num_pairs()) - key.len();
                let mut new_page = manager.allocate(LeafBuilder::required_bytes(
                    accessor.num_pairs() - 1,
                    new_size,
                ))?;
                let mut builder =
                    LeafBuilder::new(&mut new_page, accessor.num_pairs() - 1, new_key_size);
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        continue;
                    }
                    let entry = accessor.entry(i).unwrap();
                    builder.append(entry.key(), entry.value());
                }
                drop(builder);
                let uncommitted = manager.uncommitted(page.get_page_number());
                if !(uncommitted && free_uncommitted) {
                    // Won't be freed until the end of the transaction, so returning the page
                    // in the AccessGuard below is still safe
                    freed.push(page.get_page_number());
                }
                let (start, end) = accessor.value_range(position).unwrap();
                let guard = AccessGuard::new(
                    page,
                    start,
                    end - start,
                    uncommitted && free_uncommitted,
                    manager,
                );
                Ok((Subtree(new_page.get_page_number()), Some(guard)))
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let original_page_number = page.get_page_number();
            let (child_index, child_page_number) = accessor.child_for_key::<K>(key);
            let (result, found) = tree_delete_helper::<K, V>(
                manager.get_page(child_page_number),
                key,
                free_uncommitted,
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
                        copy_to_builder_and_patch(
                            &accessor,
                            0,
                            accessor.count_children(),
                            &mut builder,
                            child_index,
                            new_child,
                            None,
                        );

                        drop(builder);
                        Subtree(new_page.get_page_number())
                    }
                }
                PartialLeaf(partials) => {
                    let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                    debug_assert!(merge_with < accessor.count_children());
                    let mut children = vec![];
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
                                free_uncommitted,
                                freed,
                            )? {
                                children.push(new_page);
                            } else {
                                let (page1, page2) = split_leaf::<K>(
                                    page_number,
                                    &partials,
                                    manager,
                                    free_uncommitted,
                                    freed,
                                )?;
                                children.push(page1);
                                children.push(page2);
                            }
                        } else {
                            children.push(page_number);
                        }
                    }
                    if children.len() == 1 {
                        PartialInternal(children)
                    } else {
                        Subtree(make_index_many_pages(&children, manager)?)
                    }
                }
                PartialInternal(partials) => {
                    let merge_with = if child_index == 0 { 1 } else { child_index - 1 };
                    debug_assert!(merge_with < accessor.count_children());
                    let mut children = vec![];
                    for i in 0..accessor.count_children() {
                        if i == child_index {
                            continue;
                        }
                        let page_number = accessor.child_page(i).unwrap();
                        if i == merge_with {
                            if let Some((page1, page2)) = split_index(
                                page_number,
                                &partials,
                                manager,
                                free_uncommitted,
                                freed,
                            )? {
                                children.push(page1);
                                children.push(page2);
                            } else {
                                children.push(merge_index(
                                    page_number,
                                    &partials,
                                    manager,
                                    free_uncommitted,
                                    freed,
                                )?);
                            }
                        } else {
                            children.push(page_number);
                        }
                    }
                    if children.len() == 1 {
                        PartialInternal(children)
                    } else {
                        Subtree(make_index_many_pages(&children, manager)?)
                    }
                }
            };

            if !(free_uncommitted && manager.free_if_uncommitted(original_page_number)?) {
                freed.push(original_page_number);
            }

            Ok((final_result, found))
        }
        _ => unreachable!(),
    }
}

pub(crate) fn make_mut_single_leaf<'a>(
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(PageNumber, AccessGuardMut<'a>)> {
    let mut page = manager.allocate(LeafBuilder::required_bytes(1, key.len() + value.len()))?;
    let mut builder = LeafBuilder::new(&mut page, 1, key.len());
    builder.append(key, value);
    drop(builder);

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_first_value();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value.len());

    Ok((page_num, guard))
}

pub(crate) fn make_index(
    key: &[u8],
    lte_page: PageNumber,
    gt_page: PageNumber,
    manager: &TransactionalMemory,
) -> Result<PageNumber> {
    let mut page = manager.allocate(InternalBuilder::required_bytes(1, key.len()))?;
    let mut builder = InternalBuilder::new(&mut page, 1);
    builder.write_first_page(lte_page);
    builder.write_nth_key(key, gt_page, 0);
    drop(builder);
    Ok(page.get_page_number())
}

// Returns the page number of the sub-tree into which the key was inserted,
// and the guard which can be used to access the value, and a list of freed pages
// Safety: see tree_insert_helper
pub(crate) unsafe fn tree_insert<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(PageNumber, AccessGuardMut<'a>, Vec<PageNumber>)> {
    let mut freed = vec![];
    let (page1, more, guard) = tree_insert_helper::<K>(page, key, value, &mut freed, manager)?;

    if let Some((key, page2)) = more {
        let index_page = make_index(&key, page1, page2, manager)?;
        Ok((index_page, guard, freed))
    } else {
        Ok((page1, guard, freed))
    }
}

// Patch is applied at patch_index of the accessor children, using patch_handle to replace the child,
// and inserting patch_extension after it
// copies [start_child, end_child)
fn copy_to_builder_and_patch<'a>(
    accessor: &InternalAccessor<PageImpl<'a>>,
    start_child: usize,
    end_child: usize,
    builder: &mut InternalBuilder,
    patch_index: usize,
    patch_handle: PageNumber,
    patch_extension: Option<(&[u8], PageNumber)>,
) {
    let mut dest = 0;
    if patch_index == start_child {
        builder.write_first_page(patch_handle);
        if let Some((extra_key, extra_handle)) = patch_extension {
            builder.write_nth_key(extra_key, extra_handle, dest);
            dest += 1;
        }
    } else {
        builder.write_first_page(accessor.child_page(start_child).unwrap());
    }

    for i in (start_child + 1)..end_child {
        if let Some(key) = accessor.key(i - 1) {
            let handle = if i == patch_index {
                patch_handle
            } else {
                accessor.child_page(i).unwrap()
            };
            builder.write_nth_key(key, handle, dest);
            dest += 1;
            if i == patch_index as usize {
                if let Some((extra_key, extra_handle)) = patch_extension {
                    builder.write_nth_key(extra_key, extra_handle, dest);
                    dest += 1;
                };
            }
        } else {
            break;
        }
    }
}

#[allow(clippy::type_complexity)]
// Safety: caller must ensure that no references to uncommitted pages in this table exist
unsafe fn tree_insert_helper<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    value: &[u8],
    freed: &mut Vec<PageNumber>,
    manager: &'a TransactionalMemory,
) -> Result<
    (
        PageNumber,
        Option<(Vec<u8>, PageNumber)>,
        AccessGuardMut<'a>,
    ),
    Error,
> {
    let node_mem = page.memory();
    Ok(match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let (position, found) = accessor.position::<K>(key);
            if found {
                // Overwrite existing key
                let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
                let new_size =
                    old_size + value.len() - accessor.entry(position).unwrap().value().len();
                let mut new_page = manager
                    .allocate(LeafBuilder::required_bytes(accessor.num_pairs(), new_size))?;
                let mut builder = LeafBuilder::new(
                    &mut new_page,
                    accessor.num_pairs(),
                    accessor.length_of_keys(0, accessor.num_pairs()),
                );
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        builder.append(key, value);
                    } else {
                        let entry = accessor.entry(i).unwrap();
                        builder.append(entry.key(), entry.value());
                    }
                }
                drop(builder);

                let page_number = page.get_page_number();
                drop(page);
                if !manager.free_if_uncommitted(page_number)? {
                    freed.push(page_number);
                }

                let new_page_number = new_page.get_page_number();
                let accessor = LeafAccessor::new(&new_page);
                let offset = accessor.offset_of_value(position).unwrap();
                let guard = AccessGuardMut::new(new_page, offset, value.len());

                (new_page_number, None, guard)
            } else {
                let total_pairs_size =
                    accessor.length_of_pairs(0, accessor.num_pairs()) + key.len() + value.len();
                let total_size =
                    LeafBuilder::required_bytes(accessor.num_pairs() + 1, total_pairs_size);
                // Split the leaf if it's larger than a native page
                let too_large = total_size > manager.get_page_size() && accessor.num_pairs() > 1;
                if accessor.num_pairs() >= BTREE_ORDER || too_large {
                    // split
                    let division = accessor.num_pairs() / 2;
                    let mut new_size1 = accessor.length_of_pairs(0, division);
                    let mut new_key_size1 = accessor.length_of_keys(0, division);
                    let mut new_count1 = division;
                    let mut new_size2 = accessor.length_of_pairs(division, accessor.num_pairs());
                    let mut new_key_size2 = accessor.length_of_keys(division, accessor.num_pairs());
                    let mut new_count2 = accessor.num_pairs() - division;
                    if position < division {
                        new_size1 += key.len() + value.len();
                        new_key_size1 += key.len();
                        new_count1 += 1;
                    } else {
                        new_size2 += key.len() + value.len();
                        new_key_size2 += key.len();
                        new_count2 += 1;
                    }

                    let mut new_page1 =
                        manager.allocate(LeafBuilder::required_bytes(new_count1, new_size1))?;
                    let mut builder = LeafBuilder::new(&mut new_page1, new_count1, new_key_size1);
                    for i in 0..division {
                        if i == position {
                            builder.append(key, value);
                        }
                        let entry = accessor.entry(i).unwrap();
                        builder.append(entry.key(), entry.value());
                    }
                    drop(builder);

                    let mut new_page2 =
                        manager.allocate(LeafBuilder::required_bytes(new_count2, new_size2))?;
                    let mut builder = LeafBuilder::new(&mut new_page2, new_count2, new_key_size2);
                    for i in division..accessor.num_pairs() {
                        if i == position {
                            builder.append(key, value);
                        }
                        let entry = accessor.entry(i).unwrap();
                        builder.append(entry.key(), entry.value());
                    }
                    if accessor.num_pairs() == position {
                        builder.append(key, value);
                    }
                    drop(builder);

                    let page_number = page.get_page_number();
                    drop(page);
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }

                    let new_page_number = new_page1.get_page_number();
                    let new_page_number2 = new_page2.get_page_number();
                    let accessor = LeafAccessor::new(&new_page1);
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
                } else {
                    // insert. leaf is not full

                    let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
                    let new_size = old_size + key.len() + value.len();
                    let new_key_size = accessor.length_of_keys(0, accessor.num_pairs()) + key.len();
                    let mut new_page = manager.allocate(LeafBuilder::required_bytes(
                        accessor.num_pairs() + 1,
                        new_size,
                    ))?;
                    let mut builder =
                        LeafBuilder::new(&mut new_page, accessor.num_pairs() + 1, new_key_size);
                    for i in 0..accessor.num_pairs() {
                        if i == position {
                            builder.append(key, value);
                        }
                        let entry = accessor.entry(i).unwrap();
                        builder.append(entry.key(), entry.value());
                    }
                    if accessor.num_pairs() == position {
                        builder.append(key, value);
                    }
                    drop(builder);

                    let page_number = page.get_page_number();
                    drop(page);
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }

                    let new_page_number = new_page.get_page_number();
                    let accessor = LeafAccessor::new(&new_page);
                    let offset = accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMut::new(new_page, offset, value.len());

                    (new_page_number, None, guard)
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (child_index, child_page) = accessor.child_for_key::<K>(key);
            let (page1, more, guard) =
                tree_insert_helper::<K>(manager.get_page(child_page), key, value, freed, manager)?;

            if let Some((index_key2, page2)) = more {
                let new_children_count = 1 + accessor.count_children();

                // TODO: also check if page is large enough
                if new_children_count <= BTREE_ORDER {
                    // Rewrite page since we're splitting a child
                    let mut new_page = manager.allocate(InternalBuilder::required_bytes(
                        new_children_count - 1,
                        accessor.total_key_length() + index_key2.len(),
                    ))?;
                    let mut builder = InternalBuilder::new(&mut new_page, new_children_count - 1);

                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        accessor.count_children(),
                        &mut builder,
                        child_index,
                        page1,
                        Some((&index_key2, page2)),
                    );
                    drop(builder);
                    // Free the original page, since we've replaced it
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: If the page is uncommitted, no other transactions can have references to it,
                    // and we just dropped ours on the line above
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }
                    (new_page.get_page_number(), None, guard)
                } else {
                    // TODO: optimize to remove these Vecs
                    let mut children = vec![];
                    let mut index_keys: Vec<&[u8]> = vec![];

                    if child_index == 0 {
                        children.push(page1);
                        index_keys.push(&index_key2);
                        children.push(page2);
                    } else {
                        children.push(accessor.child_page(0).unwrap());
                    };
                    for i in 1..accessor.count_children() {
                        if let Some(temp_key) = accessor.key(i - 1) {
                            index_keys.push(temp_key);
                            if i == child_index as usize {
                                children.push(page1);
                                index_keys.push(&index_key2);
                                children.push(page2);
                            } else {
                                children.push(accessor.child_page(i).unwrap());
                            };
                        } else {
                            break;
                        }
                    }

                    // TODO: split based on size, if page is going to be too large
                    let division = BTREE_ORDER / 2;

                    // Rewrite page since we're splitting a child
                    let key_size = index_keys[0..division].iter().map(|k| k.len()).sum();
                    let mut new_page =
                        manager.allocate(InternalBuilder::required_bytes(division, key_size))?;
                    let mut builder = InternalBuilder::new(&mut new_page, division);

                    builder.write_first_page(children[0]);
                    for i in 0..division {
                        let key = &index_keys[i];
                        builder.write_nth_key(key, children[i + 1], i);
                    }
                    drop(builder);

                    let index_key = &index_keys[division];

                    let key_size = index_keys[(division + 1)..].iter().map(|k| k.len()).sum();
                    let mut new_page2 = manager.allocate(InternalBuilder::required_bytes(
                        index_keys.len() - division - 1,
                        key_size,
                    ))?;
                    let mut builder2 =
                        InternalBuilder::new(&mut new_page2, index_keys.len() - division - 1);
                    builder2.write_first_page(children[division + 1]);
                    for i in (division + 1)..index_keys.len() {
                        let key = &index_keys[i];
                        builder2.write_nth_key(key, children[i + 1], i - (division + 1));
                    }
                    drop(builder2);

                    let index_key_vec = index_key.to_vec();

                    // Free the original page, since we've replaced it
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: If the page is uncommitted, no other transactions can have references to it,
                    // and we just dropped ours on the line above
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }
                    (
                        new_page.get_page_number(),
                        Some((index_key_vec, new_page2.get_page_number())),
                        guard,
                    )
                }
            } else {
                #[allow(clippy::collapsible_else_if)]
                if page1 == child_page {
                    // NO-OP. One of our descendants is uncommitted, so there was no change
                    (page.get_page_number(), None, guard)
                } else if manager.uncommitted(page.get_page_number()) {
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: Since the page is uncommitted, no other transactions could have it open
                    // and we just dropped our reference to it, on the line above
                    let mut mutpage = manager.get_page_mut(page_number);
                    let mut mutator = InternalMutator::new(&mut mutpage);
                    mutator.write_child_page(child_index, page1);
                    (mutpage.get_page_number(), None, guard)
                } else {
                    let mut new_page = manager.allocate(InternalBuilder::required_bytes(
                        accessor.count_children() - 1,
                        accessor.total_key_length(),
                    ))?;
                    let mut builder =
                        InternalBuilder::new(&mut new_page, accessor.count_children() - 1);
                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        accessor.count_children(),
                        &mut builder,
                        child_index,
                        page1,
                        None,
                    );
                    drop(builder);

                    // Free the original page, since we've replaced it
                    let page_number = page.get_page_number();
                    drop(page);
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }
                    (new_page.get_page_number(), None, guard)
                }
            }
        }
        _ => unreachable!(),
    })
}

// Returns the value for the queried key, if present
pub(crate) fn find_key<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
    page: PageImpl<'a>,
    query: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<<<V as RedbValue>::View as WithLifetime<'a>>::Out> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let entry_index = accessor.find_key::<K>(query)?;
            let (start, end) = accessor.value_range(entry_index).unwrap();
            Some(V::from_bytes(&page.into_memory()[start..end]))
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (_, child_page) = accessor.child_for_key::<K>(query);
            return find_key::<K, V>(manager.get_page(child_page), query, manager);
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
        let height = db.stats().unwrap().tree_height();
        assert!(
            height <= expected,
            "height={} expected={}",
            height,
            expected
        );

        let reduce_to = BTREE_ORDER / 2 - num_internal_entries;

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(X).unwrap();
            for i in 0..(elements - reduce_to) {
                table.remove(&i.to_le_bytes()).unwrap();
            }
        }
        txn.commit().unwrap();

        let expected = expected_height(reduce_to + num_internal_entries);
        let height = db.stats().unwrap().tree_height();
        assert!(
            height <= expected,
            "height={} expected={}",
            height,
            expected
        );
    }
}
