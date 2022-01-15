use crate::tree_store::btree_base::{
    InternalAccessor, InternalBuilder, InternalMutator, LeafAccessor, LeafBuilder, BTREE_ORDER,
    INTERNAL, LEAF,
};
use crate::tree_store::btree_utils::DeletionResult::{PartialInternal, PartialLeaf, Subtree};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::tree_store::{AccessGuardMut, BtreeEntry};
use crate::types::RedbKey;
use crate::Error;
use std::cmp::{max, Ordering};

pub(in crate) fn tree_height<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => 1,
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut max_child_height = 0;
            for i in 0..BTREE_ORDER {
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

pub(in crate) fn print_node(page: &impl Page) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(page);
            eprint!(
                "Leaf[ (page={:?}), lt_key={:?}",
                page.get_page_number(),
                accessor.lesser().key()
            );
            if let Some(greater) = accessor.greater() {
                eprint!(" gt_key={:?}", greater.key());
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
            for i in 0..(BTREE_ORDER - 1) {
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

pub(in crate) fn node_children<'a>(
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
            for i in 0..BTREE_ORDER {
                if let Some(child) = accessor.child_page(i) {
                    children.push(manager.get_page(child));
                }
            }
            children
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn print_tree<'a>(page: PageImpl<'a>, manager: &'a TransactionalMemory) {
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

// Returns the new root, and a list of freed pages
pub(in crate) fn tree_delete<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(Option<PageNumber>, Vec<PageNumber>), Error> {
    let mut freed = vec![];
    let result = match tree_delete_helper::<K>(page, key, &mut freed, manager)? {
        DeletionResult::Subtree(page) => Some(page),
        DeletionResult::PartialLeaf(entries) => {
            assert!(entries.is_empty());
            None
        }
        DeletionResult::PartialInternal(pages) => {
            assert_eq!(pages.len(), 1);
            Some(pages[0])
        }
    };
    Ok((result, freed))
}

#[derive(Debug)]
enum DeletionResult {
    // A proper subtree
    Subtree(PageNumber),
    // A leaf subtree with too few entries
    PartialLeaf(Vec<(u64, Vec<u8>, Vec<u8>)>),
    // A index page subtree with too few children
    PartialInternal(Vec<PageNumber>),
}

// Must return the pages in order
fn split_leaf(
    leaf: PageNumber,
    partial: &[(u64, Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
    freed: &mut Vec<PageNumber>,
) -> Result<Option<(PageNumber, PageNumber)>, Error> {
    assert!(partial.is_empty());
    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    if let Some(greater) = accessor.greater() {
        let lesser = accessor.lesser();
        let page1 = make_single_leaf(lesser.key(), lesser.value(), manager)?;
        let page2 = make_single_leaf(greater.key(), greater.value(), manager)?;
        freed.push(page.get_page_number());
        Ok(Some((page1, page2)))
    } else {
        Ok(None)
    }
}

fn merge_leaf(
    leaf: PageNumber,
    partial: &[(u64, Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
) -> PageNumber {
    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    assert!(accessor.greater().is_none());
    assert!(partial.is_empty());
    leaf
}

// Splits the page, if necessary, to fit the additional pages in `partial`
// Returns the pages in order
fn split_index(
    index: PageNumber,
    partial: &[PageNumber],
    manager: &TransactionalMemory,
    freed: &mut Vec<PageNumber>,
) -> Result<Option<(PageNumber, PageNumber)>, Error> {
    let page = manager.get_page(index);
    let accessor = InternalAccessor::new(&page);

    if accessor.child_page(BTREE_ORDER - partial.len()).is_none() {
        return Ok(None);
    }

    let mut pages = vec![];
    pages.extend_from_slice(partial);
    for i in 0..BTREE_ORDER {
        if let Some(child) = accessor.child_page(i) {
            pages.push(child);
        }
    }

    pages.sort_by_key(|p| max_key(manager.get_page(*p), manager));

    let division = pages.len() / 2;

    let page1 = make_index_many_pages(&pages[0..division], manager)?;
    let page2 = make_index_many_pages(&pages[division..], manager)?;
    freed.push(page.get_page_number());

    Ok(Some((page1, page2)))
}

// Pages must be in sorted order
fn make_index_many_pages(
    children: &[PageNumber],
    manager: &TransactionalMemory,
) -> Result<PageNumber, Error> {
    let mut keys = vec![];
    let mut key_size = 0;
    for i in 1..children.len() {
        let key = max_key(manager.get_page(children[i - 1]), manager);
        key_size += key.len();
        keys.push(key);
    }
    let mut page = manager.allocate(InternalBuilder::required_bytes(key_size))?;
    let mut builder = InternalBuilder::new(&mut page);
    builder.write_first_page(children[0]);
    for i in 1..children.len() {
        let key = &keys[i - 1];
        builder.write_nth_key(key, children[i], i - 1);
    }
    Ok(page.get_page_number())
}

fn merge_index(
    index: PageNumber,
    partial: &[PageNumber],
    manager: &TransactionalMemory,
    freed: &mut Vec<PageNumber>,
) -> Result<PageNumber, Error> {
    let page = manager.get_page(index);
    let accessor = InternalAccessor::new(&page);
    assert!(accessor.child_page(BTREE_ORDER - partial.len()).is_none());

    let mut pages = vec![];
    pages.extend_from_slice(partial);
    for i in 0..BTREE_ORDER {
        if let Some(page_number) = accessor.child_page(i) {
            pages.push(page_number);
        }
    }

    pages.sort_by_key(|p| max_key(manager.get_page(*p), manager));
    assert!(pages.len() <= BTREE_ORDER);

    freed.push(page.get_page_number());

    make_index_many_pages(&pages, manager)
}

fn repair_children(
    children: Vec<DeletionResult>,
    manager: &TransactionalMemory,
    freed: &mut Vec<PageNumber>,
) -> Result<Vec<PageNumber>, Error> {
    if children.iter().all(|x| matches!(x, Subtree(_))) {
        let page_numbers: Vec<PageNumber> = children
            .iter()
            .map(|x| match x {
                Subtree(page_number) => *page_number,
                _ => unreachable!(),
            })
            .collect();
        Ok(page_numbers)
    } else if children.iter().any(|x| matches!(x, PartialLeaf(_))) {
        let mut result = vec![];
        let mut repaired = false;
        // For each whole subtree, try to merge it with a partial left to repair it, if one is neighboring
        for i in 0..children.len() {
            if let Subtree(handle) = &children[i] {
                if repaired {
                    result.push(*handle);
                    continue;
                }
                let offset = if i > 0 { i - 1 } else { i + 1 };
                if let Some(PartialLeaf(partials)) = children.get(offset) {
                    if let Some((page1, page2)) = split_leaf(*handle, partials, manager, freed)? {
                        result.push(page1);
                        result.push(page2);
                    } else {
                        result.push(merge_leaf(*handle, partials, manager));
                    }
                    repaired = true;
                } else {
                    // No adjacent partial
                    result.push(*handle);
                }
            }
        }
        Ok(result)
    } else if children.iter().any(|x| matches!(x, PartialInternal(_))) {
        let mut result = vec![];
        let mut repaired = false;
        // For each whole subtree, try to merge it with a partial left to repair it, if one is neighboring
        for i in 0..children.len() {
            if let Subtree(page_number) = &children[i] {
                if repaired {
                    result.push(*page_number);
                    continue;
                }
                let offset = if i > 0 { i - 1 } else { i + 1 };
                if let Some(PartialInternal(partials)) = children.get(offset) {
                    if let Some((page1, page2)) =
                        split_index(*page_number, partials, manager, freed)?
                    {
                        result.push(page1);
                        result.push(page2);
                    } else {
                        result.push(merge_index(*page_number, partials, manager, freed)?);
                    }
                    repaired = true;
                } else {
                    // No adjacent partial
                    result.push(*page_number);
                }
            }
        }
        Ok(result)
    } else {
        unreachable!()
    }
}

fn max_key(page: PageImpl, manager: &TransactionalMemory) -> Vec<u8> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            if let Some(greater) = accessor.greater() {
                greater.key().to_vec()
            } else {
                accessor.lesser().key().to_vec()
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            for i in (0..BTREE_ORDER).rev() {
                if let Some(child) = accessor.child_page(i) {
                    return max_key(manager.get_page(child), manager);
                }
            }
            unreachable!();
        }
        _ => unreachable!(),
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
#[allow(clippy::needless_return)]
fn tree_delete_helper<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    freed: &mut Vec<PageNumber>,
    manager: &'a TransactionalMemory,
) -> Result<DeletionResult, Error> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            #[allow(clippy::collapsible_else_if)]
            if let Some(greater) = accessor.greater() {
                if K::compare(accessor.lesser().key(), key).is_ne()
                    && K::compare(greater.key(), key).is_ne()
                {
                    // Not found
                    return Ok(Subtree(page.get_page_number()));
                }
                let (new_leaf_key, new_leaf_value) =
                    if K::compare(accessor.lesser().key(), key).is_eq() {
                        (greater.key(), greater.value())
                    } else {
                        (accessor.lesser().key(), accessor.lesser().value())
                    };

                freed.push(page.get_page_number());
                Ok(Subtree(make_single_leaf(
                    new_leaf_key,
                    new_leaf_value,
                    manager,
                )?))
            } else {
                if K::compare(accessor.lesser().key(), key).is_eq() {
                    // Deleted the entire left
                    freed.push(page.get_page_number());
                    Ok(PartialLeaf(vec![]))
                } else {
                    // Not found
                    Ok(Subtree(page.get_page_number()))
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let original_page_number = page.get_page_number();
            let mut children = vec![];
            let mut found = false;
            let mut last_valid_child = BTREE_ORDER - 1;
            for i in 0..(BTREE_ORDER - 1) {
                if let Some(index_key) = accessor.key(i) {
                    let child_page = accessor.child_page(i).unwrap();
                    if K::compare(key, index_key).is_le() && !found {
                        found = true;
                        let result = tree_delete_helper::<K>(
                            manager.get_page(child_page),
                            key,
                            freed,
                            manager,
                        )?;
                        // The key must not have been found, since the subtree didn't change
                        if let Subtree(page_number) = result {
                            if page_number == child_page {
                                return Ok(Subtree(original_page_number));
                            }
                        }
                        children.push(result);
                    } else {
                        children.push(Subtree(child_page));
                    }
                } else {
                    last_valid_child = i;
                    break;
                }
            }
            let last_page = accessor.child_page(last_valid_child).unwrap();
            if found {
                // Already found the insertion place, so just copy
                children.push(Subtree(last_page));
            } else {
                let result =
                    tree_delete_helper::<K>(manager.get_page(last_page), key, freed, manager)?;
                found = true;
                // The key must not have been found, since the subtree didn't change
                if let Subtree(page_number) = result {
                    if page_number == last_page {
                        return Ok(Subtree(original_page_number));
                    }
                }
                children.push(result);
            }
            assert!(found);
            assert!(children.len() > 1);
            freed.push(original_page_number);
            let children = repair_children(children, manager, freed)?;
            if children.len() == 1 {
                return Ok(PartialInternal(children));
            }

            Ok(Subtree(make_index_many_pages(&children, manager)?))
        }
        _ => unreachable!(),
    }
}

pub(in crate) fn make_mut_single_leaf<'a>(
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(PageNumber, AccessGuardMut<'a>), Error> {
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key, value]))?;
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(key, value);
    builder.write_greater(None);

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value.len());

    Ok((page_num, guard))
}

pub(in crate) fn make_mut_double_leaf_right<'a, K: RedbKey + ?Sized>(
    key1: &[u8],
    value1: &[u8],
    key2: &[u8],
    value2: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(PageNumber, AccessGuardMut<'a>), Error> {
    debug_assert!(K::compare(key1, key2).is_lt());
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key1, value1, key2, value2]))?;
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(key1, value1);
    builder.write_greater(Some((key2, value2)));

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_greater() + accessor.greater().unwrap().value_offset();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value2.len());

    Ok((page_num, guard))
}

pub(in crate) fn make_mut_double_leaf_left<'a, K: RedbKey + ?Sized>(
    key1: &[u8],
    value1: &[u8],
    key2: &[u8],
    value2: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(PageNumber, AccessGuardMut<'a>), Error> {
    debug_assert!(K::compare(key1, key2).is_lt());
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key1, value1, key2, value2]))?;
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(key1, value1);
    builder.write_greater(Some((key2, value2)));

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value1.len());

    Ok((page_num, guard))
}

pub(in crate) fn make_single_leaf<'a>(
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<PageNumber, Error> {
    let mut page = manager.allocate(LeafBuilder::required_bytes(&[key, value]))?;
    let mut builder = LeafBuilder::new(&mut page);
    builder.write_lesser(key, value);
    builder.write_greater(None);
    Ok(page.get_page_number())
}

pub(in crate) fn make_index(
    key: &[u8],
    lte_page: PageNumber,
    gt_page: PageNumber,
    manager: &TransactionalMemory,
) -> Result<PageNumber, Error> {
    let mut page = manager.allocate(InternalBuilder::required_bytes(key.len()))?;
    let mut builder = InternalBuilder::new(&mut page);
    builder.write_first_page(lte_page);
    builder.write_nth_key(key, gt_page, 0);
    Ok(page.get_page_number())
}

// Returns the page number of the sub-tree into which the key was inserted,
// and the guard which can be used to access the value, and a list of freed pages
pub(in crate) fn tree_insert<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    key: &[u8],
    value: &[u8],
    manager: &'a TransactionalMemory,
) -> Result<(PageNumber, AccessGuardMut<'a>, Vec<PageNumber>), Error> {
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
    patch_index: u8,
    patch_handle: PageNumber,
    patch_extension: Option<(&[u8], PageNumber)>,
) {
    let mut dest = 0;
    if patch_index as usize == start_child {
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
            let handle = if i == patch_index as usize {
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
fn tree_insert_helper<'a, K: RedbKey + ?Sized>(
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
            if let Some(entry) = accessor.greater() {
                match K::compare(entry.key(), key) {
                    Ordering::Less => {
                        // New entry goes in a new page to the right, so leave this page untouched
                        let left_page = page.get_page_number();

                        let (right_page, guard) = make_mut_single_leaf(key, value, manager)?;

                        (left_page, Some((entry.key().to_vec(), right_page)), guard)
                    }
                    Ordering::Equal => {
                        let (new_page, guard) = make_mut_double_leaf_right::<K>(
                            accessor.lesser().key(),
                            accessor.lesser().value(),
                            key,
                            value,
                            manager,
                        )?;

                        let page_number = page.get_page_number();
                        drop(page);
                        // Safety: If the page is uncommitted, no other transactions can have references to it,
                        // and we just dropped ours on the line above

                        // TODO: This call seems like it may be unsound. Another Table instance in the
                        // same transaction could have a reference to this page. We probably need to
                        // ensure this is only a single instance of each table
                        unsafe {
                            if !manager.free_if_uncommitted(page_number)? {
                                freed.push(page_number);
                            }
                        }

                        (new_page, None, guard)
                    }
                    Ordering::Greater => {
                        let right_key = entry.key();
                        let right_value = entry.value();

                        let left_key = accessor.lesser().key();
                        let left_value = accessor.lesser().value();

                        match K::compare(accessor.lesser().key(), key) {
                            Ordering::Less => {
                                let (left, guard) = make_mut_double_leaf_right::<K>(
                                    left_key, left_value, key, value, manager,
                                )?;
                                let right = make_single_leaf(right_key, right_value, manager)?;

                                let page_number = page.get_page_number();
                                drop(page);
                                // Safety: If the page is uncommitted, no other transactions can have references to it,
                                // and we just dropped ours on the line above
                                unsafe {
                                    if !manager.free_if_uncommitted(page_number)? {
                                        freed.push(page_number);
                                    }
                                }

                                (left, Some((key.to_vec(), right)), guard)
                            }
                            Ordering::Equal => {
                                let (new_page, guard) = make_mut_double_leaf_left::<K>(
                                    key,
                                    value,
                                    right_key,
                                    right_value,
                                    manager,
                                )?;

                                let page_number = page.get_page_number();
                                drop(page);
                                // Safety: If the page is uncommitted, no other transactions can have references to it,
                                // and we just dropped ours on the line above
                                unsafe {
                                    if !manager.free_if_uncommitted(page_number)? {
                                        freed.push(page_number);
                                    }
                                }

                                (new_page, None, guard)
                            }
                            Ordering::Greater => {
                                let (left, guard) = make_mut_double_leaf_left::<K>(
                                    key, value, left_key, left_value, manager,
                                )?;
                                let right = make_single_leaf(right_key, right_value, manager)?;

                                let left_key_vec = left_key.to_vec();

                                let page_number = page.get_page_number();
                                drop(page);
                                // Safety: If the page is uncommitted, no other transactions can have references to it,
                                // and we just dropped ours on the line above
                                unsafe {
                                    if !manager.free_if_uncommitted(page_number)? {
                                        freed.push(page_number);
                                    }
                                }

                                (left, Some((left_key_vec, right)), guard)
                            }
                        }
                    }
                }
            } else {
                let key1 = accessor.lesser().key();
                let key2 = key;
                let (new_page, guard) = match K::compare(key1, key2) {
                    Ordering::Less => make_mut_double_leaf_right::<K>(
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        key,
                        value,
                        manager,
                    )?,
                    Ordering::Equal => make_mut_single_leaf(key, value, manager)?,
                    Ordering::Greater => make_mut_double_leaf_left::<K>(
                        key,
                        value,
                        accessor.lesser().key(),
                        accessor.lesser().value(),
                        manager,
                    )?,
                };

                let page_number = page.get_page_number();
                drop(page);
                // Safety: If the page is uncommitted, no other transactions can have references to it,
                // and we just dropped ours on the line above
                unsafe {
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }
                }

                (new_page, None, guard)
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (child_index, child_page) = accessor.child_for_key::<K>(key);
            let (page1, more, guard) =
                tree_insert_helper::<K>(manager.get_page(child_page), key, value, freed, manager)?;

            if let Some((index_key2, page2)) = more {
                let new_children_count = 1 + accessor.count_children();

                if new_children_count <= BTREE_ORDER {
                    // Rewrite page since we're splitting a child
                    let mut new_page = manager.allocate(InternalBuilder::required_bytes(
                        accessor.total_key_length() + index_key2.len(),
                    ))?;
                    let mut builder = InternalBuilder::new(&mut new_page);

                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        BTREE_ORDER,
                        &mut builder,
                        child_index as u8,
                        page1,
                        Some((&index_key2, page2)),
                    );
                    // Free the original page, since we've replaced it
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: If the page is uncommitted, no other transactions can have references to it,
                    // and we just dropped ours on the line above
                    unsafe {
                        if !manager.free_if_uncommitted(page_number)? {
                            freed.push(page_number);
                        }
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
                    for i in 1..BTREE_ORDER {
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

                    let division = BTREE_ORDER / 2;

                    // Rewrite page since we're splitting a child
                    let key_size = index_keys[0..division].iter().map(|k| k.len()).sum();
                    let mut new_page =
                        manager.allocate(InternalBuilder::required_bytes(key_size))?;
                    let mut builder = InternalBuilder::new(&mut new_page);

                    builder.write_first_page(children[0]);
                    for i in 0..division {
                        let key = &index_keys[i];
                        builder.write_nth_key(key, children[i + 1], i);
                    }

                    let index_key = &index_keys[division];

                    let key_size = index_keys[(division + 1)..].iter().map(|k| k.len()).sum();
                    let mut new_page2 =
                        manager.allocate(InternalBuilder::required_bytes(key_size))?;
                    let mut builder2 = InternalBuilder::new(&mut new_page2);
                    builder2.write_first_page(children[division + 1]);
                    for i in (division + 1)..index_keys.len() {
                        let key = &index_keys[i];
                        builder2.write_nth_key(key, children[i + 1], i - (division + 1));
                    }

                    let index_key_vec = index_key.to_vec();

                    // Free the original page, since we've replaced it
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: If the page is uncommitted, no other transactions can have references to it,
                    // and we just dropped ours on the line above
                    unsafe {
                        if !manager.free_if_uncommitted(page_number)? {
                            freed.push(page_number);
                        }
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
                    let mut mutpage = unsafe { manager.get_page_mut(page_number) };
                    let mut mutator = InternalMutator::new(&mut mutpage);
                    mutator.write_child_page(child_index, page1);
                    (mutpage.get_page_number(), None, guard)
                } else {
                    let mut new_page = manager
                        .allocate(InternalBuilder::required_bytes(accessor.total_key_length()))?;
                    let mut builder = InternalBuilder::new(&mut new_page);
                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        BTREE_ORDER,
                        &mut builder,
                        child_index as u8,
                        page1,
                        None,
                    );

                    // Free the original page, since we've replaced it
                    let page_number = page.get_page_number();
                    drop(page);
                    // Safety: If the page is uncommitted, no other transactions can have references to it,
                    // and we just dropped ours on the line above
                    unsafe {
                        if !manager.free_if_uncommitted(page_number)? {
                            freed.push(page_number);
                        }
                    }
                    (new_page.get_page_number(), None, guard)
                }
            }
        }
        _ => unreachable!(),
    })
}

// Returns the (offset, len) of the value for the queried key, if present
pub(in crate) fn lookup_in_raw<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    query: &[u8],
    manager: &'a TransactionalMemory,
) -> Option<(PageImpl<'a>, usize, usize)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            match K::compare(query, accessor.lesser().key()) {
                Ordering::Less => None,
                Ordering::Equal => {
                    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();
                    let value_len = accessor.lesser().value().len();
                    Some((page, offset, value_len))
                }
                Ordering::Greater => {
                    if let Some(entry) = accessor.greater() {
                        if K::compare(entry.key(), query).is_eq() {
                            let offset = accessor.offset_of_greater() + entry.value_offset();
                            let value_len = entry.value().len();
                            Some((page, offset, value_len))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (_, child_page) = accessor.child_for_key::<K>(query);
            return lookup_in_raw::<K>(manager.get_page(child_page), query, manager);
        }
        _ => unreachable!(),
    }
}
#[cfg(test)]
mod test {
    use crate::tree_store::btree_utils::BTREE_ORDER;
    use crate::{Database, Table};
    use tempfile::NamedTempFile;

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

        let db = unsafe { Database::open(tmpfile.path(), 16 * 1024 * 1024).unwrap() };
        let txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();

        let elements = (BTREE_ORDER / 2).pow(2) as usize - num_internal_entries;

        for i in (0..elements).rev() {
            table.insert(&i.to_be_bytes(), b"").unwrap();
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
        let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();
        for i in 0..(elements - reduce_to) {
            table.remove(&i.to_be_bytes()).unwrap();
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
