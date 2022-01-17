use crate::tree_store::btree_base::{
    InternalAccessor, InternalBuilder, InternalMutator, LeafAccessor, LeafBuilder, BTREE_ORDER,
    INTERNAL, LEAF,
};
use crate::tree_store::btree_utils::DeletionResult::{PartialInternal, PartialLeaf, Subtree};
use crate::tree_store::page_store::{Page, PageImpl, PageNumber, TransactionalMemory};
use crate::tree_store::{AccessGuardMut, BtreeEntry};
use crate::types::RedbKey;
use crate::Error;
use std::cmp::max;

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
            if entries.is_empty() {
                None
            } else {
                let size: usize = entries.iter().map(|(k, v)| k.len() + v.len()).sum();
                let mut page =
                    manager.allocate(LeafBuilder::required_bytes(entries.len(), size))?;
                let mut builder = LeafBuilder::new(&mut page, entries.len());
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
    Ok((result, freed))
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
fn split_leaf<K: RedbKey + ?Sized>(
    leaf: PageNumber,
    partial: &[(Vec<u8>, Vec<u8>)],
    manager: &TransactionalMemory,
    freed: &mut Vec<PageNumber>,
) -> Result<(PageNumber, PageNumber), Error> {
    assert!(!partial.is_empty());
    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    assert!(partial.len() <= accessor.num_pairs());
    // TODO: clean up this duplicated code
    if K::compare(&partial[0].0, accessor.last_entry().key()).is_gt() {
        // partials go after
        let division = (accessor.num_pairs() + partial.len()) / 2;

        let new_size1 = accessor.length_of_pairs(0, division);
        let mut new_page = manager.allocate(LeafBuilder::required_bytes(division, new_size1))?;
        let mut builder = LeafBuilder::new(&mut new_page, division);
        for i in 0..division {
            let entry = accessor.entry(i).unwrap();
            builder.append(entry.key(), entry.value());
        }
        drop(builder);

        let new_size2 = accessor.length_of_pairs(division, accessor.num_pairs())
            + partial
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        let num_pairs2 = accessor.num_pairs() - division + partial.len();
        let mut new_page2 = manager.allocate(LeafBuilder::required_bytes(num_pairs2, new_size2))?;
        let mut builder = LeafBuilder::new(&mut new_page2, num_pairs2);
        for i in division..accessor.num_pairs() {
            let entry = accessor.entry(i).unwrap();
            builder.append(entry.key(), entry.value());
        }
        for (key, value) in partial {
            builder.append(key, value);
        }
        drop(builder);

        freed.push(page.get_page_number());
        Ok((new_page.get_page_number(), new_page2.get_page_number()))
    } else {
        assert!(K::compare(&partial.last().unwrap().0, accessor.first_entry().key()).is_lt());
        // partials go before
        let division = (accessor.num_pairs() + partial.len()) / 2 - partial.len();

        let new_size1 = accessor.length_of_pairs(0, division)
            + partial
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        let num_pairs1 = partial.len() + division;
        let mut new_page = manager.allocate(LeafBuilder::required_bytes(num_pairs1, new_size1))?;
        let mut builder = LeafBuilder::new(&mut new_page, num_pairs1);
        for (key, value) in partial {
            builder.append(key, value);
        }
        for i in 0..division {
            let entry = accessor.entry(i).unwrap();
            builder.append(entry.key(), entry.value());
        }
        drop(builder);

        let new_size2 = accessor.length_of_pairs(division, accessor.num_pairs());
        let num_pairs2 = accessor.num_pairs() - division;
        let mut new_page2 = manager.allocate(LeafBuilder::required_bytes(num_pairs2, new_size2))?;
        let mut builder = LeafBuilder::new(&mut new_page2, num_pairs2);
        for i in division..accessor.num_pairs() {
            let entry = accessor.entry(i).unwrap();
            builder.append(entry.key(), entry.value());
        }
        drop(builder);

        freed.push(page.get_page_number());
        Ok((new_page.get_page_number(), new_page2.get_page_number()))
    }
}

// partials must be in sorted order, and be disjoint from the pairs in the leaf
// returns None if the merged page would be too large
fn merge_leaf<K: RedbKey + ?Sized>(
    leaf: PageNumber,
    partial: &[(Vec<u8>, Vec<u8>)],
    freed: &mut Vec<PageNumber>,
    manager: &TransactionalMemory,
) -> Result<Option<PageNumber>, Error> {
    if partial.is_empty() {
        return Ok(Some(leaf));
    }

    let page = manager.get_page(leaf);
    let accessor = LeafAccessor::new(&page);
    if accessor.num_pairs() + partial.len() > BTREE_ORDER {
        return Ok(None);
    }

    let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
    let new_size = old_size
        + partial
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
    let mut new_page = manager.allocate(LeafBuilder::required_bytes(
        accessor.num_pairs() + partial.len(),
        new_size,
    ))?;
    let mut builder = LeafBuilder::new(&mut new_page, accessor.num_pairs() + partial.len());
    let mut i = 0;
    // TODO: this can probably be simplified. partials should all be strictly lesser or
    // greater than the entries in the page, since they're being merged from a neighboring leaf
    for (key, value) in partial {
        while i < accessor.num_pairs() && K::compare(accessor.entry(i).unwrap().key(), key).is_lt()
        {
            let entry = accessor.entry(i).unwrap();
            builder.append(entry.key(), entry.value());
            i += 1;
        }
        builder.append(key, value);
    }
    // Copy any remaining
    while i < accessor.num_pairs() {
        let entry = accessor.entry(i).unwrap();
        builder.append(entry.key(), entry.value());
        i += 1;
    }
    drop(builder);

    freed.push(leaf);

    Ok(Some(new_page.get_page_number()))
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

fn repair_children<K: RedbKey + ?Sized>(
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
                    if let Some(new_page) = merge_leaf::<K>(*handle, partials, freed, manager)? {
                        result.push(new_page);
                    } else {
                        let (page1, page2) = split_leaf::<K>(*handle, partials, manager, freed)?;
                        result.push(page1);
                        result.push(page2);
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
            accessor.last_entry().key().to_vec()
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
            let (position, found) = accessor.position::<K>(key);
            if !found {
                return Ok(Subtree(page.get_page_number()));
            }
            // TODO: trigger the merge logic when num_pairs() < BTREE_ORDER / 2
            if accessor.num_pairs() == 1 {
                // Deleted the entire left
                freed.push(page.get_page_number());
                Ok(PartialLeaf(vec![]))
            } else {
                freed.push(page.get_page_number());

                let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
                let new_size =
                    old_size - key.len() - accessor.entry(position).unwrap().value().len();
                let mut new_page = manager.allocate(LeafBuilder::required_bytes(
                    accessor.num_pairs() - 1,
                    new_size,
                ))?;
                let mut builder = LeafBuilder::new(&mut new_page, accessor.num_pairs() - 1);
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        continue;
                    }
                    let entry = accessor.entry(i).unwrap();
                    builder.append(entry.key(), entry.value());
                }
                drop(builder);
                Ok(Subtree(new_page.get_page_number()))
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
            let children = repair_children::<K>(children, manager, freed)?;
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
    let mut page = manager.allocate(LeafBuilder::required_bytes(1, key.len() + value.len()))?;
    let mut builder = LeafBuilder::new(&mut page, 1);
    builder.append(key, value);
    drop(builder);

    let accessor = LeafAccessor::new(&page);
    let offset = accessor.offset_of_first_value();

    let page_num = page.get_page_number();
    let guard = AccessGuardMut::new(page, offset, value.len());

    Ok((page_num, guard))
}

pub(in crate) fn make_index(
    key: &[u8],
    lte_page: PageNumber,
    gt_page: PageNumber,
    manager: &TransactionalMemory,
) -> Result<PageNumber, Error> {
    let mut page = manager.allocate(InternalBuilder::required_bytes(1, key.len()))?;
    let mut builder = InternalBuilder::new(&mut page, 1);
    builder.write_first_page(lte_page);
    builder.write_nth_key(key, gt_page, 0);
    drop(builder);
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
            let (position, found) = accessor.position::<K>(key);
            // TODO: also split if page is too big
            #[allow(clippy::collapsible_else_if)]
            if found {
                // Overwrite existing key
                let old_size = accessor.length_of_pairs(0, accessor.num_pairs());
                let new_size =
                    old_size + value.len() - accessor.entry(position).unwrap().value().len();
                let mut new_page = manager
                    .allocate(LeafBuilder::required_bytes(accessor.num_pairs(), new_size))?;
                let mut builder = LeafBuilder::new(&mut new_page, accessor.num_pairs());
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
                // TODO: This call seems like it may be unsound. Another Table instance in the
                // same transaction could have a reference to this page. We probably need to
                // ensure this is only a single instance of each table
                unsafe {
                    if !manager.free_if_uncommitted(page_number)? {
                        freed.push(page_number);
                    }
                }

                let new_page_number = new_page.get_page_number();
                let accessor = LeafAccessor::new(&new_page);
                let offset = accessor.offset_of_value(position).unwrap();
                let guard = AccessGuardMut::new(new_page, offset, value.len());

                (new_page_number, None, guard)
            } else {
                if accessor.num_pairs() >= BTREE_ORDER {
                    // split
                    let division = accessor.num_pairs() / 2;
                    let mut new_size1 = accessor.length_of_pairs(0, division);
                    let mut new_count1 = division;
                    let mut new_size2 = accessor.length_of_pairs(division, accessor.num_pairs());
                    let mut new_count2 = accessor.num_pairs() - division;
                    if position < division {
                        new_size1 += key.len() + value.len();
                        new_count1 += 1;
                    } else {
                        new_size2 += key.len() + value.len();
                        new_count2 += 1;
                    }

                    let mut new_page1 =
                        manager.allocate(LeafBuilder::required_bytes(new_count1, new_size1))?;
                    let mut builder = LeafBuilder::new(&mut new_page1, new_count1);
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
                    let mut builder = LeafBuilder::new(&mut new_page2, new_count2);
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
                    // TODO: This call seems like it may be unsound. Another Table instance in the
                    // same transaction could have a reference to this page. We probably need to
                    // ensure this is only a single instance of each table
                    unsafe {
                        if !manager.free_if_uncommitted(page_number)? {
                            freed.push(page_number);
                        }
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
                    let mut new_page = manager.allocate(LeafBuilder::required_bytes(
                        accessor.num_pairs() + 1,
                        new_size,
                    ))?;
                    let mut builder = LeafBuilder::new(&mut new_page, accessor.num_pairs() + 1);
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
                    // TODO: This call seems like it may be unsound. Another Table instance in the
                    // same transaction could have a reference to this page. We probably need to
                    // ensure this is only a single instance of each table
                    unsafe {
                        if !manager.free_if_uncommitted(page_number)? {
                            freed.push(page_number);
                        }
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
                        BTREE_ORDER,
                        &mut builder,
                        child_index as u8,
                        page1,
                        Some((&index_key2, page2)),
                    );
                    drop(builder);
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
                    let mut new_page = manager.allocate(InternalBuilder::required_bytes(
                        accessor.count_children() - 1,
                        accessor.total_key_length(),
                    ))?;
                    let mut builder =
                        InternalBuilder::new(&mut new_page, accessor.count_children() - 1);
                    copy_to_builder_and_patch(
                        &accessor,
                        0,
                        BTREE_ORDER,
                        &mut builder,
                        child_index as u8,
                        page1,
                        None,
                    );
                    drop(builder);

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
            let entry_index = accessor.find_key::<K>(query)?;
            let offset = accessor.offset_of_value(entry_index).unwrap();
            let value_len = accessor.entry(entry_index).unwrap().value().len();
            Some((page, offset, value_len))
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
