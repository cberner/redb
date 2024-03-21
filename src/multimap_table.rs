use crate::db::TransactionGuard;
use crate::multimap_table::DynamicCollectionType::{Inline, SubtreeV2};
use crate::sealed::Sealed;
use crate::table::{ReadableTableMetadata, TableStats};
use crate::tree_store::{
    btree_stats, AllPageNumbersBtreeIter, BranchAccessor, Btree, BtreeHeader, BtreeMut,
    BtreeRangeIter, BtreeStats, CachePriority, Checksum, LeafAccessor, LeafMutator, Page, PageHint,
    PageNumber, RawBtree, RawLeafBuilder, TransactionalMemory, UntypedBtreeMut, BRANCH, LEAF,
    MAX_VALUE_LENGTH,
};
use crate::types::{Key, TypeName, Value};
use crate::{AccessGuard, MultimapTableHandle, Result, StorageError, WriteTransaction};
use std::borrow::Borrow;
use std::cmp::max;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::mem;
use std::mem::size_of;
use std::ops::{RangeBounds, RangeFull};
use std::sync::{Arc, Mutex};

pub(crate) fn multimap_btree_stats(
    root: Option<PageNumber>,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<BtreeStats> {
    if let Some(root) = root {
        multimap_stats_helper(root, mem, fixed_key_size, fixed_value_size)
    } else {
        Ok(BtreeStats {
            tree_height: 0,
            leaf_pages: 0,
            branch_pages: 0,
            stored_leaf_bytes: 0,
            metadata_bytes: 0,
            fragmented_bytes: 0,
        })
    }
}

fn multimap_stats_helper(
    page_number: PageNumber,
    mem: &TransactionalMemory,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Result<BtreeStats> {
    let page = mem.get_page(page_number)?;
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(
                page.memory(),
                fixed_key_size,
                DynamicCollection::<()>::fixed_width_with(fixed_value_size),
            );
            let mut leaf_bytes = 0u64;
            let mut is_branch = false;
            for i in 0..accessor.num_pairs() {
                let entry = accessor.entry(i).unwrap();
                let collection: &UntypedDynamicCollection =
                    UntypedDynamicCollection::new(entry.value());
                match collection.collection_type() {
                    Inline => {
                        let inline_accessor = LeafAccessor::new(
                            collection.as_inline(),
                            fixed_value_size,
                            <() as Value>::fixed_width(),
                        );
                        leaf_bytes +=
                            inline_accessor.length_of_pairs(0, inline_accessor.num_pairs()) as u64;
                    }
                    SubtreeV2 => {
                        is_branch = true;
                    }
                }
            }
            let mut overhead_bytes = (accessor.total_length() as u64) - leaf_bytes;
            let mut fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            let mut max_child_height = 0;
            let (mut leaf_pages, mut branch_pages) = if is_branch { (0, 1) } else { (1, 0) };

            for i in 0..accessor.num_pairs() {
                let entry = accessor.entry(i).unwrap();
                let collection: &UntypedDynamicCollection =
                    UntypedDynamicCollection::new(entry.value());
                match collection.collection_type() {
                    Inline => {
                        // data is inline, so it was already counted above
                    }
                    SubtreeV2 => {
                        // this is a sub-tree, so traverse it
                        let stats = btree_stats(
                            Some(collection.as_subtree().root),
                            mem,
                            fixed_value_size,
                            <() as Value>::fixed_width(),
                        )?;
                        max_child_height = max(max_child_height, stats.tree_height);
                        branch_pages += stats.branch_pages;
                        leaf_pages += stats.leaf_pages;
                        fragmented_bytes += stats.fragmented_bytes;
                        overhead_bytes += stats.metadata_bytes;
                        leaf_bytes += stats.stored_leaf_bytes;
                    }
                }
            }

            Ok(BtreeStats {
                tree_height: max_child_height + 1,
                leaf_pages,
                branch_pages,
                stored_leaf_bytes: leaf_bytes,
                metadata_bytes: overhead_bytes,
                fragmented_bytes,
            })
        }
        BRANCH => {
            let accessor = BranchAccessor::new(&page, fixed_key_size);
            let mut max_child_height = 0;
            let mut leaf_pages = 0;
            let mut branch_pages = 1;
            let mut stored_leaf_bytes = 0;
            let mut metadata_bytes = accessor.total_length() as u64;
            let mut fragmented_bytes = (page.memory().len() - accessor.total_length()) as u64;
            for i in 0..accessor.count_children() {
                if let Some(child) = accessor.child_page(i) {
                    let stats =
                        multimap_stats_helper(child, mem, fixed_key_size, fixed_value_size)?;
                    max_child_height = max(max_child_height, stats.tree_height);
                    leaf_pages += stats.leaf_pages;
                    branch_pages += stats.branch_pages;
                    stored_leaf_bytes += stats.stored_leaf_bytes;
                    metadata_bytes += stats.metadata_bytes;
                    fragmented_bytes += stats.fragmented_bytes;
                }
            }

            Ok(BtreeStats {
                tree_height: max_child_height + 1,
                leaf_pages,
                branch_pages,
                stored_leaf_bytes,
                metadata_bytes,
                fragmented_bytes,
            })
        }
        _ => unreachable!(),
    }
}

// Verify all the checksums in the tree, including any Dynamic collection subtrees
pub(crate) fn verify_tree_and_subtree_checksums(
    root: Option<BtreeHeader>,
    key_size: Option<usize>,
    value_size: Option<usize>,
    mem: Arc<TransactionalMemory>,
) -> Result<bool> {
    if let Some(header) = root {
        if !RawBtree::new(
            Some(header),
            key_size,
            DynamicCollection::<()>::fixed_width_with(value_size),
            mem.clone(),
        )
        .verify_checksum()?
        {
            return Ok(false);
        }

        let table_pages_iter = AllPageNumbersBtreeIter::new(
            header.root,
            key_size,
            DynamicCollection::<()>::fixed_width_with(value_size),
            mem.clone(),
        )?;
        for table_page in table_pages_iter {
            let page = mem.get_page(table_page?)?;
            let subtree_roots = parse_subtree_roots(&page, key_size, value_size);
            for header in subtree_roots {
                if !RawBtree::new(Some(header), value_size, <()>::fixed_width(), mem.clone())
                    .verify_checksum()?
                {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

// Finalize all the checksums in the tree, including any Dynamic collection subtrees
// Returns the root checksum
pub(crate) fn finalize_tree_and_subtree_checksums(
    root: Option<BtreeHeader>,
    key_size: Option<usize>,
    value_size: Option<usize>,
    mem: Arc<TransactionalMemory>,
) -> Result<Option<BtreeHeader>> {
    let freed_pages = Arc::new(Mutex::new(vec![]));
    let mut tree = UntypedBtreeMut::new(
        root,
        mem.clone(),
        freed_pages.clone(),
        key_size,
        DynamicCollection::<()>::fixed_width_with(value_size),
    );
    tree.dirty_leaf_visitor(|mut leaf_page| {
        let mut sub_root_updates = vec![];
        let accessor = LeafAccessor::new(
            leaf_page.memory(),
            key_size,
            DynamicCollection::<()>::fixed_width_with(value_size),
        );
        for i in 0..accessor.num_pairs() {
            let entry = accessor.entry(i).unwrap();
            let collection = <&DynamicCollection<()>>::from_bytes(entry.value());
            if matches!(collection.collection_type(), SubtreeV2) {
                let sub_root = collection.as_subtree();
                if mem.uncommitted(sub_root.root) {
                    let mut subtree = UntypedBtreeMut::new(
                        Some(sub_root),
                        mem.clone(),
                        freed_pages.clone(),
                        value_size,
                        <()>::fixed_width(),
                    );
                    subtree.finalize_dirty_checksums()?;
                    sub_root_updates.push((i, entry.key().to_vec(), subtree.get_root().unwrap()));
                }
            }
        }
        drop(accessor);
        // TODO: maybe there's a better abstraction, so that we don't need to call into this low-level method?
        let mut mutator = LeafMutator::new(
            &mut leaf_page,
            key_size,
            DynamicCollection::<()>::fixed_width_with(value_size),
        );
        for (i, key, sub_root) in sub_root_updates {
            let collection = DynamicCollection::<()>::make_subtree_data(sub_root);
            mutator.insert(i, true, &key, &collection);
        }

        Ok(())
    })?;

    tree.finalize_dirty_checksums()?;
    // No pages should have been freed by this operation
    assert!(freed_pages.lock().unwrap().is_empty());
    Ok(tree.get_root())
}

pub(crate) fn parse_subtree_roots<T: Page>(
    page: &T,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Vec<BtreeHeader> {
    match page.memory()[0] {
        BRANCH => {
            vec![]
        }
        LEAF => {
            let mut result = vec![];
            let accessor = LeafAccessor::new(
                page.memory(),
                fixed_key_size,
                DynamicCollection::<()>::fixed_width_with(fixed_value_size),
            );
            for i in 0..accessor.num_pairs() {
                let entry = accessor.entry(i).unwrap();
                let collection = <&DynamicCollection<()>>::from_bytes(entry.value());
                if matches!(collection.collection_type(), SubtreeV2) {
                    result.push(collection.as_subtree());
                }
            }

            result
        }
        _ => unreachable!(),
    }
}

pub(crate) struct LeafKeyIter<'a, V: Key + 'static> {
    inline_collection: AccessGuard<'a, &'static DynamicCollection<V>>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    start_entry: isize, // inclusive
    end_entry: isize,   // inclusive
}

impl<'a, V: Key> LeafKeyIter<'a, V> {
    fn new(
        data: AccessGuard<'a, &'static DynamicCollection<V>>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
    ) -> Self {
        let accessor =
            LeafAccessor::new(data.value().as_inline(), fixed_key_size, fixed_value_size);
        let end_entry = isize::try_from(accessor.num_pairs()).unwrap() - 1;
        Self {
            inline_collection: data,
            fixed_key_size,
            fixed_value_size,
            start_entry: 0,
            end_entry,
        }
    }

    fn next_key(&mut self) -> Option<&[u8]> {
        if self.end_entry < self.start_entry {
            return None;
        }
        let accessor = LeafAccessor::new(
            self.inline_collection.value().as_inline(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        self.start_entry += 1;
        accessor
            .entry((self.start_entry - 1).try_into().unwrap())
            .map(|e| e.key())
    }

    fn next_key_back(&mut self) -> Option<&[u8]> {
        if self.end_entry < self.start_entry {
            return None;
        }
        let accessor = LeafAccessor::new(
            self.inline_collection.value().as_inline(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        self.end_entry -= 1;
        accessor
            .entry((self.end_entry + 1).try_into().unwrap())
            .map(|e| e.key())
    }
}

enum DynamicCollectionType {
    Inline,
    // Was used in file format version 1
    // Subtree,
    SubtreeV2,
}

impl From<u8> for DynamicCollectionType {
    fn from(value: u8) -> Self {
        match value {
            LEAF => Inline,
            // 2 => Subtree,
            3 => SubtreeV2,
            _ => unreachable!(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<u8> for DynamicCollectionType {
    fn into(self) -> u8 {
        match self {
            // Reuse the LEAF type id, so that we can cast this directly into the format used by
            // LeafAccessor
            Inline => LEAF,
            // Subtree => 2,
            SubtreeV2 => 3,
        }
    }
}

/// Layout:
/// type (1 byte):
/// * 1 = inline data
/// * 2 = sub tree
///
/// (when type = 1) data (n bytes): inlined leaf node
///
/// (when type = 2) root (8 bytes): sub tree root page number
/// (when type = 2) checksum (16 bytes): sub tree checksum
///
/// NOTE: Even though the [PhantomData] is zero-sized, the inner data DST must be placed last.
/// See [Exotically Sized Types](https://doc.rust-lang.org/nomicon/exotic-sizes.html#dynamically-sized-types-dsts)
/// section of the Rustonomicon for more details.
#[repr(transparent)]
pub(crate) struct DynamicCollection<V: Key> {
    _value_type: PhantomData<V>,
    data: [u8],
}

impl<V: Key> std::fmt::Debug for DynamicCollection<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicCollection")
            .field("data", &&self.data)
            .finish()
    }
}

impl<V: Key> Value for &DynamicCollection<V> {
    type SelfType<'a> = &'a DynamicCollection<V>
    where
        Self: 'a;
    type AsBytes<'a> = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a DynamicCollection<V>
    where
        Self: 'a,
    {
        DynamicCollection::new(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'a,
        Self: 'b,
    {
        &value.data
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::DynamicCollection")
    }
}

impl<V: Key> DynamicCollection<V> {
    fn new(data: &[u8]) -> &Self {
        unsafe { mem::transmute(data) }
    }

    fn collection_type(&self) -> DynamicCollectionType {
        DynamicCollectionType::from(self.data[0])
    }

    fn as_inline(&self) -> &[u8] {
        debug_assert!(matches!(self.collection_type(), Inline));
        &self.data[1..]
    }

    fn as_subtree(&self) -> BtreeHeader {
        assert!(matches!(self.collection_type(), SubtreeV2));
        BtreeHeader::from_le_bytes(
            self.data[1..(1 + BtreeHeader::serialized_size())]
                .try_into()
                .unwrap(),
        )
    }

    fn get_num_values(&self) -> u64 {
        match self.collection_type() {
            Inline => {
                let leaf_data = self.as_inline();
                let accessor =
                    LeafAccessor::new(leaf_data, V::fixed_width(), <() as Value>::fixed_width());
                accessor.num_pairs() as u64
            }
            SubtreeV2 => {
                let offset = 1 + PageNumber::serialized_size() + size_of::<Checksum>();
                u64::from_le_bytes(
                    self.data[offset..(offset + size_of::<u64>())]
                        .try_into()
                        .unwrap(),
                )
            }
        }
    }

    fn make_inline_data(data: &[u8]) -> Vec<u8> {
        let mut result = vec![Inline.into()];
        result.extend_from_slice(data);

        result
    }

    fn make_subtree_data(header: BtreeHeader) -> Vec<u8> {
        let mut result = vec![SubtreeV2.into()];
        result.extend_from_slice(&header.to_le_bytes());
        result
    }

    pub(crate) fn fixed_width_with(_value_width: Option<usize>) -> Option<usize> {
        None
    }
}

impl<V: Key> DynamicCollection<V> {
    fn iter<'a>(
        collection: AccessGuard<'a, &'static DynamicCollection<V>>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<MultimapValue<'a, V>> {
        Ok(match collection.value().collection_type() {
            Inline => {
                let leaf_iter =
                    LeafKeyIter::new(collection, V::fixed_width(), <() as Value>::fixed_width());
                MultimapValue::new_inline(leaf_iter, guard)
            }
            SubtreeV2 => {
                let root = collection.value().as_subtree().root;
                MultimapValue::new_subtree(
                    BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(&(..), Some(root), mem)?,
                    collection.value().get_num_values(),
                    guard,
                )
            }
        })
    }

    fn iter_free_on_drop<'a>(
        collection: AccessGuard<'a, &'static DynamicCollection<V>>,
        pages: Vec<PageNumber>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<MultimapValue<'a, V>> {
        let num_values = collection.value().get_num_values();
        Ok(match collection.value().collection_type() {
            Inline => {
                let leaf_iter =
                    LeafKeyIter::new(collection, V::fixed_width(), <() as Value>::fixed_width());
                MultimapValue::new_inline(leaf_iter, guard)
            }
            SubtreeV2 => {
                let root = collection.value().as_subtree().root;
                let inner = BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                    &(..),
                    Some(root),
                    mem.clone(),
                )?;
                MultimapValue::new_subtree_free_on_drop(
                    inner,
                    num_values,
                    freed_pages,
                    pages,
                    guard,
                    mem,
                )
            }
        })
    }
}

#[repr(transparent)]
pub(crate) struct UntypedDynamicCollection {
    data: [u8],
}

impl UntypedDynamicCollection {
    fn new(data: &[u8]) -> &Self {
        unsafe { mem::transmute(data) }
    }

    fn collection_type(&self) -> DynamicCollectionType {
        DynamicCollectionType::from(self.data[0])
    }

    fn as_inline(&self) -> &[u8] {
        debug_assert!(matches!(self.collection_type(), Inline));
        &self.data[1..]
    }

    fn as_subtree(&self) -> BtreeHeader {
        assert!(matches!(self.collection_type(), SubtreeV2));
        BtreeHeader::from_le_bytes(
            self.data[1..(1 + BtreeHeader::serialized_size())]
                .try_into()
                .unwrap(),
        )
    }
}

enum ValueIterState<'a, V: Key + 'static> {
    Subtree(BtreeRangeIter<V, ()>),
    InlineLeaf(LeafKeyIter<'a, V>),
}

pub struct MultimapValue<'a, V: Key + 'static> {
    inner: Option<ValueIterState<'a, V>>,
    remaining: u64,
    freed_pages: Option<Arc<Mutex<Vec<PageNumber>>>>,
    free_on_drop: Vec<PageNumber>,
    _transaction_guard: Arc<TransactionGuard>,
    mem: Option<Arc<TransactionalMemory>>,
    _value_type: PhantomData<V>,
}

impl<'a, V: Key + 'static> MultimapValue<'a, V> {
    fn new_subtree(
        inner: BtreeRangeIter<V, ()>,
        num_values: u64,
        guard: Arc<TransactionGuard>,
    ) -> Self {
        Self {
            inner: Some(ValueIterState::Subtree(inner)),
            remaining: num_values,
            freed_pages: None,
            free_on_drop: vec![],
            _transaction_guard: guard,
            mem: None,
            _value_type: Default::default(),
        }
    }

    fn new_subtree_free_on_drop(
        inner: BtreeRangeIter<V, ()>,
        num_values: u64,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        pages: Vec<PageNumber>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            inner: Some(ValueIterState::Subtree(inner)),
            remaining: num_values,
            freed_pages: Some(freed_pages),
            free_on_drop: pages,
            _transaction_guard: guard,
            mem: Some(mem),
            _value_type: Default::default(),
        }
    }

    fn new_inline(inner: LeafKeyIter<'a, V>, guard: Arc<TransactionGuard>) -> Self {
        let remaining = inner.inline_collection.value().get_num_values();
        Self {
            inner: Some(ValueIterState::InlineLeaf(inner)),
            remaining,
            freed_pages: None,
            free_on_drop: vec![],
            _transaction_guard: guard,
            mem: None,
            _value_type: Default::default(),
        }
    }

    /// Returns the number of times this iterator will return `Some(Ok(_))`
    ///
    /// Note that `Some` may be returned from `next()` more than `len()` times if `Some(Err(_))` is returned
    pub fn len(&self) -> u64 {
        self.remaining
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, V: Key + 'static> Iterator for MultimapValue<'a, V> {
    type Item = Result<AccessGuard<'a, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: optimize out this copy
        let bytes = match self.inner.as_mut().unwrap() {
            ValueIterState::Subtree(ref mut iter) => match iter.next()? {
                Ok(e) => e.key_data(),
                Err(err) => {
                    return Some(Err(err));
                }
            },
            ValueIterState::InlineLeaf(ref mut iter) => iter.next_key()?.to_vec(),
        };
        self.remaining -= 1;
        Some(Ok(AccessGuard::with_owned_value(bytes)))
    }
}

impl<'a, V: Key + 'static> DoubleEndedIterator for MultimapValue<'a, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // TODO: optimize out this copy
        let bytes = match self.inner.as_mut().unwrap() {
            ValueIterState::Subtree(ref mut iter) => match iter.next_back()? {
                Ok(e) => e.key_data(),
                Err(err) => {
                    return Some(Err(err));
                }
            },
            ValueIterState::InlineLeaf(ref mut iter) => iter.next_key_back()?.to_vec(),
        };
        Some(Ok(AccessGuard::with_owned_value(bytes)))
    }
}

impl<'a, V: Key + 'static> Drop for MultimapValue<'a, V> {
    fn drop(&mut self) {
        // Drop our references to the pages that are about to be freed
        drop(mem::take(&mut self.inner));
        if !self.free_on_drop.is_empty() {
            let mut freed_pages = self.freed_pages.as_ref().unwrap().lock().unwrap();
            for page in self.free_on_drop.iter() {
                if !self.mem.as_ref().unwrap().free_if_uncommitted(*page) {
                    freed_pages.push(*page);
                }
            }
        }
    }
}

pub struct MultimapRange<'a, K: Key + 'static, V: Key + 'static> {
    inner: BtreeRangeIter<K, &'static DynamicCollection<V>>,
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, K: Key + 'static, V: Key + 'static> MultimapRange<'a, K, V> {
    fn new(
        inner: BtreeRangeIter<K, &'static DynamicCollection<V>>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            inner,
            mem,
            transaction_guard: guard,
            _key_type: Default::default(),
            _value_type: Default::default(),
            _lifetime: Default::default(),
        }
    }
}

impl<'a, K: Key + 'static, V: Key + 'static> Iterator for MultimapRange<'a, K, V> {
    type Item = Result<(AccessGuard<'a, K>, MultimapValue<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(entry) => {
                let key = AccessGuard::with_owned_value(entry.key_data());
                let (page, _, value_range) = entry.into_raw();
                let collection = AccessGuard::with_page(page, value_range);
                Some(
                    DynamicCollection::iter(
                        collection,
                        self.transaction_guard.clone(),
                        self.mem.clone(),
                    )
                    .map(|iter| (key, iter)),
                )
            }
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a, K: Key + 'static, V: Key + 'static> DoubleEndedIterator for MultimapRange<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.inner.next_back()? {
            Ok(entry) => {
                let key = AccessGuard::with_owned_value(entry.key_data());
                let (page, _, value_range) = entry.into_raw();
                let collection = AccessGuard::with_page(page, value_range);
                Some(
                    DynamicCollection::iter(
                        collection,
                        self.transaction_guard.clone(),
                        self.mem.clone(),
                    )
                    .map(|iter| (key, iter)),
                )
            }
            Err(err) => Some(Err(err)),
        }
    }
}

/// A multimap table
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
pub struct MultimapTable<'txn, K: Key + 'static, V: Key + 'static> {
    name: String,
    num_values: u64,
    transaction: &'txn WriteTransaction,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    tree: BtreeMut<'txn, K, &'static DynamicCollection<V>>,
    mem: Arc<TransactionalMemory>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Key + 'static> MultimapTableHandle for MultimapTable<'_, K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<'txn, K: Key + 'static, V: Key + 'static> MultimapTable<'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        num_values: u64,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        mem: Arc<TransactionalMemory>,
        transaction: &'txn WriteTransaction,
    ) -> MultimapTable<'txn, K, V> {
        MultimapTable {
            name: name.to_string(),
            num_values,
            transaction,
            freed_pages: freed_pages.clone(),
            tree: BtreeMut::new(
                table_root,
                transaction.transaction_guard(),
                mem.clone(),
                freed_pages,
            ),
            mem,
            _value_type: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.tree.print_debug(include_values)
    }

    /// Add the given value to the mapping of the key
    ///
    /// Returns `true` if the key-value pair was present
    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<bool> {
        let value_bytes = V::as_bytes(value.borrow());
        let value_bytes_ref = value_bytes.as_ref();
        if value_bytes_ref.len() > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_bytes_ref.len()));
        }
        let key_bytes = K::as_bytes(key.borrow());
        if key_bytes.as_ref().len() > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_bytes.as_ref().len()));
        }
        let get_result = self.tree.get(key.borrow())?;
        let existed = if get_result.is_some() {
            #[allow(clippy::unnecessary_unwrap)]
            let guard = get_result.unwrap();
            let collection_type = guard.value().collection_type();
            match collection_type {
                Inline => {
                    let leaf_data = guard.value().as_inline();
                    let accessor = LeafAccessor::new(
                        leaf_data,
                        V::fixed_width(),
                        <() as Value>::fixed_width(),
                    );
                    let (position, found) = accessor.position::<V>(value_bytes_ref);
                    if found {
                        return Ok(true);
                    }

                    let num_pairs = accessor.num_pairs();
                    let new_pairs = num_pairs + 1;
                    let new_pair_bytes =
                        accessor.length_of_pairs(0, accessor.num_pairs()) + value_bytes_ref.len();
                    let new_key_bytes =
                        accessor.length_of_keys(0, accessor.num_pairs()) + value_bytes_ref.len();
                    let required_inline_bytes = RawLeafBuilder::required_bytes(
                        new_pairs,
                        new_pair_bytes,
                        V::fixed_width(),
                        <() as Value>::fixed_width(),
                    );

                    if required_inline_bytes < self.mem.get_page_size() / 2 {
                        let mut data = vec![0; required_inline_bytes];
                        let mut builder = RawLeafBuilder::new(
                            &mut data,
                            new_pairs,
                            V::fixed_width(),
                            <() as Value>::fixed_width(),
                            new_key_bytes,
                        );
                        for i in 0..accessor.num_pairs() {
                            if i == position {
                                builder
                                    .append(value_bytes_ref, <() as Value>::as_bytes(&()).as_ref());
                            }
                            let entry = accessor.entry(i).unwrap();
                            builder.append(entry.key(), entry.value());
                        }
                        if position == accessor.num_pairs() {
                            builder.append(value_bytes_ref, <() as Value>::as_bytes(&()).as_ref());
                        }
                        drop(builder);
                        drop(guard);
                        let inline_data = DynamicCollection::<V>::make_inline_data(&data);
                        self.tree
                            .insert(key.borrow(), &DynamicCollection::new(&inline_data))?;
                    } else {
                        // convert into a subtree
                        let mut page = self.mem.allocate(leaf_data.len(), CachePriority::Low)?;
                        page.memory_mut()[..leaf_data.len()].copy_from_slice(leaf_data);
                        let page_number = page.get_page_number();
                        drop(page);
                        drop(guard);

                        // Don't bother computing the checksum, since we're about to modify the tree
                        let mut subtree: BtreeMut<'_, V, ()> = BtreeMut::new(
                            Some(BtreeHeader::new(page_number, 0, num_pairs as u64)),
                            self.transaction.transaction_guard(),
                            self.mem.clone(),
                            self.freed_pages.clone(),
                        );
                        let existed = subtree.insert(value.borrow(), &())?.is_some();
                        assert_eq!(existed, found);
                        let subtree_data =
                            DynamicCollection::<V>::make_subtree_data(subtree.get_root().unwrap());
                        self.tree
                            .insert(key.borrow(), &DynamicCollection::new(&subtree_data))?;
                    }

                    found
                }
                SubtreeV2 => {
                    let mut subtree: BtreeMut<'_, V, ()> = BtreeMut::new(
                        Some(guard.value().as_subtree()),
                        self.transaction.transaction_guard(),
                        self.mem.clone(),
                        self.freed_pages.clone(),
                    );
                    drop(guard);
                    let existed = subtree.insert(value.borrow(), &())?.is_some();
                    let subtree_data =
                        DynamicCollection::<V>::make_subtree_data(subtree.get_root().unwrap());
                    self.tree
                        .insert(key.borrow(), &DynamicCollection::new(&subtree_data))?;

                    existed
                }
            }
        } else {
            drop(get_result);
            let required_inline_bytes = RawLeafBuilder::required_bytes(
                1,
                value_bytes_ref.len(),
                V::fixed_width(),
                <() as Value>::fixed_width(),
            );
            if required_inline_bytes < self.mem.get_page_size() / 2 {
                let mut data = vec![0; required_inline_bytes];
                let mut builder = RawLeafBuilder::new(
                    &mut data,
                    1,
                    V::fixed_width(),
                    <() as Value>::fixed_width(),
                    value_bytes_ref.len(),
                );
                builder.append(value_bytes_ref, <() as Value>::as_bytes(&()).as_ref());
                drop(builder);
                let inline_data = DynamicCollection::<V>::make_inline_data(&data);
                self.tree
                    .insert(key.borrow(), &DynamicCollection::new(&inline_data))?;
            } else {
                let mut subtree: BtreeMut<'_, V, ()> = BtreeMut::new(
                    None,
                    self.transaction.transaction_guard(),
                    self.mem.clone(),
                    self.freed_pages.clone(),
                );
                subtree.insert(value.borrow(), &())?;
                let subtree_data =
                    DynamicCollection::<V>::make_subtree_data(subtree.get_root().unwrap());
                self.tree
                    .insert(key.borrow(), &DynamicCollection::new(&subtree_data))?;
            }
            false
        };

        if !existed {
            self.num_values += 1;
        }

        Ok(existed)
    }

    /// Removes the given key-value pair
    ///
    /// Returns `true` if the key-value pair was present
    pub fn remove<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<bool> {
        let get_result = self.tree.get(key.borrow())?;
        if get_result.is_none() {
            return Ok(false);
        }
        let guard = get_result.unwrap();
        let v = guard.value();
        let existed = match v.collection_type() {
            Inline => {
                let leaf_data = v.as_inline();
                let accessor =
                    LeafAccessor::new(leaf_data, V::fixed_width(), <() as Value>::fixed_width());
                if let Some(position) = accessor.find_key::<V>(V::as_bytes(value.borrow()).as_ref())
                {
                    let old_num_pairs = accessor.num_pairs();
                    if old_num_pairs == 1 {
                        drop(guard);
                        self.tree.remove(key.borrow())?;
                    } else {
                        let old_pairs_len = accessor.length_of_pairs(0, old_num_pairs);
                        let removed_value_len = accessor.entry(position).unwrap().key().len();
                        let required = RawLeafBuilder::required_bytes(
                            old_num_pairs - 1,
                            old_pairs_len - removed_value_len,
                            V::fixed_width(),
                            <() as Value>::fixed_width(),
                        );
                        let mut new_data = vec![0; required];
                        let new_key_len =
                            accessor.length_of_keys(0, old_num_pairs) - removed_value_len;
                        let mut builder = RawLeafBuilder::new(
                            &mut new_data,
                            old_num_pairs - 1,
                            V::fixed_width(),
                            <() as Value>::fixed_width(),
                            new_key_len,
                        );
                        for i in 0..old_num_pairs {
                            if i != position {
                                let entry = accessor.entry(i).unwrap();
                                builder.append(entry.key(), entry.value());
                            }
                        }
                        drop(builder);
                        drop(guard);

                        let inline_data = DynamicCollection::<V>::make_inline_data(&new_data);
                        self.tree
                            .insert(key.borrow(), &DynamicCollection::new(&inline_data))?;
                    }
                    true
                } else {
                    drop(guard);
                    false
                }
            }
            SubtreeV2 => {
                let mut subtree: BtreeMut<V, ()> = BtreeMut::new(
                    Some(v.as_subtree()),
                    self.transaction.transaction_guard(),
                    self.mem.clone(),
                    self.freed_pages.clone(),
                );
                drop(guard);
                let existed = subtree.remove(value.borrow())?.is_some();

                if let Some(BtreeHeader {
                    root: new_root,
                    checksum: new_checksum,
                    length: new_length,
                }) = subtree.get_root()
                {
                    let page = self.mem.get_page(new_root)?;
                    match page.memory()[0] {
                        LEAF => {
                            let accessor = LeafAccessor::new(
                                page.memory(),
                                V::fixed_width(),
                                <() as Value>::fixed_width(),
                            );
                            let len = accessor.total_length();
                            if len < self.mem.get_page_size() / 2 {
                                let inline_data =
                                    DynamicCollection::<V>::make_inline_data(&page.memory()[..len]);
                                self.tree
                                    .insert(key.borrow(), &DynamicCollection::new(&inline_data))?;
                                drop(page);
                                if !self.mem.free_if_uncommitted(new_root) {
                                    (*self.freed_pages).lock().unwrap().push(new_root);
                                }
                            } else {
                                let subtree_data =
                                    DynamicCollection::<V>::make_subtree_data(BtreeHeader::new(
                                        new_root,
                                        new_checksum,
                                        accessor.num_pairs() as u64,
                                    ));
                                self.tree
                                    .insert(key.borrow(), &DynamicCollection::new(&subtree_data))?;
                            }
                        }
                        BRANCH => {
                            let subtree_data = DynamicCollection::<V>::make_subtree_data(
                                BtreeHeader::new(new_root, new_checksum, new_length),
                            );
                            self.tree
                                .insert(key.borrow(), &DynamicCollection::new(&subtree_data))?;
                        }
                        _ => unreachable!(),
                    }
                } else {
                    self.tree.remove(key.borrow())?;
                }

                existed
            }
        };

        if existed {
            self.num_values -= 1;
        }

        Ok(existed)
    }

    /// Removes all values for the given key
    ///
    /// Returns an iterator over the removed values. Values are in ascending order.
    pub fn remove_all<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<MultimapValue<V>> {
        let iter = if let Some(collection) = self.tree.remove(key.borrow())? {
            let mut pages = vec![];
            if matches!(
                collection.value().collection_type(),
                DynamicCollectionType::SubtreeV2
            ) {
                let root = collection.value().as_subtree().root;
                let all_pages = AllPageNumbersBtreeIter::new(
                    root,
                    V::fixed_width(),
                    <() as Value>::fixed_width(),
                    self.mem.clone(),
                )?;
                for page in all_pages {
                    pages.push(page?);
                }
            }

            self.num_values -= collection.value().get_num_values();

            DynamicCollection::iter_free_on_drop(
                collection,
                pages,
                self.freed_pages.clone(),
                self.transaction.transaction_guard(),
                self.mem.clone(),
            )?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(&(..), None, self.mem.clone())?,
                0,
                self.transaction.transaction_guard(),
            )
        };

        Ok(iter)
    }
}

impl<'txn, K: Key + 'static, V: Key + 'static> ReadableTableMetadata for MultimapTable<'txn, K, V> {
    fn stats(&self) -> Result<TableStats> {
        let tree_stats = multimap_btree_stats(
            self.tree.get_root().map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )?;

        Ok(TableStats {
            tree_height: tree_stats.tree_height,
            leaf_pages: tree_stats.leaf_pages,
            branch_pages: tree_stats.branch_pages,
            stored_leaf_bytes: tree_stats.stored_leaf_bytes,
            metadata_bytes: tree_stats.metadata_bytes,
            fragmented_bytes: tree_stats.fragmented_bytes,
        })
    }

    /// Returns the number of key-value pairs in the table
    fn len(&self) -> Result<u64> {
        Ok(self.num_values)
    }
}

impl<'txn, K: Key + 'static, V: Key + 'static> ReadableMultimapTable<K, V>
    for MultimapTable<'txn, K, V>
{
    /// Returns an iterator over all values for the given key. Values are in ascending order.
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<V>> {
        let guard = self.transaction.transaction_guard();
        let iter = if let Some(collection) = self.tree.get(key.borrow())? {
            DynamicCollection::iter(collection, guard, self.mem.clone())?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(&(..), None, self.mem.clone())?,
                0,
                guard,
            )
        };

        Ok(iter)
    }

    /// Returns a double-ended iterator over a range of elements in the table
    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<MultimapRange<K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let inner = self.tree.range(&range)?;
        Ok(MultimapRange::new(
            inner,
            self.transaction.transaction_guard(),
            self.mem.clone(),
        ))
    }
}

impl<K: Key + 'static, V: Key + 'static> Sealed for MultimapTable<'_, K, V> {}

impl<'txn, K: Key + 'static, V: Key + 'static> Drop for MultimapTable<'txn, K, V> {
    fn drop(&mut self) {
        self.transaction
            .close_table(&self.name, &self.tree, self.num_values);
    }
}

pub trait ReadableMultimapTable<K: Key + 'static, V: Key + 'static>: ReadableTableMetadata {
    /// Returns an iterator over all values for the given key. Values are in ascending order.
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<V>>;

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<MultimapRange<K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a;

    /// Returns an double-ended iterator over all elements in the table. Values are in ascending
    /// order.
    fn iter(&self) -> Result<MultimapRange<K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }
}

/// A read-only untyped multimap table
pub struct ReadOnlyUntypedMultimapTable {
    num_values: u64,
    tree: RawBtree,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    mem: Arc<TransactionalMemory>,
}

impl Sealed for ReadOnlyUntypedMultimapTable {}

impl ReadableTableMetadata for ReadOnlyUntypedMultimapTable {
    /// Retrieves information about storage usage for the table
    fn stats(&self) -> Result<TableStats> {
        let tree_stats = multimap_btree_stats(
            self.tree.get_root().map(|x| x.root),
            &self.mem,
            self.fixed_key_size,
            self.fixed_value_size,
        )?;

        Ok(TableStats {
            tree_height: tree_stats.tree_height,
            leaf_pages: tree_stats.leaf_pages,
            branch_pages: tree_stats.branch_pages,
            stored_leaf_bytes: tree_stats.stored_leaf_bytes,
            metadata_bytes: tree_stats.metadata_bytes,
            fragmented_bytes: tree_stats.fragmented_bytes,
        })
    }

    fn len(&self) -> Result<u64> {
        Ok(self.num_values)
    }
}

impl ReadOnlyUntypedMultimapTable {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        num_values: u64,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            num_values,
            tree: RawBtree::new(
                root,
                fixed_key_size,
                DynamicCollection::<()>::fixed_width_with(fixed_value_size),
                mem.clone(),
            ),
            fixed_key_size,
            fixed_value_size,
            mem,
        }
    }
}

/// A read-only multimap table
pub struct ReadOnlyMultimapTable<K: Key + 'static, V: Key + 'static> {
    tree: Btree<K, &'static DynamicCollection<V>>,
    num_values: u64,
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Key + 'static> ReadOnlyMultimapTable<K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        num_values: u64,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<ReadOnlyMultimapTable<K, V>> {
        Ok(ReadOnlyMultimapTable {
            tree: Btree::new(root, hint, guard.clone(), mem.clone())?,
            num_values,
            mem,
            transaction_guard: guard,
            _value_type: Default::default(),
        })
    }

    /// This method is like [`ReadableMultimapTable::get()`], but the iterator is reference counted and keeps the transaction
    /// alive until it is dropped.
    pub fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<'static, V>> {
        let iter = if let Some(collection) = self.tree.get(key.borrow())? {
            DynamicCollection::iter(collection, self.transaction_guard.clone(), self.mem.clone())?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(&(..), None, self.mem.clone())?,
                0,
                self.transaction_guard.clone(),
            )
        };

        Ok(iter)
    }

    /// This method is like [`ReadableMultimapTable::range()`], but the iterator is reference counted and keeps the transaction
    /// alive until it is dropped.
    pub fn range<'a, KR>(&self, range: impl RangeBounds<KR>) -> Result<MultimapRange<'static, K, V>>
    where
        KR: Borrow<K::SelfType<'a>>,
    {
        let inner = self.tree.range(&range)?;
        Ok(MultimapRange::new(
            inner,
            self.transaction_guard.clone(),
            self.mem.clone(),
        ))
    }
}

impl<K: Key + 'static, V: Key + 'static> ReadableTableMetadata for ReadOnlyMultimapTable<K, V> {
    fn stats(&self) -> Result<TableStats> {
        let tree_stats = multimap_btree_stats(
            self.tree.get_root().map(|x| x.root),
            &self.mem,
            K::fixed_width(),
            V::fixed_width(),
        )?;

        Ok(TableStats {
            tree_height: tree_stats.tree_height,
            leaf_pages: tree_stats.leaf_pages,
            branch_pages: tree_stats.branch_pages,
            stored_leaf_bytes: tree_stats.stored_leaf_bytes,
            metadata_bytes: tree_stats.metadata_bytes,
            fragmented_bytes: tree_stats.fragmented_bytes,
        })
    }

    fn len(&self) -> Result<u64> {
        Ok(self.num_values)
    }
}

impl<K: Key + 'static, V: Key + 'static> ReadableMultimapTable<K, V>
    for ReadOnlyMultimapTable<K, V>
{
    /// Returns an iterator over all values for the given key. Values are in ascending order.
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<V>> {
        let iter = if let Some(collection) = self.tree.get(key.borrow())? {
            DynamicCollection::iter(collection, self.transaction_guard.clone(), self.mem.clone())?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(&(..), None, self.mem.clone())?,
                0,
                self.transaction_guard.clone(),
            )
        };

        Ok(iter)
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<MultimapRange<K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let inner = self.tree.range(&range)?;
        Ok(MultimapRange::new(
            inner,
            self.transaction_guard.clone(),
            self.mem.clone(),
        ))
    }
}

impl<K: Key, V: Key> Sealed for ReadOnlyMultimapTable<K, V> {}
