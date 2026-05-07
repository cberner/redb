use crate::db::TransactionGuard;
use crate::multimap_table::DynamicCollectionType::{Inline, SubtreeV2};
use crate::sealed::Sealed;
use crate::table::{ReadableTableMetadata, TableStats};
use crate::tree_store::{
    AllPageNumbersBtreeIter, BRANCH, Btree, BtreeHeader, BtreeMut, BtreeRangeIter,
    DynamicCollection, DynamicCollectionType, LEAF, LeafAccessor, MAX_PAIR_LENGTH,
    MAX_VALUE_LENGTH, Page, PageAllocator, PageHint, PageNumber, PageResolver, PageTrackerPolicy,
    RawBtree, RawLeafBuilder, multimap_btree_stats,
};
use crate::types::{Key, Value};
use crate::{AccessGuard, MultimapTableHandle, Result, StorageError, WriteTransaction};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Range, RangeBounds, RangeFull};
use std::sync::{Arc, Mutex};

pub(crate) struct LeafKeyIter<'a, V: Key + 'static> {
    // Kept alive so any Drop side-effects on `data` (e.g. `remove_on_drop`) still run.
    _inline_collection: AccessGuard<'a, &'static DynamicCollection<V>>,
    // Arc-backed view of the page holding the inline collection.
    page_data: Arc<[u8]>,
    // Byte range in `page_data` holding the inline leaf data for the collection.
    inline_range: Range<usize>,
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
        let (page_data, value_range) = data.arc_view();
        let inline_range = DynamicCollection::<V>::inline_range_within(value_range);
        let accessor = LeafAccessor::new(
            &page_data[inline_range.clone()],
            fixed_key_size,
            fixed_value_size,
        );
        let end_entry = isize::try_from(accessor.num_pairs()).unwrap() - 1;
        Self {
            _inline_collection: data,
            page_data,
            inline_range,
            fixed_key_size,
            fixed_value_size,
            start_entry: 0,
            end_entry,
        }
    }

    fn inline_bytes(&self) -> &[u8] {
        &self.page_data[self.inline_range.clone()]
    }

    fn num_values(&self) -> u64 {
        let accessor = LeafAccessor::new(
            self.inline_bytes(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        accessor.num_pairs() as u64
    }

    fn key_at(&self, n: usize) -> Option<AccessGuard<'static, V>> {
        let accessor = LeafAccessor::new(
            self.inline_bytes(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        let (key_range, _) = accessor.entry_ranges(n)?;
        let absolute =
            (self.inline_range.start + key_range.start)..(self.inline_range.start + key_range.end);
        Some(AccessGuard::with_arc_page(self.page_data.clone(), absolute))
    }

    fn next_key(&mut self) -> Option<AccessGuard<'static, V>> {
        if self.end_entry < self.start_entry {
            return None;
        }
        self.start_entry += 1;
        self.key_at((self.start_entry - 1).try_into().unwrap())
    }

    fn next_key_back(&mut self) -> Option<AccessGuard<'static, V>> {
        if self.end_entry < self.start_entry {
            return None;
        }
        self.end_entry -= 1;
        self.key_at((self.end_entry + 1).try_into().unwrap())
    }
}

enum ValueIterState<'a, V: Key + 'static> {
    Subtree(Box<BtreeRangeIter<V, ()>>),
    InlineLeaf(LeafKeyIter<'a, V>),
}

pub struct MultimapValue<'a, V: Key + 'static> {
    inner: Option<ValueIterState<'a, V>>,
    remaining: u64,
    freed_pages: Option<Arc<Mutex<Vec<PageNumber>>>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    free_on_drop: Vec<PageNumber>,
    _transaction_guard: Arc<TransactionGuard>,
    page_allocator: Option<PageAllocator>,
    _value_type: PhantomData<V>,
}

impl<'a, V: Key + 'static> MultimapValue<'a, V> {
    fn new_subtree(
        inner: BtreeRangeIter<V, ()>,
        num_values: u64,
        guard: Arc<TransactionGuard>,
    ) -> Self {
        Self {
            inner: Some(ValueIterState::Subtree(Box::new(inner))),
            remaining: num_values,
            freed_pages: None,
            allocated_pages: Arc::new(Mutex::new(PageTrackerPolicy::Closed)),
            free_on_drop: vec![],
            _transaction_guard: guard,
            page_allocator: None,
            _value_type: PhantomData,
        }
    }

    fn new_subtree_free_on_drop(
        inner: BtreeRangeIter<V, ()>,
        num_values: u64,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
        pages: Vec<PageNumber>,
        guard: Arc<TransactionGuard>,
        page_allocator: PageAllocator,
    ) -> Self {
        Self {
            inner: Some(ValueIterState::Subtree(Box::new(inner))),
            remaining: num_values,
            freed_pages: Some(freed_pages),
            free_on_drop: pages,
            allocated_pages,
            _transaction_guard: guard,
            page_allocator: Some(page_allocator),
            _value_type: PhantomData,
        }
    }

    fn new_inline(inner: LeafKeyIter<'a, V>, guard: Arc<TransactionGuard>) -> Self {
        let remaining = inner.num_values();
        Self {
            inner: Some(ValueIterState::InlineLeaf(inner)),
            remaining,
            freed_pages: None,
            allocated_pages: Arc::new(Mutex::new(PageTrackerPolicy::Closed)),
            free_on_drop: vec![],
            _transaction_guard: guard,
            page_allocator: None,
            _value_type: PhantomData,
        }
    }

    fn from_collection(
        collection: AccessGuard<'a, &'static DynamicCollection<V>>,
        guard: Arc<TransactionGuard>,
        mem: PageResolver,
    ) -> Result<Self> {
        Ok(match collection.value().collection_type() {
            Inline => {
                let leaf_iter =
                    LeafKeyIter::new(collection, V::fixed_width(), <() as Value>::fixed_width());
                Self::new_inline(leaf_iter, guard)
            }
            SubtreeV2 => {
                let root = collection.value().as_subtree().root;
                Self::new_subtree(
                    BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                        &(..),
                        Some(root),
                        mem,
                        PageHint::None,
                    )?,
                    collection.value().get_num_values(),
                    guard,
                )
            }
        })
    }

    fn from_collection_free_on_drop(
        collection: AccessGuard<'a, &'static DynamicCollection<V>>,
        pages: Vec<PageNumber>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
        guard: Arc<TransactionGuard>,
        page_allocator: PageAllocator,
    ) -> Result<Self> {
        let num_values = collection.value().get_num_values();
        Ok(match collection.value().collection_type() {
            Inline => {
                let leaf_iter =
                    LeafKeyIter::new(collection, V::fixed_width(), <() as Value>::fixed_width());
                Self::new_inline(leaf_iter, guard)
            }
            SubtreeV2 => {
                let root = collection.value().as_subtree().root;
                let inner = BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                    &(..),
                    Some(root),
                    page_allocator.resolver(),
                    PageHint::None,
                )?;
                Self::new_subtree_free_on_drop(
                    inner,
                    num_values,
                    freed_pages,
                    allocated_pages,
                    pages,
                    guard,
                    page_allocator,
                )
            }
        })
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
        // If `free_on_drop` is non-empty, the subtree pages backing the iteration will be
        // freed when this `MultimapValue` is dropped (the `remove_all` path). Yielded
        // guards may outlive the iterator (e.g. via `collect`), so in that case we must
        // return owned copies rather than references into soon-to-be-freed pages.
        let copy_values = !self.free_on_drop.is_empty();
        let guard = match self.inner.as_mut().unwrap() {
            ValueIterState::Subtree(iter) => match iter.next()? {
                Ok(e) => {
                    if copy_values {
                        AccessGuard::with_owned_value(e.key_data())
                    } else {
                        let (page, key_range, _) = e.into_raw();
                        AccessGuard::with_page(page, key_range)
                    }
                }
                Err(err) => {
                    return Some(Err(err));
                }
            },
            ValueIterState::InlineLeaf(iter) => iter.next_key()?,
        };
        self.remaining -= 1;
        Some(Ok(guard))
    }
}

impl<V: Key + 'static> DoubleEndedIterator for MultimapValue<'_, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let copy_values = !self.free_on_drop.is_empty();
        let guard = match self.inner.as_mut().unwrap() {
            ValueIterState::Subtree(iter) => match iter.next_back()? {
                Ok(e) => {
                    if copy_values {
                        AccessGuard::with_owned_value(e.key_data())
                    } else {
                        let (page, key_range, _) = e.into_raw();
                        AccessGuard::with_page(page, key_range)
                    }
                }
                Err(err) => {
                    return Some(Err(err));
                }
            },
            ValueIterState::InlineLeaf(iter) => iter.next_key_back()?,
        };
        self.remaining -= 1;
        Some(Ok(guard))
    }
}

impl<V: Key + 'static> Drop for MultimapValue<'_, V> {
    fn drop(&mut self) {
        // Drop our references to the pages that are about to be freed
        drop(mem::take(&mut self.inner));
        if !self.free_on_drop.is_empty() {
            let mut freed_pages = self.freed_pages.as_ref().unwrap().lock().unwrap();
            let mut allocated_pages = self.allocated_pages.lock().unwrap();
            for page in &self.free_on_drop {
                if !self
                    .page_allocator
                    .as_ref()
                    .unwrap()
                    .free_if_uncommitted(*page, &mut allocated_pages)
                {
                    freed_pages.push(*page);
                }
            }
        }
    }
}

pub struct MultimapRange<'a, K: Key + 'static, V: Key + 'static> {
    inner: BtreeRangeIter<K, &'static DynamicCollection<V>>,
    mem: PageResolver,
    transaction_guard: Arc<TransactionGuard>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<K: Key + 'static, V: Key + 'static> MultimapRange<'_, K, V> {
    fn new(
        inner: BtreeRangeIter<K, &'static DynamicCollection<V>>,
        guard: Arc<TransactionGuard>,
        mem: PageResolver,
    ) -> Self {
        Self {
            inner,
            mem,
            transaction_guard: guard,
            _key_type: PhantomData,
            _value_type: PhantomData,
            _lifetime: PhantomData,
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
                    MultimapValue::from_collection(
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

impl<K: Key + 'static, V: Key + 'static> DoubleEndedIterator for MultimapRange<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.inner.next_back()? {
            Ok(entry) => {
                let key = AccessGuard::with_owned_value(entry.key_data());
                let (page, _, value_range) = entry.into_raw();
                let collection = AccessGuard::with_page(page, value_range);
                Some(
                    MultimapValue::from_collection(
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
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    tree: BtreeMut<K, &'static DynamicCollection<V>>,
    page_allocator: PageAllocator,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Key + 'static> MultimapTableHandle for MultimapTable<'_, K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<'txn, K: Key + 'static, V: Key + 'static> MultimapTable<'txn, K, V> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        num_values: u64,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
        transaction: &'txn WriteTransaction,
    ) -> MultimapTable<'txn, K, V> {
        MultimapTable {
            name: name.to_string(),
            num_values,
            transaction,
            freed_pages: freed_pages.clone(),
            allocated_pages: allocated_pages.clone(),
            tree: BtreeMut::new(
                table_root,
                transaction.transaction_guard(),
                page_allocator.clone(),
                freed_pages,
                allocated_pages,
            ),
            page_allocator,
            _value_type: PhantomData,
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
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_bytes_ref.len() + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_bytes_ref.len() + key_len));
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

                    if required_inline_bytes < self.page_allocator.get_page_size() / 2 {
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
                        let mut allocated = self.allocated_pages.lock().unwrap();
                        let mut page = self
                            .page_allocator
                            .allocate(leaf_data.len(), &mut allocated)?;
                        drop(allocated);
                        page.memory_mut()[..leaf_data.len()].copy_from_slice(leaf_data);
                        let page_number = page.get_page_number();
                        drop(page);
                        drop(guard);

                        // Don't bother computing the checksum, since we're about to modify the tree
                        let mut subtree: BtreeMut<V, ()> = BtreeMut::new(
                            Some(BtreeHeader::new(page_number, 0, num_pairs as u64)),
                            self.transaction.transaction_guard(),
                            self.page_allocator.clone(),
                            self.freed_pages.clone(),
                            self.allocated_pages.clone(),
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
                    let mut subtree: BtreeMut<V, ()> = BtreeMut::new(
                        Some(guard.value().as_subtree()),
                        self.transaction.transaction_guard(),
                        self.page_allocator.clone(),
                        self.freed_pages.clone(),
                        self.allocated_pages.clone(),
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
            if required_inline_bytes < self.page_allocator.get_page_size() / 2 {
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
                let mut subtree: BtreeMut<V, ()> = BtreeMut::new(
                    None,
                    self.transaction.transaction_guard(),
                    self.page_allocator.clone(),
                    self.freed_pages.clone(),
                    self.allocated_pages.clone(),
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
                    self.page_allocator.clone(),
                    self.freed_pages.clone(),
                    self.allocated_pages.clone(),
                );
                drop(guard);
                let existed = subtree.remove(value.borrow())?.is_some();

                if let Some(BtreeHeader {
                    root: new_root,
                    checksum: new_checksum,
                    length: new_length,
                }) = subtree.get_root()
                {
                    let page = self.page_allocator.get_page(new_root, PageHint::None)?;
                    match page.memory()[0] {
                        LEAF => {
                            let accessor = LeafAccessor::new(
                                page.memory(),
                                V::fixed_width(),
                                <() as Value>::fixed_width(),
                            );
                            let len = accessor.total_length();
                            if len < self.page_allocator.get_page_size() / 2 {
                                let inline_data =
                                    DynamicCollection::<V>::make_inline_data(&page.memory()[..len]);
                                self.tree
                                    .insert(key.borrow(), &DynamicCollection::new(&inline_data))?;
                                drop(page);
                                let mut allocated_pages = self.allocated_pages.lock().unwrap();
                                if !self
                                    .page_allocator
                                    .free_if_uncommitted(new_root, &mut allocated_pages)
                                {
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
    ) -> Result<MultimapValue<'_, V>> {
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
                    self.page_allocator.resolver(),
                    PageHint::None,
                )?;
                for page in all_pages {
                    pages.push(page?);
                }
            }

            self.num_values -= collection.value().get_num_values();

            MultimapValue::from_collection_free_on_drop(
                collection,
                pages,
                self.freed_pages.clone(),
                self.allocated_pages.clone(),
                self.transaction.transaction_guard(),
                self.page_allocator.clone(),
            )?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                    &(..),
                    None,
                    self.page_allocator.resolver(),
                    PageHint::None,
                )?,
                0,
                self.transaction.transaction_guard(),
            )
        };

        Ok(iter)
    }
}

impl<K: Key + 'static, V: Key + 'static> ReadableTableMetadata for MultimapTable<'_, K, V> {
    fn stats(&self) -> Result<TableStats> {
        let tree_stats = multimap_btree_stats(
            self.tree.get_root().map(|x| x.root),
            &self.page_allocator.resolver(),
            K::fixed_width(),
            V::fixed_width(),
            PageHint::None,
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

impl<K: Key + 'static, V: Key + 'static> ReadableMultimapTable<K, V> for MultimapTable<'_, K, V> {
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<'_, V>> {
        let guard = self.transaction.transaction_guard();
        let iter = if let Some(collection) = self.tree.get(key.borrow())? {
            MultimapValue::from_collection(collection, guard, self.page_allocator.resolver())?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                    &(..),
                    None,
                    self.page_allocator.resolver(),
                    PageHint::None,
                )?,
                0,
                guard,
            )
        };

        Ok(iter)
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<MultimapRange<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let inner = self.tree.range(&range)?;
        Ok(MultimapRange::new(
            inner,
            self.transaction.transaction_guard(),
            self.page_allocator.resolver(),
        ))
    }
}

impl<K: Key + 'static, V: Key + 'static> Sealed for MultimapTable<'_, K, V> {}

impl<K: Key + 'static, V: Key + 'static> Drop for MultimapTable<'_, K, V> {
    fn drop(&mut self) {
        self.transaction
            .close_table(&self.name, &self.tree, self.num_values);
    }
}

pub trait ReadableMultimapTable<K: Key + 'static, V: Key + 'static>: ReadableTableMetadata {
    /// Returns an iterator over all values for the given key. Values are in ascending order.
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<'_, V>>;

    /// Returns a double-ended iterator over a range of elements in the table
    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<MultimapRange<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a;

    /// Returns an double-ended iterator over all elements in the table. Values are in ascending
    /// order.
    fn iter(&self) -> Result<MultimapRange<'_, K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }
}

/// A read-only untyped multimap table
pub struct ReadOnlyUntypedMultimapTable {
    num_values: u64,
    tree: RawBtree,
    hint: PageHint,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    mem: PageResolver,
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
            self.hint,
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
        hint: PageHint,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: PageResolver,
    ) -> Self {
        Self {
            num_values,
            tree: RawBtree::new(
                root,
                fixed_key_size,
                DynamicCollection::<()>::fixed_width_with(fixed_value_size),
                mem.clone(),
                hint,
            ),
            hint,
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
    mem: PageResolver,
    transaction_guard: Arc<TransactionGuard>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Key + 'static> ReadOnlyMultimapTable<K, V> {
    pub(crate) fn new(
        root: Option<BtreeHeader>,
        num_values: u64,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: PageResolver,
    ) -> Result<ReadOnlyMultimapTable<K, V>> {
        Ok(ReadOnlyMultimapTable {
            tree: Btree::new(root, hint, guard.clone(), mem.clone())?,
            num_values,
            mem,
            transaction_guard: guard,
            _value_type: PhantomData,
        })
    }

    /// This method is like [`ReadableMultimapTable::get()`], but the iterator is reference counted and keeps the transaction
    /// alive until it is dropped.
    pub fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<'static, V>> {
        let iter = if let Some(collection) = self.tree.get(key.borrow())? {
            MultimapValue::from_collection(
                collection,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                    &(..),
                    None,
                    self.mem.clone(),
                    PageHint::None,
                )?,
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
            self.tree.hint(),
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
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<MultimapValue<'_, V>> {
        let iter = if let Some(collection) = self.tree.get(key.borrow())? {
            MultimapValue::from_collection(
                collection,
                self.transaction_guard.clone(),
                self.mem.clone(),
            )?
        } else {
            MultimapValue::new_subtree(
                BtreeRangeIter::new::<RangeFull, &V::SelfType<'_>>(
                    &(..),
                    None,
                    self.mem.clone(),
                    PageHint::None,
                )?,
                0,
                self.transaction_guard.clone(),
            )
        };

        Ok(iter)
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<MultimapRange<'_, K, V>>
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
