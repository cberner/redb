use crate::multimap_table::DynamicCollectionType::{Inline, Subtree};
use crate::tree_store::{
    AllPageNumbersBtreeIter, Btree, BtreeMut, BtreeRangeIter, Checksum, LeafAccessor, LeafKeyIter,
    Page, PageNumber, RawLeafBuilder, TransactionalMemory, BRANCH, LEAF,
};
use crate::types::{
    AsBytesWithLifetime, OwnedLifetime, RedbKey, RedbValue, RefAsBytesLifetime, WithLifetime,
};
use crate::{Result, WriteTransaction};
use std::cell::RefCell;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{RangeBounds, RangeFull};
use std::rc::Rc;

pub(crate) fn parse_subtree_roots<T: Page>(
    page: &T,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
) -> Vec<PageNumber> {
    match page.memory()[0] {
        BRANCH => {
            vec![]
        }
        LEAF => {
            let mut result = vec![];
            let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
            for i in 0..accessor.num_pairs() {
                let entry = accessor.entry(i).unwrap();
                let collection = DynamicCollection::from_bytes(entry.value());
                if matches!(collection.collection_type(), DynamicCollectionType::Subtree) {
                    result.push(collection.as_subtree().0);
                }
            }

            result
        }
        _ => unreachable!(),
    }
}

enum DynamicCollectionType {
    Inline,
    Subtree,
}

impl From<u8> for DynamicCollectionType {
    fn from(value: u8) -> Self {
        match value {
            LEAF => Inline,
            2 => Subtree,
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
            Subtree => 2,
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
#[derive(Debug)]
struct DynamicCollection {
    data: Vec<u8>,
    // TODO: include V type when GATs are stable
    // _value_type: PhantomData<V>,
}

impl RedbValue for DynamicCollection {
    type View = OwnedLifetime<DynamicCollection>;
    type ToBytes = RefAsBytesLifetime<[u8]>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        // TODO: optimize out this copy
        Self::new(data.to_vec())
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        &self.data
    }

    fn redb_type_name() -> String {
        "redb::DynamicCollection".to_string()
    }
}

impl DynamicCollection {
    fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    fn collection_type(&self) -> DynamicCollectionType {
        DynamicCollectionType::from(self.data[0])
    }

    fn as_inline(&self) -> &[u8] {
        debug_assert!(matches!(self.collection_type(), Inline));
        &self.data[1..]
    }

    fn as_subtree(&self) -> (PageNumber, Checksum) {
        debug_assert!(matches!(self.collection_type(), Subtree));
        let offset = 1 + PageNumber::serialized_size();
        let page_number = PageNumber::from_le_bytes(self.data[1..offset].try_into().unwrap());
        let checksum = Checksum::from_le_bytes(
            self.data[offset..(offset + size_of::<Checksum>())]
                .try_into()
                .unwrap(),
        );
        (page_number, checksum)
    }

    fn iter<'a, V: RedbKey + ?Sized>(
        &self,
        mem: &'a TransactionalMemory,
    ) -> MultimapValueIter<'a, V> {
        match self.collection_type() {
            Inline => {
                let leaf_iter = LeafKeyIter::new(
                    self.as_inline().to_vec(), // TODO: optimize out this copy
                    V::fixed_width(),
                    <() as RedbValue>::fixed_width(),
                );
                MultimapValueIter::new_inline(leaf_iter)
            }
            Subtree => {
                let root = self.as_subtree().0;
                MultimapValueIter::new_subtree(BtreeRangeIter::new::<RangeFull, &V>(
                    ..,
                    Some(root),
                    mem,
                ))
            }
        }
    }

    fn new_inline(data: &[u8]) -> Self {
        let mut result = vec![Inline.into()];
        result.extend_from_slice(data);

        Self::new(result)
    }

    fn new_subtree(root: PageNumber, checksum: Checksum) -> Self {
        let mut result = vec![Subtree.into()];
        result.extend_from_slice(&root.to_le_bytes());
        result.extend_from_slice(checksum.as_bytes().as_ref());

        Self::new(result)
    }
}

enum ValueIterState<'a, V: RedbKey + ?Sized + 'a> {
    Subtree(BtreeRangeIter<'a, V, ()>),
    InlineLeaf(LeafKeyIter),
}

impl<'a, V: RedbKey + ?Sized + 'a> ValueIterState<'a, V> {
    fn reverse(self) -> Self {
        match self {
            ValueIterState::Subtree(iter) => ValueIterState::Subtree(iter.reverse()),
            ValueIterState::InlineLeaf(iter) => ValueIterState::InlineLeaf(iter.reverse()),
        }
    }
}

#[doc(hidden)]
pub struct MultimapValueIter<'a, V: RedbKey + ?Sized + 'a> {
    inner: ValueIterState<'a, V>,
}

impl<'a, V: RedbKey + ?Sized + 'a> MultimapValueIter<'a, V> {
    fn new_subtree(inner: BtreeRangeIter<'a, V, ()>) -> Self {
        Self {
            inner: ValueIterState::Subtree(inner),
        }
    }

    fn new_inline(inner: LeafKeyIter) -> Self {
        Self {
            inner: ValueIterState::InlineLeaf(inner),
        }
    }

    // TODO: implement Iter when GATs are stable
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<<<V as RedbValue>::View as WithLifetime>::Out> {
        match self.inner {
            ValueIterState::Subtree(ref mut iter) => iter.next().map(|e| V::from_bytes(e.key())),
            ValueIterState::InlineLeaf(ref mut iter) => {
                iter.next_key().map(|key| V::from_bytes(key))
            }
        }
    }

    pub fn rev(self) -> Self {
        Self {
            inner: self.inner.reverse(),
        }
    }
}

#[doc(hidden)]
pub struct MultimapRangeIter<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> {
    inner: BtreeRangeIter<'a, K, DynamicCollection>,
    mem: &'a TransactionalMemory,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> MultimapRangeIter<'a, K, V> {
    fn new(inner: BtreeRangeIter<'a, K, DynamicCollection>, mem: &'a TransactionalMemory) -> Self {
        Self {
            inner,
            mem,
            _value_type: Default::default(),
        }
    }

    // TODO: Simplify this when GATs are stable
    #[allow(clippy::type_complexity)]
    // TODO: implement Iter when GATs are stable
    #[allow(clippy::should_implement_trait)]
    pub fn next(
        &mut self,
    ) -> Option<(
        <<K as RedbValue>::View as WithLifetime>::Out,
        MultimapValueIter<V>,
    )> {
        let entry = self.inner.next()?;
        let key = K::from_bytes(entry.key());
        let collection = DynamicCollection::from_bytes(entry.value());
        let iter = collection.iter(self.mem);

        Some((key, iter))
    }

    pub fn rev(self) -> Self {
        Self {
            inner: self.inner.reverse(),
            mem: self.mem,
            _value_type: Default::default(),
        }
    }
}

// TODO: add fuzzer for multimap tables

/// A multimap table
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
pub struct MultimapTable<'db, 'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    name: String,
    transaction: &'txn WriteTransaction<'db>,
    freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    tree: BtreeMut<'txn, K, DynamicCollection>,
    mem: &'db TransactionalMemory,
    _value_type: PhantomData<V>,
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultimapTable<'db, 'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<(PageNumber, Checksum)>,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
        mem: &'db TransactionalMemory,
        transaction: &'txn WriteTransaction<'db>,
    ) -> MultimapTable<'db, 'txn, K, V> {
        MultimapTable {
            name: name.to_string(),
            transaction,
            freed_pages: freed_pages.clone(),
            tree: BtreeMut::new(table_root, mem, freed_pages),
            mem,
            _value_type: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) {
        self.tree.print_debug(include_values);
    }

    /// Add the given value to the mapping of the key
    ///
    /// Returns `true` if the key-value pair was present
    pub fn insert(&mut self, key: &K, value: &V) -> Result<bool> {
        let existed = if let Some(v) = self.tree.get(key)? {
            match v.collection_type() {
                Inline => {
                    let leaf_data = v.as_inline();
                    let accessor = LeafAccessor::new(
                        leaf_data,
                        V::fixed_width(),
                        <() as RedbValue>::fixed_width(),
                    );
                    let (position, found) = accessor.position::<V>(value.as_bytes().as_ref());
                    if found {
                        return Ok(true);
                    }

                    let new_pairs = accessor.num_pairs() + 1;
                    let new_pair_bytes = accessor.length_of_pairs(0, accessor.num_pairs())
                        + value.as_bytes().as_ref().len();
                    let new_key_bytes = accessor.length_of_keys(0, accessor.num_pairs())
                        + value.as_bytes().as_ref().len();
                    let required_inline_bytes =
                        RawLeafBuilder::required_bytes(new_pairs, new_pair_bytes);

                    if required_inline_bytes < self.mem.get_page_size() / 2 {
                        let mut data = vec![0; required_inline_bytes];
                        let mut builder = RawLeafBuilder::new(
                            &mut data,
                            new_pairs,
                            V::fixed_width(),
                            <() as RedbValue>::fixed_width(),
                            new_key_bytes,
                        );
                        for i in 0..accessor.num_pairs() {
                            if i == position {
                                builder.append(value.as_bytes().as_ref(), ().as_bytes().as_ref());
                            }
                            let entry = accessor.entry(i).unwrap();
                            builder.append(entry.key(), entry.value());
                        }
                        if position == accessor.num_pairs() {
                            builder.append(value.as_bytes().as_ref(), ().as_bytes().as_ref());
                        }
                        drop(builder);
                        unsafe {
                            self.tree
                                .insert(key, &DynamicCollection::new_inline(&data))?
                        };
                    } else {
                        // convert into a subtree
                        let mut page = self.mem.allocate(leaf_data.len())?;
                        page.memory_mut()[..leaf_data.len()].copy_from_slice(leaf_data);
                        let page_number = page.get_page_number();
                        drop(page);

                        // Don't bother computing the checksum, since we're about to modify the tree
                        let mut subtree = BtreeMut::new(
                            Some((page_number, 0)),
                            self.mem,
                            self.freed_pages.clone(),
                        );
                        // Safety: No other references to this table can exist.
                        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
                        // and we borrow &mut self.
                        let existed = unsafe { subtree.insert(value, &())?.is_some() };
                        assert_eq!(existed, found);
                        let (new_root, new_checksum) = subtree.get_root().unwrap();
                        unsafe {
                            self.tree.insert(
                                key,
                                &DynamicCollection::new_subtree(new_root, new_checksum),
                            )?
                        };
                    }

                    found
                }
                Subtree => {
                    let mut subtree =
                        BtreeMut::new(Some(v.as_subtree()), self.mem, self.freed_pages.clone());
                    // Safety: No other references to this table can exist.
                    // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
                    // and we borrow &mut self.
                    let existed = unsafe { subtree.insert(value, &())?.is_some() };
                    let (new_root, new_checksum) = subtree.get_root().unwrap();
                    unsafe {
                        self.tree
                            .insert(key, &DynamicCollection::new_subtree(new_root, new_checksum))?
                    };

                    existed
                }
            }
        } else {
            let required_inline_bytes =
                RawLeafBuilder::required_bytes(1, value.as_bytes().as_ref().len());
            if required_inline_bytes < self.mem.get_page_size() / 2 {
                let mut data = vec![0; required_inline_bytes];
                let mut builder = RawLeafBuilder::new(
                    &mut data,
                    1,
                    V::fixed_width(),
                    <() as RedbValue>::fixed_width(),
                    value.as_bytes().as_ref().len(),
                );
                builder.append(value.as_bytes().as_ref(), ().as_bytes().as_ref());
                drop(builder);
                unsafe {
                    self.tree
                        .insert(key, &DynamicCollection::new_inline(&data))?
                };
            } else {
                let mut subtree = BtreeMut::new(None, self.mem, self.freed_pages.clone());
                // Safety: No other references to this table can exist.
                // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
                // and we borrow &mut self.
                unsafe { subtree.insert(value, &())? };
                let (new_root, new_checksum) = subtree.get_root().unwrap();
                unsafe {
                    self.tree
                        .insert(key, &DynamicCollection::new_subtree(new_root, new_checksum))?
                };
            }
            false
        };

        Ok(existed)
    }

    /// Removes the given key-value pair
    ///
    /// Returns `true` if the key-value pair was present
    pub fn remove(&mut self, key: &K, value: &V) -> Result<bool> {
        let existed = if let Some(v) = self.tree.get(key)? {
            match v.collection_type() {
                Inline => {
                    let leaf_data = v.as_inline();
                    let accessor = LeafAccessor::new(
                        leaf_data,
                        V::fixed_width(),
                        <() as RedbValue>::fixed_width(),
                    );
                    if let Some(position) = accessor.find_key::<V>(value.as_bytes().as_ref()) {
                        let old_num_pairs = accessor.num_pairs();
                        if old_num_pairs == 1 {
                            unsafe { self.tree.remove(key)? };
                        } else {
                            let old_pairs_len = accessor.length_of_pairs(0, old_num_pairs);
                            let removed_value_len = accessor.entry(position).unwrap().key().len();
                            let required = RawLeafBuilder::required_bytes(
                                old_num_pairs - 1,
                                old_pairs_len - removed_value_len,
                            );
                            let mut new_data = vec![0; required];
                            let new_key_len =
                                accessor.length_of_keys(0, old_num_pairs) - removed_value_len;
                            let mut builder = RawLeafBuilder::new(
                                &mut new_data,
                                old_num_pairs - 1,
                                V::fixed_width(),
                                <() as RedbValue>::fixed_width(),
                                new_key_len,
                            );
                            for i in 0..old_num_pairs {
                                if i != position {
                                    let entry = accessor.entry(i).unwrap();
                                    builder.append(entry.key(), entry.value());
                                }
                            }
                            drop(builder);

                            unsafe {
                                self.tree
                                    .insert(key, &DynamicCollection::new_inline(&new_data))?
                            };
                        }
                        true
                    } else {
                        false
                    }
                }
                Subtree => {
                    let mut subtree: BtreeMut<V, ()> =
                        BtreeMut::new(Some(v.as_subtree()), self.mem, self.freed_pages.clone());
                    // Safety: No other references to this table can exist.
                    // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
                    // and we borrow &mut self.
                    let existed = unsafe { subtree.remove(value)?.is_some() };

                    if let Some((new_root, new_checksum)) = subtree.get_root() {
                        // TODO: if the tree has shrunk to a single small leaf then inline it
                        unsafe {
                            self.tree.insert(
                                key,
                                &DynamicCollection::new_subtree(new_root, new_checksum),
                            )?
                        };
                    } else {
                        unsafe { self.tree.remove(key)? };
                    }

                    existed
                }
            }
        } else {
            false
        };

        Ok(existed)
    }

    /// Removes all values for the given key
    ///
    /// Returns an iterator over the removed values
    pub fn remove_all(&mut self, key: &K) -> Result<MultimapValueIter<V>> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        let iter = if let Some(collection) = unsafe { self.tree.remove(key)? } {
            if matches!(
                collection.to_value().collection_type(),
                DynamicCollectionType::Subtree
            ) {
                // TODO: free these when the iter is dropped, if they are uncommitted
                let root = collection.to_value().as_subtree().0;
                let all_pages = AllPageNumbersBtreeIter::new(
                    root,
                    V::fixed_width(),
                    <() as RedbValue>::fixed_width(),
                    self.mem,
                );
                for page in all_pages {
                    (*self.freed_pages).borrow_mut().push(page);
                }
            }
            collection.to_value().iter(self.mem)
        } else {
            MultimapValueIter::new_subtree(BtreeRangeIter::new::<RangeFull, &V>(.., None, self.mem))
        };

        Ok(iter)
    }
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadableMultimapTable<K, V>
    for MultimapTable<'db, 'txn, K, V>
{
    /// Returns an iterator over all values for the given key
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapValueIter<'a, V>> {
        let iter = if let Some(collection) = self.tree.get(key)? {
            collection.iter(self.mem)
        } else {
            MultimapValueIter::new_subtree(BtreeRangeIter::new::<RangeFull, &V>(.., None, self.mem))
        };

        Ok(iter)
    }

    /// Returns a double-ended iterator over a range of elements in the table
    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<'a, K, V>> {
        let inner = self.tree.range(range)?;
        Ok(MultimapRangeIter::new(inner, self.mem))
    }

    /// Returns the number of key-value pairs in the table
    fn len(&self) -> Result<usize> {
        let mut iter: MultimapRangeIter<K, V> = self.range(..)?;
        let mut count = 0;
        while let Some((_, mut values)) = iter.next() {
            while values.next().is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Returns `true` if the table is empty
    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> Drop for MultimapTable<'db, 'txn, K, V> {
    fn drop(&mut self) {
        self.transaction.close_table(&self.name, &mut self.tree);
    }
}

pub trait ReadableMultimapTable<K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapValueIter<'a, V>>;

    // TODO: Take a KR: Borrow<K>, just like Table::range
    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<'a, K, V>>;

    fn len(&self) -> Result<usize>;

    fn is_empty(&self) -> Result<bool>;
}

/// A read-only multimap table
pub struct ReadOnlyMultimapTable<'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    tree: Btree<'txn, K, DynamicCollection>,
    mem: &'txn TransactionalMemory,
    _value_type: PhantomData<V>,
}

impl<'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadOnlyMultimapTable<'txn, K, V> {
    pub(crate) fn new(
        root_page: Option<(PageNumber, Checksum)>,
        mem: &'txn TransactionalMemory,
    ) -> ReadOnlyMultimapTable<'txn, K, V> {
        ReadOnlyMultimapTable {
            tree: Btree::new(root_page, mem),
            mem,
            _value_type: Default::default(),
        }
    }
}

impl<'txn, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadableMultimapTable<K, V>
    for ReadOnlyMultimapTable<'txn, K, V>
{
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapValueIter<'a, V>> {
        let iter = if let Some(collection) = self.tree.get(key)? {
            collection.iter(self.mem)
        } else {
            MultimapValueIter::new_subtree(BtreeRangeIter::new::<RangeFull, &V>(.., None, self.mem))
        };

        Ok(iter)
    }

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<'a, K, V>> {
        let inner = self.tree.range(range)?;
        Ok(MultimapRangeIter::new(inner, self.mem))
    }

    fn len(&self) -> Result<usize> {
        let mut iter: MultimapRangeIter<K, V> = self.range(..)?;
        let mut count = 0;
        while let Some((_, mut values)) = iter.next() {
            while values.next().is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}
