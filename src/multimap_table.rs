use crate::tree_store::{Btree, BtreeMut, BtreeRangeIter, PageNumber, TransactionalMemory};
use crate::types::{
    AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
};
use crate::{Result, WriteTransaction};
use std::cmp::Ordering;
use std::collections::Bound;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::RangeBounds;

#[derive(Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum MultimapKeyCompareOp {
    KeyAndValue,
    KeyMinusEpsilon,
    KeyPlusEpsilon,
    KeyOnly,
}

impl MultimapKeyCompareOp {
    fn serialize(&self) -> u8 {
        match self {
            MultimapKeyCompareOp::KeyAndValue => 1,
            MultimapKeyCompareOp::KeyMinusEpsilon => 2,
            MultimapKeyCompareOp::KeyPlusEpsilon => 3,
            MultimapKeyCompareOp::KeyOnly => 4,
        }
    }
}

/// Layout:
/// compare_op (1 byte):
/// * 1 = key & value (compare the key & value)
/// * 2 = key - epsilon (represents a value epsilon less than the key)
/// * 3 = key + epsilon (represents a value epsilon greater than the key)
/// * 4 = key-only (compare only the key)
/// key_len: u32
/// key_data: length of key_len
/// value_data:
pub struct MultimapKVPair<K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    data: Vec<u8>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> AsRef<MultimapKVPair<K, V>>
    for MultimapKVPair<K, V>
{
    fn as_ref(&self) -> &MultimapKVPair<K, V> {
        self
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> RedbValue for MultimapKVPair<K, V> {
    type View = RefLifetime<[u8]>;
    type ToBytes = RefAsBytesLifetime<[u8]>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        &self.data
    }

    fn redb_type_name() -> &'static str {
        unreachable!()
    }
}

impl<K: RedbKey + ?Sized, V: RedbKey + ?Sized> RedbKey for MultimapKVPair<K, V> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let kv1 = MultimapKVPairAccessor::<K, V>::new(data1);
        let kv2 = MultimapKVPairAccessor::<K, V>::new(data2);
        // Only one of the inputs may be a query
        assert!(
            kv1.compare_op() == MultimapKeyCompareOp::KeyAndValue
                || kv2.compare_op() == MultimapKeyCompareOp::KeyAndValue
        );
        if kv1.compare_op() != MultimapKeyCompareOp::KeyAndValue {
            Self::compare(data2, data1).reverse()
        } else {
            // Can assume data2 is the query at this point
            match kv2.compare_op() {
                MultimapKeyCompareOp::KeyAndValue => {
                    match K::compare(kv1.key_bytes(), kv2.key_bytes()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal => V::compare(kv1.value_bytes(), kv2.value_bytes()),
                        Ordering::Greater => Ordering::Greater,
                    }
                }
                MultimapKeyCompareOp::KeyMinusEpsilon => {
                    match K::compare(kv1.key_bytes(), kv2.key_bytes()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal => Ordering::Greater,
                        Ordering::Greater => Ordering::Greater,
                    }
                }
                MultimapKeyCompareOp::KeyPlusEpsilon => {
                    match K::compare(kv1.key_bytes(), kv2.key_bytes()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal => Ordering::Less,
                        Ordering::Greater => Ordering::Greater,
                    }
                }
                MultimapKeyCompareOp::KeyOnly => K::compare(kv1.key_bytes(), kv2.key_bytes()),
            }
        }
    }
}

impl<K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultimapKVPair<K, V> {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

pub struct MultimapKVPairAccessor<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    data: &'a [u8],
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> MultimapKVPairAccessor<'a, K, V> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    fn compare_op(&self) -> MultimapKeyCompareOp {
        match self.data[0] {
            1 => MultimapKeyCompareOp::KeyAndValue,
            2 => MultimapKeyCompareOp::KeyMinusEpsilon,
            3 => MultimapKeyCompareOp::KeyPlusEpsilon,
            4 => MultimapKeyCompareOp::KeyOnly,
            _ => unreachable!(),
        }
    }

    fn key_len(&self) -> usize {
        u32::from_le_bytes(self.data[1..5].try_into().unwrap()) as usize
    }

    fn key_bytes(&self) -> &'a [u8] {
        &self.data[5..(5 + self.key_len())]
    }

    fn value_bytes(&self) -> &'a [u8] {
        &self.data[(5 + self.key_len())..]
    }
}

fn make_serialized_kv<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(
    key: &K,
    value: &V,
) -> MultimapKVPair<K, V> {
    let mut result = vec![MultimapKeyCompareOp::KeyAndValue.serialize()];
    result.extend_from_slice(&(key.as_bytes().as_ref().len() as u32).to_le_bytes());
    result.extend_from_slice(key.as_bytes().as_ref());
    result.extend_from_slice(value.as_bytes().as_ref());

    MultimapKVPair::new(result)
}

fn make_serialized_key_with_op<K: RedbKey + ?Sized>(key: &K, op: MultimapKeyCompareOp) -> Vec<u8> {
    let mut result = vec![op.serialize()];
    result.extend_from_slice(&(key.as_bytes().as_ref().len() as u32).to_le_bytes());
    result.extend_from_slice(key.as_bytes().as_ref());

    result
}

// Takes a key range and a lower & upper query bound to be used with an inclusive lower & upper bound
// Returns None if the bound is Unbounded
fn make_inclusive_query_range<'a, K: RedbKey + ?Sized + 'a, T: RangeBounds<&'a K>>(
    range: T,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let start = match range.start_bound() {
        Bound::Included(&key) => Some(make_serialized_key_with_op(
            key,
            MultimapKeyCompareOp::KeyMinusEpsilon,
        )),
        Bound::Excluded(&key) => Some(make_serialized_key_with_op(
            key,
            MultimapKeyCompareOp::KeyPlusEpsilon,
        )),
        Bound::Unbounded => None,
    };

    let end = match range.end_bound() {
        Bound::Included(&key) => Some(make_serialized_key_with_op(
            key,
            MultimapKeyCompareOp::KeyPlusEpsilon,
        )),
        Bound::Excluded(&key) => Some(make_serialized_key_with_op(
            key,
            MultimapKeyCompareOp::KeyMinusEpsilon,
        )),
        Bound::Unbounded => None,
    };

    (start, end)
}

fn make_bound<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a>(
    included_or_unbounded: Option<MultimapKVPair<K, V>>,
) -> Bound<MultimapKVPair<K, V>> {
    if let Some(kv) = included_or_unbounded {
        Bound::Included(kv)
    } else {
        Bound::Unbounded
    }
}

pub struct MultimapValueIter<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> {
    inner: BtreeRangeIter<'a, MultimapKVPair<K, V>, [u8]>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> MultimapValueIter<'a, K, V> {
    fn new(inner: BtreeRangeIter<'a, MultimapKVPair<K, V>, [u8]>) -> Self {
        Self { inner }
    }

    // TODO: implement Iter when GATs are stable
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<<<V as RedbValue>::View as WithLifetime>::Out> {
        if let Some(entry) = self.inner.next() {
            let pair = MultimapKVPairAccessor::<K, V> {
                data: entry.key(),
                _key_type: Default::default(),
                _value_type: Default::default(),
            };
            Some(V::from_bytes(pair.value_bytes()))
        } else {
            None
        }
    }

    pub fn rev(self) -> Self {
        Self::new(self.inner.reverse())
    }
}

pub struct MultimapRangeIter<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> {
    inner: BtreeRangeIter<'a, MultimapKVPair<K, V>, [u8]>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> MultimapRangeIter<'a, K, V> {
    fn new(inner: BtreeRangeIter<'a, MultimapKVPair<K, V>, [u8]>) -> Self {
        Self { inner }
    }

    // TODO: Simplify this when GATs are stable
    #[allow(clippy::type_complexity)]
    // TODO: implement Iter when GATs are stable
    #[allow(clippy::should_implement_trait)]
    pub fn next(
        &mut self,
    ) -> Option<(
        <<K as RedbValue>::View as WithLifetime>::Out,
        <<V as RedbValue>::View as WithLifetime>::Out,
    )> {
        if let Some(entry) = self.inner.next() {
            let pair = MultimapKVPairAccessor::<K, V> {
                data: entry.key(),
                _key_type: Default::default(),
                _value_type: Default::default(),
            };
            let key = K::from_bytes(pair.key_bytes());
            let value = V::from_bytes(pair.value_bytes());
            Some((key, value))
        } else {
            None
        }
    }

    pub fn rev(self) -> Self {
        Self::new(self.inner.reverse())
    }
}

pub struct MultimapTable<'s, 't, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    name: String,
    transaction: &'t WriteTransaction<'s>,
    tree: BtreeMut<'t, MultimapKVPair<K, V>, [u8]>,
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultimapTable<'s, 't, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<PageNumber>,
        mem: &'s TransactionalMemory,
        transaction: &'t WriteTransaction<'s>,
    ) -> MultimapTable<'s, 't, K, V> {
        MultimapTable {
            name: name.to_string(),
            transaction,
            tree: BtreeMut::new(table_root, mem),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        self.tree.print_debug();
    }

    pub fn insert(&mut self, key: &K, value: &V) -> Result {
        let kv = make_serialized_kv(key, value);
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert(&kv, b"") }
    }

    pub fn remove(&mut self, key: &K, value: &V) -> Result<bool> {
        let kv = make_serialized_kv(key, value);
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.remove(&kv).map(|x| x.is_some()) }
    }

    pub fn remove_all(&mut self, key: &K) -> Result<MultimapValueIter<K, V>> {
        // Match only on the key, so that we can remove all the associated values
        let key_only = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyOnly);
        let key_only = MultimapKVPair::new(key_only);
        // Save a snapshot of the btree. This is safe since we call remove_retain_uncommitted()
        // instead of remove()
        let original_tree = Btree::new(self.tree.root, self.tree.mem);
        loop {
            let found = self.tree.remove_retain_uncommitted(&key_only)?;
            if found.is_none() {
                break;
            }
        }

        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        original_tree
            .range(lower..=upper)
            .map(MultimapValueIter::new)
    }
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadableMultimapTable<K, V>
    for MultimapTable<'s, 't, K, V>
{
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapValueIter<'a, K, V>> {
        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        self.tree.range(lower..=upper).map(MultimapValueIter::new)
    }

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<'a, K, V>> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.tree.range((start, end)).map(MultimapRangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbKey + ?Sized> Drop for MultimapTable<'s, 't, K, V> {
    fn drop(&mut self) {
        self.transaction.close_table(&self.name, &mut self.tree);
    }
}

pub trait ReadableMultimapTable<K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapValueIter<'a, K, V>>;

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<'a, K, V>>;

    fn len(&self) -> Result<usize>;

    fn is_empty(&self) -> Result<bool>;
}

pub struct ReadOnlyMultimapTable<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    tree: Btree<'s, MultimapKVPair<K, V>, [u8]>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadOnlyMultimapTable<'s, K, V> {
    pub(crate) fn new(
        root_page: Option<PageNumber>,
        mem: &'s TransactionalMemory,
    ) -> ReadOnlyMultimapTable<'s, K, V> {
        ReadOnlyMultimapTable {
            tree: Btree::new(root_page, mem),
        }
    }
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadableMultimapTable<K, V>
    for ReadOnlyMultimapTable<'s, K, V>
{
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapValueIter<'a, K, V>> {
        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        self.tree.range(lower..=upper).map(MultimapValueIter::new)
    }

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<'a, K, V>> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.tree.range((start, end)).map(MultimapRangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}
