use crate::error::Error;
use crate::tree_store::{BtreeEntry, BtreeRangeIter, PageNumber, Storage, TransactionId};
use crate::types::{
    AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
};
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::Bound;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeInclusive};
use std::rc::Rc;

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
        u32::from_be_bytes(self.data[1..5].try_into().unwrap()) as usize
    }

    fn key_bytes(&self) -> &'a [u8] {
        &self.data[5..(5 + self.key_len())]
    }

    fn value_bytes(&self) -> &'a [u8] {
        &self.data[(5 + self.key_len())..]
    }
}

fn make_serialized_kv<K: RedbKey + ?Sized, V: RedbKey + ?Sized>(key: &K, value: &V) -> Vec<u8> {
    let mut result = vec![MultimapKeyCompareOp::KeyAndValue.serialize()];
    result.extend_from_slice(&(key.as_bytes().as_ref().len() as u32).to_be_bytes());
    result.extend_from_slice(key.as_bytes().as_ref());
    result.extend_from_slice(value.as_bytes().as_ref());

    result
}

fn make_serialized_key_with_op<K: RedbKey + ?Sized>(key: &K, op: MultimapKeyCompareOp) -> Vec<u8> {
    let mut result = vec![op.serialize()];
    result.extend_from_slice(&(key.as_bytes().as_ref().len() as u32).to_be_bytes());
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

pub struct MultimapValueIter<
    'a,
    T: RangeBounds<MultimapKVPair<K, V>>,
    K: RedbKey + ?Sized + 'a,
    V: RedbKey + ?Sized + 'a,
> {
    inner: BtreeRangeIter<'a, T, MultimapKVPair<K, V>, MultimapKVPair<K, V>, [u8]>,
}

impl<
        'a,
        T: RangeBounds<MultimapKVPair<K, V>>,
        K: RedbKey + ?Sized + 'a,
        V: RedbKey + ?Sized + 'a,
    > MultimapValueIter<'a, T, K, V>
{
    fn new(inner: BtreeRangeIter<'a, T, MultimapKVPair<K, V>, MultimapKVPair<K, V>, [u8]>) -> Self {
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
}

pub struct MultimapRangeIter<
    'a,
    T: RangeBounds<MultimapKVPair<K, V>>,
    K: RedbKey + ?Sized + 'a,
    V: RedbKey + ?Sized + 'a,
> {
    inner: BtreeRangeIter<'a, T, MultimapKVPair<K, V>, MultimapKVPair<K, V>, [u8]>,
}

impl<
        'a,
        T: RangeBounds<MultimapKVPair<K, V>>,
        K: RedbKey + ?Sized + 'a,
        V: RedbKey + ?Sized + 'a,
    > MultimapRangeIter<'a, T, K, V>
{
    fn new(inner: BtreeRangeIter<'a, T, MultimapKVPair<K, V>, MultimapKVPair<K, V>, [u8]>) -> Self {
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
}

pub struct MultimapTable<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'s Storage,
    transaction_id: TransactionId,
    table_root: Rc<Cell<Option<PageNumber>>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultimapTable<'s, K, V> {
    pub(in crate) fn new(
        transaction_id: TransactionId,
        table_root: Rc<Cell<Option<PageNumber>>>,
        storage: &'s Storage,
    ) -> MultimapTable<'s, K, V> {
        MultimapTable {
            storage,
            transaction_id,
            table_root,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(in crate) fn print_debug(&self) {
        if let Some(page) = self.table_root.get() {
            self.storage.print_dirty_tree_debug(page);
        }
    }

    pub fn insert(&mut self, key: &K, value: &V) -> Result<(), Error> {
        let kv = make_serialized_kv(key, value);
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        let root_page = unsafe {
            self.storage.insert::<MultimapKVPair<K, V>>(
                &kv,
                b"",
                self.transaction_id,
                self.table_root.get(),
            )
        }?;
        self.table_root.set(Some(root_page));
        Ok(())
    }

    pub fn remove(&mut self, key: &K, value: &V) -> Result<bool, Error> {
        let kv = make_serialized_kv(key, value);
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        let (root_page, found) = unsafe {
            self.storage.remove::<MultimapKVPair<K, V>, [u8]>(
                &kv,
                self.transaction_id,
                self.table_root.get(),
            )?
        };
        self.table_root.set(root_page);
        Ok(found.is_some())
    }

    // TODO: maybe this should return all the removed values as an iter?
    pub fn remove_all(&mut self, key: &K) -> Result<bool, Error> {
        // Match only on the key, so that we can remove all the associated values
        let key_only = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyOnly);
        let mut any_found = false;
        loop {
            // Safety: No other references to this table can exist.
            // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
            // and we borrow &mut self.
            let (new_root, found) = unsafe {
                self.storage.remove::<MultimapKVPair<K, V>, [u8]>(
                    &key_only,
                    self.transaction_id,
                    self.table_root.get(),
                )?
            };
            if found.is_none() {
                break;
            }
            self.table_root.set(new_root);
            any_found = true;
        }
        Ok(any_found)
    }
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadableMultimapTable<K, V>
    for MultimapTable<'s, K, V>
{
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapGetIterType<'a, K, V>, Error> {
        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        self.storage
            .get_range(lower..=upper, self.table_root.get())
            .map(MultimapValueIter::new)
    }

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapGetRangeIterType<'a, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range((start, end), self.table_root.get())
            .map(MultimapRangeIter::new)
    }

    fn range_reversed<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapGetRangeIterType<'a, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range_reversed((start, end), self.table_root.get())
            .map(MultimapRangeIter::new)
    }

    fn len(&self) -> Result<usize, Error> {
        self.storage.len(self.table_root.get())
    }

    fn is_empty(&self) -> Result<bool, Error> {
        self.storage.len(self.table_root.get()).map(|x| x == 0)
    }
}

type MultimapGetIterType<'a, K, V> =
    MultimapValueIter<'a, RangeInclusive<MultimapKVPair<K, V>>, K, V>;
type MultimapGetRangeIterType<'a, K, V> =
    MultimapRangeIter<'a, (Bound<MultimapKVPair<K, V>>, Bound<MultimapKVPair<K, V>>), K, V>;

pub trait ReadableMultimapTable<K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapGetIterType<'a, K, V>, Error>;

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapGetRangeIterType<'a, K, V>, Error>;

    fn range_reversed<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapGetRangeIterType<'a, K, V>, Error>;

    fn len(&self) -> Result<usize, Error>;

    fn is_empty(&self) -> Result<bool, Error>;
}

pub struct ReadOnlyMultimapTable<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'s Storage,
    table_root: Option<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadOnlyMultimapTable<'s, K, V> {
    pub(in crate) fn new(
        root_page: Option<PageNumber>,
        storage: &'s Storage,
    ) -> ReadOnlyMultimapTable<'s, K, V> {
        ReadOnlyMultimapTable {
            storage,
            table_root: root_page,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadableMultimapTable<K, V>
    for ReadOnlyMultimapTable<'s, K, V>
{
    fn get<'a>(&'a self, key: &'a K) -> Result<MultimapGetIterType<'a, K, V>, Error> {
        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        self.storage
            .get_range(lower..=upper, self.table_root)
            .map(MultimapValueIter::new)
    }

    fn range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapGetRangeIterType<'a, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range((start, end), self.table_root)
            .map(MultimapRangeIter::new)
    }

    fn range_reversed<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapGetRangeIterType<'a, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range_reversed((start, end), self.table_root)
            .map(MultimapRangeIter::new)
    }

    fn len(&self) -> Result<usize, Error> {
        self.storage.len(self.table_root)
    }

    fn is_empty(&self) -> Result<bool, Error> {
        self.storage.len(self.table_root).map(|x| x == 0)
    }
}
