use crate::error::Error;
use crate::tree_store::{BtreeEntry, BtreeRangeIter, NodeHandle, Storage};
use crate::types::{
    AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
};
use std::cell::Cell;
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

pub struct MultimapTable<'s, 't, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'s Storage,
    table_id: u64,
    transaction_id: u128,
    root_page: &'t Cell<Option<NodeHandle>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultimapTable<'s, 't, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        transaction_id: u128,
        root_page: &'t Cell<Option<NodeHandle>>,
        storage: &'s Storage,
    ) -> MultimapTable<'s, 't, K, V> {
        MultimapTable {
            storage,
            table_id,
            transaction_id,
            root_page,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(in crate) fn print_debug(&self) {
        if let Some(page) = self.root_page.get() {
            self.storage.print_dirty_debug(page);
        }
    }

    pub fn insert(&mut self, key: &K, value: &V) -> Result<(), Error> {
        let kv = make_serialized_kv(key, value);
        let page = Some(self.storage.insert::<MultimapKVPair<K, V>>(
            self.table_id,
            &kv,
            b"",
            self.transaction_id,
            self.root_page.get(),
        )?);
        self.root_page.set(page);
        Ok(())
    }

    pub fn get<'a>(
        &'a self,
        key: &'a K,
    ) -> Result<MultimapRangeIter<impl RangeBounds<MultimapKVPair<K, V>>, K, V>, Error> {
        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        self.storage
            .get_range(self.table_id, lower..=upper, self.root_page.get())
            .map(MultimapRangeIter::new)
    }

    pub fn remove(&mut self, key: &K, value: &V) -> Result<(), Error> {
        let kv = make_serialized_kv(key, value);
        let page = self.storage.remove::<MultimapKVPair<K, V>>(
            self.table_id,
            &kv,
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(page);
        Ok(())
    }

    pub fn remove_all(&mut self, key: &K) -> Result<(), Error> {
        // Match only on the key, so that we can remove all the associated values
        let key_only = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyOnly);
        loop {
            let old_root = self.root_page.get();
            let new_root = self.storage.remove::<MultimapKVPair<K, V>>(
                self.table_id,
                &key_only,
                self.transaction_id,
                self.root_page.get(),
            )?;
            if old_root == new_root {
                break;
            }
            self.root_page.set(new_root);
        }
        Ok(())
    }
}

pub struct ReadOnlyMultimapTable<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'s Storage,
    root_page: Option<NodeHandle>,
    table_id: u64,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbKey + ?Sized> ReadOnlyMultimapTable<'s, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        root_page: Option<NodeHandle>,
        storage: &'s Storage,
    ) -> ReadOnlyMultimapTable<'s, K, V> {
        ReadOnlyMultimapTable {
            storage,
            root_page,
            table_id,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn get<'a>(
        &'a self,
        key: &'a K,
    ) -> Result<MultimapRangeIter<impl RangeBounds<MultimapKVPair<K, V>>, K, V>, Error> {
        let lower_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultimapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultimapKVPair::<K, V>::new(lower_bytes);
        let upper = MultimapKVPair::<K, V>::new(upper_bytes);
        self.storage
            .get_range(self.table_id, lower..=upper, self.root_page)
            .map(MultimapRangeIter::new)
    }

    pub fn get_range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<impl RangeBounds<MultimapKVPair<K, V>>, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range(self.table_id, (start, end), self.root_page)
            .map(MultimapRangeIter::new)
    }

    pub fn get_range_reversed<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultimapRangeIter<impl RangeBounds<MultimapKVPair<K, V>>, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultimapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultimapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range_reversed(self.table_id, (start, end), self.root_page)
            .map(MultimapRangeIter::new)
    }

    pub fn len(&self) -> Result<usize, Error> {
        self.storage.len(self.table_id, self.root_page)
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        self.storage
            .len(self.table_id, self.root_page)
            .map(|x| x == 0)
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, MultimapTable, ReadOnlyMultimapTable};
    use tempfile::NamedTempFile;

    fn get_vec(table: &ReadOnlyMultimapTable<[u8], [u8]>, key: &[u8]) -> Vec<Vec<u8>> {
        let mut result = vec![];
        let mut iter = table.get(key).unwrap();
        loop {
            let item = iter.next();
            if let Some((item_key, item_value)) = item {
                assert_eq!(key, item_key);
                result.push(item_value.to_vec());
            } else {
                return result;
            }
        }
    }

    #[test]
    fn len() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();

        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello", b"world2").unwrap();
        table.insert(b"hi", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        assert_eq!(table.len().unwrap(), 3);
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };

        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        assert!(!table.is_empty().unwrap());
    }

    #[test]
    fn insert() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        assert_eq!(
            vec![b"world".to_vec(), b"world2".to_vec()],
            get_vec(&table, b"hello")
        );
        assert_eq!(table.len().unwrap(), 2);
    }

    #[test]
    fn range_query() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();
        for i in 0..5u8 {
            let value = vec![i];
            table.insert(b"0", &value).unwrap();
        }
        for i in 5..10u8 {
            let value = vec![i];
            table.insert(b"1", &value).unwrap();
        }
        for i in 10..15u8 {
            let value = vec![i];
            table.insert(b"2", &value).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        let start = b"0".as_ref();
        let end = b"1".as_ref();
        let mut iter = table.get_range(start..=end).unwrap();
        for i in 0..10u8 {
            let (key, value) = iter.next().unwrap();
            if i < 5 {
                assert_eq!(b"0", key);
            } else {
                assert_eq!(b"1", key);
            }
            assert_eq!(&[i], value);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn delete() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        assert_eq!(
            vec![b"world".to_vec(), b"world2".to_vec()],
            get_vec(&table, b"hello")
        );
        assert_eq!(table.len().unwrap(), 2);

        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();
        table.remove(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        assert_eq!(vec![b"world".to_vec()], get_vec(&table, b"hello"));
        assert_eq!(table.len().unwrap(), 1);

        let write_txn = db.begin_write().unwrap();
        let mut table: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table(b"x").unwrap();
        table.remove_all(b"hello").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyMultimapTable<[u8], [u8]> = read_txn.open_multimap_table(b"x").unwrap();
        assert!(table.is_empty().unwrap());
        let empty: Vec<Vec<u8>> = vec![];
        assert_eq!(empty, get_vec(&table, b"hello"));
    }
}
