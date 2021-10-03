use crate::btree::{BtreeEntry, BtreeRangeIter};
use crate::error::Error;
use crate::page_manager::PageNumber;
use crate::storage::Storage;
use crate::types::{
    AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
};
use std::cmp::Ordering;
use std::collections::Bound;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::ops::RangeBounds;

#[derive(Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum MultiMapKeyCompareOp {
    KeyAndValue,
    KeyMinusEpsilon,
    KeyPlusEpsilon,
    KeyOnly,
}

impl MultiMapKeyCompareOp {
    fn serialize(&self) -> u8 {
        match self {
            MultiMapKeyCompareOp::KeyAndValue => 1,
            MultiMapKeyCompareOp::KeyMinusEpsilon => 2,
            MultiMapKeyCompareOp::KeyPlusEpsilon => 3,
            MultiMapKeyCompareOp::KeyOnly => 4,
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
pub struct MultiMapKVPair<K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    data: Vec<u8>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> AsRef<MultiMapKVPair<K, V>>
    for MultiMapKVPair<K, V>
{
    fn as_ref(&self) -> &MultiMapKVPair<K, V> {
        self
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> RedbValue for MultiMapKVPair<K, V> {
    type View = RefLifetime<[u8]>;
    type ToBytes = RefAsBytesLifetime<[u8]>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        &self.data
    }
}

impl<K: RedbKey + ?Sized, V: RedbKey + ?Sized> RedbKey for MultiMapKVPair<K, V> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let kv1 = MultiMapKVPairAccessor::<K, V>::new(data1);
        let kv2 = MultiMapKVPairAccessor::<K, V>::new(data2);
        // Only one of the inputs may be a query
        assert!(
            kv1.compare_op() == MultiMapKeyCompareOp::KeyAndValue
                || kv2.compare_op() == MultiMapKeyCompareOp::KeyAndValue
        );
        if kv1.compare_op() != MultiMapKeyCompareOp::KeyAndValue {
            Self::compare(data2, data1).reverse()
        } else {
            // Can assume data2 is the query at this point
            match kv2.compare_op() {
                MultiMapKeyCompareOp::KeyAndValue => {
                    match K::compare(kv1.key_bytes(), kv2.key_bytes()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal => V::compare(kv1.value_bytes(), kv2.value_bytes()),
                        Ordering::Greater => Ordering::Greater,
                    }
                }
                MultiMapKeyCompareOp::KeyMinusEpsilon => {
                    match K::compare(kv1.key_bytes(), kv2.key_bytes()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal => Ordering::Greater,
                        Ordering::Greater => Ordering::Greater,
                    }
                }
                MultiMapKeyCompareOp::KeyPlusEpsilon => {
                    match K::compare(kv1.key_bytes(), kv2.key_bytes()) {
                        Ordering::Less => Ordering::Less,
                        Ordering::Equal => Ordering::Less,
                        Ordering::Greater => Ordering::Greater,
                    }
                }
                MultiMapKeyCompareOp::KeyOnly => K::compare(kv1.key_bytes(), kv2.key_bytes()),
            }
        }
    }
}

impl<K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultiMapKVPair<K, V> {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

pub struct MultiMapKVPairAccessor<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    data: &'a [u8],
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a> MultiMapKVPairAccessor<'a, K, V> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    fn compare_op(&self) -> MultiMapKeyCompareOp {
        match self.data[0] {
            1 => MultiMapKeyCompareOp::KeyAndValue,
            2 => MultiMapKeyCompareOp::KeyMinusEpsilon,
            3 => MultiMapKeyCompareOp::KeyPlusEpsilon,
            4 => MultiMapKeyCompareOp::KeyOnly,
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
    let mut result = vec![MultiMapKeyCompareOp::KeyAndValue.serialize()];
    result.extend_from_slice(&(key.as_bytes().as_ref().len() as u32).to_be_bytes());
    result.extend_from_slice(key.as_bytes().as_ref());
    result.extend_from_slice(value.as_bytes().as_ref());

    result
}

fn make_serialized_key_with_op<K: RedbKey + ?Sized>(key: &K, op: MultiMapKeyCompareOp) -> Vec<u8> {
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
            MultiMapKeyCompareOp::KeyMinusEpsilon,
        )),
        Bound::Excluded(&key) => Some(make_serialized_key_with_op(
            key,
            MultiMapKeyCompareOp::KeyPlusEpsilon,
        )),
        Bound::Unbounded => None,
    };

    let end = match range.end_bound() {
        Bound::Included(&key) => Some(make_serialized_key_with_op(
            key,
            MultiMapKeyCompareOp::KeyPlusEpsilon,
        )),
        Bound::Excluded(&key) => Some(make_serialized_key_with_op(
            key,
            MultiMapKeyCompareOp::KeyMinusEpsilon,
        )),
        Bound::Unbounded => None,
    };

    (start, end)
}

fn make_bound<'a, K: RedbKey + ?Sized + 'a, V: RedbKey + ?Sized + 'a>(
    included_or_unbounded: Option<MultiMapKVPair<K, V>>,
) -> Bound<MultiMapKVPair<K, V>> {
    if let Some(kv) = included_or_unbounded {
        Bound::Included(kv)
    } else {
        Bound::Unbounded
    }
}

pub struct MultiMapRangeIter<
    'a,
    T: RangeBounds<MultiMapKVPair<K, V>>,
    K: RedbKey + ?Sized + 'a,
    V: RedbKey + ?Sized + 'a,
> {
    inner: BtreeRangeIter<'a, T, MultiMapKVPair<K, V>, MultiMapKVPair<K, V>, [u8]>,
}

impl<
        'a,
        T: RangeBounds<MultiMapKVPair<K, V>>,
        K: RedbKey + ?Sized + 'a,
        V: RedbKey + ?Sized + 'a,
    > MultiMapRangeIter<'a, T, K, V>
{
    fn new(inner: BtreeRangeIter<'a, T, MultiMapKVPair<K, V>, MultiMapKVPair<K, V>, [u8]>) -> Self {
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
            let pair = MultiMapKVPairAccessor::<K, V> {
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

pub struct MultiMapTable<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'mmap Storage,
    table_id: u64,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultiMapTable<'mmap, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        storage: &'mmap Storage,
    ) -> Result<MultiMapTable<'mmap, K, V>, Error> {
        Ok(MultiMapTable {
            storage,
            table_id,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    pub fn begin_write(&'_ mut self) -> Result<MultiMapWriteTransaction<'mmap, K, V>, Error> {
        Ok(MultiMapWriteTransaction::new(self.table_id, self.storage))
    }

    pub fn read_transaction(&'_ self) -> Result<MultiMapReadOnlyTransaction<'mmap, K, V>, Error> {
        Ok(MultiMapReadOnlyTransaction::new(
            self.table_id,
            self.storage,
        ))
    }
}

pub struct MultiMapWriteTransaction<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'mmap Storage,
    table_id: u64,
    transaction_id: u128,
    root_page: Option<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultiMapWriteTransaction<'mmap, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        storage: &'mmap Storage,
    ) -> MultiMapWriteTransaction<'mmap, K, V> {
        let transaction_id = storage.allocate_write_transaction();
        MultiMapWriteTransaction {
            storage,
            table_id,
            transaction_id,
            root_page: storage.get_root_page_number(),
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(in crate) fn print_debug(&self) {
        if let Some(page) = self.root_page {
            self.storage.print_dirty_debug(page);
        }
    }

    pub fn insert(&mut self, key: &K, value: &V) -> Result<(), Error> {
        let kv = make_serialized_kv(key, value);
        self.root_page = Some(self.storage.insert::<MultiMapKVPair<K, V>>(
            self.table_id,
            &kv,
            b"",
            self.transaction_id,
            self.root_page,
        )?);
        Ok(())
    }

    pub fn get<'a>(
        &'a self,
        key: &'a K,
    ) -> Result<MultiMapRangeIter<impl RangeBounds<MultiMapKVPair<K, V>>, K, V>, Error> {
        let lower_bytes = make_serialized_key_with_op(key, MultiMapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultiMapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultiMapKVPair::<K, V>::new(lower_bytes);
        let upper = MultiMapKVPair::<K, V>::new(upper_bytes);
        self.storage
            .get_range(self.table_id, lower..=upper, self.root_page)
            .map(MultiMapRangeIter::new)
    }

    pub fn remove(&mut self, key: &K, value: &V) -> Result<(), Error> {
        let kv = make_serialized_kv(key, value);
        self.root_page = self.storage.remove::<MultiMapKVPair<K, V>>(
            self.table_id,
            &kv,
            self.transaction_id,
            self.root_page,
        )?;
        Ok(())
    }

    pub fn remove_all(&mut self, key: &K) -> Result<(), Error> {
        // Match only on the key, so that we can remove all the associated values
        let key_only = make_serialized_key_with_op(key, MultiMapKeyCompareOp::KeyOnly);
        loop {
            let old_root = self.root_page;
            self.root_page = self.storage.remove::<MultiMapKVPair<K, V>>(
                self.table_id,
                &key_only,
                self.transaction_id,
                self.root_page,
            )?;
            if old_root == self.root_page {
                break;
            }
        }
        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        self.storage.commit(self.root_page)?;
        Ok(())
    }

    pub fn abort(self) -> Result<(), Error> {
        self.storage.rollback_uncommited_writes(self.transaction_id)
    }
}

pub struct MultiMapReadOnlyTransaction<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    storage: &'mmap Storage,
    root_page: Option<PageNumber>,
    table_id: u64,
    transaction_id: u128,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultiMapReadOnlyTransaction<'mmap, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        storage: &'mmap Storage,
    ) -> MultiMapReadOnlyTransaction<'mmap, K, V> {
        let root_page = storage.get_root_page_number();
        let transaction_id = storage.allocate_read_transaction();
        MultiMapReadOnlyTransaction {
            storage,
            root_page,
            table_id,
            transaction_id,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn get<'a>(
        &'a self,
        key: &'a K,
    ) -> Result<MultiMapRangeIter<impl RangeBounds<MultiMapKVPair<K, V>>, K, V>, Error> {
        let lower_bytes = make_serialized_key_with_op(key, MultiMapKeyCompareOp::KeyMinusEpsilon);
        let upper_bytes = make_serialized_key_with_op(key, MultiMapKeyCompareOp::KeyPlusEpsilon);
        let lower = MultiMapKVPair::<K, V>::new(lower_bytes);
        let upper = MultiMapKVPair::<K, V>::new(upper_bytes);
        self.storage
            .get_range(self.table_id, lower..=upper, self.root_page)
            .map(MultiMapRangeIter::new)
    }

    pub fn get_range<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultiMapRangeIter<impl RangeBounds<MultiMapKVPair<K, V>>, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultiMapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultiMapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range(self.table_id, (start, end), self.root_page)
            .map(MultiMapRangeIter::new)
    }

    pub fn get_range_reversed<'a, T: RangeBounds<&'a K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<MultiMapRangeIter<impl RangeBounds<MultiMapKVPair<K, V>>, K, V>, Error> {
        let (start_bytes, end_bytes) = make_inclusive_query_range(range);
        let start_kv = start_bytes.map(MultiMapKVPair::<K, V>::new);
        let end_kv = end_bytes.map(MultiMapKVPair::<K, V>::new);
        let start = make_bound(start_kv);
        let end = make_bound(end_kv);

        self.storage
            .get_range_reversed(self.table_id, (start, end), self.root_page)
            .map(MultiMapRangeIter::new)
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

impl<'mmap, K: RedbKey + ?Sized, V: RedbKey + ?Sized> Drop
    for MultiMapReadOnlyTransaction<'mmap, K, V>
{
    fn drop(&mut self) {
        self.storage
            .deallocate_read_transaction(self.transaction_id);
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, MultiMapReadOnlyTransaction, MultiMapTable};
    use tempfile::NamedTempFile;

    fn get_vec(txn: &MultiMapReadOnlyTransaction<[u8], [u8]>, key: &[u8]) -> Vec<Vec<u8>> {
        let mut result = vec![];
        let mut iter = txn.get(key).unwrap();
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
        let mut table: MultiMapTable<[u8], [u8]> = db.open_multimap_table(b"x").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello", b"world2").unwrap();
        write_txn.insert(b"hi", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(read_txn.len().unwrap(), 3);
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let mut table: MultiMapTable<[u8], [u8]> = db.open_multimap_table(b"x").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(!read_txn.is_empty().unwrap());
    }

    #[test]
    fn insert() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let mut table: MultiMapTable<[u8], [u8]> = db.open_multimap_table(b"x").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(
            vec![b"world".to_vec(), b"world2".to_vec()],
            get_vec(&read_txn, b"hello")
        );
        assert_eq!(read_txn.len().unwrap(), 2);
    }

    #[test]
    fn range_query() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let mut table: MultiMapTable<[u8], [u8]> = db.open_multimap_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        for i in 0..5u8 {
            let value = vec![i];
            write_txn.insert(b"0", &value).unwrap();
        }
        for i in 5..10u8 {
            let value = vec![i];
            write_txn.insert(b"1", &value).unwrap();
        }
        for i in 10..15u8 {
            let value = vec![i];
            write_txn.insert(b"2", &value).unwrap();
        }
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        let start = b"0".as_ref();
        let end = b"1".as_ref();
        let mut iter = read_txn.get_range(start..=end).unwrap();
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
        let mut table: MultiMapTable<[u8], [u8]> = db.open_multimap_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(
            vec![b"world".to_vec(), b"world2".to_vec()],
            get_vec(&read_txn, b"hello")
        );
        assert_eq!(read_txn.len().unwrap(), 2);

        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert_eq!(vec![b"world".to_vec()], get_vec(&read_txn, b"hello"));
        assert_eq!(read_txn.len().unwrap(), 1);

        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove_all(b"hello").unwrap();
        write_txn.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let empty: Vec<Vec<u8>> = vec![];
        assert_eq!(empty, get_vec(&read_txn, b"hello"));
    }
}
