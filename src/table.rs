use crate::error::Error;
use crate::tree_store::{AccessGuardMut, BtreeEntry, BtreeRangeIter, NodeHandle, Storage};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::AccessGuard;
use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::RangeBounds;

pub struct Table<'s, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'s Storage,
    table_id: u64,
    transaction_id: u128,
    root_page: &'t Cell<Option<NodeHandle>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Table<'s, 't, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        transaction_id: u128,
        root_page: &'t Cell<Option<NodeHandle>>,
        storage: &'s Storage,
    ) -> Table<'s, 't, K, V> {
        Table {
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
        let page = Some(self.storage.insert::<K>(
            self.table_id,
            key.as_bytes().as_ref(),
            value.as_bytes().as_ref(),
            self.transaction_id,
            self.root_page.get(),
        )?);
        self.root_page.set(page);
        Ok(())
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<AccessGuardMut, Error> {
        let (root_page, guard) = self.storage.insert_reserve::<K>(
            self.table_id,
            key.as_bytes().as_ref(),
            value_length,
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(Some(root_page));
        Ok(guard)
    }

    pub fn get(&self, key: &K) -> Result<Option<AccessGuard<V>>, Error> {
        self.storage
            .get::<K, V>(self.table_id, key.as_bytes().as_ref(), self.root_page.get())
    }

    pub fn remove(&mut self, key: &K) -> Result<(), Error> {
        let page = self.storage.remove::<K>(
            self.table_id,
            key.as_bytes().as_ref(),
            self.transaction_id,
            self.root_page.get(),
        )?;
        self.root_page.set(page);
        Ok(())
    }
}

pub struct ReadOnlyTable<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'s Storage,
    root_page: Option<NodeHandle>,
    table_id: u64,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadOnlyTable<'s, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        root_page: Option<NodeHandle>,
        storage: &'s Storage,
    ) -> ReadOnlyTable<'s, K, V> {
        ReadOnlyTable {
            storage,
            root_page,
            table_id,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn get(&self, key: &K) -> Result<Option<AccessGuard<'s, V>>, Error> {
        self.storage
            .get::<K, V>(self.table_id, key.as_bytes().as_ref(), self.root_page)
    }

    pub fn get_range<'a, T: RangeBounds<KR> + 'a, KR: AsRef<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error> {
        self.storage
            .get_range(self.table_id, range, self.root_page)
            .map(RangeIter::new)
    }

    pub fn get_range_reversed<'a, T: RangeBounds<KR> + 'a, KR: AsRef<K>>(
        &'a self,
        range: T,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        self.storage
            .get_range_reversed(self.table_id, range, self.root_page)
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

pub struct RangeIter<
    'a,
    T: RangeBounds<KR>,
    KR: AsRef<K>,
    K: RedbKey + ?Sized + 'a,
    V: RedbValue + ?Sized + 'a,
> {
    inner: BtreeRangeIter<'a, T, KR, K, V>,
}

impl<
        'a,
        T: RangeBounds<KR>,
        KR: AsRef<K>,
        K: RedbKey + ?Sized + 'a,
        V: RedbValue + ?Sized + 'a,
    > RangeIter<'a, T, KR, K, V>
{
    fn new(inner: BtreeRangeIter<'a, T, KR, K, V>) -> Self {
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
            let key = K::from_bytes(entry.key());
            let value = V::from_bytes(entry.value());
            Some((key, value))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::BtreeEntry;
    use crate::types::{
        AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
    };
    use crate::{Database, Error, ReadOnlyTable, Table};
    use std::cmp::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn len() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello2", b"world2").unwrap();
        table.insert(b"hi", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(table.len().unwrap(), 3);
    }

    #[test]
    fn multiple_tables() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"1").unwrap();
        let mut table2: Table<[u8], [u8]> = write_txn.open_table(b"2").unwrap();

        table.insert(b"hello", b"world").unwrap();
        table2.insert(b"hello", b"world2").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"1").unwrap();
        let table2: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"2").unwrap();
        assert_eq!(table.len().unwrap(), 1);
        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(table2.len().unwrap(), 1);
        assert_eq!(b"world2", table2.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };

        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert!(!table.is_empty().unwrap());
    }

    #[test]
    fn abort() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };

        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"aborted").unwrap();
        assert_eq!(b"aborted", table.get(b"hello").unwrap().unwrap().as_ref());
        write_txn.abort().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table(b"x");
        assert!(table.is_err());

        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(table.len().unwrap(), 1);
    }

    #[test]
    fn insert_overwrite() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());

        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"replaced").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(b"replaced", table.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn u64_type() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<u64, u64> = write_txn.open_table(b"x").unwrap();
        table.insert(&0, &1).unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<u64, u64> = read_txn.open_table(b"x").unwrap();
        assert_eq!(1, table.get(&0).unwrap().unwrap().to_value());
    }

    #[test]
    fn insert_reserve() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        let value = b"world";
        let mut reserved = table.insert_reserve(b"hello", value.len()).unwrap();
        reserved.as_mut().copy_from_slice(value);
        drop(reserved);
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(value, table.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn range_query_reversed() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            table.insert(&key, b"value").unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        let start = vec![3u8];
        let end = vec![7u8];
        let mut iter = table
            .get_range_reversed(start.as_slice()..end.as_slice())
            .unwrap();
        for i in (3..7u8).rev() {
            let entry = iter.next().unwrap();
            assert_eq!(&[i], entry.key());
            assert_eq!(b"value", entry.value());
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn custom_ordering() {
        struct ReverseKey(Vec<u8>);

        impl AsRef<ReverseKey> for ReverseKey {
            fn as_ref(&self) -> &ReverseKey {
                self
            }
        }

        impl RedbValue for ReverseKey {
            type View = RefLifetime<[u8]>;
            type ToBytes = RefAsBytesLifetime<[u8]>;

            fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
                data
            }

            fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
                &self.0
            }
        }

        impl RedbKey for ReverseKey {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                data2.cmp(data1)
            }
        }

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<ReverseKey, [u8]> = write_txn.open_table(b"x").unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            table.insert(&ReverseKey(key), b"value").unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<ReverseKey, [u8]> = read_txn.open_table(b"x").unwrap();
        let start = ReverseKey(vec![7u8]); // ReverseKey is used, so 7 < 3
        let end = ReverseKey(vec![3u8]);
        let mut iter = table.get_range(start..=end).unwrap();
        for i in (3..=7u8).rev() {
            let (key, value) = iter.next().unwrap();
            assert_eq!(&[i], key);
            assert_eq!(b"value", value);
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn delete() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello2", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(table.len().unwrap(), 2);

        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.remove(b"hello").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert!(table.get(b"hello").unwrap().is_none());
        assert_eq!(table.len().unwrap(), 1);
    }

    #[test]
    fn no_dirty_reads() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table(b"x");
        assert!(table.is_err());
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn read_isolation() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());

        let write_txn = db.begin_write().unwrap();
        let mut write_table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        write_table.remove(b"hello").unwrap();
        write_table.insert(b"hello2", b"world2").unwrap();
        write_table.insert(b"hello3", b"world3").unwrap();
        write_txn.commit().unwrap();

        let read_txn2 = db.begin_read().unwrap();
        let table2: ReadOnlyTable<[u8], [u8]> = read_txn2.open_table(b"x").unwrap();
        assert!(table2.get(b"hello").unwrap().is_none());
        assert_eq!(b"world2", table2.get(b"hello2").unwrap().unwrap().as_ref());
        assert_eq!(b"world3", table2.get(b"hello3").unwrap().unwrap().as_ref());
        assert_eq!(table2.len().unwrap(), 2);

        assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
        assert!(table.get(b"hello2").unwrap().is_none());
        assert!(table.get(b"hello3").unwrap().is_none());
        assert_eq!(table.len().unwrap(), 1);
    }
}
