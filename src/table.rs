use crate::error::Error;
use crate::tree_store::{AccessGuardMut, BtreeEntry, BtreeRangeIter, PageNumber, Storage};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::AccessGuard;
use std::borrow::Borrow;
use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::rc::Rc;

pub struct Table<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'s Storage,
    transaction_id: u128,
    table_root: Rc<Cell<Option<PageNumber>>>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Table<'s, K, V> {
    pub(in crate) fn new(
        transaction_id: u128,
        table_root: Rc<Cell<Option<PageNumber>>>,
        storage: &'s Storage,
    ) -> Table<'s, K, V> {
        Table {
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
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        let root_page = unsafe {
            self.storage.insert::<K>(
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
                self.transaction_id,
                self.table_root.get(),
            )
        }?;
        self.table_root.set(Some(root_page));
        Ok(())
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<AccessGuardMut, Error> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        let (root_page, guard) = unsafe {
            self.storage.insert_reserve::<K>(
                key.as_bytes().as_ref(),
                value_length,
                self.transaction_id,
                self.table_root.get(),
            )
        }?;
        self.table_root.set(Some(root_page));
        Ok(guard)
    }

    pub fn remove(&mut self, key: &K) -> Result<bool, Error> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        let (root_page, found) = unsafe {
            self.storage.remove::<K>(
                key.as_bytes().as_ref(),
                self.transaction_id,
                self.table_root.get(),
            )
        }?;
        self.table_root.set(root_page);
        Ok(found)
    }
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V> for Table<'s, K, V> {
    fn get(&self, key: &K) -> Result<Option<AccessGuard<V>>, Error> {
        self.storage
            .get::<K, V>(key.as_bytes().as_ref(), self.table_root.get())
    }

    fn range<'a, T: RangeBounds<KR> + 'a, KR: Borrow<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error> {
        self.storage
            .get_range(range, self.table_root.get())
            .map(RangeIter::new)
    }

    fn range_reversed<'a, T: RangeBounds<KR> + 'a, KR: Borrow<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error> {
        self.storage
            .get_range_reversed(range, self.table_root.get())
            .map(RangeIter::new)
    }

    fn len(&self) -> Result<usize, Error> {
        self.storage.len(self.table_root.get())
    }

    fn is_empty(&self) -> Result<bool, Error> {
        self.storage.len(self.table_root.get()).map(|x| x == 0)
    }
}

pub trait ReadableTable<K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    fn get(&self, key: &K) -> Result<Option<AccessGuard<V>>, Error>;

    fn range<'a, T: RangeBounds<KR> + 'a, KR: Borrow<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error>;

    fn range_reversed<'a, T: RangeBounds<KR> + 'a, KR: Borrow<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error>;

    fn len(&self) -> Result<usize, Error>;

    fn is_empty(&self) -> Result<bool, Error>;
}

pub struct ReadOnlyTable<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'s Storage,
    table_root: Option<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadOnlyTable<'s, K, V> {
    pub(in crate) fn new(
        root_page: Option<PageNumber>,
        storage: &'s Storage,
    ) -> ReadOnlyTable<'s, K, V> {
        ReadOnlyTable {
            storage,
            table_root: root_page,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for ReadOnlyTable<'s, K, V>
{
    fn get(&self, key: &K) -> Result<Option<AccessGuard<V>>, Error> {
        self.storage
            .get::<K, V>(key.as_bytes().as_ref(), self.table_root)
    }

    fn range<'a, T: RangeBounds<KR> + 'a, KR: Borrow<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error> {
        self.storage
            .get_range(range, self.table_root)
            .map(RangeIter::new)
    }

    fn range_reversed<'a, T: RangeBounds<KR> + 'a, KR: Borrow<K>>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<T, KR, K, V>, Error> {
        self.storage
            .get_range_reversed(range, self.table_root)
            .map(RangeIter::new)
    }

    fn len(&self) -> Result<usize, Error> {
        self.storage.len(self.table_root)
    }

    fn is_empty(&self) -> Result<bool, Error> {
        self.storage.len(self.table_root).map(|x| x == 0)
    }
}

pub struct RangeIter<
    'a,
    T: RangeBounds<KR>,
    KR: Borrow<K>,
    K: RedbKey + ?Sized + 'a,
    V: RedbValue + ?Sized + 'a,
> {
    inner: BtreeRangeIter<'a, T, KR, K, V>,
}

impl<
        'a,
        T: RangeBounds<KR>,
        KR: Borrow<K>,
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
    use crate::types::{
        AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
    };
    use crate::{Database, ReadOnlyTable, ReadableTable, Table};
    use std::cmp::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn custom_ordering() {
        struct ReverseKey(Vec<u8>);

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
        let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<ReverseKey, [u8]> = write_txn.open_table("x").unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            table.insert(&ReverseKey(key), b"value").unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table: ReadOnlyTable<ReverseKey, [u8]> = read_txn.open_table("x").unwrap();
        let start = ReverseKey(vec![7u8]); // ReverseKey is used, so 7 < 3
        let end = ReverseKey(vec![3u8]);
        let mut iter = table.range(start..=end).unwrap();
        for i in (3..=7u8).rev() {
            let (key, value) = iter.next().unwrap();
            assert_eq!(&[i], key);
            assert_eq!(b"value", value);
        }
        assert!(iter.next().is_none());
    }
}
