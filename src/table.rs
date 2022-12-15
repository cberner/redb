use crate::tree_store::{
    AccessGuardMut, Btree, BtreeMut, BtreeRangeIter, Checksum, PageNumber, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue};
use crate::Result;
use crate::{AccessGuard, WriteTransaction};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::ops::RangeBounds;
use std::rc::Rc;

/// A table containing key-value mappings
pub struct Table<'db, 'txn, K: RedbKey + ?Sized + 'txn, V: RedbValue + ?Sized + 'txn> {
    name: String,
    transaction: &'txn WriteTransaction<'db>,
    tree: BtreeMut<'txn, K, V>,
}

impl<'db, 'txn, K: RedbKey + ?Sized + 'txn, V: RedbValue + ?Sized + 'txn> Table<'db, 'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<(PageNumber, Checksum)>,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
        mem: &'db TransactionalMemory,
        transaction: &'txn WriteTransaction<'db>,
    ) -> Table<'db, 'txn, K, V> {
        Table {
            name: name.to_string(),
            transaction,
            tree: BtreeMut::new(table_root, mem, freed_pages),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) {
        self.tree.print_debug(include_values);
    }

    /// Insert mapping of the given key to the given value
    ///
    /// Returns the old value, if the key was present in the table
    pub fn insert<'a, 'b: 'a, AK, AV>(
        &mut self,
        key: &'a AK,
        value: &'a AV,
    ) -> Result<Option<AccessGuard<V>>>
    where
        K: 'b,
        V: 'b,
        AK: Borrow<K::RefBaseType<'b>> + ?Sized,
        AV: Borrow<V::RefBaseType<'b>> + ?Sized,
    {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert(key.borrow(), value.borrow()) }
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    // TODO: return type should be V, not [u8]
    pub fn insert_reserve<'a, 'b: 'a, AK>(
        &mut self,
        key: &'a AK,
        value_length: usize,
    ) -> Result<AccessGuardMut<K, &[u8]>>
    where
        K: 'b,
        AK: Borrow<K::RefBaseType<'b>> + ?Sized,
    {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert_reserve(key.borrow(), value_length) }
    }

    /// Removes the given key
    ///
    /// Returns the old value, if the key was present in the table
    pub fn remove<'a, 'b: 'a, AK>(&mut self, key: &'a AK) -> Result<Option<AccessGuard<V>>>
    where
        K: 'b,
        AK: Borrow<K::RefBaseType<'b>> + ?Sized,
    {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.remove(key.borrow()) }
    }
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for Table<'db, 'txn, K, V>
{
    fn get<'a, 'b: 'a, AK>(&self, key: &'a AK) -> Result<Option<V::SelfType<'_>>>
    where
        K: 'b,
        AK: Borrow<K::RefBaseType<'b>> + ?Sized,
    {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&'a self, range: impl RangeBounds<KR> + 'a) -> Result<RangeIter<'a, K, V>>
    where
        K: 'a,
        KR: Borrow<K::RefBaseType<'a>> + ?Sized + 'a,
    {
        self.tree.range(range).map(RangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<'db, 'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Drop for Table<'db, 'txn, K, V> {
    fn drop(&mut self) {
        self.transaction.close_table(&self.name, &mut self.tree);
    }
}

pub trait ReadableTable<K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    /// Returns the value corresponding to the given key
    fn get<'a, 'b: 'a, AK>(&self, key: &'a AK) -> Result<Option<V::SelfType<'_>>>
    where
        K: 'b,
        AK: Borrow<K::RefBaseType<'b>> + ?Sized;

    /// Returns a double-ended iterator over a range of elements in the table
    ///
    /// # Examples
    ///
    /// Usage:
    /// ```rust
    /// use redb::*;
    /// # use tempfile::NamedTempFile;
    /// const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");
    ///
    /// # fn main() -> Result<(), Error> {
    /// # let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    /// # let filename = tmpfile.path();
    /// let db = unsafe { Database::create(filename)? };
    /// let write_txn = db.begin_write()?;
    /// {
    ///     let mut table = write_txn.open_table(TABLE)?;
    ///     table.insert("a", &0)?;
    ///     table.insert("b", &1)?;
    ///     table.insert("c", &2)?;
    /// }
    /// write_txn.commit()?;
    ///
    /// let read_txn = db.begin_read()?;
    /// let table = read_txn.open_table(TABLE)?;
    /// let mut iter = table.range("a".."c")?;
    /// let (key, value) = iter.next().unwrap();
    /// assert_eq!("a", key.to_value());
    /// assert_eq!(0, value.to_value());
    /// # Ok(())
    /// # }
    /// ```
    fn range<'a, KR>(&'a self, range: impl RangeBounds<KR> + 'a) -> Result<RangeIter<'a, K, V>>
    where
        K: 'a,
        KR: Borrow<K::RefBaseType<'a>> + ?Sized + 'a;

    /// Returns the number of entries in the table
    fn len(&self) -> Result<usize>;

    /// Returns `true` if the table is empty
    fn is_empty(&self) -> Result<bool>;

    /// Returns a double-ended iterator over all elements in the table
    fn iter(&self) -> Result<RangeIter<K, V>> {
        self.range::<K::RefBaseType<'_>>(..)
    }
}

/// A read-only table
pub struct ReadOnlyTable<'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    tree: Btree<'txn, K, V>,
}

impl<'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadOnlyTable<'txn, K, V> {
    pub(crate) fn new(
        root_page: Option<(PageNumber, Checksum)>,
        mem: &'txn TransactionalMemory,
    ) -> ReadOnlyTable<'txn, K, V> {
        ReadOnlyTable {
            tree: Btree::new(root_page, mem),
        }
    }
}

impl<'txn, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for ReadOnlyTable<'txn, K, V>
{
    fn get<'a, 'b: 'a, AK>(&self, key: &'a AK) -> Result<Option<V::SelfType<'_>>>
    where
        K: 'b,
        AK: Borrow<K::RefBaseType<'b>> + ?Sized,
    {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&'a self, range: impl RangeBounds<KR> + 'a) -> Result<RangeIter<'a, K, V>>
    where
        K: 'a,
        KR: Borrow<K::RefBaseType<'a>> + ?Sized + 'a,
    {
        self.tree.range(range).map(RangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

pub struct RangeIter<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> {
    inner: BtreeRangeIter<'a, K, V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> RangeIter<'a, K, V> {
    fn new(inner: BtreeRangeIter<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> Iterator for RangeIter<'a, K, V> {
    type Item = (AccessGuard<'a, K>, AccessGuard<'a, V>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next() {
            // TODO: optimize out these copies
            let key = AccessGuard::with_owned_value(entry.key().to_vec());
            let value = AccessGuard::with_owned_value(entry.value().to_vec());
            Some((key, value))
        } else {
            None
        }
    }
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> DoubleEndedIterator
    for RangeIter<'a, K, V>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next_back() {
            // TODO: optimize out these copies
            let key = AccessGuard::with_owned_value(entry.key().to_vec());
            let value = AccessGuard::with_owned_value(entry.value().to_vec());
            Some((key, value))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::types::{RedbKey, RedbValue};
    use crate::{Database, ReadableTable, TableDefinition};
    use std::cmp::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn custom_ordering() {
        #[derive(Debug)]
        struct ReverseKey(Vec<u8>);

        impl RedbValue for ReverseKey {
            type SelfType<'a> = ReverseKey
            where
                Self: 'a;
            type RefBaseType<'a> = ReverseKey
            where
                Self: 'a;
            type AsBytes<'a> = &'a [u8]
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                None
            }

            fn from_bytes<'a>(data: &'a [u8]) -> ReverseKey
            where
                Self: 'a,
            {
                ReverseKey(data.to_vec())
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::RefBaseType<'b>) -> &'a [u8]
            where
                Self: 'a,
                Self: 'b,
            {
                &value.0
            }

            fn redb_type_name() -> String {
                "ReverseKey".to_string()
            }
        }

        impl RedbKey for ReverseKey {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                data2.cmp(data1)
            }
        }

        let definition: TableDefinition<ReverseKey, &[u8]> = TableDefinition::new("x");

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::create(tmpfile.path()).unwrap() };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(definition).unwrap();
            for i in 0..10u8 {
                let key = vec![i];
                table.insert(&ReverseKey(key), b"value").unwrap();
            }
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(definition).unwrap();
        let start = ReverseKey(vec![7u8]); // ReverseKey is used, so 7 < 3
        let end = ReverseKey(vec![3u8]);
        let mut iter = table.range(start..=end).unwrap();
        for i in (3..=7u8).rev() {
            let (key, value) = iter.next().unwrap();
            assert_eq!(&[i], key.to_value().0.as_slice());
            assert_eq!(b"value", value.to_value());
        }
        assert!(iter.next().is_none());
    }
}
