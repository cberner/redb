use crate::tree_store::{
    AccessGuardMut, Btree, BtreeMut, BtreeRangeIter, PageNumber, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Result;
use crate::{AccessGuard, WriteTransaction};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::ops::RangeBounds;
use std::rc::Rc;

pub struct Table<'s, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    name: String,
    transaction: &'t WriteTransaction<'s>,
    tree: BtreeMut<'t, K, V>,
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Table<'s, 't, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<PageNumber>,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
        mem: &'s TransactionalMemory,
        transaction: &'t WriteTransaction<'s>,
    ) -> Table<'s, 't, K, V> {
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

    pub fn insert(&mut self, key: &K, value: &V) -> Result {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert(key, value) }
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve(&mut self, key: &K, value_length: usize) -> Result<AccessGuardMut> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.insert_reserve(key, value_length) }
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        // Safety: No other references to this table can exist.
        // Tables can only be opened mutably in one location (see Error::TableAlreadyOpen),
        // and we borrow &mut self.
        unsafe { self.tree.remove(key) }
    }
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for Table<'s, 't, K, V>
{
    fn get(&self, key: &K) -> Result<Option<<<V as RedbValue>::View as WithLifetime>::Out>> {
        self.tree.get(key)
    }

    fn range<'a, T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<K, V>> {
        self.tree.range(range).map(RangeIter::new)
    }

    fn len(&self) -> Result<usize> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<'s, 't, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Drop for Table<'s, 't, K, V> {
    fn drop(&mut self) {
        self.transaction.close_table(&self.name, &mut self.tree);
    }
}

pub trait ReadableTable<K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    fn get(&self, key: &K) -> Result<Option<<<V as RedbValue>::View as WithLifetime>::Out>>;

    fn range<'a, T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<K, V>>;

    fn len(&self) -> Result<usize>;

    fn is_empty(&self) -> Result<bool>;
}

pub struct ReadOnlyTable<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    tree: Btree<'s, K, V>,
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadOnlyTable<'s, K, V> {
    pub(crate) fn new(
        root_page: Option<PageNumber>,
        mem: &'s TransactionalMemory,
    ) -> ReadOnlyTable<'s, K, V> {
        ReadOnlyTable {
            tree: Btree::new(root_page, mem),
        }
    }
}

impl<'s, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadableTable<K, V>
    for ReadOnlyTable<'s, K, V>
{
    fn get(&self, key: &K) -> Result<Option<<<V as RedbValue>::View as WithLifetime>::Out>> {
        self.tree.get(key)
    }

    fn range<'a, T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &'a self,
        range: T,
    ) -> Result<RangeIter<K, V>> {
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

    pub fn rev(self) -> Self {
        Self::new(self.inner.reverse())
    }
}

#[cfg(test)]
mod test {
    use crate::types::{
        AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
    };
    use crate::{Database, ReadableTable, TableDefinition};
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

            fn redb_type_name() -> &'static str {
                "ReverseKey"
            }
        }

        impl RedbKey for ReverseKey {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                data2.cmp(data1)
            }
        }

        let definition: TableDefinition<ReverseKey, [u8]> = TableDefinition::new("x");

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
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
            assert_eq!(&[i], key);
            assert_eq!(b"value", value);
        }
        assert!(iter.next().is_none());
    }
}
