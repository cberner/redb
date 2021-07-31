use crate::error::Error;
use crate::storage::Storage;
use crate::transactions::WriteTransaction;
use crate::types::{RedbKey, RedbValue};
use crate::ReadOnlyTransaction;
use std::marker::PhantomData;

pub struct Table<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'mmap Storage,
    table_id: u64,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Table<'mmap, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        storage: &'mmap Storage,
    ) -> Result<Table<'mmap, K, V>, Error> {
        Ok(Table {
            storage,
            table_id,
            _key_type: Default::default(),
            _value_type: Default::default(),
        })
    }

    pub fn begin_write(&'_ mut self) -> Result<WriteTransaction<'mmap, K, V>, Error> {
        Ok(WriteTransaction::new(self.table_id, self.storage))
    }

    pub fn read_transaction(&'_ self) -> Result<ReadOnlyTransaction<'mmap, K, V>, Error> {
        Ok(ReadOnlyTransaction::new(self.table_id, self.storage))
    }
}

#[cfg(test)]
mod test {
    use crate::btree::BtreeEntry;
    use crate::types::{
        AsBytesWithLifetime, RedbKey, RedbValue, RefAsBytesLifetime, RefLifetime, WithLifetime,
    };
    use crate::{Database, Table};
    use std::cmp::Ordering;
    use tempfile::NamedTempFile;

    #[test]
    fn len() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello2", b"world2").unwrap();
        write_txn.insert(b"hi", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(read_txn.len().unwrap(), 3);
    }

    #[test]
    fn multiple_tables() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"1").unwrap();
        let mut table2: Table<[u8], [u8]> = db.open_table(b"2").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let mut write_txn2 = table2.begin_write().unwrap();
        write_txn2.insert(b"hello", b"world2").unwrap();
        write_txn2.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert_eq!(read_txn.len().unwrap(), 1);
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        let read_txn2 = table2.read_transaction().unwrap();
        assert_eq!(read_txn2.len().unwrap(), 1);
        assert_eq!(
            b"world2",
            read_txn2.get(b"hello").unwrap().unwrap().as_ref()
        );
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(!read_txn.is_empty().unwrap());
    }

    #[test]
    fn abort() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"aborted").unwrap();
        assert_eq!(
            b"aborted",
            write_txn.get(b"hello").unwrap().unwrap().as_ref()
        );
        write_txn.abort().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(read_txn.len().unwrap(), 1);
    }

    #[test]
    fn insert_overwrite() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"replaced").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(
            b"replaced",
            read_txn.get(b"hello").unwrap().unwrap().as_ref()
        );
    }

    #[test]
    fn u64_type() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<u64, u64> = db.open_table(b"x").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(&0, &1).unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(1, read_txn.get(&0).unwrap().unwrap().to_value());
    }

    #[test]
    fn insert_reserve() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        let value = b"world";
        let mut reserved = write_txn.insert_reserve(b"hello", value.len()).unwrap();
        reserved.as_mut().copy_from_slice(value);
        drop(reserved);
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(value, read_txn.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn range_query() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            write_txn.insert(&key, b"value").unwrap();
        }
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        let start = vec![3u8];
        let end = vec![7u8];
        let mut iter = read_txn
            .get_range(start.as_slice()..end.as_slice())
            .unwrap();
        for i in 3..7u8 {
            let entry = iter.next().unwrap();
            assert_eq!(&[i], entry.key());
            assert_eq!(b"value", entry.value());
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn range_query_reversed() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            write_txn.insert(&key, b"value").unwrap();
        }
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        let start = vec![3u8];
        let end = vec![7u8];
        let mut iter = read_txn
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
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<ReverseKey, [u8]> = db.open_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            write_txn.insert(&ReverseKey(key), b"value").unwrap();
        }
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        let start = ReverseKey(vec![7u8]); // ReverseKey is used, so 7 < 3
        let end = ReverseKey(vec![3u8]);
        let mut iter = read_txn.get_range(&start..=&end).unwrap();
        for i in (3..=7u8).rev() {
            let entry = iter.next().unwrap();
            assert_eq!(&[i], entry.key());
            assert_eq!(b"value", entry.value());
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn delete() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello2", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(read_txn.len().unwrap(), 2);

        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove(b"hello").unwrap();
        write_txn.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.get(b"hello").unwrap().is_none());
        assert_eq!(read_txn.len().unwrap(), 1);
    }

    #[test]
    fn no_dirty_reads() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.get(b"hello").unwrap().is_none());
        assert!(read_txn.is_empty().unwrap());
        write_txn.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn read_isolation() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());

        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove(b"hello").unwrap();
        write_txn.insert(b"hello2", b"world2").unwrap();
        write_txn.insert(b"hello3", b"world3").unwrap();
        write_txn.commit().unwrap();

        let read_txn2 = table.read_transaction().unwrap();
        assert!(read_txn2.get(b"hello").unwrap().is_none());
        assert_eq!(
            b"world2",
            read_txn2.get(b"hello2").unwrap().unwrap().as_ref()
        );
        assert_eq!(
            b"world3",
            read_txn2.get(b"hello3").unwrap().unwrap().as_ref()
        );
        assert_eq!(read_txn2.len().unwrap(), 2);

        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert!(read_txn.get(b"hello2").unwrap().is_none());
        assert!(read_txn.get(b"hello3").unwrap().is_none());
        assert_eq!(read_txn.len().unwrap(), 1);
    }
}
