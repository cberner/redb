use crate::error::Error;
use crate::storage::Storage;
use crate::transactions::WriteTransaction;
use crate::ReadOnlyTransaction;

pub struct Table<'mmap> {
    storage: &'mmap Storage,
}

impl<'mmap> Table<'mmap> {
    pub(in crate) fn new(storage: &'mmap Storage) -> Result<Table<'mmap>, Error> {
        Ok(Table { storage })
    }

    pub fn begin_write(&'_ mut self) -> Result<WriteTransaction<'mmap>, Error> {
        Ok(WriteTransaction::new(self.storage))
    }

    pub fn read_transaction(&'_ self) -> Result<ReadOnlyTransaction<'mmap>, Error> {
        Ok(ReadOnlyTransaction::new(self.storage))
    }
}

#[cfg(test)]
mod test {
    use crate::Database;
    use tempfile::NamedTempFile;

    #[test]
    fn len() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello2", b"world2").unwrap();
        write_txn.insert(b"hi", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(read_txn.len().unwrap(), 3);
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
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
        let mut table = db.open_table("").unwrap();
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
    fn insert_reserve() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        let value = b"world";
        let reserved = write_txn.insert_reserve(b"hello", value.len()).unwrap();
        reserved.copy_from_slice(value);
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(value, read_txn.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn delete() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

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
        let mut table = db.open_table("").unwrap();

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
        let mut table = db.open_table("").unwrap();

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
