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

    pub fn len(&self) -> Result<usize, Error> {
        self.storage.len()
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        self.storage.len().map(|x| x == 0)
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
        assert_eq!(table.len().unwrap(), 3);
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        assert!(table.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        assert!(!table.is_empty().unwrap());
    }
}
