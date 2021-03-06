use crate::btree::BtreeRangeIter;
use crate::error::Error;
use crate::storage::{AccessGuard, Storage};
use std::collections::{HashMap, HashSet};
use std::ops::RangeBounds;

pub struct WriteTransaction<'mmap> {
    storage: &'mmap Storage,
    table_id: u64,
    added: HashMap<Vec<u8>, Vec<u8>>,
    removed: HashSet<Vec<u8>>,
}

impl<'mmap> WriteTransaction<'mmap> {
    pub(in crate) fn new(table_id: u64, storage: &'mmap Storage) -> WriteTransaction<'mmap> {
        WriteTransaction {
            storage,
            table_id,
            added: HashMap::new(),
            removed: HashSet::new(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.removed.remove(key);
        self.added.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve(&mut self, key: &[u8], value_length: usize) -> Result<&mut [u8], Error> {
        self.removed.remove(key);
        self.added.insert(key.to_vec(), vec![0; value_length]);
        Ok(self.added.get_mut(key).unwrap())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<AccessGuard>, Error> {
        if let Some(value) = self.added.get(key) {
            return Ok(Some(AccessGuard::Local(value)));
        }
        self.storage
            .get(self.table_id, key, self.storage.get_root_page_number())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        self.added.remove(key);
        self.removed.insert(key.to_vec());
        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        self.storage.bulk_insert(self.table_id, self.added)?;
        for key in self.removed.iter() {
            self.storage.remove(self.table_id, key)?;
        }
        self.storage.fsync()?;
        Ok(())
    }

    pub fn abort(self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct ReadOnlyTransaction<'mmap> {
    storage: &'mmap Storage,
    root_page: Option<u64>,
    table_id: u64,
}

impl<'mmap> ReadOnlyTransaction<'mmap> {
    pub(in crate) fn new(table_id: u64, storage: &'mmap Storage) -> ReadOnlyTransaction<'mmap> {
        let root_page = storage.get_root_page_number();
        ReadOnlyTransaction {
            storage,
            root_page,
            table_id,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<AccessGuard<'mmap>>, Error> {
        self.storage.get(self.table_id, key, self.root_page)
    }

    pub fn get_range<'a, T: RangeBounds<&'a [u8]>>(
        &'a self,
        range: T,
    ) -> Result<BtreeRangeIter<T>, Error> {
        self.storage.get_range(self.table_id, range, self.root_page)
    }

    pub fn get_range_reversed<'a, T: RangeBounds<&'a [u8]>>(
        &'a self,
        range: T,
    ) -> Result<BtreeRangeIter<T>, Error> {
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
