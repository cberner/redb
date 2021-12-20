use crate::error::Error;
use crate::tree_store::{AccessGuard, AccessGuardMut, BtreeRangeIter, NodeHandle, Storage};
use crate::types::{RedbKey, RedbValue};
use std::marker::PhantomData;
use std::ops::RangeBounds;

pub struct WriteTransaction<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'mmap Storage,
    table_id: u64,
    transaction_id: u128,
    root_page: Option<NodeHandle>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> WriteTransaction<'mmap, K, V> {
    pub(in crate) fn new(table_id: u64, storage: &'mmap Storage) -> WriteTransaction<'mmap, K, V> {
        let transaction_id = storage.allocate_write_transaction();
        WriteTransaction {
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
        self.root_page = Some(self.storage.insert::<K>(
            self.table_id,
            key.as_bytes().as_ref(),
            value.as_bytes().as_ref(),
            self.transaction_id,
            self.root_page,
        )?);
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
            self.root_page,
        )?;
        self.root_page = Some(root_page);
        Ok(guard)
    }

    pub fn get(&self, key: &K) -> Result<Option<AccessGuard<V>>, Error> {
        self.storage
            .get::<K, V>(self.table_id, key.as_bytes().as_ref(), self.root_page)
    }

    pub fn remove(&mut self, key: &K) -> Result<(), Error> {
        self.root_page = self.storage.remove::<K>(
            self.table_id,
            key.as_bytes().as_ref(),
            self.transaction_id,
            self.root_page,
        )?;
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

pub struct ReadOnlyTransaction<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    storage: &'mmap Storage,
    root_page: Option<NodeHandle>,
    table_id: u64,
    transaction_id: u128,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> ReadOnlyTransaction<'mmap, K, V> {
    pub(in crate) fn new(
        table_id: u64,
        storage: &'mmap Storage,
    ) -> ReadOnlyTransaction<'mmap, K, V> {
        let root_page = storage.get_root_page_number();
        let transaction_id = storage.allocate_read_transaction();
        ReadOnlyTransaction {
            storage,
            root_page,
            table_id,
            transaction_id,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub fn get(&self, key: &K) -> Result<Option<AccessGuard<'mmap, V>>, Error> {
        self.storage
            .get::<K, V>(self.table_id, key.as_bytes().as_ref(), self.root_page)
    }

    pub fn get_range<'a, T: RangeBounds<KR> + 'a, KR: AsRef<K>>(
        &'a self,
        range: T,
    ) -> Result<BtreeRangeIter<T, KR, K, V>, Error> {
        self.storage.get_range(self.table_id, range, self.root_page)
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

impl<'mmap, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Drop for ReadOnlyTransaction<'mmap, K, V> {
    fn drop(&mut self) {
        self.storage
            .deallocate_read_transaction(self.transaction_id);
    }
}
