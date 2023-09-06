use crate::sealed::Sealed;
use crate::tree_store::{
    AccessGuardMut, Btree, BtreeDrain, BtreeDrainFilter, BtreeMut, BtreeRangeIter, Checksum,
    PageHint, PageNumber, TransactionalMemory, MAX_VALUE_LENGTH,
};
use crate::types::{RedbKey, RedbValue, RedbValueMutInPlace};
use crate::Result;
use crate::{AccessGuard, StorageError, WriteTransaction};
use std::borrow::Borrow;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

/// Informational storage stats about a table
#[derive(Debug)]
pub struct TableStats {
    pub(crate) tree_height: u32,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
}

impl TableStats {
    /// Maximum traversal distance to reach the deepest (key, value) pair in the table
    pub fn tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Number of leaf pages that store user data
    pub fn leaf_pages(&self) -> u64 {
        self.leaf_pages
    }

    /// Number of branch pages in the btree that store user data
    pub fn branch_pages(&self) -> u64 {
        self.branch_pages
    }

    /// Number of bytes consumed by keys and values that have been inserted.
    /// Does not include indexing overhead
    pub fn stored_bytes(&self) -> u64 {
        self.stored_leaf_bytes
    }

    /// Number of bytes consumed by keys in internal branch pages, plus other metadata
    pub fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }

    /// Number of bytes consumed by fragmentation, both in data pages and internal metadata tables
    pub fn fragmented_bytes(&self) -> u64 {
        self.fragmented_bytes
    }
}

/// A table containing key-value mappings
pub struct Table<'db, 'txn, K: RedbKey + 'static, V: RedbValue + 'static> {
    name: String,
    system: bool,
    transaction: &'txn WriteTransaction<'db>,
    tree: BtreeMut<'txn, K, V>,
}

impl<'db, 'txn, K: RedbKey + 'static, V: RedbValue + 'static> Table<'db, 'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        system: bool,
        table_root: Option<(PageNumber, Checksum)>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        mem: &'db TransactionalMemory,
        transaction: &'txn WriteTransaction<'db>,
    ) -> Table<'db, 'txn, K, V> {
        Table {
            name: name.to_string(),
            system,
            transaction,
            tree: BtreeMut::new(table_root, mem, freed_pages),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.tree.print_debug(include_values)
    }

    /// Removes and returns the first key-value pair in the table
    pub fn pop_first(&mut self) -> Result<Option<(AccessGuard<K>, AccessGuard<V>)>> {
        // TODO: optimize this
        let first = self
            .iter()?
            .next()
            .map(|x| x.map(|(key, _)| K::as_bytes(&key.value()).as_ref().to_vec()));
        if let Some(owned_key) = first {
            let owned_key = owned_key?;
            let key = K::from_bytes(&owned_key);
            let value = self.remove(&key)?.unwrap();
            drop(key);
            Ok(Some((AccessGuard::with_owned_value(owned_key), value)))
        } else {
            Ok(None)
        }
    }

    /// Removes and returns the last key-value pair in the table
    pub fn pop_last(&mut self) -> Result<Option<(AccessGuard<K>, AccessGuard<V>)>> {
        // TODO: optimize this
        let last = self
            .iter()?
            .next_back()
            .map(|x| x.map(|(key, _)| K::as_bytes(&key.value()).as_ref().to_vec()));
        if let Some(owned_key) = last {
            let owned_key = owned_key?;
            let key = K::from_bytes(&owned_key);
            let value = self.remove(&key)?.unwrap();
            drop(key);
            Ok(Some((AccessGuard::with_owned_value(owned_key), value)))
        } else {
            Ok(None)
        }
    }

    /// Removes the specified range and returns the removed entries in an iterator
    ///
    /// The iterator will consume all items in the range on drop.
    pub fn drain<'a, KR>(&mut self, range: impl RangeBounds<KR> + 'a) -> Result<Drain<K, V>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree.drain(&range).map(Drain::new)
    }

    /// Applies `predicate` to all key-value pairs in the specified range. All entries for which
    /// `predicate` evaluates to `true` are removed and returned in an iterator
    ///
    /// The iterator will consume all items in the range matching the predicate on drop.
    pub fn drain_filter<'a, KR, F: for<'f> Fn(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result<DrainFilter<K, V, F>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .drain_filter(&range, predicate)
            .map(DrainFilter::new)
    }

    /// Insert mapping of the given key to the given value
    ///
    /// Returns the old value, if the key was present in the table
    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<Option<AccessGuard<V>>> {
        let value_len = V::as_bytes(value.borrow()).as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        self.tree.insert(key.borrow(), value.borrow())
    }

    /// Removes the given key
    ///
    /// Returns the old value, if the key was present in the table
    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<V>>>
    where
        K: 'a,
    {
        self.tree.remove(key.borrow())
    }
}

impl<'db, 'txn, K: RedbKey + 'static, V: RedbValueMutInPlace + 'static> Table<'db, 'txn, K, V> {
    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
        value_length: u32,
    ) -> Result<AccessGuardMut<V>>
    where
        K: 'a,
    {
        if value_length as usize > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length as usize));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        self.tree.insert_reserve(key.borrow(), value_length)
    }
}

impl<'db, 'txn, K: RedbKey + 'static, V: RedbValue + 'static> ReadableTable<K, V>
    for Table<'db, 'txn, K, V>
{
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<V>>>
    where
        K: 'a,
    {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<K, V>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree.range(&range).map(Range::new)
    }

    fn stats(&self) -> Result<TableStats> {
        let tree_stats = self.tree.stats()?;

        Ok(TableStats {
            tree_height: tree_stats.tree_height,
            leaf_pages: tree_stats.leaf_pages,
            branch_pages: tree_stats.branch_pages,
            stored_leaf_bytes: tree_stats.stored_leaf_bytes,
            metadata_bytes: tree_stats.metadata_bytes,
            fragmented_bytes: tree_stats.fragmented_bytes,
        })
    }

    fn len(&self) -> Result<u64> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<K: RedbKey, V: RedbValue> Sealed for Table<'_, '_, K, V> {}

impl<'db, 'txn, K: RedbKey + 'static, V: RedbValue + 'static> Drop for Table<'db, 'txn, K, V> {
    fn drop(&mut self) {
        self.transaction
            .close_table(&self.name, self.system, &self.tree);
    }
}

pub trait ReadableTable<K: RedbKey + 'static, V: RedbValue + 'static>: Sealed {
    /// Returns the value corresponding to the given key
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<V>>>
    where
        K: 'a;

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
    /// let db = Database::create(filename)?;
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
    /// let (key, value) = iter.next().unwrap()?;
    /// assert_eq!("a", key.value());
    /// assert_eq!(0, value.value());
    /// # Ok(())
    /// # }
    /// ```
    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<K, V>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a;

    /// Retrieves information about storage usage for the table
    fn stats(&self) -> Result<TableStats>;

    /// Returns the number of entries in the table
    fn len(&self) -> Result<u64>;

    /// Returns `true` if the table is empty
    fn is_empty(&self) -> Result<bool>;

    /// Returns a double-ended iterator over all elements in the table
    fn iter(&self) -> Result<Range<K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }
}

/// A read-only table
pub struct ReadOnlyTable<'txn, K: RedbKey + 'static, V: RedbValue + 'static> {
    tree: Btree<'txn, K, V>,
}

impl<'txn, K: RedbKey + 'static, V: RedbValue + 'static> ReadOnlyTable<'txn, K, V> {
    pub(crate) fn new(
        root_page: Option<(PageNumber, Checksum)>,
        hint: PageHint,
        mem: &'txn TransactionalMemory,
    ) -> Result<ReadOnlyTable<'txn, K, V>> {
        Ok(ReadOnlyTable {
            tree: Btree::new(root_page, hint, mem)?,
        })
    }
}

impl<'txn, K: RedbKey + 'static, V: RedbValue + 'static> ReadableTable<K, V>
    for ReadOnlyTable<'txn, K, V>
{
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<V>>>
    where
        K: 'a,
    {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<K, V>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree.range(&range).map(Range::new)
    }

    fn stats(&self) -> Result<TableStats> {
        let tree_stats = self.tree.stats()?;

        Ok(TableStats {
            tree_height: tree_stats.tree_height,
            leaf_pages: tree_stats.leaf_pages,
            branch_pages: tree_stats.branch_pages,
            stored_leaf_bytes: tree_stats.stored_leaf_bytes,
            metadata_bytes: tree_stats.metadata_bytes,
            fragmented_bytes: tree_stats.fragmented_bytes,
        })
    }

    fn len(&self) -> Result<u64> {
        self.tree.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.len().map(|x| x == 0)
    }
}

impl<K: RedbKey, V: RedbValue> Sealed for ReadOnlyTable<'_, K, V> {}

pub struct Drain<'a, K: RedbKey + 'static, V: RedbValue + 'static> {
    inner: BtreeDrain<'a, K, V>,
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Drain<'a, K, V> {
    fn new(inner: BtreeDrain<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Iterator for Drain<'a, K, V> {
    type Item = Result<(AccessGuard<'a, K>, AccessGuard<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.inner.next()?;
        Some(entry.map(|entry| {
            let (page, key_range, value_range) = entry.into_raw();
            let key = AccessGuard::with_page(page.clone(), key_range);
            let value = AccessGuard::with_page(page, value_range);
            (key, value)
        }))
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> DoubleEndedIterator for Drain<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let entry = self.inner.next_back()?;
        Some(entry.map(|entry| {
            let (page, key_range, value_range) = entry.into_raw();
            let key = AccessGuard::with_page(page.clone(), key_range);
            let value = AccessGuard::with_page(page, value_range);
            (key, value)
        }))
    }
}

pub struct DrainFilter<
    'a,
    K: RedbKey + 'static,
    V: RedbValue + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    inner: BtreeDrainFilter<'a, K, V, F>,
}

impl<
        'a,
        K: RedbKey + 'static,
        V: RedbValue + 'static,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    > DrainFilter<'a, K, V, F>
{
    fn new(inner: BtreeDrainFilter<'a, K, V, F>) -> Self {
        Self { inner }
    }
}

impl<
        'a,
        K: RedbKey + 'static,
        V: RedbValue + 'static,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    > Iterator for DrainFilter<'a, K, V, F>
{
    type Item = Result<(AccessGuard<'a, K>, AccessGuard<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.inner.next()?;
        Some(entry.map(|entry| {
            let (page, key_range, value_range) = entry.into_raw();
            let key = AccessGuard::with_page(page.clone(), key_range);
            let value = AccessGuard::with_page(page, value_range);
            (key, value)
        }))
    }
}

impl<
        'a,
        K: RedbKey + 'static,
        V: RedbValue + 'static,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    > DoubleEndedIterator for DrainFilter<'a, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let entry = self.inner.next_back()?;
        Some(entry.map(|entry| {
            let (page, key_range, value_range) = entry.into_raw();
            let key = AccessGuard::with_page(page.clone(), key_range);
            let value = AccessGuard::with_page(page, value_range);
            (key, value)
        }))
    }
}

pub struct Range<'a, K: RedbKey + 'static, V: RedbValue + 'static> {
    inner: BtreeRangeIter<'a, K, V>,
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Range<'a, K, V> {
    fn new(inner: BtreeRangeIter<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Iterator for Range<'a, K, V> {
    type Item = Result<(AccessGuard<'a, K>, AccessGuard<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|x| {
            x.map(|entry| {
                let (page, key_range, value_range) = entry.into_raw();
                let key = AccessGuard::with_page(page.clone(), key_range);
                let value = AccessGuard::with_page(page, value_range);
                (key, value)
            })
        })
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> DoubleEndedIterator for Range<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|x| {
            x.map(|entry| {
                let (page, key_range, value_range) = entry.into_raw();
                let key = AccessGuard::with_page(page.clone(), key_range);
                let value = AccessGuard::with_page(page, value_range);
                (key, value)
            })
        })
    }
}
