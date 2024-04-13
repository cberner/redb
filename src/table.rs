use crate::db::TransactionGuard;
use crate::sealed::Sealed;
use crate::tree_store::{
    AccessGuardMut, Btree, BtreeExtractIf, BtreeHeader, BtreeMut, BtreeRangeIter, PageHint,
    PageNumber, RawBtree, TransactionalMemory, MAX_VALUE_LENGTH,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, StorageError, WriteTransaction};
use crate::{Result, TableHandle};
use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
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
pub struct Table<'txn, K: Key + 'static, V: Value + 'static> {
    name: String,
    transaction: &'txn WriteTransaction,
    tree: BtreeMut<'txn, K, V>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for Table<'_, K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<'txn, K: Key + 'static, V: Value + 'static> Table<'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        mem: Arc<TransactionalMemory>,
        transaction: &'txn WriteTransaction,
    ) -> Table<'txn, K, V> {
        Table {
            name: name.to_string(),
            transaction,
            tree: BtreeMut::new(
                table_root,
                transaction.transaction_guard(),
                mem,
                freed_pages,
            ),
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

    /// Applies `predicate` to all key-value pairs. All entries for which
    /// `predicate` evaluates to `true` are returned in an iterator, and those which are read from the iterator are removed
    ///
    /// Note: values not read from the iterator will not be removed
    pub fn extract_if<F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        predicate: F,
    ) -> Result<ExtractIf<K, V, F>> {
        self.extract_from_if::<K::SelfType<'_>, F>(.., predicate)
    }

    /// Applies `predicate` to all key-value pairs in the specified range. All entries for which
    /// `predicate` evaluates to `true` are returned in an iterator, and those which are read from the iterator are removed
    ///
    /// Note: values not read from the iterator will not be removed
    pub fn extract_from_if<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result<ExtractIf<K, V, F>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .extract_from_if(&range, predicate)
            .map(ExtractIf::new)
    }

    /// Applies `predicate` to all key-value pairs. All entries for which
    /// `predicate` evaluates to `false` are removed.
    ///
    pub fn retain<F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        predicate: F,
    ) -> Result {
        self.tree.retain_in::<K::SelfType<'_>, F>(predicate, ..)
    }

    /// Applies `predicate` to all key-value pairs in the range `start..end`. All entries for which
    /// `predicate` evaluates to `false` are removed.
    ///
    pub fn retain_in<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree.retain_in(predicate, range)
    }

    /// Insert mapping of the given key to the given value
    ///
    /// If key is already present it is replaced
    ///
    /// Returns the old value, if the key was present in the table, otherwise None is returned
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
    ) -> Result<Option<AccessGuard<V>>> {
        self.tree.remove(key.borrow())
    }
}

impl<'txn, K: Key + 'static, V: MutInPlaceValue + 'static> Table<'txn, K, V> {
    /// Reserve space to insert a key-value pair
    ///
    /// If key is already present it is replaced
    ///
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
        value_length: u32,
    ) -> Result<AccessGuardMut<V>> {
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

impl<'txn, K: Key + 'static, V: Value + 'static> ReadableTableMetadata for Table<'txn, K, V> {
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
}

impl<'txn, K: Key + 'static, V: Value + 'static> ReadableTable<K, V> for Table<'txn, K, V> {
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<V>>> {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction.transaction_guard()))
    }
}

impl<K: Key, V: Value> Sealed for Table<'_, K, V> {}

impl<'txn, K: Key + 'static, V: Value + 'static> Drop for Table<'txn, K, V> {
    fn drop(&mut self) {
        self.transaction.close_table(
            &self.name,
            &self.tree,
            self.tree.get_root().map(|x| x.length).unwrap_or_default(),
        );
    }
}

fn debug_helper<K: Key + 'static, V: Value + 'static>(
    f: &mut Formatter<'_>,
    name: &str,
    len: Result<u64>,
    first: Result<Option<(AccessGuard<K>, AccessGuard<V>)>>,
    last: Result<Option<(AccessGuard<K>, AccessGuard<V>)>>,
) -> std::fmt::Result {
    write!(f, "Table [ name: \"{}\", ", name)?;
    if let Ok(len) = len {
        if len == 0 {
            write!(f, "No entries")?;
        } else if len == 1 {
            if let Ok(first) = first {
                let (key, value) = first.as_ref().unwrap();
                write!(f, "One key-value: {:?} = {:?}", key.value(), value.value())?;
            } else {
                write!(f, "I/O Error accessing table!")?;
            }
        } else {
            if let Ok(first) = first {
                let (key, value) = first.as_ref().unwrap();
                write!(f, "first: {:?} = {:?}, ", key.value(), value.value())?;
            } else {
                write!(f, "I/O Error accessing table!")?;
            }
            if len > 2 {
                write!(f, "...{} more entries..., ", len - 2)?;
            }
            if let Ok(last) = last {
                let (key, value) = last.as_ref().unwrap();
                write!(f, "last: {:?} = {:?}", key.value(), value.value())?;
            } else {
                write!(f, "I/O Error accessing table!")?;
            }
        }
    } else {
        write!(f, "I/O Error accessing table!")?;
    }
    write!(f, " ]")?;

    Ok(())
}

impl<K: Key + 'static, V: Value + 'static> Debug for Table<'_, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        debug_helper(f, &self.name, self.len(), self.first(), self.last())
    }
}

pub trait ReadableTableMetadata {
    /// Retrieves information about storage usage for the table
    fn stats(&self) -> Result<TableStats>;

    /// Returns the number of entries in the table
    fn len(&self) -> Result<u64>;

    /// Returns `true` if the table is empty
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

pub trait ReadableTable<K: Key + 'static, V: Value + 'static>: ReadableTableMetadata {
    /// Returns the value corresponding to the given key
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<V>>>;

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
        KR: Borrow<K::SelfType<'a>> + 'a;

    /// Returns the first key-value pair in the table, if it exists
    fn first(&self) -> Result<Option<(AccessGuard<K>, AccessGuard<V>)>> {
        self.iter()?.next().transpose()
    }

    /// Returns the last key-value pair in the table, if it exists
    fn last(&self) -> Result<Option<(AccessGuard<K>, AccessGuard<V>)>> {
        self.iter()?.next_back().transpose()
    }

    /// Returns a double-ended iterator over all elements in the table
    fn iter(&self) -> Result<Range<K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }
}

/// A read-only untyped table
pub struct ReadOnlyUntypedTable {
    tree: RawBtree,
}

impl Sealed for ReadOnlyUntypedTable {}

impl ReadableTableMetadata for ReadOnlyUntypedTable {
    /// Retrieves information about storage usage for the table
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
}

impl ReadOnlyUntypedTable {
    pub(crate) fn new(
        root_page: Option<BtreeHeader>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        Self {
            tree: RawBtree::new(root_page, fixed_key_size, fixed_value_size, mem),
        }
    }
}

/// A read-only table
pub struct ReadOnlyTable<K: Key + 'static, V: Value + 'static> {
    name: String,
    tree: Btree<K, V>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<K: Key + 'static, V: Value + 'static> ReadOnlyTable<K, V> {
    pub(crate) fn new(
        name: String,
        root_page: Option<BtreeHeader>,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<ReadOnlyTable<K, V>> {
        Ok(ReadOnlyTable {
            name,
            tree: Btree::new(root_page, hint, guard.clone(), mem)?,
            transaction_guard: guard,
        })
    }

    pub fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'static, V>>> {
        self.tree.get(key.borrow())
    }

    /// This method is like [`ReadableTable::range()`], but the iterator is reference counted and keeps the transaction
    /// alive until it is dropped.
    pub fn range<'a, KR>(&self, range: impl RangeBounds<KR>) -> Result<Range<'static, K, V>>
    where
        KR: Borrow<K::SelfType<'a>>,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction_guard.clone()))
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTableMetadata for ReadOnlyTable<K, V> {
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
}

impl<K: Key + 'static, V: Value + 'static> ReadableTable<K, V> for ReadOnlyTable<K, V> {
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<V>>> {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction_guard.clone()))
    }
}

impl<K: Key, V: Value> Sealed for ReadOnlyTable<K, V> {}

impl<K: Key + 'static, V: Value + 'static> Debug for ReadOnlyTable<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        debug_helper(f, &self.name, self.len(), self.first(), self.last())
    }
}

pub struct ExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    inner: BtreeExtractIf<'a, K, V, F>,
}

impl<
        'a,
        K: Key + 'static,
        V: Value + 'static,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    > ExtractIf<'a, K, V, F>
{
    fn new(inner: BtreeExtractIf<'a, K, V, F>) -> Self {
        Self { inner }
    }
}

impl<
        'a,
        K: Key + 'static,
        V: Value + 'static,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    > Iterator for ExtractIf<'a, K, V, F>
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
        K: Key + 'static,
        V: Value + 'static,
        F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
    > DoubleEndedIterator for ExtractIf<'a, K, V, F>
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

#[derive(Clone)]
pub struct Range<'a, K: Key + 'static, V: Value + 'static> {
    inner: BtreeRangeIter<K, V>,
    _transaction_guard: Arc<TransactionGuard>,
    // This lifetime is here so that `&` can be held on `Table` preventing concurrent mutation
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, K: Key + 'static, V: Value + 'static> Range<'a, K, V> {
    pub(super) fn new(inner: BtreeRangeIter<K, V>, guard: Arc<TransactionGuard>) -> Self {
        Self {
            inner,
            _transaction_guard: guard,
            _lifetime: Default::default(),
        }
    }
}

impl<'a, K: Key + 'static, V: Value + 'static> Iterator for Range<'a, K, V> {
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

impl<'a, K: Key + 'static, V: Value + 'static> DoubleEndedIterator for Range<'a, K, V> {
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
