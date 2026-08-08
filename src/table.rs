use crate::db::TransactionGuard;
use crate::sealed::Sealed;
#[cfg(feature = "experimental-api-5")]
use crate::tree_store::BtreeCursor;
#[cfg(feature = "experimental_cursor")]
use crate::tree_store::BtreeCursorMut;
use crate::tree_store::{
    AccessGuardMutInPlace, Btree, BtreeCursorRange, BtreeExtractIf, BtreeHeader, BtreeMut,
    MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, PageAllocator, PageHint, PageNumber, PageResolver,
    PageTrackerPolicy, RawBtree,
};
use crate::types::{Key, MutInPlaceValue, Value};
use crate::{AccessGuard, AccessGuardMut, StorageError, WriteTransaction};
use crate::{Result, TableHandle};
use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
#[cfg(feature = "experimental-api-5")]
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::thread;

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
    tree: BtreeMut<K, V>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for Table<'_, K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

struct RetainPanicGuard<'txn> {
    transaction: &'txn WriteTransaction,
    disarmed: bool,
}

impl<'txn> RetainPanicGuard<'txn> {
    fn new(transaction: &'txn WriteTransaction) -> Self {
        Self {
            transaction,
            disarmed: false,
        }
    }

    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for RetainPanicGuard<'_> {
    fn drop(&mut self) {
        if !self.disarmed && thread::panicking() {
            self.transaction.poison();
        }
    }
}

impl<'txn, K: Key + 'static, V: Value + 'static> Table<'txn, K, V> {
    pub(crate) fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
        transaction: &'txn WriteTransaction,
    ) -> Table<'txn, K, V> {
        Table {
            name: name.to_string(),
            transaction,
            tree: BtreeMut::new(
                table_root,
                transaction.transaction_guard(),
                page_allocator,
                freed_pages,
                allocated_pages,
            ),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self, include_values: bool) -> Result {
        self.tree.print_debug(include_values)
    }

    /// Returns an accessor, which allows mutation, to the value corresponding to the given key
    pub fn get_mut<'k>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
    ) -> Result<Option<AccessGuardMut<'_, V>>> {
        self.tree.get_mut(key.borrow())
    }

    /// Removes and returns the first key-value pair in the table
    pub fn pop_first(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.tree.pop_first()
    }

    /// Removes and returns the last key-value pair in the table
    pub fn pop_last(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.tree.pop_last()
    }

    /// Applies `predicate` to all key-value pairs. All entries for which
    /// `predicate` evaluates to `true` are returned in an iterator, and those which are read from the iterator are removed
    ///
    /// Note: values not read from the iterator will not be removed
    ///
    /// If the iterator returns an error, later calls keep returning an error
    /// (the failure is not recoverable). Entries already yielded stay
    /// removed; if finalizing their removal fails too, the write transaction
    /// is poisoned and cannot be committed.
    ///
    /// The predicate must not panic. If it panics, the write transaction is
    /// poisoned and [`crate::WriteTransaction::commit`] will return
    /// [`crate::CommitError::TransactionPoisoned`].
    pub fn extract_if<F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        predicate: F,
    ) -> Result<ExtractIf<'_, K, V, F>> {
        self.extract_from_if::<K::SelfType<'_>, F>(.., predicate)
    }

    /// Applies `predicate` to all key-value pairs in the specified range. All entries for which
    /// `predicate` evaluates to `true` are returned in an iterator, and those which are read from the iterator are removed
    ///
    /// Note: values not read from the iterator will not be removed
    ///
    /// If the iterator returns an error, later calls keep returning an error
    /// (the failure is not recoverable). Entries already yielded stay
    /// removed; if finalizing their removal fails too, the write transaction
    /// is poisoned and cannot be committed.
    ///
    /// The predicate must not panic. If it panics, the write transaction is
    /// poisoned and [`crate::WriteTransaction::commit`] will return
    /// [`crate::CommitError::TransactionPoisoned`].
    pub fn extract_from_if<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result<ExtractIf<'_, K, V, F>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let inner = self.tree.extract_from_if(&range, predicate)?;
        Ok(ExtractIf::new(inner, Some(self.transaction)))
    }

    /// Applies `predicate` to all key-value pairs. All entries for which
    /// `predicate` evaluates to `false` are removed.
    ///
    /// The predicate must not panic. If it panics, the write transaction is
    /// poisoned and [`crate::WriteTransaction::commit`] will return
    /// [`crate::CommitError::TransactionPoisoned`].
    ///
    pub fn retain<F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        predicate: F,
    ) -> Result {
        let mut panic_guard = RetainPanicGuard::new(self.transaction);
        let mut poisoned = false;
        let result = self
            .tree
            .retain_in::<K::SelfType<'_>, F>(predicate, .., &mut poisoned);
        panic_guard.disarm();
        if poisoned {
            self.transaction.poison();
        }
        result
    }

    /// Applies `predicate` to all key-value pairs in the range `start..end`. All entries for which
    /// `predicate` evaluates to `false` are removed.
    ///
    /// The predicate must not panic. If it panics, the write transaction is
    /// poisoned and [`crate::WriteTransaction::commit`] will return
    /// [`crate::CommitError::TransactionPoisoned`].
    ///
    pub fn retain_in<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let mut panic_guard = RetainPanicGuard::new(self.transaction);
        let mut poisoned = false;
        let result = self.tree.retain_in(predicate, range, &mut poisoned);
        panic_guard.disarm();
        if poisoned {
            self.transaction.poison();
        }
        result
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
    ) -> Result<Option<AccessGuard<'_, V>>> {
        let value_len = V::as_bytes(value.borrow()).as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_len + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len + key_len));
        }
        self.tree.insert(key.borrow(), value.borrow())
    }

    /// Removes the given key
    ///
    /// Returns the old value, if the key was present in the table
    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'_, V>>> {
        self.tree.remove(key.borrow())
    }

    /// Returns a [`CursorMut`] pointing at the gap before the smallest key
    /// greater than the given bound.
    ///
    /// Passing `Bound::Included(x)` will return a cursor pointing to the gap
    /// before the smallest key greater than or equal to `x`.
    ///
    /// Passing `Bound::Excluded(x)` will return a cursor pointing to the gap
    /// before the smallest key greater than `x`.
    ///
    /// Passing `Bound::Unbounded` will return a cursor pointing to the gap
    /// before the smallest key in the table.
    ///
    /// This is analogous to [`std::collections::BTreeMap::lower_bound_mut`].
    #[cfg(feature = "experimental_cursor")]
    pub fn lower_bound_mut<'a>(
        &mut self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<CursorMut<'_, K, V>> {
        let bound = bound_to_bytes::<K, _>(&bound);
        let mut inner = self.tree.cursor_mut();
        inner.seek_lower_bound(bound.as_ref().map(|bytes| bytes.as_slice()))?;
        Ok(CursorMut::new(inner, self.transaction))
    }

    /// Returns a [`CursorMut`] pointing at the gap after the greatest key
    /// smaller than the given bound.
    ///
    /// Passing `Bound::Included(x)` will return a cursor pointing to the gap
    /// after the greatest key smaller than or equal to `x`.
    ///
    /// Passing `Bound::Excluded(x)` will return a cursor pointing to the gap
    /// after the greatest key smaller than `x`.
    ///
    /// Passing `Bound::Unbounded` will return a cursor pointing to the gap
    /// after the greatest key in the table.
    ///
    /// This is analogous to [`std::collections::BTreeMap::upper_bound_mut`].
    ///
    /// # Examples
    ///
    /// Inserting a stream of ascending keys through the gap at the end of
    /// the table:
    ///
    /// ```rust
    /// use std::ops::Bound;
    /// use redb::{Database, Error, ReadableTableMetadata, TableDefinition};
    /// # use tempfile::NamedTempFile;
    /// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
    ///
    /// # fn main() -> Result<(), Error> {
    /// # #[cfg(not(target_os = "wasi"))]
    /// # let tmpfile = NamedTempFile::new().unwrap();
    /// # #[cfg(target_os = "wasi")]
    /// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
    /// # let filename = tmpfile.path();
    /// let db = Database::create(filename)?;
    /// let write_txn = db.begin_write()?;
    /// {
    ///     let mut table = write_txn.open_table(TABLE)?;
    ///     let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded)?;
    ///     for key in 0..1000 {
    ///         cursor.insert_before(key, &(key * 2))?;
    ///     }
    ///     cursor.close()?;
    ///     assert_eq!(table.len()?, 1000);
    /// }
    /// write_txn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "experimental_cursor")]
    pub fn upper_bound_mut<'a>(
        &mut self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<CursorMut<'_, K, V>> {
        let bound = bound_to_bytes::<K, _>(&bound);
        let mut inner = self.tree.cursor_mut();
        inner.seek_upper_bound(bound.as_ref().map(|bytes| bytes.as_slice()))?;
        Ok(CursorMut::new(inner, self.transaction))
    }

    /// Gets the given key's corresponding entry in the table for in-place manipulation.
    ///
    /// This is analogous to [`std::collections::BTreeMap::entry`], and avoids the double
    /// lookup that a `get` followed by `insert` would require when updating a value.
    pub fn entry<'a>(&'a mut self, key: K::SelfType<'a>) -> Result<Entry<'a, K, V>> {
        let key_len = K::as_bytes(&key).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if self.tree.get(&key)?.is_some() {
            Ok(Entry::Occupied(OccupiedEntry {
                tree: &mut self.tree,
                key,
            }))
        } else {
            Ok(Entry::Vacant(VacantEntry {
                tree: &mut self.tree,
                key,
            }))
        }
    }
}

impl<K: Key + 'static, V: MutInPlaceValue + 'static> Table<'_, K, V> {
    /// Reserve space to insert a key-value pair
    ///
    /// If key is already present it is replaced
    ///
    /// The returned reference will have length equal to `value_length`
    pub fn insert_reserve<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
        value_length: usize,
    ) -> Result<AccessGuardMutInPlace<'_, V>> {
        if value_length > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_length + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length + key_len));
        }
        self.tree.insert_reserve(key.borrow(), value_length)
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTableMetadata for Table<'_, K, V> {
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

impl<K: Key + 'static, V: Value + 'static> ReadableTable<K, V> for Table<'_, K, V> {
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<'_, V>>> {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction.transaction_guard()))
    }

    fn first(&self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.tree.first()
    }

    fn last(&self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.tree.last()
    }

    #[cfg(feature = "experimental-api-5")]
    fn lower_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'_, K, V>> {
        let bound = bound_to_bytes::<K, _>(&bound);
        let mut inner = self.tree.cursor()?;
        inner.seek_lower_bound(bound.as_ref().map(|bytes| bytes.as_slice()))?;
        Ok(Cursor::new(inner, self.transaction.transaction_guard()))
    }

    #[cfg(feature = "experimental-api-5")]
    fn upper_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'_, K, V>> {
        let bound = bound_to_bytes::<K, _>(&bound);
        let mut inner = self.tree.cursor()?;
        inner.seek_upper_bound(bound.as_ref().map(|bytes| bytes.as_slice()))?;
        Ok(Cursor::new(inner, self.transaction.transaction_guard()))
    }
}

impl<K: Key, V: Value> Sealed for Table<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Drop for Table<'_, K, V> {
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
    write!(f, "Table [ name: \"{name}\", ")?;
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
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<'_, V>>>;

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
    /// # #[cfg(not(target_os = "wasi"))]
    /// # let tmpfile = NamedTempFile::new().unwrap();
    /// # #[cfg(target_os = "wasi")]
    /// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
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
    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a;

    /// Returns the first key-value pair in the table, if it exists
    fn first(&self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>>;

    /// Returns the last key-value pair in the table, if it exists
    fn last(&self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>>;

    /// Returns a read-only [`Cursor`] pointing at the gap before the smallest
    /// key greater than the given bound.
    ///
    /// Passing `Bound::Included(x)` will return a cursor pointing to the gap
    /// before the smallest key greater than or equal to `x`.
    ///
    /// Passing `Bound::Excluded(x)` will return a cursor pointing to the gap
    /// before the smallest key greater than `x`.
    ///
    /// Passing `Bound::Unbounded` will return a cursor pointing to the gap
    /// before the smallest key in the table.
    ///
    /// This is analogous to [`std::collections::BTreeMap::lower_bound`].
    ///
    /// # Examples
    ///
    /// Probing around a key in any table, read-only or not. The cursor's
    /// methods additionally require the `experimental_cursor` feature flag:
    ///
    #[cfg_attr(feature = "experimental_cursor", doc = "```rust")]
    #[cfg_attr(not(feature = "experimental_cursor"), doc = "```rust,ignore")]
    /// use std::ops::Bound;
    /// use redb::{Database, Error, ReadableDatabase, ReadableTable, TableDefinition};
    /// # use tempfile::NamedTempFile;
    /// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
    ///
    /// fn entry_at_or_after(
    ///     table: &impl ReadableTable<u64, u64>,
    ///     key: u64,
    /// ) -> Result<Option<u64>, Error> {
    ///     let mut cursor = table.lower_bound(Bound::Included(&key))?;
    ///     Ok(cursor.peek_next()?.map(|(key, _)| key.value()))
    /// }
    ///
    /// # fn main() -> Result<(), Error> {
    /// # #[cfg(not(target_os = "wasi"))]
    /// # let tmpfile = NamedTempFile::new().unwrap();
    /// # #[cfg(target_os = "wasi")]
    /// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
    /// # let filename = tmpfile.path();
    /// let db = Database::create(filename)?;
    /// let write_txn = db.begin_write()?;
    /// {
    ///     let mut table = write_txn.open_table(TABLE)?;
    ///     for key in 0..10 {
    ///         table.insert(key, &(key * 2))?;
    ///     }
    ///     assert_eq!(entry_at_or_after(&table, 5)?, Some(5));
    /// }
    /// write_txn.commit()?;
    ///
    /// let read_txn = db.begin_read()?;
    /// let table = read_txn.open_table(TABLE)?;
    /// assert_eq!(entry_at_or_after(&table, 5)?, Some(5));
    /// assert_eq!(entry_at_or_after(&table, 100)?, None);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "experimental-api-5")]
    fn lower_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'_, K, V>>;

    /// Returns a read-only [`Cursor`] pointing at the gap after the greatest
    /// key smaller than the given bound.
    ///
    /// Passing `Bound::Included(x)` will return a cursor pointing to the gap
    /// after the greatest key smaller than or equal to `x`.
    ///
    /// Passing `Bound::Excluded(x)` will return a cursor pointing to the gap
    /// after the greatest key smaller than `x`.
    ///
    /// Passing `Bound::Unbounded` will return a cursor pointing to the gap
    /// after the greatest key in the table.
    ///
    /// This is analogous to [`std::collections::BTreeMap::upper_bound`].
    #[cfg(feature = "experimental-api-5")]
    fn upper_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'_, K, V>>;

    /// Returns a double-ended iterator over all elements in the table
    fn iter(&self) -> Result<Range<'_, K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }
}

/// A read-only untyped table
pub struct ReadOnlyUntypedTable {
    name: String,
    tree: RawBtree,
}

impl Sealed for ReadOnlyUntypedTable {}

impl TableHandle for ReadOnlyUntypedTable {
    fn name(&self) -> &str {
        &self.name
    }
}

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
        name: &str,
        root_page: Option<BtreeHeader>,
        hint: PageHint,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        mem: PageResolver,
    ) -> Self {
        Self {
            name: name.to_string(),
            tree: RawBtree::new(root_page, fixed_key_size, fixed_value_size, mem, hint),
        }
    }
}

/// A read-only table
pub struct ReadOnlyTable<K: Key + 'static, V: Value + 'static> {
    name: String,
    tree: Btree<K, V>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for ReadOnlyTable<K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadOnlyTable<K, V> {
    pub(crate) fn new(
        name: String,
        root_page: Option<BtreeHeader>,
        hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: PageResolver,
    ) -> Result<ReadOnlyTable<K, V>> {
        Ok(ReadOnlyTable {
            name,
            tree: Btree::new(root_page, hint, guard.clone(), mem)?,
            transaction_guard: guard,
        })
    }

    /// This method is like [`ReadableTable::get()`], but the [`AccessGuard`] is reference counted
    /// and keeps the transaction alive until it is dropped.
    pub fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'static, V>>> {
        self.tree.get(key.borrow())
    }

    /// This method is like [`ReadableTable::get()`], but the returned [`OwnedAccessGuard`] is
    /// reference counted and keeps the transaction alive until it is dropped.
    pub fn get_owned<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<OwnedAccessGuard<V>>> {
        Ok(self
            .get(key)?
            .map(|x| OwnedAccessGuard::new(x, self.transaction_guard.clone())))
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

    /// This method is like [`ReadableTable::range()`], but the returned iterator is reference
    /// counted and keeps the transaction alive until it is dropped, as do the
    /// [`OwnedAccessGuard`]s it yields.
    pub fn range_owned<'a, KR>(&self, range: impl RangeBounds<KR>) -> Result<OwnedRange<K, V>>
    where
        KR: Borrow<K::SelfType<'a>>,
    {
        Ok(OwnedRange::new(
            self.range(range)?,
            self.transaction_guard.clone(),
        ))
    }

    /// This method is like [`ReadableTable::lower_bound()`], but the cursor is reference counted
    /// and keeps the transaction alive until it is dropped.
    #[cfg(feature = "experimental-api-5")]
    pub fn lower_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'static, K, V>> {
        let bound = bound_to_bytes::<K, _>(&bound);
        let mut inner = self.tree.cursor();
        inner.seek_lower_bound(bound.as_ref().map(|bytes| bytes.as_slice()))?;
        Ok(Cursor::new(inner, self.transaction_guard.clone()))
    }

    /// This method is like [`ReadableTable::upper_bound()`], but the cursor is reference counted
    /// and keeps the transaction alive until it is dropped.
    #[cfg(feature = "experimental-api-5")]
    pub fn upper_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'static, K, V>> {
        let bound = bound_to_bytes::<K, _>(&bound);
        let mut inner = self.tree.cursor();
        inner.seek_upper_bound(bound.as_ref().map(|bytes| bytes.as_slice()))?;
        Ok(Cursor::new(inner, self.transaction_guard.clone()))
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
    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<'_, V>>> {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction_guard.clone()))
    }

    fn first(&self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.tree.first()
    }

    fn last(&self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.tree.last()
    }

    #[cfg(feature = "experimental-api-5")]
    fn lower_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'_, K, V>> {
        ReadOnlyTable::lower_bound(self, bound)
    }

    #[cfg(feature = "experimental-api-5")]
    fn upper_bound<'a>(
        &self,
        bound: Bound<impl Borrow<K::SelfType<'a>>>,
    ) -> Result<Cursor<'_, K, V>> {
        ReadOnlyTable::upper_bound(self, bound)
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
    poison_target: Option<&'a WriteTransaction>,
}

impl<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> ExtractIf<'a, K, V, F>
{
    pub(crate) fn new(
        inner: BtreeExtractIf<'a, K, V, F>,
        poison_target: Option<&'a WriteTransaction>,
    ) -> Self {
        Self {
            inner,
            poison_target,
        }
    }

    /// Closes the iterator.
    ///
    /// Entries already returned by the iterator remain removed, and unread
    /// entries are not tested by the predicate or removed. Dropping the iterator
    /// also closes it, but this method returns any error encountered while
    /// finalizing the iterator, including when the iterator already closed
    /// itself after an iteration error.
    pub fn close(mut self) -> Result {
        self.inner.close()
    }
}

impl<
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> Drop for ExtractIf<'_, K, V, F>
{
    fn drop(&mut self) {
        // Entries already yielded may have removals pending; if a flush
        // failed the table would silently keep them, so poison the
        // transaction instead of letting it commit. An iteration error
        // alone must not poison: close_failed reports lost removals, and
        // close() also re-raises after a cleanly finalized error.
        let _ = self.inner.close();
        if (self.inner.close_failed() || self.inner.predicate_panicked())
            && let Some(transaction) = self.poison_target
        {
            transaction.poison();
        }
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
        self.inner.next()
    }
}

impl<
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> DoubleEndedIterator for ExtractIf<'_, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

#[derive(Clone)]
pub struct Range<'a, K: Key + 'static, V: Value + 'static> {
    inner: BtreeCursorRange<K, V>,
    _transaction_guard: Arc<TransactionGuard>,
    // This lifetime is here so that `&` can be held on `Table` preventing concurrent mutation
    _lifetime: PhantomData<&'a ()>,
}

impl<K: Key + 'static, V: Value + 'static> Range<'_, K, V> {
    pub(super) fn new(inner: BtreeCursorRange<K, V>, guard: Arc<TransactionGuard>) -> Self {
        Self {
            inner,
            _transaction_guard: guard,
            _lifetime: PhantomData,
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

impl<K: Key + 'static, V: Value + 'static> DoubleEndedIterator for Range<'_, K, V> {
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

/// An [`AccessGuard`] which also keeps the transaction alive
///
/// Returned by the reference-counted accessors of [`ReadOnlyTable`] and
/// [`crate::ReadOnlyMultimapTable`], such as [`ReadOnlyTable::get_owned()`]: in addition to
/// providing access to the data, it keeps the read transaction alive until it is dropped.
pub struct OwnedAccessGuard<V: Value + 'static> {
    // Declared before the transaction guard so the page reference is released before the
    // transaction is deallocated
    inner: AccessGuard<'static, V>,
    _transaction_guard: Arc<TransactionGuard>,
}

impl<V: Value + 'static> OwnedAccessGuard<V> {
    pub(crate) fn new(inner: AccessGuard<'static, V>, guard: Arc<TransactionGuard>) -> Self {
        Self {
            inner,
            _transaction_guard: guard,
        }
    }

    /// Access the stored value
    pub fn value(&self) -> V::SelfType<'_> {
        self.inner.value()
    }
}

/// A [`Range`] which also keeps the transaction alive
///
/// Returned by [`ReadOnlyTable::range_owned()`]. The iterator and the [`OwnedAccessGuard`]s it
/// yields keep the read transaction alive until they are dropped.
#[derive(Clone)]
pub struct OwnedRange<K: Key + 'static, V: Value + 'static> {
    inner: Range<'static, K, V>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<K: Key + 'static, V: Value + 'static> OwnedRange<K, V> {
    pub(super) fn new(inner: Range<'static, K, V>, guard: Arc<TransactionGuard>) -> Self {
        Self {
            inner,
            transaction_guard: guard,
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> Iterator for OwnedRange<K, V> {
    type Item = Result<(OwnedAccessGuard<K>, OwnedAccessGuard<V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|x| {
            x.map(|(key, value)| {
                (
                    OwnedAccessGuard::new(key, self.transaction_guard.clone()),
                    OwnedAccessGuard::new(value, self.transaction_guard.clone()),
                )
            })
        })
    }
}

impl<K: Key + 'static, V: Value + 'static> DoubleEndedIterator for OwnedRange<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|x| {
            x.map(|(key, value)| {
                (
                    OwnedAccessGuard::new(key, self.transaction_guard.clone()),
                    OwnedAccessGuard::new(value, self.transaction_guard.clone()),
                )
            })
        })
    }
}

/// A view into a single entry in a [`Table`], which may either be vacant or occupied.
///
/// This `enum` is constructed from the [`entry`] method on [`Table`], and mirrors
/// [`std::collections::btree_map::Entry`] as closely as the redb data model allows.
///
/// Unlike the in-memory `BTreeMap`, redb values are stored serialized, so methods that
/// produce a "reference to the value" return an [`AccessGuardMut`] instead of `&mut V`.
///
/// [`entry`]: Table::entry
pub enum Entry<'a, K: Key + 'static, V: Value + 'static> {
    /// An occupied entry.
    Occupied(OccupiedEntry<'a, K, V>),
    /// A vacant entry.
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: Key + 'static, V: Value + 'static> Entry<'a, K, V> {
    /// Returns a view of this entry's key.
    pub fn key(&self) -> &K::SelfType<'a> {
        match self {
            Entry::Occupied(entry) => entry.key(),
            Entry::Vacant(entry) => entry.key(),
        }
    }

    /// Ensures a value is in the entry by inserting the provided `default` if empty,
    /// and returns a mutable accessor to the value in the entry.
    pub fn or_insert<'v>(
        self,
        default: impl Borrow<V::SelfType<'v>>,
    ) -> Result<AccessGuardMut<'a, V>> {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default),
        }
    }

    /// Ensures a value is in the entry by inserting the result of `default` if empty,
    /// and returns a mutable accessor to the value in the entry.
    ///
    /// Unlike [`or_insert`](Self::or_insert), the default value is only computed if the
    /// entry is vacant.
    pub fn or_insert_with<'v, F, B>(self, default: F) -> Result<AccessGuardMut<'a, V>>
    where
        F: FnOnce() -> B,
        B: Borrow<V::SelfType<'v>>,
    {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }

    /// Ensures a value is in the entry by inserting, if empty, the result of the `default`
    /// function, which is given a view of the key.
    pub fn or_insert_with_key<'v, F, B>(self, default: F) -> Result<AccessGuardMut<'a, V>>
    where
        F: FnOnce(&K::SelfType<'a>) -> B,
        B: Borrow<V::SelfType<'v>>,
    {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let value = default(&entry.key);
                entry.insert(value)
            }
        }
    }

    /// Provides in-place mutable access to an occupied entry before any potential inserts
    /// into the table.
    ///
    /// The closure receives an [`AccessGuardMut`] and may replace the stored value via
    /// [`AccessGuardMut::insert`]. Any errors returned by the closure are propagated.
    pub fn and_modify<F>(self, f: F) -> Result<Self>
    where
        F: FnOnce(&mut AccessGuardMut<'_, V>) -> Result<()>,
    {
        match self {
            Entry::Occupied(mut entry) => {
                {
                    let mut guard = entry.get_mut()?;
                    f(&mut guard)?;
                }
                Ok(Entry::Occupied(entry))
            }
            Entry::Vacant(entry) => Ok(Entry::Vacant(entry)),
        }
    }
}

/// A view into an occupied entry in a [`Table`]. It is part of the [`Entry`] enum.
pub struct OccupiedEntry<'a, K: Key + 'static, V: Value + 'static> {
    tree: &'a mut BtreeMut<K, V>,
    key: K::SelfType<'a>,
}

impl<'a, K: Key + 'static, V: Value + 'static> OccupiedEntry<'a, K, V> {
    /// Returns a view of this entry's key.
    pub fn key(&self) -> &K::SelfType<'a> {
        &self.key
    }

    /// Returns a view of this entry's value.
    pub fn get(&self) -> Result<AccessGuard<'_, V>> {
        self.tree.get(&self.key)?.ok_or_else(|| {
            StorageError::Corrupted(
                "entry for key disappeared while OccupiedEntry was live".to_string(),
            )
        })
    }

    /// Returns a mutable accessor to the value in the entry.
    pub fn get_mut(&mut self) -> Result<AccessGuardMut<'_, V>> {
        self.tree.get_mut(&self.key)?.ok_or_else(|| {
            StorageError::Corrupted(
                "entry for key disappeared while OccupiedEntry was live".to_string(),
            )
        })
    }

    /// Converts the entry into a mutable accessor to the value in the entry with a lifetime
    /// bound to the table itself.
    pub fn into_mut(self) -> Result<AccessGuardMut<'a, V>> {
        self.tree.get_mut(&self.key)?.ok_or_else(|| {
            StorageError::Corrupted(
                "entry for key disappeared while OccupiedEntry was live".to_string(),
            )
        })
    }

    /// Replaces the value of the entry with the supplied value, and returns the old value.
    pub fn insert<'v>(
        &mut self,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<AccessGuard<'_, V>> {
        let value_len = V::as_bytes(value.borrow()).as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let key_len = K::as_bytes(&self.key).as_ref().len();
        if value_len + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len + key_len));
        }
        self.tree.insert(&self.key, value.borrow())?.ok_or_else(|| {
            StorageError::Corrupted(
                "entry for key disappeared while OccupiedEntry was live".to_string(),
            )
        })
    }

    /// Takes the value out of the entry, and returns it.
    pub fn remove(self) -> Result<AccessGuard<'a, V>> {
        self.tree.remove(&self.key)?.ok_or_else(|| {
            StorageError::Corrupted(
                "entry for key disappeared while OccupiedEntry was live".to_string(),
            )
        })
    }

    /// Takes the entry out of the table, returning the key and the value.
    pub fn remove_entry(self) -> Result<(K::SelfType<'a>, AccessGuard<'a, V>)> {
        let OccupiedEntry { tree, key } = self;
        let value = tree.remove(&key)?.ok_or_else(|| {
            StorageError::Corrupted(
                "entry for key disappeared while OccupiedEntry was live".to_string(),
            )
        })?;
        Ok((key, value))
    }
}

/// A view into a vacant entry in a [`Table`]. It is part of the [`Entry`] enum.
pub struct VacantEntry<'a, K: Key + 'static, V: Value + 'static> {
    tree: &'a mut BtreeMut<K, V>,
    key: K::SelfType<'a>,
}

impl<'a, K: Key + 'static, V: Value + 'static> VacantEntry<'a, K, V> {
    /// Returns a view of this entry's key.
    pub fn key(&self) -> &K::SelfType<'a> {
        &self.key
    }

    /// Consumes the entry and returns the key that was used to construct it.
    pub fn into_key(self) -> K::SelfType<'a> {
        self.key
    }

    /// Inserts `value` with the entry's key and returns a mutable accessor to it.
    pub fn insert<'v>(self, value: impl Borrow<V::SelfType<'v>>) -> Result<AccessGuardMut<'a, V>> {
        let value_len = V::as_bytes(value.borrow()).as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let key_len = K::as_bytes(&self.key).as_ref().len();
        if value_len + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len + key_len));
        }
        self.tree.insert(&self.key, value.borrow())?;
        self.tree.get_mut(&self.key)?.ok_or_else(|| {
            StorageError::Corrupted(
                "inserted entry not found after VacantEntry::insert".to_string(),
            )
        })
    }
}

#[cfg(feature = "experimental-api-5")]
fn bound_to_bytes<'a, K: Key + 'a, KR: Borrow<K::SelfType<'a>>>(
    bound: &Bound<KR>,
) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(key) => Bound::Included(K::as_bytes(key.borrow()).as_ref().to_vec()),
        Bound::Excluded(key) => Bound::Excluded(K::as_bytes(key.borrow()).as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// A read-only cursor over a table, pointing at a gap between two entries.
///
/// Cursors are constructed with [`ReadableTable::lower_bound`] and
/// [`ReadableTable::upper_bound`], and mirror the nightly
/// [`std::collections::btree_map::Cursor`] as closely as the redb data model
/// allows: values are stored serialized, so the entries around the gap are
/// returned as [`AccessGuard`]s instead of references, and every operation
/// can report a storage error.
///
/// The cursor's methods are behind the `experimental_cursor` feature flag,
/// separately from its constructors, so that the constructors' signatures
/// can stabilize first.
#[cfg(feature = "experimental-api-5")]
pub struct Cursor<'a, K: Key + 'static, V: Value + 'static> {
    // Only the cursor's own methods read the position; without them the
    // constructors still store it, ready for the methods' flag to be enabled.
    #[cfg_attr(not(feature = "experimental_cursor"), allow(dead_code))]
    inner: BtreeCursor<K, V>,
    _transaction_guard: Arc<TransactionGuard>,
    // This lifetime is here so that `&` can be held on `Table` preventing concurrent mutation
    _lifetime: PhantomData<&'a ()>,
}

#[cfg(feature = "experimental-api-5")]
impl<K: Key + 'static, V: Value + 'static> Cursor<'_, K, V> {
    pub(crate) fn new(inner: BtreeCursor<K, V>, guard: Arc<TransactionGuard>) -> Self {
        Self {
            inner,
            _transaction_guard: guard,
            _lifetime: PhantomData,
        }
    }
}

#[cfg(feature = "experimental_cursor")]
impl<'a, K: Key + 'static, V: Value + 'static> Cursor<'a, K, V> {
    /// Returns the entry after the cursor's gap without moving the cursor.
    ///
    /// Returns `None` if the gap is at the end of the table.
    #[allow(clippy::type_complexity)]
    pub fn peek_next(&mut self) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.inner.peek_next()
    }

    /// Returns the entry before the cursor's gap without moving the cursor.
    ///
    /// Returns `None` if the gap is at the start of the table.
    #[allow(clippy::type_complexity)]
    pub fn peek_prev(&mut self) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.inner.peek_prev()
    }

    /// Moves the cursor past the entry after the gap, returning that entry.
    ///
    /// Returns `None`, and does not move, if the gap is at the end of the
    /// table.
    ///
    /// This is analogous to the nightly
    /// [`std::collections::btree_map::Cursor::next`]. The cursor is not an
    /// [`Iterator`], since every step can report a storage error; use
    /// [`ReadableTable::range`] for iteration.
    #[allow(clippy::should_implement_trait, clippy::type_complexity)]
    pub fn next(&mut self) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.inner.next()
    }

    /// Moves the cursor before the entry preceding the gap, returning that
    /// entry.
    ///
    /// Returns `None`, and does not move, if the gap is at the start of the
    /// table.
    ///
    /// This is analogous to the nightly
    /// [`std::collections::btree_map::Cursor::prev`].
    #[allow(clippy::type_complexity)]
    pub fn prev(&mut self) -> Result<Option<(AccessGuard<'a, K>, AccessGuard<'a, V>)>> {
        self.inner.prev()
    }
}

/// A cursor over a [`Table`], pointing at a gap between two entries, with
/// support for inserting new entries into that gap.
///
/// Cursors are constructed with [`Table::lower_bound_mut`] and
/// [`Table::upper_bound_mut`], and mirror the nightly
/// [`std::collections::btree_map::CursorMut`] as closely as the redb data
/// model allows: values are stored serialized, so inspecting the entries
/// around the gap returns [`AccessGuard`]s instead of references, and every
/// operation can report a storage error.
///
/// Call [`close`](Self::close) when finished with the cursor: errors from
/// applying its inserts can be deferred, and only `close` reports them.
///
/// The cursor mutably borrows the [`Table`]: the table cannot be used while
/// a cursor into it exists.
#[cfg(feature = "experimental_cursor")]
pub struct CursorMut<'a, K: Key + 'static, V: Value + 'static> {
    inner: BtreeCursorMut<'a, K, V>,
    transaction: &'a WriteTransaction,
    // An I/O error leaves the cursor position unreliable, so later operations
    // re-raise instead of continuing. Pending inserts are still flushed (or
    // the transaction poisoned) on close: an error never latches while both
    // pending inserts and a damaged position exist.
    errored: bool,
    closed: bool,
}

#[cfg(feature = "experimental_cursor")]
impl<'a, K: Key + 'static, V: Value + 'static> CursorMut<'a, K, V> {
    pub(crate) fn new(inner: BtreeCursorMut<'a, K, V>, transaction: &'a WriteTransaction) -> Self {
        Self {
            inner,
            transaction,
            errored: false,
            closed: false,
        }
    }

    /// Returns the entry after the cursor's gap without moving the cursor.
    ///
    /// Returns `None` if the gap is at the end of the table.
    #[allow(clippy::type_complexity)]
    pub fn peek_next(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.check_usable()?;
        match self.inner.peek_next() {
            Ok(entry) => Ok(entry),
            Err(err) => {
                // Peeking never consumes pending inserts, so unlike a failed
                // splice this cannot lose reported inserts; the position is
                // merely unreliable.
                self.errored = true;
                Err(err)
            }
        }
    }

    /// Returns the entry before the cursor's gap without moving the cursor.
    ///
    /// Returns `None` if the gap is at the start of the table.
    #[allow(clippy::type_complexity)]
    pub fn peek_prev(&mut self) -> Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        self.check_usable()?;
        match self.inner.peek_prev() {
            Ok(entry) => Ok(entry),
            Err(err) => {
                self.errored = true;
                Err(err)
            }
        }
    }

    /// Inserts a new entry into the gap that the cursor is pointing at. After
    /// the insertion, the cursor points at the gap after the newly inserted
    /// entry.
    ///
    /// If the key does not sort strictly greater than the entry before the
    /// gap and strictly smaller than the entry after it,
    /// [`StorageError::UnorderedKey`] is returned and nothing is changed.
    /// Unlike [`Table::insert`], an existing key is never overwritten:
    /// inserting a key equal to either neighbor is unordered.
    ///
    /// This is analogous to the nightly
    /// [`std::collections::btree_map::CursorMut::insert_before`].
    pub fn insert_before<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<()> {
        self.check_usable()?;
        let key_bytes = K::as_bytes(key.borrow());
        let value_bytes = V::as_bytes(value.borrow());
        let key_len = key_bytes.as_ref().len();
        let value_len = value_bytes.as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_len + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len + key_len));
        }
        match self
            .inner
            .insert_before(key_bytes.as_ref(), value_bytes.as_ref())
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(StorageError::UnorderedKey),
            Err(err) => Err(self.latch_error(err)),
        }
    }

    /// Closes the cursor, applying any of its inserts that have not yet
    /// reached the table.
    ///
    /// `Ok` means every insert this cursor accepted is in the table. Dropping
    /// the cursor closes it too, but cannot report errors; if applying the
    /// inserts fails then, the write transaction is poisoned and
    /// [`crate::WriteTransaction::commit`] will return
    /// [`crate::CommitError::TransactionPoisoned`], so the loss cannot be
    /// committed.
    pub fn close(mut self) -> Result {
        self.closed = true;
        self.finish()
    }

    fn check_usable(&self) -> Result {
        if self.errored {
            return Err(StorageError::PreviousIo);
        }
        Ok(())
    }

    fn latch_error(&mut self, err: StorageError) -> StorageError {
        self.errored = true;
        if self.inner.poisoned() {
            // Inserts already reported as applied were lost; the transaction
            // must not commit without them.
            self.transaction.poison();
        }
        err
    }

    fn finish(&mut self) -> Result {
        let result = self.inner.finish();
        if result.is_err() || self.inner.poisoned() {
            self.transaction.poison();
        }
        result
    }
}

#[cfg(feature = "experimental_cursor")]
impl<K: Key + 'static, V: Value + 'static> Drop for CursorMut<'_, K, V> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        // Drop cannot surface a splice failure; finish() poisons the
        // transaction instead, so the loss cannot be committed.
        let _ = self.finish();
    }
}
