use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    AllPageNumbersBtreeIter, BtreeRangeIter, FreedTableKey, InternalTableDefinition, RawBtree,
    TableType, TransactionalMemory, PAGE_SIZE,
};
use crate::types::{RedbKey, RedbValue};
use crate::Error;
use crate::{ReadTransaction, Result, WriteTransaction};
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::RangeFull;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::multimap_table::parse_subtree_roots;
use crate::Error::Corrupted;
#[cfg(feature = "logging")]
use log::{info, warn};

struct AtomicTransactionId {
    inner: AtomicU64,
}

impl AtomicTransactionId {
    fn new(last_id: TransactionId) -> Self {
        Self {
            inner: AtomicU64::new(last_id.0),
        }
    }

    fn next(&self) -> TransactionId {
        let id = self.inner.fetch_add(1, Ordering::AcqRel);
        TransactionId(id)
    }
}

pub trait TableHandle {
    // Returns the name of the table
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedTableHandle {
    name: String,
}

impl UntypedTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl TableHandle for UntypedTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

pub trait MultimapTableHandle {
    // Returns the name of the multimap table
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedMultimapTableHandle {
    name: String,
}

impl UntypedMultimapTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl MultimapTableHandle for UntypedMultimapTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

/// Defines the name and types of a table
///
/// A [`TableDefinition`] should be opened for use by calling [`ReadTransaction::open_table`] or [`WriteTransaction::open_table`]
///
/// Note that the lifetime of the `K` and `V` type parameters does not impact the lifetimes of the data
/// that is stored or retreived from the table
pub struct TableDefinition<'a, K: RedbKey + 'static, V: RedbValue + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> TableDefinition<'a, K, V> {
    /// Construct a new table with given `name`
    ///
    /// ## Invariant
    ///
    /// `name` must not be empty.
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> TableHandle for TableDefinition<'a, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Clone for TableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Copy for TableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbValue + 'static> Display for TableDefinition<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

/// Defines the name and types of a multimap table
///
/// A [`MultimapTableDefinition`] should be opened for use by calling [`ReadTransaction::open_multimap_table`] or [`WriteTransaction::open_multimap_table`]
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
///
/// Note that the lifetime of the `K` and `V` type parameters does not impact the lifetimes of the data
/// that is stored or retreived from the table
pub struct MultimapTableDefinition<'a, K: RedbKey + 'static, V: RedbKey + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> MultimapTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> MultimapTableHandle
    for MultimapTableDefinition<'a, K, V>
{
    fn name(&self) -> &str {
        self.name
    }
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> Copy for MultimapTableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + 'static, V: RedbKey + 'static> Display for MultimapTableDefinition<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

/// Opened redb database file
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`] object that can be used to read from the database
/// Use [`Self::begin_write`] to get a [`WriteTransaction`] object that can be used to read or write to the database
///
/// Multiple reads may be performed concurrently, with each other, and with writes. Only a single write
/// may be in progress at a time.
///
/// # Examples
///
/// Basic usage:
///
/// ```rust
/// use redb::*;
/// # use tempfile::NamedTempFile;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
/// # let filename = tmpfile.path();
/// let db = Database::create(filename)?;
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// write_txn.commit()?;
/// # Ok(())
/// # }
/// ```
pub struct Database {
    mem: TransactionalMemory,
    next_transaction_id: AtomicTransactionId,
    transaction_tracker: Arc<Mutex<TransactionTracker>>,
    pub(crate) live_write_transaction: Mutex<Option<TransactionId>>,
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub fn create(path: impl AsRef<Path>) -> Result<Database> {
        Self::builder().create(path)
    }

    /// Opens an existing redb database.
    pub fn open(path: impl AsRef<Path>) -> Result<Database> {
        Self::builder().open(path)
    }

    pub(crate) fn get_memory(&self) -> &TransactionalMemory {
        &self.mem
    }

    fn verify_primary_checksums(mem: &TransactionalMemory) -> Result<bool> {
        let (root, root_checksum) = mem
            .get_data_root()
            .expect("Tried to repair an empty database");
        if !RawBtree::new(
            Some((root, root_checksum)),
            <&str>::fixed_width(),
            InternalTableDefinition::fixed_width(),
            mem,
        )
        .verify_checksum()?
        {
            return Ok(false);
        }

        if let Some((freed_root, freed_checksum)) = mem.get_freed_root() {
            if !RawBtree::new(
                Some((freed_root, freed_checksum)),
                FreedTableKey::fixed_width(),
                None,
                mem,
            )
            .verify_checksum()?
            {
                return Ok(false);
            }
        }

        // Iterate over all other tables
        let iter: BtreeRangeIter<&str, InternalTableDefinition> =
            BtreeRangeIter::new::<RangeFull, &str>(.., Some(root), mem)?;
        for entry in iter {
            let definition = entry?.value();
            if let Some((table_root, table_checksum)) = definition.get_root() {
                if !RawBtree::new(
                    Some((table_root, table_checksum)),
                    definition.get_fixed_key_size(),
                    definition.get_fixed_value_size(),
                    mem,
                )
                .verify_checksum()?
                {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Check the integrity of the database file, and repair it if possible.
    ///
    /// Returns `Ok(true)` if the database passed integrity checks; `Ok(false)` if it failed but was repaired,
    /// and `Err(Corrupted)` if the check failed and the file could not be repaired
    pub fn check_integrity(&mut self) -> Result<bool> {
        self.mem.clear_cache_and_reload()?;

        if !self.mem.needs_repair()? && Self::verify_primary_checksums(&self.mem)? {
            return Ok(true);
        }

        Self::do_repair(&mut self.mem)?;

        Ok(false)
    }

    fn do_repair(mem: &mut TransactionalMemory) -> Result {
        if !Self::verify_primary_checksums(mem)? {
            mem.repair_primary_corrupted();
            if !Self::verify_primary_checksums(mem)? {
                return Err(Corrupted(
                    "Failed to repair database. All roots are corrupted".to_string(),
                ));
            }
        }

        mem.begin_repair()?;

        let (root, root_checksum) = mem
            .get_data_root()
            .expect("Tried to repair an empty database");

        // Repair the allocator state
        // All pages in the master table
        let master_pages_iter = AllPageNumbersBtreeIter::new(root, None, None, mem)?;
        mem.mark_pages_allocated(master_pages_iter)?;

        // Iterate over all other tables
        let iter: BtreeRangeIter<&str, InternalTableDefinition> =
            BtreeRangeIter::new::<RangeFull, &str>(.., Some(root), mem)?;

        // Chain all the other tables to the master table iter
        for entry in iter {
            let definition = entry?.value();
            if let Some((table_root, _)) = definition.get_root() {
                let table_pages_iter = AllPageNumbersBtreeIter::new(
                    table_root,
                    definition.get_fixed_key_size(),
                    definition.get_fixed_value_size(),
                    mem,
                )?;
                mem.mark_pages_allocated(table_pages_iter)?;

                // Multimap tables may have additional subtrees in their values
                if definition.get_type() == TableType::Multimap {
                    let table_pages_iter = AllPageNumbersBtreeIter::new(
                        table_root,
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        mem,
                    )?;
                    for table_page in table_pages_iter {
                        let page = mem.get_page(table_page?)?;
                        let mut subtree_roots = parse_subtree_roots(
                            &page,
                            definition.get_fixed_key_size(),
                            definition.get_fixed_value_size(),
                        );
                        mem.mark_pages_allocated(subtree_roots.drain(..).map(Ok))?;
                    }
                }
            }
        }

        mem.end_repair()?;

        // Clear the freed table. We just rebuilt the allocator state by walking all the
        // reachable data pages, which implicitly frees the pages for the freed table
        let transaction_id = mem.get_last_committed_transaction_id()?.next();
        mem.commit(
            Some((root, root_checksum)),
            None,
            transaction_id,
            false,
            true,
        )?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        file: File,
        page_size: usize,
        region_size: Option<usize>,
        read_cache_size_bytes: usize,
        write_cache_size_bytes: usize,
    ) -> Result<Self> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database {:?}", &file_path);
        let mut mem = TransactionalMemory::new(
            file,
            page_size,
            region_size,
            read_cache_size_bytes,
            write_cache_size_bytes,
        )?;
        if mem.needs_repair()? {
            #[cfg(feature = "logging")]
            warn!("Database {:?} not shutdown cleanly. Repairing", &file_path);
            Self::do_repair(&mut mem)?;
        }

        mem.begin_writable()?;
        let next_transaction_id = mem.get_last_committed_transaction_id()?.next();

        Ok(Database {
            mem,
            next_transaction_id: AtomicTransactionId::new(next_transaction_id),
            transaction_tracker: Arc::new(Mutex::new(TransactionTracker::new())),
            live_write_transaction: Mutex::new(None),
        })
    }

    fn allocate_read_transaction(&self) -> Result<TransactionId> {
        let mut guard = self.transaction_tracker.lock().unwrap();
        let id = self.mem.get_last_committed_transaction_id()?;
        guard.register_read_transaction(id);

        Ok(id)
    }

    pub(crate) fn allocate_savepoint(&self) -> Result<(SavepointId, TransactionId)> {
        let id = self
            .transaction_tracker
            .lock()
            .unwrap()
            .allocate_savepoint();
        Ok((id, self.allocate_read_transaction()?))
    }

    pub(crate) fn increment_transaction_id(&self) -> TransactionId {
        self.next_transaction_id.next()
    }

    /// Convenience method for [`Builder::new`]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a single
    /// write may be in progress at a time. If a write is in progress, this function will block
    /// until it completes.
    pub fn begin_write(&self) -> Result<WriteTransaction> {
        WriteTransaction::new(self, self.transaction_tracker.clone())
    }

    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    pub fn begin_read(&self) -> Result<ReadTransaction> {
        let id = self.allocate_read_transaction()?;
        #[cfg(feature = "logging")]
        info!("Beginning read transaction id={:?}", id);
        Ok(ReadTransaction::new(
            self.get_memory(),
            self.transaction_tracker.clone(),
            id,
        ))
    }
}

/// Configuration builder of a redb [Database].
pub struct Builder {
    page_size: usize,
    region_size: Option<usize>,
    read_cache_size_bytes: usize,
    write_cache_size_bytes: usize,
}

impl Builder {
    /// Construct a new [Builder] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `read_cache_size_bytes`: 1GiB
    /// - `write_cache_size_bytes`: 100MiB
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            // Default to 4k pages. Benchmarking showed that this was a good default on all platforms,
            // including MacOS with 16k pages. Therefore, users are not allowed to configure it at the moment.
            // It is part of the file format, so can be enabled in the future.
            page_size: PAGE_SIZE,
            region_size: None,
            // TODO: Default should probably take into account the total system memory
            read_cache_size_bytes: 1024 * 1024 * 1024,
            // TODO: Default should probably take into account the total system memory
            write_cache_size_bytes: 100 * 1024 * 1024,
        }
    }

    /// Set the internal page size of the database
    ///
    /// Valid values are powers of two, greater than or equal to 512
    ///
    /// ## Defaults
    ///
    /// Default to 4 Kib pages.
    #[cfg(any(fuzzing, test))]
    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = std::cmp::max(size, 512);
        self
    }

    /// Set the amount of memory (in bytes) used for caching data that has been read
    pub fn set_read_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.read_cache_size_bytes = bytes;
        self
    }

    /// Set the amount of memory (in bytes) used for caching data that has been written
    pub fn set_write_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.write_cache_size_bytes = bytes;
        self
    }

    #[cfg(test)]
    fn set_region_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Database::new(
            file,
            self.page_size,
            self.region_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
        )
    }

    /// Opens an existing redb database.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database> {
        if !path.as_ref().exists() {
            Err(Error::Io(ErrorKind::NotFound.into()))
        } else if File::open(path.as_ref())?.metadata()?.len() > 0 {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            Database::new(
                file,
                self.page_size,
                None,
                self.read_cache_size_bytes,
                self.write_cache_size_bytes,
            )
        } else {
            Err(Error::Io(io::Error::from(ErrorKind::InvalidData)))
        }
    }
}

// This just makes it easier to throw `dbg` etc statements on `Result<Database>`
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}

#[cfg(test)]
mod test {
    use tempfile::NamedTempFile;

    use crate::{Database, Durability, ReadableTable, TableDefinition};

    #[test]
    fn small_pages() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
        let txn = db.begin_write().unwrap();
        {
            txn.open_table(table_definition).unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn small_pages2() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        let db = Database::builder()
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint0 = tx.savepoint().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint1 = tx.savepoint().unwrap();
        tx.restore_savepoint(&savepoint0).unwrap();
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&660503, 489).unwrap().as_mut().fill(0xFF);
            assert!(t.remove(&291295).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        tx.restore_savepoint(&savepoint0).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint2 = tx.savepoint().unwrap();
        drop(savepoint0);
        tx.restore_savepoint(&savepoint2).unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.get(&2059).unwrap().is_none());
            assert!(t.remove(&145227).unwrap().is_none());
            assert!(t.remove(&145227).unwrap().is_none());
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint3 = tx.savepoint().unwrap();
        drop(savepoint1);
        tx.restore_savepoint(&savepoint3).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint4 = tx.savepoint().unwrap();
        drop(savepoint2);
        tx.restore_savepoint(&savepoint3).unwrap();
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.remove(&207936).unwrap().is_none());
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        let savepoint5 = tx.savepoint().unwrap();
        drop(savepoint3);
        assert!(tx.restore_savepoint(&savepoint4).is_err());
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Paranoid);
        tx.restore_savepoint(&savepoint5).unwrap();
        tx.set_durability(Durability::None);
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();
    }

    #[test]
    fn small_pages3() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        let db = Database::builder()
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let mut tx = db.begin_write().unwrap();
        let _savepoint0 = tx.savepoint().unwrap();
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 306];
            t.insert(&539717, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();

        let mut tx = db.begin_write().unwrap();
        let savepoint1 = tx.savepoint().unwrap();
        tx.restore_savepoint(&savepoint1).unwrap();
        tx.set_durability(Durability::None);
        {
            let mut t = tx.open_table(table_def).unwrap();
            let value = vec![0; 2008];
            t.insert(&784384, value.as_slice()).unwrap();
        }
        tx.abort().unwrap();
    }

    #[test]
    fn small_pages4() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

        let db = Database::builder()
            .set_read_cache_size(1024 * 1024)
            .set_write_cache_size(0)
            .set_page_size(1024)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            assert!(t.get(&131072).unwrap().is_none());
            let value = vec![0xFF; 1130];
            t.insert(&42394, value.as_slice()).unwrap();
            t.insert_reserve(&744037, 3645).unwrap().as_mut().fill(0xFF);
            assert!(t.get(&0).unwrap().is_none());
        }
        tx.abort().unwrap();

        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(table_def).unwrap();
            t.insert_reserve(&118749, 734).unwrap().as_mut().fill(0xFF);
        }
        tx.abort().unwrap();
    }

    #[test]
    fn dynamic_shrink() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
        let big_value = vec![0u8; 1024];

        let db = Database::builder()
            .set_region_size(1024 * 1024)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..2048 {
                table.insert(&i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let file_size = tmpfile.as_file().metadata().unwrap().len();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..2048 {
                table.remove(&i).unwrap();
            }
        }
        txn.commit().unwrap();

        // Perform a couple more commits to be sure the database has a chance to compact
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            table.insert(0, [].as_slice()).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            table.remove(0).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        txn.commit().unwrap();

        let final_file_size = tmpfile.as_file().metadata().unwrap().len();
        assert!(final_file_size < file_size);
    }
}
