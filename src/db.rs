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
        // Custom alignment is not currently supported
        assert!(K::ALIGNMENT == 1);
        assert!(V::ALIGNMENT == 1);
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    /// Returns a reference of the `name` of the current `TableDefinition`.
    pub fn name(&self) -> &str {
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
        // Custom alignment is not currently supported
        assert!(K::ALIGNMENT == 1);
        assert!(V::ALIGNMENT == 1);
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
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
            let definition = entry.value();
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

    #[allow(clippy::too_many_arguments)]
    fn new(
        file: File,
        use_mmap: bool,
        page_size: usize,
        region_size: Option<usize>,
        initial_size: Option<u64>,
        read_cache_size_bytes: usize,
        write_cache_size_bytes: usize,
        write_strategy: Option<WriteStrategy>,
    ) -> Result<Self> {
        #[cfg(feature = "logging")]
        let file_path = format!("{:?}", &file);
        #[cfg(feature = "logging")]
        info!("Opening database {:?}", &file_path);
        let mut mem = TransactionalMemory::new(
            file,
            use_mmap,
            page_size,
            region_size,
            initial_size,
            read_cache_size_bytes,
            write_cache_size_bytes,
            write_strategy,
        )?;
        if mem.needs_repair()? {
            #[cfg(feature = "logging")]
            warn!("Database {:?} not shutdown cleanly. Repairing", &file_path);

            if mem.needs_checksum_verification()? && !Self::verify_primary_checksums(&mem)? {
                mem.repair_primary_corrupted();
                assert!(Self::verify_primary_checksums(&mem)?);
            }

            mem.begin_repair()?;

            let (root, root_checksum) = mem
                .get_data_root()
                .expect("Tried to repair an empty database");

            // Repair the allocator state
            // All pages in the master table
            let master_pages_iter = AllPageNumbersBtreeIter::new(root, None, None, &mem)?;
            mem.mark_pages_allocated(master_pages_iter)?;

            // Iterate over all other tables
            let iter: BtreeRangeIter<&str, InternalTableDefinition> =
                BtreeRangeIter::new::<RangeFull, &str>(.., Some(root), &mem)?;

            // Chain all the other tables to the master table iter
            for entry in iter {
                let definition = entry.value();
                if let Some((table_root, _)) = definition.get_root() {
                    let table_pages_iter = AllPageNumbersBtreeIter::new(
                        table_root,
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        &mem,
                    )?;
                    mem.mark_pages_allocated(table_pages_iter)?;

                    // Multimap tables may have additional subtrees in their values
                    if definition.get_type() == TableType::Multimap {
                        let table_pages_iter = AllPageNumbersBtreeIter::new(
                            table_root,
                            definition.get_fixed_key_size(),
                            definition.get_fixed_value_size(),
                            &mem,
                        )?;
                        for table_page in table_pages_iter {
                            let page = mem.get_page(table_page)?;
                            let mut subtree_roots = parse_subtree_roots(
                                &page,
                                definition.get_fixed_key_size(),
                                definition.get_fixed_value_size(),
                            );
                            mem.mark_pages_allocated(subtree_roots.drain(..))?;
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
                None,
            )?;
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

    // TODO: we could probably remove this method and pass this clone into the Transaction objects
    pub(crate) fn transaction_tracker(&self) -> Arc<Mutex<TransactionTracker>> {
        self.transaction_tracker.clone()
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

    /// Changes the write strategy of the database for future [`WriteTransaction`]s.
    ///
    /// Calling this method will invalidate all existing [`crate::Savepoint`]s.
    ///
    /// Note: Changing to the [`WriteStrategy::Checksum`] strategy can take a long time, as checksums
    /// will need to be calculated for every entry in the database
    pub fn set_write_strategy(&self, strategy: WriteStrategy) -> Result {
        let mut tracker = self.transaction_tracker.lock().unwrap();
        tracker.invalidate_all_savepoints();

        let guard = self.live_write_transaction.lock().unwrap();
        assert!(guard.is_none());
        // TODO: implement switching to checksum strategy
        assert!(matches!(strategy, WriteStrategy::TwoPhase));

        let id = self.increment_transaction_id();
        let root_page = self.mem.get_data_root();
        let freed_root = self.mem.get_freed_root();
        self.mem
            .commit(root_page, freed_root, id, false, Some(strategy.into()))?;
        drop(guard);

        drop(tracker);

        Ok(())
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a single
    /// write may be in progress at a time. If a write is in progress, this function will block
    /// until it completes.
    pub fn begin_write(&self) -> Result<WriteTransaction> {
        WriteTransaction::new(self)
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
        Ok(ReadTransaction::new(self, id))
    }
}

/// redb can be configured to use one of two write-and-commit strategies.
///
/// Both strategies have security tradeoffs in situations where an attacker has a high degree of
/// control over the database workload. For example being able to control the exact order of reads
/// and writes, or being able to crash the database process at will.
#[derive(Copy, Clone)]
pub enum WriteStrategy {
    /// Optimize for minimum write transaction latency by calculating and storing recursive checksums
    /// of database contents, with a single-phase [`WriteTransaction::commit`] that makes a single
    /// call to `fsync`.
    ///
    /// Data is written with checksums, with the following commit algorithm:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Flip the god byte primary bit to activate the newly updated commit slot
    /// 3. Call `fsync` to ensure all writes have been persisted to disk
    ///
    /// When opening the database after a crash, the most recent of the two commit slots with a
    /// valid checksum is used.
    ///
    /// This write strategy requires calculating checksums as data is written, which decreases write
    /// throughput, but only requires one call to `fsync`, which decreases commit latency.
    ///
    /// Security considerations: The checksum used is xxhash, a fast, non-cryptographic hash
    /// function with close to perfect collision resistance when used with non-malicious input. An
    /// attacker with an extremely high degree of control over the database's workload, including
    /// the ability to cause the database process to crash, can cause invalid data to be written
    /// with a valid checksum, leaving the database in an invalid, attacker-controlled state.
    Checksum,
    /// Optimize for maximum write transaction throughput by omitting checksums, with a two-phase
    /// [`WriteTransaction::commit`] that makes two calls to `fsync`.
    ///
    /// Data is written without checksums, with the following commit algorithm:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Call `fsync` to ensure the database slate and commit slot update have been persisted
    /// 3. Flip the god byte primary bit to activate the newly updated commit slot
    /// 4. Call `fsync` to ensure the write to the god byte has been persisted
    ///
    /// When opening the database after a crash, the got byte primary bit will always point to the
    /// most recent valid commit.
    ///
    /// This write strategy avoids calculating checksums, which increases write throughput, but
    /// requires two calls to fsync, which increases commit latency.
    ///
    /// Security considerations: Many hard disk drives and SSDs do not actually guarantee that data
    /// has been persisted to disk after calling `fsync`. An attacker with a high degree of control
    /// over the database's workload, including the ability to cause the database process to crash,
    /// can cause the database to crash with the god byte primary bit pointing to an invalid commit
    /// slot, leaving the database in an invalid, potentially attacker-controlled state.
    TwoPhase,
}

/// Configuration builder of a redb [Database].
pub struct Builder {
    page_size: usize,
    region_size: Option<usize>,
    initial_size: Option<u64>,
    read_cache_size_bytes: usize,
    write_cache_size_bytes: usize,
    write_strategy: Option<WriteStrategy>,
}

impl Builder {
    /// Construct a new [Builder] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `read_cache_size_bytes`: 1GiB
    /// - `write_cache_size_bytes`: 100MiB
    /// - `write_strategy`: [WriteStrategy::Checksum]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            // Default to 4k pages. Benchmarking showed that this was a good default on all platforms,
            // including MacOS with 16k pages. Therefore, users are not allowed to configure it at the moment.
            // It is part of the file format, so can be enabled in the future.
            page_size: PAGE_SIZE,
            region_size: None,
            initial_size: None,
            // TODO: Default should probably take into account the total system memory
            read_cache_size_bytes: 1024 * 1024 * 1024,
            // TODO: Default should probably take into account the total system memory
            write_cache_size_bytes: 100 * 1024 * 1024,
            write_strategy: None,
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

    /// Set the write strategy of the database.
    ///
    /// See [WriteStrategy] for details.
    pub fn set_write_strategy(&mut self, write_strategy: WriteStrategy) -> &mut Self {
        self.write_strategy = Some(write_strategy);
        self
    }

    /// Set the amount of memory (in bytes) used for caching data that has been read
    ///
    /// This setting is ignored when calling `create_mmapped()`/`open_mmapped()`
    pub fn set_read_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.read_cache_size_bytes = bytes;
        self
    }

    /// Set the amount of memory (in bytes) used for caching data that has been written
    ///
    /// This setting is ignored when calling `create_mmapped()`/`open_mmapped()`
    pub fn set_write_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.write_cache_size_bytes = bytes;
        self
    }

    #[cfg(test)]
    #[cfg(unix)]
    fn set_region_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// The initial amount of usable space in bytes for the database
    ///
    /// Databases grow dynamically, so it is generally unnecessary to set this. However, it can
    /// be used to avoid runtime overhead caused by resizing the database.
    pub fn set_initial_size(&mut self, size: u64) -> &mut Self {
        self.initial_size = Some(size);
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
            false,
            self.page_size,
            self.region_size,
            self.initial_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            self.write_strategy,
        )
    }

    /// Opens the specified file as a redb database using the mmap backend.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// # Safety
    ///
    /// Caller must ensure that the memory representing the memory-mapped file is not modified externally.
    /// In particular:
    /// 1) the file referenced by `path` must not be concurrently modified by any other process
    /// 2) an I/O failure writing back to disk must not mutate the the memory. You should consider
    ///    reading this paper before assuming that your OS provides this gaurantee: <https://research.cs.wisc.edu/adsl/Publications/cuttlefs-tos21.pdf>
    pub unsafe fn create_mmapped(&self, path: impl AsRef<Path>) -> Result<Database> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Database::new(
            file,
            true,
            self.page_size,
            self.region_size,
            self.initial_size,
            self.read_cache_size_bytes,
            self.write_cache_size_bytes,
            self.write_strategy,
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
                false,
                self.page_size,
                None,
                self.initial_size,
                self.read_cache_size_bytes,
                self.write_cache_size_bytes,
                None,
            )
        } else {
            Err(Error::Io(io::Error::from(ErrorKind::InvalidData)))
        }
    }

    /// Opens an existing redb database using the mmap backend.
    ///
    /// # Safety
    ///
    /// Caller must ensure that the memory representing the memory-mapped file is not modified externally.
    /// In particular:
    /// 1) the file referenced by `path` must not be concurrently modified by any other process
    /// 2) an I/O failure writing back to disk must not mutate the the memory. You should consider
    ///    reading this paper before assuming that your OS provides this gaurantee: <https://research.cs.wisc.edu/adsl/Publications/cuttlefs-tos21.pdf>
    pub unsafe fn open_mmapped(&self, path: impl AsRef<Path>) -> Result<Database> {
        if !path.as_ref().exists() {
            Err(Error::Io(ErrorKind::NotFound.into()))
        } else if File::open(path.as_ref())?.metadata()?.len() > 0 {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            Database::new(
                file,
                true,
                self.page_size,
                None,
                self.initial_size,
                self.read_cache_size_bytes,
                self.write_cache_size_bytes,
                None,
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

    use crate::{Database, Durability, ReadableTable, TableDefinition, WriteStrategy};

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
            .set_write_strategy(WriteStrategy::TwoPhase)
            .set_page_size(512)
            .create(tmpfile.path())
            .unwrap();

        let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

        let tx = db.begin_write().unwrap();
        let savepoint0 = tx.savepoint().unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
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
        tx.restore_savepoint(&savepoint0).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
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
        let savepoint3 = tx.savepoint().unwrap();
        drop(savepoint1);
        tx.restore_savepoint(&savepoint3).unwrap();
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
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
        let savepoint5 = tx.savepoint().unwrap();
        drop(savepoint3);
        assert!(tx.restore_savepoint(&savepoint4).is_err());
        {
            tx.open_table(table_def).unwrap();
        }
        tx.commit().unwrap();

        let mut tx = db.begin_write().unwrap();
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
            .set_write_strategy(WriteStrategy::Checksum)
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
            .set_write_strategy(WriteStrategy::Checksum)
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
    #[cfg(unix)]
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
