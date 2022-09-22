use crate::tree_store::{
    get_db_size, AllPageNumbersBtreeIter, BtreeRangeIter, FreedTableKey, InternalTableDefinition,
    RawBtree, TableType, TransactionalMemory,
};
use crate::types::{RedbKey, RedbValue};
use crate::Error;
use crate::{ReadTransaction, Result, WriteTransaction};
use std::collections::btree_set::BTreeSet;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::ops::RangeFull;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::multimap_table::parse_subtree_roots;
#[cfg(feature = "logging")]
use log::info;

pub(crate) type TransactionId = u64;
type AtomicTransactionId = AtomicU64;

/// Defines the name and types of a table
///
/// A [`TableDefinition`] should be opened for use by calling [`ReadTransaction::open_table`] or [`WriteTransaction::open_table`]
pub struct TableDefinition<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> TableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
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

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Clone for TableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Copy for TableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Display for TableDefinition<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::redb_type_name(),
            V::redb_type_name()
        )
    }
}

/// Defines the name and types of a multimap table
///
/// A [`MultimapTableDefinition`] should be opened for use by calling [`ReadTransaction::open_multimap_table`] or [`WriteTransaction::open_multimap_table`]
///
/// [Multimap tables](https://en.wikipedia.org/wiki/Multimap) may have multiple values associated with each key
///
pub struct MultimapTableDefinition<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> MultimapTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
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

impl<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> Copy for MultimapTableDefinition<'a, K, V> {}

impl<'a, K: RedbKey + ?Sized, V: RedbKey + ?Sized> Display for MultimapTableDefinition<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::redb_type_name(),
            V::redb_type_name()
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
/// # let db_max_size = 1024 * 1024;
/// let db = unsafe { Database::create(filename, db_max_size)? };
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
    live_read_transactions: Mutex<BTreeSet<TransactionId>>,
    live_write_transaction: Mutex<Option<TransactionId>>,
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn create(path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let db_size = db_size as u64;
        let file = if path.as_ref().exists() && File::open(path.as_ref())?.metadata()?.len() > 0 {
            let existing_size = Self::get_db_size(path.as_ref())?;
            if existing_size != db_size {
                return Err(Error::DbSizeMismatch {
                    path: path.as_ref().to_string_lossy().to_string(),
                    size: existing_size,
                    requested_size: db_size,
                });
            }
            OpenOptions::new().read(true).write(true).open(path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?
        };

        Database::new(
            file,
            db_size,
            None,
            None,
            cfg!(unix),
            Some(WriteStrategy::default()),
        )
    }

    /// Opens an existing redb database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn open(path: impl AsRef<Path>) -> Result<Database> {
        if !path.as_ref().exists() {
            Err(Error::Io(ErrorKind::NotFound.into()))
        } else if File::open(path.as_ref())?.metadata()?.len() > 0 {
            let existing_size = Self::get_db_size(path.as_ref())?;
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            Database::new(file, existing_size, None, None, cfg!(unix), None)
        } else {
            Err(Error::Io(io::Error::from(ErrorKind::InvalidData)))
        }
    }

    #[inline]
    fn get_db_size(path: impl AsRef<Path>) -> Result<u64> {
        #[cfg(unix)]
        {
            Ok(get_db_size(path)?)
        }
        #[cfg(windows)]
        {
            get_db_size(path).map_err(|err| {
                // On Windows, an exclusive file lock also applies to reads, unlike on Unix, so
                // we detect that specific error code and transform it to the correct error
                // 0x21 - ERROR_LOCK_VIOLATION
                if matches!(err.raw_os_error(), Some(0x21)) {
                    Error::DatabaseAlreadyOpen
                } else {
                    err.into()
                }
            })
        }
    }

    pub(crate) fn get_memory(&self) -> &TransactionalMemory {
        &self.mem
    }

    fn verify_primary_checksums(mem: &TransactionalMemory) -> bool {
        let (root, root_checksum) = mem
            .get_data_root()
            .expect("Tried to repair an empty database");
        if !RawBtree::new(
            Some((root, root_checksum)),
            str::fixed_width(),
            InternalTableDefinition::fixed_width(),
            mem,
        )
        .verify_checksum()
        {
            return false;
        }

        if let Some((freed_root, freed_checksum)) = mem.get_freed_root() {
            if !RawBtree::new(
                Some((freed_root, freed_checksum)),
                FreedTableKey::fixed_width(),
                None,
                mem,
            )
            .verify_checksum()
            {
                return false;
            }
        }

        // Iterate over all other tables
        let mut iter: BtreeRangeIter<str, InternalTableDefinition> =
            BtreeRangeIter::new::<RangeFull, str>(.., Some(root), mem);
        while let Some(entry) = iter.next() {
            let definition = InternalTableDefinition::from_bytes(entry.value());
            if let Some((table_root, table_checksum)) = definition.get_root() {
                if !RawBtree::new(
                    Some((table_root, table_checksum)),
                    definition.get_fixed_key_size(),
                    definition.get_fixed_value_size(),
                    mem,
                )
                .verify_checksum()
                {
                    return false;
                }
            }
        }

        true
    }

    fn new(
        file: File,
        max_capacity: u64,
        page_size: Option<usize>,
        region_size: Option<usize>,
        dynamic_growth: bool,
        write_strategy: Option<WriteStrategy>,
    ) -> Result<Self> {
        #[cfg(feature = "logging")]
        info!(
            "Opening database {:?} with max size {}",
            &file, max_capacity
        );
        let mut mem = TransactionalMemory::new(
            file,
            max_capacity,
            page_size,
            region_size,
            dynamic_growth,
            write_strategy,
        )?;
        if mem.needs_repair()? {
            if mem.needs_checksum_verification()? && !Self::verify_primary_checksums(&mem) {
                mem.repair_primary_corrupted();
                assert!(Self::verify_primary_checksums(&mem));
            }

            mem.begin_repair()?;

            let (root, root_checksum) = mem
                .get_data_root()
                .expect("Tried to repair an empty database");

            // Repair the allocator state
            // All pages in the master table
            let master_pages_iter = AllPageNumbersBtreeIter::new(root, None, None, &mem);
            mem.mark_pages_allocated(master_pages_iter)?;

            // Iterate over all other tables
            let mut iter: BtreeRangeIter<str, InternalTableDefinition> =
                BtreeRangeIter::new::<RangeFull, str>(.., Some(root), &mem);

            // Chain all the other tables to the master table iter
            while let Some(entry) = iter.next() {
                let definition = InternalTableDefinition::from_bytes(entry.value());
                if let Some((table_root, _)) = definition.get_root() {
                    let table_pages_iter = AllPageNumbersBtreeIter::new(
                        table_root,
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        &mem,
                    );
                    mem.mark_pages_allocated(table_pages_iter)?;

                    // Multimap tables may have additional subtrees in their values
                    if definition.get_type() == TableType::Multimap {
                        let table_pages_iter = AllPageNumbersBtreeIter::new(
                            table_root,
                            definition.get_fixed_key_size(),
                            definition.get_fixed_value_size(),
                            &mem,
                        );
                        for table_page in table_pages_iter {
                            let page = mem.get_page(table_page);
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
            drop(iter);

            mem.end_repair()?;

            // Clear the freed table. We just rebuilt the allocator state by walking all the
            // reachable data pages, which implicitly frees the pages for the freed table
            let transaction_id = mem.get_last_committed_transaction_id()? + 1;
            mem.commit(
                Some((root, root_checksum)),
                None,
                transaction_id,
                false,
                None,
            )?;
        }

        let next_transaction_id = mem.get_last_committed_transaction_id()? + 1;

        Ok(Database {
            mem,
            next_transaction_id: AtomicTransactionId::new(next_transaction_id),
            live_write_transaction: Mutex::new(None),
            live_read_transactions: Mutex::new(Default::default()),
        })
    }

    pub(crate) fn deallocate_read_transaction(&self, id: TransactionId) {
        self.live_read_transactions.lock().unwrap().remove(&id);
    }

    pub(crate) fn deallocate_write_transaction(&self, id: TransactionId) {
        let mut live = self.live_write_transaction.lock().unwrap();
        assert_eq!(Some(id), *live);
        *live = None;
    }

    pub(crate) fn oldest_live_read_transaction(&self) -> Option<TransactionId> {
        self.live_read_transactions
            .lock()
            .unwrap()
            .iter()
            .next()
            .cloned()
    }

    fn increment_transaction_id(&self) -> TransactionId {
        self.next_transaction_id.fetch_add(1, Ordering::AcqRel)
    }

    /// Convenience method for [`Builder::new`]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Changes the write strategy of the database for future [`WriteTransaction`]s.
    ///
    /// Note: Changing to the [`WriteStrategy::Checksum`] strategy can take a long time, as checksums
    /// will need to be calculated for every entry in the database
    pub fn set_write_strategy(&self, strategy: WriteStrategy) -> Result {
        let write_guard = self.live_write_transaction.lock().unwrap();
        assert!(write_guard.is_none());
        // TODO: implement switching to checksum strategy
        assert!(matches!(strategy, WriteStrategy::TwoPhase));

        let id = self.increment_transaction_id();
        let root_page = self.mem.get_data_root();
        let freed_root = self.mem.get_freed_root();
        self.mem
            .commit(root_page, freed_root, id, false, Some(strategy.into()))?;
        drop(write_guard);

        Ok(())
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a single
    /// write may be in progress at a time
    pub fn begin_write(&self) -> Result<WriteTransaction> {
        assert!(self.live_write_transaction.lock().unwrap().is_none());
        let id = self.increment_transaction_id();
        *self.live_write_transaction.lock().unwrap() = Some(id);
        // Safety: We just asserted there was no previous write in progress
        #[cfg(feature = "logging")]
        info!("Beginning write transaction id={}", id);
        unsafe { WriteTransaction::new(self, id) }
    }

    /// Begins a read transaction
    ///
    /// Captures a snapshot of the database, so that only data committed before calling this method
    /// is visible in the transaction
    ///
    /// Returns a [`ReadTransaction`] which may be used to read from the database. Read transactions
    /// may exist concurrently with writes
    pub fn begin_read(&self) -> Result<ReadTransaction> {
        let id = self.increment_transaction_id();
        self.live_read_transactions.lock().unwrap().insert(id);
        #[cfg(feature = "logging")]
        info!("Beginning read transaction id={}", id);
        Ok(ReadTransaction::new(self, id))
    }
}

/// redb can be configured to use one of two write-and-commit strategies.
///
/// Both strategies have security tradeoffs in situations where an attacker has a high degree of
/// control over the database workload. For example being able to control the exact order of reads
/// and writes, or being able to crash the database process at will.
#[derive(Default, Copy, Clone)]
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
    #[default]
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

pub struct Builder {
    page_size: Option<usize>,
    region_size: Option<usize>,
    dynamic_growth: bool,
    write_strategy: WriteStrategy,
}

impl Builder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            page_size: None,
            region_size: None,
            dynamic_growth: cfg!(unix),
            write_strategy: WriteStrategy::default(),
        }
    }

    /// Set the internal page size of the database
    /// Larger page sizes will reduce the database file's overhead, but may decrease write performance
    /// Defaults to the native OS page size
    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = Some(size);
        self
    }

    pub fn set_write_strategy(&mut self, write_strategy: WriteStrategy) -> &mut Self {
        self.write_strategy = write_strategy;
        self
    }

    /// Set the internal region size of the database
    /// Smaller regions may allow the database to compact more effectively, but will limit the maximum
    /// size of values that can be stored
    /// Defaults to 4GiB
    pub fn set_region_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// Whether to grow the database file dynamically.
    /// When set to true, the database file will start at a small size and grow as insertions are made
    /// When set to false, the database file will be statically sized
    #[cfg(unix)]
    pub fn set_dynamic_growth(&mut self, enabled: bool) -> &mut Self {
        self.dynamic_growth = enabled;
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn create(&self, path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Database::new(
            file,
            db_size as u64,
            self.page_size,
            self.region_size,
            self.dynamic_growth,
            Some(self.write_strategy),
        )
    }
}

// This just makes it easier to throw `dbg` etc statements on `Result<Database>`
impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish()
    }
}
