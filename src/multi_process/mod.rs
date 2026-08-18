//! A multi-process safe interface to a redb database.
//!
//! [`MultiProcessDatabase`] stores its database in a directory, alongside the lock files that
//! coordinate the processes using it. One write transaction may be in progress at a time across
//! every process, and any number of processes may read concurrently with it and with each other.
//!
//! # Protocol
//!
//! The directory contains:
//!
//! | file | purpose |
//! |------|---------|
//! | `data.redb` | the database itself, in the ordinary redb file format |
//! | `write.lock` | held exclusively by the process that owns the single logical writer |
//! | `registry.lock` | the shared state below, protected by the lock on itself |
//! | `readers/<n>` | one slot per process, holding the oldest transaction that process needs |
//!
//! `registry.lock` holds the writer mode, the number of reader slots, the last transaction that
//! has been made durable, and the highest free horizon any writer has used. Readers take it shared
//! and writers take it exclusively, which is what makes publishing a pinned transaction atomic
//! with respect to a writer scanning for the oldest one.
//!
//! A process publishes the oldest transaction it needs into its own slot, which it holds
//! exclusively locked for as long as it has the database open. A writer finds the slots that are
//! in use by failing to take a shared lock on them, so a process that exits -- for any reason --
//! stops holding pages back without leaving anything to clean up.
//!
//! See `docs/design.md` for why that is enough to keep a page from being reused while another
//! process can still see it.

mod coordinator;
mod locks;

pub(crate) use coordinator::ProcessCoordinator;
pub use locks::WriterMode;

use crate::db::{OpenParams, RepairSession};
use crate::sealed::Sealed;
use crate::transaction_tracker::TransactionTracker;
use crate::tree_store::PAGE_SIZE;
use crate::tree_store::file_backend::{FileBackend, FileLockKind};
use crate::{
    CacheStats, CommitError, Database, DatabaseError, ReadOnlyDatabase, ReadTransaction,
    ReadableDatabase, Result, TransactionError, WriteTransaction,
};
use locks::{DatabaseDir, WriteLock};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

/// A redb database that may be used from several processes at once.
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`], and [`Self::begin_write`] to get a
/// [`WriteTransaction`]. Reads never block, in this process or any other. Writes are serialized
/// across all processes: exactly one write transaction may be in progress at a time, and which
/// processes may start one depends on the [`WriterMode`] the database was created with.
///
/// Unlike [`Database`], `path` names a directory rather than a file. The directory is created by
/// [`Self::create`], and holds the database file along with the lock files that coordinate the
/// processes using it, so it must be on a filesystem that supports file locking. Nothing else may
/// be put in it.
///
/// # Limitations
///
/// This is a prototype. Compared to [`Database`]:
///
/// * [`Durability::None`](crate::Durability) is rejected in
///   [`WriterMode::MultiWriterProcess`], since a non-durable commit is only visible to the process
///   that made it.
/// * Compaction and [`Database::check_integrity`] are not available.
/// * Every commit is 2-phase, which costs an extra `fsync`: a reader in another process would
///   otherwise be able to see a header naming pages that are not in the file yet.
/// * If a writer process dies part way through a commit in
///   [`WriterMode::MultiWriterProcess`], the database must be reopened to repair it; the next
///   [`Self::begin_write`] fails rather than repairing in place, since a repair rebuilds state
///   that live read transactions may be using.
///
/// # Examples
///
/// ```rust
/// use redb::*;
/// # use tempfile::TempDir;
/// const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");
///
/// # fn main() -> Result<(), Error> {
/// # let tmpdir = TempDir::new().unwrap();
/// # let path = tmpdir.path().join("my_db");
/// let db = MultiProcessDatabase::create(&path)?;
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_table(TABLE)?;
///     table.insert(&0, &0)?;
/// }
/// write_txn.commit()?;
///
/// // Any number of other processes may open the same directory and read it
/// let read_only = MultiProcessDatabase::open_read_only(&path)?;
/// let read_txn = read_only.begin_read()?;
/// assert_eq!(0, read_txn.open_table(TABLE)?.get_owned(0)?.unwrap().value());
/// # Ok(())
/// # }
/// ```
pub struct MultiProcessDatabase {
    inner: Database,
    coordinator: Arc<ProcessCoordinator>,
}

impl MultiProcessDatabase {
    /// Opens the directory at `path` as a multi-process database, creating it if it does not
    /// exist.
    ///
    /// A database created this way uses [`WriterMode::MultiWriterProcess`]. Use
    /// [`MultiProcessBuilder::set_writer_mode`] to create one that only a single process may write
    /// to, which is faster for that process.
    pub fn create(path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        Self::builder().create(path)
    }

    /// Opens an existing multi-process database.
    pub fn open(path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        Self::builder().open(path)
    }

    /// Opens an existing multi-process database for reading only.
    ///
    /// Any number of processes may do this, concurrently with each other and with a writer.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<ReadOnlyDatabase, DatabaseError> {
        Self::builder().open_read_only(path)
    }

    /// Convenience method for [`MultiProcessBuilder::new`]
    pub fn builder() -> MultiProcessBuilder {
        MultiProcessBuilder::new()
    }

    /// The mode this database was created with, which governs which processes may write to it
    pub fn writer_mode(&self) -> WriterMode {
        self.coordinator.mode()
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a
    /// single write may be in progress at a time, across every process which has the database
    /// open. If a write is in progress, this function blocks until it completes.
    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        self.inner.begin_write()
    }

    /// The last transaction that has been committed and made durable, by any process.
    ///
    /// Intended for testing and for observing another process's progress; the value is only
    /// meaningful relative to itself.
    pub fn last_durable_commit(&self) -> Result<u64> {
        Ok(self.coordinator.last_committed()?.raw_id())
    }
}

impl Sealed for MultiProcessDatabase {}

impl ReadableDatabase for MultiProcessDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        self.inner.begin_read()
    }

    fn cache_stats(&self) -> CacheStats {
        self.inner.cache_stats()
    }
}

impl std::fmt::Debug for MultiProcessDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiProcessDatabase")
            .field("mode", &self.coordinator.mode())
            .finish_non_exhaustive()
    }
}

/// Configuration builder of a [`MultiProcessDatabase`].
pub struct MultiProcessBuilder {
    cache_size: usize,
    mode: Option<WriterMode>,
    // Only configurable in test and fuzzing builds, exactly as for `crate::Builder`
    region_size: Option<u64>,
    repair_callback: Box<dyn Fn(&mut RepairSession)>,
}

impl MultiProcessBuilder {
    /// Construct a new [`MultiProcessBuilder`] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `cache_size_bytes`: 1GiB
    /// - `writer_mode`: [`WriterMode::MultiWriterProcess`]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cache_size: 1024 * 1024 * 1024,
            mode: None,
            region_size: None,
            repair_callback: Box::new(|_| {}),
        }
    }

    /// Set the amount of memory (in bytes) used for caching data
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.cache_size = bytes;
        self
    }

    /// Set which processes may write to the database.
    ///
    /// The mode is fixed when the database is created. Setting it before opening an existing
    /// database asserts that it was created with that mode, and fails if it was not.
    pub fn set_writer_mode(&mut self, mode: WriterMode) -> &mut Self {
        self.mode = Some(mode);
        self
    }

    /// Set a callback which will be invoked periodically in the event that the database file needs
    /// to be repaired. See [`crate::Builder::set_repair_callback`].
    pub fn set_repair_callback(
        &mut self,
        callback: impl Fn(&mut RepairSession) + 'static,
    ) -> &mut Self {
        self.repair_callback = Box::new(callback);
        self
    }

    #[cfg(any(test, fuzzing))]
    pub fn set_region_size(&mut self, size: u64) -> &mut Self {
        assert!(size.is_power_of_two());
        self.region_size = Some(size);
        self
    }

    /// Opens the directory at `path` as a multi-process database, creating it if it does not exist
    pub fn create(&self, path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        let dir = DatabaseDir::new(path);
        dir.prepare(true)?;
        let mode = dir.init_registry(self.mode.unwrap_or(WriterMode::MultiWriterProcess))?;
        self.check_mode(mode)?;
        self.open_inner(&dir, mode, true)
    }

    /// Opens an existing multi-process database
    pub fn open(&self, path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        let dir = DatabaseDir::new(path);
        dir.prepare(false)?;
        let mode = dir.mode()?;
        self.check_mode(mode)?;
        self.open_inner(&dir, mode, false)
    }

    /// Opens an existing multi-process database for reading only
    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let dir = DatabaseDir::new(path);
        dir.prepare(false)?;
        let mode = dir.mode()?;
        self.check_mode(mode)?;

        let file = OpenOptions::new().read(true).open(dir.data_file())?;
        // No file lock: the lock files exclude concurrent writers, and this handle only reads
        let backend = FileBackend::new_internal(file, FileLockKind::None)?;
        let (mem, coordinator) = ReadOnlyDatabase::new_shared_read(
            Box::new(backend),
            PAGE_SIZE,
            self.cache_size,
            |mem| {
                let write_lock = WriteLock::open(&dir)?;
                Ok(Arc::new(ProcessCoordinator::new(
                    &dir, mode, true, write_lock, mem,
                )?))
            },
        )?;
        // A read-only handle never publishes a commit, so its idea of what is current comes
        // entirely from the first refresh in begin_read()
        Ok(ReadOnlyDatabase::from_parts(
            mem.clone(),
            Arc::new(TransactionTracker::new_multi_process(
                mem.get_last_committed_transaction_id()?.next(),
                coordinator,
            )),
        ))
    }

    fn check_mode(&self, actual: WriterMode) -> Result<(), DatabaseError> {
        if let Some(requested) = self.mode
            && requested != actual
        {
            return Err(crate::StorageError::Corrupted(format!(
                "Database was created with {actual:?}, but {requested:?} was requested"
            ))
            .into());
        }
        Ok(())
    }

    fn open_inner(
        &self,
        dir: &DatabaseDir,
        mode: WriterMode,
        allow_create: bool,
    ) -> Result<MultiProcessDatabase, DatabaseError> {
        // Take the write lock before touching the database file, so that no other process can be
        // part way through a transaction while this one repairs or reads the file. In
        // SingleWriterProcess mode it is never released; in MultiWriterProcess mode it is handed
        // back at the end of this function, and retaken per transaction
        let mut write_lock = WriteLock::open(dir)?;
        match mode {
            WriterMode::SingleWriterProcess => {
                if !write_lock.try_acquire()? {
                    return Err(DatabaseError::DatabaseAlreadyOpen);
                }
            }
            WriterMode::MultiWriterProcess => write_lock.acquire()?,
        }

        // A database being created is initialized under a temporary name and renamed into place
        // once `Database` has accepted it, so that a crash part way through initialization leaves
        // an unfinished attempt of ours rather than a `data.redb` that must be refused forever
        let (file, location) = dir.open_data(allow_create)?;
        // No file lock: `write.lock` and the reader slots do that job, and every process needs the
        // file open at once
        let backend = FileBackend::new_internal(file, FileLockKind::None)?;

        let dir_for_coordinator = dir.clone();
        let inner = Database::new_multi_process(
            Box::new(backend),
            allow_create,
            &OpenParams {
                page_size: PAGE_SIZE,
                region_size: self.region_size,
                cache_size: self.cache_size,
                repair_callback: &self.repair_callback,
                // Restoring persistent savepoints is enough when this process is the only writer,
                // but not when another process could reclaim their pages without knowing about them
                restore_persistent_savepoints: matches!(mode, WriterMode::SingleWriterProcess),
            },
            move |mem| {
                Ok(Arc::new(ProcessCoordinator::new(
                    &dir_for_coordinator,
                    mode,
                    false,
                    write_lock,
                    mem,
                )?))
            },
        )?;

        if allow_create {
            // Both after `Database` has accepted the file, so that a create() pointed somewhere
            // that turns out not to hold a database fails without having left either a database
            // file or a marker behind
            dir.promote_data(location)?;
            dir.write_metadata_if_missing()?;
        }

        let coordinator = inner
            .transaction_tracker()
            .process()
            .expect("a multi-process database always has a coordinator")
            .clone();
        // A database that has just been created, or repaired after an unclean shutdown, has no
        // allocator state table yet: the repair rebuilds the state in memory, and only a commit
        // writes it out. The next process to take the write lock has no way to rebuild it, so one
        // is written below before that can happen
        let needs_allocator_state = Database::allocator_state_table(&inner.get_memory())?.is_none();
        let last_committed = inner.get_memory().get_last_committed_transaction_id()?;
        // Whatever this process has just read from the file is current, because it holds the write
        // lock. Publishing it also brings a registry that was created before the database file --
        // or left behind by a writer that died before publishing -- back in step
        coordinator.mark_state_current(last_committed)?;
        coordinator.publish_durable_commit(last_committed)?;
        // Hands the write lock to whichever process wants it next, in the mode where it is taken
        // per transaction
        coordinator.end_write();

        let db = MultiProcessDatabase { inner, coordinator };
        if needs_allocator_state && matches!(mode, WriterMode::MultiWriterProcess) {
            // An empty transaction, which commits with quick-repair like every other one in this
            // mode. Taken through the ordinary path, so that it contends for the write lock with
            // any other process rather than assuming this one still holds it
            let txn = db
                .begin_write()
                .map_err(TransactionError::into_storage_error)?;
            txn.commit().map_err(CommitError::into_storage_error)?;
        }

        Ok(db)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ReadableTableMetadata, TableDefinition};

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    fn write(db: &MultiProcessDatabase, keys: std::ops::Range<u64>, len: usize) {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            let value = vec![0xab; len];
            for key in keys {
                table.insert(&key, value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // Small regions, so that a modest amount of data pushes the database into new ones. A process
    // that has not written since then holds a layout describing fewer regions than the file has,
    // which it must not use to read -- or write -- the pages in them.
    fn small_region_builder() -> MultiProcessBuilder {
        let mut builder = MultiProcessDatabase::builder();
        builder
            .set_writer_mode(WriterMode::MultiWriterProcess)
            .set_region_size(1024 * 1024);
        builder
    }

    #[test]
    fn a_reader_follows_the_database_into_new_regions() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let path = tmpdir.path().join("db");
        let writer = small_region_builder().create(&path).unwrap();
        write(&writer, 0..10, 4096);

        let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
        assert_eq!(
            10,
            reader
                .begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .len()
                .unwrap()
        );

        // Several regions worth of data, none of which the reader's layout knows about
        write(&writer, 10..2000, 4096);

        let txn = reader.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(2000, table.len().unwrap());
        for key in [0, 1000, 1999] {
            assert_eq!(4096, table.get_owned(key).unwrap().unwrap().value().len());
        }
    }

    #[test]
    fn a_writer_picks_up_regions_another_process_added() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let path = tmpdir.path().join("db");
        let first = small_region_builder().create(&path).unwrap();
        let second = small_region_builder().open(&path).unwrap();
        write(&first, 0..10, 4096);

        // The second handle grows the database well past what the first has seen...
        write(&second, 10..2000, 4096);
        // ... and the first must reload the layout before it allocates against it
        write(&first, 2000..2500, 4096);

        let txn = second.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(2500, table.len().unwrap());
        for key in [0, 1500, 2499] {
            assert_eq!(4096, table.get_owned(key).unwrap().unwrap().value().len());
        }
    }

    #[test]
    fn the_file_shrinks_back_with_readers_attached() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let path = tmpdir.path().join("db");
        let writer = small_region_builder().create(&path).unwrap();
        write(&writer, 0..2000, 4096);
        let grown = std::fs::metadata(path.join("data.redb")).unwrap().len();

        let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
        assert_eq!(
            2000,
            reader
                .begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .len()
                .unwrap()
        );

        let txn = writer.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for key in 0..2000 {
                table.remove(&key).unwrap();
            }
        }
        txn.commit().unwrap();
        // Further commits, so that the pages the delete freed are released and the file trimmed.
        // Reclamation lags a commit behind in this mode, and each commit trims one region
        for _ in 0..8 {
            write(&writer, 0..1, 8);
        }

        let shrunk = std::fs::metadata(path.join("data.redb")).unwrap().len();
        assert!(
            shrunk < grown,
            "the file did not shrink: {grown} -> {shrunk}"
        );
        assert_eq!(
            1,
            reader
                .begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .len()
                .unwrap()
        );
    }

    #[test]
    fn opening_a_directory_that_is_not_a_database_fails() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        assert!(MultiProcessDatabase::open(tmpdir.path().join("missing")).is_err());

        let empty = tmpdir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        assert!(MultiProcessDatabase::open(&empty).is_err());
        assert!(MultiProcessDatabase::open_read_only(&empty).is_err());
    }

    #[test]
    fn create_is_idempotent() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let path = tmpdir.path().join("db");
        {
            let db = MultiProcessDatabase::create(&path).unwrap();
            write(&db, 0..10, 8);
        }
        let db = MultiProcessDatabase::create(&path).unwrap();
        assert_eq!(
            10,
            db.begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .len()
                .unwrap()
        );
    }
}
