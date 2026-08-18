//! A multi-process safe interface to a redb database.
//!
//! [`MultiProcessDatabase`] stores its database in a directory, alongside the files that
//! coordinate the processes using it. One write transaction may be in progress at a time across
//! every process, and any number of processes may read concurrently with it and with each other.
//! The protocol is described in the "Directory-structured databases" section of `docs/design.md`.

mod coordinator;
mod locks;

pub(crate) use coordinator::ProcessCoordinator;
#[cfg(feature = "experimental-multiprocess")]
pub use locks::WriterMode;
#[cfg(not(feature = "experimental-multiprocess"))]
pub(crate) use locks::WriterMode;

use crate::db::{OpenParams, RepairSession};
use crate::sealed::Sealed;
use crate::transaction_tracker::TransactionTracker;
use crate::tree_store::PAGE_SIZE;
use crate::tree_store::file_backend::{FileBackend, FileLockKind};
use crate::{
    CacheStats, CommitError, Database, DatabaseError, ReadOnlyDatabase, ReadTransaction,
    ReadableDatabase, Result, TransactionError, WriteTransaction,
};
use locks::{DataLocation, DatabaseDir, HeaderGuardedBackend, RegistryLock, WriteLock};
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

/// A redb database that may be used from several processes at once.
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`], and [`Self::begin_write`] to get a
/// [`WriteTransaction`]. Reads run concurrently with writes and with each other -- a reader sees
/// the snapshot it opened and is never waited on by the writer, though beginning a transaction
/// may wait briefly on the lock that keeps header reads atomic. Writes are serialized
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
/// * Persistent savepoints are only supported in [`WriterMode::SingleWriterProcess`]. One lives in
///   the database rather than in any one process, so a writer that opened before another created
///   it has never heard of it and would reclaim the pages it holds. Ephemeral savepoints work in
///   both modes.
/// * Compaction and [`Database::check_integrity`] are not available.
/// * Every commit is 2-phase, which costs an extra `fsync`: a reader in another process would
///   otherwise be able to see a header naming pages that are not in the file yet.
// Only with the feature, since the example names a type that is exported only then. The module
// itself is compiled either way, because the coordinator is woven into the transaction tracker
#[cfg_attr(
    feature = "experimental-multiprocess",
    doc = r#"
# Examples

```rust
use redb::*;
# use tempfile::TempDir;
const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");

# fn main() -> Result<(), Error> {
# let tmpdir = TempDir::new().unwrap();
# let path = tmpdir.path().join("my_db");
let db = MultiProcessDatabase::create(&path)?;
let write_txn = db.begin_write()?;
{
    let mut table = write_txn.open_table(TABLE)?;
    table.insert(&0, &0)?;
}
write_txn.commit()?;

// Any number of other processes may open the same directory and read it
let read_only = MultiProcessDatabase::open_read_only(&path)?;
let read_txn = read_only.begin_read()?;
assert_eq!(0, read_txn.open_table(TABLE)?.get_owned(0)?.unwrap().value());
# Ok(())
# }
```
"#
)]
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
        let marked_at_preflight = dir.prepare(true)?;
        self.open_inner(&dir, true, marked_at_preflight)
    }

    /// Opens an existing multi-process database
    pub fn open(&self, path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        let dir = DatabaseDir::new(path);
        dir.prepare(false)?;
        self.open_inner(&dir, false, false)
    }

    /// Opens an existing multi-process database for reading only
    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let dir = DatabaseDir::new(path);
        dir.prepare(false)?;
        // Taken before the marker or header is read and held for as long as the database is open.
        // A writer's open sits inside `write.lock`, which a format upgrader acquires first; a
        // read-only open takes no write lock, so this shared lock is its only barrier against an
        // upgrade running mid-open. The marker is still validated through the path below, so an
        // upgrade that replaced the file before the lock landed is refused by its version instead
        let metadata_lock = dir.lock_metadata_shared()?;
        // Safe to read without the write lock: the marker is written once and never changed
        let mode = dir.mode()?;
        self.check_mode(mode)?;

        let file = OpenOptions::new().read(true).open(dir.data_file())?;
        // No file lock: the lock files exclude concurrent writers, and this handle only reads.
        // And no header-write guard on the backend, since this handle never writes at all
        let backend = FileBackend::new_internal(file, FileLockKind::None)?;
        // Construction reads the header once, which needs the registry lock like every header
        // read; the later ones take it inside begin_read()
        let registry = RegistryLock::open(&dir);
        let guard = registry.shared()?;
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
        drop(guard);
        coordinator.hold_metadata_lock(metadata_lock)?;
        // A read-only handle never writes, so its idea of what is current comes entirely from the
        // refresh every begin_read() does
        Ok(ReadOnlyDatabase::from_parts(
            mem.clone(),
            Arc::new(TransactionTracker::new_multiprocess(
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
        allow_create: bool,
        marked_at_preflight: bool,
    ) -> Result<MultiProcessDatabase, DatabaseError> {
        // Take the write lock before touching the database file, so that no other process can be
        // part way through a transaction while this one repairs or reads the file. In
        // SingleWriterProcess mode it is never released; in MultiWriterProcess mode it is handed
        // back at the end of this function, and retaken per transaction.
        //
        // The marker is written under this same lock and never changed after. open() reads it
        // first, so a directory that is not a database is left exactly as found; only create()
        // must take the lock before deciding, since the marker may be missing precisely because
        // another create() is still writing it.
        let mut write_lock;
        let mut fresh_claim = false;
        let mode = if !allow_create {
            // Read before the lock file is opened, so a directory that is not one of these is
            // left exactly as found; this pass only chooses how the lock is taken, and the read
            // under the held lock below is the authoritative one
            let provisional = dir
                .read_metadata(false)?
                .expect("read_metadata only reports a missing marker when create is set");
            self.check_mode(provisional)?;
            write_lock = WriteLock::open(dir)?;
            match provisional {
                WriterMode::SingleWriterProcess => {
                    if !write_lock.try_acquire()? {
                        return Err(DatabaseError::DatabaseAlreadyOpen);
                    }
                }
                // A held lock just means a transaction is in progress; wait our turn
                WriterMode::MultiWriterProcess => write_lock.acquire()?,
            }
            // An upgrader acquires `write.lock` before taking `metadata` exclusively, so a marker
            // read under the held lock cannot be mid-upgrade -- and one an upgrade replaced in
            // the window is refused by its version here rather than opened with the old protocol
            let mode = dir
                .read_metadata(false)?
                .expect("read_metadata only reports a missing marker when create is set");
            self.check_mode(mode)?;
            mode
        } else {
            write_lock = WriteLock::open(dir)?;
            if write_lock.try_acquire()? {
                if let Some(mode) = dir.read_metadata(true)? {
                    // A marker seen before the lock file was made is a create() to finish -- a
                    // crash can lose the lock's entry while the marker survives. One that first
                    // appears after cannot be another create()'s, since finishing one needs the
                    // lock this call holds
                    if write_lock.created() && !marked_at_preflight {
                        write_lock.mark_abandoned();
                        return Err(crate::StorageError::Io(std::io::Error::new(
                            ErrorKind::AlreadyExists,
                            "the marker appeared while the directory was being claimed",
                        ))
                        .into());
                    }
                    mode
                } else {
                    // No marker means this create() is the one making the database. A lock file
                    // this call just made vouches for nothing, so nothing may already exist under
                    // redb's other names -- and one it cannot stand behind is marked, so the next
                    // create() refuses it too
                    fresh_claim = write_lock.created() && !marked_at_preflight;
                    if fresh_claim && let Err(err) = dir.reject_files_beside_a_fresh_lock() {
                        write_lock.mark_abandoned();
                        return Err(err);
                    }
                    self.mode.unwrap_or(WriterMode::MultiWriterProcess)
                }
            } else {
                // Some process holds the write lock, and what that means depends on the marker
                match dir.read_metadata(true)? {
                    // A multi-writer database is in the middle of a transaction; wait our turn
                    Some(WriterMode::MultiWriterProcess) => {
                        self.check_mode(WriterMode::MultiWriterProcess)?;
                        write_lock.acquire()?;
                        WriterMode::MultiWriterProcess
                    }
                    // A single-writer database is open in another process, and a directory with
                    // no marker is one another process is part way through creating
                    Some(WriterMode::SingleWriterProcess) | None => {
                        return Err(DatabaseError::DatabaseAlreadyOpen);
                    }
                }
            }
        };
        self.check_mode(mode)?;

        dir.init_protocol_files()?;
        let (file, location) = match dir.open_data(allow_create, fresh_claim) {
            Ok(opened) => opened,
            Err(err) => {
                // On a fresh claim this refusal is the fresh-lock rule firing late -- a file
                // arrived under a name that was just required to be absent -- and the lock is
                // marked for the same reason it is there
                if fresh_claim
                    && let DatabaseError::Storage(crate::StorageError::Io(io_err)) = &err
                    && io_err.kind() == ErrorKind::AlreadyExists
                {
                    write_lock.mark_abandoned();
                }
                return Err(err);
            }
        };
        // The backend takes the registry lock around every header write, which is the writing
        // half of the protocol's atomic header reads; it takes no file lock, since `write.lock`
        // and the files in `txn/` do that job and every process needs the file open at once
        let backend = HeaderGuardedBackend::new(file, RegistryLock::open(dir))?;

        let dir_for_coordinator = dir.clone();
        // A guard rather than a check on the error paths: the repair callback runs inside the
        // initialization and is the caller's code, so the span can be left by a panic the caller
        // catches as well as by an error, and the temporary has to be cleaned up either way
        let mut cleanup = DiscardFailedData {
            dir,
            location,
            armed: true,
        };
        let inner = Database::new_multiprocess(
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
            dir.write_metadata_if_missing(mode)?;
        }
        cleanup.armed = false;

        let coordinator = inner
            .transaction_tracker()
            .process()
            .expect("a multi-process database always has a coordinator")
            .clone();
        // Taken here rather than earlier because a fresh create() has no marker to lock until
        // just above
        coordinator.hold_metadata_lock(dir.lock_metadata_shared()?)?;
        // Still under the write lock, which is what entitles this process to rewrite the file
        coordinator.initialize_extended_header()?;
        // A database just created, or repaired after an unclean shutdown, has no allocator state
        // table yet; one is written below so the next writer can load it instead of rebuilding it
        let needs_allocator_state = Database::allocator_state_table(&inner.get_memory())?.is_none();
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

// Not under WASI, which has no file locking: these all take one, and the module itself is
// compiled there because the coordinator is woven into the transaction tracker
#[cfg(all(test, not(target_os = "wasi")))]
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

    /// The extended header is advisory in the strict sense: a corrupted one costs every process
    /// its page cache, and nothing else. Readers fall back to assuming the worst, and the next
    /// commit's flip writes a slot that verifies again.
    #[test]
    fn a_corrupted_extended_header_only_costs_the_cache() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let path = tmpdir.path().join("db");
        let writer = small_region_builder().create(&path).unwrap();
        let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
        write(&writer, 0..10, 64);
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

        // What a crash between the extended header write and the flip leaves behind, at its worst
        std::fs::write(path.join("extended-header"), [0xabu8; 48]).unwrap();

        // A reader must fall back -- drop its cache and carry on -- rather than fail or trust it
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

        // The next commit writes the slot it flips to, and the file verifies again from there
        write(&writer, 10..11, 64);
        assert_eq!(
            11,
            reader
                .begin_read()
                .unwrap()
                .open_table(TABLE)
                .unwrap()
                .len()
                .unwrap()
        );
    }

    /// The cache floor says that nothing in the read cache predates it. A read transaction this
    /// process already holds goes on reading from its own, older snapshot, and everything it puts
    /// in the cache after a drop comes from there -- so dropping the cache cannot raise the floor
    /// past it.
    #[test]
    fn the_cache_floor_stays_at_or_below_a_live_read_transaction() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let path = tmpdir.path().join("db");
        let writer = small_region_builder().create(&path).unwrap();
        let reader = MultiProcessDatabase::open(&path).unwrap();
        let holder = MultiProcessDatabase::open(&path).unwrap();

        // Pinned before anything is committed, so that every horizon announced from here on is
        // clamped below the floor the reader opened with, and the reader keeps its cache
        let holding = holder.begin_read().unwrap();
        let settled = reader.coordinator.cache_floor();
        for key in 0..8 {
            write(&writer, key..key + 1, 64);
        }

        // Far enough past the floor for a later announcement to exceed it
        let pinned = writer.last_durable_commit().unwrap();
        let held = reader.begin_read().unwrap();
        assert!(pinned > settled + 1);
        assert_eq!(
            settled,
            reader.coordinator.cache_floor(),
            "the cache was dropped before the reader had an older snapshot to fall behind on"
        );

        // Letting the horizon rise to what `held` pins is what makes the next refresh drop the
        // cache, while `held` goes on reading the snapshot it pinned
        drop(holding);
        for key in 8..16 {
            write(&writer, key..key + 1, 64);
        }
        drop(reader.begin_read().unwrap());

        let floor = reader.coordinator.cache_floor();
        assert!(
            floor <= pinned,
            "floor {floor} passed snapshot {pinned}, which a live reader is still reading from"
        );
        drop(held);
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

/// Discards the temporary a `create()` made, unless initialization completed and disarmed it.
/// A drop guard rather than error-path cleanup, because the span it covers runs the repair
/// callback -- the caller's code -- and can be left by a panic the caller catches, which no
/// error path would see.
struct DiscardFailedData<'a> {
    dir: &'a DatabaseDir,
    location: DataLocation,
    armed: bool,
}

impl Drop for DiscardFailedData<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.dir.discard_failed_data(self.location);
        }
    }
}
