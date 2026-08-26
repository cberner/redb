//! Database access coordinated across multiple processes.
//!
//! A multiprocess database is a directory containing an ordinary redb data file and the lock
//! files described in the [design document](https://github.com/cberner/redb/blob/master/docs/design.md#directory-structured-databases).
//! Transactions use the same [`crate::ReadTransaction`], [`crate::WriteTransaction`],
//! [`crate::Table`], and [`crate::ReadOnlyTable`] types as a single-file database.
//!
//! [`WriterMode::SingleWriter`] minimizes coordination overhead when one process owns writer
//! access for the lifetime of the database. [`WriterMode::MultipleWriters`] allows writer
//! ownership to move between processes after each transaction. Both modes require immediately
//! durable, two-phase commits; multiple-writer databases additionally do not support ephemeral
//! savepoints.

use crate::db::RepairSession;
use crate::sealed::Sealed;
use crate::sync::Mutex;
use crate::transaction_tracker::TransactionId;
use crate::tree_store::file_backend::FileBackend;
use crate::tree_store::{CommitSnapshot, PAGE_SIZE, TransactionalMemory, xxh3_checksum};
use crate::{
    CacheStats, DatabaseError, ReadTransaction, ReadableDatabase, Result, StorageError,
    TransactionError, WriteTransaction,
};
use alloc::sync::Arc;
use core::fmt::{Debug, Formatter};
use core::mem::size_of;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const DATA_FILE_NAME: &str = "data.redb";
const DATA_TEMP_FILE_NAME: &str = "data.redb.tmp";
const METADATA_FILE_NAME: &str = "metadata";
const METADATA_TEMP_FILE_NAME: &str = "metadata.tmp";
const EXTENDED_HEADER_FILE_NAME: &str = "extended-header";
const WRITE_LOCK_FILE_NAME: &str = "write.lock";
const REGISTRY_LOCK_FILE_NAME: &str = "registry.lock";
const TRANSACTION_DIRECTORY_NAME: &str = "txn";

const METADATA_MAGIC: [u8; 11] = [
    b'r', b'e', b'd', b'b', b'M', b'P', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A,
];
const FORMAT_VERSION: u8 = 1;
const METADATA_LEN: usize = METADATA_MAGIC.len() + 2;
const EXTENDED_SLOT_LEN: usize = size_of::<u64>() + size_of::<u128>();
const EXTENDED_HEADER_LEN: usize = 2 * EXTENDED_SLOT_LEN;

/// Determines which processes may write to a multiprocess database.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WriterMode {
    /// One process holds writer ownership for as long as its [`Database`] is open.
    SingleWriter,
    /// Any process may write; ownership is handed off after every write transaction.
    MultipleWriters,
}

impl WriterMode {
    fn to_byte(self) -> u8 {
        match self {
            Self::SingleWriter => 1,
            Self::MultipleWriters => 2,
        }
    }

    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::SingleWriter),
            2 => Ok(Self::MultipleWriters),
            _ => Err(StorageError::Corrupted(format!(
                "Unsupported multiprocess writer mode: {value}"
            ))),
        }
    }
}

/// A redb database that may be read and written from multiple processes.
///
/// The path passed to [`Self::create`] or [`Self::open`] names a directory, not a file. Use
/// [`Self::begin_read`] and [`Self::begin_write`] exactly as with [`crate::Database`].
pub struct Database {
    inner: crate::Database,
    mode: WriterMode,
}

impl Database {
    /// Opens a multiprocess database, creating it when necessary.
    ///
    /// New databases use [`WriterMode::MultipleWriters`]. Use [`Builder::set_writer_mode`] to
    /// select the lower-overhead single-writer mode.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Builder::new().create(path)
    }

    /// Opens an existing multiprocess database for writing.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Builder::new().open(path)
    }

    /// Opens an existing multiprocess database without writer access.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<ReadOnlyDatabase, DatabaseError> {
        Builder::new().open_read_only(path)
    }

    /// Returns a builder for configuring a multiprocess database.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Starts a write transaction, blocking until the cross-process writer is available.
    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        self.inner.begin_write()
    }

    /// Returns the writer mode stored in this database's metadata.
    pub fn writer_mode(&self) -> WriterMode {
        self.mode
    }
}

impl Sealed for Database {}

impl ReadableDatabase for Database {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        self.inner.begin_read()
    }

    fn cache_stats(&self) -> CacheStats {
        self.inner.cache_stats()
    }
}

impl Debug for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("multiprocess::Database")
            .field("writer_mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// A read-only handle to a multiprocess database.
pub struct ReadOnlyDatabase {
    inner: crate::ReadOnlyDatabase,
    mode: WriterMode,
}

impl ReadOnlyDatabase {
    /// Opens an existing multiprocess database without writer access.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Builder::new().open_read_only(path)
    }

    /// Returns the writer mode stored in this database's metadata.
    pub fn writer_mode(&self) -> WriterMode {
        self.mode
    }
}

impl Sealed for ReadOnlyDatabase {}

impl ReadableDatabase for ReadOnlyDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        self.inner.begin_read()
    }

    fn cache_stats(&self) -> CacheStats {
        self.inner.cache_stats()
    }
}

impl Debug for ReadOnlyDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("multiprocess::ReadOnlyDatabase")
            .field("writer_mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// Configuration builder for a [`Database`].
pub struct Builder {
    cache_size: usize,
    writer_mode: Option<WriterMode>,
    repair_callback: Box<dyn Fn(&mut RepairSession)>,
}

impl Builder {
    /// Creates a builder with a 1GiB cache and [`WriterMode::MultipleWriters`] for new databases.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cache_size: 1024 * 1024 * 1024,
            writer_mode: None,
            repair_callback: Box::new(|_| {}),
        }
    }

    /// Sets the amount of memory used for the page cache.
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.cache_size = bytes;
        self
    }

    /// Selects the writer mode for a new database.
    ///
    /// When opening an existing database, setting this also verifies that its stored mode matches.
    pub fn set_writer_mode(&mut self, mode: WriterMode) -> &mut Self {
        self.writer_mode = Some(mode);
        self
    }

    /// Sets the callback used while repairing the data file.
    pub fn set_repair_callback(
        &mut self,
        callback: impl Fn(&mut RepairSession) + 'static,
    ) -> &mut Self {
        self.repair_callback = Box::new(callback);
        self
    }

    /// Opens a multiprocess database, creating it when necessary.
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        self.open_writable(path.as_ref(), true)
    }

    /// Opens an existing multiprocess database for writing.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        self.open_writable(path.as_ref(), false)
    }

    /// Opens an existing multiprocess database without writer access.
    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let directory = DatabaseDirectory::new(path);
        let (metadata, mode) = directory.open_metadata()?;
        self.check_mode(mode)?;
        directory.verify_layout()?;

        let coordinator = Arc::new(ProcessCoordinator::new_read_only(
            directory.clone(),
            metadata,
            mode,
        ));
        let registry = directory.lock_registry_shared()?;
        let file = OpenOptions::new().read(true).open(directory.data_file())?;
        let inner = crate::ReadOnlyDatabase::new_multiprocess(
            Box::new(FileBackend::new_unlocked(file)),
            PAGE_SIZE,
            self.cache_size,
            coordinator,
        )?;
        drop(registry);

        Ok(ReadOnlyDatabase { inner, mode })
    }

    fn open_writable(&self, path: &Path, create: bool) -> Result<Database, DatabaseError> {
        let directory = DatabaseDirectory::new(path);
        let prepared = directory.prepare_writable(
            create,
            self.writer_mode.unwrap_or(WriterMode::MultipleWriters),
        )?;
        self.check_mode(prepared.mode)?;

        let coordinator = Arc::new(ProcessCoordinator::new_writable(
            directory.clone(),
            prepared.metadata,
            prepared.mode,
            prepared.write_lock,
            true,
        ));

        let registry = directory.lock_registry_exclusive()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(&prepared.data_path)?;
        let inner = match crate::Database::new_multiprocess(
            Box::new(FileBackend::new_unlocked(file)),
            create,
            PAGE_SIZE,
            None,
            self.cache_size,
            &self.repair_callback,
            coordinator.clone(),
        ) {
            Ok(inner) => inner,
            Err(error) => {
                drop(registry);
                return Err(error);
            }
        };
        let initialized = coordinator.initialize_extended_header_locked(inner.get_memory());
        drop(registry);
        initialized?;

        if matches!(prepared.mode, WriterMode::MultipleWriters)
            && crate::Database::get_allocator_state_table(&inner.get_memory())?.is_none()
        {
            inner
                .begin_write()
                .map_err(|error| DatabaseError::Storage(error.into_storage_error()))?
                .commit()
                .map_err(|error| match error {
                    crate::CommitError::Storage(storage) => DatabaseError::Storage(storage),
                    crate::CommitError::TransactionPoisoned => unreachable!(),
                })?;
        }

        if prepared.data_pending {
            directory.install_data()?;
        }

        if prepared.metadata_pending {
            let metadata = directory.install_metadata(prepared.mode)?;
            coordinator.install_metadata(metadata)?;
        }
        coordinator.finish_open()?;

        Ok(Database {
            inner,
            mode: prepared.mode,
        })
    }

    fn check_mode(&self, actual: WriterMode) -> Result<(), DatabaseError> {
        if let Some(requested) = self.writer_mode
            && requested != actual
        {
            return Err(StorageError::Corrupted(format!(
                "Multiprocess database uses {actual:?}, but {requested:?} was requested"
            ))
            .into());
        }
        Ok(())
    }
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("multiprocess::Builder")
            .field("cache_size", &self.cache_size)
            .field("writer_mode", &self.writer_mode)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct DatabaseDirectory {
    root: PathBuf,
}

impl DatabaseDirectory {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            root: path.as_ref().to_path_buf(),
        }
    }

    fn data_file(&self) -> PathBuf {
        self.root.join(DATA_FILE_NAME)
    }

    fn data_temp_file(&self) -> PathBuf {
        self.root.join(DATA_TEMP_FILE_NAME)
    }

    fn metadata_file(&self) -> PathBuf {
        self.root.join(METADATA_FILE_NAME)
    }

    fn extended_header_file(&self) -> PathBuf {
        self.root.join(EXTENDED_HEADER_FILE_NAME)
    }

    fn write_lock_file(&self) -> PathBuf {
        self.root.join(WRITE_LOCK_FILE_NAME)
    }

    fn registry_lock_file(&self) -> PathBuf {
        self.root.join(REGISTRY_LOCK_FILE_NAME)
    }

    fn transaction_directory(&self) -> PathBuf {
        self.root.join(TRANSACTION_DIRECTORY_NAME)
    }

    fn transaction_file(&self, id: TransactionId) -> PathBuf {
        self.transaction_directory().join(id.raw_id().to_string())
    }

    fn prepare_writable(
        &self,
        create: bool,
        requested_mode: WriterMode,
    ) -> Result<PreparedDirectory, DatabaseError> {
        if create {
            std::fs::create_dir_all(&self.root)?;
        } else if !self.root.is_dir() {
            return Err(std::io::Error::new(
                ErrorKind::NotFound,
                "No such multiprocess database directory",
            )
            .into());
        }

        if self.metadata_file().exists() {
            let (metadata, mode) = self.open_metadata()?;
            self.verify_layout()?;
            let mut write_lock = WriteLock::open(self)?;
            write_lock.acquire_for_open(mode)?;
            return Ok(PreparedDirectory {
                metadata: Some(metadata),
                mode,
                write_lock,
                metadata_pending: false,
                data_path: self.data_file(),
                data_pending: false,
            });
        }
        if !create {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Not a multiprocess database directory",
            )
            .into());
        }

        // Refuse an unrelated directory before adding write.lock to it. The check is repeated
        // after locking to cover another interrupted creator finishing its setup concurrently.
        self.reject_foreign_entries()?;
        let mut write_lock = WriteLock::open_or_create(self)?;
        write_lock.acquire_blocking()?;
        // Another creator may have completed while this one waited for write.lock.
        if self.metadata_file().exists() {
            let (metadata, mode) = self.open_metadata()?;
            self.verify_layout()?;
            return Ok(PreparedDirectory {
                metadata: Some(metadata),
                mode,
                write_lock,
                metadata_pending: false,
                data_path: self.data_file(),
                data_pending: false,
            });
        }

        self.reject_foreign_entries()?;
        self.discard_data_temp()?;
        open_or_create(&self.registry_lock_file())?;
        open_or_create(&self.extended_header_file())?;
        std::fs::create_dir_all(self.transaction_directory())?;

        let (data_path, data_pending) = if self.data_file().exists() {
            (self.data_file(), false)
        } else {
            (self.data_temp_file(), true)
        };

        Ok(PreparedDirectory {
            metadata: None,
            mode: requested_mode,
            write_lock,
            metadata_pending: true,
            data_path,
            data_pending,
        })
    }

    fn discard_data_temp(&self) -> Result<(), DatabaseError> {
        match std::fs::remove_file(self.data_temp_file()) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn install_data(&self) -> Result<(), DatabaseError> {
        if self.data_file().exists() {
            return Err(std::io::Error::new(
                ErrorKind::AlreadyExists,
                "The multiprocess data file appeared during initialization",
            )
            .into());
        }
        std::fs::rename(self.data_temp_file(), self.data_file())?;
        sync_directory(&self.root)
    }

    fn open_metadata(&self) -> Result<(File, WriterMode), DatabaseError> {
        ensure_file(&self.metadata_file())?;
        let mut file = OpenOptions::new().read(true).open(self.metadata_file())?;
        file.lock_shared().map_err(lock_error)?;
        if file.metadata()?.len() != u64::try_from(METADATA_LEN).unwrap() {
            return Err(StorageError::Corrupted(
                "Invalid multiprocess metadata length".to_string(),
            )
            .into());
        }
        let mut bytes = [0; METADATA_LEN];
        file.read_exact(&mut bytes)?;
        if bytes[..METADATA_MAGIC.len()] != METADATA_MAGIC {
            return Err(StorageError::Corrupted(
                "Invalid multiprocess metadata magic number".to_string(),
            )
            .into());
        }
        if bytes[METADATA_MAGIC.len()] != FORMAT_VERSION {
            return Err(StorageError::Corrupted(format!(
                "Unsupported multiprocess format version: {}",
                bytes[METADATA_MAGIC.len()]
            ))
            .into());
        }
        Ok((
            file,
            WriterMode::from_byte(bytes[METADATA_MAGIC.len() + 1])?,
        ))
    }

    fn install_metadata(&self, mode: WriterMode) -> Result<File, DatabaseError> {
        let temporary = self.root.join(METADATA_TEMP_FILE_NAME);
        let mut bytes = [0; METADATA_LEN];
        bytes[..METADATA_MAGIC.len()].copy_from_slice(&METADATA_MAGIC);
        bytes[METADATA_MAGIC.len()] = FORMAT_VERSION;
        bytes[METADATA_MAGIC.len() + 1] = mode.to_byte();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, self.metadata_file())?;
        sync_directory(&self.root)?;
        sync_parent_directory(&self.root)?;
        let (file, actual) = self.open_metadata()?;
        debug_assert_eq!(mode, actual);
        Ok(file)
    }

    fn verify_layout(&self) -> Result<(), DatabaseError> {
        for path in [
            self.data_file(),
            self.extended_header_file(),
            self.write_lock_file(),
            self.registry_lock_file(),
        ] {
            ensure_file(&path)?;
        }
        ensure_directory(&self.transaction_directory())?;
        Ok(())
    }

    fn reject_foreign_entries(&self) -> Result<(), DatabaseError> {
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let expected_directory = if name == TRANSACTION_DIRECTORY_NAME {
                true
            } else if [
                DATA_FILE_NAME,
                DATA_TEMP_FILE_NAME,
                EXTENDED_HEADER_FILE_NAME,
                WRITE_LOCK_FILE_NAME,
                REGISTRY_LOCK_FILE_NAME,
                METADATA_TEMP_FILE_NAME,
            ]
            .iter()
            .any(|allowed| name == *allowed)
            {
                false
            } else {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "Refusing to create a database in a non-empty directory",
                )
                .into());
            };

            let file_type = std::fs::symlink_metadata(entry.path())?.file_type();
            if (expected_directory && !file_type.is_dir())
                || (!expected_directory && !file_type.is_file())
            {
                return Err(StorageError::Corrupted(format!(
                    "Invalid multiprocess database entry type: {}",
                    entry.path().display()
                ))
                .into());
            }
        }
        Ok(())
    }

    fn lock_registry_shared(&self) -> Result<OwnedFileLock> {
        OwnedFileLock::shared(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.registry_lock_file())?,
        )
    }

    fn lock_registry_exclusive(&self) -> Result<OwnedFileLock> {
        OwnedFileLock::exclusive(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.registry_lock_file())?,
        )
    }
}

struct PreparedDirectory {
    metadata: Option<File>,
    mode: WriterMode,
    write_lock: WriteLock,
    metadata_pending: bool,
    data_path: PathBuf,
    data_pending: bool,
}

struct WriteLock {
    file: File,
    held: bool,
}

impl WriteLock {
    fn open(directory: &DatabaseDirectory) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open(directory.write_lock_file())?,
            held: false,
        })
    }

    fn open_or_create(directory: &DatabaseDirectory) -> Result<Self> {
        Ok(Self {
            file: open_or_create(&directory.write_lock_file())?,
            held: false,
        })
    }

    fn acquire_for_open(&mut self, mode: WriterMode) -> Result<(), DatabaseError> {
        match mode {
            WriterMode::SingleWriter => match self.file.try_lock() {
                Ok(()) => self.held = true,
                Err(TryLockError::WouldBlock) => return Err(DatabaseError::DatabaseAlreadyOpen),
                Err(error) => return Err(lock_try_error(error).into()),
            },
            WriterMode::MultipleWriters => self.acquire_blocking()?,
        }
        Ok(())
    }

    fn acquire_blocking(&mut self) -> Result<()> {
        debug_assert!(!self.held);
        self.file.lock().map_err(lock_error)?;
        self.held = true;
        Ok(())
    }

    fn release(&mut self) {
        if self.held {
            let _ = self.file.unlock();
            self.held = false;
        }
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        self.release();
    }
}

struct OwnedFileLock {
    file: File,
}

impl OwnedFileLock {
    fn shared(file: File) -> Result<Self> {
        file.lock_shared().map_err(lock_error)?;
        Ok(Self { file })
    }

    fn exclusive(file: File) -> Result<Self> {
        file.lock().map_err(lock_error)?;
        Ok(Self { file })
    }
}

impl Drop for OwnedFileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// A shared lock on `txn/<id>`, held until the last matching local transaction ends.
pub(crate) struct TransactionPin {
    file: File,
    transaction: TransactionId,
}

impl TransactionPin {
    pub(crate) fn transaction(&self) -> TransactionId {
        self.transaction
    }
}

impl Drop for TransactionPin {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(crate) struct PreparedWrite {
    pub(crate) last_committed: TransactionId,
    pub(crate) pin: Option<TransactionPin>,
}

struct CacheState {
    floor: Option<TransactionId>,
}

struct WriteState {
    lock: Option<WriteLock>,
    opening: bool,
    transaction: bool,
}

pub(crate) struct ProcessCoordinator {
    directory: DatabaseDirectory,
    metadata: Mutex<Option<File>>,
    mode: WriterMode,
    writable: bool,
    cache: Mutex<CacheState>,
    write: Mutex<WriteState>,
}

impl ProcessCoordinator {
    fn new_writable(
        directory: DatabaseDirectory,
        metadata: Option<File>,
        mode: WriterMode,
        write_lock: WriteLock,
        opening: bool,
    ) -> Self {
        Self {
            directory,
            metadata: Mutex::new(metadata),
            mode,
            writable: true,
            cache: Mutex::new(CacheState { floor: None }),
            write: Mutex::new(WriteState {
                lock: Some(write_lock),
                opening,
                transaction: false,
            }),
        }
    }

    fn new_read_only(directory: DatabaseDirectory, metadata: File, mode: WriterMode) -> Self {
        Self {
            directory,
            metadata: Mutex::new(Some(metadata)),
            mode,
            writable: false,
            cache: Mutex::new(CacheState { floor: None }),
            write: Mutex::new(WriteState {
                lock: None,
                opening: false,
                transaction: false,
            }),
        }
    }

    pub(crate) fn multiple_writers(&self) -> bool {
        matches!(self.mode, WriterMode::MultipleWriters)
    }

    pub(crate) fn authoritative(&self) -> bool {
        self.writable && matches!(self.mode, WriterMode::SingleWriter)
    }

    fn install_metadata(&self, file: File) -> Result<()> {
        let mut metadata = self.metadata.lock()?;
        debug_assert!(metadata.is_none());
        *metadata = Some(file);
        Ok(())
    }

    fn finish_open(&self) -> Result<()> {
        let mut state = self.write.lock()?;
        state.opening = false;
        if self.multiple_writers() && !state.transaction {
            state.lock.as_mut().unwrap().release();
        }
        Ok(())
    }

    pub(crate) fn begin_read(
        &self,
        mem: &TransactionalMemory,
        local_write_live: bool,
        local_oldest: Option<TransactionId>,
    ) -> Result<(TransactionId, Option<TransactionPin>)> {
        if self.authoritative() {
            return Ok((mem.get_last_committed_transaction_id()?, None));
        }

        let _registry = self.directory.lock_registry_shared()?;
        let snapshot = if local_write_live {
            mem.current_commit_snapshot()
        } else {
            mem.reload_transaction_slots()?
        };
        let cache_floor = local_oldest.map_or(snapshot.transaction_id, |oldest| {
            oldest.min(snapshot.transaction_id)
        });
        self.revalidate_cache(mem, &snapshot, Some(cache_floor))?;
        let pin = self.pin_transaction(snapshot.transaction_id)?;
        Ok((snapshot.transaction_id, Some(pin)))
    }

    pub(crate) fn begin_write(&self, mem: &Arc<TransactionalMemory>) -> Result<PreparedWrite> {
        assert!(self.writable);
        if self.authoritative() {
            let last_committed = mem.get_last_committed_transaction_id()?;
            self.write.lock()?.transaction = true;
            return Ok(PreparedWrite {
                last_committed,
                pin: None,
            });
        }

        let reload = {
            let mut state = self.write.lock()?;
            let lock = state.lock.as_mut().unwrap();
            if self.multiple_writers() && !lock.held {
                lock.acquire_blocking()?;
            }
            self.multiple_writers() && !state.opening
        };

        let result = (|| {
            let registry = self.directory.lock_registry_shared()?;
            if reload {
                mem.reload_for_write().map_err(database_error_to_storage)?;
            }
            let snapshot = mem.current_commit_snapshot();
            // Keep an older floor after invalidation here. A read transaction may have started in
            // this process while this thread waited for write.lock; its registration updates the
            // floor, and it may continue caching pages from that older snapshot.
            self.revalidate_cache(mem, &snapshot, None)?;
            let pin = self.pin_transaction(snapshot.transaction_id)?;
            drop(registry);

            if reload {
                let Some(tree) = crate::Database::get_allocator_state_table(mem)? else {
                    return Err(StorageError::Corrupted(
                        "The previous multiprocess writer did not leave valid allocator state"
                            .to_string(),
                    ));
                };
                mem.load_allocator_state(&tree)?;
                mem.retain_recovery_required();
                #[cfg(debug_assertions)]
                crate::Database::mark_allocated_page_for_debug(mem)?;
                // Reloading abandoned every page and allocator mutation that belonged only to
                // this process, including leaks left by a caught panic.
                mem.clear_needs_repair();
            }

            Ok(PreparedWrite {
                last_committed: snapshot.transaction_id,
                pin: Some(pin),
            })
        })();

        let mut state = self.write.lock()?;
        match result {
            Ok(prepared) => {
                state.transaction = true;
                Ok(prepared)
            }
            Err(error) => {
                if self.multiple_writers() && !state.opening {
                    state.lock.as_mut().unwrap().release();
                }
                Err(error)
            }
        }
    }

    pub(crate) fn end_write(&self) {
        let Ok(mut state) = self.write.lock() else {
            return;
        };
        state.transaction = false;
        if self.multiple_writers() && !state.opening {
            state.lock.as_mut().unwrap().release();
        }
    }

    pub(crate) fn oldest_active_transaction(
        &self,
        local_oldest: Option<TransactionId>,
    ) -> Result<Option<TransactionId>> {
        let _registry = self.directory.lock_registry_exclusive()?;
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(self.directory.transaction_directory())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                StorageError::Corrupted("Invalid transaction registry filename".to_string())
            })?;
            let id = name.parse::<u64>().map_err(|_| {
                StorageError::Corrupted(format!("Invalid transaction registry filename: {name}"))
            })?;
            ids.push((id, entry.path()));
        }
        ids.sort_unstable_by_key(|(id, _)| *id);

        let mut remote = None;
        for (id, path) in ids {
            // `local_oldest` is already known to be active. Stopping here is conservative and
            // avoids ever unlinking this process's own lock file on platforms whose lock API does
            // not report a conflict between two descriptors in the same process.
            if local_oldest.is_some_and(|local| id >= local.raw_id()) {
                break;
            }
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            match file.try_lock() {
                Ok(()) => {
                    file.unlock()?;
                    drop(file);
                    std::fs::remove_file(path)?;
                }
                Err(TryLockError::WouldBlock) => {
                    remote = Some(TransactionId::new(id));
                    break;
                }
                Err(error) => return Err(lock_try_error(error)),
            }
        }

        Ok(match (local_oldest, remote) {
            (Some(local), Some(remote)) => Some(local.min(remote)),
            (local, None) => local,
            (None, remote) => remote,
        })
    }

    pub(crate) fn track_cache_floor(&self, transaction: TransactionId) -> Result<()> {
        let mut cache = self.cache.lock()?;
        cache.floor = Some(
            cache
                .floor
                .map_or(transaction, |floor| floor.min(transaction)),
        );
        Ok(())
    }

    pub(crate) fn prepare_commit(
        &self,
        mem: &TransactionalMemory,
        proposed_horizon: TransactionId,
    ) -> Result<CommitGuard> {
        let registry = self.directory.lock_registry_exclusive()?;
        let snapshot = mem.current_commit_snapshot();
        let horizon = self
            .read_extended_horizon(&snapshot)
            .map_or(proposed_horizon, |current| current.max(proposed_horizon));
        Ok(CommitGuard {
            _registry: registry,
            extended_header: self.directory.extended_header_file(),
            horizon,
        })
    }

    fn initialize_extended_header_locked(&self, mem: Arc<TransactionalMemory>) -> Result<()> {
        let snapshot = mem.current_commit_snapshot();
        if self.read_extended_horizon(&snapshot).is_some() {
            return Ok(());
        }
        let horizon = TransactionId::new(snapshot.transaction_id.raw_id().saturating_sub(1));
        let slots = mem.commit_slot_bytes();
        let file = open_or_create(&self.directory.extended_header_file())?;
        for (index, slot) in slots.iter().enumerate() {
            write_extended_slot(&file, index, horizon, slot)?;
        }
        file.set_len(EXTENDED_HEADER_LEN.try_into().unwrap())?;
        file.sync_data()?;
        Ok(())
    }

    fn revalidate_cache(
        &self,
        mem: &TransactionalMemory,
        snapshot: &CommitSnapshot,
        floor_after_clear: Option<TransactionId>,
    ) -> Result<()> {
        let mut cache = self.cache.lock()?;
        let horizon = self.read_extended_horizon(snapshot);
        let invalidate = match (cache.floor, horizon) {
            (Some(floor), Some(horizon)) => horizon > floor,
            _ => true,
        };
        if invalidate {
            mem.clear_read_cache();
            if cache.floor.is_none() || floor_after_clear.is_some() {
                cache.floor = Some(floor_after_clear.unwrap_or(snapshot.transaction_id));
            }
        } else if let Some(candidate) = floor_after_clear {
            cache.floor = Some(cache.floor.map_or(candidate, |floor| floor.min(candidate)));
        }
        Ok(())
    }

    fn read_extended_horizon(&self, snapshot: &CommitSnapshot) -> Option<TransactionId> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(self.directory.extended_header_file())
            .ok()?;
        let offset = u64::try_from(snapshot.slot_index * EXTENDED_SLOT_LEN).unwrap();
        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut bytes = [0; EXTENDED_SLOT_LEN];
        file.read_exact(&mut bytes).ok()?;
        let horizon = u64::from_le_bytes(bytes[..size_of::<u64>()].try_into().unwrap());
        let stored = u128::from_le_bytes(bytes[size_of::<u64>()..].try_into().unwrap());
        let expected = extended_hash(TransactionId::new(horizon), &snapshot.slot_bytes);
        (stored == expected).then_some(TransactionId::new(horizon))
    }

    fn pin_transaction(&self, id: TransactionId) -> Result<TransactionPin> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.transaction_file(id))?;
        file.lock_shared().map_err(lock_error)?;
        Ok(TransactionPin {
            file,
            transaction: id,
        })
    }

    pub(crate) fn close(&self, mem: &TransactionalMemory) -> Result<()> {
        if self.multiple_writers() {
            // This handle's header may be older than another writer's. Committed transactions
            // already flushed their data, so closing only has to discard local buffers and close
            // the backend; it must not wait for or disturb the active writer.
            return mem.close_multiprocess_writer();
        }

        let _registry = self.directory.lock_registry_exclusive()?;
        mem.close()
    }
}

impl Debug for ProcessCoordinator {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ProcessCoordinator")
            .field("mode", &self.mode)
            .field("writable", &self.writable)
            .finish_non_exhaustive()
    }
}

pub(crate) struct CommitGuard {
    _registry: OwnedFileLock,
    extended_header: PathBuf,
    horizon: TransactionId,
}

impl CommitGuard {
    pub(crate) fn publish(&self, slot_index: usize, slot_bytes: &[u8; 128]) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.extended_header)?;
        write_extended_slot(&file, slot_index, self.horizon, slot_bytes)
    }
}

fn write_extended_slot(
    file: &File,
    slot_index: usize,
    horizon: TransactionId,
    slot_bytes: &[u8; 128],
) -> Result<()> {
    let mut bytes = [0; EXTENDED_SLOT_LEN];
    bytes[..size_of::<u64>()].copy_from_slice(&horizon.raw_id().to_le_bytes());
    bytes[size_of::<u64>()..].copy_from_slice(&extended_hash(horizon, slot_bytes).to_le_bytes());
    let mut file = file;
    file.seek(SeekFrom::Start(
        u64::try_from(slot_index * EXTENDED_SLOT_LEN).unwrap(),
    ))?;
    file.write_all(&bytes)?;
    Ok(())
}

fn extended_hash(horizon: TransactionId, slot_bytes: &[u8; 128]) -> u128 {
    let mut bytes = [0; size_of::<u64>() + 128];
    bytes[..size_of::<u64>()].copy_from_slice(&horizon.raw_id().to_le_bytes());
    bytes[size_of::<u64>()..].copy_from_slice(slot_bytes);
    xxh3_checksum(&bytes)
}

fn open_or_create(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

fn ensure_file(path: &Path) -> Result<(), DatabaseError> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(StorageError::Corrupted(format!(
            "Invalid multiprocess database file: {}",
            path.display()
        ))
        .into());
    }
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<(), DatabaseError> {
    if !std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(StorageError::Corrupted(format!(
            "Invalid multiprocess database directory: {}",
            path.display()
        ))
        .into());
    }
    Ok(())
}

fn lock_error(error: std::io::Error) -> StorageError {
    if error.kind() == ErrorKind::Unsupported {
        StorageError::Io(std::io::Error::new(
            ErrorKind::Unsupported,
            "File locking is required for a multiprocess database",
        ))
    } else {
        StorageError::Io(error)
    }
}

fn lock_try_error(error: TryLockError) -> StorageError {
    match error {
        TryLockError::WouldBlock => StorageError::Io(std::io::Error::new(
            ErrorKind::WouldBlock,
            "File is locked by another process",
        )),
        TryLockError::Error(error) => lock_error(error),
    }
}

fn database_error_to_storage(error: DatabaseError) -> StorageError {
    match error {
        DatabaseError::Storage(storage) => storage,
        other => StorageError::Corrupted(other.to_string()),
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), DatabaseError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), DatabaseError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    sync_directory(parent)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), DatabaseError> {
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), DatabaseError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trip() {
        for mode in [WriterMode::SingleWriter, WriterMode::MultipleWriters] {
            let directory = tempfile::tempdir().unwrap();
            let layout = DatabaseDirectory::new(directory.path());
            std::fs::write(layout.metadata_file(), {
                let mut bytes = [0; METADATA_LEN];
                bytes[..METADATA_MAGIC.len()].copy_from_slice(&METADATA_MAGIC);
                bytes[METADATA_MAGIC.len()] = FORMAT_VERSION;
                bytes[METADATA_MAGIC.len() + 1] = mode.to_byte();
                bytes
            })
            .unwrap();
            assert_eq!(layout.open_metadata().unwrap().1, mode);
        }
    }
}
