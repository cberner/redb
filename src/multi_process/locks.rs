//! The on-disk lock files that implement the multi-process protocol.
//!
//! Everything in here uses only `std::fs` file operations and the advisory file locks exposed by
//! `std::fs::File`. See `docs/design.md` for the protocol these primitives implement.

use crate::{Result, StorageError};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub(super) const DATA_FILE_NAME: &str = "data.redb";
const REGISTRY_FILE_NAME: &str = "registry.lock";
const WRITE_LOCK_FILE_NAME: &str = "write.lock";
const READERS_DIR_NAME: &str = "readers";

const MAGIC: [u8; 8] = [b'r', b'e', b'd', b'b', b'M', b'P', b'r', b'o'];
const FORMAT_VERSION: u32 = 1;
const REGISTRY_LEN: usize = 48;

/// Value stored in a reader slot when its owner has no transaction pinned. Chosen so that the
/// minimum over all slots ignores idle processes without a special case.
pub(super) const UNPINNED: u64 = u64::MAX;

/// Which processes may write to a multi-process database.
///
/// The mode is fixed when the database is created and recorded in `registry.lock`, so that every
/// process which opens it agrees on the protocol.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WriterMode {
    /// Only one process may open the database for writing. That process holds the write lock for
    /// as long as it is open, so its in-memory state is always authoritative and its own
    /// transactions run at full single-process speed. Any number of other processes may open the
    /// database read-only.
    ///
    /// A second process attempting to open the database for writing fails with
    /// [`DatabaseError::DatabaseAlreadyOpen`](crate::DatabaseError::DatabaseAlreadyOpen).
    SingleWriterProcess,
    /// Any number of processes may open the database for writing, but only one write transaction
    /// may be in progress at a time: [`begin_write`](super::MultiProcessDatabase::begin_write)
    /// blocks until the process holding the write lock finishes.
    ///
    /// Every commit is a quick-repair commit, so that the next process to take the write lock can
    /// load the allocator state from the file instead of rebuilding it.
    MultiWriterProcess,
}

impl WriterMode {
    fn to_u32(self) -> u32 {
        match self {
            WriterMode::SingleWriterProcess => 1,
            WriterMode::MultiWriterProcess => 2,
        }
    }

    fn from_u32(value: u32) -> Result<Self> {
        match value {
            1 => Ok(WriterMode::SingleWriterProcess),
            2 => Ok(WriterMode::MultiWriterProcess),
            _ => Err(StorageError::Corrupted(format!(
                "Invalid writer mode in registry: {value}"
            ))),
        }
    }
}

/// The contents of `registry.lock`: the state that every process shares, protected by the lock on
/// that same file.
#[derive(Copy, Clone, Debug)]
pub(super) struct SharedState {
    pub(super) mode: WriterMode,
    /// Number of reader slot files that have been created. Slots are named `readers/<index>` for
    /// index in `0..slot_count`, so a process can find them all without scanning the directory.
    pub(super) slot_count: u32,
    /// The last transaction id that a writer has made durable. Published after the commit is on
    /// disk, so a process which reads this value can rely on the database file being at least
    /// that new.
    pub(super) last_committed: u64,
    /// The highest free horizon any writer has used. A page that was freed by a transaction older
    /// than this may have been handed back out, so a process whose cached pages are all from
    /// snapshots at or after this value knows its cache cannot be stale.
    pub(super) reclaim_horizon: u64,
    /// Counts every announcement of a free horizon, whether or not it raised `reclaim_horizon`.
    ///
    /// A process's own reclamation can never invalidate its own page cache -- writing a page
    /// replaces whatever the cache held for it -- so what a process needs to know is whether
    /// anyone *else* has reclaimed since it last looked, which the horizon alone cannot say. A
    /// writer records this counter when it publishes, and only ever publishes while it holds the
    /// write lock, so a value it does not recognize can only have come from another process.
    pub(super) reclaim_sequence: u64,
}

impl SharedState {
    fn to_bytes(self) -> [u8; REGISTRY_LEN] {
        let mut bytes = [0u8; REGISTRY_LEN];
        bytes[0..8].copy_from_slice(&MAGIC);
        bytes[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.mode.to_u32().to_le_bytes());
        bytes[16..20].copy_from_slice(&self.slot_count.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.last_committed.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.reclaim_horizon.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.reclaim_sequence.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; REGISTRY_LEN]) -> Result<Self> {
        if bytes[0..8] != MAGIC {
            return Err(StorageError::Corrupted(
                "Not a redb multi-process registry file".to_string(),
            ));
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(StorageError::Corrupted(format!(
                "Unsupported multi-process registry version: {version}"
            )));
        }
        Ok(Self {
            mode: WriterMode::from_u32(u32::from_le_bytes(bytes[12..16].try_into().unwrap()))?,
            slot_count: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            last_committed: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            reclaim_horizon: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            reclaim_sequence: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        })
    }
}

fn io_err(err: std::io::Error) -> StorageError {
    StorageError::Io(err)
}

/// Maps the "this platform has no file locks" case to an error. Multi-process databases have no
/// way to be safe without them, so unlike [`crate::Database`] they refuse to open rather than
/// warning and continuing.
fn lock_error(err: TryLockError) -> StorageError {
    match err {
        TryLockError::WouldBlock => StorageError::Io(std::io::Error::new(
            ErrorKind::WouldBlock,
            "file is locked by another process",
        )),
        TryLockError::Error(err) => unsupported_lock(err),
    }
}

fn unsupported_lock(err: std::io::Error) -> StorageError {
    if err.kind() == ErrorKind::Unsupported {
        StorageError::Io(std::io::Error::new(
            ErrorKind::Unsupported,
            "file locking is not supported on this platform, so a multi-process database cannot \
             be opened safely",
        ))
    } else {
        StorageError::Io(err)
    }
}

fn open_or_create(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(io_err)
}

fn read_at(file: &File, offset: u64, out: &mut [u8]) -> Result<()> {
    // Seek + read rather than the platform-specific positional APIs, so that this stays portable
    // and free of unsafe. The caller holds a lock that makes the file cursor's use single threaded.
    let mut handle = file;
    handle.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    handle.read_exact(out).map_err(io_err)
}

fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    let mut handle = file;
    handle.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    handle.write_all(data).map_err(io_err)
}

/// An advisory lock held on a file, released when this guard is dropped.
///
/// Every lock in this module is taken and released while a `Mutex` on the owning structure is
/// held, so that threads in the same process never share a lock: on Unix, advisory locks belong to
/// the open file description, so two threads locking the same `File` would each believe they hold
/// it and the first unlock would release it for both.
pub(super) struct FileLockGuard<'a> {
    file: &'a File,
}

impl Drop for FileLockGuard<'_> {
    fn drop(&mut self) {
        // Nothing can be done about a failure here, and the lock is released when the process
        // exits in any case
        let _ = self.file.unlock();
    }
}

fn lock_exclusive(file: &File) -> Result<FileLockGuard<'_>> {
    file.lock().map_err(unsupported_lock)?;
    Ok(FileLockGuard { file })
}

fn lock_shared(file: &File) -> Result<FileLockGuard<'_>> {
    file.lock_shared().map_err(unsupported_lock)?;
    Ok(FileLockGuard { file })
}

/// The paths that make up a multi-process database directory.
#[derive(Debug, Clone)]
pub(crate) struct DatabaseDir {
    root: PathBuf,
}

impl DatabaseDir {
    pub(super) fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub(super) fn data_file(&self) -> PathBuf {
        self.root.join(DATA_FILE_NAME)
    }

    fn registry_file(&self) -> PathBuf {
        self.root.join(REGISTRY_FILE_NAME)
    }

    fn write_lock_file(&self) -> PathBuf {
        self.root.join(WRITE_LOCK_FILE_NAME)
    }

    fn readers_dir(&self) -> PathBuf {
        self.root.join(READERS_DIR_NAME)
    }

    fn reader_slot(&self, index: u32) -> PathBuf {
        self.readers_dir().join(format!("{index:08}"))
    }

    /// Creates the directory and its lock files, if they do not already exist. Concurrent calls
    /// from several processes are safe: the registry contents are initialized under its exclusive
    /// lock, by whichever process gets there first.
    pub(super) fn create(&self, mode: WriterMode) -> Result<WriterMode> {
        std::fs::create_dir_all(&self.root).map_err(io_err)?;
        std::fs::create_dir_all(self.readers_dir()).map_err(io_err)?;
        let _ = open_or_create(&self.write_lock_file())?;
        let registry = open_or_create(&self.registry_file())?;
        let _guard = lock_exclusive(&registry)?;
        let len = registry.metadata().map_err(io_err)?.len();
        if len == 0 {
            let state = SharedState {
                mode,
                slot_count: 0,
                last_committed: 0,
                reclaim_horizon: 0,
                reclaim_sequence: 0,
            };
            write_at(&registry, 0, &state.to_bytes())?;
            registry.sync_all().map_err(io_err)?;
            Ok(mode)
        } else {
            Ok(Self::read_state(&registry)?.mode)
        }
    }

    /// Checks that this is a multi-process database directory and returns the mode it was created
    /// with.
    pub(super) fn mode(&self) -> Result<WriterMode> {
        let registry = OpenOptions::new()
            .read(true)
            .open(self.registry_file())
            .map_err(io_err)?;
        let _guard = lock_shared(&registry)?;
        Ok(Self::read_state(&registry)?.mode)
    }

    fn read_state(registry: &File) -> Result<SharedState> {
        let mut bytes = [0u8; REGISTRY_LEN];
        read_at(registry, 0, &mut bytes)?;
        SharedState::from_bytes(&bytes)
    }
}

/// `registry.lock`, plus the reader slot files it indexes.
///
/// The lock on this file is the mutual exclusion between a process publishing its pinned
/// transaction and a writer scanning for the oldest pinned transaction: readers take it shared,
/// writers take it exclusively.
pub(super) struct Registry {
    dir: DatabaseDir,
    file: File,
    /// Our own slot, held exclusively locked for as long as the database is open. Other processes
    /// detect that it is in use by failing to take a shared lock on it.
    slot: File,
    slot_index: u32,
    /// The value last written to our slot, so that repeated publications of the same value do no
    /// I/O
    published: u64,
    /// Slot files belonging to other processes, opened lazily as they appear
    other_slots: Vec<Option<File>>,
}

impl Registry {
    /// Opens the registry and claims a reader slot for this process.
    pub(super) fn open(dir: &DatabaseDir) -> Result<Self> {
        let file = open_or_create(&dir.registry_file())?;
        let guard = lock_exclusive(&file)?;
        let mut state = DatabaseDir::read_state(&file)?;

        // Claim the first slot that no live process holds, so that slot files are reused rather
        // than accumulating one per process that has ever opened the database
        let mut claimed = None;
        for index in 0..state.slot_count {
            let slot = open_or_create(&dir.reader_slot(index))?;
            match slot.try_lock() {
                Ok(()) => {
                    claimed = Some((index, slot));
                    break;
                }
                Err(TryLockError::WouldBlock) => {}
                Err(err) => return Err(lock_error(err)),
            }
        }
        let (slot_index, slot) = if let Some(claimed) = claimed {
            claimed
        } else {
            {
                let index = state.slot_count;
                let slot = open_or_create(&dir.reader_slot(index))?;
                // Nothing else can be holding a slot we just created while we hold the registry
                // lock, so this cannot legitimately fail
                slot.try_lock().map_err(lock_error)?;
                state.slot_count = index + 1;
                write_at(&file, 0, &state.to_bytes())?;
                (index, slot)
            }
        };
        write_at(&slot, 0, &UNPINNED.to_le_bytes())?;
        drop(guard);

        Ok(Self {
            dir: dir.clone(),
            file,
            slot,
            slot_index,
            published: UNPINNED,
            other_slots: vec![],
        })
    }

    #[cfg(test)]
    pub(super) fn slot_index(&self) -> u32 {
        self.slot_index
    }

    /// Locks the registry for shared access: concurrently with other processes publishing their
    /// pinned transactions, but never while a writer is scanning them.
    ///
    /// The caller must hold whatever in-process mutex owns this `Registry` for as long as the lock
    /// is held, and must call [`Self::unlock`] before releasing it.
    pub(super) fn lock_shared(&self) -> Result<()> {
        self.file.lock_shared().map_err(unsupported_lock)
    }

    /// Locks the registry exclusively: every other process's publication of its pinned transaction
    /// is either already complete or has not yet started.
    pub(super) fn lock_exclusive(&self) -> Result<()> {
        self.file.lock().map_err(unsupported_lock)
    }

    pub(super) fn unlock(&self) {
        // Nothing can be done about a failure here, and the lock is released when the process
        // exits in any case
        let _ = self.file.unlock();
    }

    /// The shared state. The caller must hold the registry lock.
    pub(super) fn state(&self) -> Result<SharedState> {
        DatabaseDir::read_state(&self.file)
    }

    /// Records that a durable commit up to `last_committed` is on disk. The caller must hold the
    /// registry lock exclusively.
    pub(super) fn publish_commit(&self, last_committed: u64) -> Result<()> {
        let mut state = DatabaseDir::read_state(&self.file)?;
        if last_committed > state.last_committed {
            state.last_committed = last_committed;
            write_at(&self.file, 0, &state.to_bytes())?;
        }
        Ok(())
    }

    /// Records that pages freed by transactions older than `horizon` may be handed back out. Must
    /// be published before any such page is reused, so that a process which later checks it either
    /// sees this announcement or has not yet cached anything that could go stale. The caller must
    /// hold the registry lock exclusively.
    ///
    /// Returns the sequence number of this announcement, which the caller records so that it can
    /// recognize its own.
    pub(super) fn publish_reclaim_horizon(&self, horizon: u64) -> Result<u64> {
        let mut state = DatabaseDir::read_state(&self.file)?;
        state.reclaim_horizon = state.reclaim_horizon.max(horizon);
        state.reclaim_sequence += 1;
        write_at(&self.file, 0, &state.to_bytes())?;
        Ok(state.reclaim_sequence)
    }

    /// True if this process's slot already holds `pinned`, so that publishing it again would do
    /// nothing. Lets a caller skip taking the registry lock at all.
    pub(super) fn already_published(&self, pinned: u64) -> bool {
        pinned == self.published
    }

    /// Publishes the oldest transaction this process needs kept alive. The caller must hold the
    /// registry lock (shared is enough: only this process writes this slot).
    pub(super) fn publish_pinned(&mut self, pinned: u64) -> Result<()> {
        if pinned == self.published {
            return Ok(());
        }
        write_at(&self.slot, 0, &pinned.to_le_bytes())?;
        self.published = pinned;
        Ok(())
    }

    /// The oldest transaction pinned by any process other than this one, or `None` if no other
    /// process has a live read transaction or savepoint. The caller must hold the registry lock
    /// exclusively, so that no process is part way through publishing.
    pub(super) fn oldest_pinned_by_others(&mut self) -> Result<Option<u64>> {
        let slot_count = self.state()?.slot_count;
        while u32::try_from(self.other_slots.len()).unwrap() < slot_count {
            self.other_slots.push(None);
        }

        let mut oldest: Option<u64> = None;
        for index in 0..slot_count {
            if index == self.slot_index {
                continue;
            }
            let position = usize::try_from(index).unwrap();
            if self.other_slots[position].is_none() {
                self.other_slots[position] = Some(open_or_create(&self.dir.reader_slot(index))?);
            }
            let slot = self.other_slots[position].as_ref().unwrap();
            match slot.try_lock_shared() {
                // No live process holds this slot, so whatever it contains is stale
                Ok(()) => {
                    let _ = slot.unlock();
                    continue;
                }
                Err(TryLockError::WouldBlock) => {}
                Err(err) => return Err(lock_error(err)),
            }
            let mut bytes = [0u8; 8];
            match read_at(slot, 0, &mut bytes) {
                Ok(()) => {}
                // A process that crashed between creating its slot and writing to it leaves the
                // file empty. It holds no transaction, since it never registered one
                Err(StorageError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => continue,
                Err(err) => return Err(err),
            }
            let pinned = u64::from_le_bytes(bytes);
            if pinned != UNPINNED {
                oldest = Some(oldest.map_or(pinned, |current: u64| current.min(pinned)));
            }
        }

        Ok(oldest)
    }
}

/// `write.lock`: held exclusively by the process which owns the single logical writer.
pub(crate) struct WriteLock {
    file: File,
    held: bool,
}

impl WriteLock {
    pub(super) fn open(dir: &DatabaseDir) -> Result<Self> {
        Ok(Self {
            file: open_or_create(&dir.write_lock_file())?,
            held: false,
        })
    }

    /// Takes the lock, failing rather than blocking if another process holds it.
    pub(super) fn try_acquire(&mut self) -> Result<bool> {
        assert!(!self.held);
        match self.file.try_lock() {
            Ok(()) => {
                self.held = true;
                Ok(true)
            }
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(err) => Err(lock_error(err)),
        }
    }

    /// Takes the lock, blocking until the process holding it releases it.
    pub(super) fn acquire(&mut self) -> Result<()> {
        assert!(!self.held);
        self.file.lock().map_err(unsupported_lock)?;
        self.held = true;
        Ok(())
    }

    pub(super) fn release(&mut self) {
        if self.held {
            let _ = self.file.unlock();
            self.held = false;
        }
    }

    pub(super) fn is_held(&self) -> bool {
        self.held
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn shared_state_round_trip() {
        let state = SharedState {
            mode: WriterMode::MultiWriterProcess,
            slot_count: 7,
            last_committed: 12345,
            reclaim_horizon: 999,
            reclaim_sequence: 3,
        };
        let decoded = SharedState::from_bytes(&state.to_bytes()).unwrap();
        assert_eq!(decoded.mode, WriterMode::MultiWriterProcess);
        assert_eq!(decoded.slot_count, 7);
        assert_eq!(decoded.last_committed, 12345);
        assert_eq!(decoded.reclaim_horizon, 999);
        assert_eq!(decoded.reclaim_sequence, 3);
    }

    #[test]
    fn shared_state_rejects_foreign_file() {
        let bytes = [0u8; REGISTRY_LEN];
        assert!(SharedState::from_bytes(&bytes).is_err());
    }

    #[test]
    fn slots_are_reused_after_a_process_exits() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path());
        dir.create(WriterMode::MultiWriterProcess).unwrap();

        let first = Registry::open(&dir).unwrap();
        let second = Registry::open(&dir).unwrap();
        assert_eq!(first.slot_index(), 0);
        assert_eq!(second.slot_index(), 1);
        drop(first);

        let third = Registry::open(&dir).unwrap();
        assert_eq!(third.slot_index(), 0);
    }

    fn scan(registry: &mut Registry) -> Option<u64> {
        registry.lock_exclusive().unwrap();
        let result = registry.oldest_pinned_by_others();
        registry.unlock();
        result.unwrap()
    }

    fn publish(registry: &mut Registry, pinned: u64) {
        registry.lock_shared().unwrap();
        let result = registry.publish_pinned(pinned);
        registry.unlock();
        result.unwrap();
    }

    #[test]
    fn pinned_transactions_are_visible_to_other_slots() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path());
        dir.create(WriterMode::MultiWriterProcess).unwrap();

        let mut writer = Registry::open(&dir).unwrap();
        assert_eq!(None, scan(&mut writer));

        let mut reader = Registry::open(&dir).unwrap();
        publish(&mut reader, 42);
        assert_eq!(Some(42), scan(&mut writer));

        // A second reader only lowers the answer
        let mut reader2 = Registry::open(&dir).unwrap();
        publish(&mut reader2, 99);
        assert_eq!(Some(42), scan(&mut writer));
        publish(&mut reader2, 7);
        assert_eq!(Some(7), scan(&mut writer));
        drop(reader2);

        publish(&mut reader, UNPINNED);
        assert_eq!(None, scan(&mut writer));

        // A slot whose owner is gone is ignored, even though it still holds a pinned value
        publish(&mut reader, 7);
        drop(reader);
        assert_eq!(None, scan(&mut writer));
    }

    #[test]
    fn shared_state_is_published_across_handles() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path());
        dir.create(WriterMode::SingleWriterProcess).unwrap();

        let writer = Registry::open(&dir).unwrap();
        writer.lock_exclusive().unwrap();
        writer.publish_commit(17).unwrap();
        assert_eq!(1, writer.publish_reclaim_horizon(5).unwrap());
        writer.unlock();

        let other = Registry::open(&dir).unwrap();
        other.lock_shared().unwrap();
        let state = other.state().unwrap();
        other.unlock();
        assert_eq!(state.last_committed, 17);
        assert_eq!(state.reclaim_horizon, 5);
        assert_eq!(state.reclaim_sequence, 1);
        assert_eq!(state.mode, WriterMode::SingleWriterProcess);

        // Both are monotonic, but every announcement is counted
        writer.lock_exclusive().unwrap();
        writer.publish_commit(3).unwrap();
        assert_eq!(2, writer.publish_reclaim_horizon(1).unwrap());
        writer.unlock();
        other.lock_shared().unwrap();
        let state = other.state().unwrap();
        other.unlock();
        assert_eq!(state.last_committed, 17);
        assert_eq!(state.reclaim_horizon, 5);
        assert_eq!(state.reclaim_sequence, 2);
    }

    #[test]
    fn the_write_lock_excludes_other_handles() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path());
        dir.create(WriterMode::MultiWriterProcess).unwrap();

        let mut first = WriteLock::open(&dir).unwrap();
        let mut second = WriteLock::open(&dir).unwrap();
        assert!(first.try_acquire().unwrap());
        assert!(!second.try_acquire().unwrap());
        first.release();
        assert!(second.try_acquire().unwrap());
        second.release();
    }

    #[test]
    fn the_mode_is_fixed_at_creation() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path());
        assert_eq!(
            WriterMode::SingleWriterProcess,
            dir.create(WriterMode::SingleWriterProcess).unwrap()
        );
        assert_eq!(
            WriterMode::SingleWriterProcess,
            dir.create(WriterMode::MultiWriterProcess).unwrap()
        );
        assert_eq!(WriterMode::SingleWriterProcess, dir.mode().unwrap());
    }
}
