//! The files that make up a multi-process database directory, and the lock that excludes other
//! processes from it.
//!
//! Everything in here uses only `std::fs` file operations and the advisory file locks exposed by
//! `std::fs::File`. See `docs/design.md` for the protocol these files implement.

use crate::{DatabaseError, Result, StorageError};
use alloc::vec::Vec;
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const DATA_FILE_NAME: &str = "data.redb";
const WRITE_LOCK_FILE_NAME: &str = "write.lock";
const METADATA_FILE_NAME: &str = "metadata";
const METADATA_TMP_FILE_NAME: &str = "metadata.tmp";
const DATA_TMP_FILE_NAME: &str = "data.redb.tmp";
const REGISTRY_FILE_NAME: &str = "registry.lock";
const PINNED_DIR_NAME: &str = "txn";

/// Length of `registry.lock`. Identity and format version live in `metadata`, so this file holds
/// nothing but the mutable shared state, and there is one place a version can need bumping.
const REGISTRY_LEN: usize = 32;

/// Stands for "this process needs no transaction kept alive", which on disk is the absence of any
/// file in `txn/` belonging to it. Chosen so that a minimum taken across processes ignores the idle
/// ones without a special case.
pub(super) const UNPINNED: u64 = u64::MAX;

const MAGIC: [u8; 8] = [b'r', b'e', b'd', b'b', b'M', b'P', b'r', b'o'];
const FORMAT_VERSION: u32 = 1;
const METADATA_LEN: usize = 12;

/// Maps the "this platform has no file locks" case to an error. A multi-process database has no way
/// to be safe without them, so unlike [`crate::Database`] it refuses to open rather than warning
/// and continuing.
fn lock_unsupported(err: io::Error) -> DatabaseError {
    if err.kind() == ErrorKind::Unsupported {
        return StorageError::Io(io::Error::new(
            ErrorKind::Unsupported,
            "file locking is not supported on this platform, so a multi-process database cannot \
             be opened safely",
        ))
        .into();
    }
    StorageError::Io(err).into()
}

fn open_or_create(path: &Path) -> Result<File, io::Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// Flushes a directory itself, so that an entry a rename just created is durable.
///
/// Best-effort about a directory this process cannot open: flushing one needs read permission,
/// while everything else here needs only to traverse it and change its entries, so requiring it
/// would fail `create()` on a database `open()` handles perfectly well.
#[cfg(unix)]
fn sync_dir(root: &Path) -> Result<()> {
    let dir = match File::open(root) {
        Ok(dir) => dir,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => return Ok(()),
        Err(err) => return Err(StorageError::Io(err)),
    };
    dir.sync_all().map_err(StorageError::Io)?;

    Ok(())
}

/// A known gap rather than a case that needs no handling: `std` exposes no directory handle to sync
/// off Unix, and no way to make the rename itself write-through. `docs/design.md` records what a
/// crash can cost here.
#[cfg(not(unix))]
fn sync_dir(_root: &Path) -> Result<()> {
    Ok(())
}

/// The directory a path lives in, as something openable. `Path::parent` gives an empty path for a
/// bare relative name, which is not a directory anything can open.
fn parent_of(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Flushes the directory holding the database directory.
fn sync_parent(root: &Path) -> Result<()> {
    // Canonicalized first: `Path::parent` is purely lexical, so for a path ending in `..` it names
    // a child of the real directory rather than its parent, and the fsync would flush the wrong one
    let root = std::fs::canonicalize(root).map_err(StorageError::Io)?;
    sync_dir(&parent_of(&root))
}

/// Whether a name is taken, by anything at all -- including a symlink that resolves to nothing,
/// which `Path::exists` reports as absent because it follows the link.
fn occupied(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

/// Refuses anything that is not an ordinary file under one of this database's own names.
///
/// Every open here traverses symlinks and would be written through to whatever one points at, and
/// opening a FIFO read-only blocks until a writer appears. A missing file is fine: that is the case
/// where one is about to be made.
///
/// This closes the door rather than locking it -- the entry can be replaced between the check and
/// the open, and doing better needs `O_NOFOLLOW`, which `std` does not expose portably.
fn require_regular_file(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(StorageError::Io(io::Error::new(
            ErrorKind::InvalidData,
            "a multi-process database directory may hold only ordinary files",
        ))),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Which name [`DatabaseDir::open_data`] put the database file under.
///
/// Carried from the open to [`DatabaseDir::promote_data`] rather than worked out again from what is
/// on disk. A temporary file that is there at the end of a `create()` is this call's own only if
/// this call made it, and nothing about the file itself says so.
#[derive(Clone, Copy)]
pub(super) enum DataLocation {
    /// `data.redb`, which is where a database that was already finished lives.
    Final,
    /// `data.redb.tmp`, which is where one being initialized lives until it is renamed into place.
    Temporary,
}

/// Which processes may write to a multi-process database.
///
/// Fixed when the database is created and recorded in `registry.lock`, so that every process which
/// opens it agrees on the protocol.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum WriterMode {
    /// Only one process may open the database for writing. That process holds the write lock for
    /// as long as it is open, so its in-memory state is always authoritative and its own
    /// transactions run at full single-process speed. Any number of other processes may open the
    /// database read-only.
    ///
    /// A second process opening it for writing fails with
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

/// The contents of `registry.lock`: the state every process shares, protected by the lock on that
/// same file.
///
/// Carries no magic number of its own. `metadata` is what says a directory is redb's, and it is
/// written last, so a directory that is marked has a complete registry and one that is not is
/// refused before this is ever read.
#[derive(Copy, Clone, Debug)]
pub(super) struct SharedState {
    pub(super) mode: WriterMode,
    /// The last transaction id a writer has made durable. Published after the commit is on disk,
    /// so a process reading this can rely on the file being at least that new.
    pub(super) last_committed: u64,
    /// The highest free horizon any writer has used. A page freed by a transaction older than this
    /// may have been handed back out, so a process whose cached pages all come from snapshots at
    /// or after it knows its cache cannot be stale.
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
        bytes[0..4].copy_from_slice(&self.mode.to_u32().to_le_bytes());
        bytes[8..16].copy_from_slice(&self.last_committed.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.reclaim_horizon.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.reclaim_sequence.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; REGISTRY_LEN]) -> Result<Self> {
        Ok(Self {
            mode: WriterMode::from_u32(u32::from_le_bytes(bytes[0..4].try_into().unwrap()))?,
            last_committed: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            reclaim_horizon: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            reclaim_sequence: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
}

fn lock_error(err: TryLockError) -> StorageError {
    match err {
        TryLockError::WouldBlock => StorageError::Io(io::Error::new(
            ErrorKind::WouldBlock,
            "a lock this database needs is held by another process",
        )),
        TryLockError::Error(err) => unsupported_lock(err),
    }
}

fn unsupported_lock(err: io::Error) -> StorageError {
    match lock_unsupported(err) {
        DatabaseError::Storage(err) => err,
        other => StorageError::Io(io::Error::other(other.to_string())),
    }
}

fn read_at(file: &File, offset: u64, out: &mut [u8]) -> Result<()> {
    // Seek and read rather than the platform-specific positional APIs, so this stays portable and
    // free of unsafe. The caller holds a lock that makes the file cursor's use single threaded.
    let mut handle = file;
    handle
        .seek(SeekFrom::Start(offset))
        .map_err(StorageError::Io)?;
    handle.read_exact(out).map_err(StorageError::Io)
}

fn write_at(file: &File, offset: u64, data: &[u8]) -> Result<()> {
    let mut handle = file;
    handle
        .seek(SeekFrom::Start(offset))
        .map_err(StorageError::Io)?;
    handle.write_all(data).map_err(StorageError::Io)
}

/// An advisory lock held on a file, released when this guard is dropped.
///
/// Every lock here is taken and released while a mutex on the owning structure is held, so threads
/// in one process never share a lock: advisory locks belong to the open file description, so two
/// threads locking the same `File` would each believe they held it and the first unlock would
/// release it for both.
struct FileLockGuard<'a> {
    file: &'a File,
}

impl Drop for FileLockGuard<'_> {
    fn drop(&mut self) {
        // Nothing can be done about a failure, and the lock goes when the process exits anyway
        let _ = self.file.unlock();
    }
}

fn lock_exclusive(file: &File) -> Result<FileLockGuard<'_>> {
    file.lock().map_err(unsupported_lock)?;
    Ok(FileLockGuard { file })
}

/// The paths that make up a multi-process database directory.
#[derive(Debug, Clone)]
pub(crate) struct DatabaseDir {
    root: PathBuf,
}

impl DatabaseDir {
    /// Every name `create()` writes after taking the write lock. `metadata` is not among them: the
    /// check that uses this runs only where there is no marker.
    const WRITTEN_UNDER_THE_LOCK: &'static [&'static str] = &[
        DATA_FILE_NAME,
        DATA_TMP_FILE_NAME,
        METADATA_TMP_FILE_NAME,
        REGISTRY_FILE_NAME,
        PINNED_DIR_NAME,
    ];

    pub(super) fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub(super) fn data_file(&self) -> PathBuf {
        self.root.join(DATA_FILE_NAME)
    }

    fn write_lock_file(&self) -> PathBuf {
        self.root.join(WRITE_LOCK_FILE_NAME)
    }

    fn metadata_file(&self) -> PathBuf {
        self.root.join(METADATA_FILE_NAME)
    }

    fn metadata_tmp_file(&self) -> PathBuf {
        self.root.join(METADATA_TMP_FILE_NAME)
    }

    fn data_tmp_file(&self) -> PathBuf {
        self.root.join(DATA_TMP_FILE_NAME)
    }

    fn registry_file(&self) -> PathBuf {
        self.root.join(REGISTRY_FILE_NAME)
    }

    fn pinned_dir(&self) -> PathBuf {
        self.root.join(PINNED_DIR_NAME)
    }

    /// The file whose *name* is `id`. Nothing is ever read from or written to it: the name carries
    /// the whole of the data, and the lock on it says whether anyone still needs that transaction.
    fn pinned_file(&self, id: u64) -> PathBuf {
        self.pinned_dir().join(id.to_string())
    }

    /// Creates `registry.lock` and the reader-slot directory, and initializes the shared state if
    /// this call is the one that made it.
    ///
    /// Guarded by the registry's own exclusive lock rather than the write lock: a process opening
    /// the database read-only never takes the write lock, but does read this file.
    ///
    /// Returns the mode the database actually has, which is the one recorded by whichever process
    /// created it -- not necessarily the one asked for here.
    pub(super) fn init_registry(&self, mode: WriterMode) -> Result<WriterMode> {
        std::fs::create_dir_all(self.pinned_dir()).map_err(StorageError::Io)?;
        require_regular_file(&self.registry_file())?;
        let registry = open_or_create(&self.registry_file()).map_err(StorageError::Io)?;
        let _guard = lock_exclusive(&registry)?;
        if registry.metadata().map_err(StorageError::Io)?.len() == 0 {
            let state = SharedState {
                mode,
                last_committed: 0,
                reclaim_horizon: 0,
                reclaim_sequence: 0,
            };
            write_at(&registry, 0, &state.to_bytes())?;
            registry.sync_all().map_err(StorageError::Io)?;
            sync_dir(&self.root)?;
            Ok(mode)
        } else {
            Ok(Self::read_state(&registry)?.mode)
        }
    }

    /// Resets the mutable shared state, keeping the mode.
    ///
    /// For the call that *initialized* the database file. Transaction ids start from the beginning
    /// in a new file, so anything a previous database left in the registry describes something that
    /// is gone -- and its `last_committed` would be read as a commit this database has not made,
    /// sending the next writer looking for allocator state that was never written.
    ///
    /// The caller holds the write lock, and the registry's own lock is taken here.
    pub(super) fn reset_shared_state(&self) -> Result<()> {
        require_regular_file(&self.registry_file())?;
        let registry = open_or_create(&self.registry_file()).map_err(StorageError::Io)?;
        let _guard = lock_exclusive(&registry)?;
        let mut state = Self::read_state(&registry)?;
        state.last_committed = 0;
        state.reclaim_horizon = 0;
        // Raised rather than reset, so that a process which has seen the old counter reads the next
        // announcement as another process's rather than as one of its own
        state.reclaim_sequence += 1;
        write_at(&registry, 0, &state.to_bytes())?;
        registry.sync_all().map_err(StorageError::Io)?;

        Ok(())
    }

    /// The mode an existing database was created with.
    pub(super) fn mode(&self) -> Result<WriterMode> {
        require_regular_file(&self.registry_file())?;
        let registry = OpenOptions::new()
            .read(true)
            .open(self.registry_file())
            .map_err(StorageError::Io)?;
        let _guard = lock_exclusive(&registry)?;
        Ok(Self::read_state(&registry)?.mode)
    }

    fn read_state(registry: &File) -> Result<SharedState> {
        let mut bytes = [0u8; REGISTRY_LEN];
        read_at(registry, 0, &mut bytes)?;
        SharedState::from_bytes(&bytes)
    }

    /// Validates the directory and creates it if `create` is set, up to but not including taking
    /// the write lock.
    ///
    /// Split from [`Self::open_data`] because the write lock now belongs to the coordinator rather
    /// than to the backend: readers hold no lock on the database file, so the caller decides how
    /// the write lock is taken, and everything that has to happen either side of it lives in one
    /// of these two halves.
    pub(super) fn prepare(&self, create: bool) -> Result<(), DatabaseError> {
        if create {
            // All three before the directory is touched, so that the common case -- a mistyped
            // path -- is refused without even the lock file having been made.
            //
            // A marker arrives by rename, so one that is there is complete: a file under that name
            // which is not a marker is not a state redb can produce, and can be refused without the
            // lock. Where there is no marker the directory's contents decide instead. Both checks
            // run again under the lock, which is the authoritative pass; these only turn away what
            // is already certain, and never accept on their own
            if occupied(&self.metadata_file()) {
                self.read_metadata(create)?;
            } else {
                self.reject_unmarked_database()?;
                self.reject_foreign_directory()?;
            }
            std::fs::create_dir_all(&self.root).map_err(StorageError::Io)?;
            // Syncing entries *inside* a directory whose own entry was never flushed loses the
            // lot. On every create(), not only the one that made the directory: finding it already
            // there says nothing about whether anyone flushed it, and the lock cannot cover the
            // directory's own creation because it lives inside it
            sync_parent(&self.root)?;
        } else if !self.root.is_dir() {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::NotFound,
                "no such multi-process database directory",
            ))
            .into());
        }

        Ok(())
    }

    /// Opens the database file, once the caller holds the write lock.
    ///
    /// The file is returned unlocked: every process needs it open at once, and `write.lock` and
    /// the reader slots are what keep a page from being reused underneath one of them.
    pub(super) fn open_data(&self, create: bool) -> Result<(File, DataLocation), DatabaseError> {
        if create {
            // Before anything else is written under these names, because it is the *absence* of
            // the lock file beside them that says a directory is not this database's. Nothing
            // orders one directory entry against another by itself, and fsyncing a file can carry
            // its own entry to disk, so without this a crash could keep `data.redb` while losing
            // `write.lock` -- and the next `create()` would refuse the database this one finished
            sync_dir(&self.root)?;
        }
        self.read_metadata(create)?;

        // A database being made for the first time is initialized under a temporary name and
        // renamed into place by `promote_data`, the same way the marker is. redb writes a new
        // header with the magic number zeroed and only then writes the magic, so a crash during
        // initialization leaves a file that is neither empty nor a database -- which `Database`
        // refuses. Under its own name that is indistinguishable from a mistyped path and must be
        // refused forever; under the temporary name it is unfinished work of ours, to be redone
        let (path, location) = if create {
            // `symlink_metadata` rather than `exists()`, which follows the link and so reports a
            // dangling symlink as an absent file. The question is whether the name is occupied at
            // all: one that is gets checked by `require_regular_file` below rather than quietly
            // replaced by the promoting rename
            match std::fs::symlink_metadata(self.data_file()) {
                Ok(metadata) if metadata.is_file() && metadata.len() == 0 => {
                    // Empty means an attempt that got as far as creating the file -- but only if
                    // nobody is holding it. A process that reached past the directory could have
                    // created and locked it a moment ago, and unlinking it would leave that
                    // process writing to an inode nothing points at
                    let existing = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(self.data_file())
                        .map_err(StorageError::Io)?;
                    match existing.try_lock() {
                        Ok(()) => {}
                        Err(TryLockError::WouldBlock) => {
                            return Err(DatabaseError::DatabaseAlreadyOpen);
                        }
                        Err(TryLockError::Error(err)) => return Err(lock_unsupported(err)),
                    }
                    // Held across the unlink: dropping it first would reopen the window it closes,
                    // since the unlink succeeds whether or not someone took the file in between
                    std::fs::remove_file(self.data_file()).map_err(StorageError::Io)?;
                    drop(existing);
                    self.discard_data_tmp()?;
                    (self.data_tmp_file(), DataLocation::Temporary)
                }
                Ok(_) => (self.data_file(), DataLocation::Final),
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    // A marked directory has already held a finished database, since the marker
                    // goes in after the promoting rename. So a missing `data.redb` with a temporary
                    // beside it means that rename did not survive -- the temporary is the database
                    // rather than an unfinished attempt, and discarding it would lose it. This is
                    // the one state where the two cannot be told apart by which files exist, so it
                    // is refused for a person to sort out instead of guessed at
                    if occupied(&self.metadata_file()) && occupied(&self.data_tmp_file()) {
                        return Err(StorageError::Io(io::Error::new(
                            ErrorKind::InvalidData,
                            "the database file is missing and a temporary one is in its place; \
                             rename it to data.redb if it holds the database",
                        ))
                        .into());
                    }
                    self.discard_data_tmp()?;
                    (self.data_tmp_file(), DataLocation::Temporary)
                }
                Err(err) => return Err(StorageError::Io(err).into()),
            }
        } else {
            (self.data_file(), DataLocation::Final)
        };

        require_regular_file(&path)?;
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(path)
            .map_err(StorageError::Io)?;

        Ok((data, location))
    }

    /// Moves a freshly initialized database file into place, if this call was the one that made it.
    ///
    /// Called once [`crate::Database`] has accepted the file, so that `data.redb` exists only when
    /// it is a database that was finished. `location` is what [`Self::open_data`] did, rather than
    /// something read back off disk: which name the database file is under is a fact this call
    /// established, and the presence of a temporary file says nothing about who left it there.
    ///
    /// The write lock is still held, so nothing can be looking at either name.
    pub(super) fn promote_data(&self, location: DataLocation) -> Result<(), DatabaseError> {
        let tmp = self.data_tmp_file();
        if matches!(location, DataLocation::Final) {
            // The database opened under its own name, so anything under the temporary one is the
            // wreckage of an earlier attempt. Done here rather than on the way in, so that a
            // `create()` pointed somewhere by mistake fails without having deleted a file it did
            // not put there -- everything above this point leaves the directory as it found it.
            //
            // Tidying only, so a failure is not the caller's problem: `Database` has already
            // accepted the file by now, promotion keys on the location this call recorded rather
            // than on what is lying about, and failing here would make `create()` an error on a
            // database `open()` handles perfectly well -- in a directory whose entries cannot be
            // changed, say. What is left behind is inert
            drop(self.discard_data_tmp());
            return Ok(());
        }
        // This call initialized the temporary, which it does only when there was no database file.
        // One appearing in the meantime means something reached past the directory and made it,
        // and renaming over it would destroy a database this call never opened
        if occupied(&self.data_file()) {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::AlreadyExists,
                "the database file appeared while it was being initialized",
            ))
            .into());
        }
        std::fs::rename(&tmp, self.data_file()).map_err(StorageError::Io)?;
        sync_dir(&self.root)?;

        Ok(())
    }

    /// Throws away a temporary database file left by an earlier attempt.
    ///
    /// Unlinks rather than truncates, so a symlink left under this name is removed rather than
    /// followed and written through.
    fn discard_data_tmp(&self) -> Result<(), DatabaseError> {
        match std::fs::remove_file(self.data_tmp_file()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io(err).into()),
        }
    }

    /// Refuses a directory holding files under these names that this type did not put there.
    ///
    /// `create()` accepts a directory with no marker, because the marker goes in last and that is
    /// what an interrupted create looks like. But `write.lock` is the *first* thing it makes, and
    /// nothing here ever removes one, so any of the names below implies a lock file beside it. One
    /// without means something else filled the directory in -- a plain [`crate::Database`] under
    /// `data.redb`, most likely -- and `create()` would go on to adopt that database, or unlink
    /// whatever holds a temporary name, in a directory it was pointed at by mistake.
    fn reject_unmarked_database(&self) -> Result<(), DatabaseError> {
        if occupied(&self.write_lock_file()) {
            return Ok(());
        }

        for name in Self::WRITTEN_UNDER_THE_LOCK {
            if occupied(&self.root.join(name)) {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "refusing to take over a directory that already holds files under the names a \
                     multi-process database uses",
                ))
                .into());
            }
        }

        Ok(())
    }

    /// Writes the marker that says this directory holds a multi-process database.
    ///
    /// Called only once the database file has been opened and initialized, so that a `create()`
    /// pointed at something that is not a redb database fails without having marked it on the way.
    ///
    /// Written under a temporary name and renamed into place, so the marker is either absent or
    /// complete and never in between: a half-written one is indistinguishable from a file that is
    /// simply not ours, which [`Self::read_metadata`] refuses, and the directory would be wedged.
    pub(super) fn write_metadata(&self) -> Result<(), DatabaseError> {
        let mut contents = [0u8; METADATA_LEN];
        contents[0..8].copy_from_slice(&MAGIC);
        contents[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());

        let tmp = self.metadata_tmp_file();
        // Removed and then created afresh rather than truncated in place: `File::create` follows a
        // symlink left under this name and writes through it to whatever it points at, and blocks
        // on a FIFO rather than failing. Unlinking first means neither is ever opened
        match std::fs::remove_file(&tmp) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(StorageError::Io(err).into()),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(StorageError::Io)?;
        file.write_all(&contents).map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        std::fs::rename(&tmp, self.metadata_file()).map_err(StorageError::Io)?;
        sync_dir(&self.root)?;

        Ok(())
    }

    /// Writes the marker only if the directory does not already carry one.
    ///
    /// [`Self::read_metadata`] validated it on the way in, under the lock, so one that is there is
    /// byte-for-byte what would be written. Rewriting it would fail outright in a directory whose
    /// entries cannot be changed, turning open-or-create into an error for no gain.
    pub(super) fn write_metadata_if_missing(&self) -> Result<(), DatabaseError> {
        if self.metadata_file().exists() {
            // The marker needs no rewriting, but this call may still have created the lock file or
            // the database file in a directory that was missing them, and `write_metadata` is
            // where the directory would otherwise be flushed
            sync_dir(&self.root)?;
            return Ok(());
        }

        self.write_metadata()
    }

    /// Refuses a directory that holds anything this database did not put there.
    ///
    /// Only consulted when there is no marker: a directory with one is this database's, and a stray
    /// file appearing next to it -- `.DS_Store`, an editor's scratch file -- must not stop it
    /// opening. Without a marker the only reason to accept a directory that already exists is that
    /// a `create()` was interrupted partway through, and such a directory holds nothing but the
    /// files `create()` itself makes. Anything else is a path this call was pointed at by mistake.
    fn reject_foreign_directory(&self) -> Result<(), DatabaseError> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            // Listing needs read permission, which nothing else here does, so a searchable but
            // unreadable directory would be one that `open()` handles and `create()` refuses to
            // finish. This rule is a guard against mistyped paths rather than a safety property --
            // a foreign `metadata`, a non-regular file under a name redb trusts, and a `data.redb`
            // with no `write.lock` beside it are all still refused, none of which needs a listing
            Err(err) if err.kind() == ErrorKind::PermissionDenied => return Ok(()),
            Err(err) => return Err(StorageError::Io(err).into()),
        };

        for entry in entries {
            let entry = entry.map_err(StorageError::Io)?;
            let name = entry.file_name();
            // The name is not enough: `file_type()` reports the entry rather than what it points
            // at, so a symlink wearing one of these names is caught here instead of being followed
            // and initialized over, somewhere outside this directory entirely
            let file_type = entry.file_type().map_err(StorageError::Io)?;
            let ours = if name == PINNED_DIR_NAME {
                // The only name here that is a directory rather than a file
                file_type.is_dir()
            } else {
                [
                    DATA_FILE_NAME,
                    DATA_TMP_FILE_NAME,
                    WRITE_LOCK_FILE_NAME,
                    METADATA_FILE_NAME,
                    METADATA_TMP_FILE_NAME,
                    REGISTRY_FILE_NAME,
                ]
                .iter()
                .any(|known| name == *known)
                    && file_type.is_file()
            };
            if !ours {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "refusing to create a multi-process database in a directory that holds files \
                     it did not write",
                ))
                .into());
            }
        }

        Ok(())
    }

    /// Checks that this directory holds a multi-process database, tolerating a missing marker when
    /// one is being created.
    ///
    /// A directory holding some other `metadata` file is refused rather than taken over, even by
    /// `create()`: overwriting it would be a destructive way to report a mistyped path. Being that
    /// strict is safe only because [`Self::write_metadata`] cannot leave one half-written.
    ///
    /// The caller must hold the write lock, which is what makes this safe against another process
    /// creating the same directory.
    fn read_metadata(&self, create: bool) -> Result<(), DatabaseError> {
        let path = self.metadata_file();
        require_regular_file(&path)?;
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if !create {
                    return Err(StorageError::Io(io::Error::new(
                        ErrorKind::NotFound,
                        "not a multi-process database directory",
                    ))
                    .into());
                }
                return self.reject_foreign_directory();
            }
            Err(err) => return Err(StorageError::Io(err).into()),
        };

        // One byte past the marker, which is enough to tell a marker from a longer file without
        // reading it: a mistyped path can point at a directory whose `metadata` is any size at all,
        // and none of it is worth allocating for
        let mut bytes = Vec::with_capacity(METADATA_LEN + 1);
        file.take((METADATA_LEN + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(StorageError::Io)?;

        if bytes.len() != METADATA_LEN || bytes[0..8] != MAGIC {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                "not a multi-process database directory",
            ))
            .into());
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                format!("unsupported multi-process database version: {version}"),
            ))
            .into());
        }

        Ok(())
    }
}

/// `registry.lock`, plus the directory of pinned transactions beside it.
///
/// The lock on this file is the mutual exclusion between a process pinning a transaction and a
/// writer scanning for the oldest one: readers take it shared, writers take it exclusively.
///
/// A pinned transaction is a file in `txn/` whose *name* is the transaction id. Nothing is read
/// from or written to those files -- the name is the whole of the data, and the lock on it says
/// whether anyone still needs that transaction. That matters for more than tidiness: a lock is
/// mandatory on some platforms, so a file another process holds locked cannot be read at all.
pub(super) struct Registry {
    dir: DatabaseDir,
    file: File,
    /// The `txn/` file this process holds, shared, for as long as it needs that transaction.
    /// Dropping it releases the lock.
    pinned: Option<File>,
    /// The id `pinned` is named for, so that publishing the same value twice does no I/O
    published: u64,
}

impl Registry {
    pub(super) fn open(dir: &DatabaseDir) -> Result<Self> {
        Ok(Self {
            dir: dir.clone(),
            file: open_or_create(&dir.registry_file()).map_err(StorageError::Io)?,
            pinned: None,
            published: UNPINNED,
        })
    }

    /// Locks the registry for shared access: concurrently with other processes pinning their
    /// transactions, but never while a writer is scanning them.
    ///
    /// The caller must hold whatever in-process mutex owns this `Registry` for as long as the lock
    /// is held, and must call [`Self::unlock`] before releasing it.
    pub(super) fn lock_shared(&self) -> Result<()> {
        self.file.lock_shared().map_err(unsupported_lock)
    }

    /// Locks the registry exclusively: every other process's pinning is either already complete or
    /// has not yet started.
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

    /// Puts `last_committed` back to an earlier value, which nothing in the protocol ever does:
    /// it stands in for a writer that made a commit durable and died before announcing it. The
    /// caller must hold the registry lock exclusively.
    #[cfg(test)]
    pub(super) fn rewind_committed(&self, last_committed: u64) -> Result<()> {
        let mut state = DatabaseDir::read_state(&self.file)?;
        state.last_committed = last_committed;
        write_at(&self.file, 0, &state.to_bytes())
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

    /// The transaction this process currently pins, or [`UNPINNED`] if it needs none.
    pub(super) fn pinned(&self) -> u64 {
        self.published
    }

    /// True if this process already pins `pinned`, so that publishing it again would do nothing.
    /// Lets a caller skip taking the registry lock at all.
    pub(super) fn already_published(&self, pinned: u64) -> bool {
        pinned == self.published
    }

    /// Pins the oldest transaction this process needs kept alive. The caller must hold the registry
    /// lock (shared is enough: this only adds a file and takes a shared lock on it).
    pub(super) fn publish_pinned(&mut self, pinned: u64) -> Result<()> {
        if pinned == self.published {
            return Ok(());
        }
        let held = if pinned == UNPINNED {
            None
        } else {
            let file = open_or_create(&self.dir.pinned_file(pinned)).map_err(StorageError::Io)?;
            // Shared, so that every process reading this transaction holds it at once and a writer
            // fails to take it exclusively for as long as any of them does
            file.lock_shared().map_err(unsupported_lock)?;
            Some(file)
        };
        // Assigned rather than taken in two steps, so the new lock is held before the old one is
        // dropped: a writer scanning in between sees this process at its older, more conservative
        // id rather than not at all
        self.pinned = held;
        self.published = pinned;
        Ok(())
    }

    /// The oldest transaction any process still needs, or `None` if nothing is pinned anywhere.
    ///
    /// Walks `txn/` from the lowest id up, deleting every file it can take exclusively -- those
    /// name transactions nobody is reading any more -- and stopping at the first it cannot, which
    /// is therefore the oldest that is still in use. The caller must hold the registry lock
    /// exclusively, so that no process is part way through pinning.
    pub(super) fn oldest_pinned(&mut self) -> Result<Option<u64>> {
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(self.dir.pinned_dir()).map_err(StorageError::Io)? {
            let entry = entry.map_err(StorageError::Io)?;
            // The name is the whole of the data, so anything that does not parse as one is not
            // ours and is left alone
            if let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u64>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();

        for id in ids {
            // Our own pin, which the caller folds in from its own tracker anyway. Skipped rather
            // than locked: taking the lock again through this process would succeed, since the
            // lock we already hold is ours, and we would delete a file we are still using
            if id == self.published {
                return Ok(Some(id));
            }
            let path = self.dir.pinned_file(id);
            let file = match File::open(&path) {
                Ok(file) => file,
                // Cleaned up by someone else between the listing and here
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::Io(err)),
            };
            match file.try_lock() {
                // Nobody needs this transaction any more, so the file is litter. Unlinked while
                // the lock is held, so that a reader cannot open it in between and end up holding
                // an inode nothing points at
                Ok(()) => {
                    let removed = std::fs::remove_file(&path);
                    let _ = file.unlock();
                    match removed {
                        Ok(()) => {}
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(StorageError::Io(err)),
                    }
                }
                // A live reader holds it, and it is the lowest, so it is the horizon
                Err(TryLockError::WouldBlock) => return Ok(Some(id)),
                Err(err) => return Err(lock_error(err)),
            }
        }

        Ok(None)
    }
}

/// `write.lock`: held exclusively by the process which owns the single logical writer.
pub(crate) struct WriteLock {
    file: File,
    held: bool,
}

impl WriteLock {
    pub(super) fn open(dir: &DatabaseDir) -> Result<Self> {
        // Before opening, for the same reason as the marker and the database file: `create(true)`
        // would otherwise follow a symlink left under this name and make its target, and the
        // directory lock would be taken on a file whose identity was never checked
        require_regular_file(&dir.write_lock_file())?;
        Ok(Self {
            file: open_or_create(&dir.write_lock_file()).map_err(StorageError::Io)?,
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

// Not under WASI, which has no file locking: these all take one, and the module itself is
// compiled there because the coordinator is woven into the transaction tracker
#[cfg(all(test, not(target_os = "wasi")))]
mod test {
    use super::*;

    /// Everything a caller does to the directory itself when opening, in order. The write lock is
    /// not taken here: it belongs to the coordinator rather than to the directory, and the tests
    /// that are about it take it themselves.
    fn open(dir: &DatabaseDir, create: bool) -> Result<(File, DataLocation), DatabaseError> {
        dir.prepare(create)?;
        dir.open_data(create)
    }

    /// The whole create sequence, which is split between here and the caller: the database file is
    /// moved into place and the marker written only once [`crate::Database`] has accepted the file.
    /// These tests are about the lock files and the marker rather than the database, so they stand
    /// in for that middle step by doing nothing -- the file they promote is simply empty.
    fn create(dir: &DatabaseDir) -> File {
        dir.prepare(true).unwrap();
        let mut lock = WriteLock::open(dir).unwrap();
        assert!(lock.try_acquire().unwrap());
        let (file, location) = dir.open_data(true).unwrap();
        dir.promote_data(location).unwrap();
        dir.write_metadata().unwrap();
        file
    }

    /// A registry with `pinned` published, standing in for another process holding that
    /// transaction: the lock belongs to the open file rather than to the process, so a second
    /// handle here is excluded exactly as a second process would be.
    fn pin(dir: &DatabaseDir, pinned: u64) -> Registry {
        let mut registry = Registry::open(dir).unwrap();
        registry.publish_pinned(pinned).unwrap();
        registry
    }

    #[test]
    fn the_write_lock_excludes_other_handles() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));

        let mut first = WriteLock::open(&dir).unwrap();
        assert!(first.try_acquire().unwrap());

        let mut second = WriteLock::open(&dir).unwrap();
        assert!(!second.try_acquire().unwrap());

        first.release();
        assert!(second.try_acquire().unwrap());
    }

    /// The database file itself is left unlocked: every process using the database has it open at
    /// once, and `write.lock` and the files in `txn/` are what keep a page from being reused
    /// underneath one of them.
    #[test]
    fn the_database_file_is_not_locked() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        let first = create(&dir);

        let second = open(&dir, false).unwrap().0;
        drop(second);
        drop(first);
    }

    #[test]
    fn a_pinned_transaction_holds_the_horizon() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        dir.init_registry(WriterMode::MultiWriterProcess).unwrap();

        let mut reader = pin(&dir, 5);
        let mut writer = Registry::open(&dir).unwrap();
        assert_eq!(Some(5), writer.oldest_pinned().unwrap());

        // Letting go of it is what releases the horizon, and the file goes with it
        reader.publish_pinned(UNPINNED).unwrap();
        assert_eq!(None, writer.oldest_pinned().unwrap());
        assert!(!dir.pinned_file(5).exists());
    }

    /// A process that pins its own transaction and then scans keeps its own file: it is still
    /// reading that transaction, and the lock it holds is one this process would be able to take
    /// again.
    #[test]
    fn a_scan_does_not_clean_up_its_own_pin() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        dir.init_registry(WriterMode::MultiWriterProcess).unwrap();

        let mut registry = pin(&dir, 5);
        assert_eq!(Some(5), registry.oldest_pinned().unwrap());
        assert!(dir.pinned_file(5).is_file());
    }

    /// The whole point of naming the files after transactions and locking them: a process that
    /// dies leaves its file behind, and the next writer to walk the directory takes it exclusively
    /// -- which nobody is preventing any more -- and unlinks it.
    #[test]
    fn a_pin_left_by_a_dead_process_is_cleaned_up() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        dir.init_registry(WriterMode::MultiWriterProcess).unwrap();

        // What a process that exited without unlinking leaves: the file, and no lock on it
        std::fs::write(dir.pinned_file(7), []).unwrap();

        let mut writer = Registry::open(&dir).unwrap();
        assert_eq!(None, writer.oldest_pinned().unwrap());
        assert!(!dir.pinned_file(7).exists());
    }

    /// The scan stops at the first file it cannot take, so a live pin hides the ones above it. They
    /// cost nothing: a later scan reaches them once this one is let go.
    #[test]
    fn the_scan_stops_at_the_oldest_live_pin() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        dir.init_registry(WriterMode::MultiWriterProcess).unwrap();

        std::fs::write(dir.pinned_file(3), []).unwrap();
        let _live = pin(&dir, 9);
        std::fs::write(dir.pinned_file(11), []).unwrap();

        let mut writer = Registry::open(&dir).unwrap();
        assert_eq!(Some(9), writer.oldest_pinned().unwrap());
        assert!(!dir.pinned_file(3).exists());
        assert!(dir.pinned_file(11).is_file());
    }

    /// The name is the whole of the data, so a name that is not a transaction id is not one of
    /// these files and is left alone rather than being deleted or read as an id.
    #[test]
    fn a_name_that_is_not_a_transaction_id_is_ignored() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        dir.init_registry(WriterMode::MultiWriterProcess).unwrap();

        let foreign = dir.pinned_dir().join("notanid");
        std::fs::write(&foreign, b"someone else's").unwrap();

        let mut writer = Registry::open(&dir).unwrap();
        assert_eq!(None, writer.oldest_pinned().unwrap());
        assert!(foreign.is_file());
    }

    /// Moving a pin forward takes the new lock before dropping the old one, so a writer scanning
    /// in between sees the older id rather than nothing.
    #[test]
    fn moving_a_pin_forward_releases_the_old_one() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        dir.init_registry(WriterMode::MultiWriterProcess).unwrap();

        let mut reader = pin(&dir, 4);
        reader.publish_pinned(6).unwrap();

        let mut writer = Registry::open(&dir).unwrap();
        assert_eq!(Some(6), writer.oldest_pinned().unwrap());
        assert!(!dir.pinned_file(4).exists());
    }

    #[test]
    fn a_directory_without_the_marker_is_not_a_database() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        std::fs::create_dir(&path).unwrap();
        // A lock file on its own is not enough -- the marker is what says the directory is one of
        // these, so it has to be what this turns on rather than the lock file's absence
        std::fs::write(path.join(WRITE_LOCK_FILE_NAME), []).unwrap();
        let dir = DatabaseDir::new(&path);
        assert!(open(&dir, false).is_err());

        // An empty file where the marker should be is rejected too, rather than being read as a
        // database with a zero version
        std::fs::write(path.join(METADATA_FILE_NAME), []).unwrap();
        assert!(open(&dir, false).is_err());
    }

    /// A `create()` that died while writing the marker leaves the partial copy under the temporary
    /// name and no marker at all, which the next `create()` finishes rather than tripping over.
    #[test]
    fn a_marker_that_never_landed_is_written_again() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));

        std::fs::remove_file(path.join(METADATA_FILE_NAME)).unwrap();
        std::fs::write(path.join(METADATA_TMP_FILE_NAME), &MAGIC[0..4]).unwrap();

        drop(create(&dir));
        assert_eq!(
            METADATA_LEN,
            std::fs::read(path.join(METADATA_FILE_NAME)).unwrap().len()
        );
    }

    /// The flip side of recovering from an interrupted create: because the marker is never left
    /// half-written, a `metadata` file that is not a marker belongs to something else, and
    /// `create()` must refuse rather than overwrite it.
    #[test]
    fn a_directory_belonging_to_something_else_is_left_alone() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join(METADATA_FILE_NAME), b"someone else's").unwrap();

        assert!(open(&DatabaseDir::new(&path), true).is_err());
        assert_eq!(
            b"someone else's",
            &std::fs::read(path.join(METADATA_FILE_NAME)).unwrap()[..]
        );
        // ... and refused before anything of redb's was put in it
        assert!(!path.join(WRITE_LOCK_FILE_NAME).exists());
    }

    /// A `metadata` file that starts with the marker but keeps going is not a marker. The read is
    /// bounded to the marker's length plus one byte, so a directory whose `metadata` is enormous
    /// costs nothing to reject.
    #[test]
    fn an_oversized_marker_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));

        let mut contents = std::fs::read(path.join(METADATA_FILE_NAME)).unwrap();
        assert_eq!(METADATA_LEN, contents.len());
        contents.extend_from_slice(&[0u8; 4096]);
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();

        assert!(open(&dir, false).is_err());
        assert!(open(&dir, true).is_err());
    }

    /// A marker has to be an ordinary file, not a symlink pointing at one somewhere else -- which
    /// would otherwise vouch for a directory holding nothing of redb's, since `File::open` follows
    /// the link. The same check keeps the open away from a FIFO, which would block rather than
    /// fail.
    #[cfg(unix)]
    #[test]
    fn a_marker_that_is_a_symlink_is_not_a_marker() {
        let tmpdir = tempfile::tempdir().unwrap();
        let real = tmpdir.path().join("real");
        let dir = DatabaseDir::new(&real);
        drop(create(&dir));

        let borrowed = tmpdir.path().join("borrowed");
        std::fs::create_dir(&borrowed).unwrap();
        std::fs::write(borrowed.join(WRITE_LOCK_FILE_NAME), []).unwrap();
        std::os::unix::fs::symlink(
            real.join(METADATA_FILE_NAME),
            borrowed.join(METADATA_FILE_NAME),
        )
        .unwrap();

        let borrowed = DatabaseDir::new(&borrowed);
        assert!(open(&borrowed, false).is_err());
        assert!(open(&borrowed, true).is_err());
    }

    /// A database file with nothing in it never gets initialized under its own name: doing so
    /// would put the header bytes there, and a crash before the magic number was written would
    /// leave a file that has to be refused forever. It is discarded and redone under the temporary
    /// name like any other unfinished attempt.
    #[test]
    fn an_empty_data_file_is_redone_through_the_temporary_name() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));
        std::fs::write(path.join(DATA_FILE_NAME), []).unwrap();

        let (data, location) = open(&dir, true).unwrap();
        assert!(!path.join(DATA_FILE_NAME).exists());
        assert!(path.join(DATA_TMP_FILE_NAME).is_file());

        // ... and promoting puts it back under the name it belongs under
        dir.promote_data(location).unwrap();
        assert!(path.join(DATA_FILE_NAME).is_file());
        assert!(!path.join(DATA_TMP_FILE_NAME).exists());
        drop(data);
    }

    /// An empty database file is only wreckage if nobody is holding it. One that a process
    /// reached past the directory to create and lock is live, and unlinking it would leave that
    /// process writing to an inode nothing points at.
    #[test]
    fn an_empty_data_file_someone_is_holding_is_not_discarded() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));
        std::fs::write(path.join(DATA_FILE_NAME), []).unwrap();

        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(DATA_FILE_NAME))
            .unwrap();
        held.try_lock().unwrap();

        assert!(matches!(
            open(&dir, true),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(path.join(DATA_FILE_NAME).is_file());

        // ... and once it is let go, the empty file is wreckage again
        held.unlock().unwrap();
        drop(held);
        drop(open(&dir, true).unwrap().0);
    }

    #[test]
    fn a_marker_from_a_later_version_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));

        let mut contents = [0u8; METADATA_LEN];
        contents[0..8].copy_from_slice(&MAGIC);
        contents[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();
        assert!(open(&dir, false).is_err());
    }
}
