//! The files that make up a multi-process database directory, and the lock that excludes other
//! processes from it.
//!
//! Everything in here uses only `std::fs` file operations and the advisory file locks exposed by
//! `std::fs::File`. See `docs/design.md` for the protocol these files implement.

use crate::tree_store::file_backend::{FileBackend, FileLockKind};
use crate::tree_store::{DB_HEADER_SIZE, MAGICNUMBER, xxh3_checksum};
use crate::{DatabaseError, Result, StorageBackend, StorageError};
use alloc::vec::Vec;
use std::ffi::OsStr;
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
const EXTENDED_HEADER_FILE_NAME: &str = "extended-header";
const PINNED_DIR_NAME: &str = "txn";

// The ASCII letters 'redbMP' followed by the same PNG-inspired tail as the database file's own
// magic number, for the same reasons.
const MAGIC: [u8; 11] = [
    b'r', b'e', b'd', b'b', b'M', b'P', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A,
];
const FORMAT_VERSION: u8 = 1;
/// A single process owns `write.lock` for as long as it has the database open.
const WRITER_MODE_SINGLE: u8 = 1;
/// `write.lock` is taken per write transaction, so any process may write.
const WRITER_MODE_MULTI: u8 = 2;
const METADATA_LEN: usize = 13;

/// A multi-process database cannot be safe without file locks, so unlike [`crate::Database`] it
/// refuses to open on a platform that lacks them.
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

/// Flushes a directory itself, so that an entry a rename just created is durable. Best-effort
/// about a directory this process cannot open: flushing needs read permission, and nothing else
/// here does.
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

/// `std` exposes no directory handle to sync off Unix, and no way to make a rename write-through.
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

/// The deepest ancestor of `path` that exists, canonicalized, before anything is created.
fn deepest_existing_ancestor(path: &Path) -> Option<PathBuf> {
    let mut current = parent_of(path);
    loop {
        if let Ok(real) = std::fs::canonicalize(&current) {
            return Some(real);
        }
        let next = parent_of(&current);
        if next == current {
            return None;
        }
        current = next;
    }
}

/// Flushes the directory entries from the database directory's parent up to and including
/// `preexisting`, so that a crash cannot drop a freshly made ancestor while keeping the synced
/// entries below it.
fn sync_ancestors(root: &Path, preexisting: Option<&Path>) -> Result<(), DatabaseError> {
    // Canonicalized first: `Path::parent` is purely lexical, so for a path ending in `..` it names
    // a child of the real directory rather than its parent, and the fsync would flush the wrong one
    let root = std::fs::canonicalize(root).map_err(StorageError::Io)?;
    let mut current = parent_of(&root);
    loop {
        sync_dir(&current)?;
        if Some(current.as_path()) == preexisting {
            return Ok(());
        }
        let next = parent_of(&current);
        if next == current {
            return Ok(());
        }
        current = next;
    }
}

/// What sits under a name, without following a symlink -- `None` only for a provable absence.
/// Any error other than `NotFound` is reported rather than read as absence, which the accepting
/// paths would then trust.
fn symlink_metadata_if_any(path: &Path) -> Result<Option<std::fs::Metadata>, StorageError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Whether a name is taken, by anything at all -- including a symlink that resolves to nothing,
/// which `Path::exists` reports as absent because it follows the link.
fn occupied(path: &Path) -> Result<bool, DatabaseError> {
    Ok(symlink_metadata_if_any(path)?.is_some())
}

/// Marks a lock file a `create()` made and then could not stand behind. It cannot be unlinked --
/// nothing ever unlinks one -- but left empty it would vouch, on the next `create()`, for the very
/// files that were just refused; written to and flushed, it fails the empty-lock-file rule
/// instead, and the directory stays refused.
fn mark_lock_abandoned(lock: &File) {
    let _ = (&*lock).write_all(b"abandoned by a rejected create\n");
    let _ = lock.sync_all();
}

/// Refuses anything that is not an ordinary file under one of this database's own names: opens
/// here traverse symlinks and would write through them, and a FIFO blocks rather than fails. A
/// missing file is fine. Best-effort -- the entry can be replaced between this check and the open,
/// and `std` does not expose `O_NOFOLLOW` portably.
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

/// Refuses a path naming the database file inside a multi-process database directory.
///
/// A multi-process database keeps its data in an ordinary redb file with *no* lock on it, so
/// nothing about the file itself stops [`crate::Database`] from opening it and writing underneath
/// the processes using it properly; the marker beside it is what says to keep away.
pub(crate) fn reject_multiprocess_data_file(path: &Path) -> Result<(), DatabaseError> {
    if path.file_name() != Some(OsStr::new(DATA_FILE_NAME)) || !marker_present(&parent_of(path)) {
        return Ok(());
    }
    Err(StorageError::Io(io::Error::new(
        ErrorKind::InvalidInput,
        "this file belongs to a multi-process database; open the directory holding it rather than \
         the file itself",
    ))
    .into())
}

/// Whether a directory carries a complete multi-process marker. Anything that is not one --
/// absent, unreadable, the wrong size, a symlink, someone else's file under that name -- answers
/// no: only a marker this database wrote vouches for the directory.
fn marker_present(root: &Path) -> bool {
    let path = root.join(METADATA_FILE_NAME);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() && metadata.len() == METADATA_LEN as u64 => {}
        _ => return false,
    }
    std::fs::read(&path).is_ok_and(|contents| contents.starts_with(&MAGIC))
}

/// Which name [`DatabaseDir::open_data`] put the database file under. Carried from the open to
/// [`DatabaseDir::promote_data`] rather than worked out again from disk: a temporary file is this
/// call's own only if this call made it, and nothing about the file itself says so.
#[derive(Clone, Copy)]
pub(super) enum DataLocation {
    /// `data.redb`, which is where a database that was already finished lives.
    Final,
    /// `data.redb.tmp`, which is where one being initialized lives until it is renamed into place.
    Temporary,
}

/// Which processes may write to a multi-process database.
///
/// Fixed when the database is created and recorded in the directory's `metadata` file, so that
/// every process which opens it agrees on the protocol.
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
    fn to_byte(self) -> u8 {
        match self {
            WriterMode::SingleWriterProcess => WRITER_MODE_SINGLE,
            WriterMode::MultiWriterProcess => WRITER_MODE_MULTI,
        }
    }

    fn from_byte(value: u8) -> Option<Self> {
        match value {
            WRITER_MODE_SINGLE => Some(WriterMode::SingleWriterProcess),
            WRITER_MODE_MULTI => Some(WriterMode::MultiWriterProcess),
            _ => None,
        }
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
    // Seek-and-read rather than the platform positional APIs, to stay portable; the caller holds a
    // lock that makes the cursor single threaded
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
        EXTENDED_HEADER_FILE_NAME,
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

    fn extended_header_file(&self) -> PathBuf {
        self.root.join(EXTENDED_HEADER_FILE_NAME)
    }

    fn pinned_dir(&self) -> PathBuf {
        self.root.join(PINNED_DIR_NAME)
    }

    /// The file whose *name* is `id`. Nothing is ever read from or written to it: the name carries
    /// the whole of the data, and the lock on it says whether anyone still needs that transaction.
    fn pinned_file(&self, id: u64) -> PathBuf {
        self.pinned_dir().join(id.to_string())
    }

    /// Creates the files the protocol coordinates through: the `txn/` directory, the empty
    /// `registry.lock`, and `extended-header`. Idempotent, and safe without the write lock:
    /// everything here is create-if-absent, and the extended header's initial zeroes read as
    /// corrupt, which every reader of it handles.
    pub(super) fn init_protocol_files(&self) -> Result<()> {
        std::fs::create_dir_all(self.pinned_dir()).map_err(StorageError::Io)?;
        require_regular_file(&self.registry_file())?;
        drop(open_or_create(&self.registry_file()).map_err(StorageError::Io)?);
        require_regular_file(&self.extended_header_file())?;
        drop(open_or_create(&self.extended_header_file()).map_err(StorageError::Io)?);
        sync_dir(&self.root)?;

        Ok(())
    }

    /// The mode an existing database was created with, from its `metadata` file.
    ///
    /// Safe to read without any lock: the marker is written once, by rename, and never changed.
    pub(super) fn mode(&self) -> Result<WriterMode, DatabaseError> {
        self.read_metadata(false).map(|mode| {
            mode.expect("read_metadata only reports a missing marker when create is set")
        })
    }

    /// The accepting pass for a directory whose lock file this call has just created. A
    /// pre-existing `write.lock` vouches for redb's other names; a fresh one vouches for nothing,
    /// so anything already under them arrived while the directory was being claimed -- another
    /// process reaching past the directory API -- and adopting it would hand this handle a file
    /// something else is using.
    pub(super) fn reject_files_beside_a_fresh_lock(&self) -> Result<(), DatabaseError> {
        let claimed = Self::WRITTEN_UNDER_THE_LOCK.iter().copied();
        for name in claimed.chain([METADATA_FILE_NAME]) {
            if occupied(&self.root.join(name))? {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "another process put files into the directory while it was being claimed",
                ))
                .into());
            }
        }

        Ok(())
    }

    /// Takes the shared lock on `metadata` that every process holds for as long as it has the
    /// database open. A future version that changes the directory's format will take this lock
    /// exclusively, so that it upgrades only once nothing is using the database.
    pub(super) fn lock_metadata_shared(&self) -> Result<File> {
        require_regular_file(&self.metadata_file())?;
        let file = File::open(self.metadata_file()).map_err(StorageError::Io)?;
        file.lock_shared().map_err(unsupported_lock)?;
        Ok(file)
    }

    /// Validates the directory and creates it if `create` is set, up to but not including taking
    /// the write lock. Split from [`Self::open_data`] because the caller decides how the write
    /// lock is taken, and this is everything that happens before it.
    ///
    /// Returns whether the marker was present during this pass, which only a `create` asks about:
    /// a marker that was already here before the caller made a lock file is the one thing a fresh
    /// lock does not have to vouch for.
    pub(super) fn prepare(&self, create: bool) -> Result<bool, DatabaseError> {
        let mut marked_at_preflight = false;
        if create {
            // All checks run before the directory is touched, so a rejected create() leaves no
            // trace. A marker arrives by rename and so is either absent or complete, which is what
            // makes refusing without the lock sound; the checks run again under the lock, which is
            // the authoritative pass
            marked_at_preflight = occupied(&self.metadata_file())?;
            if marked_at_preflight {
                let _ = self.read_metadata(create)?;
            } else {
                self.reject_unmarked_database()?;
                self.reject_foreign_directory()?;
            }
            // The deepest ancestor that already exists bounds what create_dir_all makes; every
            // entry from the database directory up to there needs its own flush below
            let preexisting = deepest_existing_ancestor(&self.root);
            std::fs::create_dir_all(&self.root).map_err(StorageError::Io)?;
            // Syncing entries inside a directory whose own entry was never flushed loses the lot.
            // On every create(): finding the directory already there says nothing about whether
            // anyone flushed it, and the lock lives inside it, so it cannot cover this window --
            // and a path of freshly made ancestors needs each one flushed, bottom-up, or a crash
            // could drop the whole subtree by losing one entry higher up
            sync_ancestors(&self.root, preexisting.as_deref())?;
        } else if !self.root.is_dir() {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::NotFound,
                "no such multi-process database directory",
            ))
            .into());
        }

        Ok(marked_at_preflight)
    }

    /// Opens the database file, once the caller holds the write lock.
    ///
    /// The file is returned unlocked: every process needs it open at once, and `write.lock` and
    /// the reader slots are what keep a page from being reused underneath one of them.
    ///
    /// `fresh_claim` says the caller created the lock file and found no marker, so the names
    /// touched here were just required to be absent; anything under them now arrived while the
    /// directory was being claimed, and is refused rather than adopted.
    pub(super) fn open_data(
        &self,
        create: bool,
        fresh_claim: bool,
    ) -> Result<(File, DataLocation), DatabaseError> {
        if create {
            // The lock file's entry must be durable before anything else is written under these
            // names: its *absence* beside them is what says a directory is not this database's,
            // and a crash could otherwise keep `data.redb` while losing `write.lock`
            sync_dir(&self.root)?;
        }
        let _ = self.read_metadata(create)?;

        // A database being made for the first time is initialized under a temporary name and
        // renamed into place by `promote_data`, like the marker. A crash during initialization
        // leaves a file that is neither empty nor a database; under its own name that would be
        // indistinguishable from a mistyped path, while under the temporary name it is unfinished
        // work of ours, to be redone
        let (path, location) = if create {
            // `symlink_metadata` rather than `exists()`: the question is whether the name is
            // occupied at all, and `exists()` reports a dangling symlink as absent
            match std::fs::symlink_metadata(self.data_file()) {
                // The fresh-lock pass just required this name to be absent, so anything under it
                // now arrived while the directory was being claimed, and is refused the same way
                Ok(_) if fresh_claim => {
                    return Err(StorageError::Io(io::Error::new(
                        ErrorKind::AlreadyExists,
                        "the database file appeared while the directory was being claimed",
                    ))
                    .into());
                }
                Ok(metadata) if metadata.is_file() && metadata.len() == 0 => {
                    // Empty means an interrupted attempt -- but only if nobody is holding it. A
                    // process that reached past the directory could have created and locked it,
                    // and unlinking would leave it writing to an inode nothing points at
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
                    // Re-read through the held lock: a process that reached past the directory
                    // could have initialized the file between the check above and the lock
                    // landing, and unlinking would then delete the database it just made
                    if existing.metadata().map_err(StorageError::Io)?.len() != 0 {
                        drop(existing);
                        (self.data_file(), DataLocation::Final)
                    } else {
                        // The lock is held across the unlink, which is what closes the window
                        std::fs::remove_file(self.data_file()).map_err(StorageError::Io)?;
                        drop(existing);
                        self.reject_data_tmp_holding_a_database()?;
                        self.discard_data_tmp()?;
                        (self.data_tmp_file(), DataLocation::Temporary)
                    }
                }
                Ok(_) => (self.data_file(), DataLocation::Final),
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    self.reject_data_tmp_holding_a_database()?;
                    self.discard_data_tmp()?;
                    (self.data_tmp_file(), DataLocation::Temporary)
                }
                Err(err) => return Err(StorageError::Io(err).into()),
            }
        } else {
            (self.data_file(), DataLocation::Final)
        };

        require_regular_file(&path)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        match location {
            // The temporary was just cleared, so a file under that name now was made by
            // something reaching past the directory in between, and must not be adopted
            DataLocation::Temporary => options.create_new(true),
            DataLocation::Final => options.create(create),
        };
        let data = match options.open(path) {
            Ok(data) => data,
            Err(err) if fresh_claim && err.kind() == ErrorKind::AlreadyExists => {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "the database file appeared while the directory was being claimed",
                ))
                .into());
            }
            Err(err) => return Err(StorageError::Io(err).into()),
        };

        Ok((data, location))
    }

    /// Moves a freshly initialized database file into place, if this call was the one that made
    /// it. Called once [`crate::Database`] has accepted the file, so that `data.redb` exists only
    /// when it holds a finished database. The write lock is still held, so nothing can be looking
    /// at either name.
    pub(super) fn promote_data(&self, location: DataLocation) -> Result<(), DatabaseError> {
        let tmp = self.data_tmp_file();
        if matches!(location, DataLocation::Final) {
            // Anything under the temporary name is the wreckage of an earlier attempt. Deleted
            // here rather than on the way in, so a create() pointed somewhere by mistake fails
            // without having deleted anything; tidying only, so a failure is not the caller's
            // problem. Except a finished database, which is never tidied away
            if matches!(self.reject_data_tmp_holding_a_database(), Ok(())) {
                drop(self.discard_data_tmp());
            }
            return Ok(());
        }
        // A database file appearing since the open means something reached past the directory and
        // made it, and renaming over it would destroy a database this call never opened
        if occupied(&self.data_file())? {
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

    /// Removes the temporary this call created, once initializing it has failed. Left behind, an
    /// initialization that failed after the magic number was written would read, to the next
    /// `create()`, as a finished database whose promoting rename was lost, and the refusal that
    /// protects that state would wedge the directory. Only a crash leaves the temporary now,
    /// where nothing could have cleaned up.
    ///
    /// Best-effort -- the initialization error is the story, and what a failed removal leaves is
    /// what a crash at the same point would have -- but still only under the file's own lock: the
    /// write lock went with the backend, so another `create()` may already be initializing a
    /// fresh temporary here, and unlinking that would pull it out from underneath it.
    pub(super) fn discard_failed_data(&self, location: DataLocation) {
        if !matches!(location, DataLocation::Temporary) {
            return;
        }
        let Ok(Some(metadata)) = symlink_metadata_if_any(&self.data_tmp_file()) else {
            return;
        };
        if !metadata.is_file() {
            return;
        }
        let Ok(held) = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.data_tmp_file())
        else {
            return;
        };
        if matches!(held.try_lock(), Ok(())) {
            let _ = std::fs::remove_file(self.data_tmp_file());
        }
    }

    /// Throws away a temporary database file left by an earlier attempt. Unlinks rather than
    /// truncates, so a symlink under this name is removed rather than written through -- without
    /// opening it first, since a link or FIFO is a name rather than a file to hold.
    ///
    /// A regular file is unlinked only under its own lock, exactly like an empty `data.redb`: a
    /// process that reached past the directory could be initializing a database under this name
    /// right now, and unlinking would leave it writing to an inode nothing points at.
    fn discard_data_tmp(&self) -> Result<(), DatabaseError> {
        match std::fs::symlink_metadata(self.data_tmp_file()) {
            Ok(metadata) if !metadata.is_file() => {
                return match std::fs::remove_file(self.data_tmp_file()) {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                    Err(err) => Err(StorageError::Io(err).into()),
                };
            }
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(StorageError::Io(err).into()),
        }
        let held = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.data_tmp_file())
        {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        match held.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) => return Err(lock_unsupported(err)),
        }
        // Re-read through the held lock: a process that reached past the directory could have
        // finished a database under this name between the inspection and the lock landing, and
        // its magic number is written last, so one present now marks a database rather than
        // wreckage
        let mut magic = Vec::with_capacity(MAGICNUMBER.len());
        (&held)
            .take(MAGICNUMBER.len() as u64)
            .read_to_end(&mut magic)
            .map_err(StorageError::Io)?;
        if magic == MAGICNUMBER {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                "the database is under the temporary name data.redb.tmp; rename it to data.redb \
                 to recover it",
            ))
            .into());
        }
        // The lock is held across the unlink, which is what closes the window
        std::fs::remove_file(self.data_tmp_file()).map_err(StorageError::Io)?;
        drop(held);

        Ok(())
    }

    /// Refuses to treat the temporary as wreckage when it holds a finished database. redb writes
    /// a new database's magic number last, so a temporary bearing it is a database whose
    /// promoting rename was lost -- possible wherever the directory could not be flushed -- and
    /// discarding it would lose the data. Refused for a person to sort out instead of guessed at:
    /// promoting it automatically would mean deciding it is newer than anything else here.
    fn reject_data_tmp_holding_a_database(&self) -> Result<(), DatabaseError> {
        // Only a regular file can be a database; anything else is a name for the discard to
        // unlink, and opening it here could block -- a FIFO waits for a writer
        match std::fs::symlink_metadata(self.data_tmp_file()) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => return Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(StorageError::Io(err).into()),
        }
        let file = match File::open(self.data_tmp_file()) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        let mut magic = Vec::with_capacity(MAGICNUMBER.len());
        file.take(MAGICNUMBER.len() as u64)
            .read_to_end(&mut magic)
            .map_err(StorageError::Io)?;
        if magic == MAGICNUMBER {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                "the database is under the temporary name data.redb.tmp; rename it to data.redb \
                 to recover it",
            ))
            .into());
        }

        Ok(())
    }

    /// Refuses a directory holding files under these names that this type did not put there.
    ///
    /// An unmarked directory is accepted only as an interrupted create, and `write.lock` is the
    /// first thing `create()` makes, so every other name implies a lock file beside it. One
    /// without means something else filled the directory in -- most likely a plain
    /// [`crate::Database`] under `data.redb`, which `create()` would otherwise adopt.
    fn reject_unmarked_database(&self) -> Result<(), DatabaseError> {
        if occupied(&self.write_lock_file())? {
            // The lock file vouches for the other names only while it could be redb's own:
            // created empty and never written to, so one with contents was put there by something
            // else. The marker's temporary is bounded the same way -- written fresh, always a
            // regular file, holding at most the marker's length -- so anything else under that
            // name cannot be an interrupted create's, and finishing the create would delete it,
            // or write the marker through whatever a planted link points at
            let lock = symlink_metadata_if_any(&self.write_lock_file())?;
            let tmp = symlink_metadata_if_any(&self.metadata_tmp_file())?;
            if lock.is_some_and(|metadata| !metadata.is_file() || metadata.len() > 0)
                || tmp.is_some_and(|metadata| {
                    !metadata.is_file() || metadata.len() > METADATA_LEN as u64
                })
            {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "refusing to take over a directory that already holds files under the names a \
                     multi-process database uses",
                ))
                .into());
            }
            return Ok(());
        }

        for name in Self::WRITTEN_UNDER_THE_LOCK {
            if occupied(&self.root.join(name))? {
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

    /// Writes the marker that says this directory holds a multi-process database. Called only once
    /// the database file has been initialized, so a failed `create()` never leaves a marker.
    ///
    /// Written under a temporary name and renamed into place, so the marker is either absent or
    /// complete and never in between: a half-written one is indistinguishable from a file that is
    /// simply not ours, which [`Self::read_metadata`] refuses, and the directory would be wedged.
    fn write_metadata(&self, mode: WriterMode) -> Result<(), DatabaseError> {
        let mut contents = [0u8; METADATA_LEN];
        contents[0..11].copy_from_slice(&MAGIC);
        contents[11] = FORMAT_VERSION;
        contents[12] = mode.to_byte();

        let tmp = self.metadata_tmp_file();
        // Unlinked and created afresh rather than truncated in place, so a symlink or FIFO left
        // under this name is never opened -- and a regular file only under its own lock, exactly
        // like the database file's temporary: a process that reached past the directory could be
        // holding a live file under this name, and unlinking would leave it writing to an inode
        // nothing points at
        match symlink_metadata_if_any(&tmp)? {
            None => {}
            Some(metadata) if !metadata.is_file() => match std::fs::remove_file(&tmp) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(StorageError::Io(err).into()),
            },
            Some(_) => {
                let held = match OpenOptions::new().read(true).write(true).open(&tmp) {
                    Ok(file) => file,
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        return self.write_metadata_tmp_and_publish(&contents);
                    }
                    Err(err) => return Err(StorageError::Io(err).into()),
                };
                match held.try_lock() {
                    Ok(()) => {}
                    Err(TryLockError::WouldBlock) => {
                        return Err(DatabaseError::DatabaseAlreadyOpen);
                    }
                    Err(TryLockError::Error(err)) => return Err(lock_unsupported(err)),
                }
                // Re-read through the held lock: redb's own temporary never exceeds the marker's
                // length, so a bigger file was finished under this name by something that reached
                // past the directory -- most likely a whole database -- and is refused rather
                // than deleted
                if held.metadata().map_err(StorageError::Io)?.len() > METADATA_LEN as u64 {
                    return Err(StorageError::Io(io::Error::new(
                        ErrorKind::AlreadyExists,
                        "the marker's temporary appeared while the directory was being claimed",
                    ))
                    .into());
                }
                // The lock is held across the unlink, which is what closes the window
                match std::fs::remove_file(&tmp) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(StorageError::Io(err).into()),
                }
                drop(held);
            }
        }
        self.write_metadata_tmp_and_publish(&contents)
    }

    /// Writes the marker's bytes under the temporary name and renames them into place, once
    /// [`Self::write_metadata`] has cleared the name.
    fn write_metadata_tmp_and_publish(&self, contents: &[u8]) -> Result<(), DatabaseError> {
        let tmp = self.metadata_tmp_file();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(StorageError::Io)?;
        file.write_all(contents).map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        // Renaming would replace whatever sits under the final name, and the caller only checked
        // it some statements ago; refused like the database file's own promotion, with the same
        // residual -- a no-replace rename is not portably available
        if occupied(&self.metadata_file())? {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::AlreadyExists,
                "the marker appeared while the directory was being claimed",
            ))
            .into());
        }
        std::fs::rename(&tmp, self.metadata_file()).map_err(StorageError::Io)?;
        sync_dir(&self.root)?;

        Ok(())
    }

    /// Writes the marker only if the directory does not already carry one. One that is there was
    /// validated by [`Self::read_metadata`] under the lock, so it is byte-for-byte what would be
    /// written.
    pub(super) fn write_metadata_if_missing(&self, mode: WriterMode) -> Result<(), DatabaseError> {
        if occupied(&self.metadata_file())? {
            // Validated here rather than assumed, and required to carry the mode being created:
            // the claim that an existing marker is byte-for-byte what would be written has to
            // survive whatever else was pointed at the directory, and one that validates but
            // differs can only have arrived from outside while the directory was being claimed
            let existing = self
                .read_metadata(false)?
                .expect("read_metadata only reports a missing marker when create is set");
            if existing != mode {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "the marker appeared while the directory was being claimed",
                ))
                .into());
            }
            // This call may still have created other files, and write_metadata() is where the
            // directory would otherwise be flushed
            sync_dir(&self.root)?;
            return Ok(());
        }

        self.write_metadata(mode)
    }

    /// Refuses a directory that holds anything this database did not put there.
    ///
    /// Only consulted when there is no marker: a directory with one is this database's, and a
    /// stray file appearing next to it must not stop it opening. Without a marker, the only
    /// directory `create()` may accept is an interrupted create of its own, which holds nothing
    /// but the files `create()` itself makes.
    fn reject_foreign_directory(&self) -> Result<(), DatabaseError> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            // Listing needs read permission, which nothing else here does. This rule guards
            // against mistyped paths rather than being a safety property, and the refusals that
            // are -- a foreign marker, a non-regular file, a `data.redb` with no `write.lock` --
            // need no listing
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
                    EXTENDED_HEADER_FILE_NAME,
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
    /// one is being created, and returns the writer mode the marker records. `None` only ever
    /// means "no marker yet, and `create` may write one". A directory holding some other
    /// `metadata` file is refused rather than taken over, even by `create()`: overwriting it would
    /// be a destructive way to report a mistyped path.
    pub(super) fn read_metadata(&self, create: bool) -> Result<Option<WriterMode>, DatabaseError> {
        let path = self.metadata_file();
        require_regular_file(&path)?;
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // Opening follows symlinks, so NotFound covers a dangling one as well as a truly
                // absent marker. The name being taken at all -- by anything -- means the
                // directory is not redb's to mark
                if occupied(&self.metadata_file())? {
                    return Err(StorageError::Io(io::Error::new(
                        ErrorKind::InvalidData,
                        "not a multi-process database directory",
                    ))
                    .into());
                }
                if !create {
                    return Err(StorageError::Io(io::Error::new(
                        ErrorKind::NotFound,
                        "not a multi-process database directory",
                    ))
                    .into());
                }
                self.reject_foreign_directory()?;
                return Ok(None);
            }
            Err(err) => return Err(StorageError::Io(err).into()),
        };

        // Bounded read: a mistyped path can point at a `metadata` of any size, and one byte past
        // the marker's length is enough to reject it
        let mut bytes = Vec::with_capacity(METADATA_LEN + 1);
        file.take((METADATA_LEN + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(StorageError::Io)?;

        if bytes.len() != METADATA_LEN || bytes[0..11] != MAGIC {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                "not a multi-process database directory",
            ))
            .into());
        }
        let version = bytes[11];
        if version != FORMAT_VERSION {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                format!("unsupported multi-process database version: {version}"),
            ))
            .into());
        }
        let Some(mode) = WriterMode::from_byte(bytes[12]) else {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                format!("unsupported writer mode: {}", bytes[12]),
            ))
            .into());
        };

        Ok(Some(mode))
    }
}

/// The lock on `registry.lock`, which guards cross-process access to `extended-header`, the `txn/`
/// directory, and the header of `data.redb`: shared to read them, exclusive to write them.
///
/// The file itself is empty. Every acquisition opens its own file handle, because advisory locks
/// belong to the open file description: with a handle per acquisition, any number of threads can
/// hold the shared lock at once, and dropping one guard never releases another's lock.
#[derive(Debug, Clone)]
pub(crate) struct RegistryLock {
    path: PathBuf,
}

impl RegistryLock {
    pub(super) fn open(dir: &DatabaseDir) -> Self {
        Self {
            path: dir.registry_file(),
        }
    }

    fn acquire(&self, exclusive: bool) -> Result<RegistryGuard> {
        let file = open_or_create(&self.path).map_err(StorageError::Io)?;
        if exclusive {
            file.lock().map_err(unsupported_lock)?;
        } else {
            file.lock_shared().map_err(unsupported_lock)?;
        }
        Ok(RegistryGuard { file })
    }

    /// Blocks until every writer of the guarded state has finished. Held while reading the header
    /// of `data.redb`, reading `extended-header`, or pinning a transaction in `txn/`.
    pub(super) fn shared(&self) -> Result<RegistryGuard> {
        self.acquire(false)
    }

    /// Blocks until every other process's reads and pins are complete. Held while writing the
    /// header of `data.redb` or `extended-header`, or scanning `txn/`.
    pub(super) fn exclusive(&self) -> Result<RegistryGuard> {
        self.acquire(true)
    }
}

/// A held registry lock. Dropping it releases the lock.
pub(crate) struct RegistryGuard {
    file: File,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        // Nothing can be done about a failure, and closing the handle releases the lock anyway;
        // the explicit unlock only makes it prompt on platforms where the close is lazy about it
        let _ = self.file.unlock();
    }
}

/// `extended-header`: two slots of {transaction collection horizon, hash}, logically an extension
/// of the commit slots in the header of `data.redb`. The active slot is the one the primary slot
/// bit in that header selects.
///
/// The hash binds a horizon to the commit slot bytes it was written beside. There is no fsync of
/// this file, so after a crash a slot can pair a stale horizon with a newer commit slot -- and the
/// hash is how a reader notices, falling back to assuming the worst. The caller must hold the
/// registry lock: shared to read, exclusive to write.
pub(super) struct ExtendedHeader {
    file: File,
}

/// 8 bytes of horizon, then 16 of hash.
const EXTENDED_HEADER_SLOT_LEN: usize = 24;

fn extended_header_hash(horizon: u64, commit_slot: &[u8]) -> u128 {
    let mut bound = Vec::with_capacity(8 + commit_slot.len());
    bound.extend_from_slice(&horizon.to_le_bytes());
    bound.extend_from_slice(commit_slot);
    xxh3_checksum(&bound)
}

impl ExtendedHeader {
    pub(super) fn open(dir: &DatabaseDir) -> Result<Self> {
        require_regular_file(&dir.extended_header_file())?;
        Ok(Self {
            file: open_or_create(&dir.extended_header_file()).map_err(StorageError::Io)?,
        })
    }

    /// The horizon in `slot`, if the slot's hash matches `commit_slot` -- the corresponding commit
    /// slot bytes exactly as the caller read them from `data.redb` under this same lock. `None`
    /// means the slot is torn, stale, or never written, and the caller must assume a horizon of
    /// its own choosing per the crash rules in `docs/design.md`.
    pub(super) fn read_horizon(&self, slot: usize, commit_slot: &[u8]) -> Result<Option<u64>> {
        let mut bytes = [0u8; EXTENDED_HEADER_SLOT_LEN];
        let offset = (slot * EXTENDED_HEADER_SLOT_LEN) as u64;
        if read_at(&self.file, offset, &mut bytes).is_err() {
            // Too short to hold the slot, which is what a freshly created file looks like. Real
            // I/O errors surface on the next write; misreading one as corruption costs a reread
            // of the cache, not correctness
            return Ok(None);
        }
        let horizon = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let hash = u128::from_le_bytes(bytes[8..24].try_into().unwrap());
        if hash != extended_header_hash(horizon, commit_slot) {
            return Ok(None);
        }
        Ok(Some(horizon))
    }

    /// Writes `slot`, binding `horizon` to `commit_slot`. Not fsynced: a commit's flip does not
    /// wait on this file, and the hash is what catches a write that never landed.
    pub(super) fn write_horizon(&self, slot: usize, horizon: u64, commit_slot: &[u8]) -> Result {
        let mut bytes = [0u8; EXTENDED_HEADER_SLOT_LEN];
        bytes[0..8].copy_from_slice(&horizon.to_le_bytes());
        bytes[8..24].copy_from_slice(&extended_header_hash(horizon, commit_slot).to_le_bytes());
        write_at(&self.file, (slot * EXTENDED_HEADER_SLOT_LEN) as u64, &bytes)
    }
}

/// This process's pin in the `txn/` directory, and the scan over everyone's.
///
/// A pinned transaction is a file in `txn/` whose *name* is the transaction id, held with a
/// *shared* lock: every process reading that transaction holds the same file at once, and a writer
/// fails to take it exclusively for as long as any of them does. Nothing is read from or written
/// to those files -- the name is the whole of the data. That matters for more than tidiness: a
/// lock is mandatory on some platforms, so a file another process holds locked cannot be read.
///
/// A process pins one file: the oldest transaction it still needs, which protects every newer one
/// as well, since the scan stops at the lowest locked id.
pub(super) struct TransactionPins {
    dir: DatabaseDir,
    /// The `txn/` file this process holds. Dropping it releases the lock; the file stays, and the
    /// next writer to scan unlinks it once nobody holds it.
    pinned: Option<File>,
    /// The id `pinned` is named for, so that publishing the same value twice does no I/O
    published: Option<u64>,
}

impl TransactionPins {
    pub(super) fn new(dir: &DatabaseDir) -> Self {
        Self {
            dir: dir.clone(),
            pinned: None,
            published: None,
        }
    }

    /// The transaction this process currently pins.
    pub(super) fn published(&self) -> Option<u64> {
        self.published
    }

    /// Pins the oldest transaction this process needs kept alive, or releases the pin. The caller
    /// must hold the registry lock -- shared is enough: this only adds a file and takes a shared
    /// lock on it. Releasing does not delete the file: only a writer that holds it exclusively
    /// may, or a reader could lock a file that is already unlinked and pin nothing.
    pub(super) fn publish(&mut self, pinned: Option<u64>) -> Result<()> {
        if pinned == self.published {
            return Ok(());
        }
        let held = if let Some(id) = pinned {
            let file = open_or_create(&self.dir.pinned_file(id)).map_err(StorageError::Io)?;
            file.lock_shared().map_err(unsupported_lock)?;
            Some(file)
        } else {
            None
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
    /// Walks `txn/` from the lowest id up, unlinking every file it can take exclusively -- those
    /// name transactions nobody is reading any more -- and stopping at the first it cannot, which
    /// is therefore the oldest still in use. The caller must hold the registry lock exclusively,
    /// so that no process is part way through pinning.
    pub(super) fn scan_oldest(&self) -> Result<Option<u64>> {
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
            // Our own pin. Checked by id rather than trusted to the try-lock below, because on
            // platforms that emulate these locks per process rather than per open file, taking it
            // again through a second handle would succeed -- and unlink a file we are still using
            if Some(id) == self.published {
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

/// The storage backend of a multi-process database: a [`FileBackend`] that takes the registry
/// lock exclusively around every write to the database header.
///
/// This is the writing half of the protocol's rule that the header is read and written only under
/// `registry.lock`. Placing it in the backend catches every header write there is, repair
/// included, and the lock is taken per physical write, never across an fsync.
///
/// Lock order: a header write can start under a write-buffer stripe lock, so code holding the
/// registry lock must never wait on the write path. The reader-side sections only read the file,
/// read `extended-header`, and lock `txn/` files, none of which touches the write buffer.
pub(crate) struct HeaderGuardedBackend {
    inner: FileBackend,
    registry: RegistryLock,
}

impl HeaderGuardedBackend {
    pub(super) fn new(file: File, registry: RegistryLock) -> Result<Self, DatabaseError> {
        Ok(Self {
            // No file lock: `write.lock` and the files in `txn/` do that job, and every process
            // needs the database file open at once
            inner: FileBackend::new_internal(file, FileLockKind::None)?,
            registry,
        })
    }
}

impl std::fmt::Debug for HeaderGuardedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeaderGuardedBackend")
            .finish_non_exhaustive()
    }
}

impl StorageBackend for HeaderGuardedBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        if offset < DB_HEADER_SIZE as u64 {
            let _guard = self.registry.exclusive().map_err(|err| match err {
                StorageError::Io(err) => err,
                other => io::Error::other(other.to_string()),
            })?;
            return self.inner.write(offset, data);
        }
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), io::Error> {
        self.inner.close()
    }
}

/// `write.lock`: held exclusively by the process which owns the single logical writer.
pub(crate) struct WriteLock {
    file: File,
    held: bool,
    created: bool,
}

impl WriteLock {
    pub(super) fn open(dir: &DatabaseDir) -> Result<Self> {
        let path = dir.write_lock_file();
        // Otherwise the create below would follow a symlink left under this name, and the lock
        // would be taken on a file whose identity was never checked
        require_regular_file(&path)?;
        // Creation is decided by the open itself rather than by a look beforehand, since a
        // snapshot can go stale while another create() runs to completion, and what the
        // fresh-lock rule may refuse turns on who really made the file
        let (file, created) = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => (file, true),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(StorageError::Io)?;
                // Re-read through the opened handle: the check above can go stale, and a lock
                // file that gained contents -- an abandonment mark included -- or stopped being
                // a regular file vouches for nothing
                let metadata = file.metadata().map_err(StorageError::Io)?;
                if !metadata.is_file() || metadata.len() > 0 {
                    return Err(StorageError::Io(io::Error::new(
                        ErrorKind::InvalidData,
                        "write.lock is not a multi-process database's lock file",
                    )));
                }
                (file, false)
            }
            Err(err) => return Err(StorageError::Io(err)),
        };
        Ok(Self {
            file,
            held: false,
            created,
        })
    }

    /// Whether this handle's own open is what made the lock file.
    pub(super) fn created(&self) -> bool {
        self.created
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

    /// See [`mark_lock_abandoned`].
    pub(super) fn mark_abandoned(&self) {
        mark_lock_abandoned(&self.file);
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
        dir.open_data(create, false)
    }

    /// The whole create sequence, which is split between here and the caller: the database file is
    /// moved into place and the marker written only once [`crate::Database`] has accepted the file.
    /// These tests are about the lock files and the marker rather than the database, so they stand
    /// in for that middle step by doing nothing -- the file they promote is simply empty.
    fn create(dir: &DatabaseDir) -> File {
        dir.prepare(true).unwrap();
        let mut lock = WriteLock::open(dir).unwrap();
        assert!(lock.try_acquire().unwrap());
        dir.init_protocol_files().unwrap();
        let (file, location) = dir.open_data(true, false).unwrap();
        dir.promote_data(location).unwrap();
        dir.write_metadata(WriterMode::MultiWriterProcess).unwrap();
        file
    }

    /// Pins `pinned`, standing in for another process holding that transaction: the lock belongs
    /// to the open file rather than to the process, so a second handle here is excluded exactly as
    /// a second process would be.
    fn pin(dir: &DatabaseDir, pinned: u64) -> TransactionPins {
        let registry = RegistryLock::open(dir);
        let mut pins = TransactionPins::new(dir);
        let guard = registry.shared().unwrap();
        pins.publish(Some(pinned)).unwrap();
        drop(guard);
        pins
    }

    /// Scans as a writer would: under the exclusive registry lock.
    fn scan(dir: &DatabaseDir, pins: &TransactionPins) -> Option<u64> {
        let guard = RegistryLock::open(dir).exclusive().unwrap();
        let oldest = pins.scan_oldest().unwrap();
        drop(guard);
        oldest
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

        let mut reader = pin(&dir, 5);
        let writer = TransactionPins::new(&dir);
        assert_eq!(Some(5), scan(&dir, &writer));

        // Letting go of it is what releases the horizon, and the next scan unlinks the file
        let guard = RegistryLock::open(&dir).shared().unwrap();
        reader.publish(None).unwrap();
        drop(guard);
        assert_eq!(None, scan(&dir, &writer));
        assert!(!dir.pinned_file(5).exists());
    }

    /// A process that pins its own transaction and then scans keeps its own file: it is still
    /// reading that transaction.
    #[test]
    fn a_scan_does_not_clean_up_its_own_pin() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));

        let pins = pin(&dir, 5);
        assert_eq!(Some(5), scan(&dir, &pins));
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

        // What a process that exited without unlinking leaves: the file, and no lock on it
        std::fs::write(dir.pinned_file(7), []).unwrap();

        let writer = TransactionPins::new(&dir);
        assert_eq!(None, scan(&dir, &writer));
        assert!(!dir.pinned_file(7).exists());
    }

    /// The scan stops at the first file it cannot take, so a live pin hides the ones above it. They
    /// cost nothing: a later scan reaches them once this one is let go.
    #[test]
    fn the_scan_stops_at_the_oldest_live_pin() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));

        std::fs::write(dir.pinned_file(3), []).unwrap();
        let _live = pin(&dir, 9);
        std::fs::write(dir.pinned_file(11), []).unwrap();

        let writer = TransactionPins::new(&dir);
        assert_eq!(Some(9), scan(&dir, &writer));
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

        let foreign = dir.pinned_dir().join("notanid");
        std::fs::write(&foreign, b"someone else's").unwrap();

        let writer = TransactionPins::new(&dir);
        assert_eq!(None, scan(&dir, &writer));
        assert!(foreign.is_file());
    }

    /// Moving a pin forward takes the new lock before dropping the old one, so a writer scanning
    /// in between sees the older id rather than nothing.
    #[test]
    fn moving_a_pin_forward_releases_the_old_one() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));

        let mut reader = pin(&dir, 4);
        let guard = RegistryLock::open(&dir).shared().unwrap();
        reader.publish(Some(6)).unwrap();
        drop(guard);

        let writer = TransactionPins::new(&dir);
        assert_eq!(Some(6), scan(&dir, &writer));
        assert!(!dir.pinned_file(4).exists());
    }

    /// The registry lock is what a writer holds while scanning, so a pin and a scan can never
    /// interleave. Each acquisition has its own file handle, which is what makes a shared and an
    /// exclusive acquisition conflict even inside one process.
    #[test]
    fn the_registry_lock_excludes_a_scan_from_a_pinning_reader() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        let registry = RegistryLock::open(&dir);

        let shared = registry.shared().unwrap();
        // A second shared acquisition coexists...
        let also_shared = registry.shared().unwrap();
        // ... and an exclusive one cannot be taken until both are released. Probed with try_lock
        // through a raw handle, since RegistryLock::exclusive would rightly block
        let probe = File::open(dir.registry_file()).unwrap();
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        drop(shared);
        drop(also_shared);
        probe.try_lock().unwrap();
    }

    /// The extended header round-trips a horizon, keyed to the commit slot bytes it was written
    /// beside: the same slot read against different commit slot bytes is reported as invalid, not
    /// as a horizon.
    #[test]
    fn the_extended_header_binds_the_horizon_to_the_commit_slot() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));
        drop(create(&dir));
        let eh = ExtendedHeader::open(&dir).unwrap();

        // Never written: nothing verifies
        assert_eq!(None, eh.read_horizon(0, b"slot zero").unwrap());

        eh.write_horizon(0, 41, b"slot zero").unwrap();
        eh.write_horizon(1, 42, b"slot one").unwrap();
        assert_eq!(Some(41), eh.read_horizon(0, b"slot zero").unwrap());
        assert_eq!(Some(42), eh.read_horizon(1, b"slot one").unwrap());

        // A slot paired with commit slot bytes it was not written beside is stale, which is
        // exactly what a crash between the extended header write and the flip leaves behind
        assert_eq!(None, eh.read_horizon(0, b"slot one").unwrap());

        // Torn bytes fail the hash too
        std::fs::write(dir.extended_header_file(), [0xab; 30]).unwrap();
        assert_eq!(None, eh.read_horizon(0, b"slot zero").unwrap());
        assert_eq!(None, eh.read_horizon(1, b"slot one").unwrap());
    }

    #[test]
    fn the_marker_records_the_writer_mode() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));
        assert_eq!(WriterMode::MultiWriterProcess, dir.mode().unwrap());

        std::fs::remove_file(path.join(METADATA_FILE_NAME)).unwrap();
        dir.write_metadata(WriterMode::SingleWriterProcess).unwrap();
        assert_eq!(WriterMode::SingleWriterProcess, dir.mode().unwrap());
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
        contents[0..11].copy_from_slice(&MAGIC);
        contents[11] = FORMAT_VERSION + 1;
        contents[12] = WRITER_MODE_SINGLE;
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();
        assert!(open(&dir, false).is_err());
        assert!(open(&dir, true).is_err());
    }

    #[test]
    fn a_marker_with_an_unknown_writer_mode_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        drop(create(&dir));

        let mut contents = [0u8; METADATA_LEN];
        contents[0..11].copy_from_slice(&MAGIC);
        contents[11] = FORMAT_VERSION;
        contents[12] = WRITER_MODE_MULTI + 1;
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();
        assert!(open(&dir, false).is_err());
        assert!(open(&dir, true).is_err());
    }
}
