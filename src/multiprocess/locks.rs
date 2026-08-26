//! The files that make up a multi-process database directory, and the lock that excludes other
//! processes from it.
//!
//! Everything in here uses only `std::fs` file operations and the advisory file locks exposed by
//! `std::fs::File`. See `docs/design.md` for the protocol these files implement.

use crate::tree_store::MAGICNUMBER;
use crate::tree_store::file_backend::FileBackend;
use crate::{DatabaseError, StorageBackend, StorageError};
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const DATA_FILE_NAME: &str = "data.redb";
const WRITE_LOCK_FILE_NAME: &str = "write.lock";
const METADATA_FILE_NAME: &str = "metadata";
const METADATA_TMP_FILE_NAME: &str = "metadata.tmp";
const DATA_TMP_FILE_NAME: &str = "data.redb.tmp";

// The ASCII letters 'redbMP' followed by the same PNG-inspired tail as the database file's own
// magic number, for the same reasons.
const MAGIC: [u8; 11] = [
    b'r', b'e', b'd', b'b', b'M', b'P', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A,
];
const FORMAT_VERSION: u8 = 1;
/// A single process owns `write.lock` for as long as it has the database open.
const WRITER_MODE_SINGLE: u8 = 1;
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

/// Flushes a directory itself, so that an entry a rename just created is durable. Best-effort
/// about a directory this process cannot open: flushing needs read permission, and nothing else
/// here does.
#[cfg(unix)]
fn sync_dir(root: &Path) -> Result<(), DatabaseError> {
    let dir = match File::open(root) {
        Ok(dir) => dir,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => return Ok(()),
        Err(err) => return Err(StorageError::Io(err).into()),
    };
    dir.sync_all().map_err(StorageError::Io)?;

    Ok(())
}

/// `std` exposes no directory handle to sync off Unix, and no way to make a rename write-through.
#[cfg(not(unix))]
fn sync_dir(_root: &Path) -> Result<(), DatabaseError> {
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
fn require_regular_file(path: &Path) -> Result<(), DatabaseError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(StorageError::Io(io::Error::new(
            ErrorKind::InvalidData,
            "a multi-process database directory may hold only ordinary files",
        ))
        .into()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(StorageError::Io(err).into()),
    }
}

/// Which name [`DatabaseDir::open`] put the database file under. Carried from the open to
/// [`DatabaseDir::promote_data`] rather than worked out again from disk: a temporary file is this
/// call's own only if this call made it, and nothing about the file itself says so.
#[derive(Clone, Copy)]
pub(super) enum DataLocation {
    /// `data.redb`, which is where a database that was already finished lives.
    Final,
    /// `data.redb.tmp`, which is where one being initialized lives until it is renamed into place.
    Temporary,
}

/// The paths that make up a multi-process database directory.
pub(super) struct DatabaseDir {
    root: PathBuf,
}

impl DatabaseDir {
    /// Every name `create()` writes after taking the write lock. `metadata` is not among them: the
    /// check that uses this runs only where there is no marker.
    const WRITTEN_UNDER_THE_LOCK: &'static [&'static str] =
        &[DATA_FILE_NAME, DATA_TMP_FILE_NAME, METADATA_TMP_FILE_NAME];

    pub(super) fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn data_file(&self) -> PathBuf {
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

    /// Takes the write lock, which excludes every other process from the database, and reports
    /// whether this call created the file. Taken before anything in the directory is read or
    /// written, so the holder has the directory to itself.
    ///
    /// Only `create()` makes the file, and nothing ever unlinks one: another process may be
    /// waiting on the lock on that same file, and unlinking would let a third process lock a
    /// fresh `write.lock` while the second holds the old inode. Creation is decided by the open
    /// itself rather than by a look beforehand, since a snapshot can go stale while another
    /// `create()` runs to completion, and what the fresh-lock rule may refuse turns on who
    /// really made the file.
    fn acquire_write_lock(&self, create: bool) -> Result<(File, bool), DatabaseError> {
        let path = self.write_lock_file();
        // Otherwise the create below would follow a symlink left under this name, and the
        // lock would be taken on a file whose identity was never checked
        require_regular_file(&path)?;
        let open_existing = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|err| {
                    if err.kind() == ErrorKind::NotFound {
                        StorageError::Io(io::Error::new(
                            ErrorKind::NotFound,
                            "not a multi-process database directory",
                        ))
                    } else {
                        StorageError::Io(err)
                    }
                })
        };
        let (file, created) = if create {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => (file, true),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => (open_existing()?, false),
                Err(err) => return Err(StorageError::Io(err).into()),
            }
        } else {
            (open_existing()?, false)
        };

        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) => return Err(lock_unsupported(err)),
        }
        if !created {
            // Re-read through the held lock: the preflight's snapshot can go stale, and a lock
            // file that gained contents -- an abandonment mark included -- or stopped being a
            // regular file vouches for nothing
            let metadata = file.metadata().map_err(StorageError::Io)?;
            if !metadata.is_file() || metadata.len() > 0 {
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    "write.lock is not a multi-process database's lock file",
                ))
                .into());
            }
        }
        Ok((file, created))
    }

    /// Opens the directory, creating it if `create` is set, and returns a backend for the database
    /// file that holds the write lock for as long as the database is open, along with which of the
    /// two names that file is under.
    ///
    /// A directory being created is not marked as one of these yet -- the caller does that with
    /// [`Self::write_metadata_if_missing`], once the database file has turned out to be usable.
    pub(super) fn open(
        &self,
        create: bool,
    ) -> Result<(Box<dyn StorageBackend>, DataLocation), DatabaseError> {
        let mut marked_at_preflight = false;
        if create {
            // All checks run before the directory is touched, so a rejected create() leaves no
            // trace. A marker arrives by rename and so is either absent or complete, which is what
            // makes refusing without the lock sound; the checks run again under the lock, which is
            // the authoritative pass
            marked_at_preflight = occupied(&self.metadata_file())?;
            if marked_at_preflight {
                self.read_metadata(create)?;
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

        let (write_lock, lock_created) = self.acquire_write_lock(create)?;
        let mut fresh_claim = false;
        if create {
            // The lock file's entry must be durable before anything else is written under these
            // names: its *absence* beside them is what says a directory is not this database's,
            // and a crash could otherwise keep `data.redb` while losing `write.lock`
            sync_dir(&self.root)?;
            // A marker seen before the lock file was made marks a create() to finish -- a crash
            // can lose the lock's entry while the marker survives -- and the fresh-lock rule has
            // nothing to refuse. One that first appears after cannot be another create()'s, since
            // finishing one needs the lock this call holds, so the rejecting pass below refuses
            // it like every other name that arrives while the directory is being claimed
            fresh_claim = lock_created && !marked_at_preflight;
            if fresh_claim && let Err(err) = self.reject_files_beside_a_fresh_lock() {
                mark_lock_abandoned(&write_lock);
                return Err(err);
            }
        }
        self.read_metadata(create)?;

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
                    mark_lock_abandoned(&write_lock);
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
                mark_lock_abandoned(&write_lock);
                return Err(StorageError::Io(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "the database file appeared while the directory was being claimed",
                ))
                .into());
            }
            Err(err) => return Err(StorageError::Io(err).into()),
        };
        // The ordinary exclusive lock, the same one a Database takes: a process that reaches past
        // the directory and opens this file directly is not looking at the write lock, so the file
        // needs a lock of its own. Exclusive rather than shared, because nothing coordinates a
        // reader that attaches this way with the pages this process frees
        let data = match FileBackend::new(data) {
            Ok(data) => data,
            Err(err) => {
                // On a fresh claim the file was created exclusively just above, so failing to
                // lock it means something took it in between -- and the lock file left empty
                // would vouch, on the next create(), for whatever that something makes of it
                if fresh_claim {
                    mark_lock_abandoned(&write_lock);
                }
                return Err(err);
            }
        };

        Ok((Box::new(DirectoryBackend { data, write_lock }), location))
    }

    /// Moves a freshly initialized database file into place, if this call was the one that made
    /// it. Called once [`crate::Database`] has accepted the file, so that `data.redb` exists only
    /// when it holds a finished database. The write lock is still held, so nothing can be looking
    /// at either name.
    pub(super) fn promote_data(&self, location: DataLocation) -> Result<(), DatabaseError> {
        let tmp = self.data_tmp_file();
        if matches!(location, DataLocation::Final) {
            // The database opened under its own name, so anything under the temporary one is the
            // wreckage of an earlier attempt. Deleted here rather than on the way in, so a
            // create() pointed somewhere by mistake fails without having deleted anything; and
            // tidying only, so a failure is not the caller's problem -- what is left is inert.
            // Except a finished database, which is never tidied away
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

    /// Writes the marker only if the directory does not already carry one. One that is there was
    /// validated by [`Self::read_metadata`] on the way in, so it is byte-for-byte what would be
    /// written.
    pub(super) fn write_metadata_if_missing(&self) -> Result<(), DatabaseError> {
        if occupied(&self.metadata_file())? {
            // Validated here rather than assumed: the write lock keeps redb out, but the claim
            // that an existing marker is byte-for-byte what would be written has to survive
            // whatever else was pointed at the directory
            self.read_metadata(false)?;
            // This call may still have recreated the lock file or the database file after a crash
            // lost their entries, and write_metadata() is where the directory would otherwise be
            // flushed
            return sync_dir(&self.root);
        }

        self.write_metadata()
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

    /// The accepting pass for a directory whose lock file this call has just created. A
    /// pre-existing `write.lock` vouches for redb's other names; a fresh one vouches for nothing,
    /// so anything already under them arrived while the directory was being claimed -- another
    /// process reaching past the directory API -- and adopting it would hand this handle a file
    /// something else is using.
    fn reject_files_beside_a_fresh_lock(&self) -> Result<(), DatabaseError> {
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

    /// Writes the marker that says this directory holds a multi-process database. Called only once
    /// the database file has been initialized, so a failed `create()` never leaves a marker.
    ///
    /// Written under a temporary name and renamed into place, so the marker is either absent or
    /// complete: a half-written one would be indistinguishable from a foreign file, which
    /// [`Self::read_metadata`] refuses, and the directory would be wedged.
    fn write_metadata(&self) -> Result<(), DatabaseError> {
        let mut contents = [0u8; METADATA_LEN];
        contents[0..11].copy_from_slice(&MAGIC);
        contents[11] = FORMAT_VERSION;
        contents[12] = WRITER_MODE_SINGLE;

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
            let named_like_ours = [
                DATA_FILE_NAME,
                DATA_TMP_FILE_NAME,
                WRITE_LOCK_FILE_NAME,
                METADATA_FILE_NAME,
                METADATA_TMP_FILE_NAME,
            ]
            .iter()
            .any(|known| name == *known);
            // `file_type()` reports the entry rather than what it points at, so a symlink wearing
            // one of these names is caught instead of followed
            let ours = named_like_ours && entry.file_type().map_err(StorageError::Io)?.is_file();
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

    /// Takes the shared lock on `metadata` that every process holds for as long as it has the
    /// database open. A future version that changes the directory's format will take this lock
    /// exclusively, so that it upgrades only once nothing is using the database.
    pub(super) fn lock_metadata_shared(&self) -> Result<File, DatabaseError> {
        let file = File::open(self.metadata_file()).map_err(StorageError::Io)?;
        file.lock_shared().map_err(lock_unsupported)?;
        Ok(file)
    }

    /// Checks that this directory holds a multi-process database, tolerating a missing marker when
    /// one is being created. A directory holding some other `metadata` file is refused rather than
    /// taken over, even by `create()`: overwriting it would be a destructive way to report a
    /// mistyped path. The caller must hold the write lock.
    fn read_metadata(&self, create: bool) -> Result<(), DatabaseError> {
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
                return self.reject_foreign_directory();
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
        let mode = bytes[12];
        if mode != WRITER_MODE_SINGLE {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::InvalidData,
                format!("unsupported writer mode: {mode}"),
            ))
            .into());
        }

        Ok(())
    }
}

/// The database file, plus the write lock that keeps other processes out of its directory.
///
/// The lock is held here rather than alongside the [`crate::Database`] because a live write
/// transaction keeps the database open past the point where the `Database` is dropped: tied to the
/// backend, the lock lasts exactly as long as the open file, released by `close()`.
#[derive(Debug)]
struct DirectoryBackend {
    data: FileBackend,
    write_lock: File,
}

impl StorageBackend for DirectoryBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.data.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.data.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.data.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.data.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.data.write(offset, data)
    }

    fn close(&self) -> Result<(), io::Error> {
        // Both run: a file that failed to close must not also leave the directory locked against
        // every other process
        let closed = self.data.close();
        let unlocked = self.write_lock.unlock();

        closed.and(unlocked)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// The whole create sequence, which is split between here and the caller: the database file is
    /// moved into place and the marker written only once [`crate::Database`] has accepted the file.
    /// These tests are about the lock and the marker rather than the database, so they stand in for
    /// that middle step by doing nothing -- the file they promote is simply empty.
    fn create(dir: &DatabaseDir) -> Box<dyn StorageBackend> {
        let (backend, location) = dir.open(true).unwrap();
        dir.promote_data(location).unwrap();
        dir.write_metadata_if_missing().unwrap();
        backend
    }

    #[test]
    fn the_write_lock_excludes_other_handles() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));

        let first = create(&dir);
        assert!(matches!(
            dir.open(false),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));

        // Closing the backend is what releases the lock, since that is when redb has finished
        // with the file
        first.close().unwrap();
        let _second = dir.open(false).unwrap().0;
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
        assert!(dir.open(false).is_err());

        // An empty file where the marker should be is rejected too, rather than being read as a
        // database with a zero version
        std::fs::write(path.join(METADATA_FILE_NAME), []).unwrap();
        assert!(dir.open(false).is_err());
    }

    /// A `create()` that died while writing the marker leaves the partial copy under the temporary
    /// name and no marker at all, which the next `create()` finishes rather than tripping over.
    #[test]
    fn a_marker_that_never_landed_is_written_again() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        create(&dir).close().unwrap();

        std::fs::remove_file(path.join(METADATA_FILE_NAME)).unwrap();
        std::fs::write(path.join(METADATA_TMP_FILE_NAME), &MAGIC[0..4]).unwrap();

        create(&dir).close().unwrap();
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

        assert!(DatabaseDir::new(&path).open(true).is_err());
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
        create(&dir).close().unwrap();
        std::fs::write(path.join(DATA_FILE_NAME), []).unwrap();

        let (backend, location) = dir.open(true).unwrap();
        assert!(!path.join(DATA_FILE_NAME).exists());
        assert!(path.join(DATA_TMP_FILE_NAME).is_file());

        // ... and promoting puts it back under the name it belongs under
        dir.promote_data(location).unwrap();
        assert!(path.join(DATA_FILE_NAME).is_file());
        assert!(!path.join(DATA_TMP_FILE_NAME).exists());
        backend.close().unwrap();
    }

    /// An empty database file is only wreckage if nobody is holding it. One that a process
    /// reached past the directory to create and lock is live, and unlinking it would leave that
    /// process writing to an inode nothing points at.
    #[test]
    fn an_empty_data_file_someone_is_holding_is_not_discarded() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        create(&dir).close().unwrap();
        std::fs::write(path.join(DATA_FILE_NAME), []).unwrap();

        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(DATA_FILE_NAME))
            .unwrap();
        held.try_lock().unwrap();

        assert!(matches!(
            dir.open(true),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(path.join(DATA_FILE_NAME).is_file());

        // ... and once it is let go, the empty file is wreckage again
        held.unlock().unwrap();
        drop(held);
        dir.open(true).unwrap().0.close().unwrap();
    }

    #[test]
    fn a_marker_from_a_later_version_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        create(&dir).close().unwrap();

        let mut contents = [0u8; METADATA_LEN];
        contents[0..11].copy_from_slice(&MAGIC);
        contents[11] = FORMAT_VERSION + 1;
        contents[12] = WRITER_MODE_SINGLE;
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();
        assert!(dir.open(false).is_err());
    }

    #[test]
    fn a_marker_with_an_unknown_writer_mode_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        create(&dir).close().unwrap();

        let mut contents = [0u8; METADATA_LEN];
        contents[0..11].copy_from_slice(&MAGIC);
        contents[11] = FORMAT_VERSION;
        contents[12] = WRITER_MODE_SINGLE + 1;
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();
        assert!(dir.open(false).is_err());
    }
}
