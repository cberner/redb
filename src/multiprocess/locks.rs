//! The files that make up a multi-process database directory, and the lock that excludes other
//! processes from it.
//!
//! Everything in here uses only `std::fs` file operations and the advisory file locks exposed by
//! `std::fs::File`. See `docs/design.md` for the protocol these files implement.

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

/// The paths that make up a multi-process database directory.
pub(super) struct DatabaseDir {
    root: PathBuf,
}

impl DatabaseDir {
    /// Every name `create()` writes after taking the write lock. `metadata` is not among them: the
    /// check that uses this runs only where there is no marker.
    const WRITTEN_UNDER_THE_LOCK: &'static [&'static str] =
        &[DATA_FILE_NAME, METADATA_TMP_FILE_NAME];

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
    /// file that holds the write lock for as long as the database is open.
    ///
    /// A directory being created is not marked as one of these yet -- the caller does that with
    /// [`Self::write_metadata_if_missing`], once the database file has turned out to be usable.
    pub(super) fn open(&self, create: bool) -> Result<Box<dyn StorageBackend>, DatabaseError> {
        let mut marked_at_preflight = false;
        if create {
            // Both checks run before the directory is touched, so a rejected create() leaves no
            // trace. A marker arrives by rename and so is either absent or complete, which is what
            // makes refusing without the lock sound; `read_metadata` below repeats the check under
            // the lock, which is the authoritative pass
            marked_at_preflight = occupied(&self.metadata_file())?;
            if marked_at_preflight {
                self.read_metadata(create)?;
            } else {
                self.reject_unmarked_database()?;
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

        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        if fresh_claim {
            // The pass above required this name to be absent, and a file appearing since arrived
            // the same way -- something reaching past the directory API -- so creating exclusively
            // turns the appearance into the same refusal instead of an adoption
            options.create_new(true);
        } else {
            options.create(create);
        }
        let data = match options.open(self.data_file()) {
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

        Ok(Box::new(DirectoryBackend { data, write_lock }))
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
        let mut file = File::create(&tmp).map_err(StorageError::Io)?;
        file.write_all(&contents).map_err(StorageError::Io)?;
        file.sync_all().map_err(StorageError::Io)?;
        drop(file);
        std::fs::rename(&tmp, self.metadata_file()).map_err(StorageError::Io)?;
        sync_dir(&self.root)?;

        Ok(())
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
        let file = match File::open(self.metadata_file()) {
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
                return Ok(());
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

    /// The whole create sequence, which is split between here and the caller: the marker is
    /// written only once [`crate::Database`] has accepted the database file. These tests are about
    /// the lock and the marker rather than the database, so they stand in for that middle step by
    /// doing nothing.
    fn create(dir: &DatabaseDir) -> Box<dyn StorageBackend> {
        let backend = dir.open(true).unwrap();
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
        let _second = dir.open(false).unwrap();
    }

    #[test]
    fn a_directory_without_the_marker_is_not_a_database() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        std::fs::create_dir(&path).unwrap();
        // A lock file on its own is not enough -- the marker is what says the directory is one of
        // these
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

    /// Because the marker is never left half-written, a `metadata` file that is not a marker
    /// belongs to something else, and `create()` must refuse rather than overwrite it.
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
