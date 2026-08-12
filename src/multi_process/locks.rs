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
fn sync_dir(root: &Path) -> Result<(), DatabaseError> {
    let dir = match File::open(root) {
        Ok(dir) => dir,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => return Ok(()),
        Err(err) => return Err(StorageError::Io(err).into()),
    };
    dir.sync_all().map_err(StorageError::Io)?;

    Ok(())
}

/// A known gap rather than a case that needs no handling: `std` exposes no directory handle to sync
/// off Unix, and no way to make the rename itself write-through. `docs/design.md` records what a
/// crash can cost here.
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

/// Flushes the directory holding the database directory.
fn sync_parent(root: &Path) -> Result<(), DatabaseError> {
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

    /// Takes the write lock, which excludes every other process from the database. Held for as
    /// long as this process has it open.
    ///
    /// Taken before anything in the directory is read or written, so that a process which gets it
    /// has the directory to itself -- including while it is being created. Only `create()` makes
    /// the file: an `open()` that is about to reject the directory should not leave one behind.
    ///
    /// A failing `create()` does leave one, and cleaning it up would be worse than leaving it --
    /// another process may be waiting on the lock on that same file, and unlinking would leave it
    /// holding a lock on an unlinked inode while a third locks a fresh `write.lock` successfully.
    /// An empty lock file means nothing on its own; the marker is what makes a directory one of
    /// these.
    fn acquire_write_lock(&self, create: bool) -> Result<File, DatabaseError> {
        let path = self.write_lock_file();
        let file = if create {
            open_or_create(&path)
        } else {
            OpenOptions::new().read(true).write(true).open(&path)
        }
        .map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                StorageError::Io(io::Error::new(
                    ErrorKind::NotFound,
                    "not a multi-process database directory",
                ))
            } else {
                StorageError::Io(err)
            }
        })?;

        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(TryLockError::WouldBlock) => Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) => Err(lock_unsupported(err)),
        }
    }

    /// Opens the directory, creating it if `create` is set, and returns a backend for the database
    /// file that holds the write lock for as long as the database is open.
    ///
    /// A directory being created is not marked as one of these yet -- the caller does that with
    /// [`Self::write_metadata`], once the database file has turned out to be usable.
    pub(super) fn open(&self, create: bool) -> Result<Box<dyn StorageBackend>, DatabaseError> {
        if create {
            // Both before the directory is touched, so that a rejected create() leaves no trace.
            //
            // A marker arrives by rename, so one that is there is complete, and a file under that
            // name which is not a marker is not a state redb can produce -- it can be refused
            // without the lock. `read_metadata` below repeats the check under the lock, which is
            // the authoritative one; this only turns away what is already certain
            if occupied(&self.metadata_file()) {
                self.read_metadata(create)?;
            } else {
                self.reject_unmarked_database()?;
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

        let write_lock = self.acquire_write_lock(create)?;
        if create {
            // Before anything else is written under these names, because it is the *absence* of
            // this file beside them that says a directory is not this database's. Nothing orders
            // one directory entry against another by itself, and fsyncing a file can carry its own
            // entry to disk, so without this a crash could keep `data.redb` while losing the lock
            // file -- and the next `create()` would refuse the database this one just finished
            sync_dir(&self.root)?;
        }
        self.read_metadata(create)?;

        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(self.data_file())
            .map_err(StorageError::Io)?;
        // The ordinary exclusive lock, the same one a Database takes. The write lock above is what
        // other multi-process handles look at, but a process that reaches past the directory and
        // opens this file directly would not be looking at it, so the file needs a lock of its own.
        // It has to be the exclusive one: a shared lock would let a ReadOnlyDatabase in, and
        // nothing yet stops this process from freeing pages that such a reader is still using.
        // Making room for readers is what the later releases in this series are for, and this is
        // the lock they have to replace
        let data = FileBackend::new(data)?;

        Ok(Box::new(DirectoryBackend { data, write_lock }))
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
            return sync_dir(&self.root);
        }

        self.write_metadata()
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
                return Ok(());
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

/// The database file, plus the write lock that keeps other processes out of the directory it lives
/// in.
///
/// The lock is held here, rather than alongside the [`crate::Database`], because a live write
/// transaction keeps the database open past the point where the `Database` is dropped. Tying the
/// lock to the backend gives it exactly the lifetime of the open file: it is released by `close()`,
/// which redb calls once, when the database has really finished with the file.
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
        // every other process. redb calls this when opening fails too, so it is a real path
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
        dir.write_metadata().unwrap();
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

    /// A `metadata` file that starts with the marker but keeps going is not a marker. The read is
    /// bounded to the marker's length plus one byte, so a directory whose `metadata` is enormous
    /// costs nothing to reject.
    #[test]
    fn an_oversized_marker_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        create(&dir).close().unwrap();

        let mut contents = std::fs::read(path.join(METADATA_FILE_NAME)).unwrap();
        assert_eq!(METADATA_LEN, contents.len());
        contents.extend_from_slice(&[0u8; 4096]);
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();

        assert!(dir.open(false).is_err());
        assert!(dir.open(true).is_err());
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
        create(&dir).close().unwrap();

        let borrowed = tmpdir.path().join("borrowed");
        std::fs::create_dir(&borrowed).unwrap();
        std::fs::write(borrowed.join(WRITE_LOCK_FILE_NAME), []).unwrap();
        std::os::unix::fs::symlink(
            real.join(METADATA_FILE_NAME),
            borrowed.join(METADATA_FILE_NAME),
        )
        .unwrap();

        let borrowed = DatabaseDir::new(&borrowed);
        assert!(borrowed.open(false).is_err());
        assert!(borrowed.open(true).is_err());
    }

    #[test]
    fn a_marker_from_a_later_version_is_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        let dir = DatabaseDir::new(&path);
        create(&dir).close().unwrap();

        let mut contents = [0u8; METADATA_LEN];
        contents[0..8].copy_from_slice(&MAGIC);
        contents[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        std::fs::write(path.join(METADATA_FILE_NAME), contents).unwrap();
        assert!(dir.open(false).is_err());
    }
}
