//! The files that make up a multi-process database directory, and the lock that excludes other
//! processes from it.
//!
//! Everything in here uses only `std::fs` file operations and the advisory file locks exposed by
//! `std::fs::File`. See `docs/design.md` for the protocol these files implement.

use crate::tree_store::file_backend::FileBackend;
use crate::{DatabaseError, StorageBackend, StorageError};
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const DATA_FILE_NAME: &str = "data.redb";
const WRITE_LOCK_FILE_NAME: &str = "write.lock";

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

/// The paths that make up a multi-process database directory.
pub(super) struct DatabaseDir {
    root: PathBuf,
}

impl DatabaseDir {
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

    /// Takes the write lock, which excludes every other process from the database. Held for as
    /// long as this process has it open.
    ///
    /// Taken before anything else in the directory is opened, so that a process which gets it has
    /// the directory to itself -- including while it is being created. The lock file is only made
    /// when the database is being created: its absence is what tells `open()` that this directory
    /// is not a multi-process database.
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
    pub(super) fn open(&self, create: bool) -> Result<Box<dyn StorageBackend>, DatabaseError> {
        if create {
            std::fs::create_dir_all(&self.root).map_err(StorageError::Io)?;
        } else if !self.root.is_dir() {
            return Err(StorageError::Io(io::Error::new(
                ErrorKind::NotFound,
                "no such multi-process database directory",
            ))
            .into());
        }

        let write_lock = self.acquire_write_lock(create)?;
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
}

/// The database file, plus the write lock that has to outlive it.
///
/// The lock is held here rather than alongside the [`crate::Database`] because a live write
/// transaction keeps the database open past the point where the handle is dropped. A lock released
/// when the handle went away would let another process start writing while that transaction was
/// still running. Tying it to the backend gives it exactly the lifetime of the open file: redb
/// calls `close()` once, when it has really finished.
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
        self.data.close()?;
        self.write_lock.unlock()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn the_write_lock_excludes_other_handles() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir = DatabaseDir::new(tmpdir.path().join("db"));

        let first = dir.open(true).unwrap();
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
    fn a_directory_without_a_lock_file_is_not_a_database() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().join("db");
        std::fs::create_dir(&path).unwrap();

        assert!(DatabaseDir::new(&path).open(false).is_err());
    }
}
