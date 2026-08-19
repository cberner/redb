//! A multi-process safe interface to a redb database.
//!
//! **This is the first step of an incomplete feature.** [`MultiProcessDatabase`] currently allows
//! only one process to have the database open at all -- it establishes the directory layout and
//! the lock file that later releases relax, so that other processes can read while one writes. Use
//! [`Database`] until then; this type has no advantage over it yet.

mod locks;

use crate::db::RepairSession;
use crate::sealed::Sealed;
use crate::tree_store::PAGE_SIZE;
use crate::{
    CacheStats, Database, DatabaseError, ReadTransaction, ReadableDatabase, Result,
    TransactionError, WriteTransaction,
};
use locks::DatabaseDir;
use std::path::Path;

/// A redb database stored in a directory, alongside the lock files that coordinate the processes
/// using it.
///
/// Use [`Self::begin_read`] to get a [`ReadTransaction`], and [`Self::begin_write`] to get a
/// [`WriteTransaction`]. Both behave exactly as they do for a [`Database`].
///
/// The directory must be on a filesystem that supports file locking, and belongs to redb -- do not
/// put anything else in it. A second process that opens the same directory fails with
/// [`DatabaseError::DatabaseAlreadyOpen`], as it would for a [`Database`] on a single file. What
/// the directory adds so far is a lock file that carries that exclusion, which is what a later
/// release needs in order to let other processes read while one writes.
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
/// let read_txn = db.begin_read()?;
/// assert_eq!(0, read_txn.open_table(TABLE)?.get_owned(0)?.unwrap().value());
/// # Ok(())
/// # }
/// ```
pub struct MultiProcessDatabase {
    inner: Database,
}

impl MultiProcessDatabase {
    /// Opens the directory at `path` as a multi-process database, creating it if it does not
    /// exist.
    pub fn create(path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        Self::builder().create(path)
    }

    /// Opens an existing multi-process database.
    pub fn open(path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        Self::builder().open(path)
    }

    /// Convenience method for [`MultiProcessBuilder::new`]
    pub fn builder() -> MultiProcessBuilder {
        MultiProcessBuilder::new()
    }

    /// Begins a write transaction
    ///
    /// Returns a [`WriteTransaction`] which may be used to read/write to the database. Only a
    /// single write may be in progress at a time. If a write is in progress, this function blocks
    /// until it completes.
    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        self.inner.begin_write()
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
        f.debug_struct("MultiProcessDatabase").finish()
    }
}

/// Configuration builder of a [`MultiProcessDatabase`].
pub struct MultiProcessBuilder {
    cache_size: usize,
    repair_callback: Box<dyn Fn(&mut RepairSession)>,
}

impl MultiProcessBuilder {
    /// Construct a new [`MultiProcessBuilder`] with sensible defaults.
    ///
    /// ## Defaults
    ///
    /// - `cache_size_bytes`: 1GiB
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cache_size: 1024 * 1024 * 1024,
            repair_callback: Box::new(|_| {}),
        }
    }

    /// Set the amount of memory (in bytes) used for caching data
    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.cache_size = bytes;
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

    /// Opens the directory at `path` as a multi-process database, creating it if it does not exist
    pub fn create(&self, path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        self.open_inner(path.as_ref(), true)
    }

    /// Opens an existing multi-process database
    pub fn open(&self, path: impl AsRef<Path>) -> Result<MultiProcessDatabase, DatabaseError> {
        self.open_inner(path.as_ref(), false)
    }

    fn open_inner(&self, path: &Path, create: bool) -> Result<MultiProcessDatabase, DatabaseError> {
        let dir = DatabaseDir::new(path);
        let backend = dir.open(create)?;
        // `create` alone decides whether an empty database file may be initialized, exactly as it
        // does for a Database: an existing one is recognized by its contents rather than by the
        // directory's marker, so a create() that died before initializing the file can be redone
        let inner = Database::new(
            backend,
            create,
            PAGE_SIZE,
            None,
            self.cache_size,
            &self.repair_callback,
        )?;
        if create {
            // Last, so that a create() pointed at a directory which turns out not to hold a
            // database fails without having marked it as one of these on the way out
            dir.write_metadata_if_missing()?;
        }

        Ok(MultiProcessDatabase { inner })
    }
}
