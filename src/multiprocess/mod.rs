//! A multi-process safe interface to a redb database.
//!
//! [`MultiProcessDatabase`] stores its database in a directory, alongside the coordination files
//! described in the "Directory-structured databases" section of `docs/design.md`. Only one
//! process may have the database open at a time.

mod locks;

use crate::db::RepairSession;
use crate::tree_store::PAGE_SIZE;
use crate::{Database, DatabaseError, Result};
use locks::{DataLocation, DatabaseDir};
use std::fs::File;
use std::path::Path;

/// A redb database stored in a directory, alongside the coordination files described in
/// `docs/design.md`.
///
/// The directory must be on a filesystem that supports file locking, and belongs to redb -- do not
/// put anything else in it. A second process that opens the same directory fails with
/// [`DatabaseError::DatabaseAlreadyOpen`]: the exclusion is carried by a lock file in the
/// directory rather than by a lock on the database file itself.
pub struct MultiProcessDatabase {
    /// Holds the database open; dropping it closes the file and releases the write lock.
    _inner: Database,
    /// The shared lock every process holds on `metadata` for as long as it has the database open.
    /// A future format upgrade takes it exclusively, and this is what it waits on.
    _metadata_lock: File,
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
        let (backend, location) = dir.open(create)?;
        // A guard rather than a check on the error path: the repair callback runs inside the
        // initialization and is the caller's code, so the span can be left by a panic the caller
        // catches as well as by an error, and the temporary has to be cleaned up either way
        let mut cleanup = DiscardFailedData {
            dir: &dir,
            location,
            armed: true,
        };
        // `create` alone decides whether an empty database file may be initialized: an existing
        // database is recognized by its contents rather than by the marker, so a create() that
        // died before initializing the file can be redone
        let inner = Database::new(
            backend,
            create,
            PAGE_SIZE,
            None,
            self.cache_size,
            &self.repair_callback,
        )?;
        if create {
            // Both last, and in this order, so that a create() pointed somewhere that turns out
            // not to hold a database fails without having left either a database file or a marker
            // behind
            dir.promote_data(location)?;
            dir.write_metadata_if_missing()?;
        }
        cleanup.armed = false;
        let metadata_lock = dir.lock_metadata_shared()?;

        Ok(MultiProcessDatabase {
            _inner: inner,
            _metadata_lock: metadata_lock,
        })
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
