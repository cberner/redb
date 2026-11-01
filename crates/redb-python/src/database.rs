use crate::error::{PyErrExt, TransactionInProgress};
use crate::transaction::{PyReadTransaction, PyWriteTransaction};
use ::redb::ReadableDatabase;
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::{RwLock, RwLockWriteGuard, TryLockError};

/// How processes may share a database file.
///
/// Both modes let other processes read the database while one writes to it. redb's third
/// mode, where a writer locks everyone else out of the file, has no Python name.
#[pyclass(
    module = "redb",
    name = "ConcurrencyMode",
    eq,
    hash,
    frozen,
    from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ConcurrencyMode {
    /// One process writes, any number may open the database read-only meanwhile; the default
    SingleWriter,
    /// Any number of processes may open the database for writing; their write transactions
    /// take turns
    MultiWriter,
}

impl ConcurrencyMode {
    const ALL: [ConcurrencyMode; 2] = [ConcurrencyMode::SingleWriter, ConcurrencyMode::MultiWriter];

    fn name(self) -> &'static str {
        match self {
            ConcurrencyMode::SingleWriter => "SingleWriter",
            ConcurrencyMode::MultiWriter => "MultiWriter",
        }
    }

    /// A builder for this mode, with `cache_size` bytes of page cache where given
    fn builder(self, cache_size: Option<usize>) -> ::redb::Builder {
        let mut builder = ::redb::Builder::new();
        builder.set_concurrency_mode(match self {
            ConcurrencyMode::SingleWriter => ::redb::ConcurrencyMode::SingleWriter,
            ConcurrencyMode::MultiWriter => ::redb::ConcurrencyMode::MultiWriter,
        });
        if let Some(bytes) = cache_size {
            builder.set_cache_size(bytes);
        }
        builder
    }
}

/// A database open for reading and writing.
///
/// `cache_size`, on each way of opening one, is the memory in bytes for caching pages: 1 GiB
/// unless given.
#[pyclass(module = "redb", name = "Database", frozen)]
pub(crate) struct PyDatabase {
    // Maintenance needs the database to itself, as `&mut`; transactions share it
    inner: RwLock<::redb::Database>,
}

impl PyDatabase {
    /// The database alone, for maintenance. Refused rather than waited for while a transaction
    /// is beginning on another thread: that thread may be waiting for the caller's own write
    /// transaction to end, which the caller cannot do from here
    fn exclusive(&self) -> PyResult<RwLockWriteGuard<'_, ::redb::Database>> {
        self.inner.try_write().map_err(|err| match err {
            TryLockError::WouldBlock => {
                TransactionInProgress::new_err("a transaction is beginning on another thread")
            }
            TryLockError::Poisoned(err) => panic!("{err}"),
        })
    }
}

#[pymethods]
impl PyDatabase {
    /// Opens the database at `path`, creating it if the file does not exist or is empty
    #[staticmethod]
    #[pyo3(signature = (path, *, concurrency = ConcurrencyMode::SingleWriter, cache_size = None))]
    fn create(
        py: Python<'_>,
        path: PathBuf,
        concurrency: ConcurrencyMode,
        cache_size: Option<usize>,
    ) -> PyResult<Self> {
        let inner = py
            .detach(|| concurrency.builder(cache_size).create(path))
            .pyerr()?;
        Ok(Self {
            inner: RwLock::new(inner),
        })
    }

    /// Opens an existing database
    #[staticmethod]
    #[pyo3(signature = (path, *, concurrency = ConcurrencyMode::SingleWriter, cache_size = None))]
    fn open(
        py: Python<'_>,
        path: PathBuf,
        concurrency: ConcurrencyMode,
        cache_size: Option<usize>,
    ) -> PyResult<Self> {
        let inner = py
            .detach(|| concurrency.builder(cache_size).open(path))
            .pyerr()?;
        Ok(Self {
            inner: RwLock::new(inner),
        })
    }

    /// Begins a write transaction, waiting for any other writer to finish
    fn begin_write(slf: Py<Self>, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let txn = py
            .detach(|| slf.get().inner.read().unwrap().begin_write())
            .pyerr()?;
        Ok(PyWriteTransaction::new(slf, txn))
    }

    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let txn = py
            .detach(|| slf.get().inner.read().unwrap().begin_read())
            .pyerr()?;
        Ok(PyReadTransaction::new(slf.into_any(), txn))
    }

    /// Rewrites the file without its free space, shrinking it; `False` if there was none to
    /// reclaim. Refused with `TransactionInProgress` while a transaction of this database is
    /// alive or beginning
    fn compact(&self, py: Python<'_>) -> PyResult<bool> {
        let mut guard = self.exclusive()?;
        let db = &mut *guard;
        py.detach(|| db.compact()).pyerr()
    }

    /// Verifies every page of the file: `True` if it was sound, `False` if it had to be
    /// repaired, `Corrupted` if it could not be. Slow, and refused with `TransactionInProgress`
    /// while a transaction of this database is alive or beginning
    fn check_integrity(&self, py: Python<'_>) -> PyResult<bool> {
        let mut guard = self.exclusive()?;
        let db = &mut *guard;
        py.detach(|| db.check_integrity()).pyerr()
    }

    /// Opens an existing database for reading only, alongside the process writing to it in
    /// either mode
    #[staticmethod]
    #[pyo3(signature = (path, *, cache_size = None))]
    fn open_read_only(
        py: Python<'_>,
        path: PathBuf,
        cache_size: Option<usize>,
    ) -> PyResult<PyReadOnlyDatabase> {
        // A reader locks and reads the file the same way in both modes
        let inner = py
            .detach(|| {
                ConcurrencyMode::SingleWriter
                    .builder(cache_size)
                    .open_read_only(path)
            })
            .pyerr()?;
        Ok(PyReadOnlyDatabase { inner })
    }
}

/// A database open for reading only, which follows the commits of the process writing to it
#[pyclass(module = "redb", name = "ReadOnlyDatabase", frozen)]
pub(crate) struct PyReadOnlyDatabase {
    inner: ::redb::ReadOnlyDatabase,
}

#[pymethods]
impl PyReadOnlyDatabase {
    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let txn = py.detach(|| slf.get().inner.begin_read()).pyerr()?;
        Ok(PyReadTransaction::new(slf.into_any(), txn))
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyReadOnlyDatabase>()?;
    m.add_class::<ConcurrencyMode>()?;
    // Each mode is a module attribute too: `redb.SingleWriter` is `redb.ConcurrencyMode.SingleWriter`
    let class = m.py().get_type::<ConcurrencyMode>();
    for mode in ConcurrencyMode::ALL {
        m.add(mode.name(), class.getattr(mode.name())?)?;
    }
    Ok(())
}
