use crate::error::{DatabaseClosed, PyErrExt, TransactionInProgress};
use crate::fork::ProcessLocal;
use crate::transaction::{PyReadTransaction, PyWriteTransaction};
use ::redb::ReadableDatabase;
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::{RwLock, RwLockWriteGuard, TryLockError};
use std::thread;
use std::time::Duration;

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
    // Maintenance needs the database to itself, as `&mut`; transactions share it. None once
    // closed
    inner: ProcessLocal<RwLock<Option<::redb::Database>>>,
}

fn closed() -> PyErr {
    DatabaseClosed::new_err(::redb::Error::DatabaseClosed.to_string())
}

/// Without waiting for the lock: `compact()` and `check_integrity()` hold it until they have the
/// GIL back, which the caller holds. Neither closes the database
fn is_open<T>(lock: &RwLock<Option<T>>) -> bool {
    match lock.try_read() {
        Ok(db) => db.is_some(),
        Err(TryLockError::WouldBlock) => true,
        Err(TryLockError::Poisoned(err)) => panic!("{err}"),
    }
}

/// The lock alone, for maintenance or for closing. Refused rather than waited for while a
/// transaction is beginning on another thread: that thread may be waiting for the caller's own
/// write transaction to end, which the caller cannot do from here
fn exclusive<T>(lock: &RwLock<T>) -> PyResult<RwLockWriteGuard<'_, T>> {
    lock.try_write().map_err(|err| match err {
        TryLockError::WouldBlock => {
            TransactionInProgress::new_err("a transaction is beginning on another thread")
        }
        TryLockError::Poisoned(err) => panic!("{err}"),
    })
}

/// Closes the database in `lock` with `close`, if it is still open, without the GIL
fn close<T: Send>(
    py: Python<'_>,
    lock: &RwLock<Option<T>>,
    close: impl FnOnce(T) -> Result<(), ::redb::DatabaseError> + Send,
) -> PyResult<()> {
    let db = exclusive(lock)?.take();
    py.detach(|| db.map_or(Ok(()), close)).pyerr()
}

/// Begins a read transaction of the database in `lock`
fn begin_read<T: ReadableDatabase + Send + Sync>(
    py: Python<'_>,
    lock: &RwLock<Option<T>>,
) -> PyResult<::redb::ReadTransaction> {
    py.detach(|| lock.read().unwrap().as_ref().map(T::begin_read))
        .ok_or_else(closed)?
        .pyerr()
}

impl PyDatabase {
    fn new(inner: ::redb::Database) -> Self {
        Self {
            inner: ProcessLocal::new(RwLock::new(Some(inner))),
        }
    }

    /// The database alone, for maintenance
    fn exclusive(&self) -> PyResult<RwLockWriteGuard<'_, Option<::redb::Database>>> {
        exclusive(self.inner.get()?)
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
        Ok(Self::new(inner))
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
        Ok(Self::new(inner))
    }

    /// Begins a write transaction, waiting for any other writer to finish. Ctrl-C interrupts the
    /// wait
    fn begin_write(&self, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let db = self.inner.get()?;
        let mut backoff = Duration::from_millis(1);
        loop {
            // The lock only for the attempt, so that close() can take it between attempts
            let attempt = py.detach(|| {
                db.read()
                    .unwrap()
                    .as_ref()
                    .map(::redb::Database::try_begin_write)
            });
            match attempt.ok_or_else(closed)? {
                Err(::redb::TransactionError::WouldBlock) => {}
                result => return Ok(PyWriteTransaction::new(result.pyerr()?)),
            }
            // Waited for without the GIL, and in steps, between which Ctrl-C can be raised
            py.check_signals()?;
            py.detach(|| thread::sleep(backoff));
            backoff = (backoff * 2).min(Duration::from_millis(32));
        }
    }

    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let txn = begin_read(py, slf.get().inner.get()?)?;
        Ok(PyReadTransaction::new(slf.into_any(), txn))
    }

    /// Closes the database. From then on it raises `DatabaseClosed`, and the read transactions,
    /// tables and ranges opened from it can no longer read the file, so their reads raise it too.
    /// What they have already loaded, such as the only page of a small table, may still be
    /// returned. A write transaction still open keeps the file open until it ends, and the close
    /// raises `TransactionInProgress`, closing the database all the same. Closing a closed
    /// database does nothing
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        close(py, self.inner.get()?, ::redb::Database::close)
    }

    /// `DatabaseClosed` once closed
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if !is_open(slf.inner.get()?) {
            return Err(closed());
        }
        Ok(slf)
    }

    /// Closes the database, whether or not an exception is propagating out of the block
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    /// Rewrites the file without its free space, shrinking it; `False` if there was none to
    /// reclaim. Refused with `TransactionInProgress` while a transaction of this database is
    /// alive or beginning
    fn compact(&self, py: Python<'_>) -> PyResult<bool> {
        let mut guard = self.exclusive()?;
        let db = guard.as_mut().ok_or_else(closed)?;
        py.detach(|| db.compact()).pyerr()
    }

    /// Verifies every page of the file: `True` if it was sound, `False` if it had to be
    /// repaired, `Corrupted` if it could not be. Slow, and refused with `TransactionInProgress`
    /// while a transaction of this database is alive or beginning
    fn check_integrity(&self, py: Python<'_>) -> PyResult<bool> {
        let mut guard = self.exclusive()?;
        let db = guard.as_mut().ok_or_else(closed)?;
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
        Ok(PyReadOnlyDatabase {
            inner: ProcessLocal::new(RwLock::new(Some(inner))),
        })
    }
}

/// A database open for reading only, which follows the commits of the process writing to it
#[pyclass(module = "redb", name = "ReadOnlyDatabase", frozen)]
pub(crate) struct PyReadOnlyDatabase {
    // None once closed
    inner: ProcessLocal<RwLock<Option<::redb::ReadOnlyDatabase>>>,
}

#[pymethods]
impl PyReadOnlyDatabase {
    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let txn = begin_read(py, slf.get().inner.get()?)?;
        Ok(PyReadTransaction::new(slf.into_any(), txn))
    }

    /// Closes the database, as `Database.close` does
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        close(py, self.inner.get()?, ::redb::ReadOnlyDatabase::close)
    }

    /// `DatabaseClosed` once closed
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if !is_open(slf.inner.get()?) {
            return Err(closed());
        }
        Ok(slf)
    }

    /// Closes the database, whether or not an exception is propagating out of the block
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

/// `Database.open`
#[pyfunction]
#[pyo3(signature = (path, *, concurrency = ConcurrencyMode::SingleWriter, cache_size = None))]
fn open(
    py: Python<'_>,
    path: PathBuf,
    concurrency: ConcurrencyMode,
    cache_size: Option<usize>,
) -> PyResult<PyDatabase> {
    PyDatabase::open(py, path, concurrency, cache_size)
}

/// `Database.open_read_only`
#[pyfunction]
#[pyo3(signature = (path, *, cache_size = None))]
fn open_read_only(
    py: Python<'_>,
    path: PathBuf,
    cache_size: Option<usize>,
) -> PyResult<PyReadOnlyDatabase> {
    PyDatabase::open_read_only(py, path, cache_size)
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyReadOnlyDatabase>()?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(open_read_only, m)?)?;
    m.add_class::<ConcurrencyMode>()?;
    // Each mode is a module attribute too: `redb.SingleWriter` is `redb.ConcurrencyMode.SingleWriter`
    let class = m.py().get_type::<ConcurrencyMode>();
    for mode in ConcurrencyMode::ALL {
        m.add(mode.name(), class.getattr(mode.name())?)?;
    }
    Ok(())
}
