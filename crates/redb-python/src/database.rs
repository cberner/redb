use crate::error::{DatabaseClosed, PyErrExt, TransactionInProgress};
use crate::fork::ProcessLocal;
use crate::transaction::{PyReadTransaction, PyWriteTransaction};
use ::redb::ReadableDatabase;
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard, TryLockError, Weak};

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

/// Whether a database is still open, and the redb objects of its read transactions, tables and
/// ranges, which keep the file open while they live. Closing the database drops them
#[derive(Clone)]
pub(crate) struct Open(Arc<OpenState>);

struct OpenState {
    open: AtomicBool,
    children: Mutex<Vec<Weak<dyn Close>>>,
}

trait Close: Send + Sync {
    fn close(&self);
}

impl<T: Send + Sync> Close for RwLock<Option<T>> {
    fn close(&self) {
        // Busy only if the operation using it runs Python code, such as a finalizer, that
        // closes the database; then it is dropped with its owner
        if let Ok(mut value) = self.try_write() {
            value.take();
        }
    }
}

impl Open {
    fn new() -> Self {
        Self(Arc::new(OpenState {
            open: AtomicBool::new(true),
            children: Mutex::new(Vec::new()),
        }))
    }

    pub(crate) fn check(&self) -> PyResult<()> {
        if self.0.open.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(closed())
        }
    }

    /// `value`, to be dropped when the database closes
    pub(crate) fn adopt<T: Send + Sync + 'static>(&self, value: T) -> Child<T> {
        let value = Arc::new(RwLock::new(Some(value)));
        let mut children = self.0.children.lock().unwrap();
        // Forgets the children already dropped before the list grows
        if children.len() == children.capacity() {
            children.retain(|child| child.strong_count() > 0);
        }
        children.push(Arc::downgrade(&value) as Weak<dyn Close>);
        drop(children);
        Child {
            value: ProcessLocal::new(value),
            open: self.clone(),
        }
    }

    fn close(&self) {
        self.0.open.store(false, Ordering::Release);
        let children = std::mem::take(&mut *self.0.children.lock().unwrap());
        for child in children.iter().filter_map(Weak::upgrade) {
            child.close();
        }
    }
}

/// A redb object of a read transaction, table or range of the database
pub(crate) struct Child<T> {
    // None once the database is closed
    value: ProcessLocal<Arc<RwLock<Option<T>>>>,
    open: Open,
}

impl<T> Child<T> {
    pub(crate) fn open(&self) -> &Open {
        &self.open
    }

    /// `f` on the object; `DatabaseClosed` once the database is closed
    pub(crate) fn with<R>(&self, f: impl FnOnce(&T) -> PyResult<R>) -> PyResult<R> {
        self.open.check()?;
        let value = self.value.get()?.read().unwrap();
        f(value.as_ref().ok_or_else(closed)?)
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> PyResult<R>) -> PyResult<R> {
        self.open.check()?;
        let mut value = self.value.get()?.write().unwrap();
        f(value.as_mut().ok_or_else(closed)?)
    }
}

fn closed() -> PyErr {
    DatabaseClosed::new_err(::redb::Error::DatabaseClosed.to_string())
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

/// Closes the database in `lock`, if it is still open: drops what was opened to read from it,
/// then the database, without the GIL
fn close<T: Send>(py: Python<'_>, lock: &RwLock<Option<T>>, open: &Open) -> PyResult<()> {
    let mut guard = exclusive(lock)?;
    open.close();
    let db = guard.take();
    drop(guard);
    py.detach(|| drop(db));
    Ok(())
}

/// Begins a read transaction of the database in `lock`, which `db` holds
fn begin_read<T: ReadableDatabase + Send + Sync>(
    py: Python<'_>,
    db: Py<PyAny>,
    lock: &RwLock<Option<T>>,
    open: &Open,
) -> PyResult<PyReadTransaction> {
    let txn = py
        .detach(|| lock.read().unwrap().as_ref().map(T::begin_read))
        .ok_or_else(closed)?
        .pyerr()?;
    // close() may have run on another thread since the lock was released; the GIL keeps it
    // from running again before the transaction is registered
    open.check()?;
    Ok(PyReadTransaction::new(db, txn, open))
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
    open: Open,
}

impl PyDatabase {
    fn new(inner: ::redb::Database) -> Self {
        Self {
            inner: ProcessLocal::new(RwLock::new(Some(inner))),
            open: Open::new(),
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

    /// Begins a write transaction, waiting for any other writer to finish
    fn begin_write(slf: Py<Self>, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let db = slf.get().inner.get()?;
        let txn = py
            .detach(|| {
                db.read()
                    .unwrap()
                    .as_ref()
                    .map(::redb::Database::begin_write)
            })
            .ok_or_else(closed)?
            .pyerr()?;
        Ok(PyWriteTransaction::new(slf, txn))
    }

    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let Self { inner, open } = slf.get();
        begin_read(py, slf.clone_ref(py).into_any(), inner.get()?, open)
    }

    /// Closes the database. From then on it, and the read transactions, tables and ranges
    /// opened from it, raise `DatabaseClosed`. A write transaction still open keeps the file
    /// open until it ends. Closing a closed database does nothing
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        close(py, self.inner.get()?, &self.open)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
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
            open: Open::new(),
        })
    }
}

/// A database open for reading only, which follows the commits of the process writing to it
#[pyclass(module = "redb", name = "ReadOnlyDatabase", frozen)]
pub(crate) struct PyReadOnlyDatabase {
    // None once closed
    inner: ProcessLocal<RwLock<Option<::redb::ReadOnlyDatabase>>>,
    open: Open,
}

#[pymethods]
impl PyReadOnlyDatabase {
    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let Self { inner, open } = slf.get();
        begin_read(py, slf.clone_ref(py).into_any(), inner.get()?, open)
    }

    /// Closes the database, as `Database.close` does
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        close(py, self.inner.get()?, &self.open)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
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
