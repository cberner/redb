use crate::error::PyErrExt;
use crate::transaction::{PyReadTransaction, PyWriteTransaction};
use pyo3::prelude::*;
use redb::ReadableDatabase;
use std::path::PathBuf;
use std::sync::RwLock;

/// How processes may share one database file
#[pyclass(
    module = "redb",
    name = "ConcurrencyMode",
    eq,
    eq_int,
    hash,
    frozen,
    from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PyConcurrencyMode {
    #[pyo3(name = "EXCLUSIVE_WRITER")]
    Exclusive,
    #[pyo3(name = "SINGLE_WRITER")]
    Single,
    #[pyo3(name = "MULTI_WRITER")]
    Multi,
}

fn builder(
    cache_size: Option<usize>,
    concurrency_mode: Option<PyConcurrencyMode>,
) -> redb::Builder {
    let mut builder = redb::Builder::new();
    if let Some(bytes) = cache_size {
        builder.set_cache_size(bytes);
    }
    builder.set_concurrency_mode(match concurrency_mode {
        None | Some(PyConcurrencyMode::Exclusive) => redb::ConcurrencyMode::ExclusiveWriter,
        Some(PyConcurrencyMode::Single) => redb::ConcurrencyMode::SingleWriter,
        Some(PyConcurrencyMode::Multi) => redb::ConcurrencyMode::MultiWriter,
    });
    builder
}

/// A database open for reading and writing
#[pyclass(module = "redb", name = "Database", frozen)]
pub(crate) struct PyDatabase {
    // Transactions begin under the read lock; the maintenance methods that need `&mut Database`
    // take the write lock, so they wait for nothing longer than a begin_* call.
    inner: RwLock<redb::Database>,
}

#[pymethods]
impl PyDatabase {
    /// Opens the database at `path`, creating it if the file does not exist or is empty
    #[staticmethod]
    #[pyo3(signature = (path, *, cache_size=None, concurrency_mode=None))]
    fn create(
        py: Python<'_>,
        path: PathBuf,
        cache_size: Option<usize>,
        concurrency_mode: Option<PyConcurrencyMode>,
    ) -> PyResult<Self> {
        let db = py
            .detach(move || builder(cache_size, concurrency_mode).create(path))
            .pyerr()?;
        Ok(Self {
            inner: RwLock::new(db),
        })
    }

    /// Opens an existing database
    #[staticmethod]
    #[pyo3(signature = (path, *, cache_size=None, concurrency_mode=None))]
    fn open(
        py: Python<'_>,
        path: PathBuf,
        cache_size: Option<usize>,
        concurrency_mode: Option<PyConcurrencyMode>,
    ) -> PyResult<Self> {
        let db = py
            .detach(move || builder(cache_size, concurrency_mode).open(path))
            .pyerr()?;
        Ok(Self {
            inner: RwLock::new(db),
        })
    }

    /// Begins a write transaction, waiting for any other writer to finish
    fn begin_write(slf: Py<Self>, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let txn = {
            let guard = slf.get().inner.read().unwrap();
            let db: &redb::Database = &guard;
            py.detach(|| db.begin_write()).pyerr()?
        };
        Ok(PyWriteTransaction::new(slf.into_any(), txn))
    }

    /// Begins a read transaction, a snapshot of the data committed so far
    fn begin_read(&self, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let guard = self.inner.read().unwrap();
        let db: &redb::Database = &guard;
        Ok(PyReadTransaction::new(
            py.detach(|| db.begin_read()).pyerr()?,
        ))
    }

    /// Checks the database for corruption, repairing it if needed; returns whether it was intact
    fn check_integrity(&self, py: Python<'_>) -> PyResult<bool> {
        let mut guard = self.inner.write().unwrap();
        let db: &mut redb::Database = &mut guard;
        py.detach(|| db.check_integrity()).pyerr()
    }

    /// Shrinks the file by moving data to its start; returns whether anything changed
    fn compact(&self, py: Python<'_>) -> PyResult<bool> {
        let mut guard = self.inner.write().unwrap();
        let db: &mut redb::Database = &mut guard;
        py.detach(|| db.compact()).pyerr()
    }
}

/// A database open for reading only, possibly alongside a writer in another process
#[pyclass(module = "redb", name = "ReadOnlyDatabase", frozen)]
pub(crate) struct PyReadOnlyDatabase {
    inner: redb::ReadOnlyDatabase,
}

#[pymethods]
impl PyReadOnlyDatabase {
    #[staticmethod]
    #[pyo3(signature = (path, *, cache_size=None, concurrency_mode=None))]
    fn open(
        py: Python<'_>,
        path: PathBuf,
        cache_size: Option<usize>,
        concurrency_mode: Option<PyConcurrencyMode>,
    ) -> PyResult<Self> {
        let db = py
            .detach(move || builder(cache_size, concurrency_mode).open_read_only(path))
            .pyerr()?;
        Ok(Self { inner: db })
    }

    fn begin_read(&self, py: Python<'_>) -> PyResult<PyReadTransaction> {
        Ok(PyReadTransaction::new(
            py.detach(|| self.inner.begin_read()).pyerr()?,
        ))
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyConcurrencyMode>()?;
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyReadOnlyDatabase>()?;
    Ok(())
}
