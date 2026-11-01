use crate::error::PyErrExt;
use crate::transaction::{PyReadTransaction, PyWriteTransaction};
use ::redb::ReadableDatabase;
use pyo3::prelude::*;
use std::path::PathBuf;

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

    fn builder(self) -> ::redb::Builder {
        let mut builder = ::redb::Builder::new();
        builder.set_concurrency_mode(match self {
            ConcurrencyMode::SingleWriter => ::redb::ConcurrencyMode::SingleWriter,
            ConcurrencyMode::MultiWriter => ::redb::ConcurrencyMode::MultiWriter,
        });
        builder
    }
}

/// A database open for reading and writing
#[pyclass(module = "redb", name = "Database", frozen)]
pub(crate) struct PyDatabase {
    inner: ::redb::Database,
}

#[pymethods]
impl PyDatabase {
    /// Opens the database at `path`, creating it if the file does not exist or is empty
    #[staticmethod]
    #[pyo3(signature = (path, *, concurrency = ConcurrencyMode::SingleWriter))]
    fn create(py: Python<'_>, path: PathBuf, concurrency: ConcurrencyMode) -> PyResult<Self> {
        let inner = py.detach(|| concurrency.builder().create(path)).pyerr()?;
        Ok(Self { inner })
    }

    /// Opens an existing database
    #[staticmethod]
    #[pyo3(signature = (path, *, concurrency = ConcurrencyMode::SingleWriter))]
    fn open(py: Python<'_>, path: PathBuf, concurrency: ConcurrencyMode) -> PyResult<Self> {
        let inner = py.detach(|| concurrency.builder().open(path)).pyerr()?;
        Ok(Self { inner })
    }

    /// Begins a write transaction, waiting for any other writer to finish
    fn begin_write(slf: Py<Self>, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let txn = py.detach(|| slf.get().inner.begin_write()).pyerr()?;
        Ok(PyWriteTransaction::new(slf, txn))
    }

    /// Begins a read transaction: a snapshot of the data committed so far
    fn begin_read(slf: Py<Self>, py: Python<'_>) -> PyResult<PyReadTransaction> {
        let txn = py.detach(|| slf.get().inner.begin_read()).pyerr()?;
        Ok(PyReadTransaction::new(slf, txn))
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<ConcurrencyMode>()?;
    // Each mode is a module attribute too: `redb.SingleWriter` is `redb.ConcurrencyMode.SingleWriter`
    let class = m.py().get_type::<ConcurrencyMode>();
    for mode in ConcurrencyMode::ALL {
        m.add(mode.name(), class.getattr(mode.name())?)?;
    }
    Ok(())
}
