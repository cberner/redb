use crate::error::PyErrExt;
use crate::transaction::PyWriteTransaction;
use pyo3::prelude::*;
use std::path::PathBuf;

/// A database open for reading and writing
#[pyclass(module = "redb", name = "Database", frozen)]
pub(crate) struct PyDatabase {
    inner: ::redb::Database,
}

#[pymethods]
impl PyDatabase {
    /// Opens the database at `path`, creating it if the file does not exist or is empty
    #[staticmethod]
    fn create(py: Python<'_>, path: PathBuf) -> PyResult<Self> {
        let inner = py.detach(|| ::redb::Database::create(path)).pyerr()?;
        Ok(Self { inner })
    }

    /// Opens an existing database
    #[staticmethod]
    fn open(py: Python<'_>, path: PathBuf) -> PyResult<Self> {
        let inner = py.detach(|| ::redb::Database::open(path)).pyerr()?;
        Ok(Self { inner })
    }

    /// Begins a write transaction, waiting for any other writer to finish
    fn begin_write(slf: Py<Self>, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let txn = py.detach(|| slf.get().inner.begin_write()).pyerr()?;
        Ok(PyWriteTransaction::new(slf, txn))
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    Ok(())
}
