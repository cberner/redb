use crate::error::{map_database_error, map_transaction_error};
use crate::transaction::PyWriteTransaction;
use pyo3::prelude::*;
use std::path::PathBuf;

#[pyclass(module = "redb", name = "Database", frozen)]
pub(crate) struct PyDatabase {
    inner: ::redb::Database,
}

#[pymethods]
impl PyDatabase {
    /// Create or open a redb database at the given filesystem path.
    ///
    /// If the file does not exist (or is empty) a new database is initialized.
    /// If the file is an existing redb database it is opened.
    #[classmethod]
    fn create(_cls: &Bound<'_, pyo3::types::PyType>, path: PathBuf) -> PyResult<Self> {
        let db = ::redb::Database::create(&path).map_err(map_database_error)?;
        Ok(Self { inner: db })
    }

    fn begin_write(slf: Py<Self>, py: Python<'_>) -> PyResult<PyWriteTransaction> {
        let txn = py
            .detach(|| slf.get().inner.begin_write())
            .map_err(map_transaction_error)?;
        Ok(PyWriteTransaction::new(slf, txn))
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    Ok(())
}
