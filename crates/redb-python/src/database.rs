use crate::error::map_database_error;
use pyo3::prelude::*;
use std::path::PathBuf;

#[pyclass(module = "redb", name = "Database", frozen)]
pub(crate) struct PyDatabase {
    _inner: ::redb::Database,
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
        Ok(Self { _inner: db })
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    Ok(())
}
