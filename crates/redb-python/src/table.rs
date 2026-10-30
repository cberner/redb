use crate::error::PyErrExt;
use crate::transaction::PyWriteTransaction;
use crate::types::{PyValue, Type, with_types};
use ::redb::{
    AccessGuard, Key, ReadableTable, ReadableTableMetadata, Table, TableDefinition, Value,
    WriteTransaction,
};
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

/// The name and types of a table
#[pyclass(module = "redb", name = "TableDefinition", frozen)]
pub(crate) struct PyTableDefinition {
    pub(crate) name: String,
    pub(crate) key: Type,
    pub(crate) value: Type,
}

#[pymethods]
impl PyTableDefinition {
    #[new]
    fn new(
        name: String,
        key_type: &Bound<'_, PyAny>,
        value_type: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("table name must not be empty"));
        }
        Ok(Self {
            name,
            key: Type::from_py(key_type)?,
            value: Type::from_py(value_type)?,
        })
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn key_type(&self) -> Type {
        self.key
    }

    #[getter]
    fn value_type(&self) -> Type {
        self.value
    }

    fn __repr__(&self) -> String {
        format!(
            "TableDefinition({:?}, redb.{}, redb.{})",
            self.name,
            self.key.name(),
            self.value.name()
        )
    }
}

/// Opens the redb table `name` with the types its definition dispatched to
fn open<'txn, K: Key + 'static, V: Value + 'static>(
    txn: &'txn WriteTransaction,
    name: &str,
) -> PyResult<Table<'txn, K, V>> {
    txn.open_table(TableDefinition::new(name)).pyerr()
}

/// The value a redb lookup found, as a Python object, or None
fn py_value<V: PyValue>(py: Python<'_>, found: Option<AccessGuard<'_, V>>) -> PyResult<Py<PyAny>> {
    match found {
        Some(value) => Ok(V::to_py(py, value.value())?.unbind()),
        None => Ok(py.None()),
    }
}

/// A write transaction's handle on one of its tables.
///
/// Each operation opens the redb table, runs, and closes it again, so this is only the name's
/// binding to the table: dropping it does nothing.
#[pyclass(module = "redb", name = "Table", frozen)]
pub(crate) struct PyTable {
    txn: Py<PyWriteTransaction>,
    name: String,
    key: Type,
    value: Type,
}

impl PyTable {
    pub(crate) fn new(
        txn: Py<PyWriteTransaction>,
        definition: &PyTableDefinition,
    ) -> PyResult<Self> {
        let table = Self {
            txn,
            name: definition.name.clone(),
            key: definition.key,
            value: definition.value,
        };
        // Creates the table if it does not exist, and checks the types of one that does
        with_types!(table.key, table.value, K, V, {
            table.with_table::<K, V, _>(|_| Ok(()))?;
        });
        Ok(table)
    }

    /// Opens the redb table for `f`, and closes it after
    fn with_table<K: Key + 'static, V: Value + 'static, R>(
        &self,
        f: impl for<'txn> FnOnce(&mut Table<'txn, K, V>) -> PyResult<R>,
    ) -> PyResult<R> {
        self.txn
            .get()
            .with_txn(|txn| f(&mut open::<K, V>(txn, &self.name)?))
    }
}

#[pymethods]
impl PyTable {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    fn __len__(&self) -> PyResult<usize> {
        with_types!(self.key, self.value, K, V, {
            self.with_table::<K, V, _>(|table| {
                Ok(usize::try_from(table.len().pyerr()?).unwrap_or(usize::MAX))
            })
        })
    }

    /// The value at `key`, or None
    fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        with_types!(self.key, self.value, K, V, {
            let key = K::from_py(key)?;
            self.with_table::<K, V, _>(|table| py_value::<V>(py, table.get(K::lend(&key)).pyerr()?))
        })
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let value = self.get(py, key)?;
        if value.is_none(py) {
            return Err(PyKeyError::new_err(key.clone().unbind()));
        }
        Ok(value)
    }

    fn __contains__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(!self.get(py, key)?.is_none(py))
    }

    /// Stores `value` at `key`; returns the value it replaced, or None
    fn insert(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        with_types!(self.key, self.value, K, V, {
            let (key, value) = (K::from_py(key)?, V::from_py(value)?);
            self.with_table::<K, V, _>(|table| {
                py_value::<V>(py, table.insert(K::lend(&key), V::lend(&value)).pyerr()?)
            })
        })
    }

    fn __setitem__(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.insert(py, key, value).map(drop)
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTableDefinition>()?;
    m.add_class::<PyTable>()?;
    Ok(())
}
