use crate::database::PyDatabase;
use crate::error::PyErrExt;
use crate::transaction::PyWriteTransaction;
use crate::types::{PyValue, Type, with_type, with_types};
use ::redb::{
    AccessGuard, Key, OwnedRange, ReadOnlyTable, ReadTransaction, ReadableTable,
    ReadableTableMetadata, Table, TableDefinition, Value, WriteTransaction,
};
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::any::Any;
use std::borrow::Borrow;
use std::ops::Bound as Limit;

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

// The operations a write transaction's table and a read-only table share, over redb's traits

fn lookup<K: Key + PyValue + 'static, V: PyValue + 'static>(
    py: Python<'_>,
    table: &impl ReadableTable<K, V>,
    key: &K::Owned,
) -> PyResult<Py<PyAny>> {
    py_value::<V>(py, table.get(K::lend(key)).pyerr()?)
}

fn length(table: &impl ReadableTableMetadata) -> PyResult<usize> {
    Ok(usize::try_from(table.len().pyerr()?).unwrap_or(usize::MAX))
}

/// A `(key, value)` entry as Python objects
type Pair = (Py<PyAny>, Py<PyAny>);

fn py_entry<K: PyValue, V: PyValue>(
    py: Python<'_>,
    entry: Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>,
) -> PyResult<Option<Pair>> {
    entry
        .map(|(key, value)| {
            Ok((
                K::to_py(py, key.value())?.unbind(),
                V::to_py(py, value.value())?.unbind(),
            ))
        })
        .transpose()
}

fn first_entry<K: Key + PyValue + 'static, V: PyValue + 'static>(
    py: Python<'_>,
    table: &impl ReadableTable<K, V>,
) -> PyResult<Option<Pair>> {
    py_entry::<K, V>(py, table.first().pyerr()?)
}

fn last_entry<K: Key + PyValue + 'static, V: PyValue + 'static>(
    py: Python<'_>,
    table: &impl ReadableTable<K, V>,
) -> PyResult<Option<Pair>> {
    py_entry::<K, V>(py, table.last().pyerr()?)
}

/// A key lent to redb as a range bound. The wrapper gives a tuple of bounds exactly one
/// `RangeBounds` impl, where bounds over a bare `&[u8]` would have two, which redb's range
/// methods cannot infer through.
struct Lent<T>(T);

impl<T> Borrow<T> for Lent<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

/// A Python range's bounds for redb: `start` inclusive and `end` exclusive, as in `range()`,
/// unless a batch already returned `start`
type Limits<T> = (Limit<Lent<T>>, Limit<Lent<T>>);

fn limits<'a, K: PyValue>(
    start: Option<&'a K::Owned>,
    start_inclusive: bool,
    end: Option<&'a K::Owned>,
) -> Limits<K::SelfType<'a>> {
    let lower = match start {
        None => Limit::Unbounded,
        Some(key) if start_inclusive => Limit::Included(Lent(K::lend(key))),
        Some(key) => Limit::Excluded(Lent(K::lend(key))),
    };
    let upper = end.map_or(Limit::Unbounded, |key| Limit::Excluded(Lent(K::lend(key))));
    (lower, upper)
}

/// A Python predicate for redb's `retain`, which cannot stop at a Python exception: once the
/// predicate has raised, it is not called again, every later entry is kept, and the exception
/// is raised when redb returns.
struct Predicate<'py> {
    callable: &'py Bound<'py, PyAny>,
    failure: Option<PyErr>,
}

impl<'py> Predicate<'py> {
    fn new(callable: &'py Bound<'py, PyAny>) -> Self {
        Self {
            callable,
            failure: None,
        }
    }

    fn test<K: PyValue, V: PyValue>(
        &mut self,
        key: K::SelfType<'_>,
        value: V::SelfType<'_>,
    ) -> bool {
        if self.failure.is_some() {
            return true;
        }
        let py = self.callable.py();
        let verdict = (|| {
            self.callable
                .call1((K::to_py(py, key)?, V::to_py(py, value)?))?
                .is_truthy()
        })();
        verdict.unwrap_or_else(|err| {
            self.failure = Some(err);
            true
        })
    }

    /// The exception the predicate raised, if it did, else `result`
    fn finish<R>(self, result: PyResult<R>) -> PyResult<R> {
        match self.failure {
            Some(err) => Err(err),
            None => result,
        }
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

    /// Opens the redb table for `f` to read, and closes it after
    fn with_table<K: Key + 'static, V: Value + 'static, R>(
        &self,
        f: impl for<'txn> FnOnce(&Table<'txn, K, V>) -> PyResult<R>,
    ) -> PyResult<R> {
        self.txn
            .get()
            .with_txn(|txn| f(&open::<K, V>(txn, &self.name)?))
    }

    /// Opens the redb table for `f` to change, and counts the change for the ranges over the
    /// table, whether or not `f` succeeded. A `&mut Table` exists only here, so no change goes
    /// uncounted.
    fn with_table_mut<K: Key + 'static, V: Value + 'static, R>(
        &self,
        f: impl for<'txn> FnOnce(&mut Table<'txn, K, V>) -> PyResult<R>,
    ) -> PyResult<R> {
        let txn = self.txn.get();
        let result = txn.with_txn(|txn| f(&mut open::<K, V>(txn, &self.name)?));
        txn.mutated(&self.name);
        result
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
            self.with_table::<K, V, _>(|table| length(table))
        })
    }

    /// The value at `key`, or None
    fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        with_types!(self.key, self.value, K, V, {
            let key = K::from_py(key)?;
            self.with_table::<K, V, _>(|table| lookup::<K, V>(py, table, &key))
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

    /// The entry with the smallest key, as a `(key, value)` pair, or None if the table is empty
    fn first(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.key, self.value, K, V, {
            self.with_table::<K, V, _>(|table| first_entry::<K, V>(py, table))
        })
    }

    /// The entry with the largest key, as a `(key, value)` pair, or None if the table is empty
    fn last(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.key, self.value, K, V, {
            self.with_table::<K, V, _>(|table| last_entry::<K, V>(py, table))
        })
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
            self.with_table_mut::<K, V, _>(|table| {
                py_value::<V>(py, table.insert(K::lend(&key), V::lend(&value)).pyerr()?)
            })
        })
    }

    /// Removes `key`; returns the value it had, or None
    fn remove(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        with_types!(self.key, self.value, K, V, {
            let key = K::from_py(key)?;
            self.with_table_mut::<K, V, _>(|table| {
                py_value::<V>(py, table.remove(K::lend(&key)).pyerr()?)
            })
        })
    }

    /// Removes and returns the entry with the smallest key, or None if the table is empty
    fn pop_first(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.key, self.value, K, V, {
            self.with_table_mut::<K, V, _>(|table| py_entry::<K, V>(py, table.pop_first().pyerr()?))
        })
    }

    /// Removes and returns the entry with the largest key, or None if the table is empty
    fn pop_last(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.key, self.value, K, V, {
            self.with_table_mut::<K, V, _>(|table| py_entry::<K, V>(py, table.pop_last().pyerr()?))
        })
    }

    /// Removes the entries for which `predicate(key, value)` is false. If the predicate raises,
    /// the entries it had rejected are gone, the rest stay, and the exception propagates.
    fn retain(&self, predicate: &Bound<'_, PyAny>) -> PyResult<()> {
        let mut predicate = Predicate::new(predicate);
        with_types!(self.key, self.value, K, V, {
            self.with_table_mut::<K, V, _>(|table| {
                let result = table.retain(|key, value| predicate.test::<K, V>(key, value));
                predicate.finish(result.pyerr())
            })
        })
    }

    /// Iterates the `(key, value)` entries with `start <= key < end` in key order, or in
    /// reverse; either bound may be None. Changing the table while iterating is an error, as
    /// it is for a dict.
    #[pyo3(signature = (start=None, end=None, *, reverse=false))]
    fn range(
        slf: Py<Self>,
        py: Python<'_>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        reverse: bool,
    ) -> PyResult<PyRange> {
        let table = slf.get();
        // Converted now, so a wrong type fails at the call and a bytearray cannot change later
        let (start, end) = with_type!(table.key, K, {
            let key = |bound: Option<&Bound<'_, PyAny>>| -> PyResult<Option<Py<PyAny>>> {
                bound
                    .map(|key| Ok(K::to_py(py, K::lend(&K::from_py(key)?))?.unbind()))
                    .transpose()
            };
            (key(start)?, key(end)?)
        });
        let mutations = table.txn.get().mutation_count(&table.name);
        Ok(PyRange {
            source: Source::Write(WriteRange {
                table: slf,
                mutations,
                start,
                start_inclusive: true,
                end,
                done: false,
            }),
            reverse,
        })
    }

    fn __iter__(slf: Py<Self>, py: Python<'_>) -> PyResult<PyRange> {
        Self::range(slf, py, None, None, false)
    }

    fn __setitem__(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.insert(py, key, value).map(drop)
    }

    fn __delitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.remove(py, key)?.is_none(py) {
            return Err(PyKeyError::new_err(key.clone().unbind()));
        }
        Ok(())
    }
}

/// A table of a read transaction's snapshot, which it keeps alive
#[pyclass(module = "redb", name = "ReadOnlyTable", frozen)]
pub(crate) struct PyReadOnlyTable {
    name: String,
    key: Type,
    value: Type,
    table: Box<dyn Any + Send + Sync>,
    // Dropped after `table`, whose snapshot needs the Database's storage open
    db: Py<PyDatabase>,
}

impl PyReadOnlyTable {
    pub(crate) fn new(
        txn: &ReadTransaction,
        db: Py<PyDatabase>,
        definition: &PyTableDefinition,
    ) -> PyResult<Self> {
        let table: Box<dyn Any + Send + Sync> =
            with_types!(definition.key, definition.value, K, V, {
                Box::new(
                    txn.open_table(TableDefinition::<K, V>::new(&definition.name))
                        .pyerr()?,
                )
            });
        Ok(Self {
            name: definition.name.clone(),
            key: definition.key,
            value: definition.value,
            table,
            db,
        })
    }

    /// The redb table, as the types its definition dispatched to
    fn table<K: Key + 'static, V: Value + 'static>(&self) -> &ReadOnlyTable<K, V> {
        self.table
            .downcast_ref()
            .expect("opened with its definition's types")
    }
}

#[pymethods]
impl PyReadOnlyTable {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    fn __len__(&self) -> PyResult<usize> {
        with_types!(self.key, self.value, K, V, { length(self.table::<K, V>()) })
    }

    /// The value at `key`, or None
    fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        with_types!(self.key, self.value, K, V, {
            lookup::<K, V>(py, self.table::<K, V>(), &K::from_py(key)?)
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

    /// The entry with the smallest key, as a `(key, value)` pair, or None if the table is empty
    fn first(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.key, self.value, K, V, {
            first_entry::<K, V>(py, self.table::<K, V>())
        })
    }

    /// The entry with the largest key, as a `(key, value)` pair, or None if the table is empty
    fn last(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.key, self.value, K, V, {
            last_entry::<K, V>(py, self.table::<K, V>())
        })
    }

    /// Iterates the `(key, value)` entries with `start <= key < end` in key order, or in
    /// reverse; either bound may be None
    #[pyo3(signature = (start=None, end=None, *, reverse=false))]
    fn range(
        &self,
        py: Python<'_>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        reverse: bool,
    ) -> PyResult<PyRange> {
        with_types!(self.key, self.value, K, V, {
            let start = start.map(K::from_py).transpose()?;
            let end = end.map(K::from_py).transpose()?;
            let range = self
                .table::<K, V>()
                .range_owned(limits::<K>(start.as_ref(), true, end.as_ref()))
                .pyerr()?;
            Ok(PyRange {
                source: Source::Read {
                    range: Box::new(range),
                    _db: self.db.clone_ref(py),
                },
                reverse,
            })
        })
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<PyRange> {
        self.range(py, None, None, false)
    }
}

/// A range of a read transaction's table, with redb's key and value types hidden
trait ReadRange: Send + Sync {
    fn next(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>>;
}

impl<K, V> ReadRange for OwnedRange<K, V>
where
    K: Key + PyValue + Send + Sync + 'static,
    V: PyValue + Send + Sync + 'static,
{
    fn next(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>> {
        let entry = if reverse {
            self.next_back()
        } else {
            Iterator::next(self)
        };
        entry
            .map(|entry| {
                let (key, value) = entry.pyerr()?;
                Ok((
                    K::to_py(py, key.value())?.unbind(),
                    V::to_py(py, value.value())?.unbind(),
                ))
            })
            .transpose()
    }
}

/// A range of a write transaction's table, which is reopened and sought again at each call;
/// a change to the table ends it, as for a dict
struct WriteRange {
    table: Py<PyTable>,
    // The table's change count when the iteration began
    mutations: u64,
    // The bounds still to cover; `start` (or `end`, in reverse) moves past each key returned
    start: Option<Py<PyAny>>,
    start_inclusive: bool,
    end: Option<Py<PyAny>>,
    done: bool,
}

impl WriteRange {
    fn next(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>> {
        if self.done {
            return Ok(None); // and stays exhausted, as a dict's iterator does
        }
        // A changed table ends the iteration, as a dict's does
        let table = self.table.get();
        if table.txn.get().mutation_count(&table.name) != self.mutations {
            return Err(PyRuntimeError::new_err("table changed during iteration"));
        }
        let entry = with_types!(table.key, table.value, K, V, {
            let start = self
                .start
                .as_ref()
                .map(|key| K::from_py(key.bind(py)))
                .transpose()?;
            let end = self
                .end
                .as_ref()
                .map(|key| K::from_py(key.bind(py)))
                .transpose()?;
            table.with_table::<K, V, _>(|redb_table| {
                let bounds = limits::<K>(start.as_ref(), self.start_inclusive, end.as_ref());
                let mut entries = redb_table.range(bounds).pyerr()?;
                let entry = if reverse {
                    entries.next_back()
                } else {
                    entries.next()
                };
                py_entry::<K, V>(py, entry.transpose().pyerr()?)
            })
        })?;
        match &entry {
            Some((key, _)) if reverse => self.end = Some(key.clone_ref(py)),
            Some((key, _)) => {
                self.start = Some(key.clone_ref(py));
                self.start_inclusive = false;
            }
            None => self.done = true,
        }
        Ok(entry)
    }
}

/// Where the entries come from; only a read-only table has an owned range in redb
enum Source {
    Read {
        range: Box<dyn ReadRange>,
        // Dropped after `range`, whose snapshot needs the Database's storage open
        _db: Py<PyDatabase>,
    },
    Write(WriteRange),
}

/// Iterates the entries of a range of a table
#[pyclass(module = "redb", name = "Range")]
pub(crate) struct PyRange {
    source: Source,
    reverse: bool,
}

#[pymethods]
impl PyRange {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Pair>> {
        match &mut self.source {
            Source::Read { range, .. } => range.next(py, self.reverse),
            Source::Write(range) => range.next(py, self.reverse),
        }
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTableDefinition>()?;
    m.add_class::<PyTable>()?;
    m.add_class::<PyReadOnlyTable>()?;
    m.add_class::<PyRange>()?;
    Ok(())
}
