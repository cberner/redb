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
use std::collections::VecDeque;
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
        let replaced = with_types!(self.key, self.value, K, V, {
            let (key, value) = (K::from_py(key)?, V::from_py(value)?);
            self.with_table::<K, V, _>(|table| {
                py_value::<V>(py, table.insert(K::lend(&key), V::lend(&value)).pyerr()?)
            })
        })?;
        self.txn.get().mutated(&self.name);
        Ok(replaced)
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
    ) -> PyResult<PyRangeIter> {
        let table = slf.get();
        // Converts the bounds now, so that a key of the wrong type fails at the call, and
        // keeps them as keys of the table's type: a bytearray could change between batches
        let (start, end) = with_type!(table.key, K, {
            let key = |bound: Option<&Bound<'_, PyAny>>| -> PyResult<Option<Py<PyAny>>> {
                bound
                    .map(|key| Ok(K::to_py(py, K::lend(&K::from_py(key)?))?.unbind()))
                    .transpose()
            };
            (key(start)?, key(end)?)
        });
        let mutations = table.txn.get().mutation_count(&table.name);
        Ok(PyRangeIter {
            source: Source::Live(Cursor {
                table: slf,
                mutations,
                start,
                start_inclusive: true,
                end,
                batch: FIRST_BATCH,
                buffer: VecDeque::new(),
                done: false,
            }),
            reverse,
        })
    }

    fn __iter__(slf: Py<Self>, py: Python<'_>) -> PyResult<PyRangeIter> {
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
    ) -> PyResult<PyRangeIter> {
        with_types!(self.key, self.value, K, V, {
            let start = start.map(K::from_py).transpose()?;
            let end = end.map(K::from_py).transpose()?;
            let range = self
                .table::<K, V>()
                .range_owned(limits::<K>(start.as_ref(), true, end.as_ref()))
                .pyerr()?;
            Ok(PyRangeIter {
                source: Source::Snapshot {
                    range: Box::new(range),
                    _db: self.db.clone_ref(py),
                },
                reverse,
            })
        })
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<PyRangeIter> {
        self.range(py, None, None, false)
    }
}

/// A range of a read-only table, streamed from the snapshot it keeps alive
trait Snapshot: Send + Sync {
    fn advance(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>>;
}

impl<K, V> Snapshot for OwnedRange<K, V>
where
    K: Key + PyValue + Send + Sync + 'static,
    V: PyValue + Send + Sync + 'static,
{
    fn advance(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>> {
        let entry = if reverse {
            self.next_back()
        } else {
            self.next()
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

// Batches start small, so a short scan converts little it will not use, and grow for long ones
const FIRST_BATCH: usize = 16;
const MAX_BATCH: usize = 256;

/// Where a range over a write transaction's table stands between batches.
///
/// The redb table is reopened for each operation, so a range over it cannot be held across
/// Python calls; each batch queries the table again, from the last key the previous one
/// returned. The batches all come from one state of the table, because the iterator stops
/// once the table has changed, as a dict's does.
struct Cursor {
    table: Py<PyTable>,
    // The table's mutation count when the iteration began
    mutations: u64,
    start: Option<Py<PyAny>>,
    start_inclusive: bool,
    end: Option<Py<PyAny>>,
    batch: usize,
    buffer: VecDeque<Pair>,
    // Whether the iteration has ended, which is when `next` first returns nothing
    done: bool,
}

impl Cursor {
    fn next(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>> {
        if self.done {
            return Ok(None); // which later changes to the table do not undo, as for a dict
        }
        let table = self.table.get();
        if table.txn.get().mutation_count(&table.name) != self.mutations {
            return Err(PyRuntimeError::new_err("table changed during iteration"));
        }
        if self.buffer.is_empty() {
            self.fill(py, reverse)?;
        }
        let entry = self.buffer.pop_front();
        self.done = entry.is_none();
        Ok(entry)
    }

    /// Queries the next batch of entries into the buffer
    fn fill(&mut self, py: Python<'_>, reverse: bool) -> PyResult<()> {
        let table = self.table.get();
        with_types!(table.key, table.value, K, V, {
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
                for _ in 0..self.batch {
                    let next = if reverse {
                        entries.next_back()
                    } else {
                        entries.next()
                    };
                    let Some(entry) = next else { break };
                    let (key, value) = entry.pyerr()?;
                    let key = K::to_py(py, key.value())?.unbind();
                    let value = V::to_py(py, value.value())?.unbind();
                    self.buffer.push_back((key.clone_ref(py), value));
                    if reverse {
                        self.end = Some(key);
                    } else {
                        self.start = Some(key);
                        self.start_inclusive = false;
                    }
                }
                Ok(())
            })?;
        });
        self.batch = (self.batch * 2).min(MAX_BATCH);
        Ok(())
    }
}

enum Source {
    /// A read transaction's snapshot, streamed
    Snapshot {
        range: Box<dyn Snapshot>,
        // Dropped after `range`, whose snapshot needs the Database's storage open
        _db: Py<PyDatabase>,
    },
    /// A write transaction's table, queried again per batch; its table keeps the Database
    Live(Cursor),
}

/// Iterates the entries of a range of a table
#[pyclass(module = "redb", name = "RangeIter")]
pub(crate) struct PyRangeIter {
    source: Source,
    reverse: bool,
}

#[pymethods]
impl PyRangeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Pair>> {
        match &mut self.source {
            Source::Snapshot { range, .. } => range.advance(py, self.reverse),
            Source::Live(cursor) => cursor.next(py, self.reverse),
        }
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTableDefinition>()?;
    m.add_class::<PyTable>()?;
    m.add_class::<PyReadOnlyTable>()?;
    m.add_class::<PyRangeIter>()?;
    Ok(())
}
