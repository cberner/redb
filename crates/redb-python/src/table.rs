use crate::error::{PyErrExt, error};
use crate::multimap::PyMultimapTable;
use crate::transaction::{Cached, Open, PyWriteTransaction, in_use};
use crate::types::{PyValue, Type, check_key, with_types};
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use redb::{
    AccessGuard, Key, OwnedRange, ReadOnlyTable, ReadableTable, ReadableTableMetadata, Table,
    TableDefinition, Value, WriteTransaction,
};
use std::any::Any;
use std::collections::VecDeque;
use std::ops::{Bound as Limit, RangeBounds};
use std::sync::MutexGuard;

/// A table's name and types, shared by the normal and multimap definitions
#[derive(Clone)]
pub(crate) struct Definition {
    pub(crate) name: String,
    pub(crate) key: Type,
    pub(crate) value: Type,
}

impl Definition {
    pub(crate) fn new(
        name: String,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        multimap: bool,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("table name must not be empty"));
        }
        let key = Type::from_py(key)?;
        let value = Type::from_py(value)?;
        if !key.is_key() {
            return Err(PyTypeError::new_err(format!(
                "{key:?} cannot be a key type"
            )));
        }
        if multimap && !value.is_key() {
            return Err(PyTypeError::new_err(format!(
                "{value:?} cannot be a multimap value type"
            )));
        }
        Ok(Self { name, key, value })
    }
}

macro_rules! definition_class {
    ($class:ident, $name:literal, $multimap:expr, $doc:literal) => {
        #[doc = $doc]
        #[pyclass(module = "redb", name = $name, frozen, from_py_object)]
        #[derive(Clone)]
        pub(crate) struct $class {
            pub(crate) def: $crate::table::Definition,
        }

        #[pymethods]
        impl $class {
            #[new]
            fn new(
                name: String,
                key_type: &Bound<'_, PyAny>,
                value_type: &Bound<'_, PyAny>,
            ) -> PyResult<Self> {
                Ok(Self {
                    def: $crate::table::Definition::new(name, key_type, value_type, $multimap)?,
                })
            }

            #[getter]
            fn name(&self) -> &str {
                &self.def.name
            }

            #[getter]
            fn key_type(&self) -> $crate::types::Type {
                self.def.key
            }

            #[getter]
            fn value_type(&self) -> $crate::types::Type {
                self.def.value
            }

            fn __repr__(&self) -> String {
                format!(
                    "{}({:?}, redb.{:?}, redb.{:?})",
                    $name, self.def.name, self.def.key, self.def.value
                )
            }
        }
    };
}

pub(crate) use definition_class;

definition_class!(
    PyTableDefinition,
    "TableDefinition",
    false,
    "The name and types of a table"
);

/// A write transaction's handle on one of its tables: the transaction caches the open redb
/// table, so this is only the name's binding to it
pub(crate) struct WriteHandle {
    txn: Py<PyWriteTransaction>,
    generation: u64,
}

impl WriteHandle {
    pub(crate) fn new(txn: Py<PyWriteTransaction>) -> Self {
        let generation = txn.get().generation();
        Self { txn, generation }
    }

    /// The cached table `name`
    pub(crate) fn cached<T: Any + Send + Sync>(
        &self,
        name: &str,
        open: impl FnOnce(&'static WriteTransaction) -> PyResult<T>,
    ) -> PyResult<Cached> {
        if self.txn.get().generation() != self.generation {
            return Err(error(
                "TableDoesNotExist",
                format!("table {name:?} was deleted, renamed or rolled back; open it again"),
            ));
        }
        self.txn.get().cached(name, open)
    }

    /// Like `cached`, for an operation that changes the table
    pub(crate) fn cached_mut<T: Any + Send + Sync>(
        &self,
        name: &str,
        open: impl FnOnce(&'static WriteTransaction) -> PyResult<T>,
    ) -> PyResult<Cached> {
        let cached = self.cached(name, open)?;
        self.txn.get().mutated(name);
        Ok(cached)
    }

    /// How many times the transaction has changed the table
    pub(crate) fn mutation_count(&self, name: &str) -> u64 {
        self.txn.get().mutation_count(name)
    }
}

/// Locks a cached table for one operation
pub(crate) fn lock<'a>(cached: &'a Cached, name: &str) -> PyResult<MutexGuard<'a, Open>> {
    cached.try_lock().map_err(|_| in_use(name))
}

/// Binds `$table` to the cached redb table of `$self`, a write-side table object, for a
/// mutation (`&mut`) or a read (`&`)
macro_rules! cached {
    ($self:expr, &mut $type:ty, $open:expr, |$table:ident| $body:expr) => {{
        let cached = $self
            .handle
            .cached_mut(&$self.def.name, |txn| $open(txn, &$self.def.name))?;
        let mut guard = lock(&cached, &$self.def.name)?;
        let $table: &mut $type = guard
            .table
            .downcast_mut()
            .expect("table matches its definition");
        $body
    }};
    ($self:expr, & $type:ty, $open:expr, |$table:ident| $body:expr) => {{
        let cached = $self
            .handle
            .cached(&$self.def.name, |txn| $open(txn, &$self.def.name))?;
        let guard = lock(&cached, &$self.def.name)?;
        let $table: &$type = guard
            .table
            .downcast_ref()
            .expect("table matches its definition");
        $body
    }};
}

/// Binds `$table` to the redb table that `$self`, a read-only table object, owns
macro_rules! owned {
    ($self:expr, $type:ty, |$table:ident| $body:expr) => {{
        let $table: &$type = $self
            .table
            .downcast_ref()
            .expect("table matches its definition");
        $body
    }};
}

pub(crate) use {cached, owned};

macro_rules! live_table {
    ($self:expr, $K:ident, $V:ident, |$table:ident| $body:expr) => {
        cached!($self, &Table<'static, $K, $V>, open::<$K, $V>, |$table| {
            $body
        })
    };
}

macro_rules! live_table_mut {
    ($self:expr, $K:ident, $V:ident, |$table:ident| $body:expr) => {
        cached!(
            $self,
            &mut Table<'static, $K, $V>,
            open::<$K, $V>,
            |$table| $body
        )
    };
}

macro_rules! snapshot_table {
    ($self:expr, $K:ident, $V:ident, |$table:ident| $body:expr) => {
        owned!($self, ReadOnlyTable<$K, $V>, |$table| $body)
    };
}

pub(crate) fn open<'txn, K: Key + 'static, V: Value + 'static>(
    txn: &'txn WriteTransaction,
    name: &str,
) -> PyResult<Table<'txn, K, V>> {
    txn.open_table(TableDefinition::new(name)).pyerr()
}

pub(crate) type Pair = (Py<PyAny>, Py<PyAny>);

fn py_value<V: PyValue>(py: Python<'_>, guard: Option<AccessGuard<'_, V>>) -> PyResult<Py<PyAny>> {
    match guard {
        Some(value) => Ok(V::to_py(py, value.value())?.unbind()),
        None => Ok(py.None()),
    }
}

fn py_pair<K: PyValue, V: PyValue>(
    py: Python<'_>,
    key: &AccessGuard<'_, K>,
    value: &AccessGuard<'_, V>,
) -> PyResult<Pair> {
    Ok((
        K::to_py(py, key.value())?.unbind(),
        V::to_py(py, value.value())?.unbind(),
    ))
}

fn py_entry<K: PyValue, V: PyValue>(
    py: Python<'_>,
    entry: Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>,
) -> PyResult<Option<Pair>> {
    entry
        .map(|(key, value)| py_pair::<K, V>(py, &key, &value))
        .transpose()
}

/// The optional start and end keys of a range, in their owned forms
pub(crate) type Bounds<K> = (Option<<K as PyValue>::Owned>, Option<<K as PyValue>::Owned>);

pub(crate) fn extract_limits<K: PyValue>(
    start: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bounds<K>> {
    Ok((
        start.map(K::from_py).transpose()?,
        end.map(K::from_py).transpose()?,
    ))
}

/// A pair of bounds over one key type. Unlike a tuple of `Bound`s it implements `RangeBounds`
/// for exactly one type, so redb's range methods can infer it.
pub(crate) struct Limits<T>(Limit<T>, Limit<T>);

impl<T> RangeBounds<T> for Limits<T> {
    fn start_bound(&self) -> Limit<&T> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> Limit<&T> {
        self.1.as_ref()
    }
}

/// The redb bounds of a Python range: `start` is inclusive, `end` exclusive, unless a batch
/// has already consumed `start`
pub(crate) fn limits<'a, K: PyValue>(
    start: Option<&'a K::Owned>,
    start_inclusive: bool,
    end: Option<&'a K::Owned>,
) -> Limits<K::SelfType<'a>> {
    let lower = match start {
        None => Limit::Unbounded,
        Some(key) if start_inclusive => Limit::Included(K::lend(key)),
        Some(key) => Limit::Excluded(K::lend(key)),
    };
    let upper = end.map_or(Limit::Unbounded, |key| Limit::Excluded(K::lend(key)));
    Limits(lower, upper)
}

/// A read transaction's range, streamed from the snapshot it keeps alive
pub(crate) trait Snapshot: Send + Sync {
    fn advance(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>>;
}

impl<K: PyValue + Key + Send + Sync, V: PyValue + Send + Sync> Snapshot for OwnedRange<K, V> {
    fn advance(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>> {
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

// Batches start small, so a short scan converts little it will not use, and grow for long ones
const FIRST_BATCH: usize = 16;
const MAX_BATCH: usize = 256;

/// Where a write transaction's range iterator stands between batches.
///
/// A write-side range borrows its table, so it cannot be held across Python calls; instead
/// each batch queries the table again from the last key returned. The batches come from one
/// state of the table because the iterator refuses to go on once its transaction has changed
/// the table, as a dict iterator does.
pub(crate) struct Cursor {
    start: Option<Py<PyAny>>,
    start_inclusive: bool,
    end: Option<Py<PyAny>>,
    reverse: bool,
    batch: usize,
    buffer: VecDeque<Pair>,
    done: bool,
}

impl Cursor {
    pub(crate) fn bounds<K: PyValue>(&self, py: Python<'_>) -> PyResult<Bounds<K>> {
        extract_limits::<K>(
            self.start.as_ref().map(|key| key.bind(py)),
            self.end.as_ref().map(|key| key.bind(py)),
        )
    }

    pub(crate) fn start_inclusive(&self) -> bool {
        self.start_inclusive
    }

    /// Takes one batch from `entries`, converting each value with `convert`
    pub(crate) fn drain<'a, K: PyValue, T>(
        &mut self,
        py: Python<'_>,
        mut entries: impl DoubleEndedIterator<Item = redb::Result<(AccessGuard<'a, K>, T)>>,
        mut convert: impl FnMut(T) -> PyResult<Py<PyAny>>,
    ) -> PyResult<()> {
        for _ in 0..self.batch {
            let next = if self.reverse {
                entries.next_back()
            } else {
                entries.next()
            };
            let Some(entry) = next else {
                self.done = true;
                return Ok(());
            };
            let (key, value) = entry.pyerr()?;
            let key = K::to_py(py, key.value())?.unbind();
            self.buffer.push_back((key.clone_ref(py), convert(value)?));
            if self.reverse {
                self.end = Some(key);
            } else {
                self.start = Some(key);
                self.start_inclusive = false;
            }
        }
        self.batch = (self.batch * 2).min(MAX_BATCH);
        Ok(())
    }
}

/// A write-side table an iterator queries
pub(crate) enum Source {
    Table(Py<PyTable>),
    Multimap(Py<PyMultimapTable>),
}

impl Source {
    fn mutation_count(&self) -> u64 {
        match self {
            Source::Table(table) => table.get().mutation_count(),
            Source::Multimap(table) => table.get().mutation_count(),
        }
    }
}

enum Iter {
    Snapshot {
        range: Box<dyn Snapshot>,
        reverse: bool,
    },
    Live {
        source: Source,
        mutations: u64,
        cursor: Cursor,
    },
}

/// Iterates the entries of a range of a table
#[pyclass(module = "redb", name = "RangeIter")]
pub(crate) struct PyRangeIter {
    iter: Iter,
}

impl PyRangeIter {
    pub(crate) fn snapshot(range: Box<dyn Snapshot>, reverse: bool) -> Self {
        Self {
            iter: Iter::Snapshot { range, reverse },
        }
    }

    pub(crate) fn live(
        source: Source,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        reverse: bool,
    ) -> Self {
        Self {
            iter: Iter::Live {
                mutations: source.mutation_count(),
                source,
                cursor: Cursor {
                    start: start.map(|key| key.clone().unbind()),
                    start_inclusive: true,
                    end: end.map(|key| key.clone().unbind()),
                    reverse,
                    batch: FIRST_BATCH,
                    buffer: VecDeque::new(),
                    done: false,
                },
            },
        }
    }
}

#[pymethods]
impl PyRangeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Pair>> {
        match &mut self.iter {
            Iter::Snapshot { range, reverse } => range.advance(py, *reverse),
            Iter::Live {
                source,
                mutations,
                cursor,
            } => {
                if source.mutation_count() != *mutations {
                    return Err(PyRuntimeError::new_err("table changed during iteration"));
                }
                if cursor.buffer.is_empty() && !cursor.done {
                    match source {
                        Source::Table(table) => table.get().fill(py, cursor)?,
                        Source::Multimap(table) => table.get().fill(py, cursor)?,
                    }
                }
                Ok(cursor.buffer.pop_front())
            }
        }
    }
}

/// Calls a Python predicate from inside a redb callback. The first exception is kept for the
/// caller to raise, and `fallback` answers for the rest of the pass.
pub(crate) fn call_predicate<K: PyValue, V: PyValue>(
    py: Python<'_>,
    predicate: &Bound<'_, PyAny>,
    key: K::SelfType<'_>,
    value: V::SelfType<'_>,
    err: &mut Option<PyErr>,
    fallback: bool,
) -> bool {
    if err.is_some() {
        return fallback;
    }
    let result = K::to_py(py, key)
        .and_then(|key| Ok((key, V::to_py(py, value)?)))
        .and_then(|args| predicate.call1(args))
        .and_then(|verdict| verdict.is_truthy());
    result.unwrap_or_else(|e| {
        *err = Some(e);
        fallback
    })
}

#[pyclass(module = "redb", name = "TableStats", frozen, get_all)]
pub(crate) struct PyTableStats {
    tree_height: u32,
    leaf_pages: u64,
    branch_pages: u64,
    stored_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
}

impl From<redb::TableStats> for PyTableStats {
    fn from(stats: redb::TableStats) -> Self {
        Self {
            tree_height: stats.tree_height(),
            leaf_pages: stats.leaf_pages(),
            branch_pages: stats.branch_pages(),
            stored_bytes: stats.stored_bytes(),
            metadata_bytes: stats.metadata_bytes(),
            fragmented_bytes: stats.fragmented_bytes(),
        }
    }
}

/// The methods a write-side and a read-only table share; `$access!` binds the typed redb table
macro_rules! table_methods {
    ($class:ident, $access:ident, { $($extra:tt)* }) => {
        #[pymethods]
        impl $class {
            #[getter]
            fn name(&self) -> &str {
                &self.def.name
            }

            /// The value stored under `key`, or None
            fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                with_types!(self.def.key, self.def.value, K, V, {
                    let key = K::from_py(key)?;
                    $access!(self, K, V, |table| py_value::<V>(
                        py,
                        table.get(K::lend(&key)).pyerr()?
                    ))
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

            fn __len__(&self) -> PyResult<usize> {
                with_types!(self.def.key, self.def.value, K, V, {
                    $access!(self, K, V, |table| Ok(usize::try_from(table.len().pyerr()?)
                        .unwrap_or(usize::MAX)))
                })
            }

            /// The first entry in key order, or None
            fn first(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
                with_types!(self.def.key, self.def.value, K, V, {
                    $access!(self, K, V, |table| py_entry::<K, V>(py, table.first().pyerr()?))
                })
            }

            fn last(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
                with_types!(self.def.key, self.def.value, K, V, {
                    $access!(self, K, V, |table| py_entry::<K, V>(py, table.last().pyerr()?))
                })
            }

            fn stats(&self) -> PyResult<PyTableStats> {
                with_types!(self.def.key, self.def.value, K, V, {
                    $access!(self, K, V, |table| Ok(table.stats().pyerr()?.into()))
                })
            }

            $($extra)*
        }
    };
}

/// A table of a write transaction. Changing the table while iterating over it raises
/// `RuntimeError`, as for a dict.
#[pyclass(module = "redb", name = "Table", frozen)]
pub(crate) struct PyTable {
    def: Definition,
    handle: WriteHandle,
}

impl PyTable {
    pub(crate) fn new(txn: Py<PyWriteTransaction>, def: Definition) -> Self {
        Self {
            def,
            handle: WriteHandle::new(txn),
        }
    }

    pub(crate) fn mutation_count(&self) -> u64 {
        self.handle.mutation_count(&self.def.name)
    }

    pub(crate) fn fill(&self, py: Python<'_>, cursor: &mut Cursor) -> PyResult<()> {
        with_types!(self.def.key, self.def.value, K, V, {
            let (start, end) = cursor.bounds::<K>(py)?;
            live_table!(self, K, V, |table| {
                let range = table
                    .range(limits::<K>(
                        start.as_ref(),
                        cursor.start_inclusive(),
                        end.as_ref(),
                    ))
                    .pyerr()?;
                cursor.drain::<K, _>(py, range, |value: AccessGuard<'_, V>| {
                    Ok(V::to_py(py, value.value())?.unbind())
                })
            })
        })
    }
}

table_methods!(PyTable, live_table, {
    /// Iterates the `(key, value)` entries with `start <= key < end`, in key order
    #[pyo3(signature = (start=None, end=None, *, reverse=false))]
    fn range(
        slf: Py<Self>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        reverse: bool,
    ) -> PyResult<PyRangeIter> {
        let key_type = slf.get().def.key;
        check_key(key_type, start)?;
        check_key(key_type, end)?;
        Ok(PyRangeIter::live(Source::Table(slf), start, end, reverse))
    }

    fn __iter__(slf: Py<Self>) -> PyRangeIter {
        PyRangeIter::live(Source::Table(slf), None, None, false)
    }

    /// Stores `value` under `key`; returns the value it replaced, or None
    fn insert(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        with_types!(self.def.key, self.def.value, K, V, {
            let (key, value) = (K::from_py(key)?, V::from_py(value)?);
            live_table_mut!(self, K, V, |table| py_value::<V>(
                py,
                table.insert(K::lend(&key), V::lend(&value)).pyerr()?
            ))
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

    /// Removes `key`; returns the value it had, or None
    fn remove(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        with_types!(self.def.key, self.def.value, K, V, {
            let key = K::from_py(key)?;
            live_table_mut!(self, K, V, |table| py_value::<V>(
                py,
                table.remove(K::lend(&key)).pyerr()?
            ))
        })
    }

    fn __delitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.remove(py, key)?.is_none(py) {
            return Err(PyKeyError::new_err(key.clone().unbind()));
        }
        Ok(())
    }

    /// Removes and returns the first entry, or None
    fn pop_first(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.def.key, self.def.value, K, V, {
            live_table_mut!(self, K, V, |table| py_entry::<K, V>(
                py,
                table.pop_first().pyerr()?
            ))
        })
    }

    fn pop_last(&self, py: Python<'_>) -> PyResult<Option<Pair>> {
        with_types!(self.def.key, self.def.value, K, V, {
            live_table_mut!(self, K, V, |table| py_entry::<K, V>(
                py,
                table.pop_last().pyerr()?
            ))
        })
    }

    /// Keeps the entries for which `predicate(key, value)` is true
    fn retain(&self, py: Python<'_>, predicate: &Bound<'_, PyAny>) -> PyResult<()> {
        self.retain_in(py, predicate, None, None)
    }

    /// Like `retain`, over the entries with `start <= key < end`
    #[pyo3(signature = (predicate, start=None, end=None))]
    fn retain_in(
        &self,
        py: Python<'_>,
        predicate: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        with_types!(self.def.key, self.def.value, K, V, {
            let (start, end) = extract_limits::<K>(start, end)?;
            live_table_mut!(self, K, V, |table| {
                let mut err = None;
                table
                    .retain_in(
                        limits::<K>(start.as_ref(), true, end.as_ref()),
                        |key, value| {
                            call_predicate::<K, V>(py, predicate, key, value, &mut err, true)
                        },
                    )
                    .pyerr()?;
                err.map_or(Ok(()), Err)
            })
        })
    }

    /// Removes and returns the entries for which `predicate(key, value)` is true
    fn extract_if(&self, py: Python<'_>, predicate: &Bound<'_, PyAny>) -> PyResult<Vec<Pair>> {
        self.extract_from_if(py, predicate, None, None)
    }

    /// Like `extract_if`, over the entries with `start <= key < end`
    #[pyo3(signature = (predicate, start=None, end=None))]
    fn extract_from_if(
        &self,
        py: Python<'_>,
        predicate: &Bound<'_, PyAny>,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<Pair>> {
        with_types!(self.def.key, self.def.value, K, V, {
            let (start, end) = extract_limits::<K>(start, end)?;
            live_table_mut!(self, K, V, |table| {
                let mut err = None;
                let mut removed = Vec::new();
                let entries = table
                    .extract_from_if(
                        limits::<K>(start.as_ref(), true, end.as_ref()),
                        |key, value| {
                            call_predicate::<K, V>(py, predicate, key, value, &mut err, false)
                        },
                    )
                    .pyerr()?;
                for entry in entries {
                    let (key, value) = entry.pyerr()?;
                    removed.push(py_pair::<K, V>(py, &key, &value)?);
                }
                err.map_or(Ok(removed), Err)
            })
        })
    }
});

/// A table of a read transaction, which it keeps alive
#[pyclass(module = "redb", name = "ReadOnlyTable", frozen)]
pub(crate) struct PyReadOnlyTable {
    def: Definition,
    table: Box<dyn Any + Send + Sync>,
}

impl PyReadOnlyTable {
    pub(crate) fn new<K: Key + 'static, V: Value + 'static>(
        def: Definition,
        table: ReadOnlyTable<K, V>,
    ) -> Self
    where
        ReadOnlyTable<K, V>: Send + Sync,
    {
        Self {
            def,
            table: Box::new(table),
        }
    }
}

table_methods!(PyReadOnlyTable, snapshot_table, {
    /// Iterates the `(key, value)` entries with `start <= key < end`, in key order
    #[pyo3(signature = (start=None, end=None, *, reverse=false))]
    fn range(
        &self,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        reverse: bool,
    ) -> PyResult<PyRangeIter> {
        with_types!(self.def.key, self.def.value, K, V, {
            let (start, end) = extract_limits::<K>(start, end)?;
            snapshot_table!(self, K, V, |table| {
                let range = table
                    .range_owned(limits::<K>(start.as_ref(), true, end.as_ref()))
                    .pyerr()?;
                Ok(PyRangeIter::snapshot(Box::new(range), reverse))
            })
        })
    }

    fn __iter__(&self) -> PyResult<PyRangeIter> {
        self.range(None, None, false)
    }
});

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTableDefinition>()?;
    m.add_class::<PyTable>()?;
    m.add_class::<PyReadOnlyTable>()?;
    m.add_class::<PyRangeIter>()?;
    m.add_class::<PyTableStats>()?;
    Ok(())
}
