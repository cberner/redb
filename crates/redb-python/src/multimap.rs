use crate::error::PyErrExt;
use crate::table::{
    Cursor, Definition, Pair, PyRangeIter, PyTableStats, Snapshot, Source, WriteHandle, cached,
    definition_class, extract_limits, limits, lock, owned,
};
use crate::transaction::PyWriteTransaction;
use crate::types::{PyValue, check_key, with_key_types};
use pyo3::prelude::*;
use pyo3::types::PyList;
use redb::{
    Key, MultimapTable, MultimapTableDefinition, MultimapValue, OwnedMultimapRange,
    OwnedMultimapValue, ReadOnlyMultimapTable, ReadableMultimapTable, ReadableTableMetadata,
    WriteTransaction,
};
use std::any::Any;

definition_class!(
    PyMultimapTableDefinition,
    "MultimapTableDefinition",
    true,
    "The name and types of a multimap table, whose values are key types too"
);

pub(crate) fn open<'txn, K: Key + 'static, V: Key + 'static>(
    txn: &'txn WriteTransaction,
    name: &str,
) -> PyResult<MultimapTable<'txn, K, V>> {
    txn.open_multimap_table(MultimapTableDefinition::new(name))
        .pyerr()
}

macro_rules! live_multimap {
    ($self:expr, $K:ident, $V:ident, |$table:ident| $body:expr) => {
        cached!(
            $self,
            &MultimapTable<'static, $K, $V>,
            open::<$K, $V>,
            |$table| $body
        )
    };
}

macro_rules! live_multimap_mut {
    ($self:expr, $K:ident, $V:ident, |$table:ident| $body:expr) => {
        cached!(
            $self,
            &mut MultimapTable<'static, $K, $V>,
            open::<$K, $V>,
            |$table| $body
        )
    };
}

macro_rules! snapshot_multimap {
    ($self:expr, $K:ident, $V:ident, |$table:ident| $body:expr) => {
        owned!($self, ReadOnlyMultimapTable<$K, $V>, |$table| $body)
    };
}

/// The values of one key, as a list
fn py_values<V: PyValue + Key>(
    py: Python<'_>,
    values: MultimapValue<'_, V>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for value in values {
        list.append(V::to_py(py, value.pyerr()?.value())?)?;
    }
    Ok(list.into_any().unbind())
}

fn py_owned_values<V: PyValue + Key>(
    py: Python<'_>,
    values: OwnedMultimapValue<V>,
) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    for value in values {
        list.append(V::to_py(py, value.pyerr()?.value())?)?;
    }
    Ok(list.into_any().unbind())
}

impl<K: PyValue + Key + Send + Sync, V: PyValue + Key + Send + Sync> Snapshot
    for OwnedMultimapRange<K, V>
{
    fn advance(&mut self, py: Python<'_>, reverse: bool) -> PyResult<Option<Pair>> {
        let entry = if reverse {
            self.next_back()
        } else {
            Iterator::next(self)
        };
        entry
            .map(|entry| {
                let (key, values) = entry.pyerr()?;
                Ok((
                    K::to_py(py, key.value())?.unbind(),
                    py_owned_values::<V>(py, values)?,
                ))
            })
            .transpose()
    }
}

/// The methods a write-side and a read-only multimap table share
macro_rules! multimap_methods {
    ($class:ident, $access:ident, { $($extra:tt)* }) => {
        #[pymethods]
        impl $class {
            #[getter]
            fn name(&self) -> &str {
                &self.def.name
            }

            /// The values stored under `key`, in order; empty if there are none
            fn get(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                with_key_types!(self.def.key, self.def.value, K, V, {
                    let key = K::from_py(key)?;
                    $access!(self, K, V, |table| py_values::<V>(
                        py,
                        table.get(K::lend(&key)).pyerr()?
                    ))
                })
            }

            fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                self.get(py, key)
            }

            /// Whether `key` holds at least one value
            fn __contains__(&self, key: &Bound<'_, PyAny>) -> PyResult<bool> {
                with_key_types!(self.def.key, self.def.value, K, V, {
                    let key = K::from_py(key)?;
                    $access!(self, K, V, |table| Ok(!table
                        .get(K::lend(&key))
                        .pyerr()?
                        .is_empty()))
                })
            }

            /// The number of `(key, value)` pairs
            fn __len__(&self) -> PyResult<usize> {
                with_key_types!(self.def.key, self.def.value, K, V, {
                    $access!(self, K, V, |table| Ok(usize::try_from(table.len().pyerr()?)
                        .unwrap_or(usize::MAX)))
                })
            }

            fn stats(&self) -> PyResult<PyTableStats> {
                with_key_types!(self.def.key, self.def.value, K, V, {
                    $access!(self, K, V, |table| Ok(table.stats().pyerr()?.into()))
                })
            }

            $($extra)*
        }
    };
}

/// A multimap table of a write transaction: each key holds a sorted set of values
#[pyclass(module = "redb", name = "MultimapTable", frozen)]
pub(crate) struct PyMultimapTable {
    def: Definition,
    handle: WriteHandle,
}

impl PyMultimapTable {
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
        with_key_types!(self.def.key, self.def.value, K, V, {
            let (start, end) = cursor.bounds::<K>(py)?;
            live_multimap!(self, K, V, |table| {
                let range = table
                    .range(limits::<K>(
                        start.as_ref(),
                        cursor.start_inclusive(),
                        end.as_ref(),
                    ))
                    .pyerr()?;
                cursor.drain::<K, _>(py, range, |values| py_values::<V>(py, values))
            })
        })
    }
}

multimap_methods!(PyMultimapTable, live_multimap, {
    /// Iterates `(key, values)` for the keys with `start <= key < end`, in key order
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
        Ok(PyRangeIter::live(
            Source::Multimap(slf),
            start,
            end,
            reverse,
        ))
    }

    fn __iter__(slf: Py<Self>) -> PyRangeIter {
        PyRangeIter::live(Source::Multimap(slf), None, None, false)
    }

    /// Adds `value` under `key`; returns whether it was already there
    fn insert(&self, key: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        with_key_types!(self.def.key, self.def.value, K, V, {
            let (key, value) = (K::from_py(key)?, V::from_py(value)?);
            live_multimap_mut!(self, K, V, |table| table
                .insert(K::lend(&key), V::lend(&value))
                .pyerr())
        })
    }

    /// Removes `value` from `key`; returns whether it was there
    fn remove(&self, key: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        with_key_types!(self.def.key, self.def.value, K, V, {
            let (key, value) = (K::from_py(key)?, V::from_py(value)?);
            live_multimap_mut!(self, K, V, |table| table
                .remove(K::lend(&key), V::lend(&value))
                .pyerr())
        })
    }

    /// Removes every value under `key`; returns them
    fn remove_all(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        with_key_types!(self.def.key, self.def.value, K, V, {
            let key = K::from_py(key)?;
            live_multimap_mut!(self, K, V, |table| py_values::<V>(
                py,
                table.remove_all(K::lend(&key)).pyerr()?
            ))
        })
    }
});

/// A multimap table of a read transaction, which it keeps alive
#[pyclass(module = "redb", name = "ReadOnlyMultimapTable", frozen)]
pub(crate) struct PyReadOnlyMultimapTable {
    def: Definition,
    table: Box<dyn Any + Send + Sync>,
}

impl PyReadOnlyMultimapTable {
    pub(crate) fn new<K: Key + 'static, V: Key + 'static>(
        def: Definition,
        table: ReadOnlyMultimapTable<K, V>,
    ) -> Self
    where
        ReadOnlyMultimapTable<K, V>: Send + Sync,
    {
        Self {
            def,
            table: Box::new(table),
        }
    }
}

multimap_methods!(PyReadOnlyMultimapTable, snapshot_multimap, {
    /// Iterates `(key, values)` for the keys with `start <= key < end`, in key order
    #[pyo3(signature = (start=None, end=None, *, reverse=false))]
    fn range(
        &self,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        reverse: bool,
    ) -> PyResult<PyRangeIter> {
        with_key_types!(self.def.key, self.def.value, K, V, {
            let (start, end) = extract_limits::<K>(start, end)?;
            snapshot_multimap!(self, K, V, |table| {
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
    m.add_class::<PyMultimapTableDefinition>()?;
    m.add_class::<PyMultimapTable>()?;
    m.add_class::<PyReadOnlyMultimapTable>()?;
    Ok(())
}
