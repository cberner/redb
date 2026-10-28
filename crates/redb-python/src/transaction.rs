use crate::error::{IntoPyErr, PyErrExt, error};
use crate::multimap::{PyMultimapTable, PyMultimapTableDefinition, PyReadOnlyMultimapTable};
use crate::table::{PyReadOnlyTable, PyTable, PyTableDefinition};
use crate::types::{with_key_types, with_types};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use redb::{MultimapTableDefinition, MultimapTableHandle, TableDefinition, TableHandle};
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Whether a commit reaches disk before it returns
#[pyclass(
    module = "redb",
    name = "Durability",
    eq,
    eq_int,
    hash,
    frozen,
    from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PyDurability {
    #[pyo3(name = "NONE")]
    None,
    #[pyo3(name = "IMMEDIATE")]
    Immediate,
}

fn completed() -> PyErr {
    error(
        "TransactionCompleted",
        "transaction already committed or aborted",
    )
}

/// The error for a table that an operation still holds. A transaction is used from one thread
/// at a time, so that operation is on this thread: a callback, such as a retain predicate,
/// calling back into the transaction.
pub(crate) fn in_use(name: &str) -> PyErr {
    error(
        "TableAlreadyOpen",
        format!("table {name:?} is in use by an operation on it, such as a retain callback"),
    )
}

/// A redb table kept open across Python calls.
///
/// The table borrows the transaction, with the lifetime erased to `'static`; this keeps the
/// transaction's allocation alive and drops the table first, so the borrow is honored.
pub(crate) struct Open {
    pub(crate) table: Box<dyn Any + Send + Sync>,
    _txn: Arc<redb::WriteTransaction>,
}

/// A cached table, locked for the duration of each operation on it
pub(crate) type Cached = Arc<Mutex<Open>>;

// Field order is the drop order: the tables borrow the transaction, and a Database's teardown
// waits for its live write transaction, so neither may outlive what follows it.
struct Active {
    tables: HashMap<String, Cached>,
    txn: Arc<redb::WriteTransaction>,
    db: Py<PyAny>,
}

impl Active {
    /// Closes the cached tables, which writes their roots back to the transaction
    fn close_tables(&mut self) -> PyResult<()> {
        // A second reference belongs to an operation in flight on the table
        if let Some(name) = self
            .tables
            .iter()
            .find(|(_, table)| Arc::strong_count(table) > 1)
            .map(|(name, _)| name)
        {
            return Err(in_use(name));
        }
        self.tables.clear();
        Ok(())
    }
}

/// A write transaction, used from one thread at a time.
///
/// It owns the redb tables opened through it, so a table object is only a name: dropping one
/// does nothing, and any order of drops is fine. The tables are closed before the transaction
/// commits, aborts, or is needed on its own.
#[pyclass(module = "redb", name = "WriteTransaction", frozen)]
pub(crate) struct PyWriteTransaction {
    state: Mutex<Option<Active>>,
    // Bumped when a table is deleted, renamed or rolled back, so table handles opened earlier
    // stop resolving instead of silently recreating the table by name
    generation: AtomicU64,
    // Mutations of each table so far, which an iterator over it compares against to notice the
    // table changing under it
    mutations: Mutex<HashMap<String, u64>>,
}

impl PyWriteTransaction {
    pub(crate) fn new(db: Py<PyAny>, txn: redb::WriteTransaction) -> Self {
        Self {
            state: Mutex::new(Some(Active {
                tables: HashMap::new(),
                txn: Arc::new(txn),
                db,
            })),
            generation: AtomicU64::new(0),
            mutations: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn mutated(&self, name: &str) {
        let mut mutations = self.mutations.lock().unwrap();
        if let Some(count) = mutations.get_mut(name) {
            *count += 1;
        } else {
            mutations.insert(name.to_string(), 1);
        }
    }

    pub(crate) fn mutation_count(&self, name: &str) -> u64 {
        self.mutations
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or_default()
    }

    fn txn(&self) -> PyResult<Arc<redb::WriteTransaction>> {
        self.state
            .lock()
            .unwrap()
            .as_ref()
            .map(|active| active.txn.clone())
            .ok_or_else(completed)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// The cached table `name`, opened with `open` on first use
    pub(crate) fn cached<T: Any + Send + Sync>(
        &self,
        name: &str,
        open: impl FnOnce(&'static redb::WriteTransaction) -> PyResult<T>,
    ) -> PyResult<Cached> {
        let mut state = self.state.lock().unwrap();
        let active = state.as_mut().ok_or_else(completed)?;
        if let Some(cached) = active.tables.get(name) {
            return Ok(cached.clone());
        }
        let txn = active.txn.clone();
        // SAFETY: the reference points into the allocation behind `txn`, which outlives the
        // table: the `Open` below holds an Arc to it and drops the table first. The transaction
        // is moved out, to commit or abort, or borrowed mutably only once every `Open` is gone,
        // which `close_tables` ensures.
        let table = open(unsafe { &*Arc::as_ptr(&txn) })?;
        let cached = Arc::new(Mutex::new(Open {
            table: Box::new(table),
            _txn: txn,
        }));
        active.tables.insert(name.to_string(), cached.clone());
        Ok(cached)
    }

    /// Closes the tables and runs `f` on the transaction
    fn with_closed<R>(
        &self,
        f: impl FnOnce(&redb::WriteTransaction) -> PyResult<R>,
    ) -> PyResult<R> {
        let mut state = self.state.lock().unwrap();
        let active = state.as_mut().ok_or_else(completed)?;
        active.close_tables()?;
        f(&active.txn)
    }

    /// Closes the tables and runs `f` with exclusive access to the transaction
    fn exclusive<R>(&self, f: impl FnOnce(&mut redb::WriteTransaction) -> R) -> PyResult<R> {
        let mut state = self.state.lock().unwrap();
        let active = state.as_mut().ok_or_else(completed)?;
        active.close_tables()?;
        // Every other reference belonged to a cached table
        let txn = Arc::get_mut(&mut active.txn).ok_or_else(|| in_use("?"))?;
        Ok(f(txn))
    }

    fn finish(&self, py: Python<'_>, commit: bool) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        state.as_mut().ok_or_else(completed)?.close_tables()?;
        let Active { tables, txn, db } = state.take().ok_or_else(completed)?;
        let txn = match Arc::try_unwrap(txn) {
            Ok(txn) => txn,
            Err(txn) => {
                *state = Some(Active { tables, txn, db });
                return Err(in_use("?"));
            }
        };
        drop(state);
        let result = if commit {
            py.detach(|| txn.commit()).pyerr()
        } else {
            py.detach(|| txn.abort()).pyerr()
        };
        drop(db);
        result
    }
}

fn untyped(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

fn untyped_multimap(name: &str) -> MultimapTableDefinition<'_, &'static [u8], &'static [u8]> {
    MultimapTableDefinition::new(name)
}

/// The name of a table given as a definition or a string
fn table_name(table: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(name) = table.extract::<String>() {
        return Ok(name);
    }
    if let Ok(definition) = table.extract::<PyTableDefinition>() {
        return Ok(definition.def.name);
    }
    if let Ok(definition) = table.extract::<PyMultimapTableDefinition>() {
        return Ok(definition.def.name);
    }
    Err(PyTypeError::new_err("expected a table name or definition"))
}

#[pymethods]
impl PyWriteTransaction {
    /// Opens a table, creating it if it does not exist
    fn open_table(slf: Py<Self>, definition: &PyTableDefinition) -> PyResult<PyTable> {
        let def = definition.def.clone();
        with_types!(def.key, def.value, K, V, {
            slf.get()
                .cached(&def.name, |txn| crate::table::open::<K, V>(txn, &def.name))?;
        });
        Ok(PyTable::new(slf, def))
    }

    /// Opens a multimap table, creating it if it does not exist
    fn open_multimap_table(
        slf: Py<Self>,
        definition: &PyMultimapTableDefinition,
    ) -> PyResult<PyMultimapTable> {
        let def = definition.def.clone();
        with_key_types!(def.key, def.value, K, V, {
            slf.get().cached(&def.name, |txn| {
                crate::multimap::open::<K, V>(txn, &def.name)
            })?;
        });
        Ok(PyMultimapTable::new(slf, def))
    }

    /// Deletes the table, given as a definition or a name; returns whether it existed.
    /// Handles on it opened earlier stop working.
    fn delete_table(&self, table: &Bound<'_, PyAny>) -> PyResult<bool> {
        let name = table_name(table)?;
        let deleted = self.with_closed(|txn| txn.delete_table(untyped(&name)).pyerr())?;
        self.bump_generation();
        Ok(deleted)
    }

    fn delete_multimap_table(&self, table: &Bound<'_, PyAny>) -> PyResult<bool> {
        let name = table_name(table)?;
        let deleted =
            self.with_closed(|txn| txn.delete_multimap_table(untyped_multimap(&name)).pyerr())?;
        self.bump_generation();
        Ok(deleted)
    }

    /// Renames the table. Handles on it opened earlier stop working.
    fn rename_table(&self, table: &Bound<'_, PyAny>, new_name: &str) -> PyResult<()> {
        let name = table_name(table)?;
        self.with_closed(|txn| txn.rename_table(untyped(&name), untyped(new_name)).pyerr())?;
        self.bump_generation();
        Ok(())
    }

    fn rename_multimap_table(&self, table: &Bound<'_, PyAny>, new_name: &str) -> PyResult<()> {
        let name = table_name(table)?;
        self.with_closed(|txn| {
            txn.rename_multimap_table(untyped_multimap(&name), untyped_multimap(new_name))
                .pyerr()
        })?;
        self.bump_generation();
        Ok(())
    }

    fn list_tables(&self) -> PyResult<Vec<String>> {
        Ok(self
            .txn()?
            .list_tables()
            .pyerr()?
            .map(|table| table.name().to_string())
            .collect())
    }

    fn list_multimap_tables(&self) -> PyResult<Vec<String>> {
        Ok(self
            .txn()?
            .list_multimap_tables()
            .pyerr()?
            .map(|table| table.name().to_string())
            .collect())
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        self.finish(py, true)
    }

    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        self.finish(py, false)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        slf.txn()?;
        Ok(slf)
    }

    /// Commits if the block finished normally, aborts if an exception is propagating; a
    /// transaction already finalized inside the block is left alone
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if self.state.lock().unwrap().is_some() {
            self.finish(py, exc_type.is_none())?;
        }
        Ok(false)
    }

    fn set_durability(&self, durability: PyDurability) -> PyResult<()> {
        let durability = match durability {
            PyDurability::None => redb::Durability::None,
            PyDurability::Immediate => redb::Durability::Immediate,
        };
        self.exclusive(|txn| txn.set_durability(durability))?
            .pyerr()
    }

    fn set_two_phase_commit(&self, enabled: bool) -> PyResult<()> {
        self.exclusive(|txn| txn.set_two_phase_commit(enabled))
    }

    fn set_quick_repair(&self, enabled: bool) -> PyResult<()> {
        self.exclusive(|txn| txn.set_quick_repair(enabled))
    }

    /// A savepoint that lives as long as the returned object, which keeps a read transaction
    /// and so the database file open
    fn ephemeral_savepoint(&self) -> PyResult<PySavepoint> {
        Ok(PySavepoint {
            inner: self.txn()?.ephemeral_savepoint().pyerr()?,
        })
    }

    /// A savepoint stored in the database until deleted; returns its id
    fn persistent_savepoint(&self) -> PyResult<u64> {
        self.txn()?.persistent_savepoint().pyerr()
    }

    fn get_persistent_savepoint(&self, id: u64) -> PyResult<PySavepoint> {
        Ok(PySavepoint {
            inner: self.txn()?.get_persistent_savepoint(id).pyerr()?,
        })
    }

    fn delete_persistent_savepoint(&self, id: u64) -> PyResult<bool> {
        self.txn()?.delete_persistent_savepoint(id).pyerr()
    }

    fn list_persistent_savepoints(&self) -> PyResult<Vec<u64>> {
        Ok(self.txn()?.list_persistent_savepoints().pyerr()?.collect())
    }

    /// Rolls the transaction back to the savepoint. Table handles opened earlier stop working.
    fn restore_savepoint(&self, savepoint: &PySavepoint) -> PyResult<()> {
        self.exclusive(|txn| txn.restore_savepoint(&savepoint.inner))?
            .pyerr()?;
        self.bump_generation();
        Ok(())
    }

    fn stats(&self) -> PyResult<PyDatabaseStats> {
        Ok(self.txn()?.stats().pyerr()?.into())
    }
}

/// A point a write transaction can be rolled back to
#[pyclass(module = "redb", name = "Savepoint", frozen)]
pub(crate) struct PySavepoint {
    inner: redb::Savepoint,
}

#[pyclass(module = "redb", name = "DatabaseStats", frozen, get_all)]
pub(crate) struct PyDatabaseStats {
    tree_height: u32,
    allocated_pages: u64,
    leaf_pages: u64,
    branch_pages: u64,
    stored_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
    page_size: usize,
}

impl From<redb::DatabaseStats> for PyDatabaseStats {
    fn from(stats: redb::DatabaseStats) -> Self {
        Self {
            tree_height: stats.tree_height(),
            allocated_pages: stats.allocated_pages(),
            leaf_pages: stats.leaf_pages(),
            branch_pages: stats.branch_pages(),
            stored_bytes: stats.stored_bytes(),
            metadata_bytes: stats.metadata_bytes(),
            fragmented_bytes: stats.fragmented_bytes(),
            page_size: stats.page_size(),
        }
    }
}

fn closed() -> PyErr {
    error("TransactionCompleted", "read transaction already closed")
}

/// A read transaction: a snapshot, which the tables and iterators opened from it keep alive
/// until they are dropped. The snapshot keeps the file open on its own, so unlike a write
/// transaction it holds no reference to the Database object.
#[pyclass(module = "redb", name = "ReadTransaction", frozen)]
pub(crate) struct PyReadTransaction {
    state: Mutex<Option<redb::ReadTransaction>>,
}

impl PyReadTransaction {
    pub(crate) fn new(txn: redb::ReadTransaction) -> Self {
        Self {
            state: Mutex::new(Some(txn)),
        }
    }
}

#[pymethods]
impl PyReadTransaction {
    fn open_table(&self, definition: &PyTableDefinition) -> PyResult<PyReadOnlyTable> {
        let def = definition.def.clone();
        let state = self.state.lock().unwrap();
        let txn = state.as_ref().ok_or_else(closed)?;
        with_types!(def.key, def.value, K, V, {
            let table = txn
                .open_table(TableDefinition::<K, V>::new(&def.name))
                .pyerr()?;
            Ok(PyReadOnlyTable::new(def, table))
        })
    }

    fn open_multimap_table(
        &self,
        definition: &PyMultimapTableDefinition,
    ) -> PyResult<PyReadOnlyMultimapTable> {
        let def = definition.def.clone();
        let state = self.state.lock().unwrap();
        let txn = state.as_ref().ok_or_else(closed)?;
        with_key_types!(def.key, def.value, K, V, {
            let table = txn
                .open_multimap_table(MultimapTableDefinition::<K, V>::new(&def.name))
                .pyerr()?;
            Ok(PyReadOnlyMultimapTable::new(def, table))
        })
    }

    fn list_tables(&self) -> PyResult<Vec<String>> {
        let state = self.state.lock().unwrap();
        let txn = state.as_ref().ok_or_else(closed)?;
        Ok(txn
            .list_tables()
            .pyerr()?
            .map(|table| table.name().to_string())
            .collect())
    }

    fn list_multimap_tables(&self) -> PyResult<Vec<String>> {
        let state = self.state.lock().unwrap();
        let txn = state.as_ref().ok_or_else(closed)?;
        Ok(txn
            .list_multimap_tables()
            .pyerr()?
            .map(|table| table.name().to_string())
            .collect())
    }

    /// Ends the transaction now; fails if a table or iterator from it is still alive
    fn close(&self) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        let txn = state.take().ok_or_else(closed)?;
        match txn.close() {
            Ok(()) => Ok(()),
            Err(redb::TransactionError::ReadTransactionStillInUse(txn)) => {
                *state = Some(*txn);
                Err(error(
                    "ReadTransactionStillInUse",
                    "a table or iterator from this transaction is still alive",
                ))
            }
            Err(err) => Err(err.into_py_err()),
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.state.lock().unwrap().is_none() {
            return Err(closed());
        }
        Ok(slf)
    }

    /// Releases the transaction; tables still referenced keep the snapshot alive
    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        self.state.lock().unwrap().take();
        false
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDurability>()?;
    m.add_class::<PyWriteTransaction>()?;
    m.add_class::<PyReadTransaction>()?;
    m.add_class::<PySavepoint>()?;
    m.add_class::<PyDatabaseStats>()?;
    Ok(())
}
