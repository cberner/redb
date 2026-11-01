use crate::database::PyDatabase;
use crate::error::{PyErrExt, ReadTransactionStillInUse, TransactionCompleted};
use crate::table::{PyReadOnlyTable, PyTable, PyTableDefinition, checked_name};
use ::redb::{TableDefinition, TableHandle, UntypedTableHandle};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, TryLockError};

/// A table named by its definition, or by its name alone as `list_tables` gives it
fn table_name(table: &Bound<'_, PyAny>) -> PyResult<String> {
    match table.cast::<PyTableDefinition>() {
        Ok(definition) => Ok(definition.get().name.clone()),
        Err(_) => checked_name(table.extract()?),
    }
}

/// redb names the table to delete or rename by a handle, whose types go unused
fn untyped(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

fn names(tables: impl Iterator<Item = UntypedTableHandle>) -> Vec<String> {
    tables.map(|table| table.name().to_string()).collect()
}

// Field order is the drop order: `txn` must go before `db`, since dropping the last reference
// to a Database waits for its live write transaction, which would be this one.
struct Active {
    txn: ::redb::WriteTransaction,
    db: Py<PyDatabase>,
}

fn completed() -> PyErr {
    TransactionCompleted::new_err("transaction already committed or aborted")
}

/// Commits or aborts, without the GIL
fn finish(py: Python<'_>, active: Active, commit: bool) -> PyResult<()> {
    // `txn` is consumed here, before `db` drops at the end of the scope
    let Active { txn, db: _db } = active;
    if commit {
        py.detach(|| txn.commit()).pyerr()
    } else {
        py.detach(|| txn.abort()).pyerr()
    }
}

/// A write transaction, used from one thread at a time.
///
/// A redb table borrows its transaction, which Python's garbage collector cannot be made to
/// respect, so no table stays open between calls: a `Table` object is only a name's binding to
/// one of this transaction's tables, and each operation on it opens the redb table, runs, and
/// closes it again. Commit or abort explicitly, or use the transaction as a context manager:
/// leaving the `with` block commits, or aborts if an exception is propagating out of it.
#[pyclass(module = "redb", name = "WriteTransaction", frozen)]
pub(crate) struct PyWriteTransaction {
    // None once committed or aborted
    state: Mutex<Option<Active>>,
    // How many times each table has been changed, and how many times all of them were, by a
    // savepoint restore; a range over a table compares the sum against what it saw, to notice
    // the table changing under it
    mutations: Mutex<HashMap<String, u64>>,
    restores: AtomicU64,
}

impl PyWriteTransaction {
    pub(crate) fn new(db: Py<PyDatabase>, txn: ::redb::WriteTransaction) -> Self {
        Self {
            state: Mutex::new(Some(Active { txn, db })),
            mutations: Mutex::new(HashMap::new()),
            restores: AtomicU64::new(0),
        }
    }

    /// The state, held for the length of one operation. An operation that finds it held is
    /// running inside another: a `retain` or `extract_if` predicate using the table it filters,
    /// or another thread's call while such a predicate runs. Waiting would deadlock, since the
    /// holder needs the GIL back to finish, so it fails instead.
    fn lock(&self) -> PyResult<MutexGuard<'_, Option<Active>>> {
        self.state.try_lock().map_err(|err| match err {
            TryLockError::WouldBlock => PyRuntimeError::new_err(
                "transaction in use by another operation (a predicate cannot use its own table)",
            ),
            TryLockError::Poisoned(err) => panic!("{err}"),
        })
    }

    fn take(&self) -> PyResult<Option<Active>> {
        Ok(self.lock()?.take())
    }

    pub(crate) fn mutated(&self, table: &str) {
        *self
            .mutations
            .lock()
            .unwrap()
            .entry(table.to_string())
            .or_default() += 1;
    }

    pub(crate) fn mutation_count(&self, table: &str) -> u64 {
        let of_table = self
            .mutations
            .lock()
            .unwrap()
            .get(table)
            .copied()
            .unwrap_or_default();
        self.restores.load(Ordering::Acquire) + of_table
    }

    /// Runs `f` on the transaction, which is this operation's alone until `f` returns
    pub(crate) fn with_txn<R>(
        &self,
        f: impl FnOnce(&::redb::WriteTransaction) -> PyResult<R>,
    ) -> PyResult<R> {
        let state = self.lock()?;
        let active = state.as_ref().ok_or_else(completed)?;
        f(&active.txn)
    }

    /// `with_txn` for what redb lets only a `&mut` transaction do
    fn with_txn_mut<R>(
        &self,
        f: impl FnOnce(&mut ::redb::WriteTransaction) -> PyResult<R>,
    ) -> PyResult<R> {
        let mut state = self.lock()?;
        let active = state.as_mut().ok_or_else(completed)?;
        f(&mut active.txn)
    }
}

#[pymethods]
impl PyWriteTransaction {
    /// Opens a table, creating it if it does not exist
    fn open_table(slf: Py<Self>, definition: &PyTableDefinition) -> PyResult<PyTable> {
        PyTable::new(slf, definition)
    }

    /// The names of the tables, in order, those created in this transaction included
    fn list_tables(&self) -> PyResult<Vec<String>> {
        self.with_txn(|txn| Ok(names(txn.list_tables().pyerr()?)))
    }

    /// Deletes a table, named by its definition or its name, and reports whether it existed. A
    /// `Table` object for it opens it anew, empty, if used again
    fn delete_table(&self, table: &Bound<'_, PyAny>) -> PyResult<bool> {
        let name = table_name(table)?;
        let existed = self.with_txn(|txn| txn.delete_table(untyped(&name)).pyerr())?;
        self.mutated(&name);
        Ok(existed)
    }

    /// Renames a table; refused with `TableExists` if the new name is taken. A `Table` object
    /// stays bound to the old name, and ranges over either name end
    fn rename_table(&self, table: &Bound<'_, PyAny>, new_name: &Bound<'_, PyAny>) -> PyResult<()> {
        let (name, new_name) = (table_name(table)?, table_name(new_name)?);
        self.with_txn(|txn| txn.rename_table(untyped(&name), untyped(&new_name)).pyerr())?;
        self.mutated(&name);
        self.mutated(&new_name);
        Ok(())
    }

    /// Records the tables as they stand, under an id that lasts, across transactions, until
    /// `delete_persistent_savepoint`, for `restore_savepoint` to bring them back. Refused with
    /// `InvalidSavepoint` once a table has been opened in this transaction. The pages a
    /// savepoint keeps are not freed while it exists
    fn persistent_savepoint(&self) -> PyResult<u64> {
        self.with_txn(|txn| txn.persistent_savepoint().pyerr())
    }

    /// The ids of the persistent savepoints, in order
    fn list_persistent_savepoints(&self) -> PyResult<Vec<u64>> {
        self.with_txn(|txn| Ok(txn.list_persistent_savepoints().pyerr()?.collect()))
    }

    /// Deletes a persistent savepoint, and reports whether it existed
    fn delete_persistent_savepoint(&self, id: u64) -> PyResult<bool> {
        self.with_txn(|txn| txn.delete_persistent_savepoint(id).pyerr())
    }

    /// Puts every table back as it was at the savepoint, in this transaction, and deletes the
    /// savepoints made after it. Ranges over the tables end. Refused with `InvalidSavepoint`
    /// for an id that is not a savepoint's
    fn restore_savepoint(&self, id: u64) -> PyResult<()> {
        self.with_txn_mut(|txn| {
            let savepoint = txn.get_persistent_savepoint(id).pyerr()?;
            txn.restore_savepoint(&savepoint).pyerr()
        })?;
        self.restores.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Writes every change made in the transaction to the database
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        finish(py, self.take()?.ok_or_else(completed)?, true)
    }

    /// Discards every change made in the transaction
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        finish(py, self.take()?.ok_or_else(completed)?, false)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.lock()?.is_none() {
            return Err(completed());
        }
        Ok(slf)
    }

    // A no-op after an explicit commit or abort inside the block, like closing a closed file
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if let Some(active) = self.take()? {
            finish(py, active, exc_type.is_none())?;
        }
        Ok(false)
    }
}

fn closed() -> PyErr {
    TransactionCompleted::new_err("read transaction already closed")
}

// Field order is the drop order: `txn` before `db`. Dropping the last reference to a Database
// closes its storage whatever read transactions are alive, so each of them keeps its database
// object, a Database or a ReadOnlyDatabase.
struct ReadActive {
    txn: ::redb::ReadTransaction,
    db: Py<PyAny>,
}

/// A read transaction: a snapshot of the database, which the tables opened from it keep alive
/// until they are dropped
#[pyclass(module = "redb", name = "ReadTransaction", frozen)]
pub(crate) struct PyReadTransaction {
    // None once closed
    state: Mutex<Option<ReadActive>>,
}

impl PyReadTransaction {
    pub(crate) fn new(db: Py<PyAny>, txn: ::redb::ReadTransaction) -> Self {
        Self {
            state: Mutex::new(Some(ReadActive { txn, db })),
        }
    }
}

#[pymethods]
impl PyReadTransaction {
    /// Opens a table of the snapshot
    fn open_table(
        &self,
        py: Python<'_>,
        definition: &PyTableDefinition,
    ) -> PyResult<PyReadOnlyTable> {
        let state = self.state.lock().unwrap();
        let active = state.as_ref().ok_or_else(closed)?;
        PyReadOnlyTable::new(&active.txn, active.db.clone_ref(py), definition)
    }

    /// The names of the tables of the snapshot, in order
    fn list_tables(&self) -> PyResult<Vec<String>> {
        let state = self.state.lock().unwrap();
        let active = state.as_ref().ok_or_else(closed)?;
        Ok(names(active.txn.list_tables().pyerr()?))
    }

    /// Ends the transaction now; refused while a table from it is still alive
    fn close(&self) -> PyResult<()> {
        let mut state = self.state.lock().unwrap();
        let ReadActive { txn, db } = state.take().ok_or_else(closed)?;
        match txn.close() {
            Ok(()) => Ok(()),
            Err(::redb::TransactionError::ReadTransactionStillInUse(txn)) => {
                *state = Some(ReadActive { txn: *txn, db });
                Err(ReadTransactionStillInUse::new_err(
                    "a table from this transaction is still alive",
                ))
            }
            Err(err) => Err(err).pyerr(),
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.state.lock().unwrap().is_none() {
            return Err(closed());
        }
        Ok(slf)
    }

    // Releases the transaction; tables still referenced keep the snapshot alive
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
    m.add_class::<PyWriteTransaction>()?;
    m.add_class::<PyReadTransaction>()?;
    Ok(())
}
