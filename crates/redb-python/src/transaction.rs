use crate::database::PyDatabase;
use crate::error::{PyErrExt, ReadTransactionStillInUse, TransactionCompleted};
use crate::table::{PyReadOnlyTable, PyTable, PyTableDefinition};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;

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
    // How many times each table has been changed, which a range over a table compares against
    // to notice the table changing under it
    mutations: Mutex<HashMap<String, u64>>,
}

impl PyWriteTransaction {
    pub(crate) fn new(db: Py<PyDatabase>, txn: ::redb::WriteTransaction) -> Self {
        Self {
            state: Mutex::new(Some(Active { txn, db })),
            mutations: Mutex::new(HashMap::new()),
        }
    }

    fn take(&self) -> Option<Active> {
        self.state.lock().unwrap().take()
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
        self.mutations
            .lock()
            .unwrap()
            .get(table)
            .copied()
            .unwrap_or_default()
    }

    /// Runs `f` on the transaction, which is this operation's alone until `f` returns
    pub(crate) fn with_txn<R>(
        &self,
        f: impl FnOnce(&::redb::WriteTransaction) -> PyResult<R>,
    ) -> PyResult<R> {
        let state = self.state.lock().unwrap();
        let active = state.as_ref().ok_or_else(completed)?;
        f(&active.txn)
    }
}

#[pymethods]
impl PyWriteTransaction {
    /// Opens a table, creating it if it does not exist
    fn open_table(slf: Py<Self>, definition: &PyTableDefinition) -> PyResult<PyTable> {
        PyTable::new(slf, definition)
    }

    /// Writes every change made in the transaction to the database
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        finish(py, self.take().ok_or_else(completed)?, true)
    }

    /// Discards every change made in the transaction
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        finish(py, self.take().ok_or_else(completed)?, false)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.state.lock().unwrap().is_none() {
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
        if let Some(active) = self.take() {
            finish(py, active, exc_type.is_none())?;
        }
        Ok(false)
    }
}

fn closed() -> PyErr {
    TransactionCompleted::new_err("read transaction already closed")
}

// Field order is the drop order: `txn` before `db`. Dropping the last reference to a Database
// closes its storage whatever read transactions are alive, so each of them keeps the Database.
struct ReadActive {
    txn: ::redb::ReadTransaction,
    db: Py<PyDatabase>,
}

/// A read transaction: a snapshot of the database, which the tables opened from it keep alive
/// until they are dropped
#[pyclass(module = "redb", name = "ReadTransaction", frozen)]
pub(crate) struct PyReadTransaction {
    // None once closed
    state: Mutex<Option<ReadActive>>,
}

impl PyReadTransaction {
    pub(crate) fn new(db: Py<PyDatabase>, txn: ::redb::ReadTransaction) -> Self {
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
