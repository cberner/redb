use crate::database::PyDatabase;
use crate::error::{PyErrExt, TransactionCompleted};
use crate::table::{PyTable, PyTableDefinition};
use pyo3::prelude::*;
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
}

impl PyWriteTransaction {
    pub(crate) fn new(db: Py<PyDatabase>, txn: ::redb::WriteTransaction) -> Self {
        Self {
            state: Mutex::new(Some(Active { txn, db })),
        }
    }

    fn take(&self) -> Option<Active> {
        self.state.lock().unwrap().take()
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

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWriteTransaction>()?;
    Ok(())
}
