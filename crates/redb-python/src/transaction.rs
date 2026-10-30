use crate::database::PyDatabase;
use crate::error::{PyErrExt, TransactionCompleted};
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
/// Commit or abort it explicitly, or use it as a context manager: leaving the `with` block
/// commits, or aborts if an exception is propagating out of it.
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
}

#[pymethods]
impl PyWriteTransaction {
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
