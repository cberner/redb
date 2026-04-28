use crate::database::PyDatabase;
use crate::error::{TransactionCompleted, map_commit_error, map_storage_error};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::sync::Mutex;

// Field order is load-bearing: `txn` MUST be dropped before `db`. If the
// `WriteTransaction` is the last holder of the database reference and the
// user drops the transaction without entering a `with` block, `Database::drop`
// runs a finalizing `begin_write()` that blocks on any active writer.
// Dropping `db` first while `txn` still holds the writer slot would
// self-deadlock; struct fields drop in declaration order, so listing `txn`
// first keeps it correct.
struct ActiveWrite {
    txn: ::redb::WriteTransaction,
    db: Py<PyDatabase>,
}

enum State {
    Pending(ActiveWrite),
    InWith(ActiveWrite),
    Finalized,
}

#[pyclass(module = "redb", name = "WriteTransaction")]
pub(crate) struct PyWriteTransaction {
    state: Mutex<State>,
}

impl PyWriteTransaction {
    pub(crate) fn new(db: Py<PyDatabase>, txn: ::redb::WriteTransaction) -> Self {
        Self {
            state: Mutex::new(State::Pending(ActiveWrite { txn, db })),
        }
    }
}

#[pymethods]
impl PyWriteTransaction {
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        {
            let mut state = slf.state.lock().unwrap();
            match std::mem::replace(&mut *state, State::Finalized) {
                State::Pending(active) => {
                    *state = State::InWith(active);
                }
                State::InWith(active) => {
                    *state = State::InWith(active);
                    return Err(TransactionCompleted::new_err(
                        "transaction is already in use by a `with` block",
                    ));
                }
                State::Finalized => {
                    return Err(TransactionCompleted::new_err(
                        "transaction already committed or aborted",
                    ));
                }
            }
        }
        Ok(slf)
    }

    // Auto-finalize on context manager exit: commit on success, abort if an
    // exception is propagating out of the `with` block. To abort early, raise
    // an exception inside the block.
    #[pyo3(signature = (exc_type, exc_value, traceback))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: &Bound<'_, PyAny>,
        exc_value: &Bound<'_, PyAny>,
        traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let _ = (exc_value, traceback);
        let active = {
            let mut state = self.state.lock().unwrap();
            match std::mem::replace(&mut *state, State::Finalized) {
                State::InWith(active) => active,
                other => {
                    // Python guarantees __exit__ pairs with a successful
                    // __enter__, so any other state means external tampering;
                    // restore and no-op rather than acting on a stale handle.
                    *state = other;
                    return Ok(false);
                }
            }
        };
        let ActiveWrite { txn, db: _db } = active;
        if exc_type.is_none() {
            py.detach(|| txn.commit()).map_err(map_commit_error)?;
        } else {
            py.detach(|| txn.abort()).map_err(map_storage_error)?;
        }
        Ok(false)
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWriteTransaction>()?;
    Ok(())
}
