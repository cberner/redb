use crate::database::PyDatabase;
use crate::error::{TransactionCompleted, map_commit_error, map_storage_error};
use pyo3::prelude::*;
use std::sync::Mutex;

// Field order is load-bearing: `txn` MUST be dropped before `db`. If the
// `WriteTransaction` is the last holder of the database reference and the
// user drops the transaction without committing or aborting, `Database::drop`
// runs a finalizing `begin_write()` that blocks on any active writer.
// Dropping `db` first while `txn` still holds the writer slot would
// self-deadlock; struct fields drop in declaration order, so listing `txn`
// first keeps it correct.
struct ActiveWrite {
    txn: ::redb::WriteTransaction,
    db: Py<PyDatabase>,
}

#[pyclass(module = "redb", name = "WriteTransaction")]
pub(crate) struct PyWriteTransaction {
    inner: Mutex<Option<ActiveWrite>>,
}

impl PyWriteTransaction {
    pub(crate) fn new(db: Py<PyDatabase>, txn: ::redb::WriteTransaction) -> Self {
        Self {
            inner: Mutex::new(Some(ActiveWrite { txn, db })),
        }
    }

    fn take(&self) -> PyResult<ActiveWrite> {
        self.inner.lock().unwrap().take().ok_or_else(|| {
            TransactionCompleted::new_err("transaction already committed or aborted")
        })
    }
}

#[pymethods]
impl PyWriteTransaction {
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let ActiveWrite { txn, db: _db } = self.take()?;
        py.detach(|| txn.commit()).map_err(map_commit_error)
    }

    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        let ActiveWrite { txn, db: _db } = self.take()?;
        py.detach(|| txn.abort()).map_err(map_storage_error)
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWriteTransaction>()?;
    Ok(())
}
