use crate::error::UsedAfterFork;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::atomic::{AtomicU64, Ordering};

// How many forks separate this process from the one that imported the module. Child hooks run in
// registration order, so an object a hook registered before ours creates in the child is counted
// as inherited.
static FORKS: AtomicU64 = AtomicU64::new(0);

#[pyfunction]
fn after_fork_in_child() {
    FORKS.fetch_add(1, Ordering::Relaxed);
}

/// A redb object, usable only in the process that created it. A forked child shares the parent's
/// file and its locks, so using the object there, or dropping it, which writes the file and
/// releases the locks, would corrupt the parent's database. The child leaks it instead.
pub(crate) struct ProcessLocal<T> {
    forks: u64,
    // Taken only to leak it
    value: Option<T>,
}

impl<T> ProcessLocal<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            forks: FORKS.load(Ordering::Relaxed),
            value: Some(value),
        }
    }

    fn inherited(&self) -> bool {
        FORKS.load(Ordering::Relaxed) != self.forks
    }

    fn check(&self) -> PyResult<()> {
        if self.inherited() {
            return Err(UsedAfterFork::new_err(
                "opened by the process this one was forked from; open the database again here",
            ));
        }
        Ok(())
    }

    pub(crate) fn get(&self) -> PyResult<&T> {
        self.check()?;
        Ok(self.value.as_ref().expect("present until dropped"))
    }
}

impl<T> Drop for ProcessLocal<T> {
    fn drop(&mut self) {
        if self.inherited() {
            std::mem::forget(self.value.take());
        }
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let os = py.import("os")?;
    // Absent where processes cannot fork
    if os.hasattr("register_at_fork")? {
        let kwargs = PyDict::new(py);
        kwargs.set_item("after_in_child", wrap_pyfunction!(after_fork_in_child, m)?)?;
        os.getattr("register_at_fork")?.call((), Some(&kwargs))?;
    }
    Ok(())
}
