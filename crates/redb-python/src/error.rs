use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::fmt::Debug;

create_exception!(
    redb,
    Error,
    PyException,
    "An error from redb. `kind` names the redb error variant, such as \"TableDoesNotExist\"."
);

/// A `redb.Error` whose `kind` is `kind`
pub(crate) fn error(kind: &str, message: impl Into<String>) -> PyErr {
    Python::attach(|py| {
        let err = Error::new_err(message.into());
        // Exception instances carry a __dict__, so this cannot fail
        let _ = err.value(py).setattr("kind", kind);
        err
    })
}

/// The variant name of a value with a derived `Debug`, which every redb error enum has
fn variant_name(err: &impl Debug) -> String {
    let debug = format!("{err:?}");
    debug
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_string()
}

pub(crate) trait IntoPyErr {
    fn into_py_err(self) -> PyErr;
}

/// `result.pyerr()?` raises a redb error as `redb.Error`
pub(crate) trait PyErrExt<T> {
    fn pyerr(self) -> PyResult<T>;
}

impl<T, E: IntoPyErr> PyErrExt<T> for Result<T, E> {
    fn pyerr(self) -> PyResult<T> {
        self.map_err(IntoPyErr::into_py_err)
    }
}

impl IntoPyErr for redb::StorageError {
    fn into_py_err(self) -> PyErr {
        error(&variant_name(&self), self.to_string())
    }
}

impl IntoPyErr for redb::SetDurabilityError {
    fn into_py_err(self) -> PyErr {
        error(&variant_name(&self), self.to_string())
    }
}

// The enums with a `Storage` variant report the storage error's own kind
macro_rules! wrapping_errors {
    ($($error:ident),*) => {$(
        impl IntoPyErr for redb::$error {
            fn into_py_err(self) -> PyErr {
                match self {
                    redb::$error::Storage(inner) => inner.into_py_err(),
                    other => error(&variant_name(&other), other.to_string()),
                }
            }
        }
    )*};
}

wrapping_errors!(
    DatabaseError,
    TransactionError,
    CommitError,
    TableError,
    SavepointError,
    CompactionError
);

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("Error", m.py().get_type::<Error>())
}
