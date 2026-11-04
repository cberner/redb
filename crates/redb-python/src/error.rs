use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyOSError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{IntoPyDict, PyType};

create_exception!(
    redb,
    Error,
    PyException,
    "The base of every error redb raises."
);

// Raised by the bindings themselves
create_exception!(
    redb,
    TransactionCompleted,
    Error,
    "The transaction was already committed or aborted."
);

create_exception!(
    redb,
    UsedAfterFork,
    Error,
    "Opened by the process this one was forked from."
);

create_exception!(
    redb,
    TableChangedDuringIteration,
    Error,
    "The table changed while a range over it was being iterated."
);

create_exception!(
    redb,
    TransactionInUse,
    Error,
    "The transaction is in use by another operation, such as the predicate of a retain()."
);

/// `redb.Io`, an `OSError` as well as an `Error`. `create_exception!` takes one base, so it is
/// made with `type()`
fn io_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static IO: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    IO.get_or_try_init(py, || {
        let bases = (py.get_type::<Error>(), py.get_type::<PyOSError>());
        let attributes = [("__module__", "redb"), ("__doc__", "An I/O error.")].into_py_dict(py)?;
        let class = py.get_type::<PyType>().call1(("Io", bases, attributes))?;
        Ok(class.cast_into::<PyType>()?.unbind())
    })
    .map(|class| class.bind(py))
}

/// With the OS error number, where there is one, as `OSError.errno`
fn io_error(err: &std::io::Error, msg: String) -> PyErr {
    Python::attach(|py| match (io_class(py), err.raw_os_error()) {
        (Ok(class), Some(errno)) => PyErr::from_type(class.clone(), (errno, msg)),
        (Ok(class), None) => PyErr::from_type(class.clone(), msg),
        (Err(err), _) => err,
    })
}

// One exception class per `redb::Error` variant, each a direct subclass of `Error`, but for
// `Io`, above. Every other redb error enum converts into `redb::Error`, which flattens them all.
macro_rules! variants {
    ($($name:ident),* $(,)?) => {
        $(create_exception!(redb, $name, Error);)*

        fn into_py_err(err: ::redb::Error) -> PyErr {
            let msg = err.to_string();
            match err {
                ::redb::Error::Io(io) => io_error(&io, msg),
                $(::redb::Error::$name { .. } => $name::new_err(msg),)*
                // redb::Error is #[non_exhaustive]
                _ => Error::new_err(msg),
            }
        }

        fn add_variants(m: &Bound<'_, PyModule>) -> PyResult<()> {
            $(m.add(stringify!($name), m.py().get_type::<$name>())?;)*
            Ok(())
        }
    };
}

variants! {
    DatabaseAlreadyOpen,
    LockProtocolMismatch,
    InvalidSavepoint,
    ImmediateDurabilityRequired,
    RepairAborted,
    PersistentSavepointModified,
    NonDurableCommitUnsupported,
    EphemeralSavepointUnsupported,
    PersistentSavepointExists,
    EphemeralSavepointExists,
    TransactionInProgress,
    TransactionPoisoned,
    Corrupted,
    UpgradeRequired,
    ValueTooLarge,
    UnorderedKey,
    TableTypeMismatch,
    TableIsMultimap,
    TableIsNotMultimap,
    TypeDefinitionChanged,
    TableDoesNotExist,
    TableExists,
    TableAlreadyOpen,
    Unsupported,
    DatabaseClosed,
    PreviousIo,
    LockPoisoned,
    ReadTransactionStillInUse,
}

// `create_exception!` types are not `#[pyclass]`es, so instead of `add_class` each class is
// stored as a module attribute, which is all `redb.Io` is.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("Error", py.get_type::<Error>())?;
    m.add(
        "TransactionCompleted",
        py.get_type::<TransactionCompleted>(),
    )?;
    m.add("UsedAfterFork", py.get_type::<UsedAfterFork>())?;
    m.add(
        "TableChangedDuringIteration",
        py.get_type::<TableChangedDuringIteration>(),
    )?;
    m.add("TransactionInUse", py.get_type::<TransactionInUse>())?;
    m.add("Io", io_class(py)?)?;
    add_variants(m)
}

/// `result.pyerr()?` raises a redb error as its exception
pub(crate) trait PyErrExt<T> {
    fn pyerr(self) -> PyResult<T>;
}

impl<T, E: Into<::redb::Error>> PyErrExt<T> for Result<T, E> {
    fn pyerr(self) -> PyResult<T> {
        self.map_err(|err| into_py_err(err.into()))
    }
}
