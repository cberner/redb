use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

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

// One exception class per `redb::Error` variant, each a direct subclass of `Error`. Every
// other redb error enum converts into `redb::Error`, which flattens them all.
macro_rules! variants {
    ($($name:ident),* $(,)?) => {
        $(create_exception!(redb, $name, Error);)*

        fn into_py_err(err: ::redb::Error) -> PyErr {
            let msg = err.to_string();
            match err {
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
    Io,
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
