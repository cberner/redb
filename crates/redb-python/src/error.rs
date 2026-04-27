use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(redb, Error, PyException);
create_exception!(redb, DatabaseError, Error);
create_exception!(redb, StorageError, Error);

create_exception!(redb, DatabaseAlreadyOpen, DatabaseError);
create_exception!(redb, RepairAborted, DatabaseError);
create_exception!(redb, UpgradeRequired, DatabaseError);
create_exception!(redb, TransactionInProgress, DatabaseError);

create_exception!(redb, Corrupted, StorageError);
create_exception!(redb, ValueTooLarge, StorageError);
create_exception!(redb, Io, StorageError);
create_exception!(redb, PreviousIo, StorageError);
create_exception!(redb, DatabaseClosed, StorageError);
create_exception!(redb, LockPoisoned, StorageError);

pub(crate) fn map_database_error(err: ::redb::DatabaseError) -> PyErr {
    let msg = format!("{err}");
    match err {
        ::redb::DatabaseError::DatabaseAlreadyOpen => DatabaseAlreadyOpen::new_err(msg),
        ::redb::DatabaseError::RepairAborted => RepairAborted::new_err(msg),
        ::redb::DatabaseError::UpgradeRequired(_) => UpgradeRequired::new_err(msg),
        ::redb::DatabaseError::TransactionInProgress => TransactionInProgress::new_err(msg),
        ::redb::DatabaseError::Storage(storage) => map_storage_error(storage),
        // redb::DatabaseError is #[non_exhaustive], so the wildcard is required.
        // Fall back to the abstract parent class for unknown future variants.
        _ => DatabaseError::new_err(msg),
    }
}

pub(crate) fn map_storage_error(err: ::redb::StorageError) -> PyErr {
    let msg = format!("{err}");
    match err {
        ::redb::StorageError::Corrupted(_) => Corrupted::new_err(msg),
        ::redb::StorageError::ValueTooLarge(_) => ValueTooLarge::new_err(msg),
        ::redb::StorageError::Io(_) => Io::new_err(msg),
        ::redb::StorageError::PreviousIo => PreviousIo::new_err(msg),
        ::redb::StorageError::DatabaseClosed => DatabaseClosed::new_err(msg),
        ::redb::StorageError::LockPoisoned(_) => LockPoisoned::new_err(msg),
        // redb::StorageError is #[non_exhaustive]; see comment above.
        _ => StorageError::new_err(msg),
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("Error", m.py().get_type::<Error>())?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;
    m.add("StorageError", m.py().get_type::<StorageError>())?;
    m.add(
        "DatabaseAlreadyOpen",
        m.py().get_type::<DatabaseAlreadyOpen>(),
    )?;
    m.add("RepairAborted", m.py().get_type::<RepairAborted>())?;
    m.add("UpgradeRequired", m.py().get_type::<UpgradeRequired>())?;
    m.add(
        "TransactionInProgress",
        m.py().get_type::<TransactionInProgress>(),
    )?;
    m.add("Corrupted", m.py().get_type::<Corrupted>())?;
    m.add("ValueTooLarge", m.py().get_type::<ValueTooLarge>())?;
    m.add("Io", m.py().get_type::<Io>())?;
    m.add("PreviousIo", m.py().get_type::<PreviousIo>())?;
    m.add("DatabaseClosed", m.py().get_type::<DatabaseClosed>())?;
    m.add("LockPoisoned", m.py().get_type::<LockPoisoned>())?;
    Ok(())
}
