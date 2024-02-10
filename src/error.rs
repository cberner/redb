use crate::tree_store::{FILE_FORMAT_VERSION2, MAX_VALUE_LENGTH};
use crate::{ReadTransaction, TypeName};
use std::fmt::{Display, Formatter};
use std::sync::PoisonError;
use std::{io, panic};

/// General errors directly from the storage layer
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// The Database is corrupted
    Corrupted(String),
    /// The value being inserted exceeds the maximum of 3GiB
    ValueTooLarge(usize),
    Io(io::Error),
    LockPoisoned(&'static panic::Location<'static>),
}

impl<T> From<PoisonError<T>> for StorageError {
    fn from(_: PoisonError<T>) -> StorageError {
        StorageError::LockPoisoned(panic::Location::caller())
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> StorageError {
        StorageError::Io(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Error {
        match err {
            StorageError::Corrupted(msg) => Error::Corrupted(msg),
            StorageError::ValueTooLarge(x) => Error::ValueTooLarge(x),
            StorageError::Io(x) => Error::Io(x),
            StorageError::LockPoisoned(location) => Error::LockPoisoned(location),
        }
    }
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Corrupted(msg) => {
                write!(f, "DB corrupted: {msg}")
            }
            StorageError::ValueTooLarge(len) => {
                write!(
                    f,
                    "The value (length={len}) being inserted exceeds the maximum of {}GiB",
                    MAX_VALUE_LENGTH / 1024 / 1024 / 1024
                )
            }
            StorageError::Io(err) => {
                write!(f, "I/O error: {err}")
            }
            StorageError::LockPoisoned(location) => {
                write!(f, "Poisoned internal lock: {location}")
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// Errors related to opening tables
#[derive(Debug)]
#[non_exhaustive]
pub enum TableError {
    /// Table types didn't match.
    TableTypeMismatch {
        table: String,
        key: TypeName,
        value: TypeName,
    },
    /// The table is a multimap table
    TableIsMultimap(String),
    /// The table is not a multimap table
    TableIsNotMultimap(String),
    TypeDefinitionChanged {
        name: TypeName,
        alignment: usize,
        width: Option<usize>,
    },
    /// Table name does not match any table in database
    TableDoesNotExist(String),
    // Tables cannot be opened for writing multiple times, since they could retrieve immutable &
    // mutable references to the same dirty pages, or multiple mutable references via insert_reserve()
    TableAlreadyOpen(String, &'static panic::Location<'static>),
    /// Error from underlying storage
    Storage(StorageError),
}

impl TableError {
    pub(crate) fn into_storage_error_or_corrupted(self, msg: &str) -> StorageError {
        match self {
            TableError::TableTypeMismatch { .. }
            | TableError::TableIsMultimap(_)
            | TableError::TableIsNotMultimap(_)
            | TableError::TypeDefinitionChanged { .. }
            | TableError::TableDoesNotExist(_)
            | TableError::TableAlreadyOpen(_, _) => {
                StorageError::Corrupted(format!("{}: {}", msg, &self))
            }
            TableError::Storage(storage) => storage,
        }
    }
}

impl From<TableError> for Error {
    fn from(err: TableError) -> Error {
        match err {
            TableError::TypeDefinitionChanged {
                name,
                alignment,
                width,
            } => Error::TypeDefinitionChanged {
                name,
                alignment,
                width,
            },
            TableError::TableTypeMismatch { table, key, value } => {
                Error::TableTypeMismatch { table, key, value }
            }
            TableError::TableIsMultimap(table) => Error::TableIsMultimap(table),
            TableError::TableIsNotMultimap(table) => Error::TableIsNotMultimap(table),
            TableError::TableDoesNotExist(table) => Error::TableDoesNotExist(table),
            TableError::TableAlreadyOpen(name, location) => Error::TableAlreadyOpen(name, location),
            TableError::Storage(storage) => storage.into(),
        }
    }
}

impl From<StorageError> for TableError {
    fn from(err: StorageError) -> TableError {
        TableError::Storage(err)
    }
}

impl Display for TableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableError::TypeDefinitionChanged {
                name,
                alignment,
                width,
            } => {
                write!(
                    f,
                    "Current definition of {} does not match stored definition (width={:?}, alignment={})",
                    name.name(),
                    width,
                    alignment,
                )
            }
            TableError::TableTypeMismatch { table, key, value } => {
                write!(
                    f,
                    "{table} is of type Table<{}, {}>",
                    key.name(),
                    value.name(),
                )
            }
            TableError::TableIsMultimap(table) => {
                write!(f, "{table} is a multimap table")
            }
            TableError::TableIsNotMultimap(table) => {
                write!(f, "{table} is not a multimap table")
            }
            TableError::TableDoesNotExist(table) => {
                write!(f, "Table '{table}' does not exist")
            }
            TableError::TableAlreadyOpen(name, location) => {
                write!(f, "Table '{name}' already opened at: {location}")
            }
            TableError::Storage(storage) => storage.fmt(f),
        }
    }
}

impl std::error::Error for TableError {}

/// Errors related to opening a database
#[derive(Debug)]
#[non_exhaustive]
pub enum DatabaseError {
    /// The Database is already open. Cannot acquire lock.
    DatabaseAlreadyOpen,
    /// [crate::RepairSession::abort] was called.
    RepairAborted,
    /// The database file is in an old file format and must be manually upgraded
    UpgradeRequired(u8),
    /// Error from underlying storage
    Storage(StorageError),
}

impl From<DatabaseError> for Error {
    fn from(err: DatabaseError) -> Error {
        match err {
            DatabaseError::DatabaseAlreadyOpen => Error::DatabaseAlreadyOpen,
            DatabaseError::RepairAborted => Error::RepairAborted,
            DatabaseError::UpgradeRequired(x) => Error::UpgradeRequired(x),
            DatabaseError::Storage(storage) => storage.into(),
        }
    }
}

impl From<io::Error> for DatabaseError {
    fn from(err: io::Error) -> DatabaseError {
        DatabaseError::Storage(StorageError::Io(err))
    }
}

impl From<StorageError> for DatabaseError {
    fn from(err: StorageError) -> DatabaseError {
        DatabaseError::Storage(err)
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::UpgradeRequired(actual) => {
                write!(f, "Manual upgrade required. Expected file format version {FILE_FORMAT_VERSION2}, but file is version {actual}")
            }
            DatabaseError::RepairAborted => {
                write!(f, "Database repair aborted.")
            }
            DatabaseError::DatabaseAlreadyOpen => {
                write!(f, "Database already open. Cannot acquire lock.")
            }
            DatabaseError::Storage(storage) => storage.fmt(f),
        }
    }
}

impl std::error::Error for DatabaseError {}

/// Errors related to savepoints
#[derive(Debug)]
#[non_exhaustive]
pub enum SavepointError {
    /// This savepoint is invalid or cannot be created.
    ///
    /// Savepoints become invalid when an older savepoint is restored after it was created,
    /// and savepoints cannot be created if the transaction is "dirty" (any tables have been opened)
    InvalidSavepoint,
    /// Error from underlying storage
    Storage(StorageError),
}

impl From<SavepointError> for Error {
    fn from(err: SavepointError) -> Error {
        match err {
            SavepointError::InvalidSavepoint => Error::InvalidSavepoint,
            SavepointError::Storage(storage) => storage.into(),
        }
    }
}

impl From<StorageError> for SavepointError {
    fn from(err: StorageError) -> SavepointError {
        SavepointError::Storage(err)
    }
}

impl Display for SavepointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SavepointError::InvalidSavepoint => {
                write!(f, "Savepoint is invalid or cannot be created.")
            }
            SavepointError::Storage(storage) => storage.fmt(f),
        }
    }
}

impl std::error::Error for SavepointError {}

/// Errors related to compaction
#[derive(Debug)]
#[non_exhaustive]
pub enum CompactionError {
    /// A persistent savepoint exists
    PersistentSavepointExists,
    /// A ephemeral savepoint exists
    EphemeralSavepointExists,
    /// Error from underlying storage
    Storage(StorageError),
}

impl From<CompactionError> for Error {
    fn from(err: CompactionError) -> Error {
        match err {
            CompactionError::PersistentSavepointExists => Error::PersistentSavepointExists,
            CompactionError::EphemeralSavepointExists => Error::EphemeralSavepointExists,
            CompactionError::Storage(storage) => storage.into(),
        }
    }
}

impl From<StorageError> for CompactionError {
    fn from(err: StorageError) -> CompactionError {
        CompactionError::Storage(err)
    }
}

impl Display for CompactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionError::PersistentSavepointExists => {
                write!(
                    f,
                    "Persistent savepoint exists. Operation cannot be performed."
                )
            }
            CompactionError::EphemeralSavepointExists => {
                write!(
                    f,
                    "Ephemeral savepoint exists. Operation cannot be performed."
                )
            }
            CompactionError::Storage(storage) => storage.fmt(f),
        }
    }
}

impl std::error::Error for CompactionError {}

/// Errors related to transactions
#[derive(Debug)]
#[non_exhaustive]
pub enum TransactionError {
    /// Error from underlying storage
    Storage(StorageError),
    /// The transaction is still referenced by a table or other object
    ReadTransactionStillInUse(ReadTransaction),
}

impl TransactionError {
    pub(crate) fn into_storage_error(self) -> StorageError {
        match self {
            TransactionError::Storage(storage) => storage,
            _ => unreachable!(),
        }
    }
}

impl From<TransactionError> for Error {
    fn from(err: TransactionError) -> Error {
        match err {
            TransactionError::Storage(storage) => storage.into(),
            TransactionError::ReadTransactionStillInUse(txn) => {
                Error::ReadTransactionStillInUse(txn)
            }
        }
    }
}

impl From<StorageError> for TransactionError {
    fn from(err: StorageError) -> TransactionError {
        TransactionError::Storage(err)
    }
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::Storage(storage) => storage.fmt(f),
            TransactionError::ReadTransactionStillInUse(_) => {
                write!(f, "Transaction still in use")
            }
        }
    }
}

impl std::error::Error for TransactionError {}

/// Errors related to committing transactions
#[derive(Debug)]
#[non_exhaustive]
pub enum CommitError {
    /// Error from underlying storage
    Storage(StorageError),
}

impl CommitError {
    pub(crate) fn into_storage_error(self) -> StorageError {
        match self {
            CommitError::Storage(storage) => storage,
        }
    }
}

impl From<CommitError> for Error {
    fn from(err: CommitError) -> Error {
        match err {
            CommitError::Storage(storage) => storage.into(),
        }
    }
}

impl From<StorageError> for CommitError {
    fn from(err: StorageError) -> CommitError {
        CommitError::Storage(err)
    }
}

impl Display for CommitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::Storage(storage) => storage.fmt(f),
        }
    }
}

impl std::error::Error for CommitError {}

/// Superset of all other errors that can occur. Convenience enum so that users can convert all errors into a single type
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The Database is already open. Cannot acquire lock.
    DatabaseAlreadyOpen,
    /// This savepoint is invalid or cannot be created.
    ///
    /// Savepoints become invalid when an older savepoint is restored after it was created,
    /// and savepoints cannot be created if the transaction is "dirty" (any tables have been opened)
    InvalidSavepoint,
    /// [crate::RepairSession::abort] was called.
    RepairAborted,
    /// A persistent savepoint exists
    PersistentSavepointExists,
    /// An Ephemeral savepoint exists
    EphemeralSavepointExists,
    /// The Database is corrupted
    Corrupted(String),
    /// The database file is in an old file format and must be manually upgraded
    UpgradeRequired(u8),
    /// The value being inserted exceeds the maximum of 3GiB
    ValueTooLarge(usize),
    /// Table types didn't match.
    TableTypeMismatch {
        table: String,
        key: TypeName,
        value: TypeName,
    },
    /// The table is a multimap table
    TableIsMultimap(String),
    /// The table is not a multimap table
    TableIsNotMultimap(String),
    TypeDefinitionChanged {
        name: TypeName,
        alignment: usize,
        width: Option<usize>,
    },
    /// Table name does not match any table in database
    TableDoesNotExist(String),
    // Tables cannot be opened for writing multiple times, since they could retrieve immutable &
    // mutable references to the same dirty pages, or multiple mutable references via insert_reserve()
    TableAlreadyOpen(String, &'static panic::Location<'static>),
    Io(io::Error),
    LockPoisoned(&'static panic::Location<'static>),
    /// The transaction is still referenced by a table or other object
    ReadTransactionStillInUse(ReadTransaction),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Error {
        Error::LockPoisoned(panic::Location::caller())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Corrupted(msg) => {
                write!(f, "DB corrupted: {msg}")
            }
            Error::UpgradeRequired(actual) => {
                write!(f, "Manual upgrade required. Expected file format version {FILE_FORMAT_VERSION2}, but file is version {actual}")
            }
            Error::ValueTooLarge(len) => {
                write!(
                    f,
                    "The value (length={len}) being inserted exceeds the maximum of {}GiB",
                    MAX_VALUE_LENGTH / 1024 / 1024 / 1024
                )
            }
            Error::TypeDefinitionChanged {
                name,
                alignment,
                width,
            } => {
                write!(
                    f,
                    "Current definition of {} does not match stored definition (width={:?}, alignment={})",
                    name.name(),
                    width,
                    alignment,
                )
            }
            Error::TableTypeMismatch { table, key, value } => {
                write!(
                    f,
                    "{table} is of type Table<{}, {}>",
                    key.name(),
                    value.name(),
                )
            }
            Error::TableIsMultimap(table) => {
                write!(f, "{table} is a multimap table")
            }
            Error::TableIsNotMultimap(table) => {
                write!(f, "{table} is not a multimap table")
            }
            Error::TableDoesNotExist(table) => {
                write!(f, "Table '{table}' does not exist")
            }
            Error::TableAlreadyOpen(name, location) => {
                write!(f, "Table '{name}' already opened at: {location}")
            }
            Error::Io(err) => {
                write!(f, "I/O error: {err}")
            }
            Error::LockPoisoned(location) => {
                write!(f, "Poisoned internal lock: {location}")
            }
            Error::DatabaseAlreadyOpen => {
                write!(f, "Database already open. Cannot acquire lock.")
            }
            Error::RepairAborted => {
                write!(f, "Database repair aborted.")
            }
            Error::PersistentSavepointExists => {
                write!(
                    f,
                    "Persistent savepoint exists. Operation cannot be performed."
                )
            }
            Error::EphemeralSavepointExists => {
                write!(
                    f,
                    "Ephemeral savepoint exists. Operation cannot be performed."
                )
            }
            Error::InvalidSavepoint => {
                write!(f, "Savepoint is invalid or cannot be created.")
            }
            Error::ReadTransactionStillInUse(_) => {
                write!(f, "Transaction still in use")
            }
        }
    }
}

impl std::error::Error for Error {}
