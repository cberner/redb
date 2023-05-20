use crate::tree_store::{FILE_FORMAT_VERSION, MAX_VALUE_LENGTH};
use crate::TypeName;
use std::fmt::{Display, Formatter};
use std::sync::PoisonError;
use std::{io, panic};

/// Possibles errors in `redb` crate
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// For use by fuzzer only
    #[cfg(any(fuzzing, test))]
    SimulatedIOFailure,
    /// The Database is already open. Cannot acquire lock.
    DatabaseAlreadyOpen,
    /// This savepoint is invalid because an older savepoint was restored after it was created
    InvalidSavepoint,
    /// A persistent savepoint exists
    PersistentSavepointExists,
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
            #[cfg(any(fuzzing, test))]
            Error::SimulatedIOFailure => {
                write!(f, "Fuzzer: Simulated I/O error")
            }
            Error::Corrupted(msg) => {
                write!(f, "DB corrupted: {msg}")
            }
            Error::UpgradeRequired(actual) => {
                write!(f, "Manual upgrade required. Expected file format version {FILE_FORMAT_VERSION}, but file is version {actual}")
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
            Error::PersistentSavepointExists => {
                write!(
                    f,
                    "Persistent savepoint exists. Operation cannot be performed."
                )
            }
            Error::InvalidSavepoint => {
                write!(
                    f,
                    "Savepoint is invalid because an older savepoint was already restored."
                )
            }
        }
    }
}

impl std::error::Error for Error {}
