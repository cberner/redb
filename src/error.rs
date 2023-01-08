use crate::tree_store::FILE_FORMAT_VERSION;
use std::fmt::{Display, Formatter};
use std::sync::PoisonError;
use std::{io, panic};

/// Possibles errors in `redb` crate
#[derive(Debug)]
pub enum Error {
    /// The Database is already open. Cannot acquire lock.
    DatabaseAlreadyOpen,
    /// This savepoint is invalid because an older savepoint was restored after it was created
    InvalidSavepoint,
    /// The Database is corrupted
    Corrupted(String),
    /// The database file is in an old file format and must be manually upgraded
    UpgradeRequired(u8),
    /// Table types didn't match.
    TableTypeMismatch(String),
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
            Error::Corrupted(msg) => {
                write!(f, "DB corrupted: {}", msg)
            }
            Error::UpgradeRequired(actual) => {
                write!(f, "Manual upgrade required. Expected file format version {}, but file is version {}", FILE_FORMAT_VERSION, actual)
            }
            Error::TableTypeMismatch(msg) => {
                write!(f, "{}", msg)
            }
            Error::TableDoesNotExist(table) => {
                write!(f, "Table '{}' does not exist", table)
            }
            Error::TableAlreadyOpen(name, location) => {
                write!(f, "Table '{}' already opened at: {}", name, location)
            }
            Error::Io(err) => {
                write!(f, "I/O error: {}", err)
            }
            Error::LockPoisoned(location) => {
                write!(f, "Poisoned internal lock: {}", location)
            }
            Error::DatabaseAlreadyOpen => {
                write!(f, "Database already open. Cannot acquire lock.")
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
