use std::fmt::{Display, Formatter};
use std::sync::PoisonError;
use std::{io, panic};

#[derive(Debug)]
pub enum Error {
    Corrupted(String),
    TableTypeMismatch(String),
    DbSizeMismatch {
        path: String,
        size: usize,
        requested_size: usize,
    },
    DoesNotExist(String),
    LeakedWriteTransaction(&'static panic::Location<'static>),
    OutOfSpace,
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
            Error::TableTypeMismatch(msg) => {
                write!(f, "{}", msg)
            }
            Error::DbSizeMismatch {
                path,
                size,
                requested_size,
            } => {
                write!(
                    f,
                    "Database {} is of size {} bytes, but you requested {} bytes",
                    path, size, requested_size
                )
            }
            Error::DoesNotExist(msg) => {
                write!(f, "{}", msg)
            }
            Error::LeakedWriteTransaction(location) => {
                write!(f, "Leaked write transaction: {}", location)
            }
            Error::OutOfSpace => {
                write!(f, "Database is out of space")
            }
            Error::Io(err) => {
                write!(f, "I/O error: {}", err)
            }
            Error::LockPoisoned(location) => {
                write!(f, "Poisoned internal lock: {}", location)
            }
        }
    }
}

impl std::error::Error for Error {}
