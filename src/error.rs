use std::fmt::{Display, Formatter};
use std::sync::PoisonError;
use std::{io, panic};

#[derive(Debug)]
pub enum Error {
    Corrupted(String),
    TableTypeMismatch(String),
    DbSizeMismatch(String),
    DoesNotExist(String),
    LeakedWriteTransaction(String),
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
        // TODO: better formatting
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
