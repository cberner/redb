use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug)]
pub enum Error {
    Corrupted(String),
    TableTypeMismatch(String),
    DoesNotExist(String),
    Io(io::Error),
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
