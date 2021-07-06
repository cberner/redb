use std::io;

#[derive(Debug)]
pub enum Error {
    Corrupted(String),
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}
