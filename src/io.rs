//! The error type that [`StorageBackend`](crate::StorageBackend) reports failures with.
//!
//! On builds with std this is a re-export of [`std::io::Error`], so a backend written against this
//! module is also a backend written against `std::io`. Builds without std get the stand-in below.

#[cfg(not(redb_no_std))]
pub use std::io::Error;

#[cfg(redb_no_std)]
pub use no_std::Error;

// The kinds redb attaches to the errors it raises itself. std's `ErrorKind` carries them where it
// is available, so a std build's errors are unchanged; without std the kind is dropped and only
// the message survives. Keeping these in one place means that if the stand-in ever grows a kind,
// only this module changes.
pub(crate) fn invalid_data(message: &str) -> Error {
    #[cfg(not(redb_no_std))]
    {
        Error::new(std::io::ErrorKind::InvalidData, message)
    }
    #[cfg(redb_no_std)]
    {
        Error::other(message)
    }
}

pub(crate) fn unsupported(message: &str) -> Error {
    #[cfg(not(redb_no_std))]
    {
        Error::new(std::io::ErrorKind::Unsupported, message)
    }
    #[cfg(redb_no_std)]
    {
        Error::other(message)
    }
}

pub(crate) fn invalid_input(message: &str) -> Error {
    #[cfg(not(redb_no_std))]
    {
        Error::new(std::io::ErrorKind::InvalidInput, message)
    }
    #[cfg(redb_no_std)]
    {
        Error::other(message)
    }
}

// Compiled for test builds as well, so that the tests below run as part of the normal test suite
// rather than only in the no_std configuration.
#[cfg(any(redb_no_std, test))]
#[cfg_attr(not(redb_no_std), allow(dead_code))]
mod no_std {
    use alloc::string::String;
    use core::fmt::{Debug, Display, Formatter};

    /// A stand-in for [`std::io::Error`], for targets without std.
    ///
    /// It carries a message, which is what a storage backend has to say about a failure that redb
    /// cannot say for it. std's classifying `ErrorKind` has no counterpart here: redb never reads
    /// one back in this configuration, and the kinds it would list are a guess at what an embedded
    /// backend reports. The type is `non_exhaustive` and its only constructor is
    /// [`other`](Self::other) -- named after [`std::io::Error::other`] -- so that a kind, or
    /// anything else, can be added later without breaking callers.
    #[derive(Debug)]
    #[non_exhaustive]
    pub struct Error {
        message: String,
    }

    impl Error {
        /// Creates an error described by `message`.
        pub fn other(message: impl Into<String>) -> Self {
            Self {
                message: message.into(),
            }
        }
    }

    impl Display for Error {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl core::error::Error for Error {}

    #[cfg(test)]
    mod test {
        use super::Error;
        use alloc::format;

        #[test]
        fn display_is_the_message() {
            let error = Error::other("Index out-of-range.");
            assert_eq!(format!("{error}"), "Index out-of-range.");
        }

        #[test]
        fn accepts_an_owned_message() {
            let error = Error::other(format!("page {} is out of range", 7));
            assert_eq!(format!("{error}"), "page 7 is out of range");
        }
    }
}
