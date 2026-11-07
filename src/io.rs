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

// Compiled for test builds as well, so that the tests below run as part of the normal test suite
// rather than only in the no_std configuration.
#[cfg(any(redb_no_std, test))]
#[cfg_attr(not(redb_no_std), allow(dead_code))]
mod no_std {
    use alloc::boxed::Box;
    use core::fmt::{Debug, Display, Formatter};

    /// A stand-in for [`std::io::Error`], for targets without std.
    ///
    /// Each method has the signature of its [`std::io::Error`] namesake, so code that uses it also
    /// compiles where std is enabled. std's `ErrorKind` has no counterpart: redb never reads one
    /// back in this configuration.
    #[derive(Debug)]
    #[non_exhaustive]
    pub struct Error {
        error: Box<dyn core::error::Error + Send + Sync>,
    }

    impl Error {
        /// Creates an error from an arbitrary error payload, as [`std::io::Error::other`] does.
        pub fn other<E>(error: E) -> Self
        where
            E: Into<Box<dyn core::error::Error + Send + Sync>>,
        {
            Self {
                error: error.into(),
            }
        }

        /// Returns a reference to the inner error.
        pub fn get_ref(&self) -> Option<&(dyn core::error::Error + Send + Sync + 'static)> {
            Some(&*self.error)
        }

        /// Returns a mutable reference to the inner error.
        pub fn get_mut(&mut self) -> Option<&mut (dyn core::error::Error + Send + Sync + 'static)> {
            Some(&mut *self.error)
        }

        /// Consumes the error, returning its inner error.
        pub fn into_inner(self) -> Option<Box<dyn core::error::Error + Send + Sync>> {
            Some(self.error)
        }
    }

    impl Display for Error {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            Display::fmt(&*self.error, f)
        }
    }

    // As std's does for an error made by `other()`, reports the inner error's source
    impl core::error::Error for Error {
        fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
            self.error.source()
        }
    }

    #[cfg(test)]
    mod test {
        use super::Error;
        use alloc::borrow::Cow;
        use alloc::boxed::Box;
        use alloc::format;
        use alloc::string::{String, ToString};
        use core::error::Error as _;
        use core::fmt::{Display, Formatter};

        #[derive(Debug)]
        struct Cause;

        impl Display for Cause {
            fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
                f.write_str("cause")
            }
        }

        impl core::error::Error for Cause {}

        #[derive(Debug)]
        struct FlashError(Cause);

        impl Display for FlashError {
            fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
                f.write_str("flash write failed")
            }
        }

        impl core::error::Error for FlashError {
            fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        #[test]
        fn display_is_the_message() {
            let error = Error::other("Index out-of-range.");
            assert_eq!(format!("{error}"), "Index out-of-range.");
            let error = Error::other(format!("page {} is out of range", 7));
            assert_eq!(format!("{error}"), "page 7 is out of range");
        }

        #[test]
        fn wraps_an_error() {
            let mut error = Error::other(FlashError(Cause));
            assert_eq!(error.to_string(), "flash write failed");
            assert_eq!(error.source().unwrap().to_string(), "cause");
            assert!(error.get_ref().unwrap().is::<FlashError>());
            assert!(error.get_mut().unwrap().is::<FlashError>());
            assert!(error.into_inner().unwrap().downcast::<FlashError>().is_ok());
        }

        // Every argument std's `other()` takes, the stand-in takes, with the same result
        #[cfg(not(redb_no_std))]
        #[test]
        fn matches_std() {
            fn both<E>(make: impl Fn() -> E) -> (std::io::Error, Error)
            where
                E: Into<Box<dyn core::error::Error + Send + Sync>>,
            {
                (std::io::Error::other(make()), Error::other(make()))
            }

            let pairs = [
                both(|| "literal"),
                both(|| String::from("owned")),
                both(|| Cow::Borrowed("cow")),
                both(|| Box::<dyn core::error::Error + Send + Sync>::from("boxed")),
                both(|| FlashError(Cause)),
            ];
            for (theirs, ours) in pairs {
                assert_eq!(theirs.to_string(), ours.to_string());
                assert_eq!(
                    theirs.source().map(ToString::to_string),
                    ours.source().map(ToString::to_string)
                );
                assert_eq!(
                    theirs.get_ref().map(ToString::to_string),
                    ours.get_ref().map(ToString::to_string)
                );
            }
        }
    }
}
