//! Compatibility shims that re-export std types when the `std` feature is enabled, and provide
//! no_std-compatible replacements otherwise. All sync primitives here expose the same API as
//! `std::sync`, so call sites do not need to change based on feature configuration.

/// Items that std makes available via its prelude. We re-export them from `alloc` for `no_std`
/// builds so each file can `use crate::std_compat::prelude::*;` to keep the same call sites.
#[cfg(not(feature = "std"))]
pub(crate) mod prelude {
    pub use alloc::borrow::ToOwned;
    pub use alloc::boxed::Box;
    pub use alloc::format;
    pub use alloc::string::{String, ToString};
    pub use alloc::vec;
    pub use alloc::vec::Vec;
}

#[cfg(feature = "std")]
pub(crate) mod prelude {}

#[cfg(feature = "std")]
pub(crate) use std::collections::{HashMap, HashSet};
#[cfg(feature = "std")]
pub(crate) use std::io;
#[cfg(feature = "std")]
pub(crate) use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

#[cfg(feature = "std")]
pub(crate) fn thread_panicking() -> bool {
    std::thread::panicking()
}

#[cfg(not(feature = "std"))]
pub(crate) use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
pub(crate) use hashbrown::{HashMap, HashSet};
#[cfg(not(feature = "std"))]
pub(crate) use nostd::{
    Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, io,
};

#[cfg(not(feature = "std"))]
pub(crate) fn thread_panicking() -> bool {
    false
}

#[cfg(not(feature = "std"))]
mod nostd {
    use core::fmt;
    use core::ops::{Deref, DerefMut};

    pub struct PoisonError<T> {
        guard: T,
    }

    impl<T> PoisonError<T> {
        pub fn new(guard: T) -> Self {
            Self { guard }
        }

        pub fn into_inner(self) -> T {
            self.guard
        }
    }

    impl<T> fmt::Debug for PoisonError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("PoisonError")
        }
    }

    #[derive(Debug, Default)]
    pub struct Mutex<T: ?Sized>(spin::Mutex<T>);

    impl<T> Mutex<T> {
        pub const fn new(value: T) -> Self {
            Self(spin::Mutex::new(value))
        }
    }

    impl<T: ?Sized> Mutex<T> {
        pub fn lock(&self) -> Result<MutexGuard<'_, T>, PoisonError<MutexGuard<'_, T>>> {
            Ok(MutexGuard(self.0.lock()))
        }
    }

    #[must_use = "MutexGuards must be held until the critical section ends"]
    pub struct MutexGuard<'a, T: ?Sized>(spin::MutexGuard<'a, T>);

    impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            (**self).fmt(f)
        }
    }

    impl<T: ?Sized> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    #[derive(Debug, Default)]
    pub struct RwLock<T: ?Sized>(spin::RwLock<T>);

    impl<T> RwLock<T> {
        pub const fn new(value: T) -> Self {
            Self(spin::RwLock::new(value))
        }
    }

    impl<T: ?Sized> RwLock<T> {
        pub fn read(&self) -> Result<RwLockReadGuard<'_, T>, PoisonError<RwLockReadGuard<'_, T>>> {
            Ok(RwLockReadGuard(self.0.read()))
        }

        pub fn write(
            &self,
        ) -> Result<RwLockWriteGuard<'_, T>, PoisonError<RwLockWriteGuard<'_, T>>> {
            Ok(RwLockWriteGuard(self.0.write()))
        }
    }

    #[must_use = "RwLockReadGuards must be held until the critical section ends"]
    pub struct RwLockReadGuard<'a, T: ?Sized>(spin::RwLockReadGuard<'a, T>);

    impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            (**self).fmt(f)
        }
    }

    impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    #[must_use = "RwLockWriteGuards must be held until the critical section ends"]
    pub struct RwLockWriteGuard<'a, T: ?Sized>(spin::RwLockWriteGuard<'a, T>);

    impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            (**self).fmt(f)
        }
    }

    impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0
        }
    }

    impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            &mut self.0
        }
    }

    // no_std has no OS thread blocking, so Condvar::wait will panic if called.
    // redb only reaches wait() when a second writer is blocking on a first, which requires
    // multiple threads. On single-threaded no_std targets this path is unreachable.
    #[derive(Debug, Default)]
    pub struct Condvar;

    impl Condvar {
        pub const fn new() -> Self {
            Self
        }

        pub fn wait<'a, T>(
            &self,
            _guard: MutexGuard<'a, T>,
        ) -> Result<MutexGuard<'a, T>, PoisonError<MutexGuard<'a, T>>> {
            panic!("Condvar::wait is not supported in no_std builds of redb");
        }

        pub fn notify_one(&self) {}
    }

    pub mod io {
        use alloc::string::{String, ToString};
        use core::fmt;

        #[non_exhaustive]
        #[derive(Debug, Copy, Clone, Eq, PartialEq)]
        pub enum ErrorKind {
            NotFound,
            PermissionDenied,
            AlreadyExists,
            InvalidInput,
            InvalidData,
            UnexpectedEof,
            WriteZero,
            Interrupted,
            Unsupported,
            OutOfMemory,
            Other,
        }

        impl fmt::Display for ErrorKind {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{self:?}")
            }
        }

        /// A minimal portable version of `std::io::Error` that is available in `no_std` builds.
        #[derive(Debug)]
        pub struct Error {
            kind: ErrorKind,
            message: Option<String>,
        }

        impl Error {
            pub fn new<M: ToString>(kind: ErrorKind, message: M) -> Self {
                Self {
                    kind,
                    message: Some(message.to_string()),
                }
            }

            pub fn kind(&self) -> ErrorKind {
                self.kind
            }
        }

        impl From<ErrorKind> for Error {
            fn from(kind: ErrorKind) -> Self {
                Self {
                    kind,
                    message: None,
                }
            }
        }

        impl fmt::Display for Error {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if let Some(msg) = &self.message {
                    write!(f, "{}: {msg}", self.kind)
                } else {
                    write!(f, "{}", self.kind)
                }
            }
        }
    }
}
