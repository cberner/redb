use std::ops::{Deref, DerefMut};

#[cfg(feature = "parking_lot")]
type InnerMutex<T> = parking_lot::Mutex<T>;
#[cfg(not(feature = "parking_lot"))]
type InnerMutex<T> = std::sync::Mutex<T>;

#[cfg(feature = "parking_lot")]
type InnerMutexGuard<'rwlock, T> = parking_lot::MutexGuard<'rwlock, T>;
#[cfg(not(feature = "parking_lot"))]
type InnerMutexGuard<'rwlock, T> = std::sync::MutexGuard<'rwlock, T>;

/// A mutual exclusion primitive useful for protecting shared data
///
/// This mutex will block threads waiting for the lock to become available. The
/// mutex can be created via a [`new`] constructor. Each mutex has a type parameter
/// which represents the data that it is protecting. The data can only be accessed
/// through the RAII guards returned from [`lock`] and [`try_lock`], which
/// guarantees that the data is only ever accessed when the mutex is locked.
///
/// # Poisoning
///
/// The mutexes in this module implement a strategy called "poisoning" where a
/// mutex is considered poisoned whenever a thread panics while holding the
/// mutex. Once a mutex is poisoned, all other threads are unable to access the
/// data by default as it is likely tainted (some invariant is not being
/// upheld).
///
/// For a mutex, this means that the [`lock`] and [`try_lock`] methods return a
/// [`Result`] which indicates whether a mutex has been poisoned or not. Most
/// usage of a mutex will simply [`unwrap()`] these results, propagating panics
/// among threads to ensure that a possibly invalid invariant is not witnessed.
///
/// A poisoned mutex, however, does not prevent all access to the underlying
/// data. The [`PoisonError`] type has an [`into_inner`] method which will return
/// the guard that would have otherwise been returned on a successful lock. This
/// allows access to the data, despite the lock being poisoned.
///
/// [`new`]: Self::new
/// [`lock`]: Self::lock
/// [`try_lock`]: Self::try_lock
/// [`unwrap()`]: Result::unwrap
/// [`PoisonError`]: super::PoisonError
/// [`into_inner`]: super::PoisonError::into_inner
#[derive(Default, Debug)]
#[repr(transparent)]
pub struct Mutex<T: ?Sized>(InnerMutex<T>);

/// RAII structure used to release the shared read access of a lock when dropped.
#[repr(transparent)]
#[must_use = "if unused the Mutex will immediately unlock"]
#[derive(Debug)]
pub struct MutexGuard<'rwlock, T: ?Sized>(InnerMutexGuard<'rwlock, T>);


impl<T> Mutex<T> {
    /// Creates a new instance of an `RwLock<T>` which is unlocked.
    pub const fn new(t: T) -> Self {
        Self(InnerMutex::new(t))
    }
}

impl<T: ?Sized> Mutex<T> {
    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        MutexGuard({
            #[cfg(feature = "parking_lot")]
            {
                self.0.lock()
            }
            #[cfg(not(feature = "parking_lot"))]
            self.0.lock().expect("Could not acquire write lock.")
        })
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
