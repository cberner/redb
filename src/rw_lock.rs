use std::ops::{Deref, DerefMut};

#[cfg(feature = "parking_lot")]
type InnerRwLock<T> = parking_lot::RwLock<T>;
#[cfg(not(feature = "parking_lot"))]
type InnerRwLock<T> = std::sync::RwLock<T>;

#[cfg(feature = "parking_lot")]
type InnerRwLockReadGuard<'rwlock, T> = parking_lot::RwLockReadGuard<'rwlock, T>;
#[cfg(not(feature = "parking_lot"))]
type InnerRwLockReadGuard<'rwlock, T> = std::sync::RwLockReadGuard<'rwlock, T>;

#[cfg(feature = "parking_lot")]
type InnerRwLockWriteGuard<'rwlock, T> = parking_lot::RwLockWriteGuard<'rwlock, T>;
#[cfg(not(feature = "parking_lot"))]
type InnerRwLockWriteGuard<'rwlock, T> = std::sync::RwLockWriteGuard<'rwlock, T>;

/// A reader-writer lock.
///
/// This type of lock allows a number of readers or at most one writer at any
/// point in time. The write portion of this lock typically allows modification
/// of the underlying data (exclusive access) and the read portion of this lock
/// typically allows for read-only access (shared access).
///
/// In comparison, a [`Mutex`] does not distinguish between readers or writers
/// that acquire the lock, therefore blocking any threads waiting for the lock to
/// become available. An `RwLock` will allow any number of readers to acquire the
/// lock as long as a writer is not holding the lock.
///
/// The type parameter `T` represents the data that this lock protects. It is
/// required that `T` satisfies [`Send`] to be shared across threads and
/// [`Sync`] to allow concurrent access through readers. The RAII guards
/// returned from the locking methods implement [`Deref`] (and [`DerefMut`]
/// for the `write` methods) to allow access to the content of the lock.
///
/// # Poisoning
///
/// An `RwLock` might become poisoned on a panic. Note, however, that an `RwLock`
/// may only be poisoned if a panic occurs while it is locked exclusively (write
/// mode). If a panic occurs in any reader, then the lock will not be poisoned.
#[derive(Default, Debug)]
#[repr(transparent)]
pub struct RwLock<T: ?Sized>(InnerRwLock<T>);

/// RAII structure used to release the shared read access of a lock when dropped.
#[repr(transparent)]
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockReadGuard<'rwlock, T: ?Sized>(InnerRwLockReadGuard<'rwlock, T>);

/// RAII structure used to release the exclusive write access of a lock when dropped.
#[repr(transparent)]
#[must_use = "if unused the RwLock will immediately unlock"]
pub struct RwLockWriteGuard<'rwlock, T: ?Sized>(InnerRwLockWriteGuard<'rwlock, T>);

impl<T> RwLock<T> {
    /// Creates a new instance of an `RwLock<T>` which is unlocked.
    pub const fn new(t: T) -> Self {
        Self(InnerRwLock::new(t))
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Locks this `RwLock` with shared read access, blocking the current thread
    /// until it can be acquired.
    ///
    /// The calling thread will be blocked until there are no more writers which
    /// hold the lock. There may be other readers currently inside the lock when
    /// this method returns. This method does not provide any guarantees with
    /// respect to the ordering of whether contentious readers or writers will
    /// acquire the lock first.
    ///
    /// Returns an RAII guard which will release this thread's shared access
    /// once it is dropped.
    ///
    /// # Panics
    ///
    /// This function might panic when called if the lock is already held by the
    /// current thread, or if the `RwLock` is poisoned. An `RwLock` might be
    /// poisoned whenever a writer panics while holding an exclusive lock.
    /// Implementations are not required to implement poisoning.
    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        RwLockReadGuard({
            #[cfg(feature = "parking_lot")]
            {
                self.0.read()
            }
            #[cfg(not(feature = "parking_lot"))]
            self.0.read().expect("Could not acquire write lock.")
        })
    }

    /// Locks this `RwLock` with exclusive write access, blocking the current
    /// thread until it can be acquired.
    ///
    /// This function will not return while other writers or other readers
    /// currently have access to the lock.
    ///
    /// Returns an RAII guard which will drop the write access of this `RwLock`
    /// when dropped.
    ///
    /// # Panics
    ///
    /// This function might panic when called if the lock is already held by the
    /// current thread, or if the `RwLock` is poisoned. An `RwLock` might be
    /// poisoned whenever a writer panics while holding an exclusive lock.
    /// Implementations are not required to implement poisoning.
    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        RwLockWriteGuard({
            #[cfg(feature = "parking_lot")]
            {
                self.0.write()
            }
            #[cfg(not(feature = "parking_lot"))]
            self.0.write().unwrap()
        })
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
