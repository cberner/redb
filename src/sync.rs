// The synchronization primitives redb locks with. std's are used whenever it is available; builds
// without it get the spinning implementations below.

#[cfg(not(redb_no_std))]
pub(crate) use std::sync::{
    Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

#[cfg(redb_no_std)]
pub(crate) use spin::{
    Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

// Compiled for test builds as well, so that the tests below run as part of the normal test suite
// rather than only in the no_std configuration. Nothing outside this module uses it in that case,
// so the parts the tests do not reach are expected to be unused there.
#[cfg(any(redb_no_std, test))]
#[cfg_attr(not(redb_no_std), allow(dead_code))]
mod spin {
    use core::cell::UnsafeCell;
    use core::fmt::{Debug, Formatter};
    use core::hint;
    use core::marker::PhantomData;
    use core::ops::{Deref, DerefMut};
    use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // Stand-ins for the std synchronization primitives, for targets that have no operating system
    // to block on. Their APIs mirror std's -- including the LockResult wrapper -- so that redb's
    // call sites are identical in both builds.
    //
    // A waiter spins instead of parking, which only makes progress if the holder is running on
    // another core. That is the same requirement the rest of redb already has: a single-threaded
    // program never contends for these locks, because it cannot hold two of redb's handles at
    // once, and redb never takes a lock from an interrupt handler.
    //
    // Nothing here poisons. Poisoning exists to stop a later thread from trusting state that a
    // panicking thread left half-updated, which requires unwinding past the guard; a no_std
    // program aborts instead. LockResult is kept anyway, so that redb's error path stays in the
    // type system and the std build keeps its poisoning behavior.

    /// Mirrors [`std::sync::PoisonError`]. These locks never poison, so this is never returned.
    pub(crate) struct PoisonError<T> {
        guard: T,
    }

    impl<T> PoisonError<T> {
        pub(crate) fn into_inner(self) -> T {
            self.guard
        }
    }

    // Hand-written, because std's is also available for guards that are not Debug.
    impl<T> Debug for PoisonError<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            f.write_str("PoisonError { .. }")
        }
    }

    pub(crate) type LockResult<G> = Result<G, PoisonError<G>>;

    /// The lock is held by someone else. Mirrors [`std::sync::TryLockError::WouldBlock`].
    #[derive(Debug)]
    pub(crate) struct WouldBlock;

    pub(crate) type TryLockResult<G> = Result<G, WouldBlock>;

    pub(crate) struct Mutex<T: ?Sized> {
        locked: AtomicBool,
        data: UnsafeCell<T>,
    }

    // Safety: `locked` serializes all access to `data`, so the mutex transfers ownership of the
    // data between threads and shares nothing else.
    unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
    unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

    impl<T> Mutex<T> {
        pub(crate) const fn new(data: T) -> Self {
            Self {
                locked: AtomicBool::new(false),
                data: UnsafeCell::new(data),
            }
        }
    }

    impl<T: ?Sized> Mutex<T> {
        pub(crate) fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
            loop {
                if !self.locked.swap(true, Ordering::Acquire) {
                    return Ok(MutexGuard {
                        mutex: self,
                        _not_send_or_sync: PhantomData,
                    });
                }
                // Spin on a plain load, so that the contended case does not keep bouncing the
                // cache line between cores
                while self.locked.load(Ordering::Relaxed) {
                    hint::spin_loop();
                }
            }
        }

        pub(crate) fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
            if self.locked.swap(true, Ordering::Acquire) {
                Err(WouldBlock)
            } else {
                Ok(MutexGuard {
                    mutex: self,
                    _not_send_or_sync: PhantomData,
                })
            }
        }
    }

    impl<T: Default> Default for Mutex<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T: ?Sized + Debug> Debug for Mutex<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            match self.try_lock() {
                Ok(guard) => f.debug_struct("Mutex").field("data", &&*guard).finish(),
                Err(_) => f.write_str("Mutex { <locked> }"),
            }
        }
    }

    pub(crate) struct MutexGuard<'a, T: ?Sized> {
        mutex: &'a Mutex<T>,
        // `&Mutex<T>` is the guard's only real field, and `Mutex<T>` is `Sync` for `T: Send`, so
        // the auto impls would make the guard `Sync` -- and `Send` -- whenever `T: Send`. Two
        // threads sharing one guard could then reach `&T` concurrently through `Deref` for a `T`
        // that is not `Sync`, which is a data race. Opt out of both auto impls and restate std's
        // bounds below.
        _not_send_or_sync: PhantomData<*const ()>,
    }

    // Safety: sharing the guard is exactly sharing the `&T` that `Deref` hands out, which is sound
    // when `T: Sync`. Matches `std::sync::MutexGuard`, which is also never `Send`.
    unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

    impl<T: ?Sized> Deref for MutexGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &T {
            // Safety: the guard's existence proves the lock is held
            unsafe { &*self.mutex.data.get() }
        }
    }

    impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            // Safety: the guard's existence proves the lock is held exclusively
            unsafe { &mut *self.mutex.data.get() }
        }
    }

    impl<T: ?Sized> Drop for MutexGuard<'_, T> {
        fn drop(&mut self) {
            self.mutex.locked.store(false, Ordering::Release);
        }
    }

    impl<T: ?Sized + Debug> Debug for MutexGuard<'_, T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            Debug::fmt(&**self, f)
        }
    }

    // `state` while a writer holds the lock. Any other value is the number of readers.
    const WRITER: usize = usize::MAX;

    pub(crate) struct RwLock<T: ?Sized> {
        state: AtomicUsize,
        data: UnsafeCell<T>,
    }

    // Safety: `state` grants either one writer or any number of readers, so `&mut T` is never
    // handed out concurrently with any other access.
    unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
    unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

    impl<T> RwLock<T> {
        pub(crate) const fn new(data: T) -> Self {
            Self {
                state: AtomicUsize::new(0),
                data: UnsafeCell::new(data),
            }
        }
    }

    impl<T: ?Sized> RwLock<T> {
        pub(crate) fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
            loop {
                let state = self.state.load(Ordering::Relaxed);
                // WRITER - 1 readers is not reachable in practice, but wrapping into WRITER would
                // hand this reader the lock as a writer
                if state >= WRITER - 1 {
                    hint::spin_loop();
                    continue;
                }
                if self
                    .state
                    .compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return Ok(RwLockReadGuard { lock: self });
                }
                hint::spin_loop();
            }
        }

        pub(crate) fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
            while self
                .state
                .compare_exchange_weak(0, WRITER, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                hint::spin_loop();
            }
            Ok(RwLockWriteGuard { lock: self })
        }
    }

    impl<T: Default> Default for RwLock<T> {
        fn default() -> Self {
            Self::new(T::default())
        }
    }

    impl<T: ?Sized + Debug> Debug for RwLock<T> {
        fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("RwLock")
                .field("data", &&*self.read().unwrap())
                .finish()
        }
    }

    // The read and write guards need no equivalent of `MutexGuard`'s marker: `RwLock<T>` is `Sync`
    // only for `T: Send + Sync`, so the auto impls already give the guards `T: Sync`, which is the
    // bound std uses.
    pub(crate) struct RwLockReadGuard<'a, T: ?Sized> {
        lock: &'a RwLock<T>,
    }

    impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &T {
            // Safety: the guard's existence proves no writer holds the lock
            unsafe { &*self.lock.data.get() }
        }
    }

    impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
        fn drop(&mut self) {
            self.lock.state.fetch_sub(1, Ordering::Release);
        }
    }

    pub(crate) struct RwLockWriteGuard<'a, T: ?Sized> {
        lock: &'a RwLock<T>,
    }

    impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &T {
            // Safety: the guard's existence proves the lock is held exclusively
            unsafe { &*self.lock.data.get() }
        }
    }

    impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            // Safety: the guard's existence proves the lock is held exclusively
            unsafe { &mut *self.lock.data.get() }
        }
    }

    impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
        fn drop(&mut self) {
            self.lock.state.store(0, Ordering::Release);
        }
    }

    /// Mirrors [`std::sync::Condvar`], for the single mutex it is paired with.
    #[derive(Default)]
    pub(crate) struct Condvar {
        // Bumped by every notification. A waiter reads it while still holding the mutex, so a
        // notification that lands after the read but before the guard is dropped is not lost.
        notifications: AtomicUsize,
    }

    impl Condvar {
        pub(crate) const fn new() -> Self {
            Self {
                notifications: AtomicUsize::new(0),
            }
        }

        pub(crate) fn wait<'a, T: ?Sized>(
            &self,
            guard: MutexGuard<'a, T>,
        ) -> LockResult<MutexGuard<'a, T>> {
            let mutex = guard.mutex;
            let seen = self.notifications.load(Ordering::Relaxed);
            drop(guard);
            while self.notifications.load(Ordering::Acquire) == seen {
                hint::spin_loop();
            }
            mutex.lock()
        }

        pub(crate) fn notify_one(&self) {
            self.notifications.fetch_add(1, Ordering::Release);
        }
    }

    #[cfg(test)]
    mod test {
        use super::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
        use std::sync::Arc;
        use std::thread;

        // The guards must stay `Sync` for data that is `Sync`. The bound they must *not* have is
        // `T: Send`, which would let two threads reach `&T` at once for a `T` like `Cell`; that
        // direction cannot be asserted here, and is held by the explicit impl on `MutexGuard`.
        const _: () = {
            const fn assert_sync<T: Sync>() {}
            assert_sync::<MutexGuard<'_, u64>>();
            assert_sync::<RwLockReadGuard<'_, u64>>();
            assert_sync::<RwLockWriteGuard<'_, u64>>();
        };

        #[test]
        fn mutex_is_exclusive() {
            let counter = Arc::new(Mutex::new(0u64));
            thread::scope(|s| {
                for _ in 0..4 {
                    let counter = counter.clone();
                    s.spawn(move || {
                        for _ in 0..10000 {
                            *counter.lock().unwrap() += 1;
                        }
                    });
                }
            });
            assert_eq!(*counter.lock().unwrap(), 40000);
        }

        #[test]
        fn mutex_try_lock() {
            let mutex = Mutex::new(7u64);
            let guard = mutex.lock().unwrap();
            assert!(mutex.try_lock().is_err());
            drop(guard);
            assert_eq!(*mutex.try_lock().unwrap(), 7);
        }

        #[test]
        fn mutex_debug() {
            let mutex = Mutex::new(7u64);
            assert!(format!("{mutex:?}").contains('7'));
            let guard = mutex.lock().unwrap();
            assert!(format!("{mutex:?}").contains("locked"));
            drop(guard);
        }

        #[test]
        fn rwlock_readers_are_concurrent() {
            let lock = RwLock::new(5u64);
            let first = lock.read().unwrap();
            let second = lock.read().unwrap();
            assert_eq!(*first + *second, 10);
        }

        #[test]
        fn rwlock_writer_is_exclusive() {
            let lock = Arc::new(RwLock::new(0u64));
            thread::scope(|s| {
                for _ in 0..4 {
                    let lock = lock.clone();
                    s.spawn(move || {
                        for _ in 0..1000 {
                            *lock.write().unwrap() += 1;
                            assert!(*lock.read().unwrap() > 0);
                        }
                    });
                }
            });
            assert_eq!(*lock.read().unwrap(), 4000);
        }

        #[test]
        fn condvar_wakes_waiter() {
            let ready = Arc::new((Mutex::new(false), Condvar::new()));
            let waiter = ready.clone();
            thread::scope(|s| {
                s.spawn(move || {
                    let (mutex, condvar) = &*waiter;
                    let mut ready = mutex.lock().unwrap();
                    while !*ready {
                        ready = condvar.wait(ready).unwrap();
                    }
                });

                let (mutex, condvar) = &*ready;
                *mutex.lock().unwrap() = true;
                condvar.notify_one();
            });
        }
    }
}
