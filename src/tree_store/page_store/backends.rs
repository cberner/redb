use crate::BackendError;
use crate::StorageBackend;
use crate::db::BackendIoError;
use crate::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt::{Debug, Formatter};
use core::ops::Bound;

#[derive(Debug)]
pub(crate) struct ReadOnlyBackend {
    inner: Box<dyn StorageBackend>,
}

impl ReadOnlyBackend {
    pub fn new(inner: Box<dyn StorageBackend>) -> Self {
        Self { inner }
    }
}

impl StorageBackend for ReadOnlyBackend {
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.inner.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.query_lock_range(start, end)
    }

    fn len(&self) -> Result<u64, BackendIoError> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendIoError> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, _len: u64) -> Result<(), BackendIoError> {
        unreachable!()
    }

    fn sync_data(&self) -> Result<(), BackendIoError> {
        unreachable!()
    }

    fn write(&self, _offset: u64, _data: &[u8]) -> Result<(), BackendIoError> {
        unreachable!()
    }

    fn close(&self) -> Result<(), BackendIoError> {
        self.inner.close()
    }
}

/// Acts as temporal in-memory database storage.
#[derive(Default)]
pub struct InMemoryBackend(RwLock<Vec<u8>>);

// Hand-written: the derived impl would print every byte of the database, and opening a database
// formats its backend into the debug log when the "logging" feature is enabled. The length stays
// meaningful when a writer panicked, so a poisoned lock is read through rather than branched on
impl Debug for InMemoryBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let guard = self.0.read().unwrap_or_else(|error| error.into_inner());
        f.debug_struct("InMemoryBackend")
            .field("len", &guard.len())
            .finish()
    }
}

impl InMemoryBackend {
    fn out_of_range() -> BackendError {
        BackendError::invalid_input("Index out-of-range.")
    }
}

impl InMemoryBackend {
    /// Creates a new, empty memory backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets a read guard for this backend.
    fn read(&self) -> RwLockReadGuard<'_, Vec<u8>> {
        self.0.read().expect("Could not acquire read lock.")
    }

    /// Gets a write guard for this backend.
    fn write(&self) -> RwLockWriteGuard<'_, Vec<u8>> {
        self.0.write().expect("Could not acquire write lock.")
    }
}

impl StorageBackend for InMemoryBackend {
    fn len(&self) -> Result<u64, BackendIoError> {
        Ok(self.read().len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendIoError> {
        let guard = self.read();
        let offset = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        let source = guard
            .get(offset..offset + out.len())
            .ok_or_else(Self::out_of_range)?;
        out.copy_from_slice(source);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), BackendIoError> {
        let mut guard = self.write();
        let len = usize::try_from(len).map_err(|_| Self::out_of_range())?;
        guard.resize(len, 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), BackendIoError> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendIoError> {
        let mut guard = self.write();
        let offset = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        let target = guard
            .get_mut(offset..offset + data.len())
            .ok_or_else(Self::out_of_range)?;
        target.copy_from_slice(data);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::InMemoryBackend;
    use crate::StorageBackend;
    use crate::db::FULL_RANGE;

    /// Existing backends remain usable without implementing the optional lock methods.
    #[test]
    fn default_locks_are_unsupported() {
        let backend = InMemoryBackend::new();
        for result in [
            backend.try_lock_range(FULL_RANGE.0, FULL_RANGE.1).err(),
            backend
                .try_lock_shared_range(FULL_RANGE.0, FULL_RANGE.1)
                .err(),
            backend.lock_range(FULL_RANGE.0, FULL_RANGE.1).err(),
            backend.lock_shared_range(FULL_RANGE.0, FULL_RANGE.1).err(),
            backend.unlock_range(FULL_RANGE.0, FULL_RANGE.1).err(),
            backend.query_lock_range(FULL_RANGE.0, FULL_RANGE.1).err(),
        ] {
            let err = result.expect("the locks are unsupported");
            assert!(matches!(err, crate::BackendError::Unsupported));
        }

        // ... and the storage underneath it still works
        backend.set_len(1024).unwrap();
        assert_eq!(backend.len().unwrap(), 1024);
    }

    #[test]
    fn debug_reports_length_not_contents() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        assert_eq!(format!("{backend:?}"), "InMemoryBackend { len: 1024 }");
    }

    // A writer that panics poisons the lock; the length is still reported. Poisoning requires
    // unwinding past the guard, so the test does too.
    #[test]
    #[cfg(panic = "unwind")]
    fn debug_reads_through_a_poisoned_lock() {
        let backend = std::sync::Arc::new(InMemoryBackend::new());
        backend.set_len(512).unwrap();
        let poisoner = backend.clone();
        let result = std::thread::spawn(move || {
            let _guard = poisoner.0.write();
            panic!("poison the lock");
        })
        .join();
        assert!(result.is_err());
        assert_eq!(format!("{backend:?}"), "InMemoryBackend { len: 512 }");
    }
}
