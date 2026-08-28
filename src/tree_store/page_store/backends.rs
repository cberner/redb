use crate::StorageBackend;
use crate::db::InternalStorageBackend;
use crate::io;
#[cfg(not(redb_no_std))]
use crate::io::Error;
use crate::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::fmt::{Debug, Formatter};
use core::ops::Range;

/// A backend supplied by a caller, which has only the public [`StorageBackend`], and no locks.
#[derive(Debug)]
pub(crate) struct LocklessBackend {
    inner: Box<dyn StorageBackend>,
}

impl LocklessBackend {
    pub(crate) fn boxed(inner: impl StorageBackend) -> Box<dyn InternalStorageBackend> {
        Box::new(Self {
            inner: Box::new(inner),
        })
    }
}

impl StorageBackend for LocklessBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), io::Error> {
        self.inner.close()
    }
}

impl InternalStorageBackend for LocklessBackend {
    fn locks_expected(&self) -> bool {
        false
    }

    fn try_lock_range(&self, _range: Range<u64>) -> Result<bool, io::Error> {
        Err(unsupported())
    }

    fn try_lock_shared_range(&self, _range: Range<u64>) -> Result<bool, io::Error> {
        Err(unsupported())
    }

    fn unlock_range(&self, _range: Range<u64>) -> Result<(), io::Error> {
        Err(unsupported())
    }

    fn query_lock_range(&self, _range: Range<u64>) -> Result<bool, io::Error> {
        Err(unsupported())
    }
}

#[cfg_attr(redb_no_std, allow(dead_code))]
fn unsupported() -> io::Error {
    io::unsupported("this storage backend does not support file locks")
}

#[cfg(not(redb_no_std))]
#[derive(Debug)]
pub(crate) struct ReadOnlyBackend {
    inner: Box<dyn InternalStorageBackend>,
}

#[cfg(not(redb_no_std))]
impl ReadOnlyBackend {
    pub fn new(inner: Box<dyn InternalStorageBackend>) -> Self {
        Self { inner }
    }
}

#[cfg(not(redb_no_std))]
impl InternalStorageBackend for ReadOnlyBackend {
    fn locks_expected(&self) -> bool {
        self.inner.locks_expected()
    }

    fn try_lock_range(&self, range: Range<u64>) -> Result<bool, Error> {
        self.inner.try_lock_range(range)
    }

    fn try_lock_shared_range(&self, range: Range<u64>) -> Result<bool, Error> {
        self.inner.try_lock_shared_range(range)
    }

    fn unlock_range(&self, range: Range<u64>) -> Result<(), Error> {
        self.inner.unlock_range(range)
    }

    fn query_lock_range(&self, range: Range<u64>) -> Result<bool, Error> {
        self.inner.query_lock_range(range)
    }
}

#[cfg(not(redb_no_std))]
impl StorageBackend for ReadOnlyBackend {
    fn len(&self) -> Result<u64, Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, _len: u64) -> Result<(), Error> {
        unreachable!()
    }

    fn sync_data(&self) -> Result<(), Error> {
        unreachable!()
    }

    fn write(&self, _offset: u64, _data: &[u8]) -> Result<(), Error> {
        unreachable!()
    }

    fn close(&self) -> Result<(), Error> {
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
    fn out_of_range() -> io::Error {
        io::invalid_input("Index out-of-range.")
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
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.read().len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let guard = self.read();
        let offset = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        if offset + out.len() <= guard.len() {
            out.copy_from_slice(&guard[offset..offset + out.len()]);
            Ok(())
        } else {
            Err(Self::out_of_range())
        }
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        let mut guard = self.write();
        let len = usize::try_from(len).map_err(|_| Self::out_of_range())?;
        guard.resize(len, 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut guard = self.write();
        let offset = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        if offset + data.len() <= guard.len() {
            guard[offset..offset + data.len()].copy_from_slice(data);
            Ok(())
        } else {
            Err(Self::out_of_range())
        }
    }
}

#[cfg(test)]
mod test {
    use super::{InMemoryBackend, LocklessBackend};
    use crate::StorageBackend;
    use crate::db::FULL_RANGE;

    /// A caller's backend has no locks to offer, so a database on one is a single process's
    #[test]
    fn a_caller_supplied_backend_reports_the_locks_unsupported() {
        let backend = LocklessBackend::boxed(InMemoryBackend::new());
        for result in [
            backend.try_lock_range(FULL_RANGE).err(),
            backend.try_lock_shared_range(FULL_RANGE).err(),
            backend.unlock_range(FULL_RANGE).err(),
            backend.query_lock_range(FULL_RANGE).err(),
        ] {
            let err = result.expect("the locks are unsupported");
            assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        }

        // ... and it is not a platform limitation worth reporting
        assert!(!backend.locks_expected());

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
