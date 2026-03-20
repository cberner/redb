use crate::StorageBackend;
use crate::compat::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use crate::error::BackendError;
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::string::String;
use alloc::vec::Vec;

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
    fn len(&self) -> Result<u64, BackendError> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, _len: u64) -> Result<(), BackendError> {
        unreachable!()
    }

    fn sync_data(&self) -> Result<(), BackendError> {
        unreachable!()
    }

    fn write(&self, _offset: u64, _data: &[u8]) -> Result<(), BackendError> {
        unreachable!()
    }

    fn close(&self) -> Result<(), BackendError> {
        self.inner.close()
    }
}

/// Acts as temporal in-memory database storage.
#[derive(Debug, Default)]
pub struct InMemoryBackend(RwLock<Vec<u8>>);

impl InMemoryBackend {
    fn out_of_range() -> BackendError {
        #[cfg(feature = "std")]
        {
            BackendError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Index out-of-range.",
            ))
        }
        #[cfg(not(feature = "std"))]
        {
            BackendError::Message(String::from("Index out-of-range."))
        }
    }
}

impl InMemoryBackend {
    /// Creates a new, empty memory backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets a read guard for this backend.
    fn read(&self) -> RwLockReadGuard<'_, Vec<u8>> {
        self.0.read()
    }

    /// Gets a write guard for this backend.
    fn write(&self) -> RwLockWriteGuard<'_, Vec<u8>> {
        self.0.write()
    }
}

impl StorageBackend for InMemoryBackend {
    fn len(&self) -> Result<u64, BackendError> {
        Ok(self.read().len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        let guard = self.read();
        let offset = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        if offset + out.len() <= guard.len() {
            out.copy_from_slice(&guard[offset..offset + out.len()]);
            Ok(())
        } else {
            Err(Self::out_of_range())
        }
    }

    fn set_len(&self, len: u64) -> Result<(), BackendError> {
        let mut guard = self.write();
        let len = usize::try_from(len).map_err(|_| Self::out_of_range())?;
        if guard.len() < len {
            let additional = len - guard.len();
            guard.reserve(additional);
            for _ in 0..additional {
                guard.push(0);
            }
        } else {
            guard.truncate(len);
        }

        Ok(())
    }

    fn sync_data(&self) -> Result<(), BackendError> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendError> {
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
