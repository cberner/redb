use crate::StorageBackend;
use std::io;
use std::sync::*;

/// Acts as temporal in-memory database storage.
#[derive(Debug, Default)]
pub struct MemoryBackend(RwLock<Vec<u8>>);

impl MemoryBackend {
    fn out_of_range() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, "Index out-of-range.")
    }
}

impl MemoryBackend {
    /// Creates a new, empty memory backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a memory backend with the specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(RwLock::new(Vec::with_capacity(capacity)))
    }

    /// Obtains the inner memory for this backend.
    pub fn into_inner(self) -> Vec<u8> {
        self.0.into_inner().expect("Could not get inner vector.")
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

impl From<Vec<u8>> for MemoryBackend {
    fn from(value: Vec<u8>) -> Self {
        Self(RwLock::new(value))
    }
}

impl StorageBackend for MemoryBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.read().len() as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let guard = self.read();
        let offset = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        if offset + len <= guard.len() {
            Ok(guard[offset..offset + len].to_owned())
        } else {
            Err(Self::out_of_range())
        }
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
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

    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
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
