use crate::db::{FULL_RANGE, InternalStorageBackend};
use crate::{DatabaseError, Result, StorageBackend};
use std::fs::{File, TryLockError};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    whole_file_locked: AtomicBool,
    file: Mutex<File>,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Ok(Self {
            whole_file_locked: AtomicBool::new(false),
            file: Mutex::new(file),
        })
    }

    fn lock_whole_file(&self, shared: bool) -> Result<bool, io::Error> {
        let file = self.file.lock().unwrap();
        let result = if shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };

        match result {
            Ok(()) => {
                self.whole_file_locked.store(true, Ordering::Release);
                Ok(true)
            }
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(TryLockError::Error(err)) => Err(err),
        }
    }
}

/// This backend has no byte-range locks. Only the whole storage can be locked, which a whole-file
/// lock does, so that is the one range it supports.
impl InternalStorageBackend for FileBackend {
    fn try_lock_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        if range == FULL_RANGE {
            self.lock_whole_file(false)
        } else {
            Err(unsupported())
        }
    }

    fn try_lock_shared_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        if range == FULL_RANGE {
            self.lock_whole_file(true)
        } else {
            Err(unsupported())
        }
    }

    fn unlock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        if range != FULL_RANGE {
            return Err(unsupported());
        }
        if self.whole_file_locked.swap(false, Ordering::AcqRel) {
            self.file.lock().unwrap().unlock()?;
        }

        Ok(())
    }
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "byte-range locks are not supported on this platform",
    )
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.lock().unwrap().metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(out)?;
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.lock().unwrap().set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.file.lock().unwrap().sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }
}
