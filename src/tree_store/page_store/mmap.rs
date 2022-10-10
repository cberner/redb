use crate::{Error, Result};
use std::fs::File;
use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix::*;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::*;

pub(crate) struct Mmap {
    file: File,
    _lock: FileLock,
    mmap: MmapInner,
    len: AtomicUsize,
    fsync_failed: AtomicBool,
}

// mmap() is documented as being multi-thread safe
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    pub(crate) fn new(file: File, max_capacity: usize) -> Result<Self> {
        let len = Self::get_valid_length(&file, max_capacity)?;
        let lock = FileLock::new(&file)?;

        let mmap = MmapInner::create_mapping(&file, len, max_capacity)?;

        let mapping = Self {
            file,
            _lock: lock,
            mmap,
            len: AtomicUsize::new(len.try_into().unwrap()),
            fsync_failed: AtomicBool::new(false),
        };

        mapping.flush()?;

        Ok(mapping)
    }

    /// Retrieves the length of the specified file and validates that it is <=
    /// the maximum capacity the file is allowed to support
    #[inline]
    pub(crate) fn get_valid_length(file: &File, max_capacity: usize) -> Result<u64> {
        let len = file.metadata()?.len();

        if len > max_capacity as u64 {
            // Unfortunately io::ErrorKind::FileTooLarge is unstable, so we just
            // cheat here instead and provide the os specific codes
            let code = if cfg!(target_os = "windows") {
                0xdf // ERROR_FILE_TOO_LARGE
            } else {
                assert!(cfg!(unix), "unsupported target platform");
                libc::EFBIG
            };
            Err(Error::Io(io::Error::from_raw_os_error(code)))
        } else {
            Ok(len)
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// SAFETY: if `new_len < len()`, caller must ensure that no references to
    /// memory in `new_len..len()` exist
    pub(crate) unsafe fn resize(&self, new_len: usize) -> Result<()> {
        assert!(new_len <= self.mmap.capacity);
        self.check_fsync_failure()?;

        self.mmap.resize(new_len as u64, self)?;

        self.len.store(new_len, Ordering::Release);
        Ok(())
    }

    #[inline]
    fn check_fsync_failure(&self) -> Result<()> {
        if self.fsync_failed.load(Ordering::Acquire) {
            Err(Error::Io(io::Error::from(ErrorKind::Other)))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn set_fsync_failed(&self, failed: bool) {
        self.fsync_failed.store(failed, Ordering::Release);
    }

    #[inline]
    pub(crate) fn flush(&self) -> Result<()> {
        self.check_fsync_failure()?;

        let res = self.mmap.flush(self);
        if res.is_err() {
            self.set_fsync_failed(true);
        }

        res
    }

    #[inline]
    pub(crate) fn eventual_flush(&self) -> Result {
        self.check_fsync_failure()?;
        let res = self.mmap.eventual_flush(self);
        if res.is_err() {
            self.set_fsync_failed(true);
        }

        res
    }

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .get_memory_mut()
    pub(crate) unsafe fn get_memory(&self, range: Range<usize>) -> &[u8] {
        assert!(range.end <= self.len());
        // TODO: propagate the error
        self.check_fsync_failure()
            .expect("fsync previously failed. Connection closed");
        let ptr = self.mmap.mmap.add(range.start);
        slice::from_raw_parts(ptr, range.len())
    }

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .get_memory() or .get_memory_mut()
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_memory_mut(&self, range: Range<usize>) -> &mut [u8] {
        assert!(range.end <= self.len());
        // TODO: propagate the error
        self.check_fsync_failure()
            .expect("fsync previously failed. Connection closed");
        let ptr = self.mmap.mmap.add(range.start);
        slice::from_raw_parts_mut(ptr, range.len())
    }
}
