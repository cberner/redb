use crate::{Error, Result};
use std::fs::File;
use std::io;
use std::io::ErrorKind;
use std::ops::Range;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix::*;

#[cfg(windows)]
mod windows;
use crate::transaction_tracker::TransactionId;
#[cfg(windows)]
use windows::*;

pub(crate) struct Mmap {
    file: File,
    _lock: FileLock,
    old_mmaps: Mutex<Vec<(TransactionId, MmapInner)>>,
    mmap: Mutex<MmapInner>,
    current_ptr: AtomicPtr<u8>,
    len: AtomicUsize,
    // TODO: this is an annoying hack and should be removed
    current_transaction_id: AtomicU64,
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

        let address = mmap.base_addr();

        let mapping = Self {
            file,
            _lock: lock,
            old_mmaps: Mutex::new(vec![]),
            mmap: Mutex::new(mmap),
            current_ptr: AtomicPtr::new(address),
            len: AtomicUsize::new(len.try_into().unwrap()),
            current_transaction_id: AtomicU64::new(0),
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

    /// SAFETY: Caller must ensure that the values passed to this method are monotonically increasing
    // TODO: Remove this method and replace it with a call that returns an accessor that uses Arc to reference count the mmaps
    pub(crate) unsafe fn mark_transaction(&self, id: TransactionId) {
        self.current_transaction_id.store(id.0, Ordering::Release);
    }

    /// SAFETY: Caller must ensure that all references, from get_memory() or get_memory_mut(), created
    /// before the matching (same value) call to mark_transaction() have been dropped
    pub(crate) unsafe fn gc(&self, oldest_live_id: TransactionId) -> Result {
        let mut mmaps = self.old_mmaps.lock().unwrap();
        for (id, mmap) in mmaps.iter() {
            // Flush all old mmaps before we drop them
            if *id < oldest_live_id {
                mmap.flush(self)?;
            }
        }
        mmaps.retain(|(id, _)| *id >= oldest_live_id);

        Ok(())
    }

    /// SAFETY: if `new_len < len()`, caller must ensure that no references to
    /// memory in `new_len..len()` exist
    pub(crate) unsafe fn resize(&self, new_len: usize) -> Result<()> {
        self.check_fsync_failure()?;

        let mut mmap = self.mmap.lock().unwrap();
        if mmap.can_resize(new_len as u64) {
            mmap.resize(new_len as u64, self)?;
        } else {
            let transaction_id = TransactionId(self.current_transaction_id.load(Ordering::Acquire));
            self.file.set_len(new_len as u64)?;
            let new_mmap = MmapInner::create_mapping(&self.file, new_len as u64, mmap.capacity())?;
            let old_mmap = std::mem::replace(&mut *mmap, new_mmap);
            self.old_mmaps
                .lock()
                .unwrap()
                .push((transaction_id, old_mmap));
            self.current_ptr.store(mmap.base_addr(), Ordering::Release);
        }

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

        let res = self.mmap.lock().unwrap().flush(self);
        if res.is_err() {
            self.set_fsync_failed(true);
        }

        res
    }

    #[inline]
    pub(crate) fn eventual_flush(&self) -> Result {
        self.check_fsync_failure()?;
        let res = self.mmap.lock().unwrap().eventual_flush(self);
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
        let ptr = self.current_ptr.load(Ordering::Acquire).add(range.start);
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
        let ptr = self.current_ptr.load(Ordering::Acquire).add(range.start);
        slice::from_raw_parts_mut(ptr, range.len())
    }
}
