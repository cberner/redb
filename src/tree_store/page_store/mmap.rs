use crate::tree_store::page_store::file_lock::LockedFile;
use crate::{Error, Result};
use std::fs::File;
use std::io;
use std::io::ErrorKind;
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
use crate::tree_store::page_store::base::{PageHack, PageHackMut, PageHint, PhysicalStorage};
use crate::types::MAX_ALIGNMENT;
#[cfg(windows)]
use windows::*;

// TODO: add some tests that use the mmap backend
pub(crate) struct Mmap {
    file: LockedFile,
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
    pub(crate) fn new(file: File) -> Result<Self> {
        let len = file.metadata()?.len();
        let lock = LockedFile::new(file)?;

        let mmap = MmapInner::create_mapping(lock.file(), len)?;

        let address = mmap.base_addr();
        assert_eq!(0, address as usize % MAX_ALIGNMENT);

        // Try to flush any pages in the page cache that are out of sync with disk.
        // See here for why: <https://github.com/cberner/redb/issues/450>
        #[cfg(all(unix, not(target_os = "android")))]
        unsafe {
            libc::posix_madvise(
                address as *mut libc::c_void,
                len.try_into().unwrap(),
                libc::POSIX_MADV_DONTNEED,
            );
        }

        let mapping = Self {
            file: lock,
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

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
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
}

impl PhysicalStorage for Mmap {
    /// SAFETY: Caller must ensure that the values passed to this method are monotonically increasing
    // TODO: Remove this method and replace it with a call that returns an accessor that uses Arc to reference count the mmaps
    unsafe fn mark_transaction(&self, id: TransactionId) {
        self.current_transaction_id.store(id.0, Ordering::Release);
    }

    /// SAFETY: Caller must ensure that all references, from get_memory() or get_memory_mut(), created
    /// before the matching (same value) call to mark_transaction() have been dropped
    unsafe fn gc(&self, oldest_live_id: TransactionId) -> Result {
        let mut mmaps = self.old_mmaps.lock().unwrap();
        for (id, mmap) in mmaps.iter() {
            // Flush all old mmaps before we drop them
            if *id < oldest_live_id {
                mmap.flush()?;
            }
        }
        mmaps.retain(|(id, _)| *id >= oldest_live_id);

        Ok(())
    }

    /// SAFETY: if `new_len < len()`, caller must ensure that no references to
    /// memory in `new_len..len()` exist
    unsafe fn resize(&self, new_len: u64) -> Result<()> {
        let new_len: usize = new_len.try_into().unwrap();
        self.check_fsync_failure()?;

        let mut mmap = self.mmap.lock().unwrap();
        self.file.file().set_len(new_len as u64)?;
        if mmap.can_resize(new_len as u64) {
            mmap.resize(new_len as u64)?;
        } else {
            let transaction_id = TransactionId(self.current_transaction_id.load(Ordering::Acquire));
            let new_mmap = MmapInner::create_mapping(self.file.file(), new_len as u64)?;
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
    fn flush(&self) -> Result<()> {
        self.check_fsync_failure()?;

        let res = self.mmap.lock().unwrap().flush();
        if res.is_err() {
            self.set_fsync_failed(true);
            #[cfg(all(unix, not(target_os = "android")))]
            {
                // Acquire lock on mmap to ensure that a resize doesn't occur
                let lock = self.mmap.lock().unwrap();
                let ptr = self.current_ptr.load(Ordering::Acquire);
                // Try to flush any pages in the page cache that are out of sync with disk.
                // See here for why: <https://github.com/cberner/redb/issues/450>
                unsafe {
                    libc::posix_madvise(
                        ptr as *mut libc::c_void,
                        self.len(),
                        libc::POSIX_MADV_DONTNEED,
                    );
                }
                drop(lock);
            }
        }

        res
    }

    #[inline]
    fn eventual_flush(&self) -> Result {
        self.check_fsync_failure()?;
        let res = self.mmap.lock().unwrap().eventual_flush();
        if res.is_err() {
            self.set_fsync_failed(true);
            #[cfg(all(unix, not(target_os = "android")))]
            {
                // Acquire lock on mmap to ensure that a resize doesn't occur
                let lock = self.mmap.lock().unwrap();
                let ptr = self.current_ptr.load(Ordering::Acquire);
                // Try to flush any pages in the page cache that are out of sync with disk.
                // See here for why: <https://github.com/cberner/redb/issues/450>
                unsafe {
                    libc::posix_madvise(
                        ptr as *mut libc::c_void,
                        self.len(),
                        libc::POSIX_MADV_DONTNEED,
                    );
                }
                drop(lock);
            }
        }

        res
    }

    fn write_barrier(&self) -> Result {
        // no-op
        Ok(())
    }

    unsafe fn read(&self, offset: u64, len: usize, _hint: PageHint) -> Result<PageHack> {
        let offset: usize = offset.try_into().unwrap();
        assert!(offset + len <= self.len());
        self.check_fsync_failure()?;
        let ptr = self.current_ptr.load(Ordering::Acquire).add(offset);
        Ok(PageHack::Ref(slice::from_raw_parts(ptr, len)))
    }

    #[allow(clippy::mut_from_ref)]
    unsafe fn write(&self, offset: u64, len: usize) -> Result<PageHackMut> {
        let offset: usize = offset.try_into().unwrap();
        assert!(offset + len <= self.len());
        self.check_fsync_failure()?;
        let ptr = self.current_ptr.load(Ordering::Acquire).add(offset);
        Ok(PageHackMut::Ref(slice::from_raw_parts_mut(ptr, len)))
    }

    fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.check_fsync_failure()?;
        self.file.read(offset, len)
    }

    fn cancel_pending_write(&self, _offset: u64, _len: usize) {
        // no-op
    }

    fn invalidate_cache(&self, _offset: u64, _len: usize) {
        // no-op
    }
}
