use crate::transaction_tracker::TransactionId;
use crate::tree_store::page_store::base::{PageHack, PageHackMut, PageHint, PhysicalStorage};
use crate::tree_store::page_store::file_lock::LockedFile;
use crate::{Error, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::mem;
use std::ops::{DerefMut, Index, IndexMut};
#[cfg(all(unix, not(fuzzing)))]
use std::os::unix::io::AsRawFd;
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub(super) struct WritablePage<'a> {
    buffer: &'a Mutex<BTreeMap<u64, Arc<Vec<u8>>>>,
    offset: u64,
    data: Vec<u8>,
}

impl<'a> WritablePage<'a> {
    pub(super) fn mem(&self) -> &[u8] {
        &self.data
    }

    pub(super) fn mem_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl<'a> Drop for WritablePage<'a> {
    fn drop(&mut self) {
        let data = mem::take(&mut self.data);
        assert!(self
            .buffer
            .lock()
            .unwrap()
            .insert(self.offset, Arc::new(data))
            .is_none());
    }
}

impl<'a, I: SliceIndex<[u8]>> Index<I> for WritablePage<'a> {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.data.index(index)
    }
}

impl<'a, I: SliceIndex<[u8]>> IndexMut<I> for WritablePage<'a> {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.data.index_mut(index)
    }
}

pub(super) struct PagedCachedFile {
    file: LockedFile,
    page_size: u64,
    max_read_cache_bytes: usize,
    read_cache_bytes: AtomicUsize,
    max_write_buffer_bytes: usize,
    write_buffer_bytes: AtomicUsize,
    reads_total: AtomicU64,
    reads_hits: AtomicU64,
    fsync_failed: AtomicBool,
    read_cache: Vec<RwLock<BTreeMap<u64, Arc<Vec<u8>>>>>,
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Mutex<BTreeMap<u64, Arc<Vec<u8>>>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: File,
        page_size: u64,
        max_read_cache_bytes: usize,
        max_write_buffer_bytes: usize,
    ) -> Result<Self> {
        let mut read_cache = Vec::with_capacity(Self::lock_stripes());
        for _ in 0..Self::lock_stripes() {
            read_cache.push(RwLock::new(BTreeMap::new()));
        }

        let lock = LockedFile::new(file)?;

        // Try to flush any pages in the page cache that are out of sync with disk.
        // See here for why: <https://github.com/cberner/redb/issues/450>
        #[cfg(target_os = "linux")]
        unsafe {
            libc::posix_fadvise64(lock.file().as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }

        Ok(Self {
            file: lock,
            page_size,
            max_read_cache_bytes,
            read_cache_bytes: AtomicUsize::new(0),
            max_write_buffer_bytes,
            write_buffer_bytes: AtomicUsize::new(0),
            reads_total: Default::default(),
            reads_hits: Default::default(),
            fsync_failed: Default::default(),
            read_cache,
            write_buffer: Mutex::new(BTreeMap::new()),
        })
    }

    const fn lock_stripes() -> usize {
        131
    }

    #[inline]
    fn check_fsync_failure(&self) -> Result<()> {
        if self.fsync_failed.load(Ordering::Acquire) {
            Err(Error::Io(io::Error::from(io::ErrorKind::Other)))
        } else {
            Ok(())
        }
    }

    #[inline]
    #[cfg(not(fuzzing))]
    fn set_fsync_failed(&self, failed: bool) {
        self.fsync_failed.store(failed, Ordering::Release);
    }

    fn flush_write_buffer(&self) -> Result {
        self.check_fsync_failure()?;
        let mut write_buffer = std::mem::take(self.write_buffer.lock().unwrap().deref_mut());
        let total_bytes: usize = write_buffer.values().map(|buffer| buffer.len()).sum();
        self.write_buffer_bytes
            .fetch_sub(total_bytes, Ordering::Release);

        for (offset, buffer) in write_buffer.iter() {
            self.file.write(*offset, buffer)?;
        }
        write_buffer.clear();

        Ok(())
    }
}

impl PhysicalStorage for PagedCachedFile {
    unsafe fn mark_transaction(&self, _id: TransactionId) {
        // no-op
    }

    unsafe fn gc(&self, _oldest_live_id: TransactionId) -> Result {
        // no-op
        Ok(())
    }

    // Caller should invalidate all cached pages that are no longer valid
    unsafe fn resize(&self, len: u64) -> Result {
        // TODO: be more fine-grained about this invalidation
        for slot in 0..self.read_cache.len() {
            let cache = mem::take(&mut *self.read_cache[slot].write().unwrap());
            for (_, buffer) in cache {
                self.read_cache_bytes
                    .fetch_sub(buffer.len(), Ordering::Release);
            }
        }

        self.file.file().set_len(len).map_err(Error::from)
    }

    fn flush(&self) -> Result {
        self.check_fsync_failure()?;
        self.flush_write_buffer()?;
        // Disable fsync when fuzzing, since it doesn't test crash consistency
        #[cfg(not(fuzzing))]
        {
            let res = self.file.file().sync_data().map_err(Error::from);
            if res.is_err() {
                self.set_fsync_failed(true);
                // Try to flush any pages in the page cache that are out of sync with disk.
                // See here for why: <https://github.com/cberner/redb/issues/450>
                #[cfg(target_os = "linux")]
                unsafe {
                    libc::posix_fadvise64(
                        self.file.file().as_raw_fd(),
                        0,
                        0,
                        libc::POSIX_FADV_DONTNEED,
                    );
                }
                return res;
            }
        }

        Ok(())
    }

    fn eventual_flush(&self) -> Result {
        self.check_fsync_failure()?;

        #[cfg(not(target_os = "macos"))]
        {
            self.flush()?;
        }
        #[cfg(all(target_os = "macos", not(fuzzing)))]
        {
            self.flush_write_buffer()?;
            let code = unsafe { libc::fcntl(self.file.file().as_raw_fd(), libc::F_BARRIERFSYNC) };
            if code == -1 {
                self.set_fsync_failed(true);
                return Err(io::Error::last_os_error().into());
            }
        }

        Ok(())
    }

    // Make writes visible to readers, but does not guarantee any durability
    fn write_barrier(&self) -> Result {
        self.flush_write_buffer()
    }

    fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.check_fsync_failure()?;
        self.file.read(offset, len)
    }

    // Caller must explicitly invalidate overlapping regions that are read
    unsafe fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<PageHack> {
        self.check_fsync_failure()?;
        debug_assert_eq!(0, offset % self.page_size);
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer.lock().unwrap();
            if let Some(cached) = lock.get(&offset) {
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(PageHack::ArcMem(cached.clone()));
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes() as u64).try_into().unwrap();
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(&offset) {
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(PageHack::ArcMem(cached.clone()));
            }
        }

        let buffer = Arc::new(self.read_direct(offset, len)?);
        let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
        let mut write_lock = self.read_cache[cache_slot].write().unwrap();
        write_lock.insert(offset, buffer.clone());
        let mut removed = 0;
        if cache_size + len > self.max_read_cache_bytes {
            while removed < len {
                if let Some((_, v)) = write_lock.pop_first() {
                    removed += v.len();
                } else {
                    break;
                }
            }
        }
        if removed > 0 {
            self.read_cache_bytes.fetch_sub(removed, Ordering::AcqRel);
        }

        Ok(PageHack::ArcMem(buffer))
    }

    fn cancel_pending_write(&self, offset: u64, _len: usize) {
        assert_eq!(0, offset % self.page_size);
        self.write_buffer.lock().unwrap().remove(&offset);
    }

    // Invalidating a cached region in subsections is permitted, as long as all subsections are invalidated
    fn invalidate_cache(&self, offset: u64, _len: usize) {
        let cache_slot: usize = (offset % self.read_cache.len() as u64).try_into().unwrap();
        let mut lock = self.read_cache[cache_slot].write().unwrap();
        if let Some(removed) = lock.remove(&offset) {
            // TODO: it would be nice to re-enable this assertion. However, when restoring a Savepoint
            // the information about page order is lost ;( and those order-0 pages are then stored
            // the freed tree
            // assert_eq!(len, removed.len());
            self.read_cache_bytes
                .fetch_sub(removed.len(), Ordering::AcqRel);
        }
    }

    unsafe fn write(&self, offset: u64, len: usize) -> Result<PageHackMut> {
        self.check_fsync_failure()?;
        assert_eq!(0, offset % self.page_size);
        let mut lock = self.write_buffer.lock().unwrap();

        // TODO: allow hint that page is known to be dirty and will not be in the read cache
        let cache_slot: usize = (offset % self.read_cache.len() as u64).try_into().unwrap();
        let existing = {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let Some(removed) = lock.remove(&offset) {
                // TODO: it would be nice to re-enable this assertion. However, when restoring a Savepoint
                // the information about page order is lost ;( and those order-0 pages are then stored
                // the freed tree
                // assert_eq!(len, removed.len());
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
                Some(Arc::try_unwrap(removed).unwrap())
            } else {
                None
            }
        };

        let data = if let Some(removed) = lock.remove(&offset) {
            Arc::try_unwrap(removed).unwrap()
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            if previous + len > self.max_write_buffer_bytes {
                let mut removed_bytes = 0;
                while removed_bytes < len {
                    if let Some((offset, buffer)) = lock.pop_first() {
                        self.write_buffer_bytes
                            .fetch_sub(buffer.len(), Ordering::Release);
                        removed_bytes += buffer.len();
                        self.file.write(offset, &buffer)?;
                    } else {
                        break;
                    }
                }
            }
            if let Some(data) = existing {
                data
            } else {
                self.read_direct(offset, len)?
            }
        };
        Ok(PageHackMut::Writable(WritablePage {
            buffer: &self.write_buffer,
            offset,
            data,
        }))
    }
}
