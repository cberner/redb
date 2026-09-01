use crate::db::InternalStorageBackend;
use crate::io;
use crate::sync::{Mutex, MutexGuard, RwLock};
use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::lru_cache::LRUCache;
use crate::{CacheStats, Result, StorageError};
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::ops::{Index, IndexMut, Range};
use core::slice::SliceIndex;
#[cfg(feature = "cache_metrics")]
use core::sync::atomic::AtomicU64;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Allocates an `Arc<[u8]>` in one step. `Arc::<[u8]>::from(vec![0; len])` would
// allocate the Vec and then allocate a new Arc and memcpy into it.
fn zero_filled_arc(len: usize) -> Arc<[u8]> {
    // This is documented to do a single allocation: https://doc.rust-lang.org/std/sync/struct.Arc.html#iterators-of-known-length
    core::iter::repeat_n(0u8, len).collect()
}

pub(super) struct WritablePage {
    buffer: Arc<Mutex<LRUWriteCache>>,
    offset: u64,
    data: Arc<[u8]>,
}

impl WritablePage {
    pub(super) fn mem(&self) -> &[u8] {
        &self.data
    }

    pub(super) fn mem_mut(&mut self) -> &mut [u8] {
        Arc::get_mut(&mut self.data).unwrap()
    }
}

impl Drop for WritablePage {
    fn drop(&mut self) {
        self.buffer
            .lock()
            .unwrap()
            .return_value(self.offset, self.data.clone());
    }
}

impl<I: SliceIndex<[u8]>> Index<I> for WritablePage {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.mem().index(index)
    }
}

impl<I: SliceIndex<[u8]>> IndexMut<I> for WritablePage {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.mem_mut().index_mut(index)
    }
}

#[derive(Default)]
struct LRUWriteCache {
    cache: LRUCache<Option<Arc<[u8]>>>,
}

impl LRUWriteCache {
    fn new() -> Self {
        Self {
            cache: LRUCache::default(),
        }
    }

    fn insert(&mut self, key: u64, value: Arc<[u8]>) {
        assert!(self.cache.insert(key, Some(value)).is_none());
    }

    fn get(&self, key: u64) -> Option<&Arc<[u8]>> {
        self.cache.get(key).map(|x| x.as_ref().unwrap())
    }

    fn remove(&mut self, key: u64) -> Option<Arc<[u8]>> {
        if let Some(value) = self.cache.remove(key) {
            assert!(value.is_some());
            return value;
        }
        None
    }

    fn return_value(&mut self, key: u64, value: Arc<[u8]>) {
        assert!(self.cache.get_mut(key).unwrap().replace(value).is_none());
    }

    fn take_value(&mut self, key: u64) -> Option<Arc<[u8]>> {
        if let Some(value) = self.cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        None
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, Arc<[u8]>)> {
        for _ in 0..self.cache.len() {
            if let Some((k, v)) = self.cache.pop_lowest_priority() {
                if let Some(v_inner) = v {
                    return Some((k, v_inner));
                }

                // Value is borrowed by take_value(). We can't evict it, so put it back.
                self.cache.insert(k, v);
            } else {
                break;
            }
        }
        None
    }

    fn clear(&mut self) {
        self.cache.clear();
    }
}

#[derive(Debug)]
struct CheckedBackend {
    file: Box<dyn InternalStorageBackend>,
    io_failed: AtomicBool,
    closed: AtomicBool,
}

// Covers the open paths that fail before there is a Database to drop. Drop cannot report the
// error, but the open is already failing.
impl Drop for CheckedBackend {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            let _ = self.file.close();
        }
    }
}

impl CheckedBackend {
    fn new(file: Box<dyn InternalStorageBackend>) -> Self {
        Self {
            file,
            io_failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn locks_expected(&self) -> bool {
        self.file.locks_expected()
    }

    fn try_lock_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        self.file.try_lock_range(range)
    }

    fn try_lock_shared_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        self.file.try_lock_shared_range(range)
    }

    #[cfg(feature = "experimental-multiprocess")]
    fn lock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        self.file.lock_range(range)
    }

    #[cfg(feature = "experimental-multiprocess")]
    fn lock_shared_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        self.file.lock_shared_range(range)
    }

    #[cfg(feature = "experimental-multiprocess")]
    fn unlock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        self.file.unlock_range(range)
    }

    fn query_lock_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        self.file.query_lock_range(range)
    }

    fn check_failure(&self) -> Result<()> {
        if self.io_failed.load(Ordering::Acquire) {
            if self.closed.load(Ordering::Acquire) {
                Err(StorageError::DatabaseClosed)
            } else {
                Err(StorageError::PreviousIo)
            }
        } else {
            Ok(())
        }
    }

    fn close(&self) -> Result {
        self.closed.store(true, Ordering::Release);
        self.io_failed.store(true, Ordering::Release);
        self.file.close()?;

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        self.check_failure()?;
        let result = self.file.len();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.read(offset, out);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.check_failure()?;
        let result = self.file.set_len(len);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn sync_data(&self) -> Result<()> {
        self.check_failure()?;
        let result = self.file.sync_data();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        let result = self.file.write(offset, data);
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    // Like write(), but a failure does not permanently fail the backend. For a write that only an
    // optimization depends on, latching would turn every later operation into a PreviousIo error
    // over data that nothing was waiting on.
    fn write_best_effort(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_failure()?;
        self.file.write(offset, data).map_err(StorageError::from)
    }
}

// Whether the data written by a write buffer flush has to reach the file. Either way a page whose
// write fails stays buffered; they differ in what a failure does to the backend.
#[derive(Copy, Clone)]
enum Writeback {
    // A failure is propagated and permanently fails the backend
    Required,
    // A failure leaves the backend usable
    BestEffort,
}

pub(super) struct PagedCachedFile {
    file: CheckedBackend,
    page_size: u64,
    // Dynamic cache partitioning.  Three invariants:
    //
    // 1. The write buffer is held at or below 50% of max_cache_size.
    //    Pages beyond this limit are flushed to disk immediately, best
    //    effort -- see the eviction in write().
    // 2. The write buffer evicts from the read cache only when
    //    write < 50% AND read > 50% (fairness).
    // 3. write + read never exceeds max_cache_size.
    //
    // Together these guarantee that the read cache can grow up to 100% when no
    // writes are in progress, while write-heavy workloads never starve readers
    // below 50%.
    //
    // Non-durable commits leave their pages in the write buffer (see write_barrier()), so it is
    // not necessarily empty between transactions. Invariant 1 bounds what they accumulate, and
    // read() flushes them out under pressure, so they cannot hold the read cache below 100%.
    //
    // We track usage with two atomic counters and compute the total on the fly.
    // The resulting read is not perfectly atomic (between loading the two
    // counters a concurrent operation could change one), but the budget is a
    // soft limit and momentary over-/under-counting by one page is harmless.
    // A third "total" counter would add contention on every insert/remove for
    // negligible accuracy gain.
    read_cache_bytes: AtomicUsize,
    write_buffer_bytes: AtomicUsize,
    // True when the write buffer holds committed, reader-visible pages, left there by a
    // non-durable commit (see write_barrier()) instead of being written to the file. While set,
    // PageHint::Clean reads must consult the buffer, since a clean page may live there rather
    // than in the file.
    //
    // The buffer also holds the in-progress write transaction's uncommitted pages, but readers
    // cannot observe them: pages are copy-on-write, and a freed offset is only reallocated once
    // no live read transaction can reference it.
    committed_pages_buffered: AtomicBool,
    max_cache_size: usize,
    // Rotates the starting stripe for read-cache eviction
    next_eviction_stripe: AtomicUsize,
    #[cfg(feature = "cache_metrics")]
    reads_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    reads_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    writes_hits: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    evictions: AtomicU64,
    read_cache: Vec<RwLock<LRUCache<Arc<[u8]>>>>,
    // Striped by page offset, like read_cache, so that concurrent writers
    // (tables of one WriteTransaction used from different threads) do not
    // contend on a single lock. Each stripe is individually Arc'd because
    // outstanding WritablePages return their buffer to the stripe on drop.
    write_buffer: Vec<Arc<Mutex<LRUWriteCache>>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: Box<dyn InternalStorageBackend>,
        page_size: u64,
        max_cache_size: usize,
    ) -> Self {
        let read_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new()))
            .collect();
        let write_buffer = (0..Self::lock_stripes())
            .map(|_| Arc::new(Mutex::new(LRUWriteCache::new())))
            .collect();

        Self {
            file: CheckedBackend::new(file),
            page_size,
            read_cache_bytes: AtomicUsize::new(0),
            write_buffer_bytes: AtomicUsize::new(0),
            committed_pages_buffered: AtomicBool::new(false),
            max_cache_size,
            next_eviction_stripe: AtomicUsize::new(0),
            #[cfg(feature = "cache_metrics")]
            reads_total: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            reads_hits: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            writes_total: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            writes_hits: AtomicU64::default(),
            #[cfg(feature = "cache_metrics")]
            evictions: AtomicU64::default(),
            read_cache,
            write_buffer,
        }
    }

    pub(crate) fn locks_expected(&self) -> bool {
        self.file.locks_expected()
    }

    pub(crate) fn try_lock_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        self.file.try_lock_range(range)
    }

    pub(crate) fn try_lock_shared_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        self.file.try_lock_shared_range(range)
    }

    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn lock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        self.file.lock_range(range)
    }

    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn lock_shared_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        self.file.lock_shared_range(range)
    }

    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn unlock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
        self.file.unlock_range(range)
    }

    /// Whether an exclusive lock over the range would conflict with one held elsewhere.
    pub(crate) fn query_lock_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
        self.file.query_lock_range(range)
    }

    fn write_buffer_stripe(&self, offset: u64) -> &Arc<Mutex<LRUWriteCache>> {
        let stripe: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        &self.write_buffer[stripe]
    }

    #[allow(clippy::unused_self)]
    pub(crate) fn cache_stats(&self) -> CacheStats {
        #[cfg(not(feature = "cache_metrics"))]
        {
            CacheStats {
                evictions: 0,
                read_hits: 0,
                read_misses: 0,
                write_hits: 0,
                write_misses: 0,
                used_bytes: 0,
            }
        }

        #[cfg(feature = "cache_metrics")]
        {
            let read_hits = self.reads_hits.load(Ordering::Acquire);
            let read_total = self.reads_total.load(Ordering::Acquire);
            let write_hits = self.writes_hits.load(Ordering::Acquire);
            let write_total = self.writes_total.load(Ordering::Acquire);
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            CacheStats {
                evictions: self.evictions.load(Ordering::Acquire),
                read_hits,
                read_misses: read_total - read_hits,
                write_hits,
                write_misses: write_total - write_hits,
                used_bytes: read_bytes + write_bytes,
            }
        }
    }

    pub(crate) fn close(&self) -> Result {
        self.file.close()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.file.check_failure()
    }

    pub(crate) fn raw_file_len(&self) -> Result<u64> {
        self.file.len()
    }

    const fn lock_stripes() -> u64 {
        131
    }

    // Evict entries from the read cache to free at least `bytes_needed` bytes.
    // Iterates through cache stripes and pops lowest-priority entries.
    //
    // Caller must hold a write buffer stripe mutex to maintain the lock ordering
    // invariant (write buffer stripe locks are always acquired before read_cache locks).
    fn evict_from_read_cache(
        &self,
        bytes_needed: usize,
        _write_lock: &MutexGuard<'_, LRUWriteCache>,
    ) {
        let num_stripes = self.read_cache.len();
        let start = self.next_eviction_stripe.fetch_add(1, Ordering::Relaxed) % num_stripes;
        let mut freed = 0;
        for i in 0..num_stripes {
            if freed >= bytes_needed {
                break;
            }
            let stripe = (start + i) % num_stripes;
            let mut lock = self.read_cache[stripe].write().unwrap();
            while freed < bytes_needed {
                if let Some((_, v)) = lock.pop_lowest_priority() {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    freed += v.len();
                    self.read_cache_bytes.fetch_sub(v.len(), Ordering::AcqRel);
                } else {
                    break;
                }
            }
        }
    }

    fn flush_write_buffer(&self) -> Result {
        for stripe in &self.write_buffer {
            let mut write_buffer = stripe.lock().unwrap();

            for (offset, buffer) in write_buffer.cache.iter() {
                self.file.write(*offset, buffer.as_ref().unwrap())?;
            }
            // Transfer flushed pages into the read cache so they are available
            // for subsequent reads without a file I/O.  The write buffer is being
            // drained, so the total check only considers the read cache size.
            for (offset, buffer) in write_buffer.cache.iter_mut() {
                let buffer = buffer.take().unwrap();
                let len = buffer.len();
                let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);

                if cache_size + len <= self.max_cache_size {
                    let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                    let mut lock = self.read_cache[cache_slot].write().unwrap();
                    if let Some(replaced) = lock.insert(*offset, buffer) {
                        // A race could cause us to replace an existing buffer
                        self.read_cache_bytes
                            .fetch_sub(replaced.len(), Ordering::AcqRel);
                    }
                } else {
                    self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
                }
                self.write_buffer_bytes.fetch_sub(len, Ordering::AcqRel);
            }
            write_buffer.clear();
        }
        // The buffer is empty, so readers no longer need to consult it
        self.committed_pages_buffered
            .store(false, Ordering::Release);

        Ok(())
    }

    // Drop all buffered writes without writing them to the file. Only valid when the state they
    // belong to is being abandoned (e.g. rolling back to the on-disk state).
    pub(super) fn discard_write_buffer(&self) {
        self.committed_pages_buffered
            .store(false, Ordering::Release);
        for stripe in &self.write_buffer {
            let mut write_buffer = stripe.lock().unwrap();
            for (_, buffer) in write_buffer.cache.iter() {
                // Like flush_write_buffer(), decrement per page rather than storing zero: a
                // concurrent writer may be adding to a stripe this loop has already passed.
                self.write_buffer_bytes
                    .fetch_sub(buffer.as_ref().unwrap().len(), Ordering::AcqRel);
            }
            write_buffer.clear();
        }
    }

    // Caller should invalidate all cached pages that are no longer valid
    pub(super) fn resize(&self, len: u64) -> Result {
        // Growing leaves all existing cached pages valid. Shrinking only
        // invalidates pages whose offset falls past the new end-of-file.
        let old_len = self.file.len()?;
        if len < old_len {
            self.invalidate_read_cache_above(len);
        }

        self.file.set_len(len)
    }

    // Drop cached read pages whose offset is at or beyond `threshold`.
    fn invalidate_read_cache_above(&self, threshold: u64) {
        for cache_slot in 0..self.read_cache.len() {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            let stale: Vec<u64> = lock
                .iter()
                .filter_map(|(k, _)| (*k >= threshold).then_some(*k))
                .collect();
            for k in stale {
                if let Some(removed) = lock.remove(k) {
                    self.read_cache_bytes
                        .fetch_sub(removed.len(), Ordering::AcqRel);
                }
            }
        }
    }

    pub(super) fn flush(&self) -> Result {
        self.flush_write_buffer()?;

        self.file.sync_data()
    }

    // Make the backing file durable without flushing the in-memory write buffer. `set_len` is
    // issued directly to the file (it is not buffered), so this is enough to make the current file
    // length durable, and unlike `flush()` it is safe to call while writable pages are still
    // outstanding (e.g. during `grow()`).
    pub(super) fn sync_file(&self) -> Result {
        self.file.sync_data()
    }

    // Make buffered writes visible to readers, without writing them to the file or guaranteeing
    // durability. They stay in the buffer, against its budget, until flushed or evicted.
    pub(super) fn write_barrier(&self) {
        if self.write_buffer_bytes.load(Ordering::Acquire) > 0 {
            self.committed_pages_buffered.store(true, Ordering::Release);
        }
    }

    // Write directly to the file, bypassing the write buffer, so the bytes are on the file when
    // this returns rather than whenever the buffer is next flushed
    #[cfg(feature = "experimental-multiprocess")]
    pub(super) fn write_direct(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.invalidate_cache(offset, data.len());
        self.file.write(offset, data)
    }

    // Read directly from the file, ignoring any cached data
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; len];
        self.file.read(offset, &mut buffer)?;
        Ok(buffer)
    }

    // Like `read_direct`, but writes directly into an `Arc<[u8]>` instead of a
    // `Vec<u8>` that is then copied into an `Arc`. The buffer is zero-filled
    // because `StorageBackend::read` takes `&mut [u8]`.
    fn read_direct_into_arc(&self, offset: u64, len: usize) -> Result<Arc<[u8]>> {
        let mut arc = zero_filled_arc(len);
        self.file.read(offset, Arc::get_mut(&mut arc).unwrap())?;
        Ok(arc)
    }

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    pub(super) fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<Arc<[u8]>> {
        // Before the caches, which would otherwise serve a page the file can no longer be read
        // for: a read transaction outliving its Database must see DatabaseClosed, not a snapshot
        // of whatever happened to be cached
        self.check_io_errors()?;
        debug_assert_eq!(0, offset % self.page_size);
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        // A write transaction's own dirty pages are in the write buffer, so look there first.
        if matches!(hint, PageHint::None) {
            let lock = self.write_buffer_stripe(offset).lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        // A clean page is only in the write buffer after a non-durable commit left committed pages
        // there, and then never also in the read cache: write() drops the read cache entry when it
        // buffers a page, and flush_write_buffer() empties the buffer as it repopulates the cache.
        // Checking the read cache first keeps the buffer's mutex, which serializes readers that
        // the striped read cache lets run concurrently, off the path of pages cached for reading.
        if matches!(hint, PageHint::Clean) && self.committed_pages_buffered.load(Ordering::Acquire)
        {
            let lock = self.write_buffer_stripe(offset).lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                let result = cached.clone();
                // Copy the page into the read cache, so that further reads of it are served by
                // the striped read locks rather than serializing on this stripe's mutex. Both
                // caches then hold the same Arc and count it, which only makes the budget
                // conservative; whichever copy is dropped first -- the buffer's, once it is
                // flushed, or the read cache's, once it is evicted -- leaves the other counted
                // exactly once.
                let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
                if cache_size + len <= self.max_cache_size {
                    let mut write_lock = self.read_cache[cache_slot].write().unwrap();
                    if let Some(replaced) = write_lock.insert(offset, result.clone()) {
                        // A race could cause us to replace an existing buffer
                        self.read_cache_bytes
                            .fetch_sub(replaced.len(), Ordering::AcqRel);
                    }
                } else {
                    self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
                }
                return Ok(result);
            }
        }

        let buffer = self.read_direct_into_arc(offset, len)?;

        // Pages a non-durable commit left in the write buffer would otherwise hold up to half the
        // cache until the next durable commit. They can always be written out, so reclaim their
        // space rather than evicting read cache entries for them. Done before the read cache lock
        // is taken, to keep the ordering of write buffer before read cache.
        //
        // Reclaiming is best effort: on a write error the page stays buffered, the eviction below
        // makes room instead, and the error is neither propagated nor latched, so a read is not
        // failed -- nor every operation after it -- by a writeback nothing depends on.
        if self.committed_pages_buffered.load(Ordering::Acquire) {
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            if read_bytes + len + write_bytes > self.max_cache_size {
                let _ = self.flush_buffered_pages(len);
            }
        }

        let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
        let mut write_lock = self.read_cache[cache_slot].write().unwrap();
        let cache_size = if let Some(replaced) = write_lock.insert(offset, buffer.clone()) {
            // A race could cause us to replace an existing buffer
            self.read_cache_bytes
                .fetch_sub(replaced.len(), Ordering::AcqRel)
        } else {
            cache_size
        };

        // Rule 3: evict from this read-cache slot if the total exceeds the
        // budget.  We evict exactly `len` bytes (one page) per miss to avoid
        // over-eviction spikes.  `write_bytes` is read after any reclaim above,
        // so this only evicts for a shortfall the reclaim did not cover.
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        let over_total = cache_size + len + write_bytes > self.max_cache_size;
        let mut removed = 0;
        if over_total {
            while removed < len {
                if let Some((_, v)) = write_lock.pop_lowest_priority() {
                    #[cfg(feature = "cache_metrics")]
                    {
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    removed += v.len();
                } else {
                    break;
                }
            }
        }
        if removed > 0 {
            self.read_cache_bytes.fetch_sub(removed, Ordering::AcqRel);
        }

        Ok(buffer)
    }

    // Discard pending writes to the given range
    pub(super) fn cancel_pending_write(&self, offset: u64, _len: usize) {
        assert_eq!(0, offset % self.page_size);
        if let Some(removed) = self
            .write_buffer_stripe(offset)
            .lock()
            .unwrap()
            .remove(offset)
        {
            self.write_buffer_bytes
                .fetch_sub(removed.len(), Ordering::Release);
        }
    }

    // Invalidate any caching of the given range. After this call overlapping reads of the range are allowed
    //
    // NOTE: Invalidating a cached region in subsections is permitted, as long as all subsections are invalidated
    pub(super) fn invalidate_cache(&self, offset: u64, len: usize) {
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let mut lock = self.read_cache[cache_slot].write().unwrap();
        if let Some(removed) = lock.remove(offset) {
            assert_eq!(len, removed.len());
            self.read_cache_bytes
                .fetch_sub(removed.len(), Ordering::AcqRel);
        }
    }

    pub(super) fn invalidate_cache_all(&self) {
        for cache_slot in 0..self.read_cache.len() {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            while let Some((_, removed)) = lock.pop_lowest_priority() {
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
            }
        }
    }

    // Writes lowest-priority pages from `stripe` to the backend until `bytes_needed` bytes have
    // been flushed, or the stripe runs out of evictable pages. Returns the bytes flushed, which
    // can overshoot `bytes_needed` by up to a page.
    fn flush_lowest_priority(
        &self,
        stripe: &mut LRUWriteCache,
        bytes_needed: usize,
        writeback: Writeback,
    ) -> Result<usize> {
        let mut flushed = 0;
        while flushed < bytes_needed {
            if let Some((offset, buffer)) = stripe.pop_lowest_priority() {
                let removed_len = buffer.len();
                let result = match writeback {
                    Writeback::Required => self.file.write(offset, &buffer),
                    Writeback::BestEffort => self.file.write_best_effort(offset, &buffer),
                };
                if result.is_err() {
                    stripe.insert(offset, buffer);
                }
                result?;
                self.write_buffer_bytes
                    .fetch_sub(removed_len, Ordering::Release);
                #[cfg(feature = "cache_metrics")]
                {
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                flushed += removed_len;
            } else {
                break;
            }
        }
        Ok(flushed)
    }

    // Writes buffered pages to the backend until `bytes_needed` bytes have been flushed, or no
    // stripe yields more. Returns the bytes flushed. Stripes are taken with try_lock, like the
    // over-budget eviction in write(), so a stripe another thread holds is skipped rather than
    // risking a deadlock against a writer evicting toward this one.
    fn flush_buffered_pages(&self, bytes_needed: usize) -> Result<usize> {
        let num_stripes = self.write_buffer.len();
        let start = self.next_eviction_stripe.fetch_add(1, Ordering::Relaxed) % num_stripes;
        let mut flushed = 0;
        for i in 0..num_stripes {
            if flushed >= bytes_needed {
                break;
            }
            let stripe = (start + i) % num_stripes;
            if let Ok(mut lock) = self.write_buffer[stripe].try_lock() {
                flushed += self.flush_lowest_priority(
                    &mut lock,
                    bytes_needed - flushed,
                    Writeback::BestEffort,
                )?;
            }
        }
        Ok(flushed)
    }

    // If overwrite is true, the page is initialized to zero
    // cache_policy takes the existing data as an argument and returns the priority. The priority should be stable and not change after WritablePage is dropped
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);
        let stripe = self.write_buffer_stripe(offset);
        let mut lock = stripe.lock().unwrap();

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let existing = {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let Some(removed) = lock.remove(offset) {
                assert_eq!(
                    len,
                    removed.len(),
                    "cache inconsistency {len} != {} for offset {offset}",
                    removed.len()
                );
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
                Some(removed)
            } else {
                None
            }
        };

        let data = if let Some(removed) = lock.take_value(offset) {
            #[cfg(feature = "cache_metrics")]
            self.writes_hits.fetch_add(1, Ordering::AcqRel);
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            let mut write_bytes = previous + len;
            let half = self.max_cache_size / 2;

            // Rule 1: hold the write buffer at or below 50%, flushing the
            // excess to disk. The budget is global, so drain the stripe we
            // already hold first, then cover the remainder from the other
            // stripes. Those are acquired with try_lock: blocking on a second
            // stripe could deadlock with a concurrent writer evicting toward
            // this one. Eviction is therefore best effort -- a stripe another
            // thread holds is skipped, and a page with an outstanding
            // WritablePage can never be flushed -- so the buffer can briefly
            // sit above the budget. `excess` is recomputed from the live total
            // by every write(), so a later one flushes what this one could not,
            // and a commit flushes every stripe regardless.
            if write_bytes > half {
                let mut excess = write_bytes - half;
                excess = excess.saturating_sub(self.flush_lowest_priority(
                    &mut lock,
                    excess,
                    Writeback::Required,
                )?);
                if excess > 0 {
                    let own: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                    for i in 1..self.write_buffer.len() {
                        let other = (own + i) % self.write_buffer.len();
                        if let Ok(mut other_lock) = self.write_buffer[other].try_lock() {
                            excess = excess.saturating_sub(self.flush_lowest_priority(
                                &mut other_lock,
                                excess,
                                Writeback::Required,
                            )?);
                            if excess == 0 {
                                break;
                            }
                        }
                    }
                }
                write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
            }

            // Rules 2 + 3: after rule 1, write <= 50%.  If the total still
            // exceeds the budget then read must be > 50%, so evict from the
            // read cache (fairness: we only take from read when read > 50%).
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            if write_bytes + read_bytes > self.max_cache_size {
                self.evict_from_read_cache(write_bytes + read_bytes - self.max_cache_size, &lock);
            }
            let result = if let Some(data) = existing {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                data
            } else if overwrite {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                zero_filled_arc(len)
            } else {
                self.read_direct_into_arc(offset, len)?
            };
            lock.insert(offset, result);
            lock.take_value(offset).unwrap()
        };
        #[cfg(feature = "cache_metrics")]
        self.writes_total.fetch_add(1, Ordering::AcqRel);
        Ok(WritablePage {
            buffer: stripe.clone(),
            offset,
            data,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::StorageBackend;
    use crate::backends::InMemoryBackend;
    use crate::tree_store::LocklessBackend;
    use crate::tree_store::PageHint;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use alloc::sync::Arc;
    use core::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    struct CountingBackend {
        inner: InMemoryBackend,
        writes: Arc<AtomicU64>,
    }

    impl CountingBackend {
        fn new(len: u64) -> (Self, Arc<AtomicU64>) {
            let inner = InMemoryBackend::new();
            inner.set_len(len).unwrap();
            let writes = Arc::new(AtomicU64::new(0));
            (
                Self {
                    inner,
                    writes: writes.clone(),
                },
                writes,
            )
        }
    }

    impl StorageBackend for CountingBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), std::io::Error> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.inner.write(offset, data)
        }
    }

    #[test]
    fn cache_leak() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(LocklessBackend::boxed(backend), 128, 1024);
        let cached_file = Arc::new(cached_file);

        let t1 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };
        let t2 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file.read(0, 128, PageHint::None).unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();
        cached_file.invalidate_cache(0, 128);
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
    }

    // The write buffer's budget is global even though the buffer is striped: a writer whose own
    // stripe has nothing evictable must flush pages from the other stripes.
    #[test]
    fn write_buffer_budget_enforced_across_stripes() {
        let backend = InMemoryBackend::new();
        backend.set_len(4096).unwrap();
        let page_size: usize = 128;
        let max_cache_size = 1024;
        let budget = max_cache_size / 2;
        let cached_file = PagedCachedFile::new(
            LocklessBackend::boxed(backend),
            page_size as u64,
            max_cache_size,
        );

        // Dirty twice as many pages as the write budget holds. Consecutive page offsets land in
        // different stripes, so each over-budget write finds its own stripe empty and must cover
        // the excess from the stripes holding the previously written pages.
        for i in 0..8u64 {
            let mut page = cached_file
                .write(i * page_size as u64, page_size, true)
                .unwrap();
            page.mem_mut().fill(0xab);
            drop(page);
            assert!(cached_file.write_buffer_bytes.load(Ordering::Acquire) <= budget);
        }

        // Every page must read back intact, whether it was flushed to disk or is still buffered.
        for i in 0..8u64 {
            let data = cached_file
                .read(i * page_size as u64, page_size, PageHint::None)
                .unwrap();
            assert!(data.iter().all(|&b| b == 0xab));
        }
    }

    #[test]
    fn resize_preserves_cached_pages() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(LocklessBackend::boxed(backend), 128, 4096);

        // Populate the read cache with two pages from opposite ends of the file.
        cached_file.read(0, 128, PageHint::None).unwrap();
        cached_file.read(512, 128, PageHint::None).unwrap();
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 256);

        // Growing must keep every cached page valid.
        cached_file.resize(2048).unwrap();
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 256);
        assert_eq!(cached_file.raw_file_len().unwrap(), 2048);

        // Shrinking only drops pages whose offset is at or beyond the new end.
        cached_file.resize(256).unwrap();
        assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 128);
        assert_eq!(cached_file.raw_file_len().unwrap(), 256);
    }

    // write_barrier() must not write to the file: the pages stay in the buffer, still visible to
    // readers, including PageHint::Clean ones
    #[test]
    fn write_barrier_issues_no_file_writes() {
        let (backend, writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(LocklessBackend::boxed(backend), 128, 1024);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0xAB);
        drop(page);
        cached_file.write_barrier();
        assert_eq!(writes.load(Ordering::SeqCst), 0);

        assert_eq!(
            &*cached_file.read(0, 128, PageHint::Clean).unwrap(),
            [0xAB; 128].as_slice()
        );
        assert_eq!(
            &*cached_file.read(0, 128, PageHint::None).unwrap(),
            [0xAB; 128].as_slice()
        );
        assert_eq!(cached_file.read_direct(0, 128).unwrap(), vec![0; 128]);

        // A flush writes the page out and clears the buffer
        cached_file.flush().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert_eq!(cached_file.read_direct(0, 128).unwrap(), vec![0xAB; 128]);
        assert_eq!(
            &*cached_file.read(0, 128, PageHint::Clean).unwrap(),
            [0xAB; 128].as_slice()
        );
    }

    // discard_write_buffer() must drop buffered pages, so reads fall through to the file
    #[test]
    fn discard_write_buffer_drops_buffered_pages() {
        let (backend, writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(LocklessBackend::boxed(backend), 128, 1024);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0xCD);
        drop(page);
        cached_file.write_barrier();

        cached_file.discard_write_buffer();
        assert_eq!(cached_file.write_buffer_bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            &*cached_file.read(0, 128, PageHint::None).unwrap(),
            [0u8; 128].as_slice()
        );
        cached_file.flush().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    // Pages retained by a non-durable commit must not hold the read cache below its share: they
    // can always be written out, so a read-heavy workload reclaims the space they occupy.
    #[test]
    fn retained_pages_do_not_starve_read_cache() {
        const PAGE: usize = 128;
        const MAX_CACHE: usize = 4096;
        const FILE_LEN: u64 = 16 * 1024;

        let (backend, _writes) = CountingBackend::new(FILE_LEN);
        let cached_file =
            PagedCachedFile::new(LocklessBackend::boxed(backend), PAGE as u64, MAX_CACHE);

        // Fill the write buffer to its half-of-cache cap, then commit non-durably so the pages
        // stay buffered
        for i in 0..(MAX_CACHE / 2 / PAGE) as u64 {
            let mut page = cached_file.write(i * PAGE as u64, PAGE, true).unwrap();
            page.mem_mut().fill(0x11);
            drop(page);
        }
        cached_file.write_barrier();
        assert_eq!(
            cached_file.write_buffer_bytes.load(Ordering::Acquire),
            MAX_CACHE / 2
        );

        // Read far more distinct pages than the cache holds, none of them buffered
        let first_unbuffered = (MAX_CACHE / 2 / PAGE) as u64;
        for i in first_unbuffered..FILE_LEN / PAGE as u64 {
            cached_file
                .read(i * PAGE as u64, PAGE, PageHint::Clean)
                .unwrap();
        }

        // The retained pages must have been flushed to make room, rather than capping the read
        // cache at the half of the budget they were occupying
        assert!(
            cached_file.read_cache_bytes.load(Ordering::Acquire) > MAX_CACHE / 2,
            "read cache starved at {} bytes by {} bytes of retained pages",
            cached_file.read_cache_bytes.load(Ordering::Acquire),
            cached_file.write_buffer_bytes.load(Ordering::Acquire)
        );
    }

    // A zero-size cache must stay empty, so that reads always reach the backend, even when
    // reclaiming write buffer space could not get the total under the budget
    #[test]
    fn zero_size_cache_stays_empty_after_reclaim() {
        let (backend, _writes) = CountingBackend::new(1024);
        let cached_file = PagedCachedFile::new(LocklessBackend::boxed(backend), 128, 0);

        let mut page = cached_file.write(0, 128, true).unwrap();
        page.mem_mut().fill(0x22);
        drop(page);
        cached_file.write_barrier();

        for i in 1..8u64 {
            cached_file.read(i * 128, 128, PageHint::Clean).unwrap();
            assert_eq!(cached_file.read_cache_bytes.load(Ordering::Acquire), 0);
        }
    }

    // Pages spilled to the file by write-buffer pressure must stay visible to readers, alongside
    // the pages still buffered
    #[test]
    fn buffered_pages_spill_under_pressure() {
        let (backend, writes) = CountingBackend::new(1024);
        // A two page budget caps the buffer at one page, so each write() spills an earlier one
        let cached_file = PagedCachedFile::new(LocklessBackend::boxed(backend), 128, 256);

        for i in 0..4u8 {
            let offset = u64::from(i) * 128;
            let mut page = cached_file.write(offset, 128, true).unwrap();
            page.mem_mut().fill(i);
            drop(page);
        }
        cached_file.write_barrier();
        assert!(writes.load(Ordering::SeqCst) > 0);

        for i in 0..4u8 {
            let offset = u64::from(i) * 128;
            assert_eq!(
                &*cached_file.read(offset, 128, PageHint::Clean).unwrap(),
                [i; 128].as_slice()
            );
        }
    }
}
