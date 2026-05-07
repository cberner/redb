use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::page_store::lru_cache::LRUCache;
use crate::{CacheStats, DatabaseError, Result, StorageBackend, StorageError};
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;
#[cfg(feature = "cache_metrics")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

// Allocates an `Arc<[u8]>` in one step. `Arc::<[u8]>::from(vec![0; len])` would
// allocate the Vec and then allocate a new Arc and memcpy into it.
fn zero_filled_arc(len: usize) -> Arc<[u8]> {
    // This is documented to do a single allocation: https://doc.rust-lang.org/std/sync/struct.Arc.html#iterators-of-known-length
    std::iter::repeat_n(0u8, len).collect()
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
    file: Box<dyn StorageBackend>,
    io_failed: AtomicBool,
    closed: AtomicBool,
}

impl CheckedBackend {
    fn new(file: Box<dyn StorageBackend>) -> Self {
        Self {
            file,
            io_failed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
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
}

pub(super) struct PagedCachedFile {
    file: CheckedBackend,
    page_size: u64,
    // Dynamic cache partitioning.  Invariants:
    //
    // 1. The combined "dirty" footprint (write_buffer + writeback) NEVER
    //    exceeds 50% of max_cache_size.  When the write buffer would push
    //    the total over the limit, pages are evicted to disk.  When a
    //    `write_barrier` would push the writeback cache over the limit, the
    //    overflow is flushed to disk.
    // 2. The write buffer evicts from the read cache only when
    //    dirty < 50% AND read > 50% (fairness).
    // 3. write_buffer + writeback + read never exceeds max_cache_size.
    //
    // Together these guarantee that the read cache can grow up to 100% when no
    // writes are in progress, while write-heavy workloads never starve readers
    // below 50%.
    //
    // We track usage with three atomic counters and compute the total on the
    // fly. The resulting read is not perfectly atomic (between loading the
    // counters a concurrent operation could change one), but the budget is a
    // soft limit and momentary over-/under-counting by one page is harmless.
    // A consolidated "total" counter would add contention on every
    // insert/remove for negligible accuracy gain.
    read_cache_bytes: AtomicUsize,
    write_buffer_bytes: AtomicUsize,
    // Bytes held by the writeback cache.  These are dirty pages that have
    // been "committed" by a non-durable transaction but have not yet been
    // written to disk.  They are visible to all readers (including
    // `read_direct`) but only persisted on the next `flush`.
    writeback_bytes: AtomicUsize,
    max_cache_size: usize,
    // Rotates the starting stripe for read-cache and writeback eviction
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
    // Holds in-progress writes for the active write transaction.  Pages move
    // out of this buffer when the WritablePage is dropped, into either the
    // read cache (durable commit) or the writeback cache (non-durable commit).
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Arc<Mutex<LRUWriteCache>>,
    // Holds dirty pages from non-durable commits.  Reads (including
    // `read_direct`) hit this layer before going to disk so that readers see
    // the latest committed state without a syscall on every commit.  Drained
    // by `flush` (and proactively when the dirty budget overflows).
    writeback_cache: Vec<RwLock<LRUCache<Arc<[u8]>>>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_cache_size: usize,
    ) -> Result<Self, DatabaseError> {
        let read_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new()))
            .collect();
        let writeback_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(LRUCache::new()))
            .collect();

        Ok(Self {
            file: CheckedBackend::new(file),
            page_size,
            read_cache_bytes: AtomicUsize::new(0),
            write_buffer_bytes: AtomicUsize::new(0),
            writeback_bytes: AtomicUsize::new(0),
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
            write_buffer: Arc::new(Mutex::new(LRUWriteCache::new())),
            writeback_cache,
        })
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
            let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
            CacheStats {
                evictions: self.evictions.load(Ordering::Acquire),
                read_hits,
                read_misses: read_total - read_hits,
                write_hits,
                write_misses: write_total - write_hits,
                used_bytes: read_bytes + write_bytes + writeback_bytes,
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
    // Caller must hold the write_buffer mutex to maintain the lock ordering
    // invariant (write_buffer lock is always acquired before read_cache locks).
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
        let mut write_buffer = self.write_buffer.lock().unwrap();

        for (offset, buffer) in write_buffer.cache.iter() {
            self.file.write(*offset, buffer.as_ref().unwrap())?;
        }
        // Transfer flushed pages into the read cache so they are available
        // for subsequent reads without a file I/O.  The write buffer is being
        // drained, so the total check only considers the read cache size.
        for (offset, buffer) in write_buffer.cache.iter_mut() {
            let buffer = buffer.take().unwrap();
            let cache_size = self
                .read_cache_bytes
                .fetch_add(buffer.len(), Ordering::AcqRel);

            if cache_size + buffer.len() <= self.max_cache_size {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                if let Some(replaced) = lock.insert(*offset, buffer) {
                    // A race could cause us to replace an existing buffer
                    self.read_cache_bytes
                        .fetch_sub(replaced.len(), Ordering::AcqRel);
                }
            } else {
                self.read_cache_bytes
                    .fetch_sub(buffer.len(), Ordering::AcqRel);
                break;
            }
        }
        self.write_buffer_bytes.store(0, Ordering::Release);
        write_buffer.clear();

        Ok(())
    }

    // Drain the writeback cache to disk and migrate evicted pages into the
    // read cache so they remain available for subsequent reads. After this
    // returns, all pages that were buffered by previous non-durable commits
    // are durable on disk (modulo `sync_data`).
    fn flush_writeback_cache(&self) -> Result {
        // Fast path: skip 131 stripe locks when nothing has been buffered
        // (the common case for workloads that don't use Durability::None).
        if self.writeback_bytes.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        for cache_slot in 0..self.writeback_cache.len() {
            // Drain this stripe under its own lock so concurrent reads don't
            // block on the (potentially long) disk writes that follow.
            let entries: Vec<(u64, Arc<[u8]>)> = {
                let mut wb_lock = self.writeback_cache[cache_slot].write().unwrap();
                let mut v = Vec::with_capacity(wb_lock.len());
                while let Some((offset, buffer)) = wb_lock.pop_lowest_priority() {
                    v.push((offset, buffer));
                }
                v
            };

            let drained_bytes: usize = entries.iter().map(|(_, b)| b.len()).sum();
            if drained_bytes == 0 {
                continue;
            }
            self.writeback_bytes
                .fetch_sub(drained_bytes, Ordering::AcqRel);

            for (offset, buffer) in &entries {
                let result = self.file.write(*offset, buffer);
                if result.is_err() {
                    // Restore the remaining entries so we don't lose data on
                    // a partial flush.
                    let mut wb_lock = self.writeback_cache[cache_slot].write().unwrap();
                    for (off, buf) in entries {
                        self.writeback_bytes.fetch_add(buf.len(), Ordering::AcqRel);
                        wb_lock.insert(off, buf);
                    }
                    return result;
                }
            }

            // Best-effort transfer to the read cache so subsequent reads
            // don't have to hit disk.  Drop entries that would push the
            // total over budget.
            for (offset, buffer) in entries {
                let len = buffer.len();
                let prev = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
                let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
                let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
                if prev + len + write_bytes + writeback_bytes <= self.max_cache_size {
                    let mut rc_lock = self.read_cache[cache_slot].write().unwrap();
                    if let Some(replaced) = rc_lock.insert(offset, buffer) {
                        self.read_cache_bytes
                            .fetch_sub(replaced.len(), Ordering::AcqRel);
                    }
                } else {
                    self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
                }
            }
        }

        Ok(())
    }

    // Evict from the writeback cache by flushing oldest entries to disk.
    // Used to keep the dirty footprint within the cache budget.
    //
    // Caller must hold the write_buffer mutex to maintain the lock ordering
    // invariant (write_buffer lock is always acquired before the per-stripe
    // writeback / read-cache locks).
    fn evict_writeback(
        &self,
        bytes_to_free: usize,
        _write_lock: &MutexGuard<'_, LRUWriteCache>,
    ) -> Result {
        let num_stripes = self.writeback_cache.len();
        let start = self.next_eviction_stripe.fetch_add(1, Ordering::Relaxed) % num_stripes;
        let mut freed: usize = 0;
        for i in 0..num_stripes {
            if freed >= bytes_to_free {
                break;
            }
            let stripe = (start + i) % num_stripes;
            // Pop entries one at a time so we can release the writeback lock
            // around the (potentially slow) disk write below.
            loop {
                if freed >= bytes_to_free {
                    break;
                }
                let entry = {
                    let mut wb_lock = self.writeback_cache[stripe].write().unwrap();
                    wb_lock.pop_lowest_priority()
                };
                let Some((offset, buffer)) = entry else { break };
                let len = buffer.len();
                let result = self.file.write(offset, &buffer);
                if result.is_err() {
                    let mut wb_lock = self.writeback_cache[stripe].write().unwrap();
                    wb_lock.insert(offset, buffer);
                    return result;
                }
                self.writeback_bytes.fetch_sub(len, Ordering::AcqRel);
                #[cfg(feature = "cache_metrics")]
                {
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
                freed += len;

                // Migrate the freshly-flushed page into the read cache so
                // subsequent reads don't have to round-trip to disk.
                let prev = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
                let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
                let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
                if prev + len + write_bytes + writeback_bytes <= self.max_cache_size {
                    let mut rc_lock = self.read_cache[stripe].write().unwrap();
                    if let Some(replaced) = rc_lock.insert(offset, buffer) {
                        self.read_cache_bytes
                            .fetch_sub(replaced.len(), Ordering::AcqRel);
                    }
                } else {
                    self.read_cache_bytes.fetch_sub(len, Ordering::AcqRel);
                }
            }
        }
        Ok(())
    }

    // Caller should invalidate all cached pages that are no longer valid
    pub(super) fn resize(&self, len: u64) -> Result {
        // Growing leaves all existing cached pages valid. Shrinking only
        // invalidates pages whose offset falls past the new end-of-file.
        let old_len = self.file.len()?;
        if len < old_len {
            self.invalidate_read_cache_above(len);
            self.invalidate_writeback_above(len);
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

    // Drop cached writeback pages whose offset is at or beyond `threshold`.
    // Pages past the new end-of-file can never be read back, so flushing them
    // to disk would be wasted I/O.
    fn invalidate_writeback_above(&self, threshold: u64) {
        for cache_slot in 0..self.writeback_cache.len() {
            let mut lock = self.writeback_cache[cache_slot].write().unwrap();
            let stale: Vec<u64> = lock
                .iter()
                .filter_map(|(k, _)| (*k >= threshold).then_some(*k))
                .collect();
            for k in stale {
                if let Some(removed) = lock.remove(k) {
                    self.writeback_bytes
                        .fetch_sub(removed.len(), Ordering::AcqRel);
                }
            }
        }
    }

    pub(super) fn flush(&self) -> Result {
        // Flush the writeback cache first so that the on-disk image reflects
        // every prior non-durable commit before we record the new write
        // buffer state.
        self.flush_writeback_cache()?;
        self.flush_write_buffer()?;

        self.file.sync_data()
    }

    // Make writes visible to readers, but does not guarantee any durability.
    // Moves the write buffer's contents into the writeback cache without any
    // disk I/O.  Reads (including `read_direct`) consult the writeback cache,
    // so the data remains visible to all readers until a durable `flush()`.
    pub(super) fn write_barrier(&self) -> Result {
        let mut write_buffer = self.write_buffer.lock().unwrap();

        // Drain the write buffer directly into the writeback cache. The
        // combined dirty footprint is unchanged, so we don't need to evict
        // here. Holding the write_buffer mutex while we acquire individual
        // writeback stripe locks is consistent with the lock ordering
        // already established by `write()` and `evict_from_read_cache`.
        let mut moved_bytes: usize = 0;
        let mut replaced_bytes: usize = 0;
        for (offset, slot) in write_buffer.cache.iter_mut() {
            let buffer = slot.take().unwrap();
            moved_bytes += buffer.len();
            let cache_slot: usize = (*offset % Self::lock_stripes()).try_into().unwrap();
            let mut wb_lock = self.writeback_cache[cache_slot].write().unwrap();
            if let Some(replaced) = wb_lock.insert(*offset, buffer) {
                replaced_bytes += replaced.len();
            }
        }
        self.write_buffer_bytes.store(0, Ordering::Release);
        write_buffer.clear();

        if moved_bytes > 0 {
            self.writeback_bytes
                .fetch_add(moved_bytes, Ordering::AcqRel);
        }
        if replaced_bytes > 0 {
            self.writeback_bytes
                .fetch_sub(replaced_bytes, Ordering::AcqRel);
        }

        // Invariant 1: dirty <= 50% of max_cache_size.  If the writeback
        // cache exceeded the budget (because the previous transaction was
        // unusually large), evict to disk so reads aren't squeezed out of
        // the cache.
        let half = self.max_cache_size / 2;
        let writeback = self.writeback_bytes.load(Ordering::Acquire);
        if writeback > half {
            self.evict_writeback(writeback - half, &write_buffer)?;
        }

        Ok(())
    }

    // Read directly from the file, ignoring any cached data.  Pages held by
    // the writeback cache haven't been written to disk yet, so we still
    // consult it to keep reads consistent with the latest committed state.
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if self.writeback_bytes.load(Ordering::Acquire) > 0 {
            let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
            let wb_lock = self.writeback_cache[cache_slot].read().unwrap();
            if let Some(cached) = wb_lock.get(offset) {
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.as_ref().to_vec());
            }
        }
        let mut buffer = vec![0; len];
        self.file.read(offset, &mut buffer)?;
        Ok(buffer)
    }

    // Like `read_direct`, but writes directly into an `Arc<[u8]>` instead of a
    // `Vec<u8>` that is then copied into an `Arc`. The buffer is zero-filled
    // because `StorageBackend::read` takes `&mut [u8]`.
    fn read_direct_into_arc(&self, offset: u64, len: usize) -> Result<Arc<[u8]>> {
        if self.writeback_bytes.load(Ordering::Acquire) > 0 {
            let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
            let wb_lock = self.writeback_cache[cache_slot].read().unwrap();
            if let Some(cached) = wb_lock.get(offset) {
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }
        let mut arc = zero_filled_arc(len);
        self.file.read(offset, Arc::get_mut(&mut arc).unwrap())?;
        Ok(arc)
    }

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    pub(super) fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<Arc<[u8]>> {
        debug_assert_eq!(0, offset % self.page_size);
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer.lock().unwrap();
            if let Some(cached) = lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        // Check the writeback cache before the read cache.  After a non-durable
        // commit, the most recently written pages live here and not on disk.
        // Read transactions (PageHint::Clean) must consult this layer or they
        // would observe stale on-disk data. Skip the lock when the cache is
        // empty (the common case when nobody uses Durability::None).
        if self.writeback_bytes.load(Ordering::Acquire) > 0 {
            let wb_lock = self.writeback_cache[cache_slot].read().unwrap();
            if let Some(cached) = wb_lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let buffer = self.read_direct_into_arc(offset, len)?;
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
        // over-eviction spikes.
        let write_bytes = self.write_buffer_bytes.load(Ordering::Acquire);
        let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
        let over_total = cache_size + len + write_bytes + writeback_bytes > self.max_cache_size;
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
        if let Some(removed) = self.write_buffer.lock().unwrap().remove(offset) {
            self.write_buffer_bytes
                .fetch_sub(removed.len(), Ordering::Release);
        }
        // The page may also have been buffered by a previous non-durable
        // commit; drop it so a subsequent read goes back to disk.
        if self.writeback_bytes.load(Ordering::Acquire) > 0 {
            let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
            let mut wb_lock = self.writeback_cache[cache_slot].write().unwrap();
            if let Some(removed) = wb_lock.remove(offset) {
                self.writeback_bytes
                    .fetch_sub(removed.len(), Ordering::Release);
            }
        }
    }

    // Invalidate any caching of the given range. After this call overlapping reads of the range are allowed
    //
    // NOTE: Invalidating a cached region in subsections is permitted, as long as all subsections are invalidated
    pub(super) fn invalidate_cache(&self, offset: u64, len: usize) {
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let Some(removed) = lock.remove(offset) {
                assert_eq!(len, removed.len());
                self.read_cache_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
            }
        }
        let mut wb_lock = self.writeback_cache[cache_slot].write().unwrap();
        if let Some(removed) = wb_lock.remove(offset) {
            assert_eq!(len, removed.len());
            self.writeback_bytes
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
        for cache_slot in 0..self.writeback_cache.len() {
            let mut lock = self.writeback_cache[cache_slot].write().unwrap();
            while let Some((_, removed)) = lock.pop_lowest_priority() {
                self.writeback_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
            }
        }
    }

    // If overwrite is true, the page is initialized to zero
    // cache_policy takes the existing data as an argument and returns the priority. The priority should be stable and not change after WritablePage is dropped
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);
        let mut lock = self.write_buffer.lock().unwrap();

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let mut existing = {
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
        // A page that was buffered by a previous non-durable commit holds the
        // most recent contents.  Reclaim it so the caller starts from the
        // correct state, and so the writeback cache no longer references a
        // page about to be overwritten. Skip the lock when the writeback
        // cache is empty.
        if existing.is_none() && self.writeback_bytes.load(Ordering::Acquire) > 0 {
            let mut wb_lock = self.writeback_cache[cache_slot].write().unwrap();
            if let Some(removed) = wb_lock.remove(offset) {
                assert_eq!(
                    len,
                    removed.len(),
                    "writeback cache inconsistency {len} != {} for offset {offset}",
                    removed.len()
                );
                self.writeback_bytes
                    .fetch_sub(removed.len(), Ordering::AcqRel);
                existing = Some(removed);
            }
        }

        let data = if let Some(removed) = lock.take_value(offset) {
            #[cfg(feature = "cache_metrics")]
            self.writes_hits.fetch_add(1, Ordering::AcqRel);
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            let mut write_bytes = previous + len;
            let half = self.max_cache_size / 2;

            // Rule 1: combined dirty footprint NEVER exceeds 50%.  First
            // evict from the writeback cache (which holds finalized
            // committed pages and so is safe to flush at any time).  If
            // dirty is still over budget, fall back to flushing the active
            // write buffer.
            let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
            if write_bytes + writeback_bytes > half {
                let excess = write_bytes + writeback_bytes - half;
                self.evict_writeback(excess, &lock)?;
            }
            let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
            if write_bytes + writeback_bytes > half {
                let excess = write_bytes + writeback_bytes - half;
                let mut flushed = 0;
                while flushed < excess {
                    if let Some((offset, buffer)) = lock.pop_lowest_priority() {
                        let removed_len = buffer.len();
                        let result = self.file.write(offset, &buffer);
                        if result.is_err() {
                            lock.insert(offset, buffer);
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
                write_bytes -= flushed;
            }

            // Rules 2 + 3: after rule 1, dirty <= 50%.  If the total still
            // exceeds the budget then read must be > 50%, so evict from the
            // read cache (fairness: we only take from read when read > 50%).
            let read_bytes = self.read_cache_bytes.load(Ordering::Acquire);
            let writeback_bytes = self.writeback_bytes.load(Ordering::Acquire);
            let total = write_bytes + writeback_bytes + read_bytes;
            if total > self.max_cache_size {
                self.evict_from_read_cache(total - self.max_cache_size, &lock);
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
            buffer: self.write_buffer.clone(),
            offset,
            data,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::StorageBackend;
    use crate::backends::InMemoryBackend;
    use crate::tree_store::PageHint;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    #[test]
    fn cache_leak() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024).unwrap();
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

    #[test]
    fn resize_preserves_cached_pages() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 4096).unwrap();

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
}
