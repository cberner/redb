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
    // Dynamic cache partitioning.  Three invariants:
    //
    // 1. The write buffer NEVER exceeds 50% of max_cache_size.
    //    Pages beyond this limit are flushed to disk immediately.
    // 2. The write buffer evicts from the read cache only when
    //    write < 50% AND read > 50% (fairness).
    // 3. write + read never exceeds max_cache_size.
    //
    // Together these guarantee that the read cache can grow up to 100% when no
    // writes are in progress, while write-heavy workloads never starve readers
    // below 50%.
    //
    // We track usage with two atomic counters and compute the total on the fly.
    // The resulting read is not perfectly atomic (between loading the two
    // counters a concurrent operation could change one), but the budget is a
    // soft limit and momentary over-/under-counting by one page is harmless.
    // A third "total" counter would add contention on every insert/remove for
    // negligible accuracy gain.
    read_cache_bytes: AtomicUsize,
    write_buffer_bytes: AtomicUsize,
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
    // Write buffer sharded by page offset so concurrent writers to different
    // pages don't all serialize on a single mutex. Small caches use a single
    // shard to preserve the strict "write buffer NEVER exceeds 50%" invariant
    // when a single page already exceeds the per-shard share of that budget.
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Vec<Arc<Mutex<LRUWriteCache>>>,
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

        let num_shards = Self::write_buffer_shards_for(max_cache_size, page_size);
        let write_buffer = (0..num_shards)
            .map(|_| Arc::new(Mutex::new(LRUWriteCache::new())))
            .collect();

        Ok(Self {
            file: CheckedBackend::new(file),
            page_size,
            read_cache_bytes: AtomicUsize::new(0),
            write_buffer_bytes: AtomicUsize::new(0),
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

    // Number of write_buffer shards. More shards = less contention between
    // concurrent writers, but sharded eviction only targets the local shard,
    // so the "write buffer NEVER exceeds 50%" invariant can momentarily
    // overshoot when writes cluster on one shard. Fall back to a single shard
    // for small caches where a single page already exceeds an N-way share of
    // the half-budget - there, strict enforcement matters more than
    // contention.
    const MAX_WRITE_BUFFER_SHARDS: usize = 8;

    fn write_buffer_shards_for(max_cache_size: usize, page_size: u64) -> usize {
        let half = max_cache_size / 2;
        let per_shard = half / Self::MAX_WRITE_BUFFER_SHARDS;
        if per_shard < 2 * page_size as usize {
            1
        } else {
            Self::MAX_WRITE_BUFFER_SHARDS
        }
    }

    fn write_buffer_shard(&self, offset: u64) -> &Arc<Mutex<LRUWriteCache>> {
        let idx = ((offset / self.page_size) as usize) % self.write_buffer.len();
        &self.write_buffer[idx]
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
        // Hold every shard for the duration of the flush so concurrent
        // writers can't slip writes into a shard we're about to clear.
        let mut shards: Vec<MutexGuard<'_, LRUWriteCache>> = self
            .write_buffer
            .iter()
            .map(|shard| shard.lock().unwrap())
            .collect();

        for shard in shards.iter() {
            for (offset, buffer) in shard.cache.iter() {
                self.file.write(*offset, buffer.as_ref().unwrap())?;
            }
        }
        // Transfer flushed pages into the read cache so they are available
        // for subsequent reads without a file I/O.  The write buffer is being
        // drained, so the total check only considers the read cache size.
        'outer: for shard in shards.iter_mut() {
            for (offset, buffer) in shard.cache.iter_mut() {
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
                    break 'outer;
                }
            }
        }
        self.write_buffer_bytes.store(0, Ordering::Release);
        for shard in shards.iter_mut() {
            shard.clear();
        }

        Ok(())
    }

    // Caller should invalidate all cached pages that are no longer valid
    pub(super) fn resize(&self, len: u64) -> Result {
        // TODO: be more fine-grained about this invalidation
        self.invalidate_cache_all();

        self.file.set_len(len)
    }

    pub(super) fn flush(&self) -> Result {
        self.flush_write_buffer()?;

        self.file.sync_data()
    }

    // Make writes visible to readers, but does not guarantee any durability
    pub(super) fn write_barrier(&self) -> Result {
        // TODO: non-durable commits would be much faster, if this did not issues writes to disk,
        // and instead just made the data visible to readers
        self.flush_write_buffer()
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
        debug_assert_eq!(0, offset % self.page_size);
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer_shard(offset).lock().unwrap();
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
            .write_buffer_shard(offset)
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

    // If overwrite is true, the page is initialized to zero
    // cache_policy takes the existing data as an argument and returns the priority. The priority should be stable and not change after WritablePage is dropped
    pub(super) fn write(&self, offset: u64, len: usize, overwrite: bool) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);

        // Evict any matching read-cache entry and speculatively allocate a
        // zero-filled buffer before acquiring the write_buffer mutex. Each
        // page is owned by at most one writer, so there is no race on this
        // offset; doing these outside the write_buffer critical section keeps
        // both the read-cache lock and the heap allocation - which can be
        // slow under malloc contention - off the hot path shared with other
        // writers.
        // TODO: allow hint that page is known to be dirty and will not be in the read cache
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
        let prealloc = if overwrite && existing.is_none() {
            Some(zero_filled_arc(len))
        } else {
            None
        };

        let shard = self.write_buffer_shard(offset);
        let mut lock = shard.lock().unwrap();

        let data = if let Some(removed) = lock.take_value(offset) {
            #[cfg(feature = "cache_metrics")]
            self.writes_hits.fetch_add(1, Ordering::AcqRel);
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            let mut write_bytes = previous + len;
            let half = self.max_cache_size / 2;

            // Rule 1: flush excess to disk if the write buffer exceeds 50%.
            // With a sharded write buffer we can only pop from the local
            // shard; pages stored in other shards can't be evicted here
            // without acquiring their mutexes, so the invariant is strict
            // only when there is a single shard. For sharded caches it
            // becomes a soft ceiling - concurrent writes distribute across
            // shards, so each shard evicts as it fills. If a single shard is
            // empty, this writer accepts the temporary overshoot and the
            // next writer evicts on its way in.
            if write_bytes > half {
                let excess = write_bytes - half;
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
            } else if let Some(zeroed) = prealloc {
                #[cfg(feature = "cache_metrics")]
                self.writes_hits.fetch_add(1, Ordering::AcqRel);
                zeroed
            } else {
                self.read_direct_into_arc(offset, len)?
            };
            lock.insert(offset, result);
            lock.take_value(offset).unwrap()
        };
        #[cfg(feature = "cache_metrics")]
        self.writes_total.fetch_add(1, Ordering::AcqRel);
        Ok(WritablePage {
            buffer: shard.clone(),
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
}
