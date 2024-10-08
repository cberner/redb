use crate::tree_store::page_store::base::PageHint;
use crate::tree_store::LEAF;
use crate::{DatabaseError, Result, StorageBackend, StorageError};
use std::collections::BTreeMap;
use std::ops::{Index, IndexMut};
use std::slice::SliceIndex;
#[cfg(feature = "cache_metrics")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

// Leaf pages are cached with low priority. Everything is cached with high priority
#[derive(Clone, Copy)]
pub(crate) enum CachePriority {
    High,
    Low,
}

impl CachePriority {
    pub(crate) fn default_btree(data: &[u8]) -> CachePriority {
        if data[0] == LEAF {
            CachePriority::Low
        } else {
            CachePriority::High
        }
    }
}

pub(super) struct WritablePage {
    buffer: Arc<Mutex<PrioritizedWriteCache>>,
    offset: u64,
    data: Arc<[u8]>,
    priority: CachePriority,
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
            .return_value(&self.offset, self.data.clone(), self.priority);
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
struct PrioritizedCache {
    cache: BTreeMap<u64, Arc<[u8]>>,
    low_pri_cache: BTreeMap<u64, Arc<[u8]>>,
}

impl PrioritizedCache {
    fn new() -> Self {
        Self {
            cache: Default::default(),
            low_pri_cache: Default::default(),
        }
    }

    fn insert(&mut self, key: u64, value: Arc<[u8]>, priority: CachePriority) -> Option<Arc<[u8]>> {
        if matches!(priority, CachePriority::Low) {
            debug_assert!(!self.cache.contains_key(&key));
            self.low_pri_cache.insert(key, value)
        } else {
            debug_assert!(!self.low_pri_cache.contains_key(&key));
            self.cache.insert(key, value)
        }
    }

    fn remove(&mut self, key: &u64) -> Option<Arc<[u8]>> {
        let result = self.cache.remove(key);
        if result.is_some() {
            return result;
        }
        self.low_pri_cache.remove(key)
    }

    fn get(&self, key: &u64) -> Option<&Arc<[u8]>> {
        let result = self.cache.get(key);
        if result.is_some() {
            return result;
        }
        self.low_pri_cache.get(key)
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, Arc<[u8]>)> {
        let result = self.low_pri_cache.pop_first();
        if result.is_some() {
            return result;
        }
        self.cache.pop_first()
    }
}

#[derive(Default)]
struct PrioritizedWriteCache {
    cache: BTreeMap<u64, Option<Arc<[u8]>>>,
    low_pri_cache: BTreeMap<u64, Option<Arc<[u8]>>>,
}

impl PrioritizedWriteCache {
    fn new() -> Self {
        Self {
            cache: Default::default(),
            low_pri_cache: Default::default(),
        }
    }

    fn insert(&mut self, key: u64, value: Arc<[u8]>, priority: CachePriority) {
        if matches!(priority, CachePriority::Low) {
            assert!(self.low_pri_cache.insert(key, Some(value)).is_none());
            debug_assert!(!self.cache.contains_key(&key));
        } else {
            assert!(self.cache.insert(key, Some(value)).is_none());
            debug_assert!(!self.low_pri_cache.contains_key(&key));
        }
    }

    fn get(&self, key: &u64) -> Option<&Arc<[u8]>> {
        let result = self.cache.get(key);
        if result.is_some() {
            return result.map(|x| x.as_ref().unwrap());
        }
        self.low_pri_cache.get(key).map(|x| x.as_ref().unwrap())
    }

    fn remove(&mut self, key: &u64) -> Option<Arc<[u8]>> {
        if let Some(value) = self.cache.remove(key) {
            assert!(value.is_some());
            return value;
        }
        if let Some(value) = self.low_pri_cache.remove(key) {
            assert!(value.is_some());
            return value;
        }
        None
    }

    fn return_value(&mut self, key: &u64, value: Arc<[u8]>, priority: CachePriority) {
        if matches!(priority, CachePriority::Low) {
            assert!(self
                .low_pri_cache
                .get_mut(key)
                .unwrap()
                .replace(value)
                .is_none());
        } else {
            assert!(self.cache.get_mut(key).unwrap().replace(value).is_none());
        }
    }

    fn take_value(&mut self, key: &u64) -> Option<Arc<[u8]>> {
        if let Some(value) = self.cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        if let Some(value) = self.low_pri_cache.get_mut(key) {
            let result = value.take().unwrap();
            return Some(result);
        }
        None
    }

    fn pop_lowest_priority(&mut self) -> Option<(u64, Arc<[u8]>, CachePriority)> {
        for (k, v) in self.low_pri_cache.range(..) {
            if v.is_some() {
                let key = *k;
                return self
                    .low_pri_cache
                    .remove(&key)
                    .map(|x| (key, x.unwrap(), CachePriority::Low));
            }
        }
        for (k, v) in self.cache.range(..) {
            if v.is_some() {
                let key = *k;
                return self
                    .cache
                    .remove(&key)
                    .map(|x| (key, x.unwrap(), CachePriority::High));
            }
        }
        None
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.low_pri_cache.clear();
    }
}

#[derive(Debug)]
struct CheckedBackend {
    file: Box<dyn StorageBackend>,
    io_failed: AtomicBool,
}

impl CheckedBackend {
    fn new(file: Box<dyn StorageBackend>) -> Self {
        Self {
            file,
            io_failed: AtomicBool::new(false),
        }
    }

    fn check_failure(&self) -> Result<()> {
        if self.io_failed.load(Ordering::Acquire) {
            Err(StorageError::PreviousIo)
        } else {
            Ok(())
        }
    }

    fn len(&self) -> Result<u64> {
        self.check_failure()?;
        let result = self.file.len();
        if result.is_err() {
            self.io_failed.store(true, Ordering::Release);
        }
        result.map_err(StorageError::from)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.check_failure()?;
        let result = self.file.read(offset, len);
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

    fn sync_data(&self, eventual: bool) -> Result<()> {
        self.check_failure()?;
        let result = self.file.sync_data(eventual);
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
    max_read_cache_bytes: usize,
    read_cache_bytes: AtomicUsize,
    max_write_buffer_bytes: usize,
    write_buffer_bytes: AtomicUsize,
    #[cfg(feature = "cache_metrics")]
    reads_total: AtomicU64,
    #[cfg(feature = "cache_metrics")]
    reads_hits: AtomicU64,
    read_cache: Box<[RwLock<PrioritizedCache>]>,
    // TODO: maybe move this cache to WriteTransaction?
    write_buffer: Arc<Mutex<PrioritizedWriteCache>>,
}

impl PagedCachedFile {
    pub(super) fn new(
        file: Box<dyn StorageBackend>,
        page_size: u64,
        max_read_cache_bytes: usize,
        max_write_buffer_bytes: usize,
    ) -> Result<Self, DatabaseError> {
        let read_cache = (0..Self::lock_stripes())
            .map(|_| RwLock::new(PrioritizedCache::new()))
            .collect();

        Ok(Self {
            file: CheckedBackend::new(file),
            page_size,
            max_read_cache_bytes,
            read_cache_bytes: AtomicUsize::new(0),
            max_write_buffer_bytes,
            write_buffer_bytes: AtomicUsize::new(0),
            #[cfg(feature = "cache_metrics")]
            reads_total: Default::default(),
            #[cfg(feature = "cache_metrics")]
            reads_hits: Default::default(),
            read_cache,
            write_buffer: Arc::new(Mutex::new(PrioritizedWriteCache::new())),
        })
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

    fn flush_write_buffer(&self) -> Result {
        let mut write_buffer = self.write_buffer.lock().unwrap();

        for (offset, buffer) in write_buffer.cache.iter() {
            self.file.write(*offset, buffer.as_ref().unwrap())?;
        }
        for (offset, buffer) in write_buffer.low_pri_cache.iter() {
            self.file.write(*offset, buffer.as_ref().unwrap())?;
        }
        for (offset, buffer) in write_buffer.cache.iter_mut() {
            let buffer = buffer.take().unwrap();
            let cache_size = self
                .read_cache_bytes
                .fetch_add(buffer.len(), Ordering::AcqRel);

            if cache_size + buffer.len() <= self.max_read_cache_bytes {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                if let Some(replaced) = lock.insert(*offset, buffer, CachePriority::High) {
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
        for (offset, buffer) in write_buffer.low_pri_cache.iter_mut() {
            let buffer = buffer.take().unwrap();
            let cache_size = self
                .read_cache_bytes
                .fetch_add(buffer.len(), Ordering::AcqRel);

            if cache_size + buffer.len() <= self.max_read_cache_bytes {
                let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
                let mut lock = self.read_cache[cache_slot].write().unwrap();
                if let Some(replaced) = lock.insert(*offset, buffer, CachePriority::Low) {
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

    // Caller should invalidate all cached pages that are no longer valid
    pub(super) fn resize(&self, len: u64) -> Result {
        // TODO: be more fine-grained about this invalidation
        self.invalidate_cache_all();

        self.file.set_len(len)
    }

    pub(super) fn flush(&self, #[allow(unused_variables)] eventual: bool) -> Result {
        self.flush_write_buffer()?;

        self.file.sync_data(eventual)
    }

    // Make writes visible to readers, but does not guarantee any durability
    pub(super) fn write_barrier(&self) -> Result {
        self.flush_write_buffer()
    }

    // Read directly from the file, ignoring any cached data
    pub(super) fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.file.read(offset, len)
    }

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    pub(super) fn read(
        &self,
        offset: u64,
        len: usize,
        hint: PageHint,
        cache_policy: impl Fn(&[u8]) -> CachePriority,
    ) -> Result<Arc<[u8]>> {
        debug_assert_eq!(0, offset % self.page_size);
        #[cfg(feature = "cache_metrics")]
        self.reads_total.fetch_add(1, Ordering::AcqRel);

        if !matches!(hint, PageHint::Clean) {
            let lock = self.write_buffer.lock().unwrap();
            if let Some(cached) = lock.get(&offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        {
            let read_lock = self.read_cache[cache_slot].read().unwrap();
            if let Some(cached) = read_lock.get(&offset) {
                #[cfg(feature = "cache_metrics")]
                self.reads_hits.fetch_add(1, Ordering::Release);
                debug_assert_eq!(cached.len(), len);
                return Ok(cached.clone());
            }
        }

        let buffer: Arc<[u8]> = self.read_direct(offset, len)?.into();
        let cache_size = self.read_cache_bytes.fetch_add(len, Ordering::AcqRel);
        let mut write_lock = self.read_cache[cache_slot].write().unwrap();
        let cache_size = if let Some(replaced) =
            write_lock.insert(offset, buffer.clone(), cache_policy(&buffer))
        {
            // A race could cause us to replace an existing buffer
            self.read_cache_bytes
                .fetch_sub(replaced.len(), Ordering::AcqRel)
        } else {
            cache_size
        };
        let mut removed = 0;
        if cache_size + len > self.max_read_cache_bytes {
            while removed < len {
                if let Some((_, v)) = write_lock.pop_lowest_priority() {
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
        if let Some(removed) = self.write_buffer.lock().unwrap().remove(&offset) {
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
        if let Some(removed) = lock.remove(&offset) {
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
    pub(super) fn write(
        &self,
        offset: u64,
        len: usize,
        overwrite: bool,
        cache_policy: impl Fn(&[u8]) -> CachePriority,
    ) -> Result<WritablePage> {
        assert_eq!(0, offset % self.page_size);
        let mut lock = self.write_buffer.lock().unwrap();

        // TODO: allow hint that page is known to be dirty and will not be in the read cache
        let cache_slot: usize = (offset % Self::lock_stripes()).try_into().unwrap();
        let existing = {
            let mut lock = self.read_cache[cache_slot].write().unwrap();
            if let Some(removed) = lock.remove(&offset) {
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

        let data = if let Some(removed) = lock.take_value(&offset) {
            removed
        } else {
            let previous = self.write_buffer_bytes.fetch_add(len, Ordering::AcqRel);
            if previous + len > self.max_write_buffer_bytes {
                let mut removed_bytes = 0;
                while removed_bytes < len {
                    if let Some((offset, buffer, removed_priority)) = lock.pop_lowest_priority() {
                        let removed_len = buffer.len();
                        let result = self.file.write(offset, &buffer);
                        if result.is_err() {
                            lock.insert(offset, buffer, removed_priority);
                        }
                        result?;
                        self.write_buffer_bytes
                            .fetch_sub(removed_len, Ordering::Release);
                        removed_bytes += removed_len;
                    } else {
                        break;
                    }
                }
            }
            let result = if let Some(data) = existing {
                data
            } else if overwrite {
                vec![0; len].into()
            } else {
                self.read_direct(offset, len)?.into()
            };
            let priority = cache_policy(&result);
            lock.insert(offset, result, priority);
            lock.take_value(&offset).unwrap()
        };
        let priority = cache_policy(&data);
        Ok(WritablePage {
            buffer: self.write_buffer.clone(),
            offset,
            data,
            priority,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::backends::InMemoryBackend;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use crate::tree_store::{CachePriority, PageHint};
    use crate::StorageBackend;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    // TODO: Switch to threaded wasi build for the tests
    #[cfg(not(target_os = "wasi"))]
    #[test]
    fn cache_leak() {
        let backend = InMemoryBackend::new();
        backend.set_len(1024).unwrap();
        let cached_file = PagedCachedFile::new(Box::new(backend), 128, 1024, 128).unwrap();
        let cached_file = Arc::new(cached_file);

        let t1 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file
                        .read(0, 128, PageHint::None, CachePriority::default_btree)
                        .unwrap();
                    cached_file.invalidate_cache(0, 128);
                }
            })
        };
        let t2 = {
            let cached_file = cached_file.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    cached_file
                        .read(0, 128, PageHint::None, CachePriority::default_btree)
                        .unwrap();
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
