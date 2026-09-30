use crate::tree_store::page_store::fast_hash::{FastHashMapU64, FastHashSetU64, Shrink};
use alloc::collections::VecDeque;
use core::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct LRUCache<T> {
    // AtomicBool is the second chance flag
    cache: FastHashMapU64<(T, AtomicBool)>,
    lru_queue: VecDeque<u64>,
}

impl<T> LRUCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: FastHashMapU64::default(),
            lru_queue: VecDeque::default(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
        let result = self
            .cache
            .insert(key, (value, AtomicBool::new(false)))
            .map(|(x, _)| x);
        if result.is_none() {
            self.lru_queue.push_back(key);
        }
        result
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
        if let Some((value, _)) = self.cache.remove(&key) {
            if self.lru_queue.len() > 2 * self.cache.len() {
                self.compact_queue();
            }
            Some(value)
        } else {
            None
        }
    }

    /// Rebuild `lru_queue` so that it holds exactly one entry per cached key.
    ///
    /// The queue is append-only outside of eviction: `insert` pushes whenever the
    /// key was absent, and `remove` only takes the key out of `cache`, leaving the
    /// queue entry behind. That is harmless while `pop_lowest_priority` runs, since
    /// it discards entries whose key is gone -- but it only runs when the cache is
    /// over its byte limit, and a cache whose limit exceeds the file it is caching
    /// never gets there. Nothing then drains the queue at all.
    ///
    /// Cycling two front entries could not bound it. A live front entry is pushed
    /// straight back, so the net drain is zero whenever long-lived pages sit at the
    /// front; and once a key has been removed and re-inserted, its duplicate entry
    /// is indistinguishable from a live one, so no pass can tell them apart. Under
    /// repeated writes to cached pages that is a permanent +1 entry per write,
    /// unbounded in the number of writes even though the working set is flat, and
    /// invisible to the cache's byte accounting.
    ///
    /// Compaction keeps the last occurrence of each still-cached key, which
    /// preserves recency order and drops stale and duplicate entries alike. It is
    /// O(queue), but the existing trigger only fires once the queue has passed
    /// twice the cache size, so the next compaction is `cache.len()` pushes away:
    /// amortised O(1) per operation.
    fn compact_queue(&mut self) {
        let mut seen = FastHashSetU64::default();
        let mut compacted = VecDeque::with_capacity(self.cache.len());
        // Walk newest-first so the entry kept is the most recent push, then rebuild
        // oldest-first to preserve the queue's ordering.
        for key in self.lru_queue.iter().rev() {
            if self.cache.contains_key(key) && seen.insert(*key) {
                compacted.push_front(*key);
            }
        }
        self.lru_queue = compacted;
    }

    pub(crate) fn get(&self, key: u64) -> Option<&T> {
        if let Some((value, second_chance)) = self.cache.get(&key) {
            second_chance.store(true, Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        if let Some((value, second_chance)) = self.cache.get_mut(&key) {
            second_chance.store(true, Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &T)> {
        self.cache.iter().map(|(k, (v, _))| (k, v))
    }

    pub(crate) fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut T)> {
        self.cache.iter_mut().map(|(k, (v, _))| (k, v))
    }

    pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
        while let Some(key) = self.lru_queue.pop_front() {
            if let Some((_, second_chance)) = self.cache.get(&key) {
                if second_chance
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.lru_queue.push_back(key);
                } else {
                    let (value, _) = self.cache.remove(&key).unwrap();
                    return Some((key, value));
                }
            }
        }
        None
    }

    pub(crate) fn clear(&mut self) {
        self.cache.shrink();
        self.cache.clear();
        self.lru_queue.shrink_to_fit();
        self.lru_queue.clear();
    }
}

#[cfg(test)]
mod churn_tests {
    use super::LRUCache;

    /// `lru_queue` is append-only outside of eviction: `insert` pushes whenever the
    /// key was absent, and `remove` only takes the key out of `cache`, leaving its
    /// queue entry behind. `pop_lowest_priority` is the only path that discards a
    /// stale entry, and it runs only when the cache is over its byte limit.
    ///
    /// A cache whose limit exceeds the file it is caching therefore never drains
    /// the queue at all. `PagedCachedFile::write` does a `remove` of any page
    /// already cached, and the flush path re-inserts it, so every write to a cached
    /// page orphans one entry and appends another: +8 bytes per write, unbounded in
    /// the number of writes, with a completely flat working set.
    #[test]
    fn the_queue_stays_bounded_when_the_cache_never_evicts() {
        const KEYS: u64 = 64;
        const ROUNDS: u64 = 5_000;

        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 0..KEYS {
            cache.insert(k, k);
        }
        let live = cache.len();

        for _ in 0..ROUNDS {
            for k in 0..KEYS {
                // One write to a cached page: `remove` the cached buffer, then
                // re-insert it at the same offset on flush. Never large enough to
                // put the cache over its limit, so eviction never runs.
                cache.remove(k);
                cache.insert(k, k);
            }
        }

        assert_eq!(cache.len(), live, "the working set must not change");
        assert!(
            cache.lru_queue.len() <= 2 * live,
            "lru_queue grew without bound: {} entries for {live} cached keys after \
             {} write cycles",
            cache.lru_queue.len(),
            ROUNDS * KEYS,
        );
    }

    /// Whatever bounds the queue must not disturb what the cache holds, only how
    /// the queue records it.
    #[test]
    fn every_cached_entry_survives_a_bounded_queue() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 0..32 {
            cache.insert(k, k * 10);
        }
        for _ in 0..500 {
            for k in 0..32 {
                cache.remove(k);
                cache.insert(k, k * 10);
            }
        }
        for k in 0..32 {
            assert_eq!(cache.get(k), Some(&(k * 10)), "key {k} lost");
        }
        assert_eq!(cache.len(), 32);
    }
}
