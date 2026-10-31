use crate::tree_store::page_store::fast_hash::{FastHashMapU64, Shrink};
use alloc::collections::VecDeque;
use core::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct LRUCache<T> {
    // AtomicBool is the second chance flag. The u32 is the sequence number of the key's live
    // entry in lru_queue. remove() leaves the queue entry behind, and a later insert() of the
    // same key pushes a new one, so a queue entry whose sequence number does not match is stale.
    cache: FastHashMapU64<(T, AtomicBool, u32)>,
    lru_queue: VecDeque<(u64, u32)>,
    // Wrapping is harmless: a stale entry could only be mistaken for a live one if it survived
    // 2^32 inserts, and the queue is bounded to a small multiple of the cache size.
    next_seq: u32,
}

impl<T> LRUCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: FastHashMapU64::default(),
            lru_queue: VecDeque::default(),
            next_seq: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn insert(&mut self, key: u64, value: T) -> Option<T> {
        if let Some((existing, second_chance, _)) = self.cache.get_mut(&key) {
            second_chance.store(false, Ordering::Release);
            return Some(core::mem::replace(existing, value));
        }
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.cache.insert(key, (value, AtomicBool::new(false), seq));
        self.lru_queue.push_back((key, seq));
        None
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<T> {
        let (value, _, _) = self.cache.remove(&key)?;
        if self.lru_queue.len() > 2 * self.cache.len() {
            // Cycle two elements of the LRU queue to ensure it doesn't grow without bound.
            // Stale entries are dropped, so each pass through the queue removes all of them.
            for _ in 0..2 {
                if let Some((removed_key, seq)) = self.lru_queue.pop_front()
                    && let Some(second_chance) = self.live_entry(removed_key, seq)
                {
                    second_chance.store(false, Ordering::Release);
                    self.lru_queue.push_back((removed_key, seq));
                }
            }
        }
        Some(value)
    }

    // Returns the second chance flag, if the queue entry is the live one for a cached key
    fn live_entry(&self, key: u64, seq: u32) -> Option<&AtomicBool> {
        self.cache
            .get(&key)
            .filter(|(_, _, live_seq)| *live_seq == seq)
            .map(|(_, second_chance, _)| second_chance)
    }

    pub(crate) fn get(&self, key: u64) -> Option<&T> {
        if let Some((value, second_chance, _)) = self.cache.get(&key) {
            second_chance.store(true, Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        if let Some((value, second_chance, _)) = self.cache.get_mut(&key) {
            second_chance.store(true, Ordering::Release);
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &T)> {
        self.cache.iter().map(|(k, (v, _, _))| (k, v))
    }

    pub(crate) fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut T)> {
        self.cache.iter_mut().map(|(k, (v, _, _))| (k, v))
    }

    pub(crate) fn pop_lowest_priority(&mut self) -> Option<(u64, T)> {
        while let Some((key, seq)) = self.lru_queue.pop_front() {
            if let Some(second_chance) = self.live_entry(key, seq) {
                if second_chance
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.lru_queue.push_back((key, seq));
                } else {
                    let (value, _, _) = self.cache.remove(&key).unwrap();
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
mod test {
    use super::LRUCache;

    // Writing to a cached page removes it from the read cache and re-inserts it on flush.
    // Nothing evicts when the cache never fills, so this must not grow the queue.
    #[test]
    fn queue_stays_bounded_without_eviction() {
        const KEYS: u64 = 64;

        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 0..KEYS {
            cache.insert(k, k);
        }
        for _ in 0..5_000 {
            for k in 0..KEYS {
                cache.remove(k);
                cache.insert(k, k);
            }
        }
        let keys = usize::try_from(KEYS).unwrap();
        assert_eq!(cache.len(), keys);
        assert!(cache.lru_queue.len() <= 3 * keys);

        // Every key is still evictable, exactly once
        let mut evicted = vec![];
        while let Some((k, _)) = cache.pop_lowest_priority() {
            evicted.push(k);
        }
        evicted.sort_unstable();
        assert_eq!(evicted, (0..KEYS).collect::<Vec<_>>());
        assert_eq!(cache.len(), 0);
        assert!(cache.lru_queue.is_empty());
    }

    // Churn concentrated on one key behind many long-lived keys
    #[test]
    fn queue_stays_bounded_with_hot_key() {
        const KEYS: u64 = 1024;

        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 0..KEYS {
            cache.insert(k, k);
        }
        for _ in 0..100_000 {
            cache.remove(KEYS - 1);
            cache.insert(KEYS - 1, KEYS - 1);
        }
        let keys = usize::try_from(KEYS).unwrap();
        assert_eq!(cache.len(), keys);
        assert!(cache.lru_queue.len() <= 3 * keys);
    }

    // A re-inserted key must be evicted at its new position, not at its stale one
    #[test]
    fn reinsert_moves_key_to_the_back() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 1..=3 {
            cache.insert(k, k);
        }
        cache.remove(1);
        cache.insert(1, 1);

        let mut evicted = vec![];
        while let Some((k, _)) = cache.pop_lowest_priority() {
            evicted.push(k);
        }
        assert_eq!(evicted, vec![2, 3, 1]);
    }

    #[test]
    fn second_chance_survives_one_eviction_pass() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        for k in 1..=3 {
            cache.insert(k, k);
        }
        assert_eq!(cache.get(1), Some(&1));

        assert_eq!(cache.pop_lowest_priority(), Some((2, 2)));
        assert_eq!(cache.pop_lowest_priority(), Some((3, 3)));
        assert_eq!(cache.pop_lowest_priority(), Some((1, 1)));
        assert_eq!(cache.pop_lowest_priority(), None);
    }

    #[test]
    fn replacing_a_value_keeps_its_queue_position() {
        let mut cache: LRUCache<u64> = LRUCache::new();
        cache.insert(1, 1);
        cache.insert(2, 2);
        assert_eq!(cache.insert(1, 10), Some(1));
        assert_eq!(cache.lru_queue.len(), 2);

        assert_eq!(cache.pop_lowest_priority(), Some((1, 10)));
        assert_eq!(cache.pop_lowest_priority(), Some((2, 2)));
    }
}
