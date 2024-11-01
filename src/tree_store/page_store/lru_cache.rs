use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub struct LRUCache<T> {
    // AtomicBool is the second chance flag
    cache: HashMap<u64, (T, AtomicBool)>,
    lru_queue: VecDeque<u64>,
}

impl<T> LRUCache<T> {
    pub(crate) fn new() -> Self {
        Self {
            cache: Default::default(),
            lru_queue: Default::default(),
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
                // Cycle two elements of the LRU queue to ensure it doesn't grow without bound
                for _ in 0..2 {
                    if let Some(removed_key) = self.lru_queue.pop_front() {
                        if let Some((_, second_chance)) = self.cache.get(&removed_key) {
                            second_chance.store(false, Ordering::Release);
                            self.lru_queue.push_back(removed_key);
                        }
                    }
                }
            }
            Some(value)
        } else {
            None
        }
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
        self.cache.clear();
        self.lru_queue.clear();
    }
}
