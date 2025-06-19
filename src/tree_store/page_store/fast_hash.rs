use crate::tree_store::PageNumber;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

// See "Computationally easy, spectrally good multipliers for congruential pseudorandom number generators" by Steele & Vigna
const K: u64 = 0xf135_7aea_2e62_a9c5;

pub(crate) type FastHashMapU64<V> = HashMap<u64, V, BuildHasherDefault<FastHasher64>>;
pub(crate) type PageNumberHashSet = HashSet<PageNumber, BuildHasherDefault<FastHasher64>>;

#[derive(Copy, Clone, Default, Eq, PartialEq)]
pub(crate) struct FastHasher64 {
    hash: u64,
}

impl Hasher for FastHasher64 {
    fn finish(&self) -> u64 {
        #[cfg(target_pointer_width = "64")]
        const ROTATE: u32 = 26;
        #[cfg(target_pointer_width = "32")]
        const ROTATE: u32 = 15;

        self.hash.rotate_left(ROTATE)
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("Only hashing 8 bytes is supported");
    }

    fn write_u64(&mut self, x: u64) {
        debug_assert_eq!(self.hash, 0);
        self.hash = x.wrapping_mul(K);
    }
}
