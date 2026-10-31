// The lookup tables that redb uses on hot paths, all keyed by an integer. `alloc` has no hash map,
// so builds without std substitute the ordered collections. That costs O(log n) lookups, but the
// iteration order of these tables is never observable, so nothing else changes.
#[cfg(not(redb_no_std))]
mod hash {
    use crate::tree_store::PageNumber;
    use core::hash::{BuildHasherDefault, Hasher};
    use std::collections::{HashMap, HashSet};

    // See "Computationally easy, spectrally good multipliers for congruential pseudorandom number generators" by Steele & Vigna
    const K: u64 = 0xf135_7aea_2e62_a9c5;

    pub(crate) type FastHashMapU64<V> = HashMap<u64, V, BuildHasherDefault<FastHasher64>>;
    pub(crate) type PageNumberHashMap<V> = HashMap<PageNumber, V, BuildHasherDefault<FastHasher64>>;
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
}

#[cfg(redb_no_std)]
mod hash {
    use crate::tree_store::PageNumber;
    use alloc::collections::{BTreeMap, BTreeSet};

    pub(crate) type FastHashMapU64<V> = BTreeMap<u64, V>;
    pub(crate) type PageNumberHashMap<V> = BTreeMap<PageNumber, V>;
    pub(crate) type PageNumberHashSet = BTreeSet<PageNumber>;
}

pub(crate) use hash::{FastHashMapU64, PageNumberHashMap, PageNumberHashSet};

// Releases the spare capacity of one of the tables above. The ordered fallbacks hold none, so it
// is a no-op for them.
pub(crate) trait Shrink {
    fn shrink(&mut self);
}

#[cfg(not(redb_no_std))]
mod shrink {
    use super::Shrink;
    use core::hash::{BuildHasher, Hash};
    use std::collections::{HashMap, HashSet};

    impl<K: Eq + Hash, V, S: BuildHasher> Shrink for HashMap<K, V, S> {
        fn shrink(&mut self) {
            self.shrink_to_fit();
        }
    }

    impl<K: Eq + Hash, S: BuildHasher> Shrink for HashSet<K, S> {
        fn shrink(&mut self) {
            self.shrink_to_fit();
        }
    }
}

#[cfg(redb_no_std)]
mod shrink {
    use super::Shrink;
    use alloc::collections::{BTreeMap, BTreeSet};

    impl<K, V> Shrink for BTreeMap<K, V> {
        fn shrink(&mut self) {}
    }

    impl<K> Shrink for BTreeSet<K> {
        fn shrink(&mut self) {}
    }
}
