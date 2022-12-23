use std::mem::size_of;
use std::sync::{Condvar, Mutex};
use arbitrary::Unstructured;
use libfuzzer_sys::arbitrary::Arbitrary;
use rand_distr::{Binomial, Distribution};
use rand::rngs::StdRng;
use rand::SeedableRng;

// Limit values to 10MiB
const MAX_CACHE_SIZE: usize = 100_000_000;
const MAX_VALUE_SIZE: usize = 10_000_000;
const KEY_SPACE: u64 = 1_000_000;
pub const MAX_SAVEPOINTS: usize = 3;

#[derive(Debug, Clone)]
pub(crate) struct BoundedU64<const N: u64> {
    pub value: u64
}

impl<const N: u64> Arbitrary<'_> for BoundedU64<N> {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let value: u64 = u.int_in_range(0..=(N - 1))?;
        Ok(Self {
            value
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (size_of::<u64>(), Some(size_of::<u64>()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BinomialDifferenceBoundedUSize<const N: usize> {
    pub value: usize
}

impl<const N: usize> Arbitrary<'_> for BinomialDifferenceBoundedUSize<N> {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed: u64 = u.arbitrary()?;
        let mut rng = StdRng::seed_from_u64(seed);
        // Distribution which is the difference from the median of B(N, 0.5)
        let distribution = Binomial::new(N as u64, 0.5).unwrap();
        let value = distribution.sample(&mut rng) as isize;
        let value = (value - N as isize / 2).abs() as usize;
        Ok(Self {
            value
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (size_of::<u64>(), Some(size_of::<u64>()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PowerOfTwoBetween<const M: u32, const N: u32> {
    pub value: usize
}

impl<const M: u32, const N: u32> Arbitrary<'_> for PowerOfTwoBetween<M, N> {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let value: u32 = u.int_in_range(M..=N)?;
        Ok(Self {
            value: 2usize.pow(value)
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (size_of::<u32>(), Some(size_of::<u32>()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BoundedUSize<const N: usize> {
    pub value: usize
}

impl<const N: usize> Arbitrary<'_> for BoundedUSize<N> {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let value: usize = u.int_in_range(0..=(N - 1))?;
        Ok(Self {
            value
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (size_of::<usize>(), Some(size_of::<usize>()))
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub(crate) enum FuzzOperation {
    Get {
        key: BoundedU64<KEY_SPACE>,
    },
    Insert {
        key: BoundedU64<KEY_SPACE>,
        value_size: BinomialDifferenceBoundedUSize<MAX_VALUE_SIZE>,
    },
    InsertReserve {
        key: BoundedU64<KEY_SPACE>,
        value_size: BinomialDifferenceBoundedUSize<MAX_VALUE_SIZE>,
    },
    Remove {
        key: BoundedU64<KEY_SPACE>,
    },
    RemoveOne {
        key: BoundedU64<KEY_SPACE>,
        value_size: BinomialDifferenceBoundedUSize<MAX_VALUE_SIZE>,
    },
    Len {
    },
    Range {
        start_key: BoundedU64<KEY_SPACE>,
        len: BoundedU64<KEY_SPACE>,
        reversed: bool,
    },
}

#[derive(Arbitrary, Debug, Clone)]
pub(crate) struct FuzzTransaction {
    pub ops: Vec<FuzzOperation>,
    pub durable: bool,
    pub commit: bool,
    pub create_savepoint: bool,
    pub restore_savepoint: BoundedUSize<MAX_SAVEPOINTS>,
}

#[derive(Arbitrary, Debug, Clone)]
pub(crate) struct FuzzConfig {
    pub use_checksums: bool,
    pub multimap_table: bool,
    pub use_mmap: bool,
    pub read_cache_size: BoundedUSize<MAX_CACHE_SIZE>,
    pub write_cache_size: BoundedUSize<MAX_CACHE_SIZE>,
    pub thread0_transactions: Vec<FuzzTransaction>,
    pub thread1_transactions: Vec<FuzzTransaction>,
    pub page_size: PowerOfTwoBetween<9, 14>,
}

pub(crate) struct CustomBarrier {
    mutex: Mutex<(u64, u64)>,
    condition: Condvar
}

impl CustomBarrier {
    pub(crate) fn new(waiters: u64) -> Self {
        Self {
            mutex: Mutex::new((0, waiters)),
            condition: Default::default()
        }
    }

    pub(crate) fn decrement_waiters(&self) {
        let mut guard = self.mutex.lock().unwrap();
        guard.1 -= 1;
        if guard.0 == guard.1 {
            guard.0 = 0;
            self.condition.notify_all()
        }
    }

    pub(crate) fn wait(&self) {
        let mut guard = self.mutex.lock().unwrap();
        guard.0 += 1;
        if guard.0 == guard.1 {
            guard.0 = 0;
            self.condition.notify_all();
        } else {
            drop(self.condition.wait(guard).unwrap());
        }
    }
}
