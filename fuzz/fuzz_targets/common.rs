use std::cmp::max;
use std::mem::size_of;
use std::sync::{Condvar, Mutex};
use arbitrary::Unstructured;
use libfuzzer_sys::arbitrary::Arbitrary;
use rand_distr::{Binomial, Distribution};
use rand::rngs::StdRng;
use rand::SeedableRng;

// Limit values to 10MiB
const MAX_VALUE_SIZE: usize = 10_000_000;
// Limit testing to 1TB databases
const MAX_DB_SIZE: usize = 2usize.pow(40);
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
    pub max_db_size: BoundedUSize<MAX_DB_SIZE>,
    pub thread0_transactions: Vec<FuzzTransaction>,
    pub thread1_transactions: Vec<FuzzTransaction>,
}

impl FuzzConfig {
    pub(crate) fn oom_plausible(&self) -> bool {
        const PAGE_SIZE: usize = 4096;

        let mut total_entries = 0;
        let mut total_value_bytes = 0;
        let mut consecutive_nondurable_commits = 0;
        let mut max_consecutive_nondurable_commits = 0;
        let mut savepoint_restores = 0;
        let mut transactions = self.thread0_transactions.clone();
        transactions.extend(self.thread1_transactions.iter().cloned());
        for transaction in transactions.iter() {
            if transaction.restore_savepoint.value > 0 {
                savepoint_restores += 1;
            }
            if !transaction.commit {
                continue;
            }
            for write in transaction.ops.iter() {
                total_entries += 1;
                total_value_bytes += match write {
                    FuzzOperation::Insert { value_size, .. } => value_size.value,
                    _ => 0
                };
            }
            if !transaction.durable {
                consecutive_nondurable_commits += 1;
            } else {
                consecutive_nondurable_commits = 0;
            }
            max_consecutive_nondurable_commits = max(max_consecutive_nondurable_commits, consecutive_nondurable_commits);
        }

        let approximate_overhead = 4 * PAGE_SIZE + PAGE_SIZE * savepoint_restores;
        let expected_size = (total_value_bytes + (size_of::<u64>() * 4) * total_entries) * (1 + max_consecutive_nondurable_commits) + approximate_overhead;
        // If we're within a factor of 10 of the max db size, then assume an OOM is plausible
        expected_size * 10 > self.max_db_size.value
    }
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
