use std::mem::size_of;
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Arbitrary, Debug)]
pub(crate) enum FuzzOperation {
    Get {
        key: BoundedU64<KEY_SPACE>,
    },
    Insert {
        key: BoundedU64<KEY_SPACE>,
        value_size: BinomialDifferenceBoundedUSize<MAX_VALUE_SIZE>,
    },
    Remove {
        key: BoundedU64<KEY_SPACE>,
    },
    Len {
    },
    Range {
        start_key: BoundedU64<KEY_SPACE>,
        len: BoundedU64<KEY_SPACE>,
        reversed: bool,
    },
}

#[derive(Arbitrary, Debug)]
pub(crate) struct FuzzTransaction {
    pub ops: Vec<FuzzOperation>,
    pub durable: bool,
}

#[derive(Arbitrary, Debug)]
pub(crate) struct FuzzConfig {
    pub transactions: Vec<FuzzTransaction>,
    pub max_db_size: BoundedUSize<MAX_DB_SIZE>,
}

impl FuzzConfig {
    pub(crate) fn oom_plausible(&self) -> bool {
        let mut total_entries = 0;
        let mut total_value_bytes = 0;
        for transaction in self.transactions.iter() {
            for write in transaction.ops.iter() {
                total_entries += 1;
                total_value_bytes += match write {
                    FuzzOperation::Insert { value_size, .. } => value_size.value,
                    _ => 0
                };
            }
        }

        let approximate_overhead = 256_000;
        let expected_size = total_value_bytes + (size_of::<u64>() * 4) * total_entries + approximate_overhead;
        // If we're within a factor of 10 of the max db size, then assume an OOM is plausible
        expected_size * 10 > self.max_db_size.value
    }
}