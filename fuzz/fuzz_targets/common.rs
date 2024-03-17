use std::mem::size_of;
use arbitrary::Unstructured;
use libfuzzer_sys::arbitrary::Arbitrary;
use rand_distr::{Binomial, Distribution};
use rand::rngs::StdRng;
use rand::SeedableRng;

const MAX_CRASH_OPS: u64 = 20;
const MAX_CACHE_SIZE: usize = 100_000_000;
// Limit values to 100KiB
const MAX_VALUE_SIZE: usize = 100_000;
const KEY_SPACE: u64 = 1_000_000;
pub const MAX_SAVEPOINTS: usize = 6;

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
pub(crate) struct U64Between<const MIN: u64, const MAX: u64> {
    pub value: u64
}

impl<const MIN: u64, const MAX: u64> Arbitrary<'_> for U64Between<MIN, MAX> {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let value: u64 = u.int_in_range(MIN..=MAX)?;
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
    PopFirst {
    },
    PopLast {
    },
    Retain {
        modulus: U64Between<1, 8>,
    },
    RetainIn {
        start_key: BoundedU64<KEY_SPACE>,
        len: BoundedU64<KEY_SPACE>,
        modulus: U64Between<1, 8>,
    },
    ExtractIf {
        modulus: U64Between<1, 8>,
        take: BoundedUSize<10>,
        reversed: bool,
    },
    ExtractFromIf {
        start_key: BoundedU64<KEY_SPACE>,
        range_len: BoundedU64<KEY_SPACE>,
        take: BoundedUSize<10>,
        modulus: U64Between<1, 8>,
        reversed: bool,
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
    pub create_ephemeral_savepoint: bool,
    pub create_persistent_savepoint: bool,
    pub restore_savepoint: Option<BoundedUSize<MAX_SAVEPOINTS>>,
}

#[derive(Arbitrary, Debug, Clone)]
pub(crate) struct FuzzConfig {
    pub multimap_table: bool,
    pub cache_size: BoundedUSize<MAX_CACHE_SIZE>,
    pub crash_after_ops: BoundedU64<MAX_CRASH_OPS>,
    pub transactions: Vec<FuzzTransaction>,
    pub page_size: PowerOfTwoBetween<9, 14>,
    // Must not be too small, otherwise persistent savepoints won't fit into a region
    pub region_size: PowerOfTwoBetween<20, 30>,
}
