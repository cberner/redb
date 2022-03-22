use std::mem::size_of;
use libfuzzer_sys::arbitrary::Arbitrary;

pub const MAX_VALUE_SIZE: usize = 10_000_000;

// TODO: expand coverage to more operations
#[derive(Arbitrary, Debug)]
pub(crate) enum RedbFuzzOperation {
    Get {
        key: u64,
    },
    Insert {
        key: u64,
        value_size: usize,
    },
    Remove {
        key: u64,
    },
}

#[derive(Arbitrary, Debug)]
pub(crate) struct RedbFuzzConfig {
    pub transactions: Vec<Vec<RedbFuzzOperation>>,
    pub max_db_size: usize,
}

impl RedbFuzzConfig {
    pub(crate) fn oom_plausible(&self) -> bool {
        let mut total_entries = 0;
        let mut total_value_bytes = 0;
        for transaction in self.transactions.iter() {
            for write in transaction.iter() {
                total_entries += 1;
                total_value_bytes += match write {
                    RedbFuzzOperation::Insert { value_size, .. } => *value_size % MAX_VALUE_SIZE,
                    _ => 0
                };
            }
        }

        let expected_size = total_value_bytes + (size_of::<u64>() * 4) * total_entries;
        // If we're within a factor of 10 of the max db size, then assume an OOM is plausible
        expected_size * 10 > self.max_db_size
    }
}
