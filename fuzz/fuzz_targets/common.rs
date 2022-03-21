use libfuzzer_sys::arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
pub(crate) enum RedbFuzzOperation {
    Insert {
        key: u64,
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
