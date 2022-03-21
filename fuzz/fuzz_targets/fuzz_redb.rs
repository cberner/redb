#![no_main]
use libfuzzer_sys::fuzz_target;
use tempfile::NamedTempFile;
use redb::{Database, TableDefinition};

mod common;
use common::*;

const TABLE_DEF: TableDefinition<u64, [u8]> = TableDefinition::new("fuzz_table");

fuzz_target!(|config: RedbFuzzConfig| {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), config.max_db_size) };

    // TODO: check that the error is sensible
    if db.is_err() {
        return;
    }
    let db = db.unwrap();

    for transaction in config.transactions {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_DEF).unwrap();
            for op in transaction {
                match op {
                    RedbFuzzOperation::Insert { key } => {
                        table.insert(&key, &[]).unwrap();
                    },
                    RedbFuzzOperation::Remove { key } => {
                        table.remove(&key).unwrap();
                    }
                }
            }
        }
        txn.commit().unwrap();
    }
});
