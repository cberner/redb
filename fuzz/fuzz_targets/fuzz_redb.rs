#![no_main]
use libfuzzer_sys::fuzz_target;
use lmdb::{EnvironmentFlags, Transaction};
use tempfile::{NamedTempFile, TempDir};
use redb::{Database, Durability, ReadableTable, TableDefinition};

mod common;
use common::*;

const TABLE_DEF: TableDefinition<u64, [u8]> = TableDefinition::new("fuzz_table");

fuzz_target!(|config: RedbFuzzConfig| {
    let redb_file: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(redb_file.path(), config.max_db_size.value) };

    // TODO: check that the error is sensible
    if db.is_err() {
        return;
    }
    let db = db.unwrap();
    // TODO: check against BtreeMap instead, to make the fuzzing faster
    let lmdb_dir: TempDir = tempfile::tempdir().unwrap();
    let env = lmdb::Environment::new().set_flags(EnvironmentFlags::NO_SYNC).open(lmdb_dir.path()).unwrap();
    env.set_map_size(10 * 1024 * 1024 * 1024).unwrap();
    let lmdb_db = env.open_db(None).unwrap();

    for transaction in config.transactions.iter() {
        let mut txn = db.begin_write().unwrap();
        // We're not trying to test crash safety, so don't bother with durability
        txn.set_durability(Durability::None);
        let mut lmdb_txn = env.begin_rw_txn().unwrap();
        {
            let mut table = txn.open_table(TABLE_DEF).unwrap();
            for op in transaction {
                match op {
                    RedbFuzzOperation::Get { key } => {
                        let key = key.value;
                        match lmdb_txn.get(lmdb_db, &key.to_be_bytes()) {
                            Ok(lmdb_value) => {
                                let value = table.get(&key).unwrap().unwrap();
                                assert_eq!(value, lmdb_value);
                            },
                            Err(err) => {
                                if matches!(err, lmdb::Error::NotFound) {
                                    assert!(table.get(&key).unwrap().is_none());
                                }
                            }
                        }
                    },
                    RedbFuzzOperation::Insert { key, value_size } => {
                        let key = key.value;
                        // Limit values to 10MiB
                        let value_size = value_size.value as usize;
                        let value = vec![0xFF; value_size];
                        if let Err(redb::Error::OutOfSpace) = table.insert(&key, &value) {
                            if config.oom_plausible() {
                                return;
                            }
                        }
                        lmdb_txn.put(lmdb_db, &key.to_be_bytes(), &value, lmdb::WriteFlags::empty()).unwrap();
                    },
                    RedbFuzzOperation::Remove { key } => {
                        let key = key.value;
                        table.remove(&key).unwrap();
                        // TODO: error checking
                        let _ = lmdb_txn.del(lmdb_db, &key.to_be_bytes(), None);
                    }
                }
            }
        }
        if let Err(redb::Error::OutOfSpace) = txn.commit() {
            if config.oom_plausible() {
                return;
            }
        }
        lmdb_txn.commit().unwrap();
    }
});
