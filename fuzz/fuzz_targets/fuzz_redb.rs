#![no_main]

use std::collections::{BTreeMap, BTreeSet};
use libfuzzer_sys::fuzz_target;
use tempfile::NamedTempFile;
use redb::{Database, Durability, Error, MultimapTableDefinition, MultimapValueIter, ReadableMultimapTable, ReadableTable, TableDefinition, WriteStrategy};

mod common;
use common::*;

const TABLE_DEF: TableDefinition<u64, [u8]> = TableDefinition::new("fuzz_table");
const MULTIMAP_TABLE_DEF: MultimapTableDefinition<u64, [u8]> = MultimapTableDefinition::new("fuzz_multimap_table");

fn exec_table(db: Database, transactions: &[FuzzTransaction]) -> Result<(), redb::Error> {
    let mut reference = BTreeMap::new();

    for transaction in transactions.iter() {
        let reference_backup = reference.clone();
        let mut txn = db.begin_write().unwrap();
        // We're not trying to test crash safety, so don't bother with durability
        if !transaction.durable {
            txn.set_durability(Durability::None);
        }
        {
            let mut table = txn.open_table(TABLE_DEF).unwrap();
            for op in transaction.ops.iter() {
                match op {
                    FuzzOperation::Get { key } => {
                        let key = key.value;
                        match reference.get(&key) {
                            Some(reference_len) => {
                                let value = table.get(&key).unwrap().unwrap();
                                assert_eq!(value.len(), *reference_len);
                            },
                            None => {
                                assert!(table.get(&key).unwrap().is_none());
                            }
                        }
                    },
                    FuzzOperation::Insert { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let value = vec![0xFF; value_size];
                        table.insert(&key, &value)?;
                        reference.insert(key, value_size);
                    },
                    FuzzOperation::Remove { key } | FuzzOperation::RemoveOne { key, .. } => {
                        let key = key.value;
                        match reference.remove(&key) {
                            Some(reference_len) => {
                                let value = table.remove(&key)?;
                                assert_eq!(value.unwrap().to_value().len(), reference_len);
                            },
                            None => {
                                assert!(table.remove(&key).unwrap().is_none());
                            }
                        }
                    },
                    FuzzOperation::Len {} => {
                        assert_eq!(reference.len(), table.len().unwrap());
                    }
                    FuzzOperation::Range { start_key, len, reversed } => {
                        let start = start_key.value;
                        let end = start + len.value;
                        let mut reference_iter: Box<dyn Iterator<Item=(&u64, &usize)>> = if *reversed {
                            Box::new(reference.range(start..end).rev())
                        } else {
                            Box::new(reference.range(start..end))
                        };
                        let mut iter = if *reversed {
                            table.range(start..end).unwrap().rev()
                        } else {
                            table.range(start..end).unwrap()
                        };
                        while let Some((ref_key, ref_value_len)) = reference_iter.next() {
                            let (key, value) = iter.next().unwrap();
                            assert_eq!(*ref_key, key);
                            assert_eq!(*ref_value_len, value.len());
                        }
                        assert!(iter.next().is_none());
                    },
                }
            }
        }
        if transaction.commit {
            txn.commit()?;
        } else {
            txn.abort().unwrap();
            reference = reference_backup;
        }
    }

    Ok(())
}

fn assert_multimap_value_eq(mut iter: MultimapValueIter<[u8]>, reference: Option<&BTreeSet<usize>>) {
    if let Some(values) = reference {
        for value in values.iter() {
            assert_eq!(iter.next().unwrap().len(), *value);
        }
    }
    assert!(iter.next().is_none());
}

fn exec_multimap_table(db: Database, transactions: &[FuzzTransaction]) -> Result<(), redb::Error> {
    let mut reference: BTreeMap<u64, BTreeSet<usize>> = BTreeMap::new();

    for transaction in transactions.iter() {
        let reference_backup = reference.clone();
        let mut txn = db.begin_write().unwrap();
        // We're not trying to test crash safety, so don't bother with durability
        if !transaction.durable {
            txn.set_durability(Durability::None);
        }
        {
            let mut table = txn.open_multimap_table(MULTIMAP_TABLE_DEF).unwrap();
            for op in transaction.ops.iter() {
                match op {
                    FuzzOperation::Get { key } => {
                        let key = key.value;
                        let iter = table.get(&key).unwrap();
                        let entry = reference.get(&key);
                        assert_multimap_value_eq(iter, entry);
                    }
                    FuzzOperation::Insert { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let value = vec![0xFFu8; value_size];
                        table.insert(&key, &value)?;
                        reference.entry(key).or_default().insert(value_size);
                    }
                    FuzzOperation::Remove { key } => {
                        let key = key.value;
                        let entry = reference.remove(&key);
                        let iter = table.remove_all(&key).unwrap();
                        assert_multimap_value_eq(iter, entry.as_ref());
                    }
                    FuzzOperation::RemoveOne { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let value = vec![0xFFu8; value_size];
                        let reference_existed = reference.entry(key).or_default().remove(&value_size);
                        if reference.entry(key).or_default().is_empty() {
                            reference.remove(&key);
                        }
                        let existed = table.remove(&key, &value).unwrap();
                        assert_eq!(reference_existed, existed);
                    }
                    FuzzOperation::Len {} => {
                        let mut reference_len = 0;
                        for v in reference.values() {
                            reference_len += v.len();
                        }
                        assert_eq!(reference_len, table.len().unwrap());
                    }
                    FuzzOperation::Range { start_key, len, reversed } => {
                        let start = start_key.value;
                        let end = start + len.value;
                        let mut reference_iter: Box<dyn Iterator<Item=(&u64, &BTreeSet<usize>)>> = if *reversed {
                            Box::new(reference.range(start..end).rev())
                        } else {
                            Box::new(reference.range(start..end))
                        };
                        let mut iter = if *reversed {
                            table.range(&start..&end).unwrap().rev()
                        } else {
                            table.range(&start..&end).unwrap()
                        };
                        while let Some((ref_key, ref_values)) = reference_iter.next() {
                            if *reversed {
                                for ref_value_len in ref_values.iter().rev() {
                                    let (key, value) = iter.next().unwrap();
                                    assert_eq!(*ref_key, key);
                                    assert_eq!(*ref_value_len, value.len());
                                }
                            } else {
                                for ref_value_len in ref_values.iter() {
                                    let (key, value) = iter.next().unwrap();
                                    assert_eq!(*ref_key, key);
                                    assert_eq!(*ref_value_len, value.len());
                                }
                            }
                        }
                        assert!(iter.next().is_none());
                    },
                }
            }
        }
        if transaction.commit {
            txn.commit()?;
        } else {
            txn.abort().unwrap();
            reference = reference_backup;
        }
    }

    Ok(())
}

fuzz_target!(|config: FuzzConfig| {
    let redb_file: NamedTempFile = NamedTempFile::new().unwrap();
    let write_strategy = if config.use_checksums {
        WriteStrategy::CommitLatency
    } else {
        WriteStrategy::Throughput
    };
    let db = unsafe { Database::builder().set_write_strategy(write_strategy).create(redb_file.path(), config.max_db_size.value) };

    if matches!(db, Err(Error::OutOfSpace)) {
        return;
    }
    let db = db.unwrap();

    let result = if config.multimap_table {
        exec_multimap_table(db, &config.transactions)
    } else {
        exec_table(db, &config.transactions)
    };

    if let Err(err) = result {
        assert!(matches!(err, Error::OutOfSpace) && config.oom_plausible());
    }
});
