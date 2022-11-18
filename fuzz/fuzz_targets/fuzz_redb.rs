#![no_main]

use libfuzzer_sys::fuzz_target;
use redb::{
    Database, Durability, Error, MultimapTableDefinition, MultimapValueIter, ReadableMultimapTable,
    ReadableTable, TableDefinition, WriteStrategy,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::NamedTempFile;

mod common;
use common::*;

const TABLE_DEF: TableDefinition<u64, [u8]> = TableDefinition::new("fuzz_table");
const MULTIMAP_TABLE_DEF: MultimapTableDefinition<u64, [u8]> =
    MultimapTableDefinition::new("fuzz_multimap_table");

fn exec_table(db: Arc<Database>, transactions: &[FuzzTransaction], reference: Arc<Mutex<BTreeMap<u64, usize>>>, barrier: Arc<CustomBarrier>) {
    exec_table_inner(db, &transactions, reference, barrier.clone()).unwrap();
    barrier.decrement_waiters();
}

fn exec_table_inner(db: Arc<Database>, transactions: &[FuzzTransaction], reference: Arc<Mutex<BTreeMap<u64, usize>>>, barrier: Arc<CustomBarrier>) -> Result<(), redb::Error> {
    let mut savepoints = vec![];
    let mut reference_savepoints = vec![];

    for transaction in transactions.iter() {
        barrier.wait();

        let mut txn = db.begin_write().unwrap();
        let mut local_reference = reference.lock().unwrap().clone();

        if transaction.create_savepoint {
            savepoints.push(txn.savepoint()?);
            let guard = reference.lock().unwrap();
            reference_savepoints.push(guard.clone());
            drop(guard);
            if savepoints.len() > MAX_SAVEPOINTS {
                savepoints.remove(0);
                reference_savepoints.remove(0);
            }
        }
        let restore_to = transaction.restore_savepoint.value;
        if restore_to > 0 && restore_to <= savepoints.len() {
            let index = savepoints.len() - restore_to;
            let result = txn.restore_savepoint(&savepoints[index]);
            if result.is_ok() {
                local_reference = reference_savepoints[index].clone();
            }
            if result.is_err() && !matches!(result, Err(Error::InvalidSavepoint)) {
                return result;
            }
        }

        // We're not trying to test crash safety, so don't bother with durability
        if !transaction.durable {
            txn.set_durability(Durability::None);
        }
        {
            let mut table = txn.open_table(TABLE_DEF)?;
            for op in transaction.ops.iter() {
                match op {
                    FuzzOperation::Get { key } => {
                        let key = key.value;
                        match local_reference.get(&key) {
                            Some(reference_len) => {
                                let value = table.get(&key).unwrap().unwrap();
                                assert_eq!(value.len(), *reference_len);
                            }
                            None => {
                                assert!(table.get(&key).unwrap().is_none());
                            }
                        }
                    }
                    FuzzOperation::Insert { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let value = vec![0xFF; value_size];
                        table.insert(&key, &value)?;
                        local_reference.insert(key, value_size);
                    }
                    FuzzOperation::InsertReserve { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let mut value = table.insert_reserve(&key, value_size)?;
                        value.as_mut().fill(0xFF);
                        local_reference.insert(key, value_size);
                    }
                    FuzzOperation::Remove { key } | FuzzOperation::RemoveOne { key, .. } => {
                        let key = key.value;
                        match local_reference.remove(&key) {
                            Some(reference_len) => {
                                let value = table.remove(&key)?;
                                assert_eq!(value.unwrap().to_value().len(), reference_len);
                            }
                            None => {
                                assert!(table.remove(&key).unwrap().is_none());
                            }
                        }
                    }
                    FuzzOperation::Len {} => {
                        assert_eq!(local_reference.len(), table.len().unwrap());
                    }
                    FuzzOperation::Range {
                        start_key,
                        len,
                        reversed,
                    } => {
                        let start = start_key.value;
                        let end = start + len.value;
                        let mut reference_iter: Box<dyn Iterator<Item = (&u64, &usize)>> =
                            if *reversed {
                                Box::new(local_reference.range(start..end).rev())
                            } else {
                                Box::new(local_reference.range(start..end))
                            };
                        let mut iter: Box<dyn Iterator<Item = (u64, &[u8])>> = if *reversed {
                            Box::new(table.range(start..end).unwrap().rev())
                        } else {
                            Box::new(table.range(start..end).unwrap())
                        };
                        while let Some((ref_key, ref_value_len)) = reference_iter.next() {
                            let (key, value) = iter.next().unwrap();
                            assert_eq!(*ref_key, key);
                            assert_eq!(*ref_value_len, value.len());
                        }
                        assert!(iter.next().is_none());
                    }
                }
            }
        }
        if transaction.commit {
            let mut guard = reference.lock().unwrap();
            txn.commit()?;
            *guard = local_reference;
        } else {
            txn.abort().unwrap();
        }
    }

    Ok(())
}

fn assert_multimap_value_eq(
    mut iter: MultimapValueIter<[u8]>,
    reference: Option<&BTreeSet<usize>>,
) {
    if let Some(values) = reference {
        for value in values.iter() {
            assert_eq!(iter.next().unwrap().len(), *value);
        }
    }
    assert!(iter.next().is_none());
}

fn exec_multimap_table(db: Arc<Database>, transactions: &[FuzzTransaction], reference: Arc<Mutex<BTreeMap<u64, BTreeSet<usize>>>>, barrier: Arc<CustomBarrier>) {
    exec_multimap_table_inner(db, &transactions, reference, barrier.clone()).unwrap();
    barrier.decrement_waiters();
}

fn exec_multimap_table_inner(db: Arc<Database>, transactions: &[FuzzTransaction], reference: Arc<Mutex<BTreeMap<u64, BTreeSet<usize>>>>, barrier: Arc<CustomBarrier>) -> Result<(), redb::Error> {
    let mut savepoints = vec![];
    let mut reference_savepoints = vec![];

    for transaction in transactions.iter() {
        barrier.wait();

        let mut txn = db.begin_write().unwrap();
        let mut local_reference = reference.lock().unwrap().clone();

        if transaction.create_savepoint {
            savepoints.push(txn.savepoint()?);
            let guard = reference.lock().unwrap();
            reference_savepoints.push(guard.clone());
            drop(guard);
            if savepoints.len() > MAX_SAVEPOINTS {
                savepoints.remove(0);
                reference_savepoints.remove(0);
            }
        }
        let restore_to = transaction.restore_savepoint.value;
        if restore_to > 0 && restore_to <= savepoints.len() {
            let index = savepoints.len() - restore_to;
            let result = txn.restore_savepoint(&savepoints[index]);
            if result.is_ok() {
                local_reference = reference_savepoints[index].clone();
            }
            if result.is_err() && !matches!(result, Err(Error::InvalidSavepoint)) {
                return result;
            }
        }

        // We're not trying to test crash safety, so don't bother with durability
        if !transaction.durable {
            txn.set_durability(Durability::None);
        }
        {
            let mut table = txn.open_multimap_table(MULTIMAP_TABLE_DEF)?;
            for op in transaction.ops.iter() {
                match op {
                    FuzzOperation::Get { key } => {
                        let key = key.value;
                        let iter = table.get(&key).unwrap();
                        let entry = local_reference.get(&key);
                        assert_multimap_value_eq(iter, entry);
                    }
                    FuzzOperation::Insert { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let value = vec![0xFFu8; value_size];
                        table.insert(&key, &value)?;
                        local_reference.entry(key).or_default().insert(value_size);
                    }
                    FuzzOperation::InsertReserve { .. } => {
                        // no-op. Multimap tables don't support insert_reserve
                    }
                    FuzzOperation::Remove { key } => {
                        let key = key.value;
                        let entry = local_reference.remove(&key);
                        let iter = table.remove_all(&key)?;
                        assert_multimap_value_eq(iter, entry.as_ref());
                    }
                    FuzzOperation::RemoveOne { key, value_size } => {
                        let key = key.value;
                        let value_size = value_size.value as usize;
                        let value = vec![0xFFu8; value_size];
                        let reference_existed =
                            local_reference.entry(key).or_default().remove(&value_size);
                        if local_reference.entry(key).or_default().is_empty() {
                            local_reference.remove(&key);
                        }
                        let existed = table.remove(&key, &value)?;
                        assert_eq!(reference_existed, existed);
                    }
                    FuzzOperation::Len {} => {
                        let mut reference_len = 0;
                        for v in local_reference.values() {
                            reference_len += v.len();
                        }
                        assert_eq!(reference_len, table.len().unwrap());
                    }
                    FuzzOperation::Range {
                        start_key,
                        len,
                        reversed,
                    } => {
                        let start = start_key.value;
                        let end = start + len.value;
                        let mut reference_iter: Box<dyn Iterator<Item = (&u64, &BTreeSet<usize>)>> =
                            if *reversed {
                                Box::new(local_reference.range(start..end).rev())
                            } else {
                                Box::new(local_reference.range(start..end))
                            };
                        let mut iter: Box<dyn Iterator<Item = (u64, MultimapValueIter<[u8]>)>> = if *reversed {
                            Box::new(table.range(&start..&end).unwrap().rev())
                        } else {
                            Box::new(table.range(&start..&end).unwrap())
                        };
                        while let Some((ref_key, ref_values)) = reference_iter.next() {
                            let (key, value_iter) = iter.next().unwrap();
                            assert_eq!(*ref_key, key);
                            assert_multimap_value_eq(value_iter, Some(ref_values));
                        }
                        assert!(iter.next().is_none());
                    }
                }
            }
        }
        if transaction.commit {
            let mut guard = reference.lock().unwrap();
            txn.commit()?;
            *guard = local_reference;
        } else {
            txn.abort().unwrap();
        }
    }

    Ok(())
}

fuzz_target!(|config: FuzzConfig| {
    let redb_file: NamedTempFile = NamedTempFile::new().unwrap();
    let write_strategy = if config.use_checksums {
        WriteStrategy::Checksum
    } else {
        WriteStrategy::TwoPhase
    };
    let db = unsafe {
        Database::builder()
            .set_write_strategy(write_strategy)
            .create(redb_file.path())
    };

    let db = Arc::new(db.unwrap());

    let barrier = Arc::new(CustomBarrier::new(2));
    if config.multimap_table {
        let reference = Arc::new(Mutex::new(Default::default()));
        let reference2 = reference.clone();
        let barrier2 = barrier.clone();
        let db2 = db.clone();
        let transactions = config.thread0_transactions.clone();
        let t0 = thread::spawn(move || {
            exec_multimap_table(db, &transactions, reference, barrier);
        });
        let transactions = config.thread1_transactions.clone();
        let t1 = thread::spawn(move || {
            exec_multimap_table(db2, &transactions, reference2, barrier2);
        });
        assert!(t0.join().is_ok());
        assert!(t1.join().is_ok());
    } else {
        let reference = Arc::new(Mutex::new(Default::default()));
        let reference2 = reference.clone();
        let barrier2 = barrier.clone();
        let db2 = db.clone();
        let transactions = config.thread0_transactions.clone();
        let t0 = thread::spawn(move || {
            exec_table(db, &transactions, reference, barrier);
        });
        let transactions = config.thread1_transactions.clone();
        let t1 = thread::spawn(move || {
            exec_table(db2, &transactions, reference2, barrier2);
        });
        assert!(t0.join().is_ok());
        assert!(t1.join().is_ok());
    };

});
