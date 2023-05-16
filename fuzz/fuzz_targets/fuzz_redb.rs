#![no_main]

use libfuzzer_sys::fuzz_target;
use redb::{AccessGuard, Database, Durability, Error, MultimapTableDefinition, MultimapValue, ReadableMultimapTable, ReadableTable, Savepoint, Table, TableDefinition};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::NamedTempFile;

mod common;
use common::*;

const TABLE_DEF: TableDefinition<u64, &[u8]> = TableDefinition::new("fuzz_table");
const MULTIMAP_TABLE_DEF: MultimapTableDefinition<u64, &[u8]> =
    MultimapTableDefinition::new("fuzz_multimap_table");

fn handle_table_op(op: &FuzzOperation, reference: &mut BTreeMap<u64, usize>, table: &mut Table<u64, &[u8]>) -> Result<(), redb::Error> {
    match op {
        FuzzOperation::Get { key } => {
            let key = key.value;
            match reference.get(&key) {
                Some(reference_len) => {
                    let value = table.get(&key)?.unwrap();
                    assert_eq!(value.value().len(), *reference_len);
                }
                None => {
                    assert!(table.get(&key)?.is_none());
                }
            }
        }
        FuzzOperation::Insert { key, value_size } => {
            let key = key.value;
            let value_size = value_size.value as usize;
            let value = vec![0xFF; value_size];
            table.insert(&key, value.as_slice())?;
            reference.insert(key, value_size);
        }
        FuzzOperation::InsertReserve { key, value_size } => {
            let key = key.value;
            let value_size = value_size.value;
            let mut value = table.insert_reserve(&key, value_size as u32)?;
            value.as_mut().fill(0xFF);
            reference.insert(key, value_size);
        }
        FuzzOperation::Remove { key } | FuzzOperation::RemoveOne { key, .. } => {
            let key = key.value;
            match reference.remove(&key) {
                Some(reference_len) => {
                    let value = table.remove(&key)?;
                    assert_eq!(value.unwrap().value().len(), reference_len);
                }
                None => {
                    assert!(table.remove(&key)?.is_none());
                }
            }
        }
        FuzzOperation::Len {} => {
            assert_eq!(reference.len() as u64, table.len()?);
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
                    Box::new(reference.range(start..end).rev())
                } else {
                    Box::new(reference.range(start..end))
                };
            let mut iter: Box<dyn Iterator<Item = Result<(AccessGuard<u64>, AccessGuard<&[u8]>), redb::Error>>> = if *reversed {
                Box::new(table.range(start..end)?.rev())
            } else {
                Box::new(table.range(start..end)?)
            };
            while let Some((ref_key, ref_value_len)) = reference_iter.next() {
                let (key, value) = iter.next().unwrap()?;
                assert_eq!(*ref_key, key.value());
                assert_eq!(*ref_value_len, value.value().len());
            }
            assert!(iter.next().is_none());
        }
    }

    Ok(())
}

fn exec_table_crash_support(config: &FuzzConfig) -> Result<(), redb::Error> {
    let mut redb_file: NamedTempFile = NamedTempFile::new().unwrap();

    let mut db = Database::builder()
        .set_page_size(config.page_size.value)
        .set_cache_size(config.cache_size.value)
        .create(redb_file.path())
        .unwrap();
    db.set_crash_countdown(config.crash_after_ops.value);

    let mut savepoints = vec![];
    let mut reference_savepoints = vec![];
    let mut reference = BTreeMap::new();
    let mut non_durable_reference = reference.clone();

    for transaction in config.thread0_transactions.iter() {
        let result = apply_crashable_transaction(&db, &mut non_durable_reference, transaction, &mut savepoints, &mut reference_savepoints);
        if result.is_err() {
            if matches!(result, Err(Error::SimulatedIOFailure)) {
                drop(db);
                savepoints.clear();
                reference_savepoints.clear();
                non_durable_reference = reference.clone();

                // Check that recovery flag is set
                redb_file.seek(SeekFrom::Start(9)).unwrap();
                let mut god_byte = vec![0u8];
                assert_eq!(redb_file.read(&mut god_byte).unwrap(), 1);
                assert_ne!(god_byte[0] & 2, 0);

                // Repair the database
                db = Database::builder()
                    .set_page_size(config.page_size.value)
                    .set_cache_size(config.cache_size.value)
                    .create(redb_file.path())
                    .unwrap();
            } else {
                return result;
            }
        } else if transaction.durable && transaction.commit {
            reference = non_durable_reference.clone();
        }
    }

    Ok(())
}

fn apply_crashable_transaction(db: &Database, reference: &mut BTreeMap<u64, usize>, transaction: &FuzzTransaction, savepoints: &mut Vec<Savepoint>, reference_savepoints: &mut Vec<BTreeMap<u64, usize>>) -> Result<(), redb::Error> {
    let mut txn = db.begin_write().unwrap();
    let mut local_reference = reference.clone();

    if transaction.create_savepoint {
        savepoints.push(txn.ephemeral_savepoint()?);
        reference_savepoints.push(local_reference.clone());
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
            handle_table_op(op, &mut local_reference, &mut table)?;
        }
    }

    if transaction.commit {
        txn.commit()?;
        *reference = local_reference;
    } else {
        txn.abort()?;
    }

    Ok(())
}

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
            savepoints.push(txn.ephemeral_savepoint()?);
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
                handle_table_op(op, &mut local_reference, &mut table)?;
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
    mut iter: MultimapValue<&[u8]>,
    reference: Option<&BTreeSet<usize>>,
) {
    if let Some(values) = reference {
        for value in values.iter() {
            assert_eq!(iter.next().unwrap().unwrap().value().len(), *value);
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
            savepoints.push(txn.ephemeral_savepoint()?);
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
                        table.insert(&key, vec![0xFFu8; value_size].as_slice())?;
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
                        let existed = table.remove(&key, value.as_slice())?;
                        assert_eq!(reference_existed, existed);
                    }
                    FuzzOperation::Len {} => {
                        let mut reference_len = 0;
                        for v in local_reference.values() {
                            reference_len += v.len();
                        }
                        assert_eq!(reference_len as u64, table.len().unwrap());
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
                        let mut iter: Box<dyn Iterator<Item = (AccessGuard<u64>, MultimapValue<&[u8]>)>> = if *reversed {
                            Box::new(table.range(start..end).unwrap().map(|x| x.unwrap()).rev())
                        } else {
                            Box::new(table.range(start..end).unwrap().map(|x| x.unwrap()))
                        };
                        while let Some((ref_key, ref_values)) = reference_iter.next() {
                            let (key, value_iter) = iter.next().unwrap();
                            assert_eq!(*ref_key, key.value());
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
    exec_table_crash_support(&config).unwrap();

    let redb_file: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::builder()
            .set_page_size(config.page_size.value)
            .set_cache_size(config.cache_size.value)
            .create(redb_file.path());

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
