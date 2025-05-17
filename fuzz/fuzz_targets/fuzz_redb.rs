#![no_main]

use libfuzzer_sys::fuzz_target;
use redb::{AccessGuard, Database, Durability, Error, MultimapTable, MultimapTableDefinition, MultimapValue, ReadableMultimapTable, ReadableTable, ReadableTableMetadata, Savepoint, StorageBackend, Table, TableDefinition, WriteTransaction};
use std::collections::{BTreeMap, BTreeSet, HashSet, HashMap};
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::NamedTempFile;

mod common;
use common::*;
use redb::backends::FileBackend;
use crate::FuzzerSavepoint::{Ephemeral, NotYetDurablePersistent, Persistent};

// These slow down the fuzzer, so don't create too many
const MAX_PERSISTENT_SAVEPOINTS: usize = 10;
// Table to count which transactions have been successfully committed so that the reference BtreeMap can be kept in sync
const COUNTER_TABLE: TableDefinition<(), u64> = TableDefinition::new("transaction_counter");
const TABLE_DEF: TableDefinition<u64, &[u8]> = TableDefinition::new("fuzz_table");
const MULTIMAP_TABLE_DEF: MultimapTableDefinition<u64, &[u8]> =
    MultimapTableDefinition::new("fuzz_multimap_table");

#[derive(Debug)]
struct FuzzerBackend {
    inner: FileBackend,
    countdown: Arc<AtomicU64>,
}

impl FuzzerBackend {
    fn new(backend: FileBackend) -> Self {
        Self {
            inner: backend,
            countdown: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    fn check_countdown(&self) -> Result<(), std::io::Error> {
        if self.countdown.load(Ordering::SeqCst) == 0 {
            return Err(std::io::Error::from(ErrorKind::Other));
        }

        Ok(())
    }

    fn decrement_countdown(&self) -> Result<(), std::io::Error> {
        if self.countdown.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| if x > 0 { Some(x - 1) } else { None } ).is_err() {
            return Err(std::io::Error::from(ErrorKind::Other));
        }

        Ok(())
    }
}

impl StorageBackend for FuzzerBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.check_countdown()?;
        self.inner.len()
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
        self.check_countdown()?;
        self.inner.read(offset, len)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.decrement_countdown()?;
        self.inner.set_len(len)
    }

    fn sync_data(&self, _eventual: bool) -> Result<(), std::io::Error> {
        self.decrement_countdown()?;
        // No-op. The fuzzer doesn't test crashes, so fsync is unnecessary
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.decrement_countdown()?;
        self.inner.write(offset, data)
    }
}

enum FuzzerSavepoint<T: Clone> {
    Ephemeral(Savepoint, BTreeMap<u64, T>),
    Persistent(u64, BTreeMap<u64, T>),
    NotYetDurablePersistent(u64, BTreeMap<u64, T>)
}

impl<T: Clone> Debug for FuzzerSavepoint<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzerSavepoint::Ephemeral(_, _) => {
                write!(f, "Ephemeral")
            }
            FuzzerSavepoint::Persistent(x, _) => {
                write!(f, "Persistent({:?})", x)
            }
            FuzzerSavepoint::NotYetDurablePersistent(x, _) => {
                write!(f, "NotYetDurablePersistent({:?})", x)
            }
        }
    }
}

struct SavepointManager<T: Clone> {
    savepoints: Vec<FuzzerSavepoint<T>>,
    uncommitted_persistent: HashSet<u64>,
    // Boolean indicates whether it is durable
    persistent_awaiting_deletion: HashMap<u64, bool>,
    persistent_countdown: usize,
}

impl<T: Clone> SavepointManager<T> {
    fn new() -> Self {
        Self {
            savepoints: vec![],
            uncommitted_persistent: Default::default(),
            persistent_awaiting_deletion: Default::default(),
            persistent_countdown: MAX_PERSISTENT_SAVEPOINTS,
        }
    }

    fn clean_shutdown(&mut self) {
        self.commit(true);
        let persistent: Vec<FuzzerSavepoint<T>> = self.savepoints.drain(..).filter(|x| matches!(x, FuzzerSavepoint::Persistent(_, _))).collect();
        self.savepoints = persistent;
    }

    fn crash(&mut self) {
        let persistent: Vec<FuzzerSavepoint<T>> = self.savepoints.drain(..).filter(|x| matches!(x, FuzzerSavepoint::Persistent(_, _))).collect();
        self.savepoints = persistent;
        let keys: Vec<u64> = self.persistent_awaiting_deletion.keys().cloned().collect();
        for i in keys {
            if !self.persistent_awaiting_deletion[&i] {
                self.persistent_awaiting_deletion.remove(&i);
            }
        }
        self.uncommitted_persistent.clear();
        self.persistent_countdown = MAX_PERSISTENT_SAVEPOINTS;
    }

    fn abort(&mut self) {
        let mut savepoints = vec![];
        for savepoint in self.savepoints.drain(..) {
            match savepoint {
                Ephemeral(x, y) => {
                    savepoints.push(Ephemeral(x, y));
                }
                Persistent(x, y) => {
                    savepoints.push(Persistent(x, y));
                }
                NotYetDurablePersistent(id, y) => {
                    if !self.uncommitted_persistent.contains(&id) {
                        savepoints.push(NotYetDurablePersistent(id, y));
                    }
                }
            }
        }
        self.savepoints = savepoints;
        self.uncommitted_persistent.clear();
    }

    fn gc_persistent_savepoints(&mut self, txn: &WriteTransaction) -> Result<(), Error> {
        let mut savepoints = HashSet::new();
        for savepoint in self.savepoints.iter() {
            match savepoint {
                Ephemeral(_, _) => {}
                Persistent(id, _) |
                NotYetDurablePersistent(id, _) => {
                    savepoints.insert(*id);
                }
            }
        }

        for id in txn.list_persistent_savepoints()? {
            if !savepoints.contains(&id) {
                txn.delete_persistent_savepoint(id)?;
            }
        }

        assert!(txn.list_persistent_savepoints()?.count() <= MAX_SAVEPOINTS);

        Ok(())
    }

    fn finalize_gc_persistent_savepoints(&mut self) {
        self.persistent_awaiting_deletion.clear();
    }

    fn commit(&mut self, durable: bool) {
        if durable {
            let mut savepoints = vec![];
            for savepoint in self.savepoints.drain(..) {
                match savepoint {
                    Ephemeral(x, y) => {
                        savepoints.push(Ephemeral(x, y));
                    }
                    Persistent(x, y) => {
                        savepoints.push(Persistent(x, y));
                    }
                    NotYetDurablePersistent(x, y) => {
                        savepoints.push(Persistent(x, y));
                    }
                }
            }
            self.savepoints = savepoints;
        }
        self.uncommitted_persistent.clear();
    }

    fn restore_savepoint(&mut self, i: usize, txn: &mut WriteTransaction, reference: &mut BTreeMap<u64, T>) -> Result<(), Error> {
        if i >= self.savepoints.len() {
            return Ok(());
        }
        match &self.savepoints[i] {
            FuzzerSavepoint::Ephemeral(savepoint, reference_savepoint) => {
                txn.restore_savepoint(savepoint)?;
                *reference = reference_savepoint.clone();
            }
            FuzzerSavepoint::NotYetDurablePersistent(savepoint_id, reference_savepoint) => {
                let savepoint = txn.get_persistent_savepoint(*savepoint_id)?;
                txn.restore_savepoint(&savepoint)?;
                *reference = reference_savepoint.clone();
            }
            FuzzerSavepoint::Persistent(savepoint_id, reference_savepoint) => {
                let savepoint = txn.get_persistent_savepoint(*savepoint_id)?;
                txn.restore_savepoint(&savepoint)?;
                *reference = reference_savepoint.clone();
            }
        }
        // Invalidate all future savepoints
        self.savepoints.drain((i + 1)..);
        Ok(())
    }

    fn ephemeral_savepoint(&mut self, txn: &WriteTransaction, reference: &BTreeMap<u64, T>) -> Result<(), Error> {
        self.savepoints.push(Ephemeral(txn.ephemeral_savepoint()?, reference.clone()));
        if self.savepoints.len() > MAX_SAVEPOINTS {
            let removed = self.savepoints.remove(0);
            match removed {
                Ephemeral(_, _) => {},
                Persistent(id, _) => {
                    self.persistent_awaiting_deletion.insert(id, true);
                }
                NotYetDurablePersistent(id, _) => {
                    self.persistent_awaiting_deletion.insert(id, false);
                }
            }
        }
        Ok(())
    }

    fn persistent_savepoint(&mut self, txn: &WriteTransaction, reference: &BTreeMap<u64, T>) -> Result<(), Error> {
        if self.persistent_countdown == 0 {
            return self.ephemeral_savepoint(txn, reference);
        } else {
            self.persistent_countdown -= 1;
        }
        let id = txn.persistent_savepoint()?;
        self.savepoints.push(NotYetDurablePersistent(id, reference.clone()));
        self.uncommitted_persistent.insert(id);
        if self.savepoints.len() > MAX_SAVEPOINTS {
            let removed = self.savepoints.remove(0);
            match removed {
                Ephemeral(_, _) => {},
                Persistent(id, _) => {
                    self.persistent_awaiting_deletion.insert(id, true);
                }
                NotYetDurablePersistent(id, _) => {
                    self.persistent_awaiting_deletion.insert(id, false);
                }
            }
        }
        Ok(())
    }
}

fn handle_multimap_table_op(op: &FuzzOperation, reference: &mut BTreeMap<u64, BTreeSet<usize>>, table: &mut MultimapTable<u64, &[u8]>) -> Result<(), redb::Error> {
    match op {
        FuzzOperation::Get { key } => {
            let key = key.value;
            let iter = table.get(&key)?;
            let entry = reference.get(&key);
            assert_multimap_value_eq(iter, entry)?;
        }
        FuzzOperation::Insert { key, value_size } => {
            let key = key.value;
            let value_size = value_size.value as usize;
            table.insert(&key, vec![0xFFu8; value_size].as_slice())?;
            reference.entry(key).or_default().insert(value_size);
        }
        FuzzOperation::InsertReserve { .. } => {
            // no-op. Multimap tables don't support insert_reserve
        }
        FuzzOperation::Remove { key } => {
            let key = key.value;
            let entry = reference.remove(&key);
            let iter = table.remove_all(&key)?;
            assert_multimap_value_eq(iter, entry.as_ref())?;
        }
        FuzzOperation::RemoveOne { key, value_size } => {
            let key = key.value;
            let value_size = value_size.value as usize;
            let value = vec![0xFFu8; value_size];
            let reference_existed =
                reference.entry(key).or_default().remove(&value_size);
            if reference.entry(key).or_default().is_empty() {
                reference.remove(&key);
            }
            let existed = table.remove(&key, value.as_slice())?;
            assert_eq!(reference_existed, existed);
        }
        FuzzOperation::Len {} => {
            let mut reference_len = 0;
            for v in reference.values() {
                reference_len += v.len();
            }
            assert_eq!(reference_len as u64, table.len()?);
        }
        FuzzOperation::PopFirst { .. } => {
            // no-op. Multimap tables don't support this
        }
        FuzzOperation::PopLast { .. } => {
            // no-op. Multimap tables don't support this
        }
        FuzzOperation::ExtractIf { .. } => {
            // no-op. Multimap tables don't support this
        }
        FuzzOperation::ExtractFromIf { .. } => {
            // no-op. Multimap tables don't support this
        }
        FuzzOperation::Retain { .. } => {
            // no-op. Multimap tables don't support this
        }
        FuzzOperation::RetainIn { .. } => {
            // no-op. Multimap tables don't support this
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
                    Box::new(reference.range(start..end).rev())
                } else {
                    Box::new(reference.range(start..end))
                };
            let mut iter: Box<dyn Iterator<Item = Result<(AccessGuard<u64>, MultimapValue<&[u8]>), redb::StorageError>>> = if *reversed {
                Box::new(table.range(start..end)?.rev())
            } else {
                Box::new(table.range(start..end)?)
            };
            while let Some((ref_key, ref_values)) = reference_iter.next() {
                let (key, value_iter) = iter.next().unwrap()?;
                assert_eq!(*ref_key, key.value());
                assert_multimap_value_eq(value_iter, Some(ref_values))?;
            }
            // This is basically assert!(iter.next().is_none()), but we also allow an Err such as a simulated IO error
            if let Some(Ok((_, _)))  = iter.next() {
                panic!();
            }
        }
    }

    Ok(())
}

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
        FuzzOperation::PopFirst { .. } => {
            if let Some((key, _value)) = reference.first_key_value() {
                let key = *key;
                let value = reference.remove(&key).unwrap();
                let removed = table.pop_first()?.unwrap();
                assert_eq!(removed.0.value(), key);
                assert_eq!(removed.1.value().len(), value);
            } else {
                assert!(table.pop_first()?.is_none());
            }
        }
        FuzzOperation::PopLast { .. } => {
            if let Some((key, _value)) = reference.last_key_value() {
                let key = *key;
                let value = reference.remove(&key).unwrap();
                let removed = table.pop_last()?.unwrap();
                assert_eq!(removed.0.value(), key);
                assert_eq!(removed.1.value().len(), value);
            } else {
                assert!(table.pop_first()?.is_none());
            }
        }
        FuzzOperation::ExtractIf { take, modulus, reversed } => {
            let modulus = modulus.value;
            let mut reference_iter: Box<dyn Iterator<Item = (&u64, &usize)>> =
                if *reversed {
                    Box::new(reference.iter().rev().take(take.value))
                } else {
                    Box::new(reference.iter().take(take.value))
                };
            let mut iter: Box<dyn Iterator<Item = Result<(AccessGuard<u64>, AccessGuard<&[u8]>), redb::StorageError>>> = if *reversed {
                Box::new(table.extract_if(|x, _| x % modulus == 0)?.rev())
            } else {
                Box::new(table.extract_if(|x, _| x % modulus == 0)?)
            };
            let mut remaining = take.value;
            let mut remove_from_reference = vec![];
            while let Some((ref_key, ref_value_len)) = reference_iter.next() {
                if *ref_key % modulus != 0 {
                    continue;
                }
                if remaining == 0 {
                    break;
                }
                remaining -= 1;
                let (key, value) = iter.next().unwrap()?;
                remove_from_reference.push(*ref_key);
                assert_eq!(*ref_key, key.value());
                assert_eq!(*ref_value_len, value.value().len());
            }
            drop(reference_iter);
            for x in remove_from_reference {
                reference.remove(&x);
            }
        }
        FuzzOperation::ExtractFromIf { start_key, range_len, take, modulus, reversed } => {
            let start = start_key.value;
            let end = start + range_len.value;
            let modulus = modulus.value;
            let mut reference_iter: Box<dyn Iterator<Item = (&u64, &usize)>> =
                if *reversed {
                    Box::new(reference.range(start..end).rev().take(take.value))
                } else {
                    Box::new(reference.range(start..end).take(take.value))
                };
            let mut iter: Box<dyn Iterator<Item = Result<(AccessGuard<u64>, AccessGuard<&[u8]>), redb::StorageError>>> = if *reversed {
                Box::new(table.extract_from_if(start..end, |x, _| x % modulus == 0)?.rev())
            } else {
                Box::new(table.extract_from_if(start..end, |x, _| x % modulus == 0)?)
            };
            let mut remaining = take.value;
            let mut remove_from_reference = vec![];
            while let Some((ref_key, ref_value_len)) = reference_iter.next() {
                if *ref_key % modulus != 0 {
                    continue;
                }
                if remaining == 0 {
                    break;
                }
                remaining -= 1;
                let (key, value) = iter.next().unwrap()?;
                remove_from_reference.push(*ref_key);
                assert_eq!(*ref_key, key.value());
                assert_eq!(*ref_value_len, value.value().len());
            }
            drop(reference_iter);
            for x in remove_from_reference {
                reference.remove(&x);
            }
        }
        FuzzOperation::RetainIn { start_key, len, modulus } => {
            let start = start_key.value;
            let end = start + len.value;
            let modulus = modulus.value;
            table.retain_in(start..end, |x, _| x % modulus == 0)?;
            reference.retain(|x, _| (*x < start || *x >= end) || *x % modulus == 0);
        }
        FuzzOperation::Retain { modulus } => {
            let modulus = modulus.value;
            table.retain(|x, _| x % modulus == 0)?;
            reference.retain(|x, _| *x % modulus == 0);
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
            let mut iter: Box<dyn Iterator<Item = Result<(AccessGuard<u64>, AccessGuard<&[u8]>), redb::StorageError>>> = if *reversed {
                Box::new(table.range(start..end)?.rev())
            } else {
                Box::new(table.range(start..end)?)
            };
            while let Some((ref_key, ref_value_len)) = reference_iter.next() {
                let (key, value) = iter.next().unwrap()?;
                assert_eq!(*ref_key, key.value());
                assert_eq!(*ref_value_len, value.value().len());
            }
            // This is basically assert!(iter.next().is_none()), but we also allow an Err such as a simulated IO error
            if let Some(Ok((_, _)))  = iter.next() {
                panic!();
            }
        }
    }

    Ok(())
}

fn is_simulated_io_error(err: &redb::Error) -> bool {
    match err {
        Error::Io(io_err) => {
            matches!(io_err.kind(), ErrorKind::Other)
        },
        _ => false
    }
}

// Open a separate file descriptor to the same file
// We need a separate file descriptor to make sure it has its own locks
fn open_dup(file: &NamedTempFile) -> File {
    OpenOptions::new().read(true).write(true).open(file.path()).unwrap()
}

fn exec_table_crash_support<T: Clone + Debug>(config: &FuzzConfig, apply: fn(WriteTransaction, &mut BTreeMap<u64, T>, &FuzzTransaction, &mut SavepointManager<T>) -> Result<(), redb::Error>) -> Result<(), redb::Error> {
    let mut redb_file: NamedTempFile = NamedTempFile::new().unwrap();
    let backend = FuzzerBackend::new(FileBackend::new(open_dup(&redb_file))?);
    let mut countdown = backend.countdown.clone();

    let mut db = Database::builder()
        .set_page_size(config.page_size.value)
        .set_cache_size(config.cache_size.value)
        .set_region_size(config.region_size.value as u64)
        .create_with_backend(backend).unwrap();

    // Disable IO error simulation while we get a baseline number of allocated pages
    let old_countdown = countdown.swap(u64::MAX, Ordering::SeqCst);
    let txn = db.begin_write().unwrap();
    // Touch the savepoints tables to be sure they get created, so that they occupy pages
    let id = txn.persistent_savepoint().unwrap();
    txn.delete_persistent_savepoint(id).unwrap();
    #[allow(unused_must_use)]
    {
        txn.list_persistent_savepoints().unwrap();
    }
    txn.commit().unwrap();
    db.begin_write().unwrap().commit().unwrap();
    let txn = db.begin_write().unwrap();
    let baseline_allocated_pages = txn.stats().unwrap().allocated_pages();
    txn.abort().unwrap();
    countdown.store(old_countdown, Ordering::SeqCst);

    let txn = db.begin_write().unwrap();
    let mut table = txn.open_table(COUNTER_TABLE).unwrap();
    table.insert((), 0)?;
    drop(table);
    txn.commit().unwrap();

    countdown.store(config.crash_after_ops.value, Ordering::SeqCst);

    let mut savepoint_manager = SavepointManager::new();
    let mut reference = BTreeMap::new();
    let mut non_durable_reference = reference.clone();
    let mut has_done_close_db = false;

    for (txn_id, transaction) in config.transactions.iter().enumerate() {
        let result = handle_savepoints(db.begin_write().unwrap(), &mut non_durable_reference, transaction, &mut savepoint_manager, countdown.clone());
        match result {
            Ok(durable) => {
                if durable {
                    reference = non_durable_reference.clone();
                }
            }
            Err(err) => {
                if is_simulated_io_error(&err) {
                    drop(db);
                    savepoint_manager.crash();
                    non_durable_reference = reference.clone();

                    // Check that recovery flag is set
                    redb_file.seek(SeekFrom::Start(9)).unwrap();
                    let mut god_byte = vec![0u8];
                    assert_eq!(redb_file.read(&mut god_byte).unwrap(), 1);
                    assert_ne!(god_byte[0] & 2, 0);

                    // Repair the database
                    let backend = FuzzerBackend::new(FileBackend::new(open_dup(&redb_file)).unwrap());
                    db = Database::builder()
                        .set_page_size(config.page_size.value)
                        .set_cache_size(config.cache_size.value)
                        .set_region_size(config.region_size.value as u64)
                        .create_with_backend(backend)
                        .unwrap();
                } else {
                    return Err(err);
                }
            }
        }

        // Disable IO error simulation while we update the transaction counter table
        let old_countdown = countdown.swap(u64::MAX, Ordering::SeqCst);
        let mut txn = db.begin_write().unwrap();
        if !transaction.durable {
            txn.set_durability(Durability::None);
        }
        txn.set_quick_repair(transaction.quick_repair);
        let mut counter_table = txn.open_table(COUNTER_TABLE).unwrap();
        let uncommitted_id = txn_id as u64 + 1;
        counter_table.insert((), uncommitted_id)?;
        drop(counter_table);
        countdown.store(old_countdown, Ordering::SeqCst);

        let mut uncommitted_reference = non_durable_reference.clone();

        let result = apply(txn, &mut uncommitted_reference, transaction, &mut savepoint_manager);
        if result.is_err() {
            if is_simulated_io_error(result.as_ref().err().unwrap()) {
                drop(db);
                savepoint_manager.crash();
                non_durable_reference = reference.clone();

                // Check that recovery flag is set
                redb_file.seek(SeekFrom::Start(9)).unwrap();
                let mut god_byte = vec![0u8];
                assert_eq!(redb_file.read(&mut god_byte).unwrap(), 1);
                assert_ne!(god_byte[0] & 2, 0);

                // Repair the database
                let backend = FuzzerBackend::new(FileBackend::new(open_dup(&redb_file)).unwrap());
                db = Database::builder()
                    .set_page_size(config.page_size.value)
                    .set_cache_size(config.cache_size.value)
                    .set_region_size(config.region_size.value as u64)
                    .create_with_backend(backend)
                    .unwrap();
            } else {
                return result;
            }
        }

        // Disable IO error simulation
        let old_countdown = countdown.swap(u64::MAX, Ordering::SeqCst);
        let txn = db.begin_read().unwrap();
        let counter_table = txn.open_table(COUNTER_TABLE).unwrap();
        let last_committed = counter_table.get(()).unwrap().unwrap().value();
        // Need to make sure this transaction is completed. Otherwise, it could conflict with closing
        // and re-opening the database below
        drop(counter_table);
        txn.close().unwrap();
        countdown.store(old_countdown, Ordering::SeqCst);

        let commit_succeeded = last_committed == uncommitted_id;
        if commit_succeeded {
            assert!(transaction.commit);
            savepoint_manager.commit(transaction.durable);
            non_durable_reference = uncommitted_reference;
            if transaction.durable {
                reference = non_durable_reference.clone();
                savepoint_manager.finalize_gc_persistent_savepoints();
            }
        } else {
            savepoint_manager.abort();
        }

        if transaction.close_db && !has_done_close_db {
            has_done_close_db = true;
            let old_countdown = countdown.swap(u64::MAX, Ordering::SeqCst);
            drop(db);
            savepoint_manager.commit(true);
            reference = non_durable_reference.clone();
            savepoint_manager.clean_shutdown();

            let backend = FuzzerBackend::new(FileBackend::new(open_dup(&redb_file))?);
            countdown = backend.countdown.clone();
            db = Database::builder()
                .set_page_size(config.page_size.value)
                .set_cache_size(config.cache_size.value)
                .set_region_size(config.region_size.value as u64)
                .create_with_backend(backend).unwrap();

            countdown.store(old_countdown, Ordering::SeqCst);
        }
    }

    match run_compaction(&mut db, &mut savepoint_manager) {
        Ok(_) => {}
        Err(err) => {
            if !is_simulated_io_error(&err) {
                return Err(err);
            }
        }
    }

    // Repair the database, if needed, and disable IO error simulation
    countdown.swap(u64::MAX, Ordering::SeqCst);
    drop(db);
    let backend = FuzzerBackend::new(FileBackend::new(open_dup(&redb_file)).unwrap());
    db = Database::builder()
        .set_page_size(config.page_size.value)
        .set_cache_size(config.cache_size.value)
        .set_region_size(config.region_size.value as u64)
        .create_with_backend(backend)
        .unwrap();

    // Check for leaked pages
    let read_txn = db.begin_read().unwrap();
    let txn = db.begin_write().unwrap();
    for table in read_txn.list_tables().unwrap() {
        assert!(txn.delete_table(table).unwrap());
    }
    for table in read_txn.list_multimap_tables().unwrap() {
        assert!(txn.delete_multimap_table(table).unwrap());
    }
    savepoint_manager.savepoints.clear();
    for id in txn.list_persistent_savepoints().unwrap() {
        txn.delete_persistent_savepoint(id).unwrap();
    }
    drop(read_txn);
    txn.commit().unwrap();

    // Clear out the freed table
    let mut allocated_pages = db.begin_write().unwrap().stats().unwrap().allocated_pages();
    loop {
        db.begin_write().unwrap().commit().unwrap();
        let new_allocated_pages = db.begin_write().unwrap().stats().unwrap().allocated_pages();
        if new_allocated_pages == allocated_pages {
            break;
        } else {
            allocated_pages = new_allocated_pages;
        }
    }

    let txn = db.begin_write().unwrap();
    let allocated_pages = txn.stats().unwrap().allocated_pages();
    txn.abort().unwrap();
    assert_eq!(allocated_pages, baseline_allocated_pages, "Found {} allocated pages at shutdown, expected {}", allocated_pages, baseline_allocated_pages);

    assert!(db.check_integrity().unwrap());

    Ok(())
}

fn run_compaction<T: Clone>(db: &mut Database, savepoint_manager: &mut SavepointManager<T>) -> Result<(), Error> {
    savepoint_manager.savepoints.clear();
    let txn = db.begin_write()?;
    let ids: Vec<u64> = txn.list_persistent_savepoints()?.collect();
    for id in ids {
        txn.delete_persistent_savepoint(id)?;
    }
    txn.commit()?;
    db.compact()?;

    Ok(())
}

// Returns true if a durable commit was made
fn handle_savepoints<T: Clone>(mut txn: WriteTransaction, reference: &mut BTreeMap<u64, T>, transaction: &FuzzTransaction, savepoints: &mut SavepointManager<T>, countdown: Arc<AtomicU64>) -> Result<bool, redb::Error> {
    if transaction.create_ephemeral_savepoint {
        savepoints.ephemeral_savepoint(&txn, &reference)?;
    }

    if transaction.create_persistent_savepoint || transaction.restore_savepoint.is_some() {
        if transaction.create_persistent_savepoint {
            savepoints.persistent_savepoint(&mut txn, reference)?;
        }
        if let Some(ref restore_to) = transaction.restore_savepoint {
            savepoints.restore_savepoint(restore_to.value, &mut txn, reference)?;
        }
        // Disable simulated IO failures. It's tricky to handle commit failures here in the fuzzer,
        // and it doesn't add value since we already fuzz failures on the main transaction path
        let old_countdown = countdown.swap(u64::MAX, Ordering::SeqCst);
        txn.commit()?;
        savepoints.commit(true);
        countdown.store(old_countdown, Ordering::SeqCst);
        Ok(true)
    } else {
        // Disable simulated IO failures. It's tricky to handle commit failures here in the fuzzer,
        // and it doesn't add value since we already fuzz failures on the main transaction path
        let old_countdown = countdown.swap(u64::MAX, Ordering::SeqCst);
        txn.abort()?;
        savepoints.abort();
        countdown.store(old_countdown, Ordering::SeqCst);
        Ok(false)
    }

}

fn apply_crashable_transaction_multimap(txn: WriteTransaction, uncommitted_reference: &mut BTreeMap<u64, BTreeSet<usize>>, transaction: &FuzzTransaction, savepoints: &mut SavepointManager<BTreeSet<usize>>) -> Result<(), redb::Error> {
    {
        let mut table = txn.open_multimap_table(MULTIMAP_TABLE_DEF)?;
        for op in transaction.ops.iter() {
            handle_multimap_table_op(op, uncommitted_reference, &mut table)?;
        }
    }

    if transaction.commit {
        if transaction.durable {
            savepoints.gc_persistent_savepoints(&txn)?;
        }
        txn.commit()?;
    } else {
        txn.abort()?;
    }

    Ok(())
}

fn apply_crashable_transaction(txn: WriteTransaction, uncommitted_reference: &mut BTreeMap<u64, usize>, transaction: &FuzzTransaction, savepoints: &mut SavepointManager<usize>) -> Result<(), redb::Error> {
    {
        let mut table = txn.open_table(TABLE_DEF)?;
        for op in transaction.ops.iter() {
            handle_table_op(op, uncommitted_reference, &mut table)?;
        }
    }

    if transaction.commit {
        if transaction.durable {
            savepoints.gc_persistent_savepoints(&txn)?;
        }
        txn.commit()?;
    } else {
        txn.abort()?;
    }

    Ok(())
}

fn assert_multimap_value_eq(
    mut iter: MultimapValue<&[u8]>,
    reference: Option<&BTreeSet<usize>>,
) -> Result<(), redb::Error> {
    if let Some(values) = reference {
        assert_eq!(values.len() as u64, iter.len());
        for value in values.iter() {
            assert_eq!(iter.next().unwrap()?.value().len(), *value);
        }
    }
    assert!(iter.is_empty());
    // This is basically assert!(iter.next().is_none()), but we also allow an Err such as a simulated IO error
    if let Some(Ok(_))  = iter.next() {
        panic!();
    }

    Ok(())
}

fuzz_target!(|config: FuzzConfig| {
    if config.multimap_table {
        exec_table_crash_support(&config, apply_crashable_transaction_multimap).unwrap();
    } else {
        exec_table_crash_support(&config, apply_crashable_transaction).unwrap();
    }
});
