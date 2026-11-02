#![no_main]
//! Corrupts a committed database and checks that opening it with
//! `set_check_integrity_on_open(true)`, then `check_integrity()`, restoring each persistent
//! savepoint, and reading and writing after a successful repair, return `Ok` or `Err` and never
//! panic.
//!
//! The pristine database is built once per process and holds every structure a repair walks: a
//! fixed-width table and a variable-width table, both deep enough for branch pages, values that
//! span several pages, a multimap with inline and subtree collections, persistent savepoints that
//! keep freed pages allocated, and deletes after them. It is closed cleanly, so a plain open would
//! trust its recorded allocator state; the repair on open must not. The fuzzer input chooses the
//! mutations applied to a copy of its bytes.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use redb::{
    Database, MultimapTableDefinition, MultimapTableHandle, ReadableDatabase,
    ReadableMultimapTable, ReadableTable, StorageBackend, TableDefinition, TableHandle,
};
use std::convert::TryFrom;
use std::hint::black_box;
use std::io;
use std::sync::{Arc, OnceLock, RwLock};

const FIXED: TableDefinition<u64, u64> = TableDefinition::new("fixed");
const VARIABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("variable");
const MULTIMAP: MultimapTableDefinition<u32, &str> = MultimapTableDefinition::new("multimap");

const PAGE_SIZE: usize = 4096;
const CACHE_SIZE: usize = 8 * 1024 * 1024;
const MAX_MUTATIONS: usize = 8;

static PRISTINE: OnceLock<Vec<u8>> = OnceLock::new();

/// The database file as a byte vector that the harness keeps a handle to.
#[derive(Debug)]
struct MemoryBackend(Arc<RwLock<Vec<u8>>>);

impl MemoryBackend {
    fn out_of_range() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, "offset out of range")
    }
}

impl StorageBackend for MemoryBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.0.read().unwrap().len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let data = self.0.read().unwrap();
        let start = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        let end = start
            .checked_add(out.len())
            .ok_or_else(Self::out_of_range)?;
        let source = data.get(start..end).ok_or_else(Self::out_of_range)?;
        out.copy_from_slice(source);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        let len = usize::try_from(len).map_err(|_| Self::out_of_range())?;
        self.0.write().unwrap().resize(len, 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut storage = self.0.write().unwrap();
        let start = usize::try_from(offset).map_err(|_| Self::out_of_range())?;
        let end = start
            .checked_add(data.len())
            .ok_or_else(Self::out_of_range)?;
        let target = storage.get_mut(start..end).ok_or_else(Self::out_of_range)?;
        target.copy_from_slice(data);
        Ok(())
    }
}

fn build_pristine() -> Vec<u8> {
    let storage = Arc::new(RwLock::new(Vec::new()));
    {
        let db = Database::builder()
            .set_cache_size(CACHE_SIZE)
            .create_with_backend(MemoryBackend(storage.clone()))
            .expect("create the pristine database");

        // Enough entries for branch pages in every table, and values spanning several pages
        let txn = db.begin_write().unwrap();
        {
            let mut fixed = txn.open_table(FIXED).unwrap();
            for i in 0..4000u64 {
                fixed
                    .insert(i, i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
                    .unwrap();
            }
            let mut variable = txn.open_table(VARIABLE).unwrap();
            for i in 0..600u32 {
                let key = format!("key-{:05}", i);
                let value = vec![(i % 251) as u8; (i as usize % 97) + 1];
                variable.insert(key.as_str(), value.as_slice()).unwrap();
            }
            variable
                .insert("large-1", vec![1u8; 3 * PAGE_SIZE].as_slice())
                .unwrap();
            variable
                .insert("large-2", vec![2u8; 40 * PAGE_SIZE].as_slice())
                .unwrap();
            let mut multimap = txn.open_multimap_table(MULTIMAP).unwrap();
            for key in 0..200u32 {
                for value in 0..(key % 4 + 1) {
                    multimap
                        .insert(key, format!("v{}", value).as_str())
                        .unwrap();
                }
            }
            for value in 0..3000u32 {
                multimap
                    .insert(u32::MAX, format!("subtree-value-{:06}", value).as_str())
                    .unwrap();
            }
        }
        txn.commit().unwrap();

        // A persistent savepoint keeps the pages freed below allocated, and is itself a record
        // that the repair decodes
        let txn = db.begin_write().unwrap();
        txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut fixed = txn.open_table(FIXED).unwrap();
            for i in (0..4000u64).step_by(3) {
                fixed.remove(i).unwrap();
            }
            let mut variable = txn.open_table(VARIABLE).unwrap();
            variable.remove("large-1").unwrap();
            for i in (0..600u32).step_by(2) {
                variable.remove(format!("key-{:05}", i).as_str()).unwrap();
            }
            let mut multimap = txn.open_multimap_table(MULTIMAP).unwrap();
            multimap.remove_all(7).unwrap();
            for value in (0..3000u32).step_by(5) {
                multimap
                    .remove(u32::MAX, format!("subtree-value-{:06}", value).as_str())
                    .unwrap();
            }
        }
        txn.commit().unwrap();

        // A second savepoint, so two generations of roots are retained, and a commit after it
        let txn = db.begin_write().unwrap();
        txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut fixed = txn.open_table(FIXED).unwrap();
            for i in 10_000..10_500u64 {
                fixed.insert(i, i).unwrap();
            }
        }
        txn.commit().unwrap();
        // Dropping the database closes it cleanly and records the allocator state
    }
    Arc::try_unwrap(storage)
        .expect("the database released its backend")
        .into_inner()
        .unwrap()
}

#[derive(Arbitrary, Debug)]
enum Mutation {
    FlipBit { offset: u32, bit: u8 },
    SetByte { offset: u32, value: u8 },
    ZeroRange { offset: u32, len: u16 },
    Truncate { len: u32 },
    SwapPages { first: u16, second: u16 },
}

#[derive(Arbitrary, Debug)]
struct Input {
    mutations: Vec<Mutation>,
}

fn apply(bytes: &mut Vec<u8>, mutation: &Mutation) {
    let len = bytes.len();
    if len == 0 {
        return;
    }
    match *mutation {
        Mutation::FlipBit { offset, bit } => {
            bytes[offset as usize % len] ^= 1 << (bit % 8);
        }
        Mutation::SetByte { offset, value } => {
            bytes[offset as usize % len] = value;
        }
        Mutation::ZeroRange { offset, len: count } => {
            let start = offset as usize % len;
            let end = (start + count as usize).min(len);
            bytes[start..end].fill(0);
        }
        Mutation::Truncate { len: new_len } => {
            bytes.truncate(new_len as usize % (len + 1));
        }
        Mutation::SwapPages { first, second } => {
            let pages = len / PAGE_SIZE;
            if pages < 2 {
                return;
            }
            let first = first as usize % pages;
            let second = second as usize % pages;
            if first == second {
                return;
            }
            let (low, high) = (first.min(second), first.max(second));
            let (head, tail) = bytes.split_at_mut(high * PAGE_SIZE);
            head[low * PAGE_SIZE..(low + 1) * PAGE_SIZE].swap_with_slice(&mut tail[..PAGE_SIZE]);
        }
    }
}

// Every result below is allowed to be an error. Only a panic is a finding, and libFuzzer reports
// those as crashes.
fn exercise(bytes: Vec<u8>) {
    let storage = Arc::new(RwLock::new(bytes));
    let mut db = match Database::builder()
        .set_cache_size(CACHE_SIZE)
        .set_check_integrity_on_open(true)
        .create_with_backend(MemoryBackend(storage))
    {
        Ok(db) => db,
        Err(_) => return,
    };
    let _ = db.check_integrity();
    read_everything(&db);
    restore_savepoints(&db);
    write_something(&db);
}

fn read_everything(db: &Database) {
    let txn = match db.begin_read() {
        Ok(txn) => txn,
        Err(_) => return,
    };
    if let Ok(tables) = txn.list_tables() {
        for table in tables {
            black_box(table.name());
        }
    }
    if let Ok(tables) = txn.list_multimap_tables() {
        for table in tables {
            black_box(table.name());
        }
    }
    if let Ok(table) = txn.open_table(FIXED) {
        read_fixed(&table);
    }
    if let Ok(table) = txn.open_table(VARIABLE) {
        read_variable(&table);
    }
    if let Ok(table) = txn.open_multimap_table(MULTIMAP) {
        read_multimap(&table);
    }
}

// Restores each persistent savepoint in turn, which makes the tree it kept the current one, and
// reads that tree. The check verified those trees, so this must not panic either. Nothing is
// committed, so every savepoint is restored from the same state.
fn restore_savepoints(db: &Database) {
    let ids: Vec<u64> = match db.begin_write() {
        Ok(txn) => {
            let ids = match txn.list_persistent_savepoints() {
                Ok(ids) => ids.collect(),
                Err(_) => Vec::new(),
            };
            let _ = txn.abort();
            ids
        }
        Err(_) => return,
    };
    for id in ids {
        let mut txn = match db.begin_write() {
            Ok(txn) => txn,
            Err(_) => return,
        };
        let restored = match txn.get_persistent_savepoint(id) {
            Ok(savepoint) => txn.restore_savepoint(&savepoint).is_ok(),
            Err(_) => false,
        };
        if restored {
            if let Ok(table) = txn.open_table(FIXED) {
                read_fixed(&table);
            }
            if let Ok(table) = txn.open_table(VARIABLE) {
                read_variable(&table);
            }
            if let Ok(table) = txn.open_multimap_table(MULTIMAP) {
                read_multimap(&table);
            }
        }
        let _ = txn.abort();
    }
}

fn read_fixed(table: &impl ReadableTable<u64, u64>) {
    let _ = table.len();
    let _ = table.first();
    let _ = table.last();
    let _ = table.get(1234u64);
    if let Ok(iter) = table.iter() {
        for entry in iter {
            match entry {
                Ok((key, value)) => {
                    black_box(key.value());
                    black_box(value.value());
                }
                Err(_) => break,
            }
        }
    }
}

fn read_variable(table: &impl ReadableTable<&'static str, &'static [u8]>) {
    let _ = table.len();
    if let Ok(Some(value)) = table.get("large-2") {
        black_box(value.value().len());
    }
    if let Ok(iter) = table.iter() {
        for entry in iter {
            match entry {
                Ok((key, value)) => {
                    black_box(key.value());
                    black_box(value.value());
                }
                Err(_) => break,
            }
        }
    }
}

fn read_multimap(table: &impl ReadableMultimapTable<u32, &'static str>) {
    let _ = table.len();
    if let Ok(values) = table.get(u32::MAX) {
        for value in values {
            match value {
                Ok(value) => {
                    black_box(value.value());
                }
                Err(_) => break,
            }
        }
    }
    if let Ok(iter) = table.iter() {
        for entry in iter {
            let (key, values) = match entry {
                Ok(entry) => entry,
                Err(_) => break,
            };
            black_box(key.value());
            for value in values {
                match value {
                    Ok(value) => {
                        black_box(value.value());
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn write_something(db: &Database) {
    let txn = match db.begin_write() {
        Ok(txn) => txn,
        Err(_) => return,
    };
    {
        if let Ok(mut table) = txn.open_table(FIXED) {
            let _ = table.insert(u64::MAX, 1u64);
            let _ = table.remove(1u64);
        }
        if let Ok(mut table) = txn.open_multimap_table(MULTIMAP) {
            let _ = table.insert(1u32, "added");
            let _ = table.remove_all(u32::MAX);
        }
    }
    let _ = txn.commit();
}

fuzz_target!(|input: Input| {
    let pristine = PRISTINE.get_or_init(build_pristine);
    let mut bytes = pristine.clone();
    for mutation in input.mutations.iter().take(MAX_MUTATIONS) {
        apply(&mut bytes, mutation);
    }
    if bytes == *pristine {
        return;
    }
    exercise(bytes);
});
