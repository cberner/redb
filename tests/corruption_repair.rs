//! `Builder::set_check_integrity_on_open(true)` runs the crash repair on every open, so the file is
//! verified before anything in it is trusted. On an intact file the open must succeed and keep the
//! data. On a corrupted file it must return an error, or open a database that `check_integrity()`
//! and the reads after it handle, and it must never panic. A panic fails these tests by itself, so
//! the assertions below only check the outcomes that must occur.
//!
//! Checksums make most corruption equivalent: any change inside the checksummed part of a page is
//! rejected the same way. The variants below therefore concentrate on the bytes with their own
//! parsing logic, the file header and the page headers, plus the structural damage a flip cannot
//! express: truncation, a zeroed page, and pages swapped with each other.

use redb::{
    Database, DatabaseError, MultimapTableDefinition, ReadableDatabase, ReadableMultimapTable,
    ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
};
use std::fs;
use std::hint::black_box;
use std::path::Path;

const FIXED: TableDefinition<u64, u64> = TableDefinition::new("fixed");
const VARIABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("variable");
const MULTIMAP: MultimapTableDefinition<u32, &str> = MultimapTableDefinition::new("multimap");

const PAGE_SIZE: usize = 4096;
const FIXED_ENTRIES: u64 = 1200;
// The super-header: magic number, flags, page and region sizes, and the two transaction slots
const HEADER_BYTES: usize = 256;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// Every structure a repair walks: tables deep enough for branch pages, values spanning several
// pages, a multimap with inline and subtree collections, persistent savepoints that keep freed
// pages allocated, and deletes after them. Closed cleanly, so a plain open would trust the
// recorded allocator state.
fn build_database(path: &Path) {
    let db = Database::create(path).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut fixed = txn.open_table(FIXED).unwrap();
        for i in 0..FIXED_ENTRIES {
            fixed
                .insert(i, i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
                .unwrap();
        }
        let mut variable = txn.open_table(VARIABLE).unwrap();
        for i in 0..300u32 {
            let key = format!("key-{i:05}");
            let value = vec![(i % 251) as u8; (i as usize % 97) + 1];
            variable.insert(key.as_str(), value.as_slice()).unwrap();
        }
        variable
            .insert("large-1", vec![1u8; 3 * PAGE_SIZE].as_slice())
            .unwrap();
        variable
            .insert("large-2", vec![2u8; 6 * PAGE_SIZE].as_slice())
            .unwrap();
        let mut multimap = txn.open_multimap_table(MULTIMAP).unwrap();
        for key in 0..100u32 {
            for value in 0..(key % 4 + 1) {
                multimap.insert(key, format!("v{value}").as_str()).unwrap();
            }
        }
        for value in 0..800u32 {
            multimap
                .insert(u32::MAX, format!("subtree-value-{value:06}").as_str())
                .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut fixed = txn.open_table(FIXED).unwrap();
        for i in (0..FIXED_ENTRIES).step_by(3) {
            fixed.remove(i).unwrap();
        }
        let mut variable = txn.open_table(VARIABLE).unwrap();
        variable.remove("large-1").unwrap();
        let mut multimap = txn.open_multimap_table(MULTIMAP).unwrap();
        multimap.remove_all(7).unwrap();
        for value in (0..800u32).step_by(5) {
            multimap
                .remove(u32::MAX, format!("subtree-value-{value:06}").as_str())
                .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
}

fn read_everything(db: &Database) {
    let Ok(txn) = db.begin_read() else {
        return;
    };
    if let Ok(table) = txn.open_table(FIXED) {
        let _ = table.len();
        let _ = table.first();
        let _ = table.get(1234u64);
        if let Ok(iter) = table.iter() {
            for entry in iter {
                let Ok((key, value)) = entry else { break };
                black_box(key.value());
                black_box(value.value());
            }
        }
    }
    if let Ok(table) = txn.open_table(VARIABLE) {
        if let Ok(Some(value)) = table.get("large-2") {
            black_box(value.value().len());
        }
        if let Ok(iter) = table.iter() {
            for entry in iter {
                let Ok((key, value)) = entry else { break };
                black_box(key.value());
                black_box(value.value());
            }
        }
    }
    if let Ok(table) = txn.open_multimap_table(MULTIMAP)
        && let Ok(iter) = table.iter()
    {
        for entry in iter {
            let Ok((key, values)) = entry else { break };
            black_box(key.value());
            for value in values {
                let Ok(value) = value else { break };
                black_box(value.value());
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    // The repair accepted the file; `check_integrity()` and a full read then ran on it
    Opened,
    Corrupted,
    OtherError,
}

// Writes `data` to `path` and opens it with a repair, exercising the database if that succeeds.
// Running to completion is the check; the outcome lets a caller confirm the paths it meant to
// cover were hit.
fn open_corrupted(path: &Path, data: &[u8]) -> Outcome {
    fs::write(path, data).unwrap();
    match Database::builder()
        .set_check_integrity_on_open(true)
        .open(path)
    {
        Ok(mut db) => {
            let _ = db.check_integrity();
            read_everything(&db);
            Outcome::Opened
        }
        Err(DatabaseError::Storage(StorageError::Corrupted(_))) => Outcome::Corrupted,
        Err(_) => Outcome::OtherError,
    }
}

fn count(outcomes: &[Outcome], outcome: Outcome) -> usize {
    outcomes.iter().filter(|o| **o == outcome).count()
}

#[test]
fn check_integrity_on_open_opens_an_intact_database() {
    let tmpfile = create_tempfile();
    build_database(tmpfile.path());

    let mut db = Database::builder()
        .set_check_integrity_on_open(true)
        .open(tmpfile.path())
        .unwrap();
    assert!(db.check_integrity().unwrap());
    let txn = db.begin_read().unwrap();
    let fixed = txn.open_table(FIXED).unwrap();
    assert_eq!(
        fixed.len().unwrap(),
        FIXED_ENTRIES - FIXED_ENTRIES.div_ceil(3)
    );
    assert_eq!(
        fixed.get(1u64).unwrap().unwrap().value(),
        1u64.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    );
    assert!(fixed.get(3u64).unwrap().is_none());
    let variable = txn.open_table(VARIABLE).unwrap();
    assert_eq!(
        variable.get("large-2").unwrap().unwrap().value().len(),
        6 * PAGE_SIZE
    );
    let multimap = txn.open_multimap_table(MULTIMAP).unwrap();
    assert_eq!(multimap.get(u32::MAX).unwrap().count(), 640);
    drop(txn);

    // The first savepoint still holds the data deleted after it
    let mut txn = db.begin_write().unwrap();
    let ids: Vec<u64> = txn.list_persistent_savepoints().unwrap().collect();
    assert_eq!(ids.len(), 2);
    let savepoint = txn.get_persistent_savepoint(ids[0]).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    let fixed = txn.open_table(FIXED).unwrap();
    assert_eq!(fixed.len().unwrap(), FIXED_ENTRIES);
    let variable = txn.open_table(VARIABLE).unwrap();
    assert_eq!(
        variable.get("large-1").unwrap().unwrap().value().len(),
        3 * PAGE_SIZE
    );
    drop(fixed);
    drop(variable);
    txn.abort().unwrap();
    drop(db);

    // The repair committed, and the database opens normally afterwards
    let db = Database::open(tmpfile.path()).unwrap();
    let txn = db.begin_read().unwrap();
    assert_eq!(
        txn.open_table(FIXED).unwrap().len().unwrap(),
        FIXED_ENTRIES - FIXED_ENTRIES.div_ceil(3)
    );
}

// Every byte of the super-header has its own meaning, so each gets a flipped bit, and the leading
// fields that are read as sizes and counts also get the largest value
#[test]
fn check_integrity_on_open_survives_a_corrupted_header() {
    let tmpfile = create_tempfile();
    build_database(tmpfile.path());
    let pristine = fs::read(tmpfile.path()).unwrap();

    let mut outcomes = vec![];
    for byte in 0..HEADER_BYTES {
        let mut data = pristine.clone();
        data[byte] ^= 1 << (byte % 8);
        outcomes.push(open_corrupted(tmpfile.path(), &data));
    }
    for byte in 0..64 {
        let mut data = pristine.clone();
        data[byte] = 0xFF;
        outcomes.push(open_corrupted(tmpfile.path(), &data));
    }
    // Slot checksums and unused bytes tolerate a flip, the magic number and sizes do not
    assert!(count(&outcomes, Outcome::Opened) > 0, "{outcomes:?}");
    assert!(count(&outcomes, Outcome::Corrupted) > 0, "{outcomes:?}");
}

// One flip in the header of every page, where the type byte, counts and offsets live, and one in
// the middle of every fourth page
#[test]
fn check_integrity_on_open_survives_a_flipped_bit_in_every_page() {
    let tmpfile = create_tempfile();
    build_database(tmpfile.path());
    let pristine = fs::read(tmpfile.path()).unwrap();
    let pages = pristine.len() / PAGE_SIZE;
    assert!(pages > 16, "database is unexpectedly small");

    let mut outcomes = vec![];
    for page in 0..pages {
        let mut data = pristine.clone();
        data[page * PAGE_SIZE + 8] ^= 0x10;
        outcomes.push(open_corrupted(tmpfile.path(), &data));
        if page % 4 == 0 {
            let mut data = pristine.clone();
            data[page * PAGE_SIZE + PAGE_SIZE / 2] ^= 0x10;
            outcomes.push(open_corrupted(tmpfile.path(), &data));
        }
    }
    // Free pages tolerate a flip, pages of the committed trees do not
    assert!(count(&outcomes, Outcome::Opened) > 0, "{outcomes:?}");
    assert!(count(&outcomes, Outcome::Corrupted) > 0, "{outcomes:?}");
}

// Truncation at page boundaries, inside a page, and down to nothing
#[test]
fn check_integrity_on_open_survives_truncation() {
    let tmpfile = create_tempfile();
    build_database(tmpfile.path());
    let pristine = fs::read(tmpfile.path()).unwrap();

    let mut lengths = vec![0, 1, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, 2 * PAGE_SIZE];
    for divisor in [2, 3, 4, 8] {
        lengths.push(pristine.len() / divisor);
        lengths.push(pristine.len() / divisor / PAGE_SIZE * PAGE_SIZE);
    }
    lengths.push(pristine.len() - 1);
    lengths.push(pristine.len() - PAGE_SIZE);
    for len in lengths {
        open_corrupted(tmpfile.path(), &pristine[..len]);
    }
}

// A page lost to zeroes, and pages that are intact but in the wrong place, which a per-page check
// cannot tell apart from a good page
#[test]
fn check_integrity_on_open_survives_zeroed_and_swapped_pages() {
    let tmpfile = create_tempfile();
    build_database(tmpfile.path());
    let pristine = fs::read(tmpfile.path()).unwrap();
    let pages = pristine.len() / PAGE_SIZE;

    let mut outcomes = vec![];
    for page in (0..pages).step_by(2) {
        let mut data = pristine.clone();
        data[page * PAGE_SIZE..(page + 1) * PAGE_SIZE].fill(0);
        outcomes.push(open_corrupted(tmpfile.path(), &data));
    }
    let mut swaps: Vec<(usize, usize)> = (1..pages - 1).step_by(7).map(|p| (p, p + 1)).collect();
    swaps.push((1, pages - 1));
    swaps.push((0, 1));
    for (first, second) in swaps {
        let mut data = pristine.clone();
        let (head, tail) = data.split_at_mut(second * PAGE_SIZE);
        head[first * PAGE_SIZE..(first + 1) * PAGE_SIZE].swap_with_slice(&mut tail[..PAGE_SIZE]);
        outcomes.push(open_corrupted(tmpfile.path(), &data));
    }
    assert!(count(&outcomes, Outcome::Corrupted) > 0, "{outcomes:?}");
}
