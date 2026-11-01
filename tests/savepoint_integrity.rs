//! `check_integrity()` must verify the pages that only a persistent savepoint still references,
//! since restoring the savepoint makes them the current tree again.

use redb::{Database, DatabaseError, ReadableTable, StorageError, TableDefinition};
use std::fs;

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("table");
const PAGE_SIZE: usize = 4096;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// The savepoint keeps the leaf holding the deleted `large` value, whose bytes are all ones. Stale
// copies of that leaf may also sit in free pages, which tolerate a flip, so only some of the flips
// must be rejected.
#[test]
fn check_integrity_verifies_the_trees_of_persistent_savepoints() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table
            .insert("large", vec![1u8; 3 * PAGE_SIZE].as_slice())
            .unwrap();
        table.insert("small", [2u8; 16].as_slice()).unwrap();
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    let savepoint_id = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.remove("large").unwrap();
    }
    txn.commit().unwrap();
    drop(db);
    let pristine = fs::read(tmpfile.path()).unwrap();

    let mut candidates = 0;
    let mut corrupted = 0;
    for page in 0..pristine.len() / PAGE_SIZE {
        let range = page * PAGE_SIZE..(page + 1) * PAGE_SIZE;
        if !pristine[range.clone()].iter().all(|byte| *byte == 1) {
            continue;
        }
        candidates += 1;
        let mut data = pristine.clone();
        data[range.start + PAGE_SIZE / 2] ^= 0x10;
        fs::write(tmpfile.path(), &data).unwrap();
        // The file was shut down cleanly, so the open trusts it; only the check reads the page
        let mut db = Database::open(tmpfile.path()).unwrap();
        match db.check_integrity() {
            Err(DatabaseError::Storage(StorageError::Corrupted(_))) => corrupted += 1,
            Ok(_) => {}
            Err(err) => panic!("unexpected error: {err:?}"),
        }
    }
    assert!(candidates >= 2, "{candidates} pages of the deleted value");
    assert!(
        corrupted > 0,
        "{corrupted} of {candidates} flips were rejected"
    );

    // The intact file passes, and the savepoint still restores the deleted value
    fs::write(tmpfile.path(), &pristine).unwrap();
    let mut db = Database::open(tmpfile.path()).unwrap();
    assert!(db.check_integrity().unwrap());
    let mut txn = db.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(
        table.get("large").unwrap().unwrap().value().len(),
        3 * PAGE_SIZE
    );
    drop(table);
    txn.abort().unwrap();
}
