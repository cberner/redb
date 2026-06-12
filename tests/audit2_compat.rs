// Audit tests: file-format compatibility, repair, persistent savepoints, and compaction.
//
// Each test in this file asserts *correct* behavior and currently FAILS on master,
// demonstrating a confirmed bug.

use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("table");
const TABLE_26: redb2_6::TableDefinition<u64, &[u8]> = redb2_6::TableDefinition::new("table");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn value_for(i: u64, len: usize) -> Vec<u8> {
    vec![(i % 250) as u8 + 1; len]
}

// === Bug 1: compact() returns the wrong error when savepoints exist ===
//
// `CompactionError::PersistentSavepointExists` and `CompactionError::EphemeralSavepointExists`
// are documented (and explicitly checked for in `Database::compact()`), but they are
// unreachable: every live savepoint holds a registered read transaction in the
// `TransactionTracker`, so the `any_user_read_reference_exists()` check at the top of
// `compact()` short-circuits with the misleading `TransactionInProgress` first.
// A user who sees `TransactionInProgress` will drop all their read transactions and
// still be unable to compact, with no indication that a (possibly persistent, on-disk)
// savepoint is the actual cause.
#[test]
fn compact_returns_persistent_savepoint_error() {
    let tmpfile = create_tempfile();
    let mut db = redb::Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(1, b"one".as_slice()).unwrap();
    }
    txn.commit().unwrap();

    // No read transactions exist. Only a persistent savepoint.
    let txn = db.begin_write().unwrap();
    let _id = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    let err = db.compact().unwrap_err();
    assert!(
        matches!(err, redb::CompactionError::PersistentSavepointExists),
        "expected PersistentSavepointExists, got {err:?}"
    );
}

#[test]
fn compact_returns_ephemeral_savepoint_error() {
    let tmpfile = create_tempfile();
    let mut db = redb::Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(1, b"one".as_slice()).unwrap();
    }
    txn.commit().unwrap();

    // No read transactions exist. Only an ephemeral savepoint.
    let txn = db.begin_write().unwrap();
    let savepoint = txn.ephemeral_savepoint().unwrap();
    txn.commit().unwrap();

    let err = db.compact().unwrap_err();
    assert!(
        matches!(err, redb::CompactionError::EphemeralSavepointExists),
        "expected EphemeralSavepointExists, got {err:?}"
    );
    drop(savepoint);
}

// === Bug 2: check_integrity() panics after a persistent savepoint is restored and deleted ===
//
// `restore_savepoint()` queues the pages recorded in the `data_pages_allocated` system table
// (for transactions newer than the savepoint) to be freed, but leaves the table entries in
// place. When the savepoint is later deleted, the post-commit free epilogue actually
// deallocates those pages, leaving `data_pages_allocated` entries that reference unallocated
// pages. `Database::check_integrity()` then panics (debug builds) on the internal invariant
// `assert!(mem.is_allocated(...))` in `check_repaired_allocated_pages_table`.
//
// check_integrity() on a valid database produced purely via the safe public API must never
// panic; it should return Ok(true).
#[test]
fn check_integrity_after_persistent_savepoint_restore_and_delete() {
    let tmpfile = create_tempfile();
    let mut db = redb::Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for i in 0..100u64 {
            table.insert(i, value_for(i, 100).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let id = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    // Allocate data pages after the savepoint; these get recorded in data_pages_allocated
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for i in 0..50u64 {
            table.remove(i).unwrap();
        }
        for i in 100..200u64 {
            table.insert(i, value_for(i, 100).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Restore the savepoint and then delete it
    let mut txn = db.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    drop(savepoint);
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    assert!(txn.delete_persistent_savepoint(id).unwrap());
    txn.commit().unwrap();

    // Must not panic
    assert!(db.check_integrity().unwrap());

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 100);
    for i in 0..100u64 {
        assert_eq!(
            table.get(i).unwrap().unwrap().value(),
            value_for(i, 100).as_slice()
        );
    }
}

// Same root cause as above, demonstrated on the backward-compatibility surface: a redb 2.6
// (file format v3) database containing a live persistent savepoint is opened by current redb,
// the savepoint is restored and deleted (all valid, documented operations), and then
// check_integrity() panics in debug builds.
#[test]
fn check_integrity_after_restoring_redb2_6_persistent_savepoint() {
    let tmpfile = create_tempfile();
    let savepoint_id;
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(tmpfile.path())
            .unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_26).unwrap();
            for i in 0..100u64 {
                table.insert(i, value_for(i, 100).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        savepoint_id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();

        // Modify the data after the savepoint (still in redb 2.6)
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_26).unwrap();
            for i in 0..50u64 {
                table.remove(i).unwrap();
            }
            for i in 100..200u64 {
                table.insert(i, value_for(i, 100).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let mut db = redb::Database::open(tmpfile.path()).unwrap();
    let ids: Vec<u64> = {
        let txn = db.begin_write().unwrap();
        let ids = txn.list_persistent_savepoints().unwrap().collect();
        txn.abort().unwrap();
        ids
    };
    assert_eq!(ids, vec![savepoint_id]);

    let mut txn = db.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    drop(savepoint);
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    assert!(txn.delete_persistent_savepoint(savepoint_id).unwrap());
    txn.commit().unwrap();

    // Must not panic
    assert!(db.check_integrity().unwrap());

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 100);
    for i in 0..100u64 {
        assert_eq!(
            table.get(i).unwrap().unwrap().value(),
            value_for(i, 100).as_slice()
        );
    }
}
