//! Tests that an aborted transaction leaves nothing behind for `check_integrity()` to repair.
//!
//! Growing the file updates the layout in memory, but only a header write publishes it. A commit
//! writes the header itself; an abort does not, and rolling the allocations back does not shrink
//! the file. The abort must therefore publish the layout, or the header on disk keeps describing
//! fewer regions than the file holds and the database reads as needing repair.

#[cfg(feature = "experimental-api-5")]
use redb::ReadableTable;
use redb::{Database, ReadableDatabase, TableDefinition};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("t");

// Inserts enough data to make the file grow, then abandons it.
fn grow_then_abort(db: &Database) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..3000u64 {
            table.insert(&k, vec![0xAAu8; 2000].as_slice()).unwrap();
        }
    }
    txn.abort().unwrap();
}

#[test]
fn check_integrity_passes_after_an_abort_that_grew_the_file() {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::create(tmpfile.path()).unwrap();
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&0u64, b"committed".as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    assert!(db.check_integrity().unwrap());

    let len_before = tmpfile.path().metadata().unwrap().len();
    grow_then_abort(&db);
    assert!(
        tmpfile.path().metadata().unwrap().len() > len_before,
        "the aborted transaction was expected to grow the file"
    );

    assert!(
        db.check_integrity().unwrap(),
        "an aborted transaction left the database reading as unclean"
    );

    // The abort rolled back, and the committed value is untouched
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.get(&0u64).unwrap().unwrap().value(), b"committed");
    assert!(table.get(&1u64).unwrap().is_none());
}

#[test]
fn check_integrity_passes_after_a_dropped_transaction_that_grew_the_file() {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::create(tmpfile.path()).unwrap();
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&0u64, b"committed".as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    // Dropping a write transaction aborts it, and must leave the same state
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for k in 0..3000u64 {
                table.insert(&k, vec![0xAAu8; 2000].as_slice()).unwrap();
            }
        }
    }

    assert!(
        db.check_integrity().unwrap(),
        "a dropped transaction left the database reading as unclean"
    );
}
