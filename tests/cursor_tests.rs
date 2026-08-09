#![cfg(feature = "experimental_cursor")]

use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError,
    TableDefinition, TransactionError,
};
use std::ops::Bound;

const U64_TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");
const STR_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("x");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// Zero padded so that lexicographic order matches numeric order
fn key(i: u64) -> String {
    format!("{i:016}")
}

fn assert_u64_table_contents(db: &Database, expected: &[(u64, u64)]) {
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), expected.len() as u64);
    let actual: Vec<(u64, u64)> = table
        .iter()
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value(), value.value())
        })
        .collect();
    assert_eq!(actual, expected);
    for (key, value) in expected {
        assert_eq!(table.get(key).unwrap().unwrap().value(), *value);
    }
}

#[test]
fn bulk_load_into_empty_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
        for i in 0..10_000 {
            cursor.insert_before(i, &(i * 7)).unwrap();
        }
        cursor.close().unwrap();
        assert_eq!(table.len().unwrap(), 10_000);
        assert_eq!(table.get(&5432).unwrap().unwrap().value(), 5432 * 7);
    }
    txn.commit().unwrap();

    let expected: Vec<(u64, u64)> = (0..10_000).map(|i| (i, i * 7)).collect();
    assert_u64_table_contents(&db, &expected);

    // Also readable after reopening the database
    drop(db);
    let db = Database::open(tmpfile.path()).unwrap();
    assert_u64_table_contents(&db, &expected);
}

// Enough data to cross the cursor's internal buffer threshold several times,
// so mid-load splices and reseeks are exercised through the public API.
#[test]
fn bulk_load_crosses_flush_threshold() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(STR_TABLE).unwrap();
        let mut cursor = table.upper_bound_mut(Bound::<&str>::Unbounded).unwrap();
        for i in 0..600u64 {
            // Varying sizes exercise the leaf packing decisions
            let value = vec![i as u8; 2000 + (i as usize % 3000)];
            cursor
                .insert_before(key(i).as_str(), value.as_slice())
                .unwrap();
        }
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(STR_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 600);
    for (index, entry) in table.iter().unwrap().enumerate() {
        let (k, v) = entry.unwrap();
        let i = index as u64;
        assert_eq!(k.value(), key(i));
        assert_eq!(v.value(), vec![i as u8; 2000 + (index % 3000)]);
    }
}

// A value that does not fit in a single page must get a page of its own,
// like `Table::insert` gives it.
#[test]
fn bulk_load_large_values() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(STR_TABLE).unwrap();
        let mut cursor = table.upper_bound_mut(Bound::<&str>::Unbounded).unwrap();
        for i in 0..20u64 {
            let size = if i % 4 == 0 { 100_000 } else { 10 };
            let value = vec![i as u8; size];
            cursor
                .insert_before(key(i).as_str(), value.as_slice())
                .unwrap();
        }
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(STR_TABLE).unwrap();
    for i in 0..20u64 {
        let size = if i % 4 == 0 { 100_000 } else { 10 };
        let value = table.get(key(i).as_str()).unwrap().unwrap();
        assert_eq!(value.value(), vec![i as u8; size]);
    }
}

// Dropping the cursor splices the pending inserts, like closing it does.
#[test]
fn append_to_existing_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in 0..1000 {
            table.insert(i, i).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        {
            let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
            assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 999);
            for i in 1000..2000 {
                cursor.insert_before(i, &i).unwrap();
            }
        }
        assert_eq!(table.len().unwrap(), 2000);
    }
    txn.commit().unwrap();

    let expected: Vec<(u64, u64)> = (0..2000).map(|i| (i, i)).collect();
    assert_u64_table_contents(&db, &expected);
}

#[test]
fn insert_into_middle_gap() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        // Even keys, leaving odd keys insertable in the gaps
        for i in 0..1000 {
            table.insert(i * 2, i * 2).unwrap();
        }
        let mut cursor = table.lower_bound_mut(Bound::Included(&500)).unwrap();
        assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 498);
        assert_eq!(cursor.peek_next().unwrap().unwrap().0.value(), 500);
        cursor.insert_before(499, &499).unwrap();
        // The gap now sits between the new entry and 500
        assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 499);
        assert_eq!(cursor.peek_next().unwrap().unwrap().0.value(), 500);
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let mut expected: Vec<(u64, u64)> = (0..1000).map(|i| (i * 2, i * 2)).collect();
    expected.push((499, 499));
    expected.sort_unstable();
    assert_u64_table_contents(&db, &expected);
}

// Middle inserts under a non-rightmost branch of a taller tree exercise the
// separator bookkeeping of the ancestor rebuild.
#[test]
fn insert_into_middle_of_tall_tree() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        {
            let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
            for i in 0..100_000 {
                cursor.insert_before(i * 2, &(i * 2)).unwrap();
            }
        }
        for target in [101u64, 50_001, 100_001, 150_001, 199_001] {
            let mut cursor = table.lower_bound_mut(Bound::Included(&target)).unwrap();
            cursor.insert_before(target, &target).unwrap();
            cursor.close().unwrap();
        }
    }
    txn.commit().unwrap();

    let mut expected: Vec<(u64, u64)> = (0..100_000).map(|i| (i * 2, i * 2)).collect();
    expected.extend([101u64, 50_001, 100_001, 150_001, 199_001].map(|i| (i, i)));
    expected.sort_unstable();
    assert_u64_table_contents(&db, &expected);
}

#[test]
fn unordered_keys_rejected() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in [10u64, 20, 30] {
            table.insert(i, i).unwrap();
        }
        let mut cursor = table.upper_bound_mut(Bound::Included(&20)).unwrap();
        // Equal to either neighbor, below the gap, and above the gap all fail
        for bad in [10u64, 15, 20, 30, 35] {
            assert!(matches!(
                cursor.insert_before(bad, &bad),
                Err(StorageError::UnorderedKey)
            ));
        }
        // The cursor remains usable after rejections
        cursor.insert_before(25, &25).unwrap();
        // Later inserts must also stay above the pending insert
        assert!(matches!(
            cursor.insert_before(25, &25),
            Err(StorageError::UnorderedKey)
        ));
        assert!(matches!(
            cursor.insert_before(24, &24),
            Err(StorageError::UnorderedKey)
        ));
        cursor.insert_before(26, &26).unwrap();
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let expected: Vec<(u64, u64)> = [10, 20, 25, 26, 30].iter().map(|&i| (i, i)).collect();
    assert_u64_table_contents(&db, &expected);
}

#[test]
fn bound_positions() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in [10u64, 20, 30] {
            table.insert(i, i).unwrap();
        }

        let peeked = |cursor: &mut redb::CursorMut<'_, u64, u64>| {
            let prev = cursor.peek_prev().unwrap().map(|(k, _)| k.value());
            let next = cursor.peek_next().unwrap().map(|(k, _)| k.value());
            (prev, next)
        };

        let mut cursor = table.lower_bound_mut(Bound::<u64>::Unbounded).unwrap();
        assert_eq!(peeked(&mut cursor), (None, Some(10)));
        drop(cursor);
        let mut cursor = table.lower_bound_mut(Bound::Included(&20)).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(10), Some(20)));
        drop(cursor);
        let mut cursor = table.lower_bound_mut(Bound::Excluded(&20)).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(20), Some(30)));
        drop(cursor);
        let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(30), None));
        drop(cursor);
        let mut cursor = table.upper_bound_mut(Bound::Included(&20)).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(20), Some(30)));
        drop(cursor);
        let mut cursor = table.upper_bound_mut(Bound::Excluded(&20)).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(10), Some(20)));
        drop(cursor);
        // A bound between entries points into the same gap either way
        let mut cursor = table.lower_bound_mut(Bound::Included(&25)).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(20), Some(30)));
        drop(cursor);
        let mut cursor = table.upper_bound_mut(Bound::Included(&25)).unwrap();
        assert_eq!(peeked(&mut cursor), (Some(20), Some(30)));
    }
    txn.abort().unwrap();
}

// Peeks observe pending inserts without splicing them.
#[test]
fn peeks_during_pending_inserts() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in [10u64, 30] {
            table.insert(i, i).unwrap();
        }
        let mut cursor = table.upper_bound_mut(Bound::Included(&10)).unwrap();
        cursor.insert_before(20, &200).unwrap();
        let (prev_key, prev_value) = cursor.peek_prev().unwrap().unwrap();
        assert_eq!(prev_key.value(), 20);
        assert_eq!(prev_value.value(), 200);
        drop((prev_key, prev_value));
        assert_eq!(cursor.peek_next().unwrap().unwrap().0.value(), 30);
        cursor.insert_before(21, &210).unwrap();
        assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 21);
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let expected: Vec<(u64, u64)> = vec![(10, 10), (20, 200), (21, 210), (30, 30)];
    assert_u64_table_contents(&db, &expected);
}

#[test]
fn peeks_on_empty_table_and_at_start() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        {
            let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
            assert!(cursor.peek_prev().unwrap().is_none());
            assert!(cursor.peek_next().unwrap().is_none());
            cursor.insert_before(100, &100).unwrap();
            assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 100);
            assert!(cursor.peek_next().unwrap().is_none());
            cursor.close().unwrap();
        }
        // At the start of a non-empty table, a pending insert becomes the
        // predecessor while the old first entry stays the successor
        {
            let mut cursor = table.lower_bound_mut(Bound::<u64>::Unbounded).unwrap();
            assert!(cursor.peek_prev().unwrap().is_none());
            assert_eq!(cursor.peek_next().unwrap().unwrap().0.value(), 100);
            cursor.insert_before(50, &50).unwrap();
            assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 50);
            assert_eq!(cursor.peek_next().unwrap().unwrap().0.value(), 100);
            cursor.close().unwrap();
        }
    }
    txn.commit().unwrap();

    assert_u64_table_contents(&db, &[(50, 50), (100, 100)]);
}

// A cursor that inserts nothing, including one whose every insert was
// rejected, leaves the table untouched.
#[test]
fn empty_and_rejected_only_cursors() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(10, 10).unwrap();
        let cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
        cursor.close().unwrap();
        let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
        assert!(matches!(
            cursor.insert_before(10, &10),
            Err(StorageError::UnorderedKey)
        ));
        cursor.close().unwrap();
        assert_eq!(table.len().unwrap(), 1);
    }
    txn.commit().unwrap();

    assert_u64_table_contents(&db, &[(10, 10)]);
}

// Several transactions each appending a sorted batch through a cursor: the
// day-two shape of a bulk load.
#[test]
fn incremental_appends_across_transactions() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    for batch in 0..5u64 {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(U64_TABLE).unwrap();
            let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
            for i in (batch * 1000)..((batch + 1) * 1000) {
                cursor.insert_before(i, &i).unwrap();
            }
            cursor.close().unwrap();
        }
        txn.commit().unwrap();
    }

    let expected: Vec<(u64, u64)> = (0..5000).map(|i| (i, i)).collect();
    assert_u64_table_contents(&db, &expected);
}

// An aborted transaction discards inserts spliced through a cursor.
#[test]
fn abort_discards_cursor_inserts() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(1, 1).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        let mut cursor = table.upper_bound_mut(Bound::<u64>::Unbounded).unwrap();
        for i in 2..1000 {
            cursor.insert_before(i, &i).unwrap();
        }
        cursor.close().unwrap();
    }
    txn.abort().unwrap();

    assert_u64_table_contents(&db, &[(1, 1)]);
}

fn populate_u64_table(db: &Database, entries: impl Iterator<Item = (u64, u64)>) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for (key, value) in entries {
            table.insert(key, &value).unwrap();
        }
    }
    txn.commit().unwrap();
}

#[allow(clippy::type_complexity)]
fn peeked_key(entry: Option<(redb::AccessGuard<u64>, redb::AccessGuard<u64>)>) -> Option<u64> {
    entry.map(|(key, _)| key.value())
}

#[test]
fn read_cursor_all_bound_positions() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    populate_u64_table(&db, [10, 20, 30].into_iter().map(|key| (key, key * 2)));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();

    // (bound constructor, is_lower, expected peek_prev, expected peek_next)
    #[allow(clippy::type_complexity)]
    let cases: &[(Bound<u64>, bool, Option<u64>, Option<u64>)] = &[
        (Bound::Unbounded, true, None, Some(10)),
        (Bound::Included(20), true, Some(10), Some(20)),
        (Bound::Excluded(20), true, Some(20), Some(30)),
        (Bound::Included(15), true, Some(10), Some(20)),
        (Bound::Unbounded, false, Some(30), None),
        (Bound::Included(20), false, Some(20), Some(30)),
        (Bound::Excluded(20), false, Some(10), Some(20)),
        (Bound::Included(15), false, Some(10), Some(20)),
    ];
    for (bound, is_lower, previous, next) in cases {
        let mut cursor = if *is_lower {
            table.lower_bound(bound.as_ref()).unwrap()
        } else {
            table.upper_bound(bound.as_ref()).unwrap()
        };
        assert_eq!(peeked_key(cursor.peek_prev().unwrap()), *previous);
        assert_eq!(peeked_key(cursor.peek_next().unwrap()), *next);
        // Peeks never move the cursor.
        assert_eq!(peeked_key(cursor.peek_prev().unwrap()), *previous);
        assert_eq!(peeked_key(cursor.peek_next().unwrap()), *next);
        // The values came along.
        if let Some((key, value)) = cursor.peek_next().unwrap() {
            assert_eq!(value.value(), key.value() * 2);
        }
    }
}

#[test]
fn read_cursor_walks_both_directions() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    populate_u64_table(&db, (0..1_000).map(|key| (key, key * 3)));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();

    let mut cursor = table.lower_bound(Bound::<u64>::Unbounded).unwrap();
    for expected in 0..1_000 {
        let (key, value) = cursor.next().unwrap().unwrap();
        assert_eq!(key.value(), expected);
        assert_eq!(value.value(), expected * 3);
    }
    assert!(cursor.next().unwrap().is_none());
    // The gap is now at the end; walk all the way back.
    for expected in (0..1_000).rev() {
        let (key, _) = cursor.prev().unwrap().unwrap();
        assert_eq!(key.value(), expected);
    }
    assert!(cursor.prev().unwrap().is_none());

    // Stepping over an entry and back returns the same entry.
    let mut cursor = table.lower_bound(Bound::Included(&500)).unwrap();
    assert_eq!(peeked_key(cursor.next().unwrap()), Some(500));
    assert_eq!(peeked_key(cursor.prev().unwrap()), Some(500));
    assert_eq!(peeked_key(cursor.peek_next().unwrap()), Some(500));
    assert_eq!(peeked_key(cursor.peek_prev().unwrap()), Some(499));
}

#[test]
fn read_cursor_string_keys() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(STR_TABLE).unwrap();
        for i in 0..100u64 {
            table.insert(key(i).as_str(), key(i).as_bytes()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(STR_TABLE).unwrap();
    let mut cursor = table
        .lower_bound(Bound::Included(key(50).as_str()))
        .unwrap();
    let (first, _) = cursor.peek_next().unwrap().unwrap();
    assert_eq!(first.value(), key(50));
    drop(first);
    for i in 50..100 {
        let (entry_key, entry_value) = cursor.next().unwrap().unwrap();
        assert_eq!(entry_key.value(), key(i));
        assert_eq!(entry_value.value(), key(i).as_bytes());
    }
    assert!(cursor.next().unwrap().is_none());
}

#[test]
fn read_cursor_empty_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(U64_TABLE).map(drop).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    let mut cursor = table.lower_bound(Bound::<u64>::Unbounded).unwrap();
    assert!(cursor.peek_next().unwrap().is_none());
    assert!(cursor.peek_prev().unwrap().is_none());
    assert!(cursor.next().unwrap().is_none());
    assert!(cursor.prev().unwrap().is_none());
    let mut cursor = table.upper_bound(Bound::Included(&5)).unwrap();
    assert!(cursor.next().unwrap().is_none());
    assert!(cursor.prev().unwrap().is_none());
}

#[test]
fn read_cursor_sees_uncommitted_writes() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(i, &(i + 1)).unwrap();
        }
        // A read-only cursor over the write table sees the uncommitted state.
        let mut cursor = table.upper_bound(Bound::<u64>::Unbounded).unwrap();
        assert_eq!(peeked_key(cursor.peek_prev().unwrap()), Some(99));
        assert_eq!(peeked_key(cursor.prev().unwrap()), Some(99));
        assert_eq!(peeked_key(cursor.prev().unwrap()), Some(98));
        drop(cursor);

        let mut cursor = table.lower_bound(Bound::Excluded(&41)).unwrap();
        let (key, value) = cursor.next().unwrap().unwrap();
        assert_eq!((key.value(), value.value()), (42, 43));
    }
    txn.abort().unwrap();
}

#[test]
fn read_cursor_outlives_read_transaction() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    populate_u64_table(&db, (0..10).map(|key| (key, key)));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    let mut cursor = table.lower_bound(Bound::Included(&3)).unwrap();
    // The table keeps the transaction alive, so the cursor and its guards
    // stay usable after the ReadTransaction is dropped.
    drop(txn);
    let (key, _) = cursor.next().unwrap().unwrap();
    assert_eq!(key.value(), 3);
    assert_eq!(peeked_key(cursor.peek_next().unwrap()), Some(4));
}

#[test]
fn read_cursor_keeps_transaction_alive() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    populate_u64_table(&db, (0..1_000).map(|key| (key, key)));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    let mut cursor = table.lower_bound(Bound::Included(&5)).unwrap();
    assert_eq!(peeked_key(cursor.next().unwrap()), Some(5));

    // The live cursor keeps the transaction registered. Dropping the table
    // instead is rejected at compile time, by the cursor's Drop impl; see the
    // compile_fail example on `Cursor`.
    assert!(matches!(
        txn.close(),
        Err(TransactionError::ReadTransactionStillInUse(_))
    ));

    // Concurrent writers must not reclaim the pages the cursor holds
    for _ in 0..10 {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(U64_TABLE).unwrap();
            for key in 0..1_000 {
                table.insert(key, &(key + 1)).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // The pages the cursor was holding across all of that are still intact
    assert_eq!(peeked_key(cursor.next().unwrap()), Some(6));
}

#[test]
fn read_cursor_guards_outlive_cursor() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    populate_u64_table(&db, (0..10).map(|key| (key, key)));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    let mut cursor = table.lower_bound(Bound::Included(&3)).unwrap();
    // The guards borrow the table, not the cursor: they stay valid after the
    // cursor moves on or is dropped.
    let (stepped, _) = cursor.next().unwrap().unwrap();
    let peeked = cursor.peek_next().unwrap().unwrap();
    drop(cursor);
    assert_eq!(stepped.value(), 3);
    assert_eq!(peeked.0.value(), 4);
}

#[test]
fn read_cursor_generic_over_readable_table() {
    fn nearest_at_or_above<T: ReadableTable<u64, u64>>(table: &T, key: u64) -> Option<u64> {
        let mut cursor = table.lower_bound(Bound::Included(&key)).unwrap();
        cursor.peek_next().unwrap().map(|(key, _)| key.value())
    }

    fn nearest_below<T: ReadableTable<u64, u64>>(table: &T, key: u64) -> Option<u64> {
        let mut cursor = table.upper_bound(Bound::Excluded(&key)).unwrap();
        cursor.peek_prev().unwrap().map(|(key, _)| key.value())
    }

    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    populate_u64_table(&db, [10, 20, 30].into_iter().map(|key| (key, key)));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    assert_eq!(nearest_at_or_above(&table, 15), Some(20));
    assert_eq!(nearest_below(&table, 15), Some(10));
    assert_eq!(nearest_at_or_above(&table, 31), None);
    drop(table);
    drop(txn);

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(25, &25).unwrap();
        assert_eq!(nearest_at_or_above(&table, 21), Some(25));
        assert_eq!(nearest_below(&table, 25), Some(20));
    }
    txn.abort().unwrap();
}

#[test]
fn bulk_load_descending() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        let mut cursor = table.lower_bound_mut(Bound::<u64>::Unbounded).unwrap();
        for i in (0..10_000).rev() {
            cursor.insert_after(i, &(i * 7)).unwrap();
        }
        cursor.close().unwrap();
        assert_eq!(table.len().unwrap(), 10_000);
        assert_eq!(table.get(&5432).unwrap().unwrap().value(), 5432 * 7);
    }
    txn.commit().unwrap();

    let expected: Vec<(u64, u64)> = (0..10_000).map(|i| (i, i * 7)).collect();
    assert_u64_table_contents(&db, &expected);

    // Also readable after reopening the database
    drop(db);
    let db = Database::open(tmpfile.path()).unwrap();
    assert_u64_table_contents(&db, &expected);
}

// Enough data to cross the cursor's internal buffer threshold several times
// while loading backward, so mid-run splices and reseeks on the after side
// are exercised through the public API.
#[test]
fn descending_load_crosses_flush_threshold() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(STR_TABLE).unwrap();
        let mut cursor = table.lower_bound_mut(Bound::<&str>::Unbounded).unwrap();
        for i in (0..600u64).rev() {
            let value = vec![i as u8; 2000 + (i as usize % 3000)];
            cursor
                .insert_after(key(i).as_str(), value.as_slice())
                .unwrap();
        }
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(STR_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 600);
    for (index, entry) in table.iter().unwrap().enumerate() {
        let (k, v) = entry.unwrap();
        let i = index as u64;
        assert_eq!(k.value(), key(i));
        assert_eq!(v.value(), vec![i as u8; 2000 + (index % 3000)]);
    }
}

#[test]
fn mixed_direction_inserts_and_peeks() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in [10u64, 20, 30] {
            table.insert(i, &i).unwrap();
        }
        let mut cursor = table.upper_bound_mut(Bound::Included(&20)).unwrap();
        cursor.insert_before(21, &21).unwrap();
        cursor.insert_after(29, &29).unwrap();
        cursor.insert_after(25, &25).unwrap();
        // The peeks return the pending inserts nearest the gap on each side.
        assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 21);
        assert_eq!(cursor.peek_next().unwrap().unwrap().0.value(), 25);
        cursor.insert_before(22, &22).unwrap();
        assert_eq!(cursor.peek_prev().unwrap().unwrap().0.value(), 22);
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    let expected: Vec<(u64, u64)> = [10, 20, 21, 22, 25, 29, 30]
        .map(|i| (i, i))
        .into_iter()
        .collect();
    assert_u64_table_contents(&db, &expected);
}

#[test]
fn insert_after_unordered_keys_rejected() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        for i in [10u64, 30] {
            table.insert(i, &i).unwrap();
        }
        let mut cursor = table.lower_bound_mut(Bound::Included(&30)).unwrap();
        // The gap is between 10 and 30.
        assert!(matches!(
            cursor.insert_after(10, &10),
            Err(StorageError::UnorderedKey)
        ));
        assert!(matches!(
            cursor.insert_after(30, &30),
            Err(StorageError::UnorderedKey)
        ));
        cursor.insert_after(25, &25).unwrap();
        // Later inserts must stay strictly below the pending one.
        assert!(matches!(
            cursor.insert_after(25, &25),
            Err(StorageError::UnorderedKey)
        ));
        assert!(matches!(
            cursor.insert_after(27, &27),
            Err(StorageError::UnorderedKey)
        ));
        assert!(matches!(
            cursor.insert_before(26, &26),
            Err(StorageError::UnorderedKey)
        ));
        cursor.insert_after(20, &20).unwrap();
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    assert_u64_table_contents(&db, &[(10, 10), (20, 20), (25, 25), (30, 30)]);
}
