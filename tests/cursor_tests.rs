#![cfg(feature = "experimental_cursor")]

use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
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
