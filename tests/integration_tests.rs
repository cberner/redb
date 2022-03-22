use std::fs;
use std::io::ErrorKind;
use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use redb::{
    Database, DatabaseBuilder, Durability, Error, MultimapTableDefinition, ReadableTable,
    TableDefinition,
};

const ELEMENTS: usize = 100;

const SLICE_TABLE: TableDefinition<[u8], [u8]> = TableDefinition::new("x");
const SLICE_TABLE2: TableDefinition<[u8], [u8]> = TableDefinition::new("y");
const U64_TABLE: TableDefinition<u64, u64> = TableDefinition::new("u64");

/// Returns pairs of key, value
fn gen_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = vec![];

    for _ in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rand::thread_rng().gen()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rand::thread_rng().gen()).collect();
        pairs.push((key, value));
    }

    pairs
}

#[test]
fn mixed_durable_commit() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 129 * 4096;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let mut txn = db.begin_write().unwrap();
    txn.set_durability(Durability::None);
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&0, &0).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    txn.commit().unwrap();
}

#[test]
fn non_durable_commit_persistence() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let mut txn = db.begin_write().unwrap();
    txn.set_durability(Durability::None);
    let pairs = gen_data(100, 16, 20);
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            table.insert(key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    // Check that cleanly closing the database persists the non-durable commit
    drop(db);
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(SLICE_TABLE).unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &table.get(key).unwrap().unwrap();
            assert_eq!(result, value);
        }
    }
}

fn test_persistence(durability: Durability) {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let mut txn = db.begin_write().unwrap();
    txn.set_durability(durability);
    let pairs = gen_data(100, 16, 20);
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            table.insert(key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    drop(db);
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(SLICE_TABLE).unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &table.get(key).unwrap().unwrap();
            assert_eq!(result, value);
        }
    }
}

#[test]
fn eventual_persistence() {
    test_persistence(Durability::Eventual);
}

#[test]
fn immediate_persistence() {
    test_persistence(Durability::Immediate);
}

#[test]
fn change_db_size() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    drop(db);

    let db = unsafe { Database::create(tmpfile.path(), db_size * 2) };
    assert!(matches!(db.err().unwrap(), Error::DbSizeMismatch { .. }));
}

#[test]
fn free() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 8 * 1024 * 1024;
    let db = unsafe {
        Database::builder()
            .set_dynamic_growth(false)
            .create(tmpfile.path(), db_size)
            .unwrap()
    };
    let txn = db.begin_write().unwrap();
    {
        let _table = txn.open_table(SLICE_TABLE).unwrap();
        let _table = txn.open_table(SLICE_TABLE2).unwrap();
    }

    txn.commit().unwrap();
    let free_pages = db.stats().unwrap().free_pages();

    let txn = db.begin_write().unwrap();

    let key = vec![0; 100];
    let value = vec![0; 1024];
    // Write 10% of db space each iteration
    let num_writes = db_size / 10 / (key.len() + value.len());
    // Make sure an internal index page is required
    assert!(num_writes > 64);

    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..num_writes {
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_be_bytes());
            table.insert(&mut_key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    {
        let key_range: Vec<usize> = (0..num_writes).collect();
        // Delete in chunks to be sure that we don't run out of pages due to temp allocations
        for chunk in key_range.chunks(10) {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(SLICE_TABLE).unwrap();
                for i in chunk {
                    let mut mut_key = key.clone();
                    mut_key.extend_from_slice(&(*i as u64).to_be_bytes());
                    table.remove(&mut_key).unwrap();
                }
            }
            txn.commit().unwrap();
        }
    }

    assert_eq!(free_pages, db.stats().unwrap().free_pages());
}

#[test]
fn large_values() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 50 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();

    let mut key = vec![0; 1024];
    let value = vec![0; 2_000_000];
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..5 {
            key[0] = i;
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..5 {
            key[0] = i;
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();
}

#[test]
fn large_keys() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024_1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();

    let mut key = vec![0; 1024];
    let value = vec![0; 1];
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..100 {
            key[0] = i;
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..100 {
            key[0] = i;
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();
}

#[test]
fn dynamic_growth() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let table_definition: TableDefinition<u64, [u8]> = TableDefinition::new("x");
    let big_value = vec![0; 1024];

    let db_size = 10 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_definition).unwrap();
        table.insert(&0, &big_value).unwrap();
    }
    txn.commit().unwrap();

    let initial_file_size = tmpfile.as_file().metadata().unwrap().len();
    assert!(initial_file_size < (db_size / 2) as u64);

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_definition).unwrap();
        for i in 0..2048 {
            table.insert(&i, &big_value).unwrap();
        }
    }
    txn.commit().unwrap();

    let file_size = tmpfile.as_file().metadata().unwrap().len();

    assert!(file_size > initial_file_size);
}

#[test]
fn multi_page_kv() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let elements = 4;
    let page_size = 4096;

    let db_size = 1024_1024;
    let db = unsafe {
        DatabaseBuilder::new()
            .set_page_size(page_size)
            .create(tmpfile.path(), db_size)
            .unwrap()
    };
    let txn = db.begin_write().unwrap();

    let mut key = vec![0; page_size + 1];
    let mut value = vec![0; page_size + 1];
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..elements {
            key[0] = i;
            value[0] = i;
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(SLICE_TABLE).unwrap();
    for i in 0..elements {
        key[0] = i;
        value[0] = i;
        assert_eq!(&value, table.get(&key).unwrap().unwrap());
    }

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..elements {
            key[0] = i;
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();
}

#[test]
// Test for a bug in the deletion code, where deleting a key accidentally deleted other keys
fn regression() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&1, &1).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&6, &9).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&12, &10).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&18, &27).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&24, &33).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.insert(&30, &14).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(U64_TABLE).unwrap();
        table.remove(&30).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(U64_TABLE).unwrap();
    let v = table.get(&6).unwrap().unwrap();
    assert_eq!(v, 9);
}

#[test]
// Test for a bug in table creation code, where multiple tables could end up with the same id
fn regression2() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let tx = db.begin_write().unwrap();

    let a_def: TableDefinition<[u8], [u8]> = TableDefinition::new("a");
    let b_def: TableDefinition<[u8], [u8]> = TableDefinition::new("b");
    let c_def: TableDefinition<[u8], [u8]> = TableDefinition::new("c");

    let _c = tx.open_table(c_def).unwrap();
    let b = tx.open_table(b_def).unwrap();
    let mut a = tx.open_table(a_def).unwrap();
    a.insert(b"hi", b"1").unwrap();
    assert!(b.get(b"hi").unwrap().is_none());
}

#[test]
// Test for a bug in deletion code, where deletions could delete neighboring keys in a leaf,
// due to the partial leaf entries being dropped
fn regression3() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(SLICE_TABLE).unwrap();
        let big_value = vec![0u8; 1000];
        for i in 0..20 {
            t.insert(&[i], &big_value).unwrap();
        }
        for i in (10..20).rev() {
            t.remove(&[i]).unwrap();
            for j in 0..i {
                assert!(t.get(&[j]).unwrap().is_some());
            }
        }
    }
    tx.commit().unwrap();
}

#[test]
// Test for bug in growth code
fn regression4() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1507328;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(SLICE_TABLE).unwrap();
        let big_value = vec![0u8; 9134464];
        assert!(matches!(t.insert(&[0], &big_value), Err(Error::OutOfSpace)));
    }
    tx.commit().unwrap();
}

#[test]
fn regression5() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 387;
    let db = unsafe { Database::create(tmpfile.path(), db_size) };
    assert!(matches!(db, Err(Error::OutOfSpace)));
}

#[test]
fn non_durable_read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let mut write_txn = db.begin_write().unwrap();
    write_txn.set_durability(Durability::None);
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let read_table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", read_table.get(b"hello").unwrap().unwrap());

    let mut write_txn = db.begin_write().unwrap();
    write_txn.set_durability(Durability::None);
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.remove(b"hello").unwrap();
        table.insert(b"hello2", b"world2").unwrap();
        table.insert(b"hello3", b"world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let read_table2 = read_txn2.open_table(SLICE_TABLE).unwrap();
    assert!(read_table2.get(b"hello").unwrap().is_none());
    assert_eq!(b"world2", read_table2.get(b"hello2").unwrap().unwrap());
    assert_eq!(b"world3", read_table2.get(b"hello3").unwrap().unwrap());
    assert_eq!(read_table2.len().unwrap(), 2);

    assert_eq!(b"world", read_table.get(b"hello").unwrap().unwrap());
    assert!(read_table.get(b"hello2").unwrap().is_none());
    assert!(read_table.get(b"hello3").unwrap().is_none());
    assert_eq!(read_table.len().unwrap(), 1);
}

#[test]
fn range_query() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    let mut iter = table.range(3..7).unwrap();
    for i in 3..7u64 {
        let (key, value) = iter.next().unwrap();
        assert_eq!(i, key);
        assert_eq!(i, value);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range(3..=7).unwrap();
    for i in 3..=7u64 {
        let (key, value) = iter.next().unwrap();
        assert_eq!(i, key);
        assert_eq!(i, value);
    }
    assert!(iter.next().is_none());
}

#[test]
fn range_query_reversed() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    let mut iter = table.range(3..7).unwrap().rev();
    for i in (3..7u64).rev() {
        let (key, value) = iter.next().unwrap();
        assert_eq!(i, key);
        assert_eq!(i, value);
    }
    assert!(iter.next().is_none());

    // Test reversing multiple times
    let mut iter = table.range(3..7).unwrap();
    let (key, _) = iter.next().unwrap();
    assert_eq!(3, key);

    iter = iter.rev();
    let (key, _) = iter.next().unwrap();
    assert_eq!(6, key);
    let (key, _) = iter.next().unwrap();
    assert_eq!(5, key);

    iter = iter.rev();
    let (key, _) = iter.next().unwrap();
    assert_eq!(4, key);

    assert!(iter.next().is_none());
}

#[test]
fn alias_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let table = write_txn.open_table(SLICE_TABLE).unwrap();
    let result = write_txn.open_table(SLICE_TABLE);
    assert!(matches!(
        result.err().unwrap(),
        Error::TableAlreadyOpen(_, _)
    ));
    drop(table);
}

#[test]
fn delete_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let y_def: MultimapTableDefinition<[u8], [u8]> = MultimapTableDefinition::new("y");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        let mut multitable = write_txn.open_multimap_table(y_def).unwrap();
        multitable.insert(b"hello2", b"world2").unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    assert!(write_txn.delete_table(SLICE_TABLE).unwrap());
    assert!(!write_txn.delete_table(SLICE_TABLE).unwrap());
    assert!(write_txn.delete_multimap_table(y_def).unwrap());
    assert!(!write_txn.delete_multimap_table(y_def).unwrap());
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let result = read_txn.open_table(SLICE_TABLE);
    assert!(result.is_err());
    let result = read_txn.open_multimap_table(y_def);
    assert!(result.is_err());
}

#[test]
fn leaked_write() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    drop(write_txn);
    let result = db.begin_write();
    if let Err(Error::LeakedWriteTransaction(_message)) = result {
        // Good
    } else {
        panic!();
    }
}

#[test]
fn non_page_size_multiple() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024 * 1024 + 1;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let key = vec![0; 1024];
    let value = vec![0; 1];
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        table.insert(&key, &value).unwrap();
    }
    txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn does_not_exist() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    fs::remove_file(&tmpfile.path()).unwrap();
    let result = unsafe { Database::open(tmpfile.path()) };
    if let Err(Error::Io(e)) = result {
        assert!(matches!(e.kind(), ErrorKind::NotFound));
    } else {
        panic!();
    }

    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let result = unsafe { Database::open(tmpfile.path()) };
    if let Err(Error::Io(e)) = result {
        assert!(matches!(e.kind(), ErrorKind::InvalidData));
    } else {
        panic!();
    }
}

#[test]
fn wrong_types() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: TableDefinition<u32, u32> = TableDefinition::new("x");
    let wrong_definition: TableDefinition<u64, u64> = TableDefinition::new("x");

    let txn = db.begin_write().unwrap();
    txn.open_table(definition).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.open_table(wrong_definition),
        Err(Error::TableTypeMismatch(_))
    ));
    txn.abort().unwrap();

    let txn = db.begin_read().unwrap();
    txn.open_table(definition).unwrap();
    assert!(matches!(
        txn.open_table(wrong_definition),
        Err(Error::TableTypeMismatch(_))
    ));
}
