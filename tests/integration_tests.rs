use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use redb::{Database, ReadOnlyTable, Table};

const ELEMENTS: usize = 100;

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
fn non_durable_commit_persistence() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();

    let pairs = gen_data(100, 16, 20);

    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            table.insert(key, value).unwrap();
        }
    }
    txn.non_durable_commit().unwrap();

    // Check that cleanly closing the database persists the non-durable commit
    drop(db);
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = txn.open_table(b"x").unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &table.get(key).unwrap().unwrap();
            assert_eq!(result.as_ref(), value);
        }
    }
}

#[test]
fn persistence() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();

    let pairs = gen_data(100, 16, 20);

    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            table.insert(key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    drop(db);
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = txn.open_table(b"x").unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &table.get(key).unwrap().unwrap();
            assert_eq!(result.as_ref(), value);
        }
    }
}

#[test]
fn free() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 512 * 1024;
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();

    let free_pages = db.stats().unwrap().free_pages();

    let key = vec![0; 100];
    let value = vec![0; 1024];
    // Write 25% of db space each iteration
    let num_writes = db_size / 4 / (key.len() + value.len());
    // Make sure an internal index page is required
    assert!(num_writes > 64);

    {
        for _ in 0..num_writes {
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();
    {
        for _ in 0..num_writes {
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();

    assert_eq!(free_pages, db.stats().unwrap().free_pages());
}

#[test]
fn large_keys() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024_1024;
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();

    let mut key = vec![0; 1024];
    let value = vec![0; 1];
    {
        for i in 0..100 {
            key[0] = i;
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table(b"x").unwrap();
    {
        for i in 0..100 {
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
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();

    table.insert(&1, &1).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();
    table.insert(&6, &9).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();
    table.insert(&12, &10).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();
    table.insert(&18, &27).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();
    table.insert(&24, &33).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();
    table.insert(&30, &14).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table(b"x").unwrap();
    table.remove(&30).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u64, u64> = txn.open_table(b"x").unwrap();
    let v = table.get(&6).unwrap().unwrap().to_value();
    assert_eq!(v, 9);
}

#[test]
fn non_durable_read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();

    table.insert(b"hello", b"world").unwrap();
    write_txn.non_durable_commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let read_table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(
        b"world",
        read_table.get(b"hello").unwrap().unwrap().as_ref()
    );

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.remove(b"hello").unwrap();
    table.insert(b"hello2", b"world2").unwrap();
    table.insert(b"hello3", b"world3").unwrap();
    write_txn.non_durable_commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let read_table2: ReadOnlyTable<[u8], [u8]> = read_txn2.open_table(b"x").unwrap();
    assert!(read_table2.get(b"hello").unwrap().is_none());
    assert_eq!(
        b"world2",
        read_table2.get(b"hello2").unwrap().unwrap().as_ref()
    );
    assert_eq!(
        b"world3",
        read_table2.get(b"hello3").unwrap().unwrap().as_ref()
    );
    assert_eq!(read_table2.len().unwrap(), 2);

    assert_eq!(
        b"world",
        read_table.get(b"hello").unwrap().unwrap().as_ref()
    );
    assert!(read_table.get(b"hello2").unwrap().is_none());
    assert!(read_table.get(b"hello3").unwrap().is_none());
    assert_eq!(read_table.len().unwrap(), 1);
}
