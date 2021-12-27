use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use redb::{Database, Table};

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
    let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let pairs = gen_data(100, 16, 20);

    let mut txn = table.begin_write().unwrap();
    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            txn.insert(key, value).unwrap();
        }
    }
    txn.non_durable_commit().unwrap();

    drop(table);
    // Check that cleanly closing the database persists the non-durable commit
    drop(db);
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    let txn = table.read_transaction().unwrap();
    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &txn.get(key).unwrap().unwrap();
            assert_eq!(result.as_ref(), value);
        }
    }
}

#[test]
fn persistence() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let pairs = gen_data(100, 16, 20);

    let mut txn = table.begin_write().unwrap();
    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            txn.insert(key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    drop(table);
    drop(db);
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    let txn = table.read_transaction().unwrap();
    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &txn.get(key).unwrap().unwrap();
            assert_eq!(result.as_ref(), value);
        }
    }
}

#[test]
fn free() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 512 * 1024;
    let db = unsafe { Database::open(tmpfile.path(), db_size).unwrap() };
    let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let free_pages = db.stats().unwrap().free_pages();

    let key = vec![0; 100];
    let value = vec![0; 1024];
    // Write 25% of db space each iteration
    let num_writes = db_size / 4 / (key.len() + value.len());
    // Make sure an internal index page is required
    assert!(num_writes > 64);

    let mut txn = table.begin_write().unwrap();
    {
        for _ in 0..num_writes {
            txn.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    {
        for _ in 0..num_writes {
            txn.remove(&key).unwrap();
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
    let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let mut key = vec![0; 1024];
    let value = vec![0; 1];
    let mut txn = table.begin_write().unwrap();
    {
        for i in 0..100 {
            key[0] = i;
            txn.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    {
        for i in 0..100 {
            key[0] = i;
            txn.remove(&key).unwrap();
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
    let mut table: Table<u64, u64> = db.open_table(b"x").unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.insert(&1, &1).unwrap();
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.insert(&6, &9).unwrap();
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.insert(&12, &10).unwrap();
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.insert(&18, &27).unwrap();
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.insert(&24, &33).unwrap();
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.insert(&30, &14).unwrap();
    txn.commit().unwrap();

    let mut txn = table.begin_write().unwrap();
    txn.remove(&30).unwrap();
    txn.commit().unwrap();

    let txn = table.read_transaction().unwrap();
    let v = txn.get(&6).unwrap().unwrap().to_value();
    assert_eq!(v, 9);
}

#[test]
fn non_durable_read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let mut write_txn = table.begin_write().unwrap();
    write_txn.insert(b"hello", b"world").unwrap();
    write_txn.non_durable_commit().unwrap();

    let read_txn = table.read_transaction().unwrap();
    assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());

    let mut write_txn = table.begin_write().unwrap();
    write_txn.remove(b"hello").unwrap();
    write_txn.insert(b"hello2", b"world2").unwrap();
    write_txn.insert(b"hello3", b"world3").unwrap();
    write_txn.non_durable_commit().unwrap();

    let read_txn2 = table.read_transaction().unwrap();
    assert!(read_txn2.get(b"hello").unwrap().is_none());
    assert_eq!(
        b"world2",
        read_txn2.get(b"hello2").unwrap().unwrap().as_ref()
    );
    assert_eq!(
        b"world3",
        read_txn2.get(b"hello3").unwrap().unwrap().as_ref()
    );
    assert_eq!(read_txn2.len().unwrap(), 2);

    assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
    assert!(read_txn.get(b"hello2").unwrap().is_none());
    assert!(read_txn.get(b"hello3").unwrap().is_none());
    assert_eq!(read_txn.len().unwrap(), 1);
}
