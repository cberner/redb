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
