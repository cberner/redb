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

    let db = unsafe { Database::open(tmpfile.path()).unwrap() };
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
    let db = unsafe { Database::open(tmpfile.path()).unwrap() };
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
fn tree_balance() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::open(tmpfile.path()).unwrap() };
    let mut table: Table<[u8], [u8]> = db.open_table(b"x").unwrap();

    let elements = 129usize;

    let mut txn = table.begin_write().unwrap();
    for i in (0..elements).rev() {
        txn.insert(&i.to_be_bytes(), b"").unwrap();
    }
    txn.commit().unwrap();

    let expected_height = (elements as f32).log2() as usize + 1;
    let height = db.stats().unwrap().tree_height();
    assert!(
        height <= expected_height,
        "height={} expected={}",
        height,
        expected_height
    );

    let reduce_to = 9;

    let mut txn = table.begin_write().unwrap();
    for i in 0..(elements - reduce_to) {
        txn.remove(&i.to_be_bytes()).unwrap();
    }
    txn.commit().unwrap();

    let expected_height = (reduce_to as f32).log2() as usize + 1;
    let height = db.stats().unwrap().tree_height();
    assert!(
        height <= expected_height,
        "height={} expected={}",
        height,
        expected_height
    );
}
