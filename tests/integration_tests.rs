use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use redb::Database;

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
    let mut table = db.open_table(b"x").unwrap();

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
    let table = db.open_table(b"x").unwrap();

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
