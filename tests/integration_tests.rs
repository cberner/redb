use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use redb::{
    Database, DatabaseBuilder, Error, MultimapTable, ReadOnlyMultimapTable, ReadOnlyTable,
    ReadableTable, Table,
};

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
fn mixed_durable_commit() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 129 * 4096;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();

    table.insert(&0, &0).unwrap();
    txn.non_durable_commit().unwrap();

    let txn = db.begin_write().unwrap();
    txn.commit().unwrap();
}

#[test]
fn non_durable_commit_persistence() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();

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
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = txn.open_table("x").unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &table.get(key).unwrap().unwrap();
            assert_eq!(result.to_value(), value);
        }
    }
}

#[test]
fn persistence() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();

    let pairs = gen_data(100, 16, 20);

    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            table.insert(key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    drop(db);
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = txn.open_table("x").unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = &table.get(key).unwrap().unwrap();
            assert_eq!(result.to_value(), value);
        }
    }
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
fn resize_db() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 16 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let mut i = 0u64;
    loop {
        let txn = db.begin_write().unwrap();
        let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
        // Fill the database
        match table.insert(&i, &i) {
            Ok(_) => {}
            Err(err) => match err {
                Error::OutOfSpace => {
                    txn.abort().unwrap();
                    break;
                }
                _ => unreachable!(),
            },
        }
        match txn.non_durable_commit() {
            Ok(_) => {}
            Err(err) => match err {
                Error::OutOfSpace => break,
                _ => unreachable!(),
            },
        };
        i += 1;
    }

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    let mut found = false;
    for _ in 0..999 {
        if matches!(table.insert(&i, &i), Err(Error::OutOfSpace)) {
            found = true;
            break;
        }
        i += 1;
    }
    assert!(found);
    txn.abort().unwrap();
    drop(db);

    unsafe {
        Database::resize(tmpfile.path(), db_size * 2).unwrap();
    }

    let db = unsafe { Database::create(tmpfile.path(), db_size * 2).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    assert!(table.insert(&i, &i).is_ok());
    txn.commit().unwrap();
}

#[test]
fn free() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 8 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let _table: Table<[u8], [u8]> = txn.open_table("x").unwrap();
    let _table: Table<[u8], [u8]> = txn.open_table("y").unwrap();

    txn.commit().unwrap();
    let free_pages = db.stats().unwrap().free_pages();

    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();

    let key = vec![0; 100];
    let value = vec![0; 1024];
    // Write 10% of db space each iteration
    let num_writes = db_size / 10 / (key.len() + value.len());
    // Make sure an internal index page is required
    assert!(num_writes > 64);

    {
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
            let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();
            for i in chunk {
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&(*i as u64).to_be_bytes());
                table.remove(&mut_key).unwrap();
            }
            txn.commit().unwrap();
        }
    }

    assert_eq!(free_pages, db.stats().unwrap().free_pages());
}

#[test]
fn large_keys() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024_1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();

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
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();
    {
        for i in 0..100 {
            key[0] = i;
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();
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
            .open(tmpfile.path(), db_size)
            .unwrap()
    };
    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();

    let mut key = vec![0; page_size + 1];
    let mut value = vec![0; page_size + 1];
    {
        for i in 0..elements {
            key[0] = i;
            value[0] = i;
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = txn.open_table("x").unwrap();
    for i in 0..elements {
        key[0] = i;
        value[0] = i;
        assert_eq!(&value, table.get(&key).unwrap().unwrap().to_value());
    }

    let txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();
    {
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
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();

    table.insert(&1, &1).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    table.insert(&6, &9).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    table.insert(&12, &10).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    table.insert(&18, &27).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    table.insert(&24, &33).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    table.insert(&30, &14).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = txn.open_table("x").unwrap();
    table.remove(&30).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u64, u64> = txn.open_table("x").unwrap();
    let v = table.get(&6).unwrap().unwrap().to_value();
    assert_eq!(v, 9);
}

#[test]
// Test for a bug in table creation code, where multiple tables could end up with the same id
fn regression2() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db_size = 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let tx = db.begin_write().unwrap();
    let _c: Table<[u8], [u8]> = tx.open_table("c").unwrap();
    let b: Table<[u8], [u8]> = tx.open_table("b").unwrap();
    let mut a: Table<[u8], [u8]> = tx.open_table("a").unwrap();
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
    let mut t: Table<[u8], [u8]> = tx.open_table("x").unwrap();
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
    tx.commit().unwrap();
}

#[test]
fn non_durable_read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();

    table.insert(b"hello", b"world").unwrap();
    write_txn.non_durable_commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let read_table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(
        b"world",
        read_table.get(b"hello").unwrap().unwrap().to_value()
    );

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.remove(b"hello").unwrap();
    table.insert(b"hello2", b"world2").unwrap();
    table.insert(b"hello3", b"world3").unwrap();
    write_txn.non_durable_commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let read_table2: ReadOnlyTable<[u8], [u8]> = read_txn2.open_table("x").unwrap();
    assert!(read_table2.get(b"hello").unwrap().is_none());
    assert_eq!(
        b"world2",
        read_table2.get(b"hello2").unwrap().unwrap().to_value()
    );
    assert_eq!(
        b"world3",
        read_table2.get(b"hello3").unwrap().unwrap().to_value()
    );
    assert_eq!(read_table2.len().unwrap(), 2);

    assert_eq!(
        b"world",
        read_table.get(b"hello").unwrap().unwrap().to_value()
    );
    assert!(read_table.get(b"hello2").unwrap().is_none());
    assert!(read_table.get(b"hello3").unwrap().is_none());
    assert_eq!(read_table.len().unwrap(), 1);
}

#[test]
fn range_query() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    for i in 0..10u8 {
        let key = vec![i];
        table.insert(&key, b"value").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    let start = vec![3u8];
    let end = vec![7u8];
    let mut iter = table.get_range(start..end).unwrap();
    for i in 3..7u8 {
        let (key, value) = iter.next().unwrap();
        assert_eq!(&[i], key);
        assert_eq!(b"value", value);
    }
    assert!(iter.next().is_none());
}

#[test]
fn range_query_reversed() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    for i in 0..10u8 {
        let key = vec![i];
        table.insert(&key, b"value").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    let start = vec![3u8];
    let end = vec![7u8];
    let mut iter = table
        .get_range_reversed(start.as_slice()..end.as_slice())
        .unwrap();
    for i in (3..7u8).rev() {
        let (key, value) = iter.next().unwrap();
        assert_eq!(&[i], key);
        assert_eq!(b"value", value);
    }
    assert!(iter.next().is_none());
}

#[test]
fn alias_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    let result: Result<Table<[u8], [u8]>, Error> = write_txn.open_table("x");
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

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    let mut multitable: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table("y").unwrap();
    multitable.insert(b"hello2", b"world2").unwrap();
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    assert!(write_txn.delete_table("x").unwrap());
    assert!(!write_txn.delete_table("x").unwrap());
    assert!(write_txn.delete_multimap_table("y").unwrap());
    assert!(!write_txn.delete_multimap_table("y").unwrap());
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let result: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table("x");
    assert!(result.is_err());
    let result: Result<ReadOnlyMultimapTable<[u8], [u8]>, Error> =
        read_txn.open_multimap_table("y");
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
    let mut table: Table<[u8], [u8]> = txn.open_table("x").unwrap();

    let key = vec![0; 1024];
    let value = vec![0; 1];
    table.insert(&key, &value).unwrap();
    txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(table.len().unwrap(), 1);
}
