use std::fs;
use std::io::ErrorKind;
use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use redb::ReadableMultimapTable;
use redb::{
    Builder, Database, Durability, Error, MultimapTableDefinition, ReadableTable, TableDefinition,
    WriteStrategy,
};

const ELEMENTS: usize = 100;

const SLICE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");
const SLICE_TABLE2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("y");
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

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(SLICE_TABLE).unwrap();

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    {
        for i in &key_order {
            let (key, value) = &pairs[*i % pairs.len()];
            let result = table.get(key).unwrap().unwrap();
            assert_eq!(result, value);
        }
    }
}

fn test_persistence(durability: Durability) {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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
fn free() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();
    {
        let _table = txn.open_table(SLICE_TABLE).unwrap();
        let mut table = txn.open_table(SLICE_TABLE2).unwrap();
        table.insert(&[], &[]).unwrap();
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(SLICE_TABLE2).unwrap();
        table.remove(&[]).unwrap();
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let allocated_pages = txn.stats().unwrap().allocated_pages();

    let key = vec![0; 100];
    let value = vec![0u8; 1024];
    let target_db_size = 8 * 1024 * 1024;
    // Write 10% of db space each iteration
    let num_writes = target_db_size / 10 / (key.len() + value.len());
    // Make sure an internal index page is required
    assert!(num_writes > 64);

    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..num_writes {
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_le_bytes());
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
                    mut_key.extend_from_slice(&(*i as u64).to_le_bytes());
                    table.remove(&mut_key).unwrap();
                }
            }
            txn.commit().unwrap();
        }
    }

    // Extra commit to finalize the cleanup of the freed pages
    let txn = db.begin_write().unwrap();
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    assert_eq!(allocated_pages, txn.stats().unwrap().allocated_pages());
    txn.abort().unwrap();
}

#[test]
fn large_values() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();

    let mut key = vec![0u8; 1024];
    let value = vec![0u8; 2_000_000];
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

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();

    let mut key = vec![0u8; 1024];
    let value = vec![0u8; 1];
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
    let table_definition: TableDefinition<u64, &[u8]> = TableDefinition::new("x");
    let big_value = vec![0u8; 1024];

    let expected_size = 10 * 1024 * 1024;
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_definition).unwrap();
        table.insert(&0, &big_value).unwrap();
    }
    txn.commit().unwrap();

    let initial_file_size = tmpfile.as_file().metadata().unwrap().len();
    assert!(initial_file_size < (expected_size / 2) as u64);

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

    let db = unsafe { Builder::new().create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();

    let mut key = vec![0u8; page_size + 1];
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

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let tx = db.begin_write().unwrap();

    let a_def: TableDefinition<&[u8], &[u8]> = TableDefinition::new("a");
    let b_def: TableDefinition<&[u8], &[u8]> = TableDefinition::new("b");
    let c_def: TableDefinition<&[u8], &[u8]> = TableDefinition::new("c");

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

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(SLICE_TABLE).unwrap();
        let big_value = vec![0u8; 1000];
        for i in 0..20u8 {
            t.insert(&[i], &big_value).unwrap();
        }
        for i in (10..20u8).rev() {
            t.remove(&[i]).unwrap();
            for j in 0..i {
                assert!(t.get(&[j]).unwrap().is_some());
            }
        }
    }
    tx.commit().unwrap();
}

#[test]
fn regression7() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let big_value = vec![0u8; 4063];
        t.insert(&35723, &big_value).unwrap();
        t.remove(&145278).unwrap();
        t.remove(&145227).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None);
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 47];
        t.insert(&66469, &v).unwrap();
        let v = vec![0u8; 2414];
        t.insert(&146255, &v).unwrap();
        let v = vec![0u8; 159];
        t.insert(&153701, &v).unwrap();
        let v = vec![0u8; 1186];
        t.insert(&145227, &v).unwrap();
        let v = vec![0u8; 223];
        t.insert(&118749, &v).unwrap();

        t.remove(&145227).unwrap();

        let mut iter = t.range(138763..(138763 + 232359)).unwrap().rev();
        assert_eq!(iter.next().unwrap().0, 153701);
        assert_eq!(iter.next().unwrap().0, 146255);
        assert!(iter.next().is_none());
    }
    tx.commit().unwrap();
}

#[test]
fn regression8() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None);
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 1186];
        t.insert(&145227, &v).unwrap();
        let v = vec![0u8; 1585];
        t.insert(&565922, &v).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 2040];
        t.insert(&94937, &v).unwrap();
        let v = vec![0u8; 2058];
        t.insert(&130571, &v).unwrap();
        t.remove(&145227).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 947];
        t.insert(&118749, &v).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let t = tx.open_table(table_def).unwrap();
        let mut iter = t.range(118749..142650).unwrap();
        assert_eq!(iter.next().unwrap().0, 118749);
        assert_eq!(iter.next().unwrap().0, 130571);
        assert!(iter.next().is_none());
    }
    tx.commit().unwrap();
}

#[test]
fn regression9() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 118665];
        t.insert(&452, &v).unwrap();
        t.len().unwrap();
    }
    tx.commit().unwrap();
}

#[test]
fn regression10() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 1043];
        t.insert(&118749, &v).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 952];
        t.insert(&118757, &v).unwrap();
    }
    tx.abort().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let t = tx.open_table(table_def).unwrap();
        t.get(&829513).unwrap();
    }
    tx.abort().unwrap();
}

#[test]
fn regression11() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 1204];
        t.insert(&118749, &v).unwrap();
        let v = vec![0u8; 2062];
        t.insert(&153697, &v).unwrap();
        let v = vec![0u8; 2980];
        t.insert(&110557, &v).unwrap();
        let v = vec![0u8; 1999];
        t.insert(&677853, &v).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let v = vec![0u8; 691];
        t.insert(&103591, &v).unwrap();
        let v = vec![0u8; 952];
        t.insert(&118757, &v).unwrap();
    }
    tx.abort().unwrap();

    let tx = db.begin_write().unwrap();
    tx.commit().unwrap();
}

#[test]
// Test that for stale read bug when re-opening a table during a write
fn regression12() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        t.insert(&0, &0).unwrap();
        assert_eq!(t.get(&0).unwrap().unwrap(), 0);
        drop(t);

        let t2 = tx.open_table(table_def).unwrap();
        assert_eq!(t2.get(&0).unwrap().unwrap(), 0);
    }
    tx.commit().unwrap();
}

#[test]
fn regression13() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: MultimapTableDefinition<u64, &[u8]> = MultimapTableDefinition::new("x");

    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None);
    {
        let mut t = tx.open_multimap_table(table_def).unwrap();
        let value = vec![0; 1026];
        t.insert(&539717, &value).unwrap();
        let value = vec![0; 530];
        t.insert(&539717, &value).unwrap();
    }
    tx.abort().unwrap();
}

#[test]
fn regression14() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let table_def: MultimapTableDefinition<u64, &[u8]> = MultimapTableDefinition::new("x");

    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None);
    {
        let mut t = tx.open_multimap_table(table_def).unwrap();
        let value = vec![0; 1424];
        t.insert(&539749, &value).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None);
    {
        let mut t = tx.open_multimap_table(table_def).unwrap();
        let value = vec![0; 2230];
        t.insert(&776971, &value).unwrap();

        let mut iter = t.range(&514043..&(514043 + 514043)).unwrap().rev();
        {
            let (key, mut value_iter) = iter.next().unwrap();
            assert_eq!(key.to_value(), 776971);
            assert_eq!(value_iter.next().unwrap().to_value(), &[0; 2230]);
        }
        {
            let (key, mut value_iter) = iter.next().unwrap();
            assert_eq!(key.to_value(), 539749);
            assert_eq!(value_iter.next().unwrap().to_value(), &[0; 1424]);
        }
    }
    tx.abort().unwrap();
}

#[test]
fn regression17() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe {
        Database::builder()
            .set_write_strategy(WriteStrategy::Checksum)
            .create(tmpfile.path())
            .unwrap()
    };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let mut tx = db.begin_write().unwrap();
    tx.set_durability(Durability::None);
    {
        let mut t = tx.open_table(table_def).unwrap();
        let value = vec![0; 4578];
        t.insert(&671325, &value).unwrap();

        let mut value = t.insert_reserve(&723904, 2246).unwrap();
        value.as_mut().fill(0xFF);
    }
    tx.abort().unwrap();
}

#[test]
fn regression18() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe {
        Database::builder()
            .set_write_strategy(WriteStrategy::Checksum)
            .create(tmpfile.path())
            .unwrap()
    };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    let savepoint0 = tx.savepoint().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let mut value = t.insert_reserve(&118749, 817).unwrap();
        value.as_mut().fill(0xFF);
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let savepoint1 = tx.savepoint().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let mut value = t.insert_reserve(&65373, 1807).unwrap();
        value.as_mut().fill(0xFF);
    }
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    let savepoint2 = tx.savepoint().unwrap();

    tx.restore_savepoint(&savepoint2).unwrap();
    tx.commit().unwrap();

    drop(savepoint0);

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let mut value = t.insert_reserve(&118749, 2494).unwrap();
        value.as_mut().fill(0xFF);
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let savepoint4 = tx.savepoint().unwrap();
    tx.abort().unwrap();
    drop(savepoint1);

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let mut value = t.insert_reserve(&429469, 667).unwrap();
        value.as_mut().fill(0xFF);
        drop(value);
        let mut value = t.insert_reserve(&266845, 1614).unwrap();
        value.as_mut().fill(0xFF);
    }
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    tx.restore_savepoint(&savepoint4).unwrap();
    tx.commit().unwrap();

    drop(savepoint2);
    drop(savepoint4);
}

#[test]
fn regression19() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe {
        Database::builder()
            .set_write_strategy(WriteStrategy::Checksum)
            .create(tmpfile.path())
            .unwrap()
    };

    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let value = vec![0xFF; 100];
        t.insert(&1, &value).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    let savepoint0 = tx.savepoint().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let value = vec![0xFF; 101];
        t.insert(&1, &value).unwrap();
    }
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table_def).unwrap();
        let value = vec![0xFF; 102];
        t.insert(&1, &value).unwrap();
    }
    tx.commit().unwrap();

    let mut tx = db.begin_write().unwrap();
    tx.restore_savepoint(&savepoint0).unwrap();
    tx.commit().unwrap();

    let tx = db.begin_write().unwrap();
    tx.open_table(table_def).unwrap();
}

#[test]
fn change_invalidate_savepoint() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe {
        Database::builder()
            .set_write_strategy(WriteStrategy::Checksum)
            .create(tmpfile.path())
            .unwrap()
    };
    let tx = db.begin_write().unwrap();
    let savepoint = tx.savepoint().unwrap();
    tx.abort().unwrap();
    db.set_write_strategy(WriteStrategy::TwoPhase).unwrap();

    let mut tx = db.begin_write().unwrap();
    assert!(matches!(
        tx.restore_savepoint(&savepoint),
        Err(Error::InvalidSavepoint)
    ));
}

#[test]
fn create_open_mismatch() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe {
        Database::builder()
            .set_write_strategy(WriteStrategy::TwoPhase)
            .create(tmpfile.path())
            .unwrap()
    };
    drop(db);

    unsafe { Database::create(tmpfile.path()).unwrap() };

    unsafe { Database::builder().create(tmpfile.path()).unwrap() };
}

#[test]
fn twophase_open() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe {
        Database::builder()
            .set_write_strategy(WriteStrategy::TwoPhase)
            .create(tmpfile.path())
            .unwrap()
    };
    drop(db);
    unsafe {
        Database::open(tmpfile.path()).unwrap();
    }
}

#[test]
fn non_durable_read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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

    let total: u64 = table.range(1..=3).unwrap().map(|(_, v)| v).sum();
    assert_eq!(total, 6);
}

#[test]
fn range_query_reversed() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
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

    let mut iter = iter.rev();
    let (key, _) = iter.next().unwrap();
    assert_eq!(6, key);
    let (key, _) = iter.next().unwrap();
    assert_eq!(5, key);

    let mut iter = iter.rev();
    let (key, _) = iter.next().unwrap();
    assert_eq!(4, key);

    assert!(iter.next().is_none());
}

#[test]
fn alias_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

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
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let y_def: MultimapTableDefinition<&[u8], &[u8]> = MultimapTableDefinition::new("y");

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
fn dropped_write() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    drop(write_txn);
    let read_txn = db.begin_read().unwrap();
    let result = read_txn.open_table(SLICE_TABLE);
    assert!(matches!(result, Err(Error::TableDoesNotExist(_))));
}

#[test]
fn non_page_size_multiple() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();
    let key = vec![0u8; 1024];
    let value = vec![0u8; 1];
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
    fs::remove_file(tmpfile.path()).unwrap();
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
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

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

#[test]
fn tree_balance() {
    const EXPECTED_ORDER: usize = 9;
    fn expected_height(mut elements: usize) -> usize {
        // Root may have only 2 entries
        let mut height = 1;
        elements /= 2;

        // Leaves may have only a single entry
        height += 1;

        // Each internal node half-full, plus 1 to round up
        height += (elements as f32).log((EXPECTED_ORDER / 2) as f32) as usize + 1;

        height
    }

    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();

    // One for the last table id counter, and one for the "x" -> TableDefinition entry
    let num_internal_entries = 2;

    // Pages are 4kb, so use a key size such that 9 keys will fit
    let key_size = 410;
    let db = unsafe { Database::builder().create(tmpfile.path()).unwrap() };
    let txn = db.begin_write().unwrap();

    let elements = (EXPECTED_ORDER / 2).pow(2) - num_internal_entries;

    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in (0..elements).rev() {
            let mut key = vec![0u8; key_size];
            key[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            table.insert(&key, b"").unwrap();
        }
    }
    txn.commit().unwrap();

    let expected = expected_height(elements + num_internal_entries);
    let txn = db.begin_write().unwrap();
    let height = txn.stats().unwrap().tree_height();
    assert!(
        height <= expected,
        "height={} expected={}",
        height,
        expected
    );

    let reduce_to = EXPECTED_ORDER / 2 - num_internal_entries;
    {
        let mut table = txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..(elements - reduce_to) {
            let mut key = vec![0u8; key_size];
            key[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();

    let expected = expected_height(reduce_to + num_internal_entries);
    let txn = db.begin_write().unwrap();
    let height = txn.stats().unwrap().tree_height();
    txn.abort().unwrap();
    assert!(
        height <= expected,
        "height={} expected={}",
        height,
        expected
    );
}

#[test]
fn database_lock() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let result = unsafe { Database::create(tmpfile.path()) };
    assert!(result.is_ok());
    let result2 = unsafe { Database::open(tmpfile.path()) };
    assert!(
        matches!(result2, Err(Error::DatabaseAlreadyOpen)),
        "{:?}",
        result2
    );
    drop(result);
    let result = unsafe { Database::open(tmpfile.path()) };
    assert!(result.is_ok());
}

#[test]
fn savepoint() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let definition: TableDefinition<u32, &str> = TableDefinition::new("x");

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(definition).unwrap();
        table.insert(&0, "hello").unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let savepoint = txn.savepoint().unwrap();
    {
        let mut table = txn.open_table(definition).unwrap();
        table.remove(&0).unwrap();
    }
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    let savepoint2 = txn.savepoint().unwrap();

    txn.restore_savepoint(&savepoint).unwrap();

    assert!(matches!(
        txn.restore_savepoint(&savepoint2).err().unwrap(),
        Error::InvalidSavepoint
    ));
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(definition).unwrap();
    assert_eq!(table.get(&0).unwrap().unwrap(), "hello");

    // Test that savepoints can be used multiple times
    let mut txn = db.begin_write().unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();
}
