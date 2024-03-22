use redb::{DatabaseError, ReadableTableMetadata};
use redb1::ReadableTable as ReadableTable1;

const ELEMENTS: usize = 3;

trait TestData: redb::Value + redb2::Value {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a;

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a;
}

impl TestData for u8 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u16 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u32 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u64 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u128 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for i8 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i16 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i32 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i64 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i128 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for f32 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [f32::NAN, f32::INFINITY, f32::MIN_POSITIVE]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [f32::NAN, f32::INFINITY, f32::MIN_POSITIVE]
    }
}

impl TestData for f64 {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [f64::MIN, f64::NEG_INFINITY, f64::MAX]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [f64::MIN, f64::NEG_INFINITY, f64::MAX]
    }
}

impl TestData for () {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [(), (), ()]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [(), (), ()]
    }
}

impl TestData for &'static str {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        ["hello", "world1", "hi"]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        ["hello", "world1", "hi"]
    }
}

impl TestData for &'static [u8] {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [b"test", b"bytes", b"now"]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [b"test", b"bytes", b"now"]
    }
}

impl TestData for &'static [u8; 5] {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [b"test1", b"bytes", b"now12"]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [b"test1", b"bytes", b"now12"]
    }
}

impl TestData for [&str; 3] {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a,
    {
        [
            ["test1", "hi", "world"],
            ["test2", "hi", "world"],
            ["test3", "hi", "world"],
        ]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a,
    {
        [
            ["test1", "hi", "world"],
            ["test2", "hi", "world"],
            ["test3", "hi", "world"],
        ]
    }
}

impl TestData for [u128; 3] {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [[1, 2, 3], [3, 2, 1], [300, 200, 100]]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [[1, 2, 3], [3, 2, 1], [300, 200, 100]]
    }
}

impl TestData for Vec<&str> {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a,
    {
        [
            vec!["test1", "hi", "world"],
            vec!["test2", "hi", "world"],
            vec!["test3", "hi", "world"],
        ]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a,
    {
        [
            vec!["test1", "hi", "world"],
            vec!["test2", "hi", "world"],
            vec!["test3", "hi", "world"],
        ]
    }
}

impl TestData for Option<u64> {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [None, Some(0), Some(7)]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [None, Some(0), Some(7)]
    }
}

impl TestData for (u64, &'static str) {
    fn gen2<'a>() -> [<Self as redb2::Value>::SelfType<'a>; ELEMENTS] {
        [(0, "hi"), (1, "bye"), (2, "byte")]
    }

    fn gen<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [(0, "hi"), (1, "bye"), (2, "byte")]
    }
}

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn test_helper<K: TestData + redb::Key + redb2::Key + 'static, V: TestData + 'static>() {
    let tmpfile = create_tempfile();
    let db = redb2::Database::create(tmpfile.path()).unwrap();
    let table_def: redb2::TableDefinition<K, V> = redb2::TableDefinition::new("table");
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        for i in 0..ELEMENTS {
            table.insert(&K::gen2()[i], &V::gen2()[i]).unwrap();
        }
    }
    write_txn.commit().unwrap();
    drop(db);

    let db = redb::Database::open(tmpfile.path()).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table_def: redb::TableDefinition<K, V> = redb::TableDefinition::new("table");
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(table.len().unwrap(), ELEMENTS as u64);
    for i in 0..ELEMENTS {
        let result = table.get(&K::gen()[i]).unwrap().unwrap();
        let value = result.value();
        let bytes = <V as redb::Value>::as_bytes(&value);
        let expected = &V::gen()[i];
        let expected_bytes = <V as redb::Value>::as_bytes(expected);
        assert_eq!(bytes.as_ref(), expected_bytes.as_ref());
    }
}

#[test]
fn primitive_types() {
    test_helper::<u8, u8>();
    test_helper::<u16, u16>();
    test_helper::<u32, u32>();
    test_helper::<u64, u64>();
    test_helper::<u128, u128>();
    test_helper::<i8, i8>();
    test_helper::<i16, i16>();
    test_helper::<i32, i32>();
    test_helper::<i64, i64>();
    test_helper::<i128, i128>();
    test_helper::<i128, f32>();
    test_helper::<i128, f64>();
    test_helper::<&str, &str>();
    test_helper::<u8, ()>();
}

#[test]
fn container_types() {
    test_helper::<&[u8], &[u8]>();
    test_helper::<&[u8; 5], &[u8; 5]>();
    test_helper::<u64, Option<u64>>();
    test_helper::<(u64, &str), &str>();
    test_helper::<[&str; 3], [u128; 3]>();
    test_helper::<u64, Vec<&str>>();
}

#[test]
fn mixed_width() {
    test_helper::<u8, &[u8]>();
    test_helper::<&[u8; 5], &str>();
}

#[test]
fn upgrade_v1_to_v2() {
    let tmpfile1 = create_tempfile();
    let tmpfile2 = create_tempfile();
    let table_def1: redb1::TableDefinition<u64, u64> = redb1::TableDefinition::new("my_data");
    let db = redb1::Database::create(tmpfile1.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def1).unwrap();
        table.insert(0, 0).unwrap();
    }
    write_txn.commit().unwrap();
    drop(db);

    let table_def2: redb::TableDefinition<u64, u64> = redb::TableDefinition::new("my_data");
    match redb::Database::create(tmpfile1.path()).err().unwrap() {
        DatabaseError::UpgradeRequired(_) => {
            let db1 = redb1::Database::create(tmpfile1.path()).unwrap();
            let db2 = redb::Database::create(tmpfile2.path()).unwrap();
            let read_txn = db1.begin_read().unwrap();
            let table1 = read_txn.open_table(table_def1).unwrap();
            let write_txn = db2.begin_write().unwrap();
            {
                let mut table2 = write_txn.open_table(table_def2).unwrap();
                for r in table1.iter().unwrap() {
                    let (k, v) = r.unwrap();
                    table2.insert(k.value(), v.value()).unwrap();
                }
            }
            write_txn.commit().unwrap();
        }
        _ => unreachable!(),
    };

    let db = redb::Database::open(tmpfile2.path()).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def2).unwrap();
    assert_eq!(table.get(0).unwrap().unwrap().value(), 0);
}
