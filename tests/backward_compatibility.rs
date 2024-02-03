use redb::{ReadableTable, ReadableTableMetadata};

const ELEMENTS: usize = 3;

trait TestData: redb::Value + redb1::RedbValue {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS];

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS];
}

impl TestData for u8 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u16 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u32 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u64 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u128 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for i8 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i16 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i32 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i64 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i128 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for f32 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [f32::NAN, f32::INFINITY, f32::MIN_POSITIVE]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [f32::NAN, f32::INFINITY, f32::MIN_POSITIVE]
    }
}

impl TestData for f64 {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [f64::MIN, f64::NEG_INFINITY, f64::MAX]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [f64::MIN, f64::NEG_INFINITY, f64::MAX]
    }
}

impl TestData for () {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [(), (), ()]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [(), (), ()]
    }
}

impl TestData for &'static str {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        ["hello", "world1", "hi"]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        ["hello", "world1", "hi"]
    }
}

impl TestData for &'static [u8] {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [b"test", b"bytes", b"now"]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [b"test", b"bytes", b"now"]
    }
}

impl TestData for &'static [u8; 5] {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [b"test1", b"bytes", b"now12"]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [b"test1", b"bytes", b"now12"]
    }
}

impl TestData for Option<u64> {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [None, Some(0), Some(7)]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
        [None, Some(0), Some(7)]
    }
}

impl TestData for (u64, &'static str) {
    fn gen1() -> [<Self as redb1::RedbValue>::SelfType<'static>; ELEMENTS] {
        [(0, "hi"), (1, "bye"), (2, "byte")]
    }

    fn gen() -> [<Self as redb::Value>::SelfType<'static>; ELEMENTS] {
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

fn test_helper<K: TestData + redb::Key + redb1::RedbKey + 'static, V: TestData + 'static>() {
    let tmpfile = create_tempfile();
    let db = redb1::Database::create(tmpfile.path()).unwrap();
    let table_def: redb1::TableDefinition<K, V> = redb1::TableDefinition::new("table");
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        for i in 0..ELEMENTS {
            table.insert(&K::gen1()[i], &V::gen1()[i]).unwrap();
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
}

#[test]
fn mixed_width() {
    test_helper::<u8, &[u8]>();
    test_helper::<&[u8; 5], &str>();
}
