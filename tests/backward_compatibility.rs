// The deprecated ReadOnlyTable and ReadOnlyMultimapTable accessors are exercised throughout
// these tests; they remain covered until they are removed.
#![allow(deprecated)]

use redb::{ReadableDatabase, ReadableTableMetadata};

const ELEMENTS: usize = 3;

// A user-defined value type whose type name deliberately collides with the built-in `u32` and
// whose width and encoding match it. It implements both the current and the redb 2.6 `Value`
// trait so the same table can be written by one version and read by the other.
#[derive(Debug)]
#[allow(dead_code)]
struct CollidingUserType;

impl redb::Value for CollidingUserType {
    type SelfType<'a> = u32;
    type AsBytes<'a> = [u8; 4];

    fn fixed_width() -> Option<usize> {
        Some(4)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> u32
    where
        Self: 'a,
    {
        u32::from_le_bytes(data.try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a u32) -> [u8; 4]
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("u32")
    }
}

impl redb2_6::Value for CollidingUserType {
    type SelfType<'a> = u32;
    type AsBytes<'a> = [u8; 4];

    fn fixed_width() -> Option<usize> {
        Some(4)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> u32
    where
        Self: 'a,
    {
        u32::from_le_bytes(data.try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a u32) -> [u8; 4]
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> redb2_6::TypeName {
        redb2_6::TypeName::new("u32")
    }
}

trait TestData: redb::Value + redb2_6::Value {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a;

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a;
}

impl TestData for u8 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u16 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u32 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u64 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for u128 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [0, 1, 2]
    }
}

impl TestData for i8 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i16 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i32 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i64 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for i128 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [-1, 1, 2]
    }
}

impl TestData for f32 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [f32::NAN, f32::INFINITY, f32::MIN_POSITIVE]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [f32::NAN, f32::INFINITY, f32::MIN_POSITIVE]
    }
}

impl TestData for f64 {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [f64::MIN, f64::NEG_INFINITY, f64::MAX]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [f64::MIN, f64::NEG_INFINITY, f64::MAX]
    }
}

impl TestData for () {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [(), (), ()]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [(), (), ()]
    }
}

impl TestData for &'static str {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        ["hello", "world1", "hi"]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        ["hello", "world1", "hi"]
    }
}

impl TestData for &'static [u8] {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [b"test", b"bytes", b"now"]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [b"test", b"bytes", b"now"]
    }
}

impl TestData for &'static [u8; 5] {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [b"test1", b"bytes", b"now12"]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [b"test1", b"bytes", b"now12"]
    }
}

impl TestData for [&str; 3] {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a,
    {
        [
            ["test1", "hi", "world"],
            ["test2", "hi", "world"],
            ["test3", "hi", "world"],
        ]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS]
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
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [[1, 2, 3], [3, 2, 1], [300, 200, 100]]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [[1, 2, 3], [3, 2, 1], [300, 200, 100]]
    }
}

impl TestData for Vec<&str> {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS]
    where
        Self: 'a,
    {
        [
            vec!["test1", "hi", "world"],
            vec!["test2", "hi", "world"],
            vec!["test3", "hi", "world"],
        ]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS]
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
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [None, Some(0), Some(7)]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [None, Some(0), Some(7)]
    }
}

impl TestData for (u64, &'static str) {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [(0, "hi"), (1, "bye"), (2, "byte")]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [(0, "hi"), (1, "bye"), (2, "byte")]
    }
}

impl TestData for (u64, u32) {
    fn make_data_v2_6<'a>() -> [<Self as redb2_6::Value>::SelfType<'a>; ELEMENTS] {
        [(0, 3), (1, 4), (2, 5)]
    }

    fn make_data<'a>() -> [<Self as redb::Value>::SelfType<'a>; ELEMENTS] {
        [(0, 3), (1, 4), (2, 5)]
    }
}

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn test_helper<K: TestData + redb::Key + redb2_6::Key + 'static, V: TestData + 'static>() {
    {
        let tmpfile = create_tempfile();
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(tmpfile.path())
            .unwrap();
        let table_def: redb2_6::TableDefinition<K, V> = redb2_6::TableDefinition::new("table");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(table_def).unwrap();
            for i in 0..ELEMENTS {
                table
                    .insert(&K::make_data_v2_6()[i], &V::make_data_v2_6()[i])
                    .unwrap();
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
            let result = table.get(&K::make_data()[i]).unwrap().unwrap();
            let value = result.value();
            let bytes = <V as redb::Value>::as_bytes(&value);
            let expected = &V::make_data()[i];
            let expected_bytes = <V as redb::Value>::as_bytes(expected);
            assert_eq!(bytes.as_ref(), expected_bytes.as_ref());
        }
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
    test_helper::<(u64, u32), &str>();
    test_helper::<[&str; 3], [u128; 3]>();
    test_helper::<u64, Vec<&str>>();
}

#[test]
fn mixed_width() {
    test_helper::<u8, &[u8]>();
    test_helper::<&[u8; 5], &str>();
}

// Invariant: a persistent savepoint created by redb 2.6 (file format v3) must remain fully
// usable in the current version: restoring and deleting it must leave the allocated-pages
// bookkeeping referencing only allocated pages, so check_integrity() reports a clean database.
#[test]
fn restore_and_delete_redb2_6_persistent_savepoint() {
    let table_def: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("table");
    let table_def_26: redb2_6::TableDefinition<u64, &[u8]> = redb2_6::TableDefinition::new("table");
    fn value_for(i: u64, len: usize) -> Vec<u8> {
        vec![(i % 250) as u8 + 1; len]
    }

    let tmpfile = create_tempfile();
    let savepoint_id;
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(tmpfile.path())
            .unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_def_26).unwrap();
            for i in 0..100u64 {
                table.insert(i, value_for(i, 100).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        savepoint_id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();

        // Modify the data after the savepoint (still in redb 2.6)
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_def_26).unwrap();
            for i in 0..50u64 {
                table.remove(i).unwrap();
            }
            for i in 100..200u64 {
                table.insert(i, value_for(i, 100).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let mut db = redb::Database::open(tmpfile.path()).unwrap();
    let ids: Vec<u64> = {
        let txn = db.begin_write().unwrap();
        let ids = txn.list_persistent_savepoints().unwrap().collect();
        txn.abort().unwrap();
        ids
    };
    assert_eq!(ids, vec![savepoint_id]);

    let mut txn = db.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    drop(savepoint);
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    assert!(txn.delete_persistent_savepoint(savepoint_id).unwrap());
    txn.commit().unwrap();

    // Must not panic
    assert!(db.check_integrity().unwrap());

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(table_def).unwrap();
    assert_eq!(table.len().unwrap(), 100);
    for i in 0..100u64 {
        assert_eq!(
            table.get(i).unwrap().unwrap().value(),
            value_for(i, 100).as_slice()
        );
    }
}

// A composite (`Option<T>`) of a user-defined type written by redb 2.6 stored its type name with
// the `Internal` classification. The current version bubbles the inner user-defined classification
// up to the composite, so opening such a table must succeed via the legacy `Internal` spelling.
#[test]
fn composite_of_user_type_created_by_redb2_6_still_opens() {
    let tmpfile = create_tempfile();
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(tmpfile.path())
            .unwrap();
        let def: redb2_6::TableDefinition<u64, Option<CollidingUserType>> =
            redb2_6::TableDefinition::new("table");
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(def).unwrap();
            table.insert(&1u64, &Some(7u32)).unwrap();
            table.insert(&2u64, &None).unwrap();
        }
        txn.commit().unwrap();
    }

    let db = redb::Database::open(tmpfile.path()).unwrap();
    let def: redb::TableDefinition<u64, Option<CollidingUserType>> =
        redb::TableDefinition::new("table");
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(def).unwrap();
    assert_eq!(table.len().unwrap(), 2);
    assert_eq!(table.get(&1u64).unwrap().unwrap().value(), Some(7u32));
    assert_eq!(table.get(&2u64).unwrap().unwrap().value(), None);
}

// redb 2.6 wrote `Option<u32>` and `Option<user "u32">` with the same stored type name:
// `Internal + Option<u32>`. Current redb accepts that legacy spelling for built-in composites so
// old built-in tables remain readable, which means this already-ambiguous legacy table also opens
// under the built-in spelling.
#[test]
fn legacy_colliding_user_composite_can_still_open_as_builtin() {
    let tmpfile = create_tempfile();
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(tmpfile.path())
            .unwrap();
        let def: redb2_6::TableDefinition<u64, Option<CollidingUserType>> =
            redb2_6::TableDefinition::new("table");
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(def).unwrap();
            table.insert(&1u64, &Some(7u32)).unwrap();
            table.insert(&2u64, &None).unwrap();
        }
        txn.commit().unwrap();
    }

    let db = redb::Database::open(tmpfile.path()).unwrap();
    let def: redb::TableDefinition<u64, Option<u32>> = redb::TableDefinition::new("table");
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(def).unwrap();
    assert_eq!(table.len().unwrap(), 2);
    assert_eq!(table.get(&1u64).unwrap().unwrap().value(), Some(7u32));
    assert_eq!(table.get(&2u64).unwrap().unwrap().value(), None);
}

// The bug this guards against: a composite of a user-defined type must never silently alias the
// built-in composite with the same name string. Opening an `Option<user "u32">` table under the
// built-in `Option<u32>` must now fail with a type mismatch rather than misinterpreting the data.
#[test]
fn composite_of_user_type_does_not_alias_builtin() {
    let tmpfile = create_tempfile();
    let db = redb::Database::create(tmpfile.path()).unwrap();
    {
        let def: redb::TableDefinition<u64, Option<CollidingUserType>> =
            redb::TableDefinition::new("table");
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(def).unwrap();
            table.insert(&1u64, &Some(7u32)).unwrap();
        }
        txn.commit().unwrap();
    }

    let builtin_def: redb::TableDefinition<u64, Option<u32>> = redb::TableDefinition::new("table");
    let txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.open_table(builtin_def),
        Err(redb::TableError::TableTypeMismatch { .. })
    ));
    txn.abort().unwrap();
}

// Built-in composites use the new Internal3 classification, so the legacy compatibility rule for
// a user composite must not allow it to open a newly created built-in composite in reverse.
#[test]
fn builtin_composite_does_not_alias_user_type() {
    let tmpfile = create_tempfile();
    let db = redb::Database::create(tmpfile.path()).unwrap();
    {
        let def: redb::TableDefinition<u64, Option<u32>> = redb::TableDefinition::new("table");
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(def).unwrap();
            table.insert(&1u64, &Some(7u32)).unwrap();
        }
        txn.commit().unwrap();
    }

    let user_def: redb::TableDefinition<u64, Option<CollidingUserType>> =
        redb::TableDefinition::new("table");
    let txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.open_table(user_def),
        Err(redb::TableError::TableTypeMismatch { .. })
    ));
    txn.abort().unwrap();
}
