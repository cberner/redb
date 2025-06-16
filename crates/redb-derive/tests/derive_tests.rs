use redb::{Database, Key, ReadableDatabase, TableDefinition, Value};
use redb_derive::{Key, Value};
use std::fmt::Debug;
use tempfile::NamedTempFile;

fn create_tempfile() -> NamedTempFile {
    if cfg!(target_os = "wasi") {
        NamedTempFile::new_in("/tmp").unwrap()
    } else {
        NamedTempFile::new().unwrap()
    }
}

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SimpleStruct {
    id: u32,
    name: String,
}

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TupleStruct0();

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TupleStruct1(u64);

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct TupleStruct2(u64, bool);

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ZeroField {}

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SingleField {
    value: i32,
}

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ComplexStruct<'inner, 'inner2> {
    tuple_field: (u8, u16, u32),
    array_field: [(u8, Option<u16>); 2],
    reference: &'inner str,
    reference2: &'inner2 str,
}

#[derive(Value, Debug, PartialEq)]
struct UnitStruct;

fn test_key_helper<K: Key + 'static>(key: &<K as Value>::SelfType<'_>) {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let table_def: TableDefinition<K, u32> = TableDefinition::new("test");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(key, 1).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    let retrieved = table.get(key).unwrap().unwrap();
    let retrieved_value = retrieved.value();
    assert_eq!(retrieved_value, 1);
}

fn test_value_helper<V: Value + 'static>(
    value: <V as Value>::SelfType<'_>,
    expected_type_name: &str,
) where
    for<'x> <V as Value>::SelfType<'x>: PartialEq,
{
    let type_name = V::type_name();
    assert_eq!(type_name.name(), expected_type_name);

    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();
    let table_def: TableDefinition<u32, V> = TableDefinition::new("test");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(1, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    let retrieved = table.get(1).unwrap().unwrap();
    // Due to the lifetimes of SelfType we can't compare the values themselves, so instead compare
    // the serialized representation
    let retrieved_value = retrieved.value();
    let expected_bytes = V::as_bytes(&value);
    let bytes = V::as_bytes(&retrieved_value);
    assert_eq!(expected_bytes.as_ref(), bytes.as_ref());
}

#[test]
fn test_key_ordering() {
    let first = SimpleStruct {
        id: 1,
        name: "a".to_string(),
    };
    let second = SimpleStruct {
        id: 2,
        name: "a".to_string(),
    };
    let third = SimpleStruct {
        id: 2,
        name: "b".to_string(),
    };
    let fourth = SimpleStruct {
        id: 3,
        name: "a".to_string(),
    };

    let first_bytes = SimpleStruct::as_bytes(&first);
    let second_bytes = SimpleStruct::as_bytes(&second);
    let third_bytes = SimpleStruct::as_bytes(&third);
    let fourth_bytes = SimpleStruct::as_bytes(&fourth);

    assert_eq!(
        SimpleStruct::compare(&first_bytes, &second_bytes),
        first.cmp(&second)
    );
    assert_eq!(
        SimpleStruct::compare(&second_bytes, &third_bytes),
        second.cmp(&third)
    );
    assert_eq!(
        SimpleStruct::compare(&third_bytes, &fourth_bytes),
        third.cmp(&fourth)
    );
}

#[test]
fn test_simple_struct() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };
    let bytes = SimpleStruct::as_bytes(&original);
    let (id, name) = <(u32, String)>::from_bytes(&bytes);
    assert_eq!(id, original.id);
    assert_eq!(name, original.name);

    test_key_helper::<SimpleStruct>(&original);
    test_value_helper::<SimpleStruct>(original, "SimpleStruct {id: u32, name: String}");
}

#[test]
fn test_unit_struct() {
    let original = UnitStruct;
    let bytes = UnitStruct::as_bytes(&original);
    <()>::from_bytes(&bytes);
    test_value_helper::<UnitStruct>(original, "UnitStruct");
}

#[test]
fn test_tuple_struct0() {
    let original = TupleStruct0();
    let bytes = TupleStruct0::as_bytes(&original);
    <()>::from_bytes(&bytes);
    test_key_helper::<TupleStruct0>(&original);
    test_value_helper::<TupleStruct0>(original, "TupleStruct0()");
}

#[test]
fn test_tuple_struct1() {
    let original = TupleStruct1(123456789);
    let bytes = TupleStruct1::as_bytes(&original);
    let (x,) = <(u64,)>::from_bytes(&bytes);
    assert_eq!(x, original.0);
    test_key_helper::<TupleStruct1>(&original);
    test_value_helper::<TupleStruct1>(original, "TupleStruct1(u64)");
}

#[test]
fn test_tuple_struct2() {
    let original = TupleStruct2(123456789, true);
    let bytes = TupleStruct2::as_bytes(&original);
    let (x, y) = <(u64, bool)>::from_bytes(&bytes);
    assert_eq!(x, original.0);
    assert_eq!(y, original.1);
    test_key_helper::<TupleStruct2>(&original);
    test_value_helper::<TupleStruct2>(original, "TupleStruct2(u64, bool)");
}

#[test]
fn test_zero_fields() {
    let original = ZeroField {};
    let bytes = ZeroField::as_bytes(&original);
    <()>::from_bytes(&bytes);
    test_key_helper::<ZeroField>(&original);
    test_value_helper::<ZeroField>(original, "ZeroField {}");
}

#[test]
fn test_single_field() {
    let original = SingleField { value: -42 };
    let bytes = SingleField::as_bytes(&original);
    let value = <i32>::from_bytes(&bytes);
    assert_eq!(value, original.value);
    test_key_helper::<SingleField>(&original);
    test_value_helper::<SingleField>(original, "SingleField {value: i32}");
}

#[test]
fn test_complex_struct() {
    let original = ComplexStruct {
        tuple_field: (1, 2, 3),
        array_field: [(4, Some(5)), (6, None)],
        reference: "hello",
        reference2: "world",
    };
    let bytes = ComplexStruct::as_bytes(&original);
    let (tuple_field, array_field, reference, reference2) =
        <((u8, u16, u32), [(u8, Option<u16>); 2], &str, &str)>::from_bytes(&bytes);
    assert_eq!(tuple_field, original.tuple_field);
    assert_eq!(array_field, original.array_field);
    assert_eq!(reference, original.reference);
    assert_eq!(reference2, original.reference2);

    let expected_name = "ComplexStruct {tuple_field: (u8,u16,u32), array_field: [(u8,Option<u16>);2], reference: &str, reference2: &str}";
    test_key_helper::<ComplexStruct>(&original);
    test_value_helper::<ComplexStruct>(original, expected_name);
}
