use redb::{Database, TableDefinition, Value};
use redb_derive::Value;
use std::fmt::Debug;
use tempfile::NamedTempFile;

fn create_tempfile() -> NamedTempFile {
    if cfg!(target_os = "wasi") {
        NamedTempFile::new_in("/tmp").unwrap()
    } else {
        NamedTempFile::new().unwrap()
    }
}

#[derive(Value, Debug, PartialEq)]
struct SimpleStruct {
    id: u32,
    name: String,
}

#[derive(Value, Debug, PartialEq)]
struct TupleStruct0();

#[derive(Value, Debug, PartialEq)]
struct TupleStruct1(u64);

#[derive(Value, Debug, PartialEq)]
struct TupleStruct2(u64, bool);

#[derive(Value, Debug, PartialEq)]
struct ZeroField {}

#[derive(Value, Debug, PartialEq)]
struct SingleField {
    value: i32,
}

#[derive(Value, Debug, PartialEq)]
struct ComplexStruct<'inner, 'inner2> {
    tuple_field: (u8, u16, u32),
    array_field: [(u8, Option<u16>); 2],
    reference: &'inner str,
    reference2: &'inner2 str,
}

#[derive(Value, Debug, PartialEq)]
struct UnitStruct;

fn test_helper<V: Value + 'static>(value: <V as Value>::SelfType<'_>, expected_type_name: &str)
where
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
fn test_simple_struct() {
    let original = SimpleStruct {
        id: 42,
        name: "test".to_string(),
    };
    let bytes = SimpleStruct::as_bytes(&original);
    let (id, name) = <(u32, String)>::from_bytes(&bytes);
    assert_eq!(id, original.id);
    assert_eq!(name, original.name);

    test_helper::<SimpleStruct>(original, "SimpleStruct {id: u32, name: String}");
}

#[test]
fn test_unit_struct() {
    let original = UnitStruct;
    let bytes = UnitStruct::as_bytes(&original);
    <()>::from_bytes(&bytes);
    test_helper::<UnitStruct>(original, "UnitStruct");
}

#[test]
fn test_tuple_struct0() {
    let original = TupleStruct0();
    let bytes = TupleStruct0::as_bytes(&original);
    <()>::from_bytes(&bytes);
    test_helper::<TupleStruct0>(original, "TupleStruct0()");
}

#[test]
fn test_tuple_struct1() {
    let original = TupleStruct1(123456789);
    let bytes = TupleStruct1::as_bytes(&original);
    let (x,) = <(u64,)>::from_bytes(&bytes);
    assert_eq!(x, original.0);
    test_helper::<TupleStruct1>(original, "TupleStruct1(u64)");
}

#[test]
fn test_tuple_struct2() {
    let original = TupleStruct2(123456789, true);
    let bytes = TupleStruct2::as_bytes(&original);
    let (x, y) = <(u64, bool)>::from_bytes(&bytes);
    assert_eq!(x, original.0);
    assert_eq!(y, original.1);
    test_helper::<TupleStruct2>(original, "TupleStruct2(u64, bool)");
}

#[test]
fn test_zero_fields() {
    let original = ZeroField {};
    let bytes = ZeroField::as_bytes(&original);
    <()>::from_bytes(&bytes);
    test_helper::<ZeroField>(original, "ZeroField {}");
}

#[test]
fn test_single_field() {
    let original = SingleField { value: -42 };
    let bytes = SingleField::as_bytes(&original);
    let value = <i32>::from_bytes(&bytes);
    assert_eq!(value, original.value);
    test_helper::<SingleField>(original, "SingleField {value: i32}");
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
    test_helper::<ComplexStruct>(original, expected_name);
}
