#[allow(unused_imports)]
use redb::ReadableTable;
use redb::{Database, Key, ReadableDatabase, TableDefinition, TableError, TypeName, Value};
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

// A field type whose inherent methods share names with the `Value` trait methods but lie.
// The derived code must resolve the trait methods, never these.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Inherent(u32);

#[allow(dead_code)]
impl Inherent {
    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes(_data: &[u8]) -> Inherent {
        Inherent(u32::MAX)
    }

    fn as_bytes(&self) -> Vec<u8> {
        vec![0xAB]
    }

    fn type_name() -> TypeName {
        TypeName::new("bogus")
    }
}

impl Value for Inherent {
    type SelfType<'a> = Inherent;
    type AsBytes<'a> = [u8; 4];

    fn fixed_width() -> Option<usize> {
        Some(4)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Inherent
    where
        Self: 'a,
    {
        Inherent(u32::from_le_bytes(data.try_into().unwrap()))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Inherent) -> [u8; 4]
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("Inherent")
    }
}

impl Key for Inherent {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        <Inherent as Value>::from_bytes(data1).cmp(&<Inherent as Value>::from_bytes(data2))
    }
}

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InherentHolder {
    a: Inherent,
    b: u64,
}

#[test]
fn test_inherent_methods_do_not_shadow_trait_methods() {
    // The inherent `fixed_width` claims variable width; the trait impl is 4 bytes fixed
    assert_eq!(<InherentHolder as Value>::fixed_width(), Some(12));

    let original = InherentHolder {
        a: Inherent(7),
        b: 9,
    };
    // Both fields are fixed width, so the encoding is the concatenation of the two fields with
    // no length prefixes. The inherent `as_bytes`/`from_bytes` would produce something else.
    let bytes = InherentHolder::as_bytes(&original);
    assert_eq!(&bytes[..], &[7u8, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0][..]);
    let decoded = InherentHolder::from_bytes(&bytes);
    assert_eq!(decoded, original);

    // The inherent `type_name` returns "bogus"
    assert_eq!(
        InherentHolder::type_name().name(),
        "InherentHolder {a: Inherent#user, b: u64}"
    );

    test_key_helper::<InherentHolder>(&original);
}

// An inherent `from_bytes` on the derived struct itself must not be picked up by the derived
// `Key::compare`
#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OwnFromBytes {
    value: u32,
}

#[allow(dead_code)]
impl OwnFromBytes {
    fn from_bytes(_data: &[u8]) -> OwnFromBytes {
        OwnFromBytes { value: 0 }
    }
}

#[test]
fn test_compare_uses_trait_from_bytes() {
    let small = OwnFromBytes { value: 1 };
    let large = OwnFromBytes { value: 2 };
    let small_bytes = OwnFromBytes::as_bytes(&small);
    let large_bytes = OwnFromBytes::as_bytes(&large);
    // The inherent `from_bytes` decodes everything to the same value, which would compare Equal
    assert_eq!(
        OwnFromBytes::compare(&small_bytes, &large_bytes),
        std::cmp::Ordering::Less
    );
}

// Mirrors `uuid::Uuid`: inherent `from_bytes`/`as_bytes` whose signatures are incompatible with
// the `Value` trait methods. Deriving on a struct with such a field used to fail to compile.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FakeUuid([u8; 16]);

#[allow(dead_code)]
impl FakeUuid {
    fn from_bytes(bytes: [u8; 16]) -> FakeUuid {
        FakeUuid(bytes)
    }

    const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Value for FakeUuid {
    type SelfType<'a> = FakeUuid;
    type AsBytes<'a> = &'a [u8; 16];

    fn fixed_width() -> Option<usize> {
        Some(16)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> FakeUuid
    where
        Self: 'a,
    {
        FakeUuid(data.try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a FakeUuid) -> &'a [u8; 16]
    where
        Self: 'b,
    {
        &value.0
    }

    fn type_name() -> TypeName {
        TypeName::new("FakeUuid")
    }
}

#[derive(Value, Debug, PartialEq)]
struct UuidHolder {
    id: FakeUuid,
    tag: String,
}

#[test]
fn test_field_with_conflicting_inherent_signatures() {
    let original = UuidHolder {
        id: FakeUuid([3; 16]),
        tag: "x".to_string(),
    };
    let bytes = UuidHolder::as_bytes(&original);
    let decoded = UuidHolder::from_bytes(&bytes);
    assert_eq!(decoded, original);
    assert_eq!(
        UuidHolder::type_name().name(),
        "UuidHolder {id: FakeUuid#user, tag: String}"
    );
}

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RealUuidHolder {
    id: uuid::Uuid,
    tag: String,
}

#[test]
fn test_uuid_field() {
    let original = RealUuidHolder {
        id: uuid::Uuid::new_v4(),
        tag: "x".to_string(),
    };
    let bytes = RealUuidHolder::as_bytes(&original);
    let decoded = RealUuidHolder::from_bytes(&bytes);
    assert_eq!(decoded, original);
    assert_eq!(
        RealUuidHolder::type_name().name(),
        "RealUuidHolder {id: uuid::Uuid#user, tag: String}"
    );
    test_key_helper::<RealUuidHolder>(&original);
}

// The generated code must not resolve any name at the derive site: shadow everything it could
// plausibly reference and check that it still compiles and round trips.
#[allow(
    non_camel_case_types,
    dead_code,
    unused_macros,
    unused_imports,
    missing_copy_implementations
)]
mod hostile {
    use redb_derive::{Key, Value};

    pub struct u8;
    pub struct u16;
    pub struct u32;
    pub struct u64;
    pub struct usize;
    pub struct Vec;
    pub struct String;
    pub struct Option;
    pub struct Some;
    pub struct None;
    pub struct Ok;
    pub struct Err;
    pub struct Result;
    pub struct Ord;
    pub struct Ordering;
    pub struct AsRef;
    pub struct TryFrom;
    pub struct TryInto;
    pub struct From;
    pub struct Into;
    pub mod std {}
    pub mod redb {}

    macro_rules! format {
        () => {};
    }
    macro_rules! stringify {
        () => {};
    }
    macro_rules! vec {
        () => {};
    }

    #[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Sneaky {
        pub a: ::std::primitive::u16,
        pub b: ::std::string::String,
        pub c: ::std::vec::Vec<::std::primitive::u8>,
    }
}

#[test]
fn test_shadowed_names_at_derive_site() {
    // Exercise all three length-prefix encodings of the variable width field b
    for len in [0usize, 253, 254, 65535, 65536] {
        let value = hostile::Sneaky {
            a: 5,
            b: "x".repeat(len),
            c: vec![9, 2, 3],
        };
        let bytes = hostile::Sneaky::as_bytes(&value);
        let decoded = hostile::Sneaky::from_bytes(&bytes);
        assert_eq!(decoded, value);
    }

    let original = hostile::Sneaky {
        a: 1,
        b: "hello".to_string(),
        c: vec![4, 5],
    };
    test_key_helper::<hostile::Sneaky>(&original);
}

// Shadowing cannot catch a reliance on trait methods the prelude brings into scope -- shadowing
// the `TryFrom` *name* does not undo the prelude's trait import for method resolution. Dropping
// the prelude entirely does, and also covers derive sites in crates of editions before 2021,
// whose preludes lack `TryFrom`.
#[no_implicit_prelude]
mod no_prelude {
    use ::redb_derive::{Key, Value};

    #[derive(
        Key,
        Value,
        ::std::fmt::Debug,
        ::std::cmp::PartialEq,
        ::std::cmp::Eq,
        ::std::cmp::PartialOrd,
        ::std::cmp::Ord,
    )]
    pub struct Bare {
        pub a: ::std::primitive::u16,
        pub b: ::std::string::String,
        pub c: ::std::vec::Vec<::std::primitive::u8>,
    }
}

#[test]
fn test_no_prelude_at_derive_site() {
    // Exercise all three length-prefix encodings of the variable width field b
    for len in [0usize, 253, 254, 65535, 65536] {
        let value = no_prelude::Bare {
            a: 5,
            b: "x".repeat(len),
            c: vec![9, 2, 3],
        };
        let bytes = no_prelude::Bare::as_bytes(&value);
        let decoded = no_prelude::Bare::from_bytes(&bytes);
        assert_eq!(decoded, value);
    }

    let original = no_prelude::Bare {
        a: 1,
        b: "hello".to_string(),
        c: vec![4, 5],
    };
    test_key_helper::<no_prelude::Bare>(&original);
}

mod name_collision {
    use redb::TypeName;

    // A user-defined type that shares its name with the built-in `String`, with an incompatible
    // serialized representation
    #[derive(Debug, PartialEq)]
    pub struct String {
        pub inner: u64,
    }

    impl redb::Value for String {
        type SelfType<'a> = String;
        type AsBytes<'a> = [u8; 8];

        fn fixed_width() -> Option<usize> {
            Some(8)
        }

        fn from_bytes<'a>(data: &'a [u8]) -> String
        where
            Self: 'a,
        {
            String {
                inner: u64::from_le_bytes(data.try_into().unwrap()),
            }
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a String) -> [u8; 8]
        where
            Self: 'b,
        {
            value.inner.to_le_bytes()
        }

        fn type_name() -> TypeName {
            TypeName::new("String")
        }
    }
}

#[derive(Value, Debug)]
struct BuiltinField {
    s: String,
}

mod user_field {
    use redb_derive::Value;

    #[derive(Value, Debug)]
    pub struct BuiltinField {
        pub s: super::name_collision::String,
    }
}

#[test]
fn test_user_defined_field_type_name_does_not_collide() {
    // Structs made only of built-in field types keep the exact pre-0.2 type identity, so their
    // existing tables still open
    assert_eq!(
        BuiltinField::type_name(),
        TypeName::new("BuiltinField {s: String}")
    );
    // A user-defined field type with a colliding name yields a distinct identity
    assert_eq!(
        user_field::BuiltinField::type_name().name(),
        "BuiltinField {s: String#user}"
    );
    assert_ne!(
        BuiltinField::type_name(),
        user_field::BuiltinField::type_name()
    );
}

#[test]
fn test_colliding_types_do_not_open_each_others_tables() {
    let file = create_tempfile();
    let db = Database::create(file.path()).unwrap();

    let def: TableDefinition<u32, BuiltinField> = TableDefinition::new("test");
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(def).unwrap();
        table
            .insert(
                1,
                BuiltinField {
                    s: "hello".to_string(),
                },
            )
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let colliding_def: TableDefinition<u32, user_field::BuiltinField> =
        TableDefinition::new("test");
    assert!(matches!(
        read_txn.open_table(colliding_def),
        Err(TableError::TableTypeMismatch { .. })
    ));
}

#[derive(Value, Debug)]
struct Nested {
    inner: SingleField,
}

#[derive(Value, Debug)]
struct WrappedUser {
    v: Vec<SingleField>,
    w: Vec<u32>,
}

#[derive(Value, Debug)]
struct TupleNested(SingleField, u32);

#[test]
fn test_nested_user_types_are_marked() {
    assert_eq!(
        Nested::type_name().name(),
        "Nested {inner: SingleField {value: i32}#user}"
    );
    // Composites wrapping a user type are user-defined; composites of built-ins are not
    assert_eq!(
        WrappedUser::type_name().name(),
        "WrappedUser {v: Vec<SingleField {value: i32}>#user, w: Vec<u32>}"
    );
    assert_eq!(
        TupleNested::type_name().name(),
        "TupleNested(SingleField {value: i32}#user, u32)"
    );
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
