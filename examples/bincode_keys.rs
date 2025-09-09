use std::any::type_name;
use std::cmp::Ordering;
use std::fmt::Debug;

use bincode::{Decode, Encode, decode_from_slice, encode_to_vec};
use redbx::{Database, Error, Key, Range, ReadableDatabase, TableDefinition, TypeName, Value};

#[derive(Debug, Decode, Encode, PartialEq, Eq, PartialOrd, Ord)]
struct SomeKey {
    foo: String,
    bar: i32,
}

#[derive(Debug, Decode, Encode, PartialEq)]
struct SomeValue {
    foo: [f64; 3],
    bar: bool,
}

const TABLE: TableDefinition<Bincode<SomeKey>, Bincode<SomeValue>> =
    TableDefinition::new("my_data");

#[allow(clippy::result_large_err)]
fn main() -> Result<(), Error> {
    let some_key = SomeKey {
        foo: "hello world".to_string(),
        bar: 42,
    };
    let some_value = SomeValue {
        foo: [1., 2., 3.],
        bar: true,
    };
    let lower = SomeKey {
        foo: "a".to_string(),
        bar: 42,
    };
    let upper = SomeKey {
        foo: "z".to_string(),
        bar: 42,
    };

    let db = Database::create("bincode_keys.redb", "password")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;

        table.insert(&some_key, &some_value).unwrap();
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;

    let mut iter: Range<Bincode<SomeKey>, Bincode<SomeValue>> = table.range(lower..upper).unwrap();
    assert_eq!(iter.next().unwrap().unwrap().1.value(), some_value);
    assert!(iter.next().is_none());

    Ok(())
}

/// Wrapper type to handle keys and values using bincode serialization
#[derive(Debug)]
pub struct Bincode<T>(pub T);

impl<T> Value for Bincode<T>
where
    T: Debug + Encode + Decode<()>,
{
    type SelfType<'a>
        = T
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        decode_from_slice(data, bincode::config::standard())
            .unwrap()
            .0
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        encode_to_vec(value, bincode::config::standard()).unwrap()
    }

    fn type_name() -> TypeName {
        TypeName::new(&format!("Bincode<{}>", type_name::<T>()))
    }
}

impl<T> Key for Bincode<T>
where
    T: Debug + Decode<()> + Encode + Ord,
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}
