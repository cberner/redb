use std::any::type_name;
use std::cmp::Ordering;
use std::fmt::Debug;

use bincode::{deserialize, serialize};
use redb::{Database, Error, Key, Range, TableDefinition, TypeName, Value};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct SomeKey {
    foo: String,
    bar: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct SomeValue {
    foo: [f64; 3],
    bar: bool,
}

const TABLE: TableDefinition<Bincode<SomeKey>, Bincode<SomeValue>> =
    TableDefinition::new("my_data");

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

    let db = Database::create("bincode_keys.redb")?;
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
    T: Debug + Serialize + for<'a> Deserialize<'a>,
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
        deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        serialize(value).unwrap()
    }

    fn type_name() -> TypeName {
        TypeName::new(&format!("Bincode<{}>", type_name::<T>()))
    }
}

impl<T> Key for Bincode<T>
where
    T: Debug + Serialize + DeserializeOwned + Ord,
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}
