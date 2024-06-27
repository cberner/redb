//! Example of making any Borsh type to be Value or Key using BorshSchema

use core::cmp::Ordering;
use core::fmt::Debug;

use borsh::{schema::BorshSchemaContainer, BorshDeserialize, BorshSchema, BorshSerialize};
use redb::{Database, Error, Key, Range, TableDefinition, TypeName, Value};

#[derive(Debug, BorshDeserialize, BorshSerialize, BorshSchema, PartialEq, Eq, PartialOrd, Ord)]
struct SomeKey {
    foo: String,
    bar: i32,
}

#[derive(Debug, BorshDeserialize, BorshSerialize, BorshSchema, PartialEq)]
struct SomeValue {
    foo: [f64; 3],
    bar: bool,
}

const TABLE: TableDefinition<Borsh<SomeKey>, Borsh<SomeValue>> = TableDefinition::new("my_data");

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

    let db = Database::create("borsh.redb")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;

        table.insert(&some_key, &some_value).unwrap();
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;

    let mut iter: Range<Borsh<SomeKey>, Borsh<SomeValue>> = table.range(lower..upper).unwrap();
    assert_eq!(iter.next().unwrap().unwrap().1.value(), some_value);
    assert!(iter.next().is_none());

    Ok(())
}

/// Wrapper type to handle keys and values using Borsh serialization
#[derive(Debug)]
pub struct Borsh<T>(pub T);

impl<'c, T> Value for Borsh<T>
where
    T: Debug + BorshDeserialize + BorshSchema + BorshSerialize + 'c,
{
    type SelfType<'a> = T
    where
        Self: 'a;

    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn fixed_width<'a>() -> Option<usize> {
        let schema = BorshSchemaContainer::for_type::<Self::SelfType<'c>>();
        match schema
            .get_definition(schema.declaration())
            .expect("must have definition")
        {
            borsh::schema::Definition::Primitive(size) => Some(*size as usize),
            // here we can match on other branches recursiverly and output fixed with sequences, stuctures, enums and tuples.
            _ => None,
        }
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        BorshDeserialize::try_from_slice(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut writer = Vec::new();
        value.serialize(&mut writer).unwrap();
        writer
    }

    fn type_name() -> TypeName {
        redb::TypeName::new(BorshSchemaContainer::for_type::<Self::SelfType<'c>>().declaration())
    }
}

impl<'c, T> Key for Borsh<T>
where
    T: Debug + BorshDeserialize + Ord + BorshSerialize + BorshSchema + 'c,
{
    fn compare(mut data1: &[u8], mut data2: &[u8]) -> Ordering {
        Self::SelfType::<'c>::deserialize(&mut data1)
            .expect("valid data")
            .cmp(&Self::SelfType::<'c>::deserialize(&mut data2).expect("valid data"))
    }
}
