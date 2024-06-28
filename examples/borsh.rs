//! Example of making any Borsh type to be Value or Key using BorshSchema

use core::fmt::Debug;
use core::{cmp::Ordering, ops::RangeInclusive};

use borsh::{
    schema::{BorshSchemaContainer, Declaration},
    BorshDeserialize, BorshSchema, BorshSerialize,
};
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

    /// Width of a fixed type, or None for variable width.
    /// Uses Borsh schema to determine if the type is fixed width.
    fn fixed_width<'a>() -> Option<usize> {
        let schema = BorshSchemaContainer::for_type::<Self::SelfType<'c>>();
        is_fixed_width(&schema, schema.declaration())
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

    /// Globally unique identifier for this type.
    /// Returns the type name Borsh schema declaration for `T`.
    fn type_name() -> TypeName {
        let schema = BorshSchemaContainer::for_type::<Self::SelfType<'c>>();
        redb::TypeName::new(schema.declaration())
    }
}

fn is_fixed_width(schema: &BorshSchemaContainer, declaration: &Declaration) -> Option<usize> {
    use borsh::schema::Definition::*;
    match schema
        .get_definition(declaration)
        .expect("must have definition")
    {
        Primitive(size) => Some(*size as usize),
        Struct { fields } => match fields {
            borsh::schema::Fields::NamedFields(fields) => {
                are_fixed_width(schema, fields.iter().map(|(_, field)| field))
            }
            borsh::schema::Fields::UnnamedFields(fields) => are_fixed_width(schema, fields),
            borsh::schema::Fields::Empty => Some(0),
        },
        // fixed sequence
        Sequence {
            length_width,
            length_range,
            elements,
        } if is_fixed_width_sequence(length_range, length_width) => {
            let fixed_witdh = is_fixed_width(schema, elements);
            fixed_witdh.map(|width| width * *length_range.end() as usize)
        }
        Tuple { elements } => are_fixed_width(schema, elements),
        Enum {
            tag_width,
            variants,
        } => {
            let max_width =
                are_fixed_width(schema, variants.iter().map(|(_, _, element)| element))?;
            Some(max_width + *tag_width as usize)
        }
        _ => None,
    }
}

fn is_fixed_width_sequence(length_range: &RangeInclusive<u64>, length_width: &u8) -> bool {
    length_range.end() == length_range.start() && *length_width == 0
}

fn are_fixed_width<'a>(
    schema: &BorshSchemaContainer,
    elements: impl IntoIterator<Item = &'a String>,
) -> Option<usize> {
    let mut width = 0;
    for element in elements {
        if let Some(element_width) = is_fixed_width(schema, element) {
            width += element_width;
        } else {
            return None;
        }
    }
    Some(width)
}

impl<'c, T> Key for Borsh<T>
where
    T: Debug + BorshDeserialize + Ord + BorshSerialize + BorshSchema + 'c,
{
    /// Panics in case of deserialization error.
    /// Data integrity should be guaranteed by the database
    /// and data migration should be handled properly.
    fn compare(mut data1: &[u8], mut data2: &[u8]) -> Ordering {
        Self::SelfType::<'c>::deserialize(&mut data1)
            .expect("valid data")
            .cmp(&Self::SelfType::<'c>::deserialize(&mut data2).expect("valid data"))
    }
}
