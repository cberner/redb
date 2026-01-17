use redb::{Database, Error, ReadableDatabase, TableDefinition, Value};

use redb_derive::{Key, Value};

#[derive(Debug, Key, Value, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct SomeKey {
    foo: String,
    bar: i32,
}

const TABLE: TableDefinition<SomeKey, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    let db = Database::create("derived_keys.redb")?;
    let key = SomeKey {
        foo: "example".to_string(),
        bar: 42,
    };
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert(key.clone(), 0)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get(&key)?.unwrap().value(), 0);

    Ok(())
}
