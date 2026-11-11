# redb-derive

Derive macros for [redb](https://github.com/cberner/redb), a simple, portable, high-performance,
ACID, embedded key-value store.

`#[derive(Value)]` implements `redb::Value` for a struct whose fields all implement it, and
`#[derive(Key)]` does the same for `redb::Key`. A table's key type needs both.

```rust
use redb::{Database, Error, ReadableDatabase, TableDefinition};

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
    assert_eq!(table.get_owned(&key)?.unwrap().value(), 0);

    Ok(())
}
```

If redb is a renamed dependency, name it with `#[redb(crate = "my_redb")]` on the struct.

## License

Licensed under either of

* [Apache License, Version 2.0](LICENSE-APACHE)
* [MIT License](LICENSE-MIT)

at your option.
