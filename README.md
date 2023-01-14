# redb

![CI](https://github.com/cberner/redb/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/redb.svg)](https://crates.io/crates/redb)
[![Documentation](https://docs.rs/redb/badge.svg)](https://docs.rs/redb)
[![License](https://img.shields.io/crates/l/redb)](https://crates.io/crates/redb)
[![dependency status](https://deps.rs/repo/github/cberner/redb/status.svg)](https://deps.rs/repo/github/cberner/redb)

A simple, portable, high-performance, ACID, embedded key-value store.

redb is written in pure Rust and is loosely inspired by [lmdb](http://www.lmdb.tech/doc/). Data is stored in a collection
of copy-on-write B-trees. For more details, see the [design doc](docs/design.md)

```rust
use redb::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    let db = Database::create("my_db.redb")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert("my_key", &123)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get("my_key")?.unwrap().value(), 123);

    Ok(())
}
```

## Status
redb is undergoing active development, and should be considered beta quality. It may eat your data, and does not
have any guarantees of file format stability :)

## Features
* Zero-copy, thread-safe, `BTreeMap` based API
* Fully ACID-compliant transactions
* MVCC support for concurrent readers & writer, without blocking
* Crash-safe by default
* Savepoints and rollbacks

## Roadmap
The following features are planned before the 1.0 release
* Stable file format
* User-defined zero-copy types

## Benchmarks
redb is nearly as fast as lmdb, and faster than sled, on many benchmarks
```
+--------------------+--------------+------------+--------+---------+---------+
|                    | redb (1PC+C) | redb (2PC) | lmdb   | rocksdb | sled    |
+=============================================================================+
| bulk load          | 1770ms       | 1370ms     | 976ms  | 5263ms  | 4534ms  |
|--------------------+--------------+------------+--------+---------+---------|
| individual writes  | 227ms        | 381ms      | 388ms  | 701ms   | 642ms   |
|--------------------+--------------+------------+--------+---------+---------|
| batch writes       | 2346ms       | 2533ms     | 2136ms | 992ms   | 1395ms  |
|--------------------+--------------+------------+--------+---------+---------|
| large writes       | 8805ms       | 6532ms     | 7793ms | 21475ms | 37736ms |
|--------------------+--------------+------------+--------+---------+---------|
| random reads       | 734ms        | 734ms      | 642ms  | 5814ms  | 1514ms  |
|--------------------+--------------+------------+--------+---------+---------|
| random range reads | 832ms        | 834ms      | 712ms  | 6074ms  | 1826ms  |
|--------------------+--------------+------------+--------+---------+---------|
| removals           | 1281ms       | 1149ms     | 676ms  | 2481ms  | 1792ms  |
+--------------------+--------------+------------+--------+---------+---------+
```

## License

Licensed under either of

* [Apache License, Version 2.0](LICENSE-APACHE)
* [MIT License](LICENSE-MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
