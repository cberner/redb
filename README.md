# redb

![CI](https://github.com/cberner/redb/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/redb.svg)](https://crates.io/crates/redb)
[![Documentation](https://docs.rs/redb/badge.svg)](https://docs.rs/redb)
[![License](https://img.shields.io/crates/l/redb)](https://crates.io/crates/redb)
[![dependency status](https://deps.rs/repo/github/cberner/redb/status.svg)](https://deps.rs/repo/github/cberner/redb)

A simple, portable, high-performance, ACID, embedded key-value store.

redb is written in pure Rust and is loosely inspired by [lmdb](http://www.lmdb.tech/doc/). Data is stored in a collection
of mmap'ed, copy-on-write, B-trees. For more details, see the [design doc](docs/design.md)

```rust
use redb::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<str, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    let db = unsafe { Database::create("my_db.redb", 1024 * 1024)? };
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert("my_key", &123)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get("my_key")?.unwrap(), 123);

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

## Roadmap
The following features are planned before the 1.0 release
* Stable file format
* User-defined zero-copy types
* Further performance optimizations

## Benchmarks
redb is nearly as fast as lmdb, and faster than sled, on many benchmarks
```
+--------------------+--------+--------+--------+
|                    | redb   | lmdb   | sled   |
+===============================================+
| bulk load          | 1605ms | 1294ms | 4642ms |
|--------------------+--------+--------+--------|
| individual writes  | 516ms  | 411ms  | 527ms  |
|--------------------+--------+--------+--------|
| batch writes       | 7444ms | 3938ms | 1465ms |
|--------------------+--------+--------+--------|
| large writes       |  12s   |  11s   |  315s  |
|--------------------+--------+--------+--------|
| random reads       | 716ms  | 649ms  | 1552ms |
|--------------------+--------+--------+--------|
| removals           | 1282ms | 1012ms | 1911ms |
+--------------------+--------+--------+--------+
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
