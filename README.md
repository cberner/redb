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

## Benchmarks
redb is nearly as fast as lmdb, and faster than sled, on many benchmarks
```
+---------------------------+--------+--------+---------+--------+-----------+
|                           | redb   | lmdb   | rocksdb | sled   | sanakirja |
+============================================================================+
| bulk load                 | 4285ms | 1031ms | 5387ms  | 5030ms | 1000ms    |
|---------------------------+--------+--------+---------+--------+-----------|
| individual writes         | 229ms  | 430ms  | 678ms   | 650ms  | 411ms     |
|---------------------------+--------+--------+---------+--------+-----------|
| batch writes              | 1948ms | 1684ms | 1027ms  | 1508ms | 2071ms    |
|---------------------------+--------+--------+---------+--------+-----------|
| random reads              | 948ms  | 538ms  | 3112ms  | 1325ms | 622ms     |
|---------------------------+--------+--------+---------+--------+-----------|
| random range reads        | 2496ms | 960ms  | 5726ms  | 4493ms | 1030ms    |
|---------------------------+--------+--------+---------+--------+-----------|
| random reads (4 threads)  | 350ms  | 142ms  | 1216ms  | 410ms  | 277ms     |
|---------------------------+--------+--------+---------+--------+-----------|
| random reads (8 threads)  | 193ms  | 77ms   | 660ms   | 218ms  | 844ms     |
|---------------------------+--------+--------+---------+--------+-----------|
| random reads (16 threads) | 136ms  | 45ms   | 468ms   | 138ms  | 4596ms    |
|---------------------------+--------+--------+---------+--------+-----------|
| random reads (32 threads) | 118ms  | 38ms   | 409ms   | 119ms  | 4457ms    |
|---------------------------+--------+--------+---------+--------+-----------|
| removals                  | 4371ms | 697ms  | 2533ms  | 1943ms | 1065ms    |
+---------------------------+--------+--------+---------+--------+-----------+
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
