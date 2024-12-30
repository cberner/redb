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
redb is undergoing active development, and should be considered beta quality. The file format is stable,
but redb has not been widely deployed in production systems (at least to my knowledge).

## Features
* Zero-copy, thread-safe, `BTreeMap` based API
* Fully ACID-compliant transactions
* MVCC support for concurrent readers & writer, without blocking
* Crash-safe by default
* Savepoints and rollbacks

## Development
To run all the tests and benchmarks a few extra dependencies are required:
* `cargo install cargo-deny --locked`
* `cargo install cargo-fuzz --locked`
* `apt install libclang-dev`

## Benchmarks
redb has similar performance to other top embedded key-value stores such as lmdb and rocksdb

|                           | redb       | lmdb       | rocksdb        | sled       | sanakirja |
|---------------------------|------------|------------|----------------|------------|-----------|
| bulk load                 | 2454ms     | **1168ms** | 5187ms         | 5560ms     | 1209ms    |
| individual writes         | **223ms**  | 418ms      | 695ms          | 825ms      | 393ms     |
| batch writes              | 2912ms     | 2155ms     | **1017ms**     | 1705ms     | 2914ms    |
| len()                     | 0ms        | **0ms**    | 265ms          | 470ms      | 63ms      |
| random reads              | 835ms      | **600ms**  | 2441ms         | 1571ms     | 871ms     |
| random reads              | 808ms      | **599ms**  | 2466ms         | 1458ms     | 859ms     |
| random range reads        | 2275ms     | **1186ms** | 5031ms         | 4660ms     | 1418ms    |
| random range reads        | 2280ms     | **1190ms** | 4755ms         | 4735ms     | 1456ms    |
| random reads (4 threads)  | 331ms      | **156ms**  | 658ms          | 483ms      | 282ms     |
| random reads (8 threads)  | 176ms      | **80ms**   | 345ms          | 265ms      | 452ms     |
| random reads (16 threads) | 112ms      | **49ms**   | 252ms          | 165ms      | 1564ms    |
| random reads (32 threads) | 95ms       | **42ms**   | 204ms          | 145ms      | 4654ms    |
| removals                  | 1774ms     | **822ms**  | 1959ms         | 2298ms     | 1161ms    |
| compaction                | 970ms      | N/A        | **779ms**      | N/A        | N/A       |
| size after bench          | 311.23 MiB | 582.22 MiB | **106.26 MiB** | 458.51 MiB | 4.00 GiB  |

Source code for benchmark [here](./benches/lmdb_benchmark.rs). Results collected on a Ryzen 5900X with Samsung 980 PRO NVMe.

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
