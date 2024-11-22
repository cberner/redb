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
| bulk load                 | 2330ms     | **1122ms** | 6343ms         | 5557ms     | 1145ms    |
| individual writes         | **226ms**  | 413ms      | 714ms          | 828ms      | 393ms     |
| batch writes              | 2973ms     | 1924ms     | **1131ms**     | 1839ms     | 2882ms    |
| len()                     | **0ms**    | **0ms**    | 257ms          | 401ms      | 64ms      |
| random reads              | 811ms      | **580ms**  | 2325ms         | 1567ms     | 837ms     |
| random reads              | 780ms      | **578ms**  | 2329ms         | 1550ms     | 820ms     |
| random range reads        | 2376ms     | **1186ms** | 4512ms         | 4534ms     | 1372ms    |
| random range reads        | 2359ms     | **1197ms** | 4448ms         | 4488ms     | 1366ms    |
| random reads (4 threads)  | 329ms      | **154ms**  | 644ms          | 479ms      | 342ms     |
| random reads (8 threads)  | 173ms      | **77ms**   | 326ms          | 255ms      | 440ms     |
| random reads (16 threads) | 109ms      | **46ms**   | 237ms          | 165ms      | 1584ms    |
| random reads (32 threads) | 90ms       | **41ms**   | 180ms          | 136ms      | 4686ms    |
| removals                  | 1737ms     | **795ms**  | 2660ms         | 2341ms     | 1138ms    |
| compaction                | 963ms      | N/A        | N/A            | N/A        | N/A       |
| size after bench          | 311.23 MiB | 582.22 MiB | **206.39 MiB** | 454.01 MiB | 4.00 GiB  |

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
