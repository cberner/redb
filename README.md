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

|                           | redb       | lmdb       | rocksdb        | sled       | sanakirja   |
|---------------------------|------------|------------|----------------|------------|-------------|
| bulk load                 | 2689ms     | 1247ms     | 5330ms         | 5892ms     | **1187ms**  |
| individual writes         | **226ms**  | 419ms      | 703ms          | 816ms      | 398ms       |
| batch writes              | 2522ms     | 2070ms     | **1047ms**     | 1867ms     | 2776ms      |
| len()                     | **0ms**    | **0ms**    | 304ms          | 444ms      | 64ms        |
| random reads              | 860ms      | **624ms**  | 2432ms         | 1596ms     | 875ms       |
| random reads              | 866ms      | **624ms**  | 2464ms         | 1588ms     | 842ms       |
| random range reads        | 2347ms     | **1179ms** | 4436ms         | 4907ms     | 1367ms      |
| random range reads        | 2322ms     | **1207ms** | 4465ms         | 4732ms     | 1373ms      |
| random reads (4 threads)  | 337ms      | **158ms**  | 732ms          | 488ms      | 349ms       |
| random reads (8 threads)  | 185ms      | **81ms**   | 433ms          | 259ms      | 277ms       |
| random reads (16 threads) | 116ms      | **49ms**   | 238ms          | 165ms      | 1708ms      |
| random reads (32 threads) | 100ms      | **44ms**   | 203ms          | 142ms      | 4714ms      |
| removals                  | 1889ms     | **803ms**  | 2038ms         | 2371ms     | 1170ms      |
| uncompacted size          | 1.00 GiB   | 582.22 MiB | **206.38 MiB** | 457.01 MiB | 4.00 GiB    |
| compacted size            | 311.23 MiB | 284.46 MiB | **106.26 MiB** | N/A        | N/A         |

Source code for benchmark [here](./crates/redb-bench/benches/lmdb_benchmark.rs). Results collected on a Ryzen 5900X with Samsung 980 PRO NVMe.

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
