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

redb has similar performance to other top embedded key-value stores such as lmdb and rocksdb.

|                           | redb         | lmdb         | rocksdb        | sled       | sanakirja |
|---------------------------|--------------|--------------|----------------|------------|-----------|
| bulk load                 | 2.44s        | 1.05s        | 6.37s          | 5.80s      | **1.01s** |
| individual writes         | **104.61ms** | 177.21ms     | 638.13ms       | 462.88ms   | 160.41ms  |
| batch writes              | 1.94s        | 1.20s        | **765.37ms**   | 1.18s      | 1.58s     |
| len()                     | 3.93µs       | **352.00ns** | 203.00ms       | 415.50ms   | 31.14ms   |
| random reads              | 933.80ms     | **577.05ms** | 2.68s          | 1.63s      | 978.73ms  |
| random reads              | 912.94ms     | **579.27ms** | 2.74s          | 1.64s      | 1.03s     |
| random range reads        | 2.37s        | **930.70ms** | 4.67s          | 4.81s      | 1.33s     |
| random range reads        | 2.41s        | **925.27ms** | 4.71s          | 4.89s      | 1.35s     |
| random reads (4 threads)  | 251.39ms     | **151.90ms** | 691.21ms       | 431.72ms   | 374.82ms  |
| random reads (8 threads)  | 139.29ms     | **93.60ms**  | 396.04ms       | 239.21ms   | 745.92ms  |
| random reads (16 threads) | 108.88ms     | **79.04ms**  | 276.27ms       | 149.45ms   | 1.75s     |
| random reads (32 threads) | 98.35ms      | **70.71ms**  | 259.77ms       | 162.62ms   | 1.76s     |
| removals                  | 1.63s        | **756.30ms** | 3.22s          | 2.46s      | 1.14s     |
| compaction                | 857.02ms     | N/A          | N/A            | N/A        | N/A       |
| size after bench          | 311.23 MiB   | 582.22 MiB   | **206.39 MiB** | 458.51 MiB | 4.00 GiB  |

Source code for benchmark [here](./benches/lmdb_benchmark.rs). Results collected on an Intel i9 11900k with Samsung 990 EVO NVMe.

Benchmarked with the following configuration:

```rs
const ELEMENTS: usize = 1_000_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const CACHE_SIZE: usize = 4 * 1_024 * 1_024 * 1_024;
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
