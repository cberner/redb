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

|                           | redb   | lmdb   | rocksdb | sled   | sanakirja |
|---------------------------|--------|--------|---------|--------|-----------|
| bulk load                 | 2792ms | 1115ms | 5610ms  | 5005ms | 1161ms    |
| individual writes         | 462ms  | 1119ms | 1097ms  | 957ms  | 662ms     |
| batch writes              | 2568ms | 2247ms | 1344ms  | 1622ms | 2713ms    |
| random reads              | 988ms  | 558ms  | 3469ms  | 1509ms | 678ms     |
| random reads              | 962ms  | 556ms  | 3377ms  | 1425ms | 671ms     |
| random range reads        | 2534ms | 985ms  | 6058ms  | 4670ms | 1089ms    |
| random range reads        | 2493ms | 998ms  | 5801ms  | 4665ms | 1119ms    |
| random reads (4 threads)  | 344ms  | 141ms  | 1247ms  | 424ms  | 266ms     |
| random reads (8 threads)  | 192ms  | 72ms   | 673ms   | 230ms  | 620ms     |
| random reads (16 threads) | 131ms  | 47ms   | 476ms   | 148ms  | 3500ms    |
| random reads (32 threads) | 118ms  | 44ms   | 412ms   | 129ms  | 4313ms    |
| removals                  | 2184ms | 784ms  | 2451ms  | 2047ms | 1344ms    |

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
