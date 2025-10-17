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
use redb::{Database, Error, ReadableDatabase, TableDefinition};

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
Stable and maintained.

The file format is stable, and a reasonable effort will be made to provide an upgrade path if there
are any future changes to it.

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

|                           | redb      | lmdb       | rocksdb        | sled     | fjall       | sqlite     |
|---------------------------|-----------|------------|----------------|----------|-------------|------------|
| bulk load                 | 17063ms   | **9232ms** | 13969ms        | 24971ms  | 18619ms     | 15341ms    |
| individual writes         | **920ms** | 1598ms     | 2432ms         | 2701ms   | 3488ms      | 7040ms     |
| batch writes              | 1595ms    | 942ms      | 451ms          | 853ms    | **353ms**   | 2625ms     |
| len()                     | **0ms**   | **0ms**    | 749ms          | 1573ms   | 1181ms      | 30ms       |
| random reads              | 1138ms    | **637ms**  | 2911ms         | 1601ms   | 2177ms      | 4283ms     |
| random reads              | 934ms     | **631ms**  | 2884ms         | 1592ms   | 2357ms      | 4281ms     |
| random range reads        | 1174ms    | **565ms**  | 2734ms         | 1992ms   | 2564ms      | 8431ms     |
| random range reads        | 1173ms    | **565ms**  | 2742ms         | 1993ms   | 2690ms      | 8449ms     |
| random reads (4 threads)  | 1390ms    | **840ms**  | 3995ms         | 1913ms   | 2606ms      | 7000ms     |
| random reads (8 threads)  | 757ms     | **427ms**  | 2147ms         | 1019ms   | 1352ms      | 8123ms     |
| random reads (16 threads) | 652ms     | **216ms**  | 1478ms         | 690ms    | 963ms       | 23022ms    |
| random reads (32 threads) | 410ms     | **125ms**  | 1100ms         | 444ms    | 576ms       | 26536ms    |
| removals                  | 23297ms   | 10435ms    | 6900ms         | 11088ms  | **6004ms**  | 10323ms    |
| uncompacted size          | 4.00 GiB  | 2.61 GiB   | **893.18 MiB** | 2.13 GiB | 1000.95 MiB | 1.09 GiB   |
| compacted size            | 1.69 GiB  | 1.26 GiB   | **454.71 MiB** | N/A      | 1000.95 MiB | 556.85 MiB |

Source code for benchmark [here](./crates/redb-bench/benches/lmdb_benchmark.rs). Results collected on a Ryzen 9950X3D with Samsung 9100 PRO NVMe.

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
