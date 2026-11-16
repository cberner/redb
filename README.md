# redb

![CI](https://github.com/cberner/redb/actions/workflows/ci.yml/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/redb.svg)](https://crates.io/crates/redb)
[![Documentation](https://docs.rs/redb/badge.svg)](https://docs.rs/redb)
[![License](https://img.shields.io/crates/l/redb)](https://crates.io/crates/redb)
[![dependency status](https://deps.rs/repo/github/cberner/redb/status.svg)](https://deps.rs/repo/github/cberner/redb)

A simple, portable, high-performance, ACID, embedded key-value store.

redb is written in pure Rust and is loosely inspired by [lmdb](http://www.lmdb.tech/doc/). Data is stored in a collection
of copy-on-write B+trees. For more details, see the [design doc](docs/design.md)

```rust
use redb::{Database, Error, ReadableDatabase, ReadableTable, TableDefinition};

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
* Multi-process access to one database, with a single or several writer processes

## `no_std`

Turning off the `std` feature builds redb without the standard
library. It needs `alloc`, `panic = "abort"`, and a target with atomic compare-and-swap -- Cortex-M3
and above, but not Cortex-M0. Embedded targets abort on panic by default. Tests always unwind, so
tests run on the host need the `std` feature, which a dev-dependency on redb can enable. There is no
filesystem, so storage is supplied by implementing `StorageBackend`, which reports failures with
`BackendError::Other`, and passing it to `Builder::create_with_backend()` or
`Builder::open_read_only_with_backend()`. The `chrono_v0_4` and `uuid` features require std, and
`cache_metrics` requires a target with 64-bit atomics.

## Development

[`just`](https://github.com/casey/just) and rootless
[`podman`](https://podman.io/) are required.
Toolchain setup, dependency vendoring, and `cargo-deny` checks may use network access without
compiling project dependencies. Other development recipes continue to run on
the host. Cargo build artifacts are cached in the `redb-sandbox-target` Podman
volume; remove that volume to force a clean sandbox build.

## Benchmarks
redb has similar performance to other top embedded key-value stores such as lmdb and rocksdb

|                                   | redb       | lmdb       | rocksdb        | fjall    | sqlite     |
|-----------------------------------|------------|------------|----------------|----------|------------|
| bulk load (key/s)                 | **608K**   | 593K       | 365K           | 390K     | 386K       |
| individual writes (txn/s)         | **1.11K**  | 620        | 408            | 414      | 142        |
| small batch writes (key/s)        | 79.2K      | 70.7K      | 205K           | **254K** | 37.6K      |
| sorted inserts (key/s)            | 4.38M      | **7.74M**  | 518K           | 570K     | 868K       |
| nosync writes (txn/s)             | 88.9K      | 125K       | 382K           | **595K** | 58.3K      |
| len()                             | **0ms**    | **0ms**    | 701ms          | 972ms    | 25ms       |
| random reads (key/s)              | 1.18M      | **1.59M**  | 423K           | 360K     | 370K       |
| random range reads (scan/s)       | 522K       | **933K**   | 155K           | 190K     | 85.5K      |
| random reads (4 threads) (key/s)  | 4.41M      | **6.01M**  | 1.46M          | 1.44M    | 1.77M      |
| random reads (8 threads) (key/s)  | 8.28M      | **12.3M**  | 2.53M          | 2.81M    | 3.38M      |
| random reads (16 threads) (key/s) | 13.7M      | **23.9M**  | 2.12M          | 4.52M    | 2.72M      |
| random reads (32 threads) (key/s) | 20.3M      | **33.6M**  | 2.07M          | 6.55M    | 2.26M      |
| removals (key/s)                  | 434K       | 361K       | 367K           | **513K** | 282K       |
| retain (key/s)                    | 2.22M      | **2.29M**  | 549K           | 271K     | 189K       |
| extract_if (key/s)                | 1.92M      | **3.50M**  | 544K           | 327K     | 105K       |
| pop (key/s)                       | 1.73M      | **6.36M**  | 1.94K          | 2.63K    | 110K       |
| uncompacted size                  | 4.00 GiB   | 3.35 GiB   | **558.01 MiB** | 1.85 GiB | 1.10 GiB   |
| compacted size                    | 650.89 MiB | 741.61 MiB | **459.28 MiB** | 1.85 GiB | 562.30 MiB |

Source code for benchmark [here](./crates/redb-bench-compare/benches/lmdb_benchmark.rs). Results collected on a Ryzen 9950X3D with Samsung 9100 PRO NVMe.

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
