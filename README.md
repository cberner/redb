# shodh-redb

[![Crates.io](https://img.shields.io/crates/v/shodh-redb.svg)](https://crates.io/crates/shodh-redb)
[![Documentation](https://docs.rs/shodh-redb/badge.svg)](https://docs.rs/shodh-redb)
[![License](https://img.shields.io/crates/l/shodh-redb)](https://crates.io/crates/shodh-redb)

Multi-modal embedded database for Rust -- vectors, blobs, TTL, merge operators, and causal tracking built on ACID B-trees.

shodh-redb extends [redb](https://github.com/cberner/redb) with capabilities for AI/ML workloads, edge computing, and multi-modal data storage. Written in pure Rust with `no_std` support.

## What's different from redb?

| Capability | redb | shodh-redb |
|---|---|---|
| ACID key-value store | Yes | Yes |
| Vector types & similarity search | No | `FixedVec<N>`, `DynVec`, `BinaryQuantized<N>`, `ScalarQuantized<N>` |
| Blob store (streaming, dedup, compaction) | No | Append-only with SHA-256 dedup, seekable reads, causal lineage |
| TTL (per-key expiration) | No | `TtlTable` with lazy filtering and bulk purge |
| Merge operators (atomic RMW) | No | `NumericAdd`, `NumericMax`, `BitwiseOr`, closures |
| Group commit (batched fsync) | No | Leader-election batching for concurrent writes |
| Hybrid Logical Clock | No | 48-bit physical + 16-bit logical for causal ordering |
| Memory budget | No | Hard RAM cap with adaptive cache sizing |
| `no_std` support | No | `#![no_std]` with `std` feature flag (default on) |

## Quick start

```toml
[dependencies]
shodh-redb = "0.1"
```

### Key-value store

```rust
use shodh_redb::{Database, Error, ReadableDatabase, ReadableTable, TableDefinition};

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

### Vector embeddings

```rust
use shodh_redb::{Database, FixedVec, TableDefinition, cosine_distance, nearest_k};

// Store 384-dim embeddings
const EMBEDDINGS: TableDefinition<u64, FixedVec<384>> = TableDefinition::new("embeddings");

// Quantized storage (32x smaller)
use shodh_redb::{BinaryQuantized, quantize_binary, hamming_distance};
const BINARY: TableDefinition<u64, BinaryQuantized<12>> = TableDefinition::new("binary_vecs");
```

### TTL tables

```rust
use shodh_redb::{Database, TtlTableDefinition};
use std::time::Duration;

const SESSIONS: TtlTableDefinition<&str, &[u8]> = TtlTableDefinition::new("sessions");

// Insert with 30-minute expiry
// table.insert_with_ttl("session_abc", data, Duration::from_secs(1800))?;
// Expired entries are automatically filtered on read
// Bulk cleanup: table.purge_expired()?;
```

### Merge operators

```rust
use shodh_redb::{NumericAdd, MergeOperator};

// Atomic counter increment -- no read-modify-write boilerplate
// table.merge(&"page_views", &1u64, &NumericAdd)?;
```

### Blob store

```rust
use shodh_redb::{Database, ContentType, StoreOptions};

// Store large data with streaming writes and content dedup
// let blob_id = write_txn.store_blob(image_bytes, ContentType::ImagePng, "photo", &StoreOptions::default())?;
// let reader = read_txn.blob_reader(blob_id)?; // Seekable reader
```

## Feature flags

| Flag | Default | Description |
|---|---|---|
| `std` | Yes | File backends, group commit, TTL, full error types |
| `logging` | No | Enable `log` crate messages |
| `cache_metrics` | No | Cache hit/miss counters |
| `compression_lz4` | No | LZ4 page compression |
| `compression_zstd` | No | Zstandard page compression |
| `compression` | No | Enable all compression algorithms |

### `no_std` usage

```toml
[dependencies]
shodh-redb = { version = "0.1", default-features = false }
```

Requires `alloc`. File backends, group commit, and TTL are unavailable in `no_std` mode. Use `InMemoryBackend` or implement a custom `StorageBackend`.

## Credits

shodh-redb is built on top of [redb](https://github.com/cberner/redb) by Christopher Berner. The core B-tree engine, page cache, MVCC, and crash recovery are inherited from redb. All extended features (vectors, blobs, TTL, merge operators, HLC, group commit, memory budget, `no_std`) are original additions.

## License

Licensed under either of

* [Apache License, Version 2.0](LICENSE-APACHE)
* [MIT License](LICENSE-MIT)

at your option.
