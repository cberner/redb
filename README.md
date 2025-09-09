# redbx

[![License](https://img.shields.io/crates/l/redbx)](https://img.shields.io/crates/l/redbx)

A simple, portable, high-performance, ACID, embedded key-value store with built-in AES encryption.

redbx is a fork of redb that provides transparent encryption for all user data. It's written in pure Rust
and is loosely inspired by [lmdb](http://www.lmdb.tech/doc/). Data is stored in a collection of copy-on-write
B-trees with AES-256-GCM encryption. For more details, see the [design doc](docs/redbx-design.md)

## Key Features

- **Built-in AES-256-GCM encryption** for all user data
- **PBKDF2-SHA256 key derivation** with 100,000 iterations
- **Transparent encryption/decryption** at the storage layer
- Zero-copy, thread-safe, `BTreeMap` based API
- Fully ACID-compliant transactions
- MVCC support for concurrent readers & writers, without blocking
- Crash-safe by default
- Savepoints and rollbacks

```rust
use redbx::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    // Create an encrypted database with a password
    let db = Database::create("my_db.redbx", "my_secure_password")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert("my_key", &123)?;
    }
    write_txn.commit()?;

    // Open the encrypted database with the same password
    let db = Database::open("my_db.redbx", "my_secure_password")?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get("my_key")?.unwrap().value(), 123);

    Ok(())
}
```

## Features
* Zero-copy, thread-safe, `BTreeMap` based API
* Fully ACID-compliant transactions
* MVCC support for concurrent readers & writer, without blocking
* Crash-safe by default
* Savepoints and rollbacks
* **Built-in AES-256-GCM encryption** for all user data
* **PBKDF2-SHA256 key derivation** with 100,000 iterations
* **Transparent encryption/decryption** at the storage layer

## Development
To run all the tests and benchmarks a few extra dependencies are required:

```bash
# Install dependencies
sudo apt-get install libclang-dev

# Run tests
cargo test

# Run benchmarks
cd crates/redbx-bench
cargo bench --bench encryption_overhead_benchmark
```

## Known problem

- lack of performance, work in progress.

## License

Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Security

redbx uses industry-standard cryptographic algorithms:
- **AES-256-GCM** for authenticated encryption
- **PBKDF2-SHA256** with 100,000 iterations for key derivation
- **Cryptographically secure random** salt generation

All user data is encrypted at rest, while metadata remains unencrypted for performance and debugging purposes.

## Migration from redb

redbx is designed to be a drop-in replacement for redb with the addition of password-based encryption:

```rust
// redb (unencrypted)
let db = Database::create("db.redb")?;

// redbx (encrypted)
let db = Database::create("db.redbx", "password")?;
```

The API is otherwise identical, making migration straightforward.
