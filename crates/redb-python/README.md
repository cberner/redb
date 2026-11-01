# redb

Python bindings for [redb](https://github.com/cberner/redb), a simple, portable, high-performance,
ACID, embedded key-value store.

The bindings use the same file format as the Rust crate, so a database can be read and written
from both languages.

```bash
pip install redb
```

Requires Python 3.12 or later.

```python
import redb

TABLE = redb.TableDefinition("my_data", redb.str, redb.u64)

db = redb.Database.create("my_db.redb")
with db.begin_write() as txn:
    table = txn.open_table(TABLE)
    table["my_key"] = 123

read_txn = db.begin_read()
table = read_txn.open_table(TABLE)
assert table["my_key"] == 123
```

## Features
* `dict`-like tables: `table[key]`, `table[key] = value`, `del table[key]`, `key in table`,
  `len(table)`, iteration, `get()`, `range()`, `first()`, `last()`, `pop_first()`, `pop_last()`
  and `retain()`
* Fully ACID-compliant transactions. Leaving a `with db.begin_write()` block commits the
  transaction, or aborts it if an exception was raised
* Concurrent readers and a writer, in one process or in several
* Savepoints and rollbacks
* Compaction and integrity checking

## Types
Keys and values are typed. The types are `redb.bytes`, `redb.str`, `redb.bool`, `redb.u64`,
`redb.i64`, `redb.u128`, `redb.i128`, and `redb.f64`. `f64` can only be used for values, since
floats have no total order. The Python builtins `bytes`, `str`, `bool` and `float` can be used in
place of the redb type of the same name. Keys are sorted in redb's order: integers numerically,
`bytes` and `str` lexicographically.

## Multiple processes
One process can open a database for writing with `redb.open()` while any number of other
processes open it for reading with `redb.open_read_only()`. Each read transaction sees the latest
commit.

To allow more than one writer process, open the database with `concurrency=redb.MultiWriter` in
every process. Write transactions from different processes take turns.

```python
import redb

db = redb.open("my_db.redb", concurrency=redb.MultiWriter)
```

## Development
The bindings are built with [maturin](https://www.maturin.rs). `just install_py` builds and
installs them into the active virtualenv, `just test_py` runs the tests, and `just fuzz_py` runs
the Hypothesis-based fuzzer.

## License

Licensed under either of the Apache License, Version 2.0 or the MIT License, at your option.
