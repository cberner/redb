# redb

Python bindings for [redb](https://github.com/cberner/redb), an embedded key-value store with
ACID transactions, readers that never block the writer, savepoints, and typed tables that sort
by their key type.

```python
import redb

USERS = redb.TableDefinition("users", redb.U64, redb.Str)

db = redb.Database.create("my_db.redb")
with db.begin_write() as txn:
    users = txn.open_table(USERS)
    users[1] = "alice"
    users.insert(2, "bob")

read = db.begin_read()
users = read.open_table(USERS)
assert users[1] == "alice"
for key, name in users.range(1, 3):
    print(key, name)
```

## Types

A table declares a key type and a value type, from `redb.Bytes`, `redb.Str`, `redb.Bool`,
`redb.U64`, `redb.I64`, `redb.U128` and `redb.F64`. The builtins `bytes`, `str`, `bool` and
`float` are accepted as shorthand; `int` needs an explicit width. Each maps to the redb type of
the same name, so a table written by a Rust program opens from Python with those types and sorts
the same way. `F64` cannot be a key.

A `MultimapTableDefinition` declares a table whose keys each hold a sorted set of values; its
value type must also be a key type.

## Transactions

`Database.begin_write()` returns a `WriteTransaction` with `commit()` and `abort()`. Used as a
context manager it commits when the block finishes normally and aborts when an exception is
propagating. A write transaction, and the tables opened from it, are used from one thread at a
time. `Database.begin_read()` returns a `ReadTransaction`, a snapshot of everything committed
so far.

Tables are opened per transaction with `open_table(definition)` and
`open_multimap_table(definition)`: a `Table` or `MultimapTable` from a write transaction, a
`ReadOnlyTable` or `ReadOnlyMultimapTable` from a read transaction. A table supports `get`,
`len()`, `in`, `first()`, `last()`, `range(start, end, reverse=False)`, iteration over
`(key, value)` pairs and `stats()`; a write-side table adds `insert`, `remove`, `pop_first()`,
`pop_last()`, `retain(predicate)` and `extract_if(predicate)`. Changing a table while iterating
over it raises `RuntimeError`, as for a dict. A write transaction also offers savepoints,
durability settings, and `list_tables()`, `delete_table()` and `rename_table()`; deleting or
renaming a table, or restoring a savepoint, invalidates the table objects opened before it,
which raise until opened again.

## Errors

Everything redb reports raises `redb.Error`. Its `kind` attribute names the redb error
variant, such as `"TableDoesNotExist"` or `"DatabaseAlreadyOpen"`, for code that needs to tell
them apart. Wrong argument types raise `TypeError`, out-of-range integers `OverflowError`, and
a missing key under `table[key]` raises `KeyError`.

## Several processes

`Database.create(path, concurrency_mode=redb.ConcurrencyMode.MULTI_WRITER)` lets any number
of processes write, one transaction at a time. `SINGLE_WRITER` lets one process write while
others follow its commits through `redb.ReadOnlyDatabase.open(path, concurrency_mode=...)`.
The default, `EXCLUSIVE_WRITER`, locks the whole file for one process.

## Development

`just test_py` builds the extension and runs the tests. `just fuzz_py 300` runs the
Hypothesis state machine in `test/test_fuzz.py` against a Python model for five minutes.
