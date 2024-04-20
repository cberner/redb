# redb - Changelog

## 2.1.0 - 2024-04-20
* Implement `Key` and `Value` for `String`
* Allow users to implement `ReadableTableMetadata`, `ReadableTable`, and `ReadableMultimapTable`

## 2.0.0 - 2024-03-22

### Major file format change
2.0.0 uses a new file format that optimizes `len()` to be constant time. This means that it is not
backwards compatible with 1.x. To upgrade, consider using a pattern like that shown in the
[upgrade_v1_to_v2](https://github.com/cberner/redb/blob/222a37f4600588261b0983eebcd074bb69d6e5a0/tests/backward_compatibility.rs#L282-L299) test.

### Other changes
* `check_integrity()` now returns a `DatabaseError` instead of a `StorageError`
* Table metadata methods have moved to a new `ReadableTableMetadata` trait
* Rename `RedbKey` to `Key`
* Rename `RedbValue` to `Value`
* Remove lifetimes from read-only tables
* Remove lifetime from `WriteTransaction` and `ReadTransaction`
* Remove `drain()` and `drain_filter()` from `Table`. Use `retain`, `retain_in`, `extract_if` or `extract_from_if` instead
* impl `Clone` for `Range`
* Add support for `[T;N]` as a `Value` or `Key` as appropriate for the type `T`
* Add `len()` and `is_empty()` to `MultimapValue`
* Add `retain()` and `retain_in()` to `Table`
* Add `extract_if()` and `extract_from_if()` to `Table`
* Add `range()` returning a `Range` with the `'static` lifetime to read-only tables
* Add `get()` returning a range with the `'static` lifetime to read-only tables
* Add `close()` method to `ReadTransaction`

## 1.5.1 - 2024-03-16
* Fix `check_integrity()` so that it returns `Ok(true)` when no repairs were preformed. Previously,
  it returned `Ok(false)`

## 1.5.0 - 2024-01-15
* Export `TableStats` type
* Export `MutInPlaceValue` which allows custom types to support `insert_reserve()`
* Add untyped table API which allows metadata, such as table stats, to be retrieved for at table
  without knowing its type at compile time
* Fix compilation on uncommon platforms (those other than Unix and Windows)

## 1.4.0 - 2023-11-21
* Add `Builder::set_repair_callback()` which can be used to set a callback function that will be invoked if the database needs repair while opening it.
* Add support for custom storage backends. This is done by implementing the `StorageBackend` trait and
  using the `Builder::create_with_backend` function. This allows the database to be stored in a location other
  than the filesystem
* Implement `RedbKey` and `RedbValue` for `char`
* Implement `RedbKey` and `RedbValue` for `bool`
* Implement `TableHandle` for `Table`
* Implement `MultimapTableHandle` for `MultimapTable`
* Fix panic that could occur when inserting a large number of fixed width values into a table within a single transaction
* Fix panic when calling `delete_table()` on a table that is already open
* Improve performance for fixed width types
* Support additional platforms

## 1.3.0 - 2023-10-22
* Implement `RedbKey` for `Option<T>`
* Implement `RedbValue` for `Vec<T>`
* Implement `Debug` for tables
* Add `ReadableTable::first()` and `last()` which retrieve the first and last key-value pairs, respectively`
* Reduce lock contention for mixed read-write workloads
* Documentation improvements

## 1.2.0 - 2023-09-24
* Add `Builder::create_file()` which does the same thing as `create()` but
  takes a `File` instead of a path
* Add `stats()` to tables which provides informational statistics on the table's storage
* Fix `WriteTransaction::stats()` to correctly count the storage used by multi-map tables
* Fix panics that could occur when operating on savepoints concurrently from multiple threads
  on the same `WriteTransaction`
* Implement `Send` for `WriteTransaction`
* Change MSRV to 1.66
* Performance optimizations

## 1.1.0 - 2023-08-20
* Fix panic when calling `compact()` on certain databases
* Fix panic when calling `compact()` when an ephemeral `Savepoint` existed
* Improve performance of `compact()`
* Relax lifetime requirements on arguments to `insert()`

## 1.0.5 - 2023-07-16
* Fix a rare panic when recovering a database file after a crash
* Minor performance improvement to write heavy workloads

## 1.0.4 - 2023-07-01
* Fix serious data corruption issue when calling `drain()` or `drain_filter()` on a `Table` that had
  uncommitted data

## 1.0.3 - 2023-06-30
* Fix panic when re-opening databases of certain, small, sizes

## 1.0.2 - 2023-06-29
* Fix panic when recovering some databases after a forceful shutdown
* Fix panic when recovering databases with multimaps that have fixed width values after a forceful shutdown

## 1.0.1 - 2023-06-26
* Fix panic that could occur after an IO error when reopening a database
* Fix panic that could occur after an IO error when opening a table
* Improve error message when opening a table twice to include a more meaningful line number
* Performance improvements

## 1.0.0 - 2023-06-16
### Announcement
redb has reached its first stable release! The file format is now gauranteed to be backward compatible,
and the API is stable. I've run pretty extensive fuzz testing, but please report any bugs you encounter.

The following features are complete:
* MVCC with a single `WriteTransaction` and multiple `ReadTransaction`s
* Zero-copy reads
* ACID semantics, including non-durable transactions which only sacrifice Durability
* Savepoints which allow the state of the database to be captured and restored later

#### Changes from 0.22.0:
* Stabilize file format
* Improve performance of `restore_savepoint()`

## 0.22.0 - 2023-06-12
* Fix panic while repairing a database file after crash
* Fix rare panic in `restore_savepoint()`

## 0.21.0 - 2023-06-09
* Improve cache heuristic. This asymptotically improves performance on large databases. Benchmarks show 30% to 5x+
* Fix rare crash that could occur under certain conditions when inserting values > 2GiB
* Fix crash when growing database beyond 4TiB
* Fix panic when repairing a database containing a multimap table with fixed width values
* Performance optimizations
* File format simplifications

## 0.20.0 - 2023-05-30
* Export `TransactionError` and `CommitError`. These were unintentionally private
* Implement `std::error::Error` for all error enums

## 0.19.0 - 2023-05-29
* Remove `Clone` bound from range argument type on `drain()` and `drain_filter()`
* File format changes to improve future extensibility

## 0.18.0 - 2023-05-28
* Improve errors to be more granular. `Error` has been split into multiple different `enum`s, which
  can all be implicitly converted back to `Error` for convenience
* Rename `savepoint()` to `ephemeral_savepoint()`
* Add support for persistent savepoints. These persist across database restarts and must be explicitly
  released
* Optimize `restore_savepoint()` to be ~30x faster
* Add experimental support for WASI. This requires nightly
* Implement `RedbKey` for `()`
* Fix some rare crash and data corruption bugs

## 0.17.0 - 2023-05-09
* Enforce a limit of 3GiB on keys & values
* Fix database corruption bug that could occur if a `Durability::None` commit was made,
  followed by a durable commit and the durable commit crashed or encountered an I/O error during `commit()`
* Fix panic when re-openning a database file, when the process that last had it open had crashed
* Fix several bugs where an I/O error during `commit()` could cause a panic instead of returning an `Err`
* Change `length` argument to `insert_reserve()` to `u32`
* Change `Table::len()` to return `u64`
* Change width of most fields in `DatabaseStats` to `u64`
* Remove `K` type parameter from `AccessGuardMut`
* Add `Database::compact()` which compacts the database file
* Performance optimizations

## 0.16.0 - 2023-04-28
* Combine `Builder::set_read_cache_size()` and `Builder::set_write_cache_size()` into a single,
  `Builder::set_cache_size()` setting
* Relax lifetime constraints on read methods on tables
* Optimizations to `Savepoint`

## 0.15.0 - 2023-04-09
* Add `Database::check_integrity()` to explicitly run repair process (it is still always run if needed on db open)
* Change `list_tables()` to return a `TableHandle`
* Change `delete_table()` to take a `TableHandle`
* Make `insert_reserve()` API signature type safe
* Change all iterators to return `Result` and propagate I/O errors
* Replace `WriteStrategy` with `Durability::Paranoid`
* Remove `Builder::set_initial_size()`
* Enable db file shrinking on Windows
* Performance optimizations

## 0.14.0 - 2023-03-26
* Remove `Builder::create_mmapped()` and `Builder::open_mmapped()`. The mmap backend has been removed
  because it was infeasible to prove that it was sound. This makes the redb API entirely safe,
  and the remaining `File` based backed is within a factor of ~2x on all workloads that I've benchmarked
* Make `Table` implement `Send`. It is now possible to insert into multiple `Table`s concurrently
* Expose `AccessGuardMut`, `Drain` and `DrainFilter` in the public API
* Rename `RangeIter` to `Range`
* Rename`MultimapRangeIter` to `MultimapRange`
* Rename `MultimapValueIter` to `MultimapValue`
* Performance optimizations

## 0.13.0 - 2023-02-05
* Fix a major data corruption issue that was introduced in version 0.12.0. It caused databases
  greater than ~4GB to become irrecoverably corrupted due to an integer overflow in `PageNumber::address_range`
  that was introduced by commit `b2c44a824d1ba69f526a1a75c56ae8484bae7248`
* Add `drain_filter()` to `Table`
* Make key and value type bounds more clear for tables

## 0.12.1 - 2023-01-22
* Fix `open()` on platforms with OS page size != 4KiB
* Relax lifetime requirements on argument to `range()` and `drain()`

## 0.12.0 - 2023-01-21
* Add `pop_first()` and `pop_last()` to `Table`
* Add `drain()` to `Table`
* Add support for `Option<T>` as a value type
* Add support for user defined key and value types. Users must implement `RedbKey` and/or `RedbValue`
* Change `get()`, `insert()`, `remove()`...etc to take arguments of type `impl Borrow<SelfType>`
* Return `Error::UpgradeRequired` when opening a file with an outdated file format
* Improve support for 32bit platforms
* Performance optimizations

## 0.11.0 - 2022-12-26
* Remove `[u8]` and `str` type support. Use `&[u8]` and `&str` instead.
* Change `get()`, `range()` and several other methods to return `AccessGuard`.
* Rename `AccessGuard::to_value()` to `value()`
* Add a non-mmap based backend which is now the default. This makes `Database::create()` and
  `Database::open()` safe, but has worse performance in some cases. The mmap backend is available
  via `create_mmapped()`/`open_mmapped()`. There is no difference in the file format, so applications
  can switch from one backend to the other.
* Better handling of fsync failures

## 0.10.0 - 2022-11-23
* Remove maximum database size argument from `create()`. Databases are now unbounded in size
* Reduce address space usage on Windows
* Remove `set_dynamic_growth()`
* Add `set_initial_size()` to `Builder`
* Optimize cleanup of deleted pages. This resolves a performance issue where openning a Database
  or performing a small transaction, could be slow if the last committed transaction deleted a large
  number of pages
* Remove `set_page_size()`. 4kB pages are always used now
* Add `iter()` method to `Table` and `MultimapTable`
* Fix various lifetime issues with type that had a lifetime, such as `&str` and `(&[u8], u64)`

## 0.9.0 - 2022-11-05
* Add support for dynamic file growth on Windows
* Add support for tuple types as keys and values
* Remove `Builder::set_region_size`
* Save lifetime from `Savepoint`
* Fix crash when using `create()` to open an existing database created with `WriteStrategy::TwoPhase`
* Fix rare crash when writing a mix of small and very large values into the same table
* Performance optimizations

## 0.8.0 - 2022-10-18
* Performance improvements for database files that are too large to fit in RAM
* Fix deadlock in concurrent calls to `savepoint()` and `restore_savepoint()`
* Fix crash if `restore_savepoint()` failed
* Move `savepoint()` and `restore_savepoint()` methods to `WriteTransaction`
* Implement `Iterator` for the types returned from `range()` and `remove_all()`

## 0.7.0 - 2022-09-25
* Add support for Windows
* Add `Database::set_write_strategy` which allows the `WriteStrategy` of the database to be changed after creation
* Make `Database::begin_write` block, instead of panic'ing, if there is another write already in progress
* Add `Database::savepoint` and `Database::restore_savepoint` which can be used to snapshot and rollback the database
* Rename `DatabaseBuilder` to `Builder`
* Performance optimizations for large databases

## 0.6.1 - 2022-09-11
* Fix crash when `Database::open()` was called on a database that had been created with `WriteStrategy::TwoPhase`
* Change default region size on 32bit platforms to 4GiB

## 0.6.0 - 2022-09-10
* Return `Err` instead of panic'ing when opening a database file with an incompatible file format version
* Many optimizations to the file format, and progress toward stabilizing it
* Fix race between read & write transactions, which could cause reads to return corrupted data
* Better document the different `WriteStrategy`s
* Fix panic when recovering a database that was uncleanly shutdown, which had been created with `WriteStrategy::Checksum` (which is the default)
* Fix panic when using `insert_reserve()` in certain cases

## 0.5.0 - 2022-08-06
* Optimize `MultimapTable` storage format to use `O(k * log(n_k) + v * log(n_v / n_k))` space instead of `O(k * log(n_k + n_v) + v * log(n_k + n_v))` space,
  where k is the size of the stored keys, v is the size of the stored values, n_k is the number of stored keys,
  n_v is the number of stored values
* Fix compilation errors for 32bit x86 targets
* Add support for the unit type, `()`, as a value
* Return an error when attempting to open the same database file for writing in multiple locations, concurrently
* More robust handling of fsync failures
* Change `MultimapTable::range` to return an iterator of key-value-collection pairs, instead of key-value pairs
* Automatically abort `WriteTransaction` on drop

## 0.4.0 - 2022-07-26
* Add single phase with checksum commit strategy. This is now the default and reduces commit latency by ~2x. For more details,
  see the [design doc](docs/design.md#1-phase--checksum-durable-commits) and
  [blog post](https://www.redb.org/post/2022/07/26/faster-commits-with-1pcc-instead-of-2pc/). The previous behavior is available
  via `WriteStrategy::Throughput`, and can have better performance when writing a large number of bytes per transaction.

## 0.3.1 - 2022-07-20
* Fix a bug where re-opening a `Table` during a `WriteTransaction` lead to stale results being read

## 0.3.0 - 2022-07-19
* Fix a serious data corruption issue that caused many write operations to corrupt the database
* Make redb region size configurable
* Implement garbage collection of empty regions
* Fixes and optimizations to make the file format more efficient

## 0.2.0 - 2022-06-10
* Add information log messages which can be enabled with the `logging` feature
* Add support for `[u8; N]` type
* Optimize storage of fixed width types. The value length is no longer stored, which reduces storage space by ~50% for `u64`,
  2x for `u32`, and also improves performance.

## 0.1.2 - 2022-05-08
* Change `insert()` to return an `Option<V>` with the previous value, instead of `()`

## 0.1.1 - 2022-04-24
* Improved documentation

## 0.1.0 - 2022-04-23
* Initial beta release
