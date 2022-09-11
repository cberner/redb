# redb - Changelog

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
