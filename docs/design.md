# redb design

redb is a simple, portable, high-performance, ACID, embedded key-value store.

Each redb database contains a collection of tables, and supports a single writer and multiple 
concurrent readers. redb uses MVCC to provide isolation, and provides a single isolation level:
serializable, in which all writes are applied sequentially.

Each table is a key-value mapping. The keys & values can be either fixed size or dynamically
sized byte arrays, as specified at the time of table creation.

## Assumptions about underlying media
redb is designed to be safe even in the event of power failure or on poorly behaved media,
therefore we make only a few assumptions about the guarantees provided by the underlying filesystem:
1. single byte writes are atomic: i.e., each byte will be written completely or not at all,
   even in the event of a power failure
2. following a `fsync` operation writes are durable
3. ["powersafe overwrite"](https://www.sqlite.org/psow.html): When an application writes
   a range of bytes in a file, no bytes outside of that range will change,
   even if the write occurs just before a crash or power failure. sqlite makes this same
   assumption, by default, in all modern versions.
 
## File format

A redb database file consists of a header, and several B+ trees:
* free space tree: mapping from transaction ids to the list of pages they freed
* table tree: name -> table definition mapping of table names to their definitions
* data tree(s) (per one table): key -> value mapping for table

Except for the database header, all other data structures are copy-on-write and
may not be mutated in-place

### Database header
Mutable fields in the database header are all double buffered, so that writes occur on the
inactive copy, which is then atomically promoted to the primary via the `field_mutex` field
* magic number (immutable, 8 bytes): 64-bit big-endian magic number
* version (immutable, 1 byte): single byte which must be `1`
* length1 (mutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the number of valid
  bytes in the database file
* length2 (mutable, 8 bytes): double buffer for `length1`
* free_space_root1 (mutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the offset
  to the root page of the free space tree
* free_space_root2 (mutable, 8 bytes): double buffer for `free_space_root1`
* table_root1 (mutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the offset
  to the root page of the table tree
* table_root2 (mutable, 8 bytes): double buffer for `table_root1`
* transaction_id1 (mutable, 8 bytes): 64-bit unsigned big-endian integer, monotonically
  increasing id indicating the currently committed transaction
* transaction_id2 (mutable, 8 bytes): double buffer for `transaction_id1`
* field_mutex (mutable, 1 byte): either `1` indicating that the first version of each
  versioned field is valid, or `2` indicating that the second version of each versioned field is valid.

### Table definition
* key size (immutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the length of
  keys. `0` indicates dynamically sized keys.
* value size (immutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the length of
  values. `0` indicates dynamically sized values.
* root page (CoW mutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the offset of the
  root page.

