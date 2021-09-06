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

### Page allocator state
The page allocator state is double buffered, and the state is technically just a cache: it can be reconstructed
by walking the btree from all active roots.
Both copies of the page allocator state are kept up to date after a transaction commit. This is done safely by
using the following protocol:
1) the shadow copy is updated
2) the dirty bit is set for the primary copy. If the system crashes at this point, and the dirty bit gets written out,
   then when the database is next opened the page allocation state will need to be reconstructed by walking the entire
   btree. Setting this dirty bit on the primary before fsync'ing to swap the buffers is necessary because we do not
   fsync after step (4) to avoid introducing an extra fsync in the commit protocol, but those writes must be marked
   dirty.
3) fsync the shadow metadata & shadow page allocator state.
4) fsync to swap the shadow copy with the primary copy is completed, and unset the dirty bit on the shadow page
   allocator state as clean
5) the old primary copy is updated

When the database is closed, the shadow copy should be fsync'ed, it's dirty bit should be cleared, and a second
fsync performed.

If the dirty bit is found set on the primary page allocator state, when the database is opened, it can be ignored. It
must have come from a crash during or after step 2, but before step 5, therefore the state is correct.
If the dirty bit is found set on the shadow page allocator state, when the database is opened, then it must be repaired
by copying it from the primary or re-walking all the btree roots