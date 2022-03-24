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

A redb database file consists of a header, and several B-trees:
* free space tree: mapping from transaction ids to the list of pages they freed
* table tree: name -> table definition mapping of table names to their definitions
* data tree(s) (per one table): key -> value mapping for table
While logically separate, all the B-trees are stored in a single structure

Except for the database header, all other data structures are copy-on-write or append-only and
may not be mutated in-place

### Database header
Mutable fields in the database header are all double buffered, so that writes occur on the
inactive copy, which is then atomically promoted to the primary via the `field_mutex` field
* magic number (immutable, 9 bytes): magic number
* page_size (immutable, 1 byte): single byte, x, where `2^x` represents the size of pages
* region_size: (immutable, 8 bytes): 64-bit unsigned little-endian integer, indicating the maximum number of usable
  bytes in a region
* db_size: (immutable, 8 bytes): 64-bit unsigned little-endian integer, indicating the number of valid
  bytes in the database file
* root1 (mutable, 8 bytes): 64-bit unsigned little-endian integer, indicating the offset
  to the root page of the B-tree
* root2 (mutable, 8 bytes): double buffer for `root1`
* transaction_id1 (mutable, 8 bytes): 64-bit unsigned little-endian integer, monotonically
  increasing id indicating the currently committed transaction
* transaction_id2 (mutable, 8 bytes): double buffer for `transaction_id1`
* version1 (mutable, 1 byte): single byte which must be `1`
* version2 (mutable, 1 byte): double buffer for `version1`
* field_mutex (mutable, 1 byte): either `1` indicating that the first version of each
  versioned field is valid, or `2` indicating that the second version of each versioned field is valid.

### Page allocator state
The page allocator uses a two level allocator approach. The top level, the "region allocator", allocates regions of memory
in the data section.
Each region has a "regional allocator", which is stored in the footer of the region. This allocator uses a buddy allocator
approach to allocate pages of sizes between the configured page size and the size of the region.

The page allocator state is technically just a cache: it can be reconstructed by walking the btree from all active roots.
The dirty bit is set on the allocator state before it is written to, and cleared when the database is closed.
If the dirty bit is found set on the shadow page allocator state, when the database is opened, then it must be repaired
by re-walking all the btree roots.

### Non-durable commits
redb supports "non-durable" commits, meaning that there is no guarantee of durability. However, in the event of a crash
the database is still guaranteed to be consistent, and will return to either the last non-durable commit or the last
full commit that was made.
Non-durable commits are implemented by marking both the primary & secondary page allocator states as dirty, and keeping
both in sync, an in-memory flag is set to direct readers to read from the secondary page, but it is not promoted to the
primary. In the event of a crash, the primary page allocator state will be corrupt, but since it is marked dirty it will
be safely rebuilt on the next database open.
Note that freeing pages during a non-durable commit is not permitted, because it could be rolled back at anytime.
