# redb design

redb is a simple, portable, high-performance, ACID, embedded key-value store.

Each redb database contains a collection of tables, and supports a single writer and multiple
concurrent readers. redb uses MVCC to provide isolation, and provides a single isolation level:
serializable, in which all writes are applied sequentially.

Each table is a key-value mapping providing an interface similar to `BTreeMap`.

## Assumptions about underlying media
redb is designed to be safe even in the event of power failure or on poorly behaved media.
Therefore, we make only a few assumptions about the guarantees provided by the underlying filesystem:
1. single byte writes are atomic: i.e., each byte will be written completely or not at all,
   even in the event of a power failure
2. following a `fsync` operation writes are durable
3. ["powersafe overwrite"](https://www.sqlite.org/psow.html): When an application writes
   a range of bytes in a file, no bytes outside of that range will change,
   even if the write occurs just before a crash or power failure. sqlite makes this same
   assumption, by default, in all modern versions.

## File format
A redb database file consists of a header, and several B-trees:
* pending free tree: mapping from transaction ids to the list of pages they freed
* table tree: name -> table definition mapping of table names to their definitions
* data tree(s) (per one table): key -> value mapping for table

Except for the database header, all other data structures are copy-on-write.

The database file begins with the database header, and is followed by one or more regions. Each region contains a
header, and a data section which is split into many pages. These regions allow for efficient, dynamic, growth of the
database file.

```
-------------------------------------------------
|               Database header                 |
-------------------------------------------------
|               Region header                   |
-------------------------------------------------
|               Region data                     |
-------------------------------------------------
|               ...more regions                 |
-------------------------------------------------
```

### Database header
The database header contains several immutable fields, such the database page size, region size, and a magic number.
Transaction data is stored in a double buffered field, and the primary copy is managed by updating a single byte that
controls which transaction pointer is the primary.

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

### 2-phase (durable) commits
redb uses a 2-phase commit strategy. First, data is written to a new copy of the btree, second an `fsync` is performed,
finally the byte controlling which copy of the btree is the primary is flipped and a second `fsync` is performed.

### 1-phase + checksum (durable) commits
A reduced latency commit strategy is planned, in which all branch pages in a btree will contain checksums of their children,
these checksums form a non-cryptographic Merkle tree allowing corruption of a btree to be detected. A commit can then be
performed with a single `fsync`. All data and checksums will be written, along with a monotonically incrementing transaction
id, then the primary will be flipped and `fsync` called. If a crash occurs, we must verify that the primary has a larger
transaction id and that all of its checksums are valid. If this check fails, then the database will replace the partially
updated primary with the secondary.
