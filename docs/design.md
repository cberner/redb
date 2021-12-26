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

A redb database file consists of a header, and several modified B-trees (described below):
* free space tree: mapping from transaction ids to the list of pages they freed
* table tree: name -> table definition mapping of table names to their definitions
* data tree(s) (per one table): key -> value mapping for table
While logically separate, all the B-trees are stored in a single structure

Except for the database header, all other data structures are copy-on-write or append-only and
may not be mutated in-place

### Database header
Mutable fields in the database header are all double buffered, so that writes occur on the
inactive copy, which is then atomically promoted to the primary via the `field_mutex` field
* magic number (immutable, 4 bytes): 32-bit magic number
* version (immutable, 1 byte): single byte which must be `1`
* page_size (immutable, 1 byte): single byte, x, where `2^x` represents the size of pages
* db_size: (immutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the number of valid
  bytes in the database file
* root1 (mutable, 8 bytes): 64-bit unsigned big-endian integer, indicating the offset
  to the root page of the B-tree
* root2 (mutable, 8 bytes): double buffer for `root1`
* transaction_id1 (mutable, 16 bytes): 128-bit unsigned big-endian integer, monotonically
  increasing id indicating the currently committed transaction
* transaction_id2 (mutable, 16 bytes): double buffer for `transaction_id1`
* field_mutex (mutable, 1 byte): either `1` indicating that the first version of each
  versioned field is valid, or `2` indicating that the second version of each versioned field is valid.

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
by copying it from the primary or re-walking all the btree roots.

### Modified B-tree
We use a modified B-tree that uses a delta update approach similar to Bw-trees (described in [The Bw-Tree A B-tree for New Hardware
Platforms](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/bw-tree-icde2013-final.pdf)),
to create a lock-free, versioned, B-tree, without relying on copy-on-write. The main benefit over a traditional B-tree
is that it avoids write amplification when implementing an MVCC data store.

Both internal & leaf nodes are extended to include a messages field: `Vec<Message>`. Also, internal node's
child pointers are extended to include a count of valid messages in that child.

Internal node messages have the form `(child_index, page_ptr, valid_messages_count)`, and leaf messages have the form
`(key, optional value)`.

When inserting, a message containing the new key & value is inserted into the leaf. If the messages
buffer is full then the leaf is compacted by applying all the messages to the keys & values. Finally, the new page number
and count of valid messages are inserted into the parent as a message, and compacted if necessary.
Deletes are handled the same way, with an empty value being inserted.

During a lookup, the valid messages (as defined by its parent) for each node are scanned from back to front, and the
message's value is used instead, if a matching one is found.

The fact that parents always define the valid range of messages provides the required MVCC semantics, by allowing two
transactions to have differing views of the same node, and even allows the node to be mutated by appending new messages