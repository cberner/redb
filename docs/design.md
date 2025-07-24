# redb design

--------------------------------------------------------------------------------------------------

redb is a simple, portable, high-performance, ACID, embedded key-value store.

Each redb database contains a collection of tables, and supports a single writer and multiple
concurrent readers. redb uses MVCC to provide isolation, and provides a single isolation level:
serializable, in which all writes are applied sequentially.

Each table is a key-value mapping providing an interface similar to `BTreeMap`.

# File format
Logically, a redb database file consists of some metadata, and several B-trees:
* pending free tree: mapping from transaction ids to the list of pages they freed
* table tree: name -> table definition mapping of table names to their definitions
* data tree(s) (per one table): key -> value mapping for table

Except for the database metadata, all other data structures are copy-on-write.

All multi-byte integers are stored in little-endian order.

The database file begins with the database header, and is followed by one or more regions. Each region contains a
header, and a data section which is split into many pages. These regions allow for efficient, dynamic, growth of the
database file.

```
<-------------------------------------------- 8 bytes ------------------------------------------->
========================================== Super header ==========================================
---------------------------------------- Header (64 bytes) ---------------------------------------
| magic number                                                                                   |
| magic con.| god byte  | padding               | page size                                      |
| region header pages                           | region max data pages                          |
| number of full regions                        | data pages in trailing region                  |
| padding                                                                                        |
| padding                                                                                        |
| padding                                                                                        |
| padding                                                                                        |
------------------------------------ Commit slot 0 (128 bytes) -----------------------------------
| version   | user nn   | sys nn    | f-root nn | padding                                        |
| user root page number                                                                          |
| user root checksum                                                                             |
| user root checksum (cont.)                                                                     |
| user root length                                                                               |
| system root page number                                                                        |
| system root checksum                                                                           |
| system root checksum (cont.)                                                                   |
| system root length                                                                             |
| padding                                                                                        |
| padding                                                                                        |
| padding                                                                                        |
| padding                                                                                        |
| transaction id                                                                                 |
| slot checksum                                                                                  |
| slot checksum (cont.)                                                                          |
----------------------------------------- Commit slot 1 ------------------------------------------
|                                 Same layout as commit slot 0                                   |
----------------------------------- footer padding (192+ bytes) ------------------------------------
==================================================================================================
| Region header (130 pages) - unused as of v3                                                    |
--------------------------------------------------------------------------------------------------
| Region data                                                                                    |
==================================================================================================
| ...more regions                                                                                |
==================================================================================================
```

## Database super-header

The database super-header is 512 bytes (rounded up to the nearest page) long, and consists of a header and two "commit slots".

### Database header (64 bytes)
The database header contains several immutable fields, such as the database page size, region size, and a magic number.
Transaction data is stored in a double buffered field, and the primary copy is managed by updating a single byte that
controls which transaction pointer is the primary.

* 9 bytes: magic number
* 1 byte: god byte
* 2 byte: padding
* 4 bytes: page size
* 4 bytes: region header pages
* 4 bytes: region max data pages
* 4 bytes: number of full regions
* 4 bytes: data pages in partial trailing region
* 32 bytes: padding to 64 bytes

`magic number` must be set to the ASCII letters 'redb' followed by 0x1A, 0x0A, 0xA9, 0x0D, 0x0A. This sequence is
inspired by the PNG magic number.

`god byte`, so named because this byte controls the state of the entire database, is a bitfield containing three flags:
* first bit: `primary_bit` flag which indicates whether transaction slot 0 or transaction slot 1 contains the latest commit.
* second bit: `recovery_required` flag, if set then the recovery process must be run when opening the database. This can be
  a full repair, in which the region tracker and regional allocator states -- described below -- are reconstructed by walking
  the btree from all active roots, or a quick-repair, in which the state is simply loaded from the allocator state table.
* third bit: `two_phase_commit` flag, which indicates whether the transaction in the primary slot was written using 2-phase
  commit. If so, the primary slot is guaranteed to be valid, and repair won't look at the secondary slot. This flag is always
  updated atomically along with the primary bit.

redb relies on the fact that this is a single byte to perform atomic commits.

`page size` is the size of a redb page in bytes

`region header pages` is the number of pages in each region's header

`region max data pages` is the maximum number of data pages in each region

`number of full regions` stores the number of full regions in the database. This can change during a transaction if the database
grows or shrinks. This field is only valid when the database does not need recovery. Otherwise it must be recalculated from the file length and verified

`pages in partial trailing region` all regions except the last must be full. This stores the number of pages in the last region.
This field is only valid when the database does not need recovery. Otherwise it must be recalculated from the file length and verified

### Transaction slot 0 (128 bytes):
* 1 byte: file format version number
* 1 byte: boolean indicating that user root page is non-null
* 1 byte: boolean indicating that system root page is non-null
* 1 byte: boolean indicating that freed table root page is non-null
* 4 bytes: padding to 64-bit aligned
* 8 bytes: user root page
* 16 bytes: user root checksum
* 8 bytes: user root length
* 8 bytes: system root page
* 16 bytes: system root checksum
* 8 bytes: system root length
* 40 bytes: padding
* 8 bytes: last committed transaction id
* 16 bytes: slot checksum

`version` the file format version of the database. This is stored in the transaction data, so that it can be atomically
changed during an upgrade.

`user root page` is the page number of the root of the user table tree.

`user root checksum` stores the XXH3_128bit checksum of the user root page, which in turn stores the checksum of its child pages.

`user root length` is the number of tables in the user table tree. This field is new in file format v2.

`system root page` is the page number of the root of the system table tree.

`system root checksum` stores the XXH3_128bit checksum of the system root page, which in turn stores the checksum of its child pages.

`system root length` is the number of tables in the system table tree. This field is new in file format v2.

`slot checksum` is the XXH3_128bit checksum of all the preceding fields in the transaction slot.

### Transaction slot 1 (128 bytes):
* Same layout as slot 0

### Region tracker
The region tracker is an array of `BtreeBitmap`s that tracks the page orders which are free in each region.
There are two different places it can be stored: on shutdown, it's written to a page in the data section of
a region, and when making a commit with quick-repair enabled, it's written to an entry in the allocator state
table. The former is valid only after a clean shutdown; the latter is usable even after a crash.
```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| num_allocators                                | sub_allocator_len                              |
| BtreeBitmap data...                                                                            |
==================================================================================================
```

A `BtreeBitmap` is a 64-way tree, where each node is a single value indicating whether any descendant is free.
These nodes are stored as single bits, packed into `u64` values. Every height of the tree is fully populated, except
the leaf layer.

* 4 bytes: tree height
* 4 bytes (repeating): ending offset of layers. Does not include the root layer
* n bytes: tree data

```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| height                                        | end offset...                                  |
==================================================================================================
| Tree data                                                                                      |
==================================================================================================
```

## Region layout
* 1 byte: region format version
* 3 bytes: padding
* 4 bytes: length of the allocator state in bytes
* n bytes: the allocator state

Each region consists of a header containing metadata -- primarily the allocation state of the region's pages -- and a
data section containing pages. Pages have a base size, which defaults to the OS page size, and are
variably sized in higher orders in power of 2 multiples of the base size. The format of pages is
described in the [following section](#b-tree-pages)

```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| version  | padding                            | allocator state length                         |
--------------------------------------------------------------------------------------------------
| Regional allocator state                                                                       |
==================================================================================================
| Region pages                                                                                   |
==================================================================================================
```

### Regional allocator state
The regional allocator is a buddy allocator and allocates pages within the region. Its buddy allocator implementation relies
on the `BtreeBitmap` described above and the `U64GroupedBitmap`. The btree map is used to track the
pages which are free, and the grouped bitmap is used to track the order at which a even address
range has been allocated
* 1 byte: max order
* 3 byte: padding to 32bits aligned
* 4 bytes: number of pages
* 4 byte (repeating): end offset of order allocator free state
* 4 byte (repeating): end offset of order allocator allocated page state
* n bytes: free index data
* n bytes: allocated data

Like the region tracker, there are two different places where the regional allocator state can be
stored. On shutdown, it's written to the region header as described above, and when making a commit
with quick-repair enabled, it's written to an entry in the allocator state table. The former is valid
only after a clean shutdown; the latter is usable even after a crash.

```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| max order | padding                           | number of pages                                |
--------------------------------------------------------------------------------------------------
| Order end offsets...                                                                           |
==================================================================================================
| Order allocator state...                                                                       |
==================================================================================================
```

## B-tree pages

Allocated pages may be of two types: b-tree branch pages, or b-tree leaf pages. The format of each is described below:

### Branch page:
* 1 byte: type
* 1 byte: padding to 16bit alignment
* 2 bytes: num_keys (number of keys)
* 4 byte: padding to 64bit alignment
* repeating (num_keys + 1 times):
* * 16 bytes: child page checksum
* repeating (num_keys + 1 times):
* * 8 bytes: page number
* (optional) repeating (num_keys times):
* * 4 bytes: key end. Ending offset of the key, exclusive
* repeating (num_keys times):
* * n bytes: key data
```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| type     | padding   | number of keys        | padding                                         |
--------------------------------------------------------------------------------------------------
| child page checksum (repeated num_keys + 1 times)                                              |
|                                                                                                |
--------------------------------------------------------------------------------------------------
| child page number (repeated num_keys + 1 times)                                                |
--------------------------------------------------------------------------------------------------
| (optional) key end (repeated num_keys times) | alignment padding                               |
==================================================================================================
| Key data                                                                                       |
==================================================================================================
```
`type` is `2` for a branch page

`num_keys` specifies the number of key in the page

`child page checksum` is an array of checksums of the child pages in the `page number` array

`page number` is an array of child page numbers

`key_end` is an array of ending offsets for the keys. It is optional, MUST NOT be stored for fixed width key types

`alignment padding` optional padding so that the key data begins at a multiple of the key type's required alignment

### Leaf page:
* 1 byte: type
* 1 byte: reserved (padding to 16bits aligned)
* 2 bytes: num_entries (number of pairs)
* (optional) repeating (num_entries times):
* * 4 bytes: key_end
* (optional) repeating (num_entries times):
* * 4 bytes: value_end
* repeating (num_entries times):
* * n bytes: key data
* repeating (num_entries times):
* * n bytes: value data
```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| type     | padding   | number of entries      | (optional) key end (repeated entries times)    |
--------------------------------------------------------------------------------------------------
| (optional) value end (repeated entries times) | (optional) key alignment padding               |
==================================================================================================
| Key data                                      | (optional) value alignment padding             |
==================================================================================================
| Value data                                                                                     |
==================================================================================================
```

`type` is `1` for a leaf page

`num_entries` specifies the number of key-value pairs in the leaf

`key_end` is an array of ending offsets for the keys. It is optional, MUST NOT be stored for fixed width key types

`value_end` is an array of ending offsets for the values. It is optional, MUST NOT be stored for fixed width value types

`key alignment padding` optional padding so that the key data begins at a multiple of the key type's required alignment

`value alignment padding` optional padding so that the value data begins at a multiple of the value type's required alignment

# Commit strategies

All data is checksumed when written, using a non-cryptographic Merkle tree with XXH3_128. This
allows a partially committed transaction to be detected and rolled back, after a crash.

All redb transactions are atomic, and use one of the following commit strategies.

## Non-durable commits
redb supports "non-durable" commits, meaning that there is no guarantee of durability. However, in the event of a crash
the database is still guaranteed to be consistent, and will return to either the last non-durable commit or the last
full commit that was made.
Non-durable commits are implemented with an in-memory flag that directs readers to read from the secondary page,
even though it is not yet promoted to the primary.
In the event of a crash, the database will simply rollback to the primary page and the allocator state can be safely
rebuilt via the normal repair process.
Note that freeing pages during a non-durable commit is not permitted, because it could be rolled back at anytime.

## 1-phase + checksum durable commits (1PC+C)
A reduced latency commit strategy is used by default. A commit is performed with a single `fsync`.
First, all data and checksums are written, along with a monotonically incrementing transaction
id, then the primary is flipped and `fsync` called. If a crash occurs, we must verify that the primary has a larger
transaction id and that all of its checksums are valid. If this check fails, then the database will replace the partially
updated primary with the secondary.

Below we give a brief correctness analysis of this 1-phase commit approach, and consider the different failure cases that could
occur during the fsync:

1. If all or none of the data for the transaction is written to disk, this strategy clearly works
2. If some, but not all the data is written, then there are a few cases to consider:
   1. If the bit controlling the primary page is not updated, then the transaction has no effect and this approach is safe
   2. If it is updated, then we must verify that the transaction was completely written, or roll it back:
      1. If none of the transaction data was written, we will detect that the transaction id is older, and roll it back
      2. If some, but not all was written, then the checksum verification will fail, and it will be rolled back.

## 2-phase durable commits (2PC)
A 2-phase commit strategy can also be used which mitigates a theoretical attack when handling malicious data
and when the attacker has high degree of control over the redb process (see below).
First, data is written to a new copy of the btree, second an `fsync` is performed,
finally the byte controlling which copy of the btree is the primary is flipped and a second `fsync` is performed.

### Security of 1PC+C
Given that the 1PC+C commit strategy relies on a non-cyptographic checksum (XXH3) there is, at least in theory, a way to attack it.
Users who need to accept malicious input are encouraged to use 2PC instead.

An attacker, could make a partially committed transaction appear as if it were completely committed.
The scenario would be something like:
```
table.insert(malicious_key, malicious_value);
table.insert(good_key, good_value);
txn.commit();
```
and the attacker wants the transaction to appear as:
```
table.insert(malicious_key, malicious_value);
txn.commit();
```
To do this they need to:
1) control the order in which pages are flushed to disk, so that the ones related to `good_key` are never written
2) introduce a crash during, or immediately before, the fsync() operation of the commit
3) ensure that the checksums for the partially written data are valid

With complete control over the workload, or read access to the database file (3) is possible, since XXH3 is not collision resistant.
However, it requires the attacker to have knowledge of the database contents, because the input to the checksum includes
many other values (all the other keys in the b-tree root, along with their child node numbers)

# MVCC (multi-version concurrency control)

redb uses MVCC to isolate transactions from one another. This is implemented on top of the copy-on-write
b-tree data structure which underlies most of redb. Read transactions make a private copy of the root
of the b-tree, and are registered in the database so that no pages that root references are freed.
When a write transaction frees a page it is pushed into a queue, and only reused after all read transactions
that could reference it have completed.

## Savepoints

Savepoints and rollback are implemented on the same MVCC structures. When a savepoint is created it
registers as a read transaction to preserve a snapshot of the database. Additionally, it saves a copy
of the page allocator state -- this is about 64kB per 1GB of values (assuming 4kb pages). To rollback
the database to a savepoint the root of the b-tree is restored, and the snapshot of the page allocator
is diff'ed against the currently allocated pages to determine which have been allocated since the savepoint
was created. These pages are then queued to be freed, completing the rollback.

Savepoints come in two varieties:
1. Ephemeral. These savepoints are immediately deallocated when they are dropped
2. Persistent. These savepoints are persisted in the database file and therefore across restarts.
   They are stored in a table inside the system table tree. They must be explicitly deallocated.

## Epoch based reclamation
As described above transactions and savepoints rely on epoch based reclamation of pages to ensure
that pages are only freed after they are no longer referenced. Care must be taken to ensure that a
page which is still referenced is never freed. The high level design is described below:

### Definitions
* Transaction states:
  * Uncommitted. An in-progress write transaction
  * Committed. A committed write transaction
  * Aborted. An aborted write transaction
  * Orphaned. A read or write transaction which is no longer referenced
  * Reclaimed. A write transaction which became orphaned and all of its pending free pages have
    been reclaimed
* Dirty page: a page that is allocated and the transaction in which it was allocated has not committed yet
* Committed page: a page that is allocated and the transaction in which it was allocated has committed
* Pending free page: a page that is allocated that is no longer reachable from the data tree or system
  tree roots, as of the associated transaction. These are stored in the freed tree.

### Invariants
The following invariants must be maintained
* Committed pages are never modified in-place
* Committed pages may only be freed by transitioning into the "pending free page" state, and after
  the associated transaction and all prior transactions have reached the Orphaned state
* Pages will only contain page pointers to pages in the same, or an earlier, transaction. This follows
  from the fact that committed pages are never modified, and transaction ids being monotonically increasing
* Freed tree must only contain pages in the pending free page state

### Key operations
#### Modification during write transactions
Write transactions uphold the required invariants through a very simple mechanism. All modifications
to commited pages are performed copy-on-write. That is a new page is allocated, modifications are
made in the new page, and a new b-tree is constructed to reference the new page. Dirty pages are
allowed to be modified in-place -- usually accomplished by freeing them.
The normal tables, system tables, and also the freed tree use this approach.
When a transaction aborts, all pages it allocated are immediately freed.

#### Savepoint restore
Restoring a savepoint brings all normal tables back to the state they were in when the savepoint was
captured. The savepoint system tables are unaffected, and the freed trees need to be put into a
consistent state.
Restoring the normal tables is trivial, since the savepoint captures the data root.

The freed table needs to be updated in two ways:
1) the restored data root from the savepoint must reference only committed pages, so we remove all
   pending free pages from future transactions that are in the data freed tree.
2) all pages that become unreachable by restoring the savepoint data root must be added to the freed
   table. This is done by adding all pages in the allocated pages table from the data tree to the
   data freed tree.

#### Database repair
To repair the database after an unclean shutdown we must:
1) Update the super header to reference the last fully committed transaction
2) Update the allocator state, so that it is consistent with all the database roots in the above
   transaction

If the last commit before the crash had quick-repair enabled, then these are both trivial. The
primary commit slot is guaranteed to be valid, because it was written using 2-phase commit, and
the corresponding allocator state is stored in the allocator state table.

Otherwise, we need to perform a full repair:

For (1), if the primary commit slot is invalid we switch to the secondary slot.

For (2), we rebuild the allocator state by walking the following trees and marking all referenced
pages as allocated:
* data tree
* system tree
* freed tree, and all pending free pages contained within
All pages referenced by a savepoint must be contained in the above, because it is either:
a) referenced directly by the data, system, or freed tree -- i.e. it's a committed page
b) it is not referenced, in which case it is in the pending free state and is contained in the freed tree

# Version changes
## v1
Initial file format

## v2
Added a length field to both btrees and tables. This allows for constant time `len()`

## v3
* Removed the freed tree. Instead, these pages are stored in two system tables, one for the data tree
and one for the system tree.
* Added an "allocated pages" system table which tracks the pages allocated by each transaction when
  a savepoint exists
* Removed the allocator state. Instead, the "quick repair" code path is used.

# Assumptions about underlying media
redb is designed to be safe even in the event of power failure or on poorly behaved media.
Therefore, we make only a few assumptions about the guarantees provided by the underlying filesystem:
1. single byte writes are atomic: i.e., each byte will be written completely or not at all,
   even in the event of a power failure
2. following an `fsync` operation writes are durable
3. ["powersafe overwrite"](https://www.sqlite.org/psow.html): When an application writes
   a range of bytes in a file, no bytes outside of that range will change,
   even if the write occurs just before a crash or power failure. sqlite makes this same
   assumption, by default, in all modern versions.
