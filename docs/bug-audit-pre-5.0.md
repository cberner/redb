# Pre-5.0 bug audit

Audited at commit ef30223 (2026-08-18). Scope: the full `redb` crate plus `redb-derive` and the
Python bindings, focused on data loss, correctness, and defects whose fix needs a backwards
incompatible change. Security/malicious-input hardening was explicitly out of scope.

Method: seven parallel deep reviews (page store/allocators; cached file + backends + xxh3; btree
core; cursors/iteration; transactions/db; tables/multimap; types/serialization/derive), each
reading its scope line by line, followed by independent re-verification of every major finding
against the code. Several findings were confirmed with throwaway reproduction tests; xxh3 was
differentially tested against the official C reference (12,525 cases, AVX2 and scalar paths);
separator implementations were property-tested over ~3M adversarial pairs. Baseline:
`cargo test --all-features` passes (443 tests) at the audited commit.

Severity tiers: P0 = can corrupt or lose committed data; P1 = correctness bugs without direct
loss of committed data; P2 = minor bugs (panics, hangs, churn, drift). Within each tier, items
are ordered by estimated real-world impact. Confidence and reproduction status are given per
item.

---

## P0: data loss / durable corruption

### 1. `restore_savepoint()` is not poisoned on mid-restore errors; commit then corrupts the database
`src/transactions.rs:1329-1420`. Confidence: certain that the half-restored state is committable
(the corruption endgames are step-by-step traces; the trigger needs a non-latching error).

The data root is swapped to the savepoint's root first (line 1330). After that, only the step-1a
drain failure poisons the transaction. Every other fallible step returns `?` without poisoning:
the `open_system_table(DATA_FREED_TABLE)?` before the drain (line 1344), all of step 2
(`open_system_table(DATA_ALLOCATED_TABLE)?`, `range?`, per-entry `?`), and step 3 (persistent
savepoint listing/deletion). `restored_transaction` -- which is what makes commit drop the
in-memory freed-page records of rolled-back non-durable commits -- is only set as the very last
statement.

A `StorageError::Corrupted` (e.g. from `check_page_order` or the descent-depth bound; these do
NOT latch `io_failed`, unlike real I/O errors) or `LockPoisoned` in that window leaves a
committable transaction in which, variously: the restored root is committed while
`DATA_FREED_TABLE` still holds records of transactions after the savepoint (their pages are
reachable from the restored root -> later freed and reused -> durable corruption); the deferred
in-memory freed records of rolled-back non-durable commits get durably written by
`durable_commit()` with the same effect; pages allocated after the savepoint are never queued
(permanent leak); or stale persistent savepoints survive on disk and reference freed pages after
reopen.

The code's own standard exists on both sides of the gap: step 1a poisons explicitly for exactly
this reason, and `btree_mutator.rs:376-378` states the rule "freed only after every fallible
step". Commit 2471234 (malformed freed-page records now return `Corrupted` instead of panicking)
widened the reachable trigger surface. Fix shape: poison on any error after the root swap.

### 2. `delete_table()` frees the table's pages before the fallible catalog removal
`src/tree_store/table_tree.rs:596-618`. Confidence: certain (ordering defect); trigger needs the
same non-latching error class as item 1.

All pages of the table are pushed to `freed_pages` -- or freed immediately when uncommitted --
and the staged update is discarded, before `self.tree.remove(&name)?` runs. If the remove fails
with a non-latching error, the table stays reachable in the master tree while every one of its
pages is queued for freeing. The error propagates to the caller, but the transaction is still
committable; committing persists the freed-page records and a later commit reclaims and reuses
pages under a live table. Fix shape: remove from the catalog first (or poison on error), freeing
pages only after every fallible step, as the mutator does.

### 3. `rename_table()` half-rename on error: committable silent table loss
`src/tree_store/table_tree.rs:574-586`. Confidence: certain (states reachable on a mid-op
error); same trigger class as items 1-2.

The staged root update is re-keyed to the new name, then `tree.remove(&name)?` and
`tree.insert(&new_name, ...)?` run as two fallible steps. If the insert fails after the remove
succeeded: with no staged update the table exists under neither name and committing durably
loses it (data unreachable, pages leaked); with a staged update, `flush_table_root_updates()`
panics at commit (`.unwrap()` on the missing definition), which aborts the commit via the
allocator latch -- safe but converts an error into a panic plus a forced repair.

Items 1-3 share one root cause: there is no uniform "a failed mutating operation poisons the
transaction" contract. Real I/O errors are latched by the backend, cursor/extract paths poison,
restore step 1a poisons, and the remaining paths silently leave inconsistent committable state.
Making poisoning uniform (and documenting it) is the single fix for the class -- a good 5.0
behavior change.

### 4. `check_integrity()` repair commits reuse a transaction id; 1PC+C recovery can then roll back an fsync-acknowledged commit
`src/db.rs:724-734` (untracked repair commit), `src/transaction_tracker.rs:30-34` (id issuance),
`src/tree_store/page_store/header.rs:328-333` (strict `>` slot selection). Confidence: the id
collision is certain (reproduced); the data-loss step follows in a specific crash window.

`check_integrity_inner()`'s non-promote repair path calls
`mem.commit(..., last_committed + 1, ...)` directly without reserving the id in the
`TransactionTracker` (unlike `Database::new`, which re-seeds the tracker after its repair
commit). The tracker's counter holds the last issued id, so after any user write transaction it
equals the last committed id -- and the next `begin_write()` issues the same id the repair
commit just used. Both on-disk slots can then hold equal transaction ids with different content.
1PC+C recovery resolves the crash-during-fsync case ("only the god byte reached disk") by
`secondary.transaction_id > primary.transaction_id`; with equal ids the comparison keeps the
wrong slot and the user's fsync-acknowledged commit silently disappears (e.g. sequence: repair
commit N, user durable commit N, non-durable commit, durable commit N+2 crashing with only the
god-byte write persisted -> recovery keeps the repair commit).

Design.md documents that 1PC+C depends on monotonically increasing ids, but nothing enforces it.
Fix shape: route repair-commit ids through the tracker (or re-seed after), plus a cheap
monotonicity assert in `TransactionalMemory::commit`.

### 5. Read iterators silently skip an entire subtree when resumed after a mid-iteration `Corrupted` error
`src/tree_store/btree_cursor.rs:188-204` (`move_to_adjacent_leaf`), also
`descend_to_position`. Confidence: certain -- reproduced (silent omission of a key range from an
otherwise normal-looking iteration).

The cursor commits its step (`path[index].child_index = child_index; path.truncate(...)`) before
the fallible `get_page(child_page)?`. I/O errors latch, so retries keep failing; but
`Corrupted` from page-order validation (commit e6a753e) and the depth bound (commit 8bfe8e4)
latch nothing, and the public read surfaces (`Range`, `Table::iter`, read `Cursor`,
`MultimapRange`) carry no per-iterator error fuse (unlike `ExtractIf` and `CursorMut`). A caller
that calls `next()` again after `Some(Err(Corrupted))` -- permitted by the per-item `Result`
API and documented nowhere as forbidden -- resumes past the unreadable child and receives the
rest of the tree with no further error. Reproduced: 5000-key table with one corrupted branch
pointer yields keys 0..=254, one `Err`, then 510..4999 and a clean `None`; 255..509 are silently
absent. This defeats the graceful-error intent of the two recent hardening commits. Fix shape:
fuse read iterators/cursors to their first error, and/or fetch the child page before committing
the step.

---

## P1: correctness bugs (no direct loss of committed data)

### 6. A write transaction dropped during a caught panic leaks its pages permanently; clean shutdown persists the leak
`src/transactions.rs:2556-2571` (`Drop` skips `abort_inner()` when `panicking()`, with no
invalidation), `src/tree_store/page_store/page_manager.rs` (`flush_shutdown_header`),
`src/db.rs` (quick-repair close commit). Confidence: certain -- reproduced (409 leaked pages
surviving repeated clean open/close cycles).

Skipping abort during unwind is deliberate, but nothing marks the in-memory allocator state
tainted, so `catch_unwind` + continue + clean close writes `recovery_required = false` with the
orphaned pages still allocated. The leak survives restarts (quick-load trusts the saved state),
compounds across panic-recover cycles, and is only reclaimed by a full repair. It is also the
easiest way to reach the "not clean" precondition of item 4. Contrast: `commit_inner`'s
`AllocatorStateLatch` handles panic-during-commit correctly; the Drop path lacks the equivalent
(`invalidate_allocator_state()` is already poison-tolerant and would do).

### 7. Type-identity aliasing between user composites and flat user types (4.2.0 regression)
`src/types.rs:121-135` (`into_composite` keeps the name string, sets `UserDefined`).
Confidence: certain -- demonstrated with a live database.

`Vec<FakeU32>` where `FakeU32` declares `TypeName::new("u32")` produces exactly
`(UserDefined, "Vec<u32>")` -- byte-identical to a flat user type declaring
`TypeName::new("Vec<u32>")`. A table written as one opens successfully as the other and returns
framed bytes (or misparses) as values. Pre-4.2 the two were distinguishable (`Internal` vs
`UserDefined`); the 4.2 composite reclassification moved built-in composites of user types into
the user namespace untagged. redb-derive already solves this exact problem with an in-name
`#user` tag; the core composites (`Vec`, `Option`, arrays, tuples) do not. Fix is a format
change (tag user-defined inner names inside composite name strings, keep `matches_legacy` for
old spellings) -- 5.0 is the moment.

### 8. `ReadOnlyTable::get()/range()` (and multimap equivalents) return `'static` guards that do not keep the transaction alive
`src/table.rs:884-915`, `src/multimap_table.rs:1210-1265`. Confidence: certain
(maintainer-known: the removal is already staged behind `experimental-api-5`).

The doc comments claim the guard "keeps the transaction alive until it is dropped"; it does not.
Dropping the `ReadTransaction` while holding a guard lets a concurrent writer free and reuse the
guarded pages: in debug builds the writer panics on the page ref-count assertion; in release it
is memory-safe (the guard reads its old snapshot Arc) but unspecified. The audit confirms the
staged removal is a necessary correctness fix, not an API-taste change -- it should ship in 5.0
unconditionally, not stay behind the flag.

### 9. `DateTime<FixedOffset>`: `compare()` ignores the stored offset, so byte-different keys are Equal
`src/types/chrono_v0_4.rs:229-273`. Confidence: certain -- demonstrated.

`as_bytes` stores the UTC instant plus the offset (bytes 13..17); `compare` deserializes and
uses chrono's `Ord`, which orders by instant only. Two encodings of the same instant with
different offsets compare Equal, so the table keeps one representative, and which offset
survives an overwriting insert is path-dependent (the in-place and same-size clone-patch paths
keep the OLD key bytes, `btree_mutator.rs:837-884`; the rebuild path writes the NEW ones). Not
data loss, but round-trip identity does not hold through the database and the result is
nondeterministic from the API's perspective. 5.0 options: tie-break `compare` on the offset
(sort-order change -> table rewrite) or drop the offset from the encoding.

### 10. Fallback `FileBackend` takes no file lock: no `DatabaseAlreadyOpen` protection on fallback targets
`src/tree_store/page_store/file_backend/fallback.rs:19-23`. Confidence: certain (guard absent);
only affects targets that are neither unix, windows, nor wasi.

`new_internal` ignores the lock parameter entirely. On fallback platforms two processes (or two
`Database` instances in one process) can open the same file read-write and interleave commits,
corrupting it -- silently, with no warning log (the optimized backend at least warns when
locking is unsupported).

---

## P2: minor bugs (panics, hangs, churn, drift)

### 11. Windows `FileBackend::write` can loop forever on `seek_write` returning `Ok(0)`
`src/tree_store/page_store/file_backend/optimized.rs:110-119`. The read loop directly above was
explicitly fixed for this ("...instead of looping forever") and the WASI sibling returns
`WriteZero`; the Windows write loop was missed. A zero-length write report wedges the commit
thread.

### 12. Leaked `pop_first`/`pop_last` guards wedge the page and desync the length header
`src/tree_store/btree_mutator.rs:219-235` (length written via `finish_deletion` before the
guard's deferred removal runs at drop), `btree_base.rs:279-297`. `mem::forget` on the returned
guards leaves the leaf's write-cache slot checked out: every later access panics and commit can
never complete -- so nothing wrong persists, but it is a process-panic trap, and the
length-before-removal ordering is a latent corruption hazard for any future change that makes
commit tolerant of checked-out pages. Same leak-panic applies to other `PageMut`-backed guards
(`insert_reserve`, `get_mut`).

### 13. `StorageBackend::close()` can race in-flight reads from live read transactions
Contract at `src/db.rs:64-71` ("redb will not access the backend after calling this method") vs
`Drop for Database`, which defers close only for live write transactions. A custom backend that
frees resources in `close()` can see a use-after-close its author was told cannot happen.
Built-in backends are unaffected. Fix or re-document in 5.0.

### 14. Multimap: inserting an already-present value into a subtree-backed collection rewrites both trees
`src/multimap_table.rs:660-676`. The inline arm early-returns on `found`
(`multimap_table.rs:585-587`); the subtree arm CoWs the subtree and re-inserts into the outer
tree on a logical no-op. The identical churn on the `remove()` side was already fixed (with a
regression test); the insert side was missed. Page churn / write amplification only.

### 15. `TypeName::from_bytes` panics on unknown classification bytes and non-UTF-8 names
`src/types.rs:36-44,102-111`. Concretely: redb 4.1 panics (`unreachable!`) opening a 4.2 file,
because 4.2 introduced the `Internal3` classification. The next classification addition will do
it again to 4.2 unless 5.0 makes this a proper `Corrupted`/unsupported-format error.

### 16. redb-derive: `#[derive(Value)]` fails on any struct whose lifetime is named `'a`
`crates/redb-derive/src/lib.rs:132-193`. The generated GAT re-declares `'a` and shadows the
user's lifetime (E0496). Compile-time only; rename the generated lifetime.

### 17. `get_mut()` on a missing key CoWs the whole root-to-leaf path before discovering the miss
`src/tree_store/btree.rs:592-686`. Pure write amplification on a no-op (also hit via
`entry()`-style probes on vacant keys). Same class as the fixed multimap-remove churn.

### 18. `AccessGuardMut::insert()` (the `get_mut` value-resize path) never splits the leaf
`src/tree_store/btree_base.rs:376-412`. Growing a value via the guard rebuilds the leaf with
`build()` unconditionally -- no `should_split()` -- so a large value produces one oversized
higher-order page holding all the leaf's pairs. Valid tree (verified: later inserts split it),
but pathological page shapes.

### 19. `replace_leaf_children()` can build a branch exceeding the u16 child limit (exotic configs)
`src/tree_store/btree_mutator.rs:321` -> `RawBranchBuilder`'s `u16::try_from(...).unwrap()`
(`btree_base.rs:2143`). Needs page sizes around 2.5 MiB or more plus a coalescing
retain/extract over higher-order multi-pair leaves; panic mid-operation, no on-disk damage.
Ordinary page sizes provably cannot hit it.

### 20. `mem::forget` on a `Table` handle silently drops that table's writes at commit
`src/transactions.rs:787-800`. A forgotten handle never stages its root; commit succeeds with
the transaction-start root. Consistent state, silently missing writes. Cheap 5.0 hardening:
error at commit when `open_tables` is non-empty.

### 21. Oversized composite members panic inside `as_bytes` before the `ValueTooLarge` check
`src/complex_types.rs:14`, `src/types.rs:536`, `crates/redb-derive/src/lib.rs:391-394`.
A tuple/Vec/array/derived member >= 2^32 bytes hits `u32::try_from(...).unwrap()` during
serialization instead of returning `StorageError::ValueTooLarge`.

### 22. `ExtractIf` yields `Err` forever after latching; a `for` loop that skips errors never terminates
`src/table.rs` extract iterator latch semantics. Consider fusing to `None` after the first
repeated error. Related (exotic): a non-predicate panic escaping
`RangeMut::with_live_cursor` (`btree_cursor.rs:2324-2342`) under `catch_unwind` can lose
already-yielded extract removals without poisoning the transaction -- corruption-induced panics
only.

### 23. Assorted small items
- `restore_savepoint` version mismatch is `assert_eq!` (`transactions.rs:1317`) -- panics
  instead of `SavepointError`; will matter the first time a v4 format exists.
- `rename_table(name, name)` fails with `TableExists` instead of being a no-op.
- `persistent_savepoint()`/`delete_persistent_savepoint()` mark the transaction dirty via the
  system-table open, so a second savepoint in the same transaction fails with
  `InvalidSavepoint` even though the data root is untouched.
- Stale comment: `transactions.rs:381-382` references removed `Durability::Paranoid`;
  `set_quick_repair` doc does not say it is a no-op for `Durability::None` commits;
  `cached_file.rs:749-750` describes a parameter that no longer exists.
- Fallback spin `RwLock::Debug` (no_std) takes a read lock (std prints "<locked>"), a
  theoretical self-deadlock with no current call site.

---

## 5.0 breaking-change agenda

Bug-driven (fixes above that need or deserve the major bump):
1. Uniform transaction error-atomicity: any failed mutating operation poisons the transaction
   (fixes the class behind items 1-3, and makes the contract documentable).
2. Ship the staged `experimental-api-5` removals unconditionally (item 8), including the
   `KeyRange` signature change.
3. TypeName format: tag user-defined inner names in composite name strings (item 7); make
   classification parsing fallible (item 15); consider dropping `matches_legacy` acceptance of
   pre-3.0 composite spellings (documented migration), which currently cannot distinguish
   legacy user composites from built-ins.
4. `DateTime<FixedOffset>`: injective ordering (tie-break on offset) or drop the offset from
   the encoding (item 9).
5. Commit-slot hygiene: enforce transaction-id monotonicity in `commit()` (item 4); gate the
   slot version-byte parse on the slot checksum (today a torn slot's garbled version byte fails
   the whole open with `Corrupted`/`UpgradeRequired` even though the primary is intact).

Format/API opportunities surfaced by the audit (not bugs today):
6. Move `num_full_regions`/`data pages in trailing region` into the checksummed commit slots
   (or checksum the header region). They are unchecksummed and torn-able; correctness currently
   rests on recompute-from-file-length plus `recovery_required` being set during commits.
7. Widen branch/leaf entry counts from u16 to u32. Several subsystems exist only to dance
   around the u16 ceiling (split clamps, in-place gates, the item-19 edge); a u32 count deletes
   them.
8. `Key::separator()` contract: decide whether release builds validate (two extra compares per
   split; a broken user implementation currently mis-routes silently, same trust class as a
   broken `compare`), and consider a `Cow` return so implementations can synthesize separators
   shorter than either input.
9. Iterator-after-error semantics: fuse all read iterators/cursors (item 5), document the
   reversed-range behavior (redb returns empty where `BTreeMap::range` panics), and pin down
   `MultimapCursor`'s entry-level gap semantics before the constructors stabilize.
10. `Durability::None` interplay with clean close: a clean close persists non-durable commits
    (the close-time commit + slot promotion), which is safe but means "None" data can outlive
    the documented expectations; document or gate it.
11. Smaller items: fail commit on forgotten table handles (item 20); document that leaked
    `ReadTransaction`/`Savepoint` pins pages until reopen; `SystemTableDefinition` is `pub` but
    unusable externally; unify `&str`/`String` type identity (byte-identical encodings, distinct
    names); derived `TypeName`s include no module path, so same-named structs in two crates
    collide; branch-page format ideas (truncated child checksums for fanout, eliding the
    zero-width value section in multimap subtree leaves, replacing the in-band `DEFERRED`
    checksum sentinel).

---

## Verified clean (high-effort areas with no findings)

- 1PC+C and 2PC commit ordering, including single-fsync tear analysis, non-durable ->
  durable transitions, the new write-buffer-resident non-durable commits (ca17569), deferred
  freed-page records (b2159c1), and post-commit epilogue page adoption (e433e52). Failed durable
  commits latch and cannot be silently followed by a successful one.
- Freed-page epoch machinery: `free_until` boundaries, non-durable freeing restricted to
  unpersisted pages, savepoint-restore requeue logic, allocated-pages table maintenance. No
  double-free or premature-free scenario survived tracing.
- Repair: slot selection, quick-repair validity checks, layout reconciliation from file length,
  shrink/grow crash windows, v1/v2 rejection.
- Buddy allocator and bitmaps (order math, resize, serialization; state round-trips verified
  against v3.0.0 and v4.0.0 upstream tags), `PageNumber` packing.
- The new `Key::separator()` machinery end to end: contract (`left <= sep < right`) established
  from `child_for_key`, verified at every creation/move site (splits, merges, cursor splice
  bound propagation), `&[u8]`/`&str`/`String` implementations property-tested (~3M pairs,
  UTF-8 boundary rounding included), plus adversarial integration probes around separator
  boundary holes. Fixed-width keys bypass separators by construction.
- Cursor/iteration protocols: double-ended convergence, park/activate deferred-removal batches,
  insert-run splices, gap semantics; extract/retain poison discipline.
- Multimap inline<->subtree transitions (randomized model check), subtree checksum
  finalization, `remove_all` exactly-once freeing, table catalog open/rename/delete guards.
- xxh3 (12,525-case differential vs the C reference, AVX2 + scalar), cache coherency,
  `PageMut` aliasing (panics, never UB), unix/wasi short-I/O handling.
- Tuple/derive/Option/array encodings: ordering lawful and consistent with byte equality;
  round-trips exact; varint framing canonical at all width boundaries.

## Test-coverage recommendations

- The fuzzer uses a `u64`-keyed main table, so `branch_separator` short-circuits (fixed width)
  and the entire new separator path -- including the rewritten cursor-splice `routes` logic --
  is unfuzzed. Add an `&[u8]`- or `&str`-keyed table with prefix-heavy key generation.
- No test resumes a read iterator after an error (item 5's hole).
- The in-tree xxh3 suite has one known-answer test (empty input, 64-bit); the file format
  depends on `hash128_with_seed`. Add 128-bit KATs at each length-class boundary.
- `tests/backward_compatibility.rs` gaps: `char`, `bool`, `String`, one-element tuples, nested
  `Option`, mixed variable-width tuples as values, and no pinned golden bytes for chrono/uuid.
