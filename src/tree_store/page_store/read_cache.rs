use crate::tree_store::page_store::fast_hash::fast_hash64;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Mutex};

// A concurrent map from page offset to page data, used as the read cache of
// `PagedCachedFile`.
//
// The design goal is that cache hits perform no writes to shared memory other
// than the unavoidable `Arc` refcount increment on the returned page. In a
// B-tree every lookup passes through the root page, so any per-entry or
// per-stripe lock (even a reader-writer lock) becomes a globally contended
// cache line that prevents `get()` from scaling with the number of threads.
//
// Structure:
// * A fixed-size, power-of-two array of hash buckets. Each bucket is the head
//   of a singly-linked list of immutable nodes. The array is sized at
//   construction from the cache budget and never grows.
// * Readers (`get`) traverse the list with atomic loads only. They take no
//   locks. The CLOCK `referenced` bit is only written when it is not already
//   set, so hot entries stay on a clean, shared cache line.
// * Writers (insert/remove/evict) serialize on one mutex per shard of the
//   bucket array. They unlink nodes RCU-style: the node is removed from the
//   list but left intact, so concurrent readers can finish traversing it.
//   Before freeing an unlinked node, the writer waits for all in-flight
//   readers to finish (see below).
//
// Reader tracking (userspace RCU, like liburcu's memory-barrier flavor):
// every thread that ever reads owns a cache-line-padded slot in a
// process-wide registry. Only the owning thread writes its slot: on entering
// a critical section it stores `2 * phase + 1` (odd = active, tagged with the
// grace phase observed at entry), on exit `2 * phase + 2` (even = idle).
// `GRACE_PHASE` is a global counter incremented by writers.
//
// After unlinking nodes, a writer increments `GRACE_PHASE` to `P` and then
// waits, for every slot, until it observes an even value or an odd value
// tagged with a phase >= P. Readers' critical sections are tiny (a list
// traversal plus an `Arc` clone) and never block, so this wait is short and
// cannot deadlock.
//
// Why this is safe -- there are three cases per slot:
//
// * Slot is even: the reader is idle. Its exiting Release store synchronizes
//   with the writer's slot load (SeqCst implies Acquire), so everything a
//   previous section did to the nodes happens-before the free. A section that
//   begins after the writer's load is handled by the argument below.
// * Slot is odd with phase >= P: the reader's Acquire load of `GRACE_PHASE`
//   read a value that the writer's increment (or a later one) published.
//   The unlinking stores are sequenced before the increment, so they
//   happen-before the reader's list traversal: the section only sees the
//   rewired lists, from which no unlinked node is reachable (an unlink
//   rewires the node's unique predecessor, and batch-mates are only freed
//   after the same wait).
// * Slot is odd with phase < P: the writer keeps waiting. The reader will
//   eventually exit (even) or re-enter with the new phase. Waiting for the
//   value to merely *change* would be unsound: the next section could have
//   started before the unlinks were visible to it.
//
// The remaining risk is a writer that frees a node while a reader that
// entered before the phase increment is still traversing it (classic store
// buffering: each side's store is not yet visible to the other side's load).
// This is resolved with a pair of SeqCst fences:
//
//   Reader (`active_reader`):            Writer (`wait_for_readers`):
//     slot.store(2 * phase + 1) (Relaxed)  bucket/next.store(..)    (Release)
//     fence(SeqCst)                        GRACE_PHASE.fetch_add(1) (SeqCst)
//     bucket/next.load(..)      (Acquire)  fence(SeqCst)
//                                          slot.load(..)            (Acquire)
//
// SeqCst fences have a single total order. Either the reader's fence precedes
// the writer's fence -- then the writer's slot loads observe the announce
// store, see the section as active with a stale phase, and wait for its exit
// -- or the writer's fence precedes the reader's fence, and the reader's
// loads observe every store made before the writer's fence: the unlinks (so
// the section cannot reach the freed nodes) and the phase increment (so the
// section tags itself >= P, and the writer does not need to have observed its
// announce store).
//
// This is deliberately the fence formulation rather than one built on SeqCst
// loads and stores: it matches the canonical store-buffering pattern, and
// Miri's weak-memory emulation validates it (`concurrent_stress` runs under
// Miri).
//
// The `Arc` refcount of the B-tree root page remains a shared contended line,
// but that is inherent to returning owned `Arc`s from the cache.

// Padded so that no two reader threads share a cache line, and so that writers
// scanning the registry do not disturb readers on adjacent slots.
#[repr(align(128))]
struct ReaderSlot {
    // `2 * phase + 1` while the owning thread is inside a read-side critical
    // section, `2 * phase + 2` after it exits one (even = idle). Written only
    // by the owning thread.
    state: AtomicU64,
}

struct Registry {
    // Every slot ever created, never deallocated. `free` holds slots whose
    // owning thread has exited, for reuse. Leaked memory is bounded by the
    // peak number of concurrently live threads.
    all: Vec<&'static ReaderSlot>,
    free: Vec<&'static ReaderSlot>,
}

// Process-wide (not per-database): a thread has one slot no matter how many
// databases it reads from. Writers of one database may wait on readers of
// another, which only makes an already rare wait slightly longer.
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    all: Vec::new(),
    free: Vec::new(),
});

// Incremented by `wait_for_readers()`. Read-side critical sections tag their
// slot with the phase they observed on entry (see module comment).
static GRACE_PHASE: AtomicU64 = AtomicU64::new(0);

struct SlotHandle {
    slot: &'static ReaderSlot,
}

impl SlotHandle {
    fn acquire() -> Self {
        let mut registry = REGISTRY.lock().unwrap();
        let slot = if let Some(slot) = registry.free.pop() {
            slot
        } else {
            let slot: &'static ReaderSlot = Box::leak(Box::new(ReaderSlot {
                state: AtomicU64::new(0),
            }));
            registry.all.push(slot);
            slot
        };
        Self { slot }
    }
}

impl Drop for SlotHandle {
    fn drop(&mut self) {
        REGISTRY.lock().unwrap().free.push(self.slot);
    }
}

thread_local! {
    static READER_SLOT: SlotHandle = SlotHandle::acquire();
}

// Runs `f` as a read-side critical section: nodes unlinked by writers before
// or during `f` are not freed until `f` returns. `f` must not block, call back
// into this module, or panic (the exit guard makes a panic safe, but the
// critical section should remain short).
#[inline]
fn with_active_reader<R>(f: impl FnOnce() -> R) -> R {
    if let Ok(slot) = READER_SLOT.try_with(|handle| handle.slot) {
        active_reader(slot, f)
    } else {
        // Thread-local storage is gone (thread destruction); use a slot
        // borrowed from the registry for this one call.
        let handle = SlotHandle::acquire();
        active_reader(handle.slot, f)
    }
}

#[inline]
fn active_reader<R>(slot: &ReaderSlot, f: impl FnOnce() -> R) -> R {
    struct ExitGuard<'a> {
        slot: &'a ReaderSlot,
        exit_value: u64,
    }
    impl Drop for ExitGuard<'_> {
        fn drop(&mut self) {
            self.slot.state.store(self.exit_value, Ordering::Release);
        }
    }

    // Acquire pairs with the increment in `wait_for_readers()`: if this load
    // observes phase P, the unlinks that preceded P's publication are visible
    // to this section.
    let phase = GRACE_PHASE.load(Ordering::Acquire);
    debug_assert_eq!(
        slot.state.load(Ordering::Relaxed) % 2,
        0,
        "active_reader() must not be nested"
    );
    slot.state.store(phase * 2 + 1, Ordering::Relaxed);
    fence(Ordering::SeqCst);
    let _guard = ExitGuard {
        slot,
        exit_value: phase * 2 + 2,
    };
    f()
}

// Waits until every read-side critical section that could have observed a
// pointer to an already unlinked node has exited (see module comment). Holding
// the registry lock serializes grace periods and briefly blocks slot
// registration; readers are unaffected.
fn wait_for_readers() {
    let registry = REGISTRY.lock().unwrap();
    let phase = GRACE_PHASE.fetch_add(1, Ordering::SeqCst) + 1;
    fence(Ordering::SeqCst);
    for slot in &registry.all {
        let mut spins = 0u32;
        loop {
            let value = slot.state.load(Ordering::Acquire);
            // Idle, or a section that observed our phase increment (or a
            // later one) and therefore sees the unlinked lists. Sections
            // re-entering with a stale phase keep us waiting, but every entry
            // reloads GRACE_PHASE, so this resolves as soon as the increment
            // propagates.
            if value % 2 == 0 || (value - 1) / 2 >= phase {
                break;
            }
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

struct Node {
    offset: u64,
    data: Arc<[u8]>,
    // CLOCK second-chance bit: set on access, cleared by the eviction sweep.
    referenced: AtomicBool,
    next: AtomicPtr<Node>,
}

// Mutations of a shard's buckets are serialized by its mutex.
struct Shard {
    // Number of nodes in this shard's buckets. Lets eviction skip empty shards.
    entries: usize,
    // Bucket index, relative to the shard's first bucket, where the next
    // eviction sweep resumes.
    clock_hand: usize,
}

pub(super) struct ReadCache {
    buckets: Box<[AtomicPtr<Node>]>,
    shards: Box<[Mutex<Shard>]>,
    // bucket index >> shard_shift == index of the shard owning that bucket
    shard_shift: u32,
    // Rotates the shard where the next eviction sweep starts
    next_evict_shard: AtomicUsize,
}

// About one bucket per expected page keeps chains at ~1 node. The cap bounds
// the bucket array to 16MiB of pointers for very large cache budgets.
const MIN_BUCKETS: usize = 64;
const MAX_BUCKETS: usize = 1 << 21;
const MAX_SHARDS: usize = 64;

impl ReadCache {
    pub(super) fn new(max_entries_hint: usize) -> Self {
        let num_buckets = max_entries_hint
            .next_power_of_two()
            .clamp(MIN_BUCKETS, MAX_BUCKETS);
        let num_shards = MAX_SHARDS.min(num_buckets);
        let shard_shift = (num_buckets / num_shards).trailing_zeros();
        let buckets = (0..num_buckets)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect();
        let shards = (0..num_shards)
            .map(|_| {
                Mutex::new(Shard {
                    entries: 0,
                    clock_hand: 0,
                })
            })
            .collect();
        Self {
            buckets,
            shards,
            shard_shift,
            next_evict_shard: AtomicUsize::new(0),
        }
    }

    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn bucket_index(&self, offset: u64) -> usize {
        // Cast truncates on 32-bit targets, which only shrinks the usable
        // index range; the mask keeps it in bounds.
        (fast_hash64(offset) as usize) & (self.buckets.len() - 1)
    }

    fn shard_for_bucket(&self, bucket: usize) -> &Mutex<Shard> {
        &self.shards[bucket >> self.shard_shift]
    }

    #[inline]
    pub(super) fn get(&self, offset: u64) -> Option<Arc<[u8]>> {
        let bucket = &self.buckets[self.bucket_index(offset)];
        with_active_reader(|| {
            let mut current = bucket.load(Ordering::Acquire);
            while !current.is_null() {
                // SAFETY: non-null pointers reachable from a bucket point to
                // initialized nodes, which are not freed until all active
                // readers have exited (see module comment).
                let node = unsafe { &*current };
                if node.offset == offset {
                    // Avoid dirtying the cache line when the bit is already
                    // set, the common case for hot pages.
                    if !node.referenced.load(Ordering::Relaxed) {
                        node.referenced.store(true, Ordering::Relaxed);
                    }
                    return Some(node.data.clone());
                }
                current = node.next.load(Ordering::Acquire);
            }
            None
        })
    }

    // Inserts `data`, replacing and returning any previous entry for `offset`.
    pub(super) fn insert(&self, offset: u64, data: Arc<[u8]>) -> Option<Arc<[u8]>> {
        let bucket_index = self.bucket_index(offset);
        let replaced = {
            let mut shard = self.shard_for_bucket(bucket_index).lock().unwrap();
            let bucket = &self.buckets[bucket_index];
            let node = Box::into_raw(Box::new(Node {
                offset,
                data,
                referenced: AtomicBool::new(true),
                next: AtomicPtr::new(bucket.load(Ordering::Acquire)),
            }));
            // Release publishes the node's fields; unlike unlinking this does
            // not need SeqCst because it makes no node unreachable.
            bucket.store(node, Ordering::Release);
            shard.entries += 1;
            // Unlink any older entry for the same offset. Searching from the
            // new node's `next` keeps the new entry in place, and readers that
            // traverse the list concurrently find the new entry first.
            //
            // SAFETY: `node` was just linked under the shard lock, and the
            // shard lock is held.
            let replaced = unsafe { Self::unlink(&(*node).next, offset) };
            if replaced.is_some() {
                shard.entries -= 1;
            }
            replaced
        };
        replaced.map(Self::reclaim)
    }

    // Removes the entry for `offset`. The returned `Arc` has no other
    // reference held by this cache (concurrent `get()` calls may still have
    // cloned it).
    pub(super) fn remove(&self, offset: u64) -> Option<Arc<[u8]>> {
        let bucket_index = self.bucket_index(offset);
        let removed = {
            let mut shard = self.shard_for_bucket(bucket_index).lock().unwrap();
            // SAFETY: the shard lock is held.
            let removed = unsafe { Self::unlink(&self.buckets[bucket_index], offset) };
            if removed.is_some() {
                shard.entries -= 1;
            }
            removed
        };
        removed.map(Self::reclaim)
    }

    // Unlinks and returns the first node for `offset` reachable from `link`.
    // The node is left intact so concurrent readers can finish traversing it;
    // the caller must reclaim it with `reclaim()`/`reclaim_batch()`.
    //
    // SAFETY: the caller must hold the lock of the shard containing `link`.
    unsafe fn unlink(link: &AtomicPtr<Node>, offset: u64) -> Option<*mut Node> {
        let mut link = link;
        let mut current = link.load(Ordering::Acquire);
        while !current.is_null() {
            // SAFETY: reachable non-null pointers are valid, per above
            let node = unsafe { &*current };
            if node.offset == offset {
                link.store(node.next.load(Ordering::Acquire), Ordering::Release);
                return Some(current);
            }
            link = &node.next;
            current = link.load(Ordering::Acquire);
        }
        None
    }

    // Waits for in-flight readers, then frees an unlinked node, returning its
    // data. Must not be called while holding a shard lock: that is not a
    // deadlock risk (readers take no locks), but keeps writer stalls local.
    fn reclaim(node: *mut Node) -> Arc<[u8]> {
        wait_for_readers();
        // SAFETY: the node is unlinked, so new readers cannot reach it, and
        // all readers that could have seen it have exited.
        let node = unsafe { Box::from_raw(node) };
        node.data
    }

    fn reclaim_batch(victims: Vec<*mut Node>) {
        if victims.is_empty() {
            return;
        }
        wait_for_readers();
        for victim in victims {
            // SAFETY: as in `reclaim()`. A victim's `next` may point at
            // another victim of the same batch, but nothing is freed until
            // no reader can be traversing any of them.
            drop(unsafe { Box::from_raw(victim) });
        }
    }

    // Evicts unreferenced entries until at least `bytes_needed` bytes are
    // freed or the whole cache has been swept. Entries whose CLOCK bit is set
    // survive one sweep: the first pass clears the bit and the entry is only
    // evicted if it is not accessed again before the hand comes back around.
    // Returns (bytes freed, entries evicted).
    pub(super) fn evict(&self, bytes_needed: usize) -> (usize, u64) {
        let num_shards = self.shards.len();
        let buckets_per_shard = self.buckets.len() / num_shards;
        let start_shard = self.next_evict_shard.fetch_add(1, Ordering::Relaxed) % num_shards;
        let mut victims: Vec<*mut Node> = Vec::new();
        let mut freed = 0usize;
        'shards: for i in 0..num_shards {
            let shard_index = (start_shard + i) % num_shards;
            let mut shard = self.shards[shard_index].lock().unwrap();
            let shard_start = shard_index * buckets_per_shard;
            // Up to two passes over the shard, since the first pass may only
            // clear CLOCK bits. The hand persists across calls, so in steady
            // state each call visits only a few buckets.
            for _ in 0..(2 * buckets_per_shard) {
                if freed >= bytes_needed || shard.entries == 0 {
                    break;
                }
                let bucket = &self.buckets[shard_start + shard.clock_hand];
                shard.clock_hand = (shard.clock_hand + 1) % buckets_per_shard;
                let mut link = bucket;
                let mut current = link.load(Ordering::Acquire);
                while !current.is_null() && freed < bytes_needed {
                    // SAFETY: reachable non-null pointers are valid, and the
                    // shard lock is held for the unlink.
                    let node = unsafe { &*current };
                    let next = node.next.load(Ordering::Acquire);
                    if node.referenced.load(Ordering::Relaxed) {
                        node.referenced.store(false, Ordering::Relaxed);
                        link = &node.next;
                    } else {
                        link.store(next, Ordering::Release);
                        freed += node.data.len();
                        shard.entries -= 1;
                        victims.push(current);
                    }
                    current = next;
                }
            }
            if freed >= bytes_needed {
                break 'shards;
            }
        }
        let evicted = victims.len() as u64;
        Self::reclaim_batch(victims);
        (freed, evicted)
    }

    // Removes all entries whose offset satisfies `predicate`; returns the
    // total bytes removed.
    pub(super) fn remove_matching(&self, predicate: impl Fn(u64) -> bool) -> usize {
        let buckets_per_shard = self.buckets.len() / self.shards.len();
        let mut victims: Vec<*mut Node> = Vec::new();
        let mut freed = 0usize;
        for (shard_index, shard_mutex) in self.shards.iter().enumerate() {
            let mut shard = shard_mutex.lock().unwrap();
            for bucket_index in 0..buckets_per_shard {
                let bucket = &self.buckets[shard_index * buckets_per_shard + bucket_index];
                let mut link = bucket;
                let mut current = link.load(Ordering::Acquire);
                while !current.is_null() {
                    // SAFETY: as in `evict()`
                    let node = unsafe { &*current };
                    let next = node.next.load(Ordering::Acquire);
                    if predicate(node.offset) {
                        link.store(next, Ordering::Release);
                        freed += node.data.len();
                        shard.entries -= 1;
                        victims.push(current);
                    } else {
                        link = &node.next;
                    }
                    current = next;
                }
            }
        }
        Self::reclaim_batch(victims);
        freed
    }

    // Removes everything; returns the total bytes removed.
    pub(super) fn clear(&self) -> usize {
        self.remove_matching(|_| true)
    }
}

impl Drop for ReadCache {
    fn drop(&mut self) {
        // Exclusive ownership: no reader or writer can be active.
        for bucket in &self.buckets {
            let mut current = bucket.load(Ordering::Relaxed);
            while !current.is_null() {
                // SAFETY: reachable nodes are valid and no longer shared
                let node = unsafe { Box::from_raw(current) };
                current = node.next.load(Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ReadCache;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn value_for(offset: u64, len: usize) -> Arc<[u8]> {
        (0..len)
            .map(|i| u8::try_from((offset + u64::try_from(i).unwrap()) % 256).unwrap())
            .collect()
    }

    #[test]
    fn insert_get_remove() {
        let cache = ReadCache::new(1024);
        assert!(cache.get(0).is_none());
        assert!(cache.insert(0, value_for(0, 128)).is_none());
        assert!(cache.insert(4096, value_for(4096, 128)).is_none());
        assert_eq!(cache.get(0).unwrap(), value_for(0, 128));
        assert_eq!(cache.get(4096).unwrap(), value_for(4096, 128));

        // Replacement returns the old value
        let replaced = cache.insert(0, value_for(1, 128)).unwrap();
        assert_eq!(replaced, value_for(0, 128));
        assert_eq!(cache.get(0).unwrap(), value_for(1, 128));

        assert_eq!(cache.remove(4096).unwrap(), value_for(4096, 128));
        assert!(cache.get(4096).is_none());
        assert!(cache.remove(4096).is_none());
    }

    #[test]
    fn remove_returns_unique_arc() {
        let cache = ReadCache::new(1024);
        cache.insert(0, value_for(0, 128));
        assert!(cache.get(0).is_some());
        let mut removed = cache.remove(0).unwrap();
        // The cache holds no other reference, so the buffer is writable
        assert!(Arc::get_mut(&mut removed).is_some());
    }

    #[test]
    fn clock_eviction() {
        let cache = ReadCache::new(64);
        let count = 100u64;
        for i in 0..count {
            cache.insert(i * 4096, value_for(i * 4096, 64));
        }
        // All entries start referenced: the first sweep only clears bits, the
        // second evicts, so requesting everything drains the cache.
        let expected_bytes = usize::try_from(count).unwrap() * 64;
        let (freed, evicted) = cache.evict(expected_bytes);
        assert_eq!(freed, expected_bytes);
        assert_eq!(evicted, count);
        for i in 0..count {
            assert!(cache.get(i * 4096).is_none());
        }

        // With multiple entries present, a small request evicts exactly one
        cache.insert(0, value_for(0, 64));
        cache.insert(4096, value_for(4096, 64));
        let (freed, evicted) = cache.evict(1);
        assert_eq!(freed, 64);
        assert_eq!(evicted, 1);
        let survivors = [0u64, 4096]
            .iter()
            .filter(|&&offset| cache.get(offset).is_some())
            .count();
        assert_eq!(survivors, 1);

        // The survivor is drained even though the get() above re-referenced it
        let (freed, evicted) = cache.evict(usize::MAX);
        assert_eq!(freed, 64);
        assert_eq!(evicted, 1);
    }

    #[test]
    fn remove_matching_and_clear() {
        let cache = ReadCache::new(1024);
        for i in 0..10u64 {
            cache.insert(i * 4096, value_for(i * 4096, 32));
        }
        let freed = cache.remove_matching(|offset| offset >= 5 * 4096);
        assert_eq!(freed, 5 * 32);
        for i in 0..10u64 {
            assert_eq!(cache.get(i * 4096).is_some(), i < 5);
        }
        assert_eq!(cache.clear(), 5 * 32);
        for i in 0..10u64 {
            assert!(cache.get(i * 4096).is_none());
        }
    }

    #[test]
    fn concurrent_stress() {
        let cache = Arc::new(ReadCache::new(256));
        let offsets: Vec<u64> = (0..64u64).map(|i| i * 4096).collect();
        let stop = Arc::new(AtomicBool::new(false));

        let mut readers = vec![];
        for _ in 0..4 {
            let cache = cache.clone();
            let offsets = offsets.clone();
            let stop = stop.clone();
            readers.push(std::thread::spawn(move || {
                let mut hits = 0u64;
                let mut i = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let offset = offsets[i % offsets.len()];
                    if let Some(data) = cache.get(offset) {
                        // Validate contents to catch use-after-free / torn reads
                        assert_eq!(data, value_for(offset, 512));
                        hits += 1;
                    }
                    i += 1;
                }
                hits
            }));
        }

        let mut writers = vec![];
        for w in 0..2 {
            let cache = cache.clone();
            let offsets = offsets.clone();
            writers.push(std::thread::spawn(move || {
                // Miri runs this test to validate the memory model; keep it short there
                let iterations = if cfg!(miri) { 300 } else { 20_000 };
                for i in 0..iterations {
                    let offset = offsets[(i * 7 + w * 13) % offsets.len()];
                    match i % 5 {
                        0..=2 => {
                            cache.insert(offset, value_for(offset, 512));
                        }
                        3 => {
                            cache.remove(offset);
                        }
                        _ => {
                            cache.evict(512);
                        }
                    }
                }
            }));
        }

        for writer in writers {
            writer.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let mut total_hits = 0;
        for reader in readers {
            total_hits += reader.join().unwrap();
        }
        assert!(total_hits > 0);
        cache.clear();
    }
}
