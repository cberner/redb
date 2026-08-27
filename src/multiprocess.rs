//! Cross-process coordination for a database file shared between processes, built on the
//! byte-range locks of docs/design.md's multi-process locking protocol. Aside from the
//! header lock, which spans the real header bytes, everything here operates on offsets far
//! past any possible end of file: those locks are a pure namespace, and never touch data.
//!
//! The coordinator operates on the open file description the participant's storage I/O
//! goes through. Locks belong to the description they are taken through, so distinct
//! participants -- in one process or in many -- contend with each other, and a
//! participant's locks live exactly as long as its description. Sharing the storage's
//! description is load-bearing on Windows, where these locks are mandatory: the header
//! lock covers the header bytes themselves, and only the description holding it can
//! perform the header I/O it exists to serialize. What a scan sees of this participant's
//! own pins differs by platform -- a probe never conflicts with its own description's
//! locks on Linux and the Apple platforms, while on Windows a probe is an acquisition and
//! does -- so the scan filters its own pins out where the platform reports them, and the
//! caller still combines the floor with its own tracker, which knows this participant's
//! transactions exactly.

use crate::transaction_tracker::TransactionId;
use crate::tree_store::range_lock::{
    self, LockKind, Probe, lock_blocking, probe, try_lock_byte, unlock_byte,
};
use crate::{Result, StorageError};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Mutex;

/// The header lock: the super-header bytes, which readers of the header lock shared and
/// writers of it lock exclusively. The length equals `DB_HEADER_SIZE`, asserted where the
/// two definitions can both be named.
pub(crate) const HEADER_LOCK_START: u64 = 0;
pub(crate) const HEADER_LOCK_LEN: u64 = 320;

/// The base of the coordination namespace, far past any possible file size.
pub(crate) const LOCK_BASE: u64 = 1 << 62;
/// Held exclusively by the active writer: the writing process in single-writer mode, the
/// active write transaction in multi-writer mode.
#[cfg(test)]
pub(crate) const WRITER_BYTE: u64 = LOCK_BASE;
/// Whether the database is open for a single writer (held exclusively by it) or for many
/// (held shared by each writing process while the database is open).
#[cfg(test)]
pub(crate) const SHARED_WRITER_BYTE: u64 = LOCK_BASE + 1;
/// Held shared by every read-only multi-process handle while the database is open, so that
/// a single-process open conflicts with a live reader no matter what the reader is doing.
#[cfg(test)]
pub(crate) const SHARED_READER_BYTE: u64 = LOCK_BASE + 2;
/// Transaction id `t` is pinned by holding the byte `TXN_BASE + t` shared.
pub(crate) const TXN_BASE: u64 = LOCK_BASE + 1024;

fn lock_error(err: std::io::Error) -> StorageError {
    StorageError::Io(err)
}

/// The byte pinning transaction `id`. The active transaction range ends at the last
/// addressable offset, so an id past it -- unreachable by counting commits, but readable
/// out of a corrupt header -- has no byte, and is refused rather than wrapped onto some
/// other lock's offset.
fn transaction_byte(id: TransactionId) -> Result<u64> {
    match TXN_BASE.checked_add(id.raw_id()) {
        Some(offset) if offset < 1 << 63 => Ok(offset),
        _ => Err(StorageError::Corrupted(format!(
            "transaction id {} is outside the multi-process lock range",
            id.raw_id()
        ))),
    }
}

/// Coordinates this handle with the other processes using the database file. One per open
/// multi-process handle; the storage layer knows nothing of it.
pub(crate) struct Coordinator {
    file: File,
    // Local holders per pinned transaction id. The byte lock does not nest through one file
    // description -- a second acquisition is absorbed and a single release drops it -- so the
    // lock is taken when the count rises from zero and released when it returns there.
    // The mutex also serializes every header-lock section through the description: a
    // same-description acquisition converts an existing hold instead of conflicting with
    // it, and the unlock then releases whatever remains, so two overlapping sections would
    // dismantle each other's holds
    pins: Mutex<BTreeMap<u64, u64>>,
}

impl Coordinator {
    /// `file` must be the description this participant's storage I/O goes through -- never
    /// a fresh open of its own, and never one shared with another participant. A separate
    /// open could not hold the header lock on Windows, where the lock is mandatory, without
    /// failing the storage's own header I/O; a description shared between participants
    /// could neither exclude the other nor reliably see its pins.
    #[cfg(test)]
    pub(crate) fn new(file: File) -> Self {
        Self {
            file,
            pins: Mutex::new(BTreeMap::new()),
        }
    }

    /// Pins the transaction `read_id` returns, and returns it, keeping every page its
    /// snapshot references from being reclaimed by any process until the matching
    /// [`Self::unpin_transaction`]. `read_id` runs inside a shared hold of the header lock
    /// and the pin byte is taken before the hold ends. A reader beginning a transaction
    /// reads the currently committed id there, so its pin always names the snapshot still
    /// published: a writer's reclamation scan -- which holds the header exclusively --
    /// either sees the pin, or finished before the hold began and so never reclaimed that
    /// snapshot, since a scan reclaims only pages already dead in the published state. A
    /// caller pinning an id it already knows must itself guarantee the id cannot be
    /// collected while this call runs, the way a transaction a persistent savepoint
    /// references is guaranteed by the savepoint's presence in the database.
    pub(crate) fn pin_transaction(
        &self,
        read_id: impl FnOnce() -> Result<TransactionId>,
    ) -> Result<TransactionId> {
        let mut pins = self.pins.lock().unwrap();
        lock_blocking(
            &self.file,
            HEADER_LOCK_START,
            HEADER_LOCK_LEN,
            LockKind::Shared,
        )
        .map_err(lock_error)?;
        let pinned = Self::pin_under_header_lock(&self.file, &mut pins, read_id);
        let released = range_lock::unlock(&self.file, HEADER_LOCK_START, HEADER_LOCK_LEN);
        let id = pinned?;
        if let Err(err) = released {
            // The pin stands but its holder will never be recorded, so nothing would ever
            // release it: undo it before reporting, keeping the table and the byte agreed
            let _ = Self::unpin_locked(&self.file, &mut pins, id);
            return Err(lock_error(err));
        }
        Ok(id)
    }

    fn pin_under_header_lock(
        file: &File,
        pins: &mut BTreeMap<u64, u64>,
        read_id: impl FnOnce() -> Result<TransactionId>,
    ) -> Result<TransactionId> {
        let id = read_id()?;
        let byte = transaction_byte(id)?;
        if let Some(count) = pins.get_mut(&id.raw_id()) {
            *count += 1;
            return Ok(id);
        }
        match try_lock_byte(file, byte, LockKind::Shared) {
            // A pin byte is only ever held shared or probed under an exclusive header lock,
            // which the shared hold above excludes, so the lock cannot be refused
            Ok(true) => {
                pins.insert(id.raw_id(), 1);
                Ok(id)
            }
            Ok(false) => Err(StorageError::Corrupted(
                "another process holds a transaction pin exclusively".to_string(),
            )),
            Err(err) => Err(lock_error(err)),
        }
    }

    /// Releases one hold on `id`. The byte is released without the header lock: dropping a
    /// pin can only widen what a scanning writer may reclaim, and a scan that misses the
    /// release simply reclaims less.
    pub(crate) fn unpin_transaction(&self, id: TransactionId) -> Result {
        let mut pins = self.pins.lock().unwrap();
        Self::unpin_locked(&self.file, &mut pins, id)
    }

    fn unpin_locked(file: &File, pins: &mut BTreeMap<u64, u64>, id: TransactionId) -> Result {
        let count = pins
            .get_mut(&id.raw_id())
            .expect("unpinned a transaction that was not pinned");
        *count -= 1;
        if *count == 0 {
            pins.remove(&id.raw_id());
            // Only ids that mapped to a byte were ever pinned
            unlock_byte(file, transaction_byte(id)?).map_err(lock_error)?;
        }
        Ok(())
    }

    /// The oldest transaction pinned by another participant, or `None` when no other
    /// participant pins anything. Pins exist only between the collection horizon and the
    /// last committed transaction, so the scan is bounded by `floor` and `ceiling`, both
    /// inclusive. The floor must be inclusive: reclamation behind a pin stops at the
    /// transaction after it -- the pinned snapshot no longer references pages its own
    /// commit freed -- so the horizon can advance to the pinned transaction itself while
    /// the pin still stands. The horizon is always derived from a commit no newer than the
    /// last one, so a header claiming otherwise is corrupt, and is refused rather than
    /// read as an empty range that would silence every scan. This participant's own pins
    /// are never reported, but a transaction both it and another participant pin may be
    /// skipped, so the caller combines the result with its own tracker, which knows this
    /// participant's transactions exactly.
    pub(crate) fn oldest_foreign_pin(
        &self,
        floor: TransactionId,
        ceiling: TransactionId,
    ) -> Result<Option<TransactionId>> {
        if ceiling.raw_id() < floor.raw_id() {
            return Err(StorageError::Corrupted(format!(
                "collection horizon {} is past the last committed transaction {}",
                floor.raw_id(),
                ceiling.raw_id()
            )));
        }
        // Every id scanned is at most the ceiling, so this bounds the whole scan's offsets
        transaction_byte(ceiling)?;
        // Held across the exclusive header section: see the invariant on the field
        let pins = self.pins.lock().unwrap();
        lock_blocking(
            &self.file,
            HEADER_LOCK_START,
            HEADER_LOCK_LEN,
            LockKind::Exclusive,
        )
        .map_err(lock_error)?;
        let result = oldest_pinned(&self.file, floor.raw_id(), ceiling.raw_id(), &pins);
        let released = range_lock::unlock(&self.file, HEADER_LOCK_START, HEADER_LOCK_LEN);
        let oldest = result.map_err(lock_error)?;
        released.map_err(lock_error)?;
        Ok(oldest.map(TransactionId::new))
    }
}

/// The lowest id in `low..=high` whose pin byte is held by another participant, found
/// without enumerating: byte locks cannot be listed. Where the platform reports a
/// conflicting lock's offset the search descends -- probe the whole range, step to just
/// below the conflict it names, repeat -- which costs one probe per pin stepped over, and
/// never meets `own` pins, since such a probe is blind to its own description's locks.
/// Where it does not (Windows), the range is bisected on the monotone predicate "is
/// anything at or below the midpoint", in `log2(high - low)` probes; there a probe is an
/// acquisition released at once, which does collide with this description's own pins, so
/// ids `own` claims are stepped over. An id pinned both locally and by another participant
/// may be skipped that way -- the caller covers it from its own table.
fn oldest_pinned(
    file: &File,
    mut low: u64,
    high: u64,
    own: &BTreeMap<u64, u64>,
) -> std::io::Result<Option<u64>> {
    let mut best: Option<u64> = None;
    let mut ceiling = high;
    loop {
        if low > ceiling {
            return Ok(best);
        }
        let len = ceiling - low + 1;
        match probe(file, TXN_BASE + low, len, LockKind::Exclusive)? {
            Probe::Free => return Ok(best),
            Probe::Conflict { start: Some(start) } => {
                let id = start.saturating_sub(TXN_BASE).max(low);
                best = Some(best.map_or(id, |b| b.min(id)));
                if id == low {
                    return Ok(best);
                }
                ceiling = id - 1;
            }
            Probe::Conflict { start: None } => {
                let lowest = bisect_lowest(file, low, ceiling)?;
                if !own.contains_key(&lowest) {
                    return Ok(Some(lowest));
                }
                // Our own pin is the lowest held here: resume the scan just above it
                low = lowest + 1;
            }
        }
    }
}

/// The bisection arm: something in `low..=high` is held, and the platform cannot say where.
fn bisect_lowest(file: &File, mut low: u64, mut high: u64) -> std::io::Result<u64> {
    while low < high {
        let mid = low + (high - low) / 2;
        let held = matches!(
            probe(file, TXN_BASE + low, mid - low + 1, LockKind::Exclusive)?,
            Probe::Conflict { .. }
        );
        if held {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::OpenOptions;
    use std::path::Path;

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    fn coordinator(path: &Path) -> Coordinator {
        Coordinator::new(reopen(path))
    }

    fn byte_is_free(file: &File, offset: u64, kind: LockKind) -> bool {
        let acquired = try_lock_byte(file, offset, kind).unwrap();
        if acquired {
            unlock_byte(file, offset).unwrap();
        }
        acquired
    }

    #[test]
    fn header_lock_length_matches_the_header() {
        assert_eq!(
            HEADER_LOCK_LEN,
            u64::try_from(crate::tree_store::DB_HEADER_SIZE).unwrap()
        );
    }

    #[test]
    fn the_shared_writer_byte_negotiates_the_mode() {
        let tmpfile = crate::create_tempfile();
        let sole = reopen(tmpfile.path());
        let other = reopen(tmpfile.path());

        // A sole writer holds the byte exclusively, so a would-be co-writer is refused ...
        assert!(try_lock_byte(&sole, SHARED_WRITER_BYTE, LockKind::Exclusive).unwrap());
        assert!(!try_lock_byte(&other, SHARED_WRITER_BYTE, LockKind::Shared).unwrap());
        unlock_byte(&sole, SHARED_WRITER_BYTE).unwrap();

        // ... and a multi-writer cohort holds it shared, refusing a sole writer
        assert!(try_lock_byte(&other, SHARED_WRITER_BYTE, LockKind::Shared).unwrap());
        let third = reopen(tmpfile.path());
        assert!(try_lock_byte(&third, SHARED_WRITER_BYTE, LockKind::Shared).unwrap());
        assert!(!try_lock_byte(&sole, SHARED_WRITER_BYTE, LockKind::Exclusive).unwrap());
        unlock_byte(&other, SHARED_WRITER_BYTE).unwrap();
        unlock_byte(&third, SHARED_WRITER_BYTE).unwrap();

        // Read-only handles announce themselves the same way: shared with each other, and
        // conflicting with the exclusive whole-range lock a single-process open takes
        assert!(try_lock_byte(&other, SHARED_READER_BYTE, LockKind::Shared).unwrap());
        assert!(try_lock_byte(&third, SHARED_READER_BYTE, LockKind::Shared).unwrap());
        assert!(!try_lock_byte(&sole, SHARED_READER_BYTE, LockKind::Exclusive).unwrap());
        unlock_byte(&other, SHARED_READER_BYTE).unwrap();
        unlock_byte(&third, SHARED_READER_BYTE).unwrap();
    }

    #[test]
    fn pins_are_counted_per_handle_and_held_once() {
        let tmpfile = crate::create_tempfile();
        let coordinator = coordinator(tmpfile.path());
        let observer = reopen(tmpfile.path());
        let id = TransactionId::new(7);

        // Two local holders, one byte lock: the byte stays held across the first release,
        // because a re-lock through one description would not have nested
        assert_eq!(coordinator.pin_transaction(|| Ok(id)).unwrap(), id);
        assert_eq!(coordinator.pin_transaction(|| Ok(id)).unwrap(), id);
        assert!(!byte_is_free(&observer, TXN_BASE + 7, LockKind::Exclusive));
        coordinator.unpin_transaction(id).unwrap();
        assert!(!byte_is_free(&observer, TXN_BASE + 7, LockKind::Exclusive));
        coordinator.unpin_transaction(id).unwrap();
        assert!(byte_is_free(&observer, TXN_BASE + 7, LockKind::Exclusive));

        // Shared with another process's pin of the same transaction
        coordinator.pin_transaction(|| Ok(id)).unwrap();
        assert!(byte_is_free(&observer, TXN_BASE + 7, LockKind::Shared));
        coordinator.unpin_transaction(id).unwrap();
    }

    #[test]
    fn the_scan_sees_other_handles_but_not_its_own() {
        let tmpfile = crate::create_tempfile();
        let scanning = coordinator(tmpfile.path());
        let other = coordinator(tmpfile.path());
        let ceiling = TransactionId::new(1 << 20);

        // Its own pin is never reported: the caller accounts for its own transactions
        scanning
            .pin_transaction(|| Ok(TransactionId::new(5)))
            .unwrap();
        assert_eq!(
            scanning
                .oldest_foreign_pin(TransactionId::new(0), ceiling)
                .unwrap(),
            None
        );

        // Another handle's pins are found, lowest first, wherever they sit
        other
            .pin_transaction(|| Ok(TransactionId::new(77777)))
            .unwrap();
        other
            .pin_transaction(|| Ok(TransactionId::new(1234)))
            .unwrap();
        assert_eq!(
            scanning
                .oldest_foreign_pin(TransactionId::new(0), ceiling)
                .unwrap(),
            Some(TransactionId::new(1234))
        );

        // The floor is inclusive: the horizon can advance to a pinned transaction while
        // the pin still stands, and the pin must still be seen there
        assert_eq!(
            scanning
                .oldest_foreign_pin(TransactionId::new(1234), ceiling)
                .unwrap(),
            Some(TransactionId::new(1234))
        );
        assert_eq!(
            scanning
                .oldest_foreign_pin(TransactionId::new(1235), ceiling)
                .unwrap(),
            Some(TransactionId::new(77777))
        );
        assert_eq!(
            scanning
                .oldest_foreign_pin(TransactionId::new(77778), ceiling)
                .unwrap(),
            None
        );

        other.unpin_transaction(TransactionId::new(77777)).unwrap();
        other.unpin_transaction(TransactionId::new(1234)).unwrap();
        scanning.unpin_transaction(TransactionId::new(5)).unwrap();
    }

    #[test]
    fn the_scan_and_the_bisection_agree() {
        let tmpfile = crate::create_tempfile();
        let scanning = reopen(tmpfile.path());
        let span = 1u64 << 30;

        // A deliberately unseeded multiplicative generator: the ids only need to be spread
        let mut state = 0x5eed_1234_9876_4321u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 34
        };

        for _ in 0..50 {
            let mut holders = Vec::new();
            let count = (next() % 5) + 1;
            for _ in 0..count {
                let id = next() % span;
                let holder = reopen(tmpfile.path());
                if try_lock_byte(&holder, TXN_BASE + id, LockKind::Shared).unwrap() {
                    holders.push((holder, id));
                }
            }
            let expected = holders.iter().map(|(_, id)| *id).min();

            let descended = oldest_pinned(&scanning, 0, span, &BTreeMap::new()).unwrap();
            assert_eq!(descended, expected);
            if let Some(lowest) = expected {
                assert_eq!(bisect_lowest(&scanning, 0, span).unwrap(), lowest);
            }

            for (holder, id) in &holders {
                unlock_byte(holder, TXN_BASE + id).unwrap();
            }
            assert_eq!(
                oldest_pinned(&scanning, 0, span, &BTreeMap::new()).unwrap(),
                None
            );
        }
    }

    #[test]
    fn the_writer_byte_hands_over_to_a_blocked_waiter() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(try_lock_byte(&holder, WRITER_BYTE, LockKind::Exclusive).unwrap());

        let path = tmpfile.path().to_path_buf();
        let waiter = std::thread::spawn(move || {
            let waiter = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            range_lock::lock_byte_blocking(&waiter, WRITER_BYTE, LockKind::Exclusive).unwrap();
            unlock_byte(&waiter, WRITER_BYTE).unwrap();
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        unlock_byte(&holder, WRITER_BYTE).unwrap();
        waiter.join().unwrap();
    }

    #[test]
    fn the_header_lock_admits_readers_together_and_a_writer_alone() {
        let tmpfile = crate::create_tempfile();
        let first = reopen(tmpfile.path());
        let second = reopen(tmpfile.path());

        assert!(
            range_lock::try_lock(&first, HEADER_LOCK_START, HEADER_LOCK_LEN, LockKind::Shared)
                .unwrap()
        );
        assert!(
            range_lock::try_lock(
                &second,
                HEADER_LOCK_START,
                HEADER_LOCK_LEN,
                LockKind::Shared
            )
            .unwrap()
        );
        let third = reopen(tmpfile.path());
        assert!(
            !range_lock::try_lock(
                &third,
                HEADER_LOCK_START,
                HEADER_LOCK_LEN,
                LockKind::Exclusive
            )
            .unwrap()
        );
        range_lock::unlock(&first, HEADER_LOCK_START, HEADER_LOCK_LEN).unwrap();
        range_lock::unlock(&second, HEADER_LOCK_START, HEADER_LOCK_LEN).unwrap();
        assert!(
            range_lock::try_lock(
                &third,
                HEADER_LOCK_START,
                HEADER_LOCK_LEN,
                LockKind::Exclusive
            )
            .unwrap()
        );
        range_lock::unlock(&third, HEADER_LOCK_START, HEADER_LOCK_LEN).unwrap();
    }

    #[test]
    fn the_id_is_read_and_pinned_under_one_header_hold() {
        let tmpfile = crate::create_tempfile();
        let coordinator = coordinator(tmpfile.path());
        let observer = reopen(tmpfile.path());

        // A writer's scan -- an exclusive header acquisition -- cannot begin between the
        // read of the id and the placement of the pin
        let id = coordinator
            .pin_transaction(|| {
                assert!(
                    !range_lock::try_lock(
                        &observer,
                        HEADER_LOCK_START,
                        HEADER_LOCK_LEN,
                        LockKind::Exclusive
                    )
                    .unwrap()
                );
                Ok(TransactionId::new(42))
            })
            .unwrap();
        assert_eq!(id, TransactionId::new(42));

        // Afterwards the header is free again and the pin stands
        assert!(
            range_lock::try_lock(
                &observer,
                HEADER_LOCK_START,
                HEADER_LOCK_LEN,
                LockKind::Exclusive
            )
            .unwrap()
        );
        range_lock::unlock(&observer, HEADER_LOCK_START, HEADER_LOCK_LEN).unwrap();
        assert!(!byte_is_free(&observer, TXN_BASE + 42, LockKind::Exclusive));
        coordinator
            .unpin_transaction(TransactionId::new(42))
            .unwrap();
    }

    #[test]
    fn the_pin_section_and_the_scan_exclude_each_other_in_process() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let tmpfile = crate::create_tempfile();
        let coordinator = coordinator(tmpfile.path());
        let entered = AtomicBool::new(false);
        let section_done = AtomicBool::new(false);

        std::thread::scope(|scope| {
            let scan = scope.spawn(|| {
                while !entered.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                // Same-description locks convert rather than conflict, so only the
                // coordinator's own serialization keeps the scan out of the pin section
                assert_eq!(
                    coordinator
                        .oldest_foreign_pin(TransactionId::new(0), TransactionId::new(1 << 20))
                        .unwrap(),
                    None
                );
                assert!(section_done.load(Ordering::SeqCst));
            });
            coordinator
                .pin_transaction(|| {
                    entered.store(true, Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    section_done.store(true, Ordering::SeqCst);
                    Ok(TransactionId::new(9))
                })
                .unwrap();
            scan.join().unwrap();
        });
        coordinator
            .unpin_transaction(TransactionId::new(9))
            .unwrap();
    }

    #[test]
    fn a_horizon_past_the_last_commit_is_refused() {
        let tmpfile = crate::create_tempfile();
        let coordinator = coordinator(tmpfile.path());
        // The horizon always comes from a commit older than the newest one, so a header
        // claiming otherwise is corruption, not an empty range for the scan to shrug at
        assert!(matches!(
            coordinator
                .oldest_foreign_pin(TransactionId::new(10), TransactionId::new(9))
                .unwrap_err(),
            StorageError::Corrupted(_)
        ));
    }

    #[test]
    fn an_id_past_the_lock_range_is_refused() {
        let tmpfile = crate::create_tempfile();
        let coordinator = coordinator(tmpfile.path());

        // The last id with a byte pins; the first one past the range is corruption, since
        // no database can commit often enough to reach it
        let last_in_range = TransactionId::new((1u64 << 63) - 1 - TXN_BASE);
        let past_range = TransactionId::new((1u64 << 63) - TXN_BASE);
        assert_eq!(
            coordinator.pin_transaction(|| Ok(last_in_range)).unwrap(),
            last_in_range
        );
        coordinator.unpin_transaction(last_in_range).unwrap();
        assert!(matches!(
            coordinator.pin_transaction(|| Ok(past_range)).unwrap_err(),
            StorageError::Corrupted(_)
        ));
        // An id large enough to wrap the offset arithmetic is the same refusal, and a scan
        // is bounded by its ceiling the same way
        assert!(matches!(
            coordinator
                .pin_transaction(|| Ok(TransactionId::new(u64::MAX)))
                .unwrap_err(),
            StorageError::Corrupted(_)
        ));
        assert!(matches!(
            coordinator
                .oldest_foreign_pin(TransactionId::new(0), past_range)
                .unwrap_err(),
            StorageError::Corrupted(_)
        ));
    }
}
