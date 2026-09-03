//! The active transaction range of the multi-process locking protocol (`docs/design.md`): a
//! handle reading transaction `t` holds the byte `TXN_BASE + t` shared for as long as it reads
//! it, and a writer about to reclaim pages asks which of those bytes other processes hold.

use crate::db::TXN_BASE;
use alloc::collections::BTreeSet;
use core::ops::Range;

/// The lowest id in `low..=high` whose byte `query` reports held, if any: the oldest transaction
/// another process is reading. The bytes cannot be listed, only probed over a range, so the id
/// is found by bisection, after a probe of `low` alone: a scan starts from what the one before
/// it found, which is the likeliest answer. On Windows a probe also sees this description's own
/// locks, so `own`, this process's own reads, are stepped over.
pub(super) fn oldest_foreign_pin<E>(
    query: impl Fn(Range<u64>) -> Result<bool, E>,
    mut low: u64,
    high: u64,
    own: &BTreeSet<u64>,
) -> Result<Option<u64>, E> {
    while low <= high {
        let id = if query(bytes(low, low))? {
            low
        } else if low < high && query(bytes(low + 1, high))? {
            lowest_held(&query, low + 1, high)?
        } else {
            return Ok(None);
        };
        if !own.contains(&id) {
            return Ok(Some(id));
        }
        low = id + 1;
    }
    Ok(None)
}

/// The lowest held id in `low..=high`, some byte of which is known to be held.
fn lowest_held<E>(
    query: impl Fn(Range<u64>) -> Result<bool, E>,
    mut low: u64,
    mut high: u64,
) -> Result<u64, E> {
    while low < high {
        let mid = low + (high - low) / 2;
        if query(bytes(low, mid))? {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok(low)
}

/// The bytes of the ids `low..=high`
fn bytes(low: u64, high: u64) -> Range<u64> {
    let end = TXN_BASE + high + 1;
    TXN_BASE + low..end
}

#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod test {
    use super::oldest_foreign_pin;
    use crate::db::{TXN_BASE, byte_range};
    use crate::tree_store::file_backend::range_lock::RangeLock;
    use alloc::collections::BTreeSet;
    use alloc::vec::Vec;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    // Stands in for a read transaction's lock: the scan only cares that the byte is held, and
    // by which description
    fn pin(file: &File, id: u64) {
        assert!(
            file.try_lock_shared_range(byte_range(TXN_BASE + id))
                .unwrap()
        );
    }

    fn unpin(file: &File, id: u64) {
        file.unlock_range(byte_range(TXN_BASE + id)).unwrap();
    }

    #[test]
    fn the_scan_sees_other_handles_but_not_its_own() {
        let tmpfile = crate::create_tempfile();
        let scanning = reopen(tmpfile.path());
        let other = reopen(tmpfile.path());
        let own = BTreeSet::from([5]);
        let scan = |low| {
            oldest_foreign_pin(|range| scanning.query_lock(range), low, 1 << 20, &own).unwrap()
        };

        // Its own pin is never reported: `own` names it, since the probe sees it on Windows
        pin(&scanning, 5);
        assert_eq!(scan(0), None);

        // The oldest of another handle's pins is what bounds reclamation, wherever they sit
        pin(&other, 77777);
        pin(&other, 1234);
        assert_eq!(scan(0), Some(1234));
        assert_eq!(scan(1234), Some(1234));
        // Nothing below the start is looked at
        assert_eq!(scan(1235), Some(77777));

        // ... and the scan follows it up as the older ones are released
        unpin(&other, 1234);
        assert_eq!(scan(0), Some(77777));
        unpin(&other, 77777);
        assert_eq!(scan(0), None);

        unpin(&scanning, 5);
    }

    #[test]
    fn the_scan_finds_the_oldest_of_many() {
        let tmpfile = crate::create_tempfile();
        let scanning = reopen(tmpfile.path());
        let span = 1u64 << 30;
        let scan = |own: &BTreeSet<u64>| {
            oldest_foreign_pin(|range| scanning.query_lock(range), 0, span, own).unwrap()
        };

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
            for _ in 0..=next() % 5 {
                let id = next() % span;
                let holder = reopen(tmpfile.path());
                if holder
                    .try_lock_shared_range(byte_range(TXN_BASE + id))
                    .unwrap()
                {
                    holders.push((holder, id));
                }
            }
            let ids = || holders.iter().map(|(_, id)| *id);
            let oldest = ids().min();
            assert_eq!(scan(&BTreeSet::new()), oldest);
            // Stepping over the oldest as this description's own finds the next
            if let Some(oldest) = oldest {
                assert_eq!(
                    scan(&BTreeSet::from([oldest])),
                    ids().filter(|id| *id != oldest).min()
                );
            }

            for (holder, id) in &holders {
                holder.unlock_range(byte_range(TXN_BASE + id)).unwrap();
            }
            assert_eq!(scan(&BTreeSet::new()), None);
        }
    }
}
