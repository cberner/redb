//! A 2-phase commit publishes its new slot in a header write, and the pages that slot names are
//! only durable after the flush that follows. Whoever reads the header in that window -- crash
//! recovery, or a reader in another process -- must keep selecting the old primary, and the
//! header's 2-phase flag is the only thing that makes them: without it the newer, checksum-valid
//! slot simply wins.
//!
//! So the flag has to be durable before the slot it guards is published, however the previous
//! commit left it. redb assumes only that single-byte writes are atomic, and the god byte holding
//! the flag is 55 bytes from the nearest slot, so a crash can persist the slot without the flag.
//! The write that sets the flag therefore leaves every other byte of the header as the file
//! already has it, which is what the second test here pins down.

use redb::{Database, Durability, StorageBackend, TableDefinition};
use std::sync::{Arc, Mutex, RwLock};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("t");

// The god byte, the flag in it that matters here, and the transaction slots it guards.
const GOD_BYTE: usize = 9;
const TWO_PHASE_COMMIT: u8 = 4;
const SLOTS: std::ops::Range<usize> = 64..320;

#[derive(Debug)]
enum Event {
    Header { god_byte: u8, slots: Vec<u8> },
    Sync,
}

/// Records header writes and syncs in order, so a commit's durability sequence can be inspected.
#[derive(Clone, Debug, Default)]
struct RecordingBackend {
    inner: Arc<RwLock<Vec<u8>>>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl RecordingBackend {
    fn take_events(&self) -> Vec<Event> {
        std::mem::take(&mut self.events.lock().unwrap())
    }

    fn slots(&self) -> Vec<u8> {
        self.inner.read().unwrap()[SLOTS].to_vec()
    }
}

impl StorageBackend for RecordingBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        Ok(self.inner.read().unwrap().len() as u64)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        let offset = usize::try_from(offset).unwrap();
        let guard = self.inner.read().unwrap();
        if offset + out.len() > guard.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        out.copy_from_slice(&guard[offset..offset + out.len()]);
        Ok(())
    }
    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner
            .write()
            .unwrap()
            .resize(len.try_into().unwrap(), 0);
        Ok(())
    }
    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.events.lock().unwrap().push(Event::Sync);
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        let offset = usize::try_from(offset).unwrap();
        let mut guard = self.inner.write().unwrap();
        if offset + data.len() > guard.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        guard[offset..offset + data.len()].copy_from_slice(data);
        if offset <= GOD_BYTE && offset + data.len() > GOD_BYTE {
            self.events.lock().unwrap().push(Event::Header {
                god_byte: data[GOD_BYTE - offset],
                slots: guard[SLOTS].to_vec(),
            });
        }
        Ok(())
    }
}

// The transition case: the last commit was 1-phase, so the flag on disk is clear, and the next
// commit is 2-phase. It is the case that would have inherited the cleared value.
#[test]
fn a_two_phase_commit_publishes_its_slot_under_a_durable_flag() {
    let backend = RecordingBackend::default();
    let db = Database::builder()
        .create_with_backend(backend.clone())
        .unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.set_two_phase_commit(false);
    txn.open_table(TABLE)
        .unwrap()
        .insert(&1u64, [1u8; 64].as_slice())
        .unwrap();
    txn.commit().unwrap();
    let last = backend.take_events();
    let last_god = last
        .iter()
        .filter_map(|e| match e {
            Event::Header { god_byte, .. } => Some(*god_byte),
            Event::Sync => None,
        })
        .next_back()
        .unwrap();
    assert_eq!(
        last_god & TWO_PHASE_COMMIT,
        0,
        "the 1-phase commit left the flag set, so the transition is not being tested"
    );

    let before = backend.slots();
    let mut txn = db.begin_write().unwrap();
    txn.set_two_phase_commit(true);
    txn.open_table(TABLE)
        .unwrap()
        .insert(&2u64, [2u8; 64].as_slice())
        .unwrap();
    txn.commit().unwrap();
    let events = backend.take_events();

    let publish = events
        .iter()
        .position(|e| matches!(e, Event::Header { slots, .. } if *slots != before))
        .expect("no header write published a new transaction slot");

    let flag = events[..publish]
        .iter()
        .position(
            |e| matches!(e, Event::Header { god_byte, .. } if god_byte & TWO_PHASE_COMMIT != 0),
        )
        .expect("the new slot was published before any header write set the 2-phase flag");
    assert!(
        events[flag..publish]
            .iter()
            .any(|e| matches!(e, Event::Sync)),
        "the new slot was published with no flush between it and the write that set the flag"
    );

    for (n, event) in events.iter().enumerate() {
        if let Event::Header { god_byte, .. } = event {
            assert_ne!(
                god_byte & TWO_PHASE_COMMIT,
                0,
                "header write at event {n} published without the 2-phase flag"
            );
        }
    }
}

// The same transition, but with a `Durability::None` commit in between. That commit leaves the
// in-memory secondary slot ahead of the file with its pages merely buffered, and the flag write
// publishes the whole header -- so those pages have to reach the file first, or a torn write of
// this header leaves a newer slot naming pages the file does not hold.
#[test]
fn staging_the_flag_does_not_publish_a_slot_whose_pages_are_buffered() {
    let backend = RecordingBackend::default();
    let db = Database::builder()
        .create_with_backend(backend.clone())
        .unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.set_two_phase_commit(false);
    txn.open_table(TABLE)
        .unwrap()
        .insert(&1u64, [1u8; 64].as_slice())
        .unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.set_durability(Durability::None).unwrap();
    txn.open_table(TABLE)
        .unwrap()
        .insert(&2u64, [2u8; 64].as_slice())
        .unwrap();
    txn.commit().unwrap();
    backend.take_events();

    let before = backend.slots();
    let mut txn = db.begin_write().unwrap();
    txn.set_two_phase_commit(true);
    txn.open_table(TABLE)
        .unwrap()
        .insert(&3u64, [3u8; 64].as_slice())
        .unwrap();
    txn.commit().unwrap();
    let events = backend.take_events();

    // The staged flag write must carry the slots the file already has, whatever the in-memory
    // header holds, or a torn write of it leaves a newer slot naming pages the file does not hold
    let first = events
        .iter()
        .position(|e| matches!(e, Event::Header { .. }))
        .expect("the commit wrote no header");
    assert!(
        matches!(&events[first], Event::Header { slots, .. } if *slots == before),
        "the staged flag write moved a transaction slot as well"
    );

    let publish = events
        .iter()
        .position(|e| matches!(e, Event::Header { slots, .. } if *slots != before))
        .expect("no header write published a new transaction slot");
    let flag = events[..=publish]
        .iter()
        .position(
            |e| matches!(e, Event::Header { god_byte, .. } if god_byte & TWO_PHASE_COMMIT != 0),
        )
        .expect("the slot moved before any header write set the 2-phase flag");
    assert!(flag <= publish, "the flag was set after the slot moved");
}
