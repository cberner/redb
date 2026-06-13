//! Regression test for a crash during a transaction that grows the database file.
//!
//! `grow()` extends the file with `set_len` and a subsequent commit writes the grown layout into
//! the header, whose layout fields are shared by both commit slots. If a crash persisted that
//! header but not the file extension, every later open failed with
//! `Corrupted("File truncated below stored layout")` -- permanent data loss -- even though the
//! previous durable state was intact. The fix makes the file extension durable as the file grows.

use redb::backends::InMemoryBackend;
use redb::{Database, ReadableDatabase, StorageBackend, TableDefinition};
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

#[derive(Debug, Default)]
struct CrashState {
    // Current file contents (every write is applied here immediately).
    live: Vec<u8>,
    // Contents made durable by a completed sync.
    durable: Vec<u8>,
    // The most recent write to offset 0 (the header), which reaches the disk page cache even when
    // the following sync does not complete.
    last_header: Vec<u8>,
    syncs: u64,
    // Once `syncs` reaches this value, syncs stop advancing `durable` (the simulated crash point).
    freeze_from: Option<u64>,
}

// A backend that can reconstruct the file image left by a crash in which the header write reached
// the disk but the file extension and the data written after the file grew did not.
#[derive(Clone, Debug, Default)]
struct CrashBackend {
    state: Arc<Mutex<CrashState>>,
}

impl CrashBackend {
    fn syncs(&self) -> u64 {
        self.state.lock().unwrap().syncs
    }

    fn freeze_at(&self, sync: u64) {
        self.state.lock().unwrap().freeze_from = Some(sync);
    }

    // The bytes that survive the crash: everything durable, with the header write overlaid (it
    // reached the page cache) but the file length and post-grow data left at their durable values.
    fn crash_image(&self) -> Vec<u8> {
        let state = self.state.lock().unwrap();
        let mut image = state.durable.clone();
        if !state.last_header.is_empty() && image.len() >= state.last_header.len() {
            image[..state.last_header.len()].copy_from_slice(&state.last_header);
        }
        image
    }
}

impl StorageBackend for CrashBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        Ok(self.state.lock().unwrap().live.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        let state = self.state.lock().unwrap();
        let offset = usize::try_from(offset).unwrap();
        if offset + out.len() > state.live.len() {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }
        out.copy_from_slice(&state.live[offset..offset + out.len()]);
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.state
            .lock()
            .unwrap()
            .live
            .resize(usize::try_from(len).unwrap(), 0);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().unwrap();
        state.syncs += 1;
        if state.freeze_from.is_none_or(|f| state.syncs < f) {
            state.durable = state.live.clone();
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().unwrap();
        let offset = usize::try_from(offset).unwrap();
        if offset + data.len() > state.live.len() {
            state.live.resize(offset + data.len(), 0);
        }
        state.live[offset..offset + data.len()].copy_from_slice(data);
        if offset == 0 {
            state.last_header = data.to_vec();
        }
        Ok(())
    }
}

// Runs a small workload that commits a baseline durably and then commits a transaction large
// enough to grow the file.
fn workload(db: &Database) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(&0, [0u8; 100].as_slice()).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        let value = vec![0xAB; 4096];
        for k in 1..2000u64 {
            table.insert(&k, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
}

#[test]
fn crash_during_growing_commit_is_recoverable() {
    // Pass 1: count the syncs performed through the growing commit (before the Database is dropped,
    // whose own shutdown performs additional syncs).
    let probe = CrashBackend::default();
    let total_syncs = {
        let db = Database::builder()
            .create_with_backend(probe.clone())
            .unwrap();
        workload(&db);
        probe.syncs()
    };

    // Pass 2: freeze durability at the growing commit's final flush, simulating a crash in which
    // the header reached the disk but the file extension did not.
    let backend = CrashBackend::default();
    backend.freeze_at(total_syncs);
    let image = {
        let db = Database::builder()
            .create_with_backend(backend.clone())
            .unwrap();
        workload(&db);
        backend.crash_image()
    };

    // The recovered database must open and expose a valid committed state. The baseline commit was
    // durable before the file grew, so its data must survive.
    let recovered = InMemoryBackend::new();
    recovered.set_len(image.len() as u64).unwrap();
    recovered.write(0, &image).unwrap();
    let mut db = Database::builder()
        .create_with_backend(recovered)
        .expect("database must be recoverable after a crash during a file-growing commit");
    {
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(table.get(&0).unwrap().unwrap().value(), [0u8; 100]);
    }
    assert!(db.check_integrity().unwrap());
}
