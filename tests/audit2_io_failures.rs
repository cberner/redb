// Fault-injection audit of redb's I/O error handling and crash consistency.
//
// A FaultBackend wraps an in-memory file image and can:
//   * fail the Nth write/sync/set_len/read with an io::Error
//   * record all writes since the last completed sync and produce "crash images"
//     where an arbitrary subset of the unsynced writes survived (writes are
//     applied in issue order; sync is the only ordering barrier)
//   * optionally apply half of a failed write (torn write on error)
//
// Invariants checked after every injected failure:
//   * No panic inside redb (the backend only ever returns io::Error)
//   * If commit() returned Ok, every crash image must recover to the new state
//   * If commit() returned Err, every crash image must recover to either the
//     previous durable state or the (fully applied) new state -- never a
//     mixture, never data loss of older commits
//   * After reopening a crash image, a second Database::check_integrity()
//     must return Ok(true)
//   * After a failed operation the database is poisoned: begin_write() fails
//   * StorageBackend::close() is called exactly once when Database is dropped

use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, StorageBackend, TableDefinition,
    TableError,
};
use std::collections::BTreeMap;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("audit");

type Model = BTreeMap<u64, Vec<u8>>;
type StepFn = dyn Fn(&mut redb::Table<u64, &[u8]>, &mut Model) -> Result<(), redb::Error>;

// ---------------------------------------------------------------------------
// Fault-injection backend
// ---------------------------------------------------------------------------

const FAIL_WRITE: u8 = 1;
const FAIL_SYNC: u8 = 2;
const FAIL_SETLEN: u8 = 4;
const FAIL_READ: u8 = 8;

#[derive(Debug, Clone)]
enum LogOp {
    Write(u64, Vec<u8>),
    SetLen(u64),
}

#[derive(Debug, Default, Clone)]
struct DiskState {
    // Contents guaranteed durable (covered by a completed sync)
    synced: Vec<u8>,
    // Contents after all writes (what reads observe)
    live: Vec<u8>,
    // Ops issued since the last completed sync
    log: Vec<LogOp>,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<DiskState>,
    // Fails the op when fetch_sub returns 1. i64::MAX => never fire.
    countdown: AtomicI64,
    fail_kinds: u8,
    half_failed_writes: bool,
    eligible_ops: AtomicU64,
    close_calls: AtomicU64,
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, DiskState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn should_fail(&self, kind: u8) -> bool {
        if self.fail_kinds & kind == 0 {
            return false;
        }
        self.eligible_ops.fetch_add(1, Ordering::SeqCst);
        self.countdown.fetch_sub(1, Ordering::SeqCst) == 1
    }

    fn snapshot(&self) -> DiskState {
        self.lock().clone()
    }
}

#[derive(Debug)]
struct FaultBackend(Arc<Inner>);

impl FaultBackend {
    fn new(fail_kinds: u8, fail_at: Option<u64>, half_failed_writes: bool) -> (Self, Arc<Inner>) {
        let inner = Arc::new(Inner {
            state: Mutex::new(DiskState::default()),
            countdown: AtomicI64::new(
                fail_at
                    .map(|n| i64::try_from(n).unwrap() + 1)
                    .unwrap_or(i64::MAX),
            ),
            fail_kinds,
            half_failed_writes,
            eligible_ops: AtomicU64::new(0),
            close_calls: AtomicU64::new(0),
        });
        (Self(inner.clone()), inner)
    }

    fn from_image(image: Vec<u8>) -> (Self, Arc<Inner>) {
        let (backend, inner) = Self::new(0, None, false);
        {
            let mut state = inner.lock();
            state.synced = image.clone();
            state.live = image;
        }
        (backend, inner)
    }
}

fn injected_error() -> io::Error {
    io::Error::other("injected fault")
}

impl StorageBackend for FaultBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.0.lock().live.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        if self.0.should_fail(FAIL_READ) {
            return Err(injected_error());
        }
        let state = self.0.lock();
        let offset = usize::try_from(offset).unwrap();
        if offset + out.len() <= state.live.len() {
            out.copy_from_slice(&state.live[offset..offset + out.len()]);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "out of range"))
        }
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        if self.0.should_fail(FAIL_SETLEN) {
            return Err(injected_error());
        }
        let mut state = self.0.lock();
        let len = usize::try_from(len).unwrap();
        state.live.resize(len, 0);
        state.log.push(LogOp::SetLen(len as u64));
        Ok(())
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        if self.0.should_fail(FAIL_SYNC) {
            return Err(injected_error());
        }
        let mut state = self.0.lock();
        let live = state.live.clone();
        state.synced = live;
        state.log.clear();
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        if self.0.should_fail(FAIL_WRITE) {
            if self.0.half_failed_writes && data.len() >= 2 {
                // Torn write: the first half reached the disk cache before the error
                let half = &data[..data.len() / 2];
                let mut state = self.0.lock();
                let offset_usize = usize::try_from(offset).unwrap();
                if offset_usize + half.len() <= state.live.len() {
                    state.live[offset_usize..offset_usize + half.len()].copy_from_slice(half);
                    state.log.push(LogOp::Write(offset, half.to_vec()));
                }
            }
            return Err(injected_error());
        }
        let mut state = self.0.lock();
        let offset_usize = usize::try_from(offset).unwrap();
        if offset_usize + data.len() <= state.live.len() {
            state.live[offset_usize..offset_usize + data.len()].copy_from_slice(data);
            state.log.push(LogOp::Write(offset, data.to_vec()));
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "out of range"))
        }
    }

    fn close(&self) -> Result<(), io::Error> {
        self.0.close_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Crash image generation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum CrashSubset {
    None,
    All,
    Rng(u64),
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// Builds the post-crash file image: the synced contents plus an arbitrary
// subset of the unsynced writes, applied in issue order. set_len ops are
// either all applied (fdatasync semantics make them durable with the data)
// or all dropped (keep_setlen == false); a surviving write beyond EOF
// extends the file, as it would on a real filesystem.
fn crash_image(snap: &DiskState, subset: CrashSubset, keep_setlen: bool) -> Vec<u8> {
    let mut img = snap.synced.clone();
    let mut rng_state = match subset {
        CrashSubset::Rng(seed) => seed,
        _ => 0,
    };
    for op in &snap.log {
        match op {
            LogOp::SetLen(len) => {
                if keep_setlen {
                    img.resize(usize::try_from(*len).unwrap(), 0);
                }
            }
            LogOp::Write(offset, data) => {
                let keep = match subset {
                    CrashSubset::None => false,
                    CrashSubset::All => true,
                    CrashSubset::Rng(_) => splitmix64(&mut rng_state) & 1 == 1,
                };
                if keep {
                    let offset = usize::try_from(*offset).unwrap();
                    if img.len() < offset + data.len() {
                        img.resize(offset + data.len(), 0);
                    }
                    img[offset..offset + data.len()].copy_from_slice(data);
                }
            }
        }
    }
    img
}

// ---------------------------------------------------------------------------
// Scripted workload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Config {
    two_phase: bool,
    quick_repair: bool,
    nondurable_layer: bool,
    half_failed_writes: bool,
    savepoint_script: bool,
}

#[derive(Debug, Default)]
struct Outcome {
    // States that are acceptable after crash + reopen. Index 0 is the latest
    // fully-durable state; a second entry is the pending state of a durable
    // commit that returned Err (commit may or may not have hit the disk).
    candidates: Vec<Model>,
    error: Option<String>,
    created: bool,
    completed: bool,
    // Number of fault-eligible ops issued by create + script (excludes Database::drop)
    script_ops: u64,
    // Some(true) means begin_write() unexpectedly succeeded after an error
    begin_write_ok_after_error: Option<bool>,
}

fn fmt_model(m: &Model) -> String {
    let keys: Vec<String> = m
        .iter()
        .map(|(k, v)| format!("{}:{}b", k, v.len()))
        .collect();
    format!("{{{}}}", keys.join(","))
}

fn durable_step(
    db: &Database,
    cfg: Config,
    model: &mut Model,
    out: &mut Outcome,
    step: &str,
    f: &StepFn,
) -> Result<(), String> {
    let mut next = model.clone();
    let result = (|| -> Result<(), redb::Error> {
        let mut txn = db.begin_write()?;
        txn.set_two_phase_commit(cfg.two_phase);
        if cfg.quick_repair {
            txn.set_quick_repair(true);
        }
        {
            let mut table = txn.open_table(TABLE)?;
            f(&mut table, &mut next)?;
        }
        txn.commit()?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            *model = next.clone();
            out.candidates = vec![next];
            Ok(())
        }
        Err(e) => {
            // The commit may or may not have reached the disk before the error
            out.candidates.push(next);
            Err(format!("{step}: {e}"))
        }
    }
}

fn nondurable_step(db: &Database, model: &mut Model, step: &str, f: &StepFn) -> Result<(), String> {
    let mut next = model.clone();
    let result = (|| -> Result<(), redb::Error> {
        let mut txn = db.begin_write()?;
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE)?;
            f(&mut table, &mut next)?;
        }
        txn.commit()?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            // Durable state unchanged: crash must still recover the last durable state
            *model = next;
            Ok(())
        }
        Err(e) => Err(format!("{step}: {e}")),
    }
}

const BLOB_KEYS: std::ops::Range<u64> = 100..106;
const BLOB_SIZE: usize = 256 * 1024;

fn main_script(db: &Database, cfg: Config, out: &mut Outcome) -> Result<(), String> {
    let mut model = Model::new();
    out.candidates = vec![model.clone()];

    // C1: initial inserts
    durable_step(db, cfg, &mut model, out, "C1", &|t, m| {
        for i in 0..10u64 {
            let v = vec![i as u8; 50];
            t.insert(&i, v.as_slice())?;
            m.insert(i, v);
        }
        Ok(())
    })?;

    // C2: inserts + removals (creates freed pages)
    durable_step(db, cfg, &mut model, out, "C2", &|t, m| {
        for i in 10..20u64 {
            let v = vec![i as u8; 50];
            t.insert(&i, v.as_slice())?;
            m.insert(i, v);
        }
        for i in 0..5u64 {
            t.remove(&i)?;
            m.remove(&i);
        }
        Ok(())
    })?;

    if cfg.nondurable_layer {
        // N3: non-durable commit layered on top of the durable state
        nondurable_step(db, &mut model, "N3", &|t, m| {
            for i in 20..30u64 {
                let v = vec![i as u8; 50];
                t.insert(&i, v.as_slice())?;
                m.insert(i, v);
            }
            for i in 5..10u64 {
                t.remove(&i)?;
                m.remove(&i);
            }
            Ok(())
        })?;
    }

    // C4: durable commit (also makes N3 durable when present)
    durable_step(db, cfg, &mut model, out, "C4", &|t, m| {
        for i in 30..35u64 {
            let v = vec![i as u8; 50];
            t.insert(&i, v.as_slice())?;
            m.insert(i, v);
        }
        Ok(())
    })?;

    // C5: big values to force file growth (set_len)
    durable_step(db, cfg, &mut model, out, "C5", &|t, m| {
        for i in BLOB_KEYS {
            let v = vec![(i % 251) as u8; BLOB_SIZE];
            t.insert(&i, v.as_slice())?;
            m.insert(i, v);
        }
        Ok(())
    })?;

    // C6: delete the blobs (queues lots of freed pages)
    durable_step(db, cfg, &mut model, out, "C6", &|t, m| {
        for i in BLOB_KEYS {
            t.remove(&i)?;
            m.remove(&i);
        }
        Ok(())
    })?;

    // C7: small commit; the freed blob pages get reclaimed and the file shrinks
    durable_step(db, cfg, &mut model, out, "C7", &|t, m| {
        let v = vec![7u8; 50];
        t.insert(&77, v.as_slice())?;
        m.insert(77, v);
        Ok(())
    })?;

    Ok(())
}

fn savepoint_script(db: &Database, cfg: Config, out: &mut Outcome) -> Result<(), String> {
    let mut model = Model::new();
    out.candidates = vec![model.clone()];

    durable_step(db, cfg, &mut model, out, "C1", &|t, m| {
        for i in 0..10u64 {
            let v = vec![i as u8; 50];
            t.insert(&i, v.as_slice())?;
            m.insert(i, v);
        }
        Ok(())
    })?;
    let model_at_savepoint = model.clone();

    // Create an ephemeral savepoint in its own transaction
    let savepoint = (|| -> Result<redb::Savepoint, redb::Error> {
        let txn = db.begin_write()?;
        let savepoint = txn.ephemeral_savepoint()?;
        txn.commit()?;
        Ok(savepoint)
    })()
    .map_err(|e| {
        out.candidates = vec![out.candidates.last().unwrap().clone()];
        format!("SP-create: {e}")
    })?;

    // C2: diverge from the savepoint
    durable_step(db, cfg, &mut model, out, "C2", &|t, m| {
        for i in 10..20u64 {
            let v = vec![i as u8; 1000];
            t.insert(&i, v.as_slice())?;
            m.insert(i, v);
        }
        for i in 0..3u64 {
            t.remove(&i)?;
            m.remove(&i);
        }
        Ok(())
    })?;

    // T3: restore the savepoint and commit. Afterwards the durable state must
    // be either the pre-restore state (C2) or the fully restored state.
    let mut restored = model_at_savepoint.clone();
    restored.insert(50, vec![50u8; 50]);
    let result = (|| -> Result<(), redb::Error> {
        let mut txn = db.begin_write()?;
        txn.set_two_phase_commit(cfg.two_phase);
        if cfg.quick_repair {
            txn.set_quick_repair(true);
        }
        txn.restore_savepoint(&savepoint)?;
        {
            let mut table = txn.open_table(TABLE)?;
            table.insert(&50, vec![50u8; 50].as_slice())?;
        }
        txn.commit()?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            out.candidates = vec![restored];
            Ok(())
        }
        Err(e) => {
            out.candidates.push(restored);
            Err(format!("T3-restore: {e}"))
        }
    }
}

// Runs the scripted workload against a fresh database with a fault scheduled
// at the fail_at'th eligible op. Returns the outcome, the disk snapshot taken
// at the moment the script ended (before Database::drop), and the backend.
fn run_one(cfg: Config, fail_kinds: u8, fail_at: Option<u64>) -> (Outcome, DiskState, Arc<Inner>) {
    let (backend, inner) = FaultBackend::new(fail_kinds, fail_at, cfg.half_failed_writes);
    let mut out = Outcome::default();
    let snap = Arc::new(Mutex::new(DiskState::default()));
    let snap2 = snap.clone();
    let inner2 = inner.clone();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let db = match Database::builder().create_with_backend(backend) {
            Ok(db) => db,
            Err(e) => {
                out.error = Some(format!("create: {e}"));
                *snap2.lock().unwrap() = inner2.snapshot();
                return out;
            }
        };
        out.created = true;
        let script_result = if cfg.savepoint_script {
            savepoint_script(&db, cfg, &mut out)
        } else {
            main_script(&db, cfg, &mut out)
        };
        match script_result {
            Ok(()) => out.completed = true,
            Err(e) => {
                out.error = Some(e);
                // Post-error usability: the database must be poisoned
                out.begin_write_ok_after_error = Some(db.begin_write().is_ok());
            }
        }
        // Snapshot before Database::drop performs additional shutdown I/O
        *snap2.lock().unwrap() = inner2.snapshot();
        out.script_ops = inner2.eligible_ops.load(Ordering::SeqCst);
        drop(db);
        out
    }));
    let out = match result {
        Ok(out) => out,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic".to_string());
            Outcome {
                error: Some(format!("PANIC inside redb: {msg}")),
                ..Default::default()
            }
        }
    };
    let snapshot = snap.lock().unwrap().clone();
    (out, snapshot, inner)
}

fn read_state(db: &Database) -> Result<Model, String> {
    let txn = db.begin_read().map_err(|e| format!("begin_read: {e}"))?;
    match txn.open_table(TABLE) {
        Ok(table) => {
            let mut model = Model::new();
            let iter = table.iter().map_err(|e| format!("iter: {e}"))?;
            for item in iter {
                let (k, v) = item.map_err(|e| format!("iter item: {e}"))?;
                model.insert(k.value(), v.value().to_vec());
            }
            Ok(model)
        }
        Err(TableError::TableDoesNotExist(_)) => Ok(Model::new()),
        Err(e) => Err(format!("open_table: {e}")),
    }
}

// Opens a crash image, verifies the recovered data equals one of the candidate
// states, and verifies that integrity checking converges.
fn validate_crash_image(
    image: Vec<u8>,
    candidates: &[Model],
    ctx: &str,
    check_integrity: bool,
    violations: &mut Vec<String>,
) {
    let (backend, _inner) = FaultBackend::from_image(image);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut db = match Database::builder().create_with_backend(backend) {
            Ok(db) => db,
            Err(e) => return Err(format!("reopen failed: {e}")),
        };
        let state = read_state(&db)?;
        if !candidates.contains(&state) {
            return Err(format!(
                "recovered state {} not in candidates [{}]",
                fmt_model(&state),
                candidates
                    .iter()
                    .map(fmt_model)
                    .collect::<Vec<_>>()
                    .join(" ; ")
            ));
        }
        if check_integrity {
            let _first = db
                .check_integrity()
                .map_err(|e| format!("check_integrity #1: {e}"))?;
            let second = db
                .check_integrity()
                .map_err(|e| format!("check_integrity #2: {e}"))?;
            if !second {
                return Err("second check_integrity returned false".to_string());
            }
            let state2 = read_state(&db)?;
            if state2 != state {
                return Err(format!(
                    "state changed by check_integrity: {} -> {}",
                    fmt_model(&state),
                    fmt_model(&state2)
                ));
            }
        }
        Ok(())
    }));
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => violations.push(format!("{ctx}: {e}")),
        Err(payload) => {
            let msg = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic".to_string());
            violations.push(format!("{ctx}: PANIC on reopen: {msg}"));
        }
    }
}

fn validate_run(
    fail_at: u64,
    cfg: Config,
    out: &Outcome,
    snap: &DiskState,
    violations: &mut Vec<String>,
) {
    let ctx_base = format!(
        "fail_at={fail_at} cfg={cfg:?} error={:?}",
        out.error.as_deref().unwrap_or("none")
    );

    if let Some(err) = &out.error
        && err.starts_with("PANIC")
    {
        violations.push(format!("{ctx_base}: {err}"));
        return;
    }

    if let Some(true) = out.begin_write_ok_after_error {
        violations.push(format!(
            "{ctx_base}: begin_write() succeeded after an I/O error (database not poisoned)"
        ));
    }

    if !out.created {
        // Failure during initial creation: no database to validate; just make
        // sure reopening any crash image does not panic.
        for subset in [CrashSubset::All, CrashSubset::Rng(fail_at + 1)] {
            let image = crash_image(snap, subset, true);
            if image.len() < 9 {
                continue;
            }
            let (backend, _inner) = FaultBackend::from_image(image);
            let result = catch_unwind(AssertUnwindSafe(|| {
                let _ = Database::builder().create_with_backend(backend);
            }));
            if result.is_err() {
                violations.push(format!(
                    "{ctx_base}: PANIC reopening partially-created database"
                ));
            }
        }
        return;
    }

    let subsets = [
        CrashSubset::None,
        CrashSubset::All,
        CrashSubset::Rng(0x1234_5678 ^ fail_at),
        CrashSubset::Rng(0xDEAD_BEEF ^ (fail_at << 8)),
        CrashSubset::Rng(0x00C0_FFEE ^ (fail_at << 16)),
    ];
    for (i, subset) in subsets.iter().enumerate() {
        for keep_setlen in [true, false] {
            let image = crash_image(snap, *subset, keep_setlen);
            // Run the expensive integrity check on a representative subset
            let check_integrity = keep_setlen && i < 3;
            validate_crash_image(
                image,
                &out.candidates,
                &format!("{ctx_base} subset={subset:?} keep_setlen={keep_setlen}"),
                check_integrity,
                violations,
            );
        }
    }
}

fn sweep(cfg: Config, fail_kinds: u8) {
    // Probe run: count the eligible ops and confirm the script completes
    let (out, _snap, _inner) = run_one(cfg, fail_kinds, None);
    assert!(
        out.completed,
        "probe run did not complete: {:?} (cfg={cfg:?})",
        out.error
    );
    let total_ops = out.script_ops;
    println!("cfg={cfg:?} fail_kinds={fail_kinds:#b} total eligible ops: {total_ops}");

    let mut violations = Vec::new();
    for fail_at in 0..total_ops {
        let (out, snap, _inner) = run_one(cfg, fail_kinds, Some(fail_at));
        if out.completed {
            // fail_at is below the probe's script op count, so the fault fired
            // during the script: the error must have been swallowed somewhere.
            // The data check below will catch silent loss, but flag it explicitly.
            violations.push(format!(
                "fail_at={fail_at} cfg={cfg:?}: injected failure fired but the workload completed without error"
            ));
        }
        validate_run(fail_at, cfg, &out, &snap, &mut violations);
    }

    assert!(
        violations.is_empty(),
        "{} violations:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Sweeps
// ---------------------------------------------------------------------------

#[test]
fn sweep_1pc_commits() {
    sweep(
        Config {
            two_phase: false,
            quick_repair: false,
            nondurable_layer: false,
            half_failed_writes: false,
            savepoint_script: false,
        },
        FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN,
    );
}

#[test]
fn sweep_1pc_commits_with_nondurable_layer() {
    sweep(
        Config {
            two_phase: false,
            quick_repair: false,
            nondurable_layer: true,
            half_failed_writes: false,
            savepoint_script: false,
        },
        FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN,
    );
}

#[test]
fn sweep_2pc_commits() {
    sweep(
        Config {
            two_phase: true,
            quick_repair: false,
            nondurable_layer: true,
            half_failed_writes: false,
            savepoint_script: false,
        },
        FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN,
    );
}

#[test]
fn sweep_quick_repair_commits() {
    sweep(
        Config {
            two_phase: false,
            quick_repair: true,
            nondurable_layer: true,
            half_failed_writes: false,
            savepoint_script: false,
        },
        FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN,
    );
}

#[test]
fn sweep_1pc_commits_torn_failed_writes() {
    sweep(
        Config {
            two_phase: false,
            quick_repair: false,
            nondurable_layer: true,
            half_failed_writes: true,
            savepoint_script: false,
        },
        FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN,
    );
}

#[test]
fn sweep_savepoint_restore() {
    sweep(
        Config {
            two_phase: false,
            quick_repair: false,
            nondurable_layer: false,
            half_failed_writes: false,
            savepoint_script: true,
        },
        FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN,
    );
}

// Failure injected into every read while reopening a crashed database:
// open must return Err (or succeed), never panic, and never report success
// with wrong data.
#[test]
fn sweep_read_failures_on_reopen() {
    let cfg = Config {
        two_phase: false,
        quick_repair: false,
        nondurable_layer: true,
        half_failed_writes: false,
        savepoint_script: false,
    };
    let (out, snap, _inner) = run_one(cfg, 0, None);
    assert!(out.completed, "probe run failed: {:?}", out.error);
    let final_state = out.candidates[0].clone();
    let image = crash_image(&snap, CrashSubset::All, true);

    // Probe the number of reads needed to open
    let (backend, probe) = FaultBackend::from_image(image.clone());
    // Enable read-fault counting without arming a failure
    let probe_backend = FaultBackend(Arc::new(Inner {
        state: Mutex::new(backend.0.lock().clone()),
        countdown: AtomicI64::new(i64::MAX),
        fail_kinds: FAIL_READ,
        half_failed_writes: false,
        eligible_ops: AtomicU64::new(0),
        close_calls: AtomicU64::new(0),
    }));
    drop(probe);
    let probe_inner = probe_backend.0.clone();
    {
        let db = Database::builder()
            .create_with_backend(probe_backend)
            .unwrap();
        assert_eq!(read_state(&db).unwrap(), final_state);
    }
    let total_reads = probe_inner.eligible_ops.load(Ordering::SeqCst);
    println!("reopen takes {total_reads} reads");

    let mut violations = Vec::new();
    for fail_at in 0..total_reads {
        let inner = Arc::new(Inner {
            state: Mutex::new(DiskState {
                synced: image.clone(),
                live: image.clone(),
                log: vec![],
            }),
            countdown: AtomicI64::new(i64::try_from(fail_at).unwrap() + 1),
            fail_kinds: FAIL_READ,
            half_failed_writes: false,
            eligible_ops: AtomicU64::new(0),
            close_calls: AtomicU64::new(0),
        });
        let backend = FaultBackend(inner.clone());
        let result = catch_unwind(AssertUnwindSafe(|| {
            match Database::builder().create_with_backend(backend) {
                Ok(db) => match read_state(&db) {
                    Ok(state) => {
                        if state != final_state {
                            Err(format!("opened with wrong data: {}", fmt_model(&state)))
                        } else {
                            Ok(())
                        }
                    }
                    Err(_) => Ok(()), // read error surfaced: fine
                },
                Err(_) => Ok(()), // open error surfaced: fine
            }
        }));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => violations.push(format!("read fail_at={fail_at}: {e}")),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic".to_string());
                violations.push(format!("read fail_at={fail_at}: PANIC: {msg}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "{} violations:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

// Minimal deterministic repro for the "File truncated below stored layout"
// recovery failure:
//
// 1. Commit some data (durable state S1).
// 2. Begin a commit that grows the file (set_len) and fail its final fsync.
//    commit() correctly returns Err.
// 3. Crash. The unsynced set_len is lost (no fsync completed, so there is no
//    ordering guarantee between the file extension and the header page write),
//    but the unsynced header write at offset 0 -- which carries the grown
//    layout in the fields shared by BOTH commit slots -- survives.
// 4. Reopen: every open now fails with
//    Corrupted("File truncated below stored layout") even though the durable
//    state S1 is fully intact in the image. The database is permanently
//    unopenable: total data loss for the user.
#[test]
fn crash_during_growing_commit_must_not_brick_database() {
    // Probe: count the syncs issued by create + setup commit + blob commit
    let setup = |db: &Database| {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for i in 0..10u64 {
                table.insert(&i, vec![i as u8; 50].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    };
    let blob_commit = |db: &Database| -> Result<(), redb::Error> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(TABLE)?;
            for i in BLOB_KEYS {
                table.insert(&i, vec![(i % 251) as u8; BLOB_SIZE].as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    };

    let (backend, probe_inner) = FaultBackend::new(FAIL_SYNC, None, false);
    {
        let db = Database::builder().create_with_backend(backend).unwrap();
        setup(&db);
        blob_commit(&db).unwrap();
        // We never drop this db cleanly; count syncs up to here
    }
    let total_syncs = probe_inner.eligible_ops.load(Ordering::SeqCst);

    // Find the last sync before the blob commit completes by re-running with a
    // failure injected at each candidate index from the end; the final fsync of
    // the blob commit is the last eligible sync op issued by the script, but
    // Database::drop in the probe added more, so locate it by re-probing.
    let (backend, probe_inner) = FaultBackend::new(FAIL_SYNC, None, false);
    let script_syncs;
    {
        let db = Database::builder().create_with_backend(backend).unwrap();
        setup(&db);
        blob_commit(&db).unwrap();
        script_syncs = probe_inner.eligible_ops.load(Ordering::SeqCst);
        drop(db);
    }
    assert!(script_syncs <= total_syncs);
    let fail_at = script_syncs - 1; // the blob commit's final fsync

    // Real run: fail the blob commit's fsync
    let (backend, inner) = FaultBackend::new(FAIL_SYNC, Some(fail_at), false);
    let db = Database::builder().create_with_backend(backend).unwrap();
    setup(&db);
    let mut expected_old = Model::new();
    for i in 0..10u64 {
        expected_old.insert(i, vec![i as u8; 50]);
    }
    let mut expected_new = expected_old.clone();
    for i in BLOB_KEYS {
        expected_new.insert(i, vec![(i % 251) as u8; BLOB_SIZE]);
    }
    let result = blob_commit(&db);
    assert!(result.is_err(), "fsync failure must surface from commit()");
    let snap = inner.snapshot();
    drop(db);

    // Crash: all unsynced writes survive, the unsynced file extension does not
    let image = crash_image(&snap, CrashSubset::All, false);

    let (backend, _inner) = FaultBackend::from_image(image);
    let db = Database::builder()
        .create_with_backend(backend)
        .unwrap_or_else(|e| {
            panic!("database is unopenable after crash; committed data is lost: {e}")
        });
    let state = read_state(&db).unwrap();
    assert!(
        state == expected_old || state == expected_new,
        "recovered state is neither the old nor the new committed state: {}",
        fmt_model(&state)
    );
}

// StorageBackend::close() is documented to be called exactly once when the
// Database is dropped. Verify that this holds even when shutdown I/O fails.
#[test]
fn close_called_exactly_once_on_shutdown_io_error() {
    let cfg = Config {
        two_phase: false,
        quick_repair: false,
        nondurable_layer: false,
        half_failed_writes: false,
        savepoint_script: false,
    };
    // Count the shutdown ops: run the script, then measure ops consumed by drop
    let (backend, inner) = FaultBackend::new(FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN, None, false);
    {
        let db = Database::builder().create_with_backend(backend).unwrap();
        let mut out = Outcome::default();
        main_script(&db, cfg, &mut out).unwrap();
        inner.eligible_ops.store(0, Ordering::SeqCst);
        drop(db);
    }
    let shutdown_ops = inner.eligible_ops.load(Ordering::SeqCst);
    assert_eq!(inner.close_calls.load(Ordering::SeqCst), 1);
    println!("shutdown takes {shutdown_ops} write/sync/set_len ops");

    let mut violations = Vec::new();
    for fail_at in 0..shutdown_ops {
        let (backend, inner) = FaultBackend::new(FAIL_WRITE | FAIL_SYNC | FAIL_SETLEN, None, false);
        let db = Database::builder().create_with_backend(backend).unwrap();
        let mut out = Outcome::default();
        main_script(&db, cfg, &mut out).unwrap();
        // Arm the fault to fire during shutdown
        inner
            .countdown
            .store(i64::try_from(fail_at).unwrap() + 1, Ordering::SeqCst);
        let result = catch_unwind(AssertUnwindSafe(|| drop(db)));
        if result.is_err() {
            violations.push(format!(
                "shutdown fail_at={fail_at}: PANIC during Database::drop"
            ));
            continue;
        }
        let closes = inner.close_calls.load(Ordering::SeqCst);
        if closes != 1 {
            violations.push(format!(
                "shutdown fail_at={fail_at}: StorageBackend::close() called {closes} times (expected 1)"
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "{} violations:\n{}",
        violations.len(),
        violations.join("\n")
    );
}
