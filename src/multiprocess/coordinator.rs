//! The cross-process half of [`TransactionTracker`](crate::transaction_tracker::TransactionTracker):
//! pins what this process is reading in the `txn/` directory that other processes scan, scans what
//! they have pinned before this process reclaims anything, and keeps this process's view of the
//! file and its page cache current by checking the transaction collection horizon in
//! `extended-header` whenever it looks at the file.

use crate::multiprocess::locks::{
    DatabaseDir, ExtendedHeader, RegistryLock, TransactionPins, WriteLock, WriterMode,
};
use crate::transaction_tracker::TransactionId;
use crate::tree_store::{CommitHook, RawCommitSlots, TransactionalMemory};
use crate::{DatabaseError, Result, StorageError};
use std::fs::File;
use std::sync::{Arc, Mutex, MutexGuard};

/// Writes the transaction collection horizon into `extended-header` at the one point the protocol
/// allows: inside a commit, after the phase-1 flush and before the flip that makes the new commit
/// slot primary.
struct HorizonPublisher {
    registry: RegistryLock,
    extended_header: ExtendedHeader,
    /// The newest transaction this process has garbage collected, which the next flip publishes.
    /// Never decreases: a page freed by a transaction at or below this may already be reused.
    pending: Mutex<u64>,
}

impl HorizonPublisher {
    /// The horizon stored for the primary commit slot, or `None` if the extended header does not
    /// verify against it. The caller must hold the registry lock and have read `raw` under it.
    fn read_stored(&self, raw: &RawCommitSlots) -> Result<Option<u64>> {
        self.extended_header
            .read_horizon(raw.primary_index, raw.primary())
    }

    /// Raises the horizon the next flip will publish.
    fn record(&self, horizon: u64) -> Result<()> {
        let mut pending = self.pending.lock()?;
        *pending = (*pending).max(horizon);
        Ok(())
    }

    /// Rewrites both slots of the extended header to `horizon`, bound to the commit slots in
    /// `raw`. The caller must hold the registry lock exclusively.
    fn rewrite(&self, horizon: u64, raw: &RawCommitSlots) -> Result<()> {
        for slot in 0..2 {
            self.extended_header
                .write_horizon(slot, horizon, raw.slot(slot))?;
        }
        self.record(horizon)
    }
}

impl CommitHook for HorizonPublisher {
    fn before_primary_flip(&self, slot: usize, slot_bytes: &[u8]) -> Result {
        let horizon = *self.pending.lock()?;
        let guard = self.registry.exclusive()?;
        let result = self
            .extended_header
            .write_horizon(slot, horizon, slot_bytes);
        drop(guard);
        result
    }
}

struct Inner {
    /// This process's pin in `txn/`: the oldest transaction it still needs.
    pins: TransactionPins,
    /// The last committed transaction the whole in-memory state reflects, allocator state and
    /// layout included. Only a write transaction loads those, and only it may rely on this.
    state_committed: u64,
    /// A bound on how old the pages in the read cache are: every one was read from a snapshot at
    /// or after this transaction. A page cached from snapshot `S` was reachable at `S`, so only a
    /// transaction newer than `S` can have freed it -- a collection horizon above `S` may have
    /// reused it, one at or below cannot. Raised only when the cache is emptied.
    cache_floor: u64,
}

pub(crate) struct ProcessCoordinator {
    mem: Arc<TransactionalMemory>,
    mode: WriterMode,
    read_only: bool,
    registry: RegistryLock,
    horizon: Arc<HorizonPublisher>,
    inner: Mutex<Inner>,
    /// Held for this process's whole lifetime in [`WriterMode::SingleWriterProcess`], and for the
    /// duration of each write transaction in [`WriterMode::MultiWriterProcess`]
    write_lock: Mutex<WriteLock>,
    /// The shared lock on `metadata` that every process holds for as long as it has the database
    /// open. Nothing reads it again; a future format upgrade takes it exclusively, and this is
    /// what it waits on.
    metadata_lock: Mutex<Option<File>>,
}

impl ProcessCoordinator {
    pub(crate) fn new(
        dir: &DatabaseDir,
        mode: WriterMode,
        read_only: bool,
        write_lock: WriteLock,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self, DatabaseError> {
        let registry = RegistryLock::open(dir);
        let horizon = Arc::new(HorizonPublisher {
            registry: registry.clone(),
            extended_header: ExtendedHeader::open(dir)?,
            pending: Mutex::new(0),
        });
        if !read_only {
            // From here on every commit binds the pending horizon to the slot it flips to
            mem.set_commit_hook(horizon.clone());
        }
        Ok(Self {
            mem,
            mode,
            read_only,
            registry,
            horizon,
            inner: Mutex::new(Inner {
                pins: TransactionPins::new(dir),
                // Zero assumes nothing about the file or the cache; a read-write open sets both
                // properly via initialize_extended_header()
                state_committed: 0,
                cache_floor: 0,
            }),
            write_lock: Mutex::new(write_lock),
            metadata_lock: Mutex::new(None),
        })
    }

    /// True when this process's in-memory state is authoritative: it is the only process that may
    /// write, and it holds the write lock for as long as it is open, so nothing can change the
    /// file behind its back and its state may legitimately be ahead of the file.
    fn authoritative(&self) -> bool {
        matches!(self.mode, WriterMode::SingleWriterProcess) && !self.read_only
    }

    pub(crate) fn mode(&self) -> WriterMode {
        self.mode
    }

    /// True when the write lock is taken per transaction, so that another process can take over
    /// between this one's transactions.
    pub(crate) fn shares_write_lock(&self) -> bool {
        matches!(self.mode, WriterMode::MultiWriterProcess)
    }

    /// A non-durable commit lives only in the committing process's memory, so it would be silently
    /// discarded the moment another process took the write lock.
    pub(crate) fn allows_non_durable_commit(&self) -> bool {
        matches!(self.mode, WriterMode::SingleWriterProcess)
    }

    /// Keeps the shared lock on the directory's `metadata` file for as long as this coordinator
    /// lives. Taken once the marker exists.
    pub(crate) fn hold_metadata_lock(&self, file: File) -> Result<()> {
        *self.metadata_lock.lock()? = Some(file);
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        Ok(self.inner.lock()?)
    }

    /// Drops the read cache if the collection horizon says a page in it may have been reused, and
    /// raises the floor to what the cache can hold from now on. `horizon` is what the extended
    /// header stored, `None` if it did not verify -- in which case the worst is assumed, per the
    /// crash rules in `docs/design.md`. `id` is the latest transaction id, read from the file
    /// under the registry lock the caller still holds.
    fn revalidate_cache(
        &self,
        inner: &mut Inner,
        horizon: Option<u64>,
        local_oldest: Option<TransactionId>,
        id: TransactionId,
    ) {
        let stale = match horizon {
            Some(horizon) => horizon > inner.cache_floor,
            None => true,
        };
        if stale {
            self.mem.clear_read_cache();
            // The oldest transaction this process has active once the caller's begins. Not simply
            // `id`: a read transaction this process already holds goes on reading from its own,
            // older snapshot
            let floor = local_oldest.map_or(id, |oldest| oldest.min(id));
            inner.cache_floor = floor.raw_id();
        }
    }

    /// Registers a read transaction and returns the transaction it may read.
    ///
    /// Reading the latest transaction id, checking the horizon against the cache, and pinning the
    /// transaction in `txn/` all happen under one shared hold of the registry lock. A writer takes
    /// that lock exclusively to flip the header or scan the pins, so either the writer sees this
    /// pin, or this read starts strictly after the flip and reads the file it left behind.
    pub(crate) fn begin_read(
        &self,
        local_write_transaction_live: bool,
        local_oldest: Option<TransactionId>,
    ) -> Result<TransactionId> {
        if self.authoritative() {
            // The single writer's own state is the database, and no other process can collect
            // pages out from under it
            return self.mem.get_last_committed_transaction_id();
        }
        let mut inner = self.lock()?;
        let guard = self.registry.shared()?;
        let result = self.begin_read_locked(&mut inner, local_write_transaction_live, local_oldest);
        drop(guard);
        result
    }

    fn begin_read_locked(
        &self,
        inner: &mut Inner,
        local_write_transaction_live: bool,
        local_oldest: Option<TransactionId>,
    ) -> Result<TransactionId> {
        let id = if local_write_transaction_live {
            // This process holds the write lock, so nothing can have committed or collected since
            // the transaction began, and rereading the file could roll back state the writer is
            // using
            self.mem.get_last_committed_transaction_id()?
        } else {
            // Always from the file, never cached
            let (id, raw) = self.mem.reload_transaction_slots()?;
            let horizon = self.horizon.read_stored(&raw)?;
            self.revalidate_cache(inner, horizon, local_oldest, id);
            id
        };
        let pinned = local_oldest.map_or(id, |oldest| oldest.min(id));
        inner.pins.publish(Some(pinned.raw_id()))?;
        Ok(id)
    }

    /// Publishes the oldest transaction this process still needs, which is only ever used to raise
    /// it: lowering it happens in [`Self::begin_read`], under the same lock as the read of the
    /// transaction being pinned.
    pub(crate) fn publish_pinned(&self, local_oldest: Option<TransactionId>) -> Result<()> {
        if self.authoritative() {
            // Nothing scans this process's pin: it is the only writer, and it consults its own
            // in-memory tracker directly
            return Ok(());
        }
        let mut inner = self.lock()?;
        let pinned = local_oldest.map(TransactionId::raw_id);
        if inner.pins.published() == pinned {
            return Ok(());
        }
        let guard = self.registry.shared()?;
        let result = inner.pins.publish(pinned);
        drop(guard);
        result
    }

    /// The oldest transaction pinned by any process, past which no page may be reclaimed.
    /// `local_oldest` is this process's own answer, which the caller must read under the same lock
    /// that read transactions register under.
    pub(crate) fn oldest_pinned_globally(
        &self,
        local_oldest: Option<TransactionId>,
    ) -> Result<Option<TransactionId>> {
        let inner = self.lock()?;
        let guard = self.registry.exclusive()?;
        let result = inner.pins.scan_oldest();
        drop(guard);
        // Includes this process's own pin, which is harmless: it is the same value
        // `local_oldest` carries, so the minimum below is unchanged
        let remote = result?.map(TransactionId::new);
        Ok(match (local_oldest, remote) {
            (Some(local), Some(remote)) => Some(local.min(remote)),
            (local, None) => local,
            (None, remote) => remote,
        })
    }

    /// Records that transactions up to and including `free_until - 1` have been garbage collected.
    /// Nothing is written yet: the reuse only becomes visible to another process at the commit
    /// flip, and that is when the commit hook writes the horizon recorded here.
    pub(crate) fn record_collection_horizon(&self, free_until: TransactionId) -> Result<()> {
        // `free_until` is exclusive; the extended header stores the newest collected, inclusive
        self.horizon.record(free_until.raw_id().saturating_sub(1))
    }

    /// The last transaction any process has committed durably, straight from the file. Read from
    /// the file even for the sole writer: its in-memory state runs ahead of the file after a
    /// non-durable commit, and this reports what would survive a crash.
    pub(crate) fn last_committed(&self) -> Result<TransactionId> {
        let guard = self.registry.shared()?;
        let result = self.mem.peek_committed_transaction_id();
        drop(guard);
        result
    }

    /// Takes the write lock and makes this process's state current, so that a write transaction
    /// can be started. Returns the last transaction any process has committed, which the caller
    /// uses to number the new transaction.
    ///
    /// In [`WriterMode::SingleWriterProcess`] the lock is already held and the state is already
    /// current, so this is free.
    pub(crate) fn begin_write(&self) -> Result<TransactionId> {
        if self.authoritative() {
            debug_assert!(self.write_lock.lock()?.is_held());
            return self.mem.get_last_committed_transaction_id();
        }
        assert!(
            !self.read_only,
            "a read-only handle cannot start a write transaction"
        );
        // Taken before the state is refreshed, so nothing can commit between the refresh and the
        // start of this transaction
        self.write_lock.lock()?.acquire()?;
        match self.refresh_for_write() {
            Ok(last_committed) => Ok(last_committed),
            Err(err) => {
                self.write_lock
                    .lock()
                    .map(|mut lock| lock.release())
                    .unwrap_or(());
                Err(err)
            }
        }
    }

    fn refresh_for_write(&self) -> Result<TransactionId> {
        let mut inner = self.lock()?;
        let guard = self.registry.shared()?;
        let (last_committed, raw) = self.mem.reload_transaction_slots()?;
        let horizon = self.horizon.read_stored(&raw)?;
        // Even when nothing new was committed, another process may have collected pages this one
        // has cached
        let local_oldest = inner.pins.published().map(TransactionId::new);
        self.revalidate_cache(&mut inner, horizon, local_oldest, last_committed);
        drop(guard);

        if let Some(horizon) = horizon {
            // What the file has published is a floor under everything this process publishes
            self.horizon.record(horizon)?;
        } else {
            // The extended header did not survive some previous writer's crash. Holding the write
            // lock entitles this process to repair it in place, with the fallback the crash rules
            // prescribe: the lowest pinned transaction, or the latest one minus one. Both
            // overstate the true horizon, which costs caches, not correctness
            let guard = self.registry.exclusive()?;
            let fallback = inner
                .pins
                .scan_oldest()?
                .unwrap_or_else(|| last_committed.raw_id().saturating_sub(1))
                .min(last_committed.raw_id().saturating_sub(1));
            let result = self.horizon.rewrite(fallback, &raw);
            drop(guard);
            result?;
        }

        // Ids are ordered across processes, so an unchanged id means the file is exactly the
        // state this process already has. Otherwise all of it is reloaded: even if a read
        // transaction has picked up newer commit slots, the rest still describes the older
        // database
        if last_committed.raw_id() != inner.state_committed {
            self.mem.reload_for_write().map_err(storage_error)?;
            self.load_allocator_state()?;
            inner.state_committed = last_committed.raw_id();
        }

        Ok(last_committed)
    }

    /// Loads the allocator state the previous writer left behind, or rebuilds it if that writer
    /// died mid-commit. The rebuild is safe concurrently with readers because it only reads
    /// committed pages and repopulates in-memory state.
    fn load_allocator_state(&self) -> Result<()> {
        if let Some(tree) = crate::Database::allocator_state_table(&self.mem)? {
            self.mem.load_allocator_state(&tree)?;
            #[cfg(debug_assertions)]
            crate::Database::mark_allocated_page_for_debug(&self.mem)?;
            return Ok(());
        }
        crate::Database::rebuild_allocator_state_shared(&self.mem).map_err(storage_error)?;
        #[cfg(debug_assertions)]
        crate::Database::mark_allocated_page_for_debug(&self.mem)?;
        Ok(())
    }

    /// Brings `extended-header` in step with the file, as part of opening the database read-write.
    /// The caller holds the write lock, and the in-memory state was just loaded from the file.
    ///
    /// Rewriting both slots makes the file verify from here on, whatever was in it: the zeroes of
    /// a fresh directory, a horizon torn by a crash, or one left by an earlier database whose file
    /// was replaced -- which the clamp also guards, since a horizon can never legitimately reach
    /// the latest transaction.
    pub(crate) fn initialize_extended_header(&self) -> Result<()> {
        let mut inner = self.lock()?;
        let guard = self.registry.exclusive()?;
        let (last_committed, raw) = self.mem.reload_transaction_slots()?;
        let newest_possible = last_committed.raw_id().saturating_sub(1);
        let horizon = match self.horizon.read_stored(&raw)? {
            Some(horizon) => horizon.min(newest_possible),
            // The crash-rule fallback; the scan also unlinks any pins a dead process left
            None => inner
                .pins
                .scan_oldest()?
                .unwrap_or(newest_possible)
                .min(newest_possible),
        };
        let result = self.horizon.rewrite(horizon, &raw);
        drop(guard);
        result?;

        // Everything cached so far was read through the state that was just loaded
        inner.state_committed = last_committed.raw_id();
        inner.cache_floor = last_committed.raw_id();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn cache_floor(&self) -> u64 {
        self.lock().unwrap().cache_floor
    }

    /// Releases the write lock, if this mode takes it per transaction. Also used to hand back the
    /// lock that opening the database takes.
    pub(crate) fn end_write(&self) {
        if self.authoritative() {
            return;
        }
        if let Ok(mut lock) = self.write_lock.lock() {
            lock.release();
        }
    }

    /// Releases everything the directory was being held with, once the database is closed. A
    /// lingering transaction guard can keep this coordinator alive past the close, and the locks
    /// must not outlive the storage they protect: the sole writer's lifetime hold would refuse
    /// every reopen, and the shared `metadata` hold would block an upgrader, for as long as that
    /// unusable guard exists.
    pub(crate) fn release_for_close(&self) {
        if let Ok(mut lock) = self.write_lock.lock() {
            lock.release();
        }
        if let Ok(mut metadata) = self.metadata_lock.lock() {
            *metadata = None;
        }
        // Unpinned as well: a transaction a lingering guard can never read again must not go on
        // holding every page freed after its snapshot
        if let Ok(mut inner) = self.lock() {
            let _ = inner.pins.publish(None);
        }
    }
}

impl std::fmt::Debug for ProcessCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessCoordinator")
            .field("mode", &self.mode)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

fn storage_error(error: DatabaseError) -> StorageError {
    match error {
        DatabaseError::Storage(storage) => storage,
        other => StorageError::Corrupted(other.to_string()),
    }
}
