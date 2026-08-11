//! The cross-process half of [`TransactionTracker`](crate::transaction_tracker::TransactionTracker).
//!
//! A single-process redb tracks live read transactions in memory, and a writer consults that
//! tracking before it hands a freed page back out. This type extends both halves of that across
//! processes: it publishes what this process has pinned into a lock file that other processes read,
//! and it reads what they have pinned before this process reclaims anything.
//!
//! It also keeps this process's view of the database file current, since another process's commits
//! only reach it through the file.

use crate::multi_process::locks::{
    DatabaseDir, Registry, SharedState, UNPINNED, WriteLock, WriterMode,
};
use crate::transaction_tracker::TransactionId;
use crate::tree_store::TransactionalMemory;
use crate::{DatabaseError, Result, StorageError};
use std::sync::{Arc, Mutex, MutexGuard};

struct Inner {
    registry: Registry,
    /// The `last_committed` value the in-memory commit slots reflect. A reader needs nothing more
    /// than these, so when the registry still reports this value it can skip re-reading the header.
    slots_committed: u64,
    /// The `last_committed` value the whole in-memory state reflects, including the allocator
    /// state and the layout. Only a write transaction loads those, and only it may rely on this:
    /// reloading the commit slots alone leaves the rest describing an older database.
    state_committed: u64,
    /// A bound on how old the pages in the read cache are: every one of them was read from a
    /// snapshot at or after this transaction. Raised only when the cache is emptied.
    ///
    /// A page cached from snapshot `S` can only be reallocated after being freed by some
    /// transaction `F > S` and reclaimed under a horizon `H > F`, so `H > S + 1` is exactly the
    /// condition under which the cache may have gone stale. Note that a write transaction reads
    /// the snapshot it starts from, so committing does not make this process's cache any newer.
    cache_floor: u64,
    /// The last reclamation announcement this process has seen or made. A different value means
    /// another process has reclaimed pages since, which is the only kind of reclamation that can
    /// invalidate this process's cache. See `SharedState::reclaim_sequence`.
    seen_reclaim_sequence: u64,
}

impl Inner {
    /// Whether another process may have reused a page this one has cached.
    fn cache_may_be_stale(&self, state: &SharedState) -> bool {
        state.reclaim_sequence != self.seen_reclaim_sequence
            && state.reclaim_horizon > self.cache_floor.saturating_add(1)
    }

    /// Drops the read cache if another process may have reused a page in it. The floor is only
    /// raised here, where the pages that set it are actually gone.
    fn revalidate_cache(&mut self, state: &SharedState, mem: &TransactionalMemory) {
        if self.cache_may_be_stale(state) {
            mem.clear_read_cache();
            self.cache_floor = state.last_committed;
        }
        self.seen_reclaim_sequence = state.reclaim_sequence;
    }

    /// Brings this process's view of the database up to date with the file, for a read
    /// transaction. Must be called with the registry locked, so that it is ordered against writers
    /// publishing.
    fn refresh(&mut self, mem: &TransactionalMemory) -> Result<()> {
        let state = self.registry.state()?;
        self.revalidate_cache(&state, mem);
        if state.last_committed != self.slots_committed {
            mem.reload_transaction_slots()?;
            self.slots_committed = state.last_committed;
        }
        Ok(())
    }
}

pub(crate) struct ProcessCoordinator {
    mem: Arc<TransactionalMemory>,
    mode: WriterMode,
    read_only: bool,
    inner: Mutex<Inner>,
    /// Held for this process's whole lifetime in [`WriterMode::SingleWriterProcess`], and for the
    /// duration of each write transaction in [`WriterMode::MultiWriterProcess`]
    write_lock: Mutex<WriteLock>,
}

impl ProcessCoordinator {
    pub(crate) fn new(
        dir: &DatabaseDir,
        mode: WriterMode,
        read_only: bool,
        write_lock: WriteLock,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self, DatabaseError> {
        let registry = Registry::open(dir)?;
        Ok(Self {
            mem,
            mode,
            read_only,
            inner: Mutex::new(Inner {
                registry,
                // Nothing has been read from the file through the page cache yet, and a read-only
                // handle has not had a chance to publish a slot, so assume the worst until the
                // first refresh
                slots_committed: 0,
                state_committed: 0,
                cache_floor: 0,
                seen_reclaim_sequence: 0,
            }),
            write_lock: Mutex::new(write_lock),
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

    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        Ok(self.inner.lock()?)
    }

    /// Records that this process's in-memory state matches the file as of `last_committed`, and
    /// that nothing it has cached predates that. Only valid while this process holds the write
    /// lock, which is what makes the two consistent.
    pub(crate) fn mark_state_current(&self, last_committed: TransactionId) -> Result<()> {
        let mut inner = self.lock()?;
        inner.registry.lock_shared()?;
        let result = inner.registry.state();
        inner.registry.unlock();
        inner.slots_committed = last_committed.raw_id();
        inner.state_committed = last_committed.raw_id();
        inner.cache_floor = last_committed.raw_id();
        inner.seen_reclaim_sequence = result?.reclaim_sequence;
        Ok(())
    }

    /// Registers a read transaction and returns the transaction it may read.
    ///
    /// Reading the transaction id and publishing it happen under the shared registry lock, which a
    /// writer scanning for the oldest pinned transaction takes exclusively. That makes the pair
    /// atomic with respect to the scan: either the writer sees this transaction pinned, or this
    /// process reads the file only after the scan, by which point the file is at least as new as
    /// the horizon that scan produced.
    pub(crate) fn begin_read(
        &self,
        local_write_transaction_live: bool,
        local_oldest: Option<TransactionId>,
    ) -> Result<TransactionId> {
        if self.authoritative() {
            return self.mem.get_last_committed_transaction_id();
        }
        let mut inner = self.lock()?;
        inner.registry.lock_shared()?;
        let result = self.begin_read_locked(&mut inner, local_write_transaction_live, local_oldest);
        inner.registry.unlock();
        result
    }

    fn begin_read_locked(
        &self,
        inner: &mut Inner,
        local_write_transaction_live: bool,
        local_oldest: Option<TransactionId>,
    ) -> Result<TransactionId> {
        // While a write transaction is live this process holds the write lock, so no other process
        // can commit, and its in-memory state can be ahead of the file. Reloading would be at best
        // pointless and at worst would roll back the layout the writer is allocating against
        if !local_write_transaction_live {
            inner.refresh(&self.mem)?;
        }
        let id = self.mem.get_last_committed_transaction_id()?;
        let pinned = local_oldest.map_or(id.raw_id(), |oldest| oldest.raw_id().min(id.raw_id()));
        inner.registry.publish_pinned(pinned)?;
        Ok(id)
    }

    /// Publishes the oldest transaction this process still needs, which is only ever used to raise
    /// it: lowering it happens in [`Self::begin_read`], under the same lock as the read of the
    /// transaction being pinned.
    pub(crate) fn publish_pinned(&self, local_oldest: Option<TransactionId>) -> Result<()> {
        if self.authoritative() {
            // Nothing scans this process's slot: it is the only writer, and it consults its own
            // in-memory tracker directly
            return Ok(());
        }
        let mut inner = self.lock()?;
        let pinned = local_oldest.map_or(UNPINNED, TransactionId::raw_id);
        if inner.registry.already_published(pinned) {
            return Ok(());
        }
        inner.registry.lock_shared()?;
        let result = inner.registry.publish_pinned(pinned);
        inner.registry.unlock();
        result
    }

    /// The oldest transaction pinned by any process, which is the horizon past which no page may
    /// be reclaimed. `local_oldest` is this process's own answer, which the caller must read under
    /// the same lock that read transactions register under.
    pub(crate) fn oldest_pinned_globally(
        &self,
        local_oldest: Option<TransactionId>,
    ) -> Result<Option<TransactionId>> {
        let mut inner = self.lock()?;
        inner.registry.lock_exclusive()?;
        let result = inner.registry.oldest_pinned_by_others();
        inner.registry.unlock();
        let remote = result?.map(TransactionId::new);
        Ok(match (local_oldest, remote) {
            (Some(local), Some(remote)) => Some(local.min(remote)),
            (local, None) => local,
            (None, remote) => remote,
        })
    }

    /// Announces that pages freed by transactions older than `horizon` are about to be handed back
    /// out. Publishing before reusing them is what lets another process decide whether its cached
    /// pages are still valid.
    pub(crate) fn publish_reclaim_horizon(&self, horizon: TransactionId) -> Result<()> {
        let mut inner = self.lock()?;
        inner.registry.lock_exclusive()?;
        let result = inner.registry.publish_reclaim_horizon(horizon.raw_id());
        inner.registry.unlock();
        // Recording the announcement as seen is what keeps this process from invalidating its own
        // cache over its own reclamation, which cannot make it stale: the pages it reuses are
        // written through the same cache
        inner.seen_reclaim_sequence = result?;
        Ok(())
    }

    /// Announces a commit that is on disk. Publishing after the commit is durable is what lets
    /// another process rely on the file being at least this new once it has read this value.
    pub(crate) fn publish_durable_commit(&self, last_committed: TransactionId) -> Result<()> {
        let mut inner = self.lock()?;
        inner.registry.lock_exclusive()?;
        let result = inner.registry.publish_commit(last_committed.raw_id());
        inner.registry.unlock();
        result?;
        // This process made the commit, so everything it holds describes it
        inner.slots_committed = last_committed.raw_id();
        inner.state_committed = last_committed.raw_id();
        Ok(())
    }

    /// The last transaction any process has made durable.
    pub(crate) fn last_committed(&self) -> Result<TransactionId> {
        let inner = self.lock()?;
        inner.registry.lock_shared()?;
        let result = inner.registry.state();
        inner.registry.unlock();
        Ok(TransactionId::new(result?.last_committed))
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
        // Blocks until the process that holds the write lock finishes its transaction. Taken
        // before the state is refreshed, so that nothing can commit between the refresh and the
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
        inner.registry.lock_shared()?;
        let result = inner.registry.state();
        inner.registry.unlock();
        let state = result?;

        // Whether or not anything has been committed, another process may have reused pages this
        // one has cached
        inner.revalidate_cache(&state, &self.mem);
        // When this process was the last to write, its in-memory state -- allocator state and
        // layout included -- is still the state of the file. Otherwise all of it is reloaded: even
        // if a read transaction has picked up another process's commit slots since, everything
        // else still describes the older database
        if state.last_committed != inner.state_committed {
            self.mem.reload_for_write().map_err(storage_error)?;
            self.load_allocator_state()?;
            inner.slots_committed = state.last_committed;
            inner.state_committed = state.last_committed;
        }

        Ok(TransactionId::new(state.last_committed))
    }

    /// Loads the allocator state another process left behind. Every commit in
    /// [`WriterMode::MultiWriterProcess`] is a quick-repair commit, so it is always there unless a
    /// writer died part way through one -- which needs a repair that only reopening can do, since
    /// it rebuilds state that live read transactions in this process may be using.
    fn load_allocator_state(&self) -> Result<()> {
        let Some(tree) = crate::Database::allocator_state_table(&self.mem)? else {
            return Err(StorageError::Corrupted(
                "No valid allocator state was left in the database by the previous writer. \
                 Reopen the database to repair it"
                    .to_string(),
            ));
        };
        self.mem.load_allocator_state(&tree)?;
        // The debug-assertion mirror of the allocator state was dropped with the state it mirrors
        #[cfg(debug_assertions)]
        crate::Database::mark_allocated_page_for_debug(&self.mem)?;
        Ok(())
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
