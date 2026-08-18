#[cfg(all(feature = "experimental-multiprocess", not(redb_no_std)))]
use crate::multi_process::ProcessCoordinator;
use crate::sync::{Condvar, Mutex};
use crate::tree_store::TransactionalMemory;
use crate::{Key, Result, Savepoint, TypeName, Value};
use alloc::collections::BTreeSet;
use alloc::collections::btree_map::BTreeMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::mem;
use core::mem::size_of;
#[cfg(feature = "logging")]
use log::debug;

#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct TransactionId(u64);

impl TransactionId {
    pub(crate) fn new(value: u64) -> TransactionId {
        Self(value)
    }

    pub(crate) fn raw_id(self) -> u64 {
        self.0
    }

    pub(crate) fn next(self) -> TransactionId {
        TransactionId(self.0 + 1)
    }

    pub(crate) fn increment(&mut self) -> TransactionId {
        let next = self.next();
        *self = next;
        next
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SavepointId(pub u64);

impl SavepointId {
    pub(crate) fn next(self) -> SavepointId {
        SavepointId(self.0 + 1)
    }
}

impl Value for SavepointId {
    type SelfType<'a> = SavepointId;
    type AsBytes<'a> = [u8; size_of::<u64>()];

    fn fixed_width() -> Option<usize> {
        Some(size_of::<u64>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        SavepointId(u64::from_le_bytes(data.try_into().unwrap()))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::SavepointId")
    }
}

impl Key for SavepointId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).0.cmp(&Self::from_bytes(data2).0)
    }
}

struct State {
    next_savepoint_id: SavepointId,
    // reference count of read transactions per transaction id
    live_read_transactions: BTreeMap<TransactionId, u64>,
    next_transaction_id: TransactionId,
    live_write_transaction: Option<TransactionId>,
    valid_savepoints: BTreeMap<SavepointId, TransactionId>,
    // Subset of valid_savepoints that are persistent
    persistent_savepoints: BTreeSet<SavepointId>,
    // Non-durable commits that are still in-memory, and waiting for a durable commit to get flushed
    // We need to make sure that the freed-table does not get processed for these, since they are not durable yet
    // Therefore, we hold a read transaction on their nearest durable ancestor
    //
    // Maps non-durable transaction id -> durable ancestor
    pending_non_durable_commits: BTreeMap<TransactionId, TransactionId>,
    // Non-durable commits which have NOT been processed in the freed table
    unprocessed_freed_non_durable_commits: BTreeSet<TransactionId>,
    // True while a thread is starting a write transaction but has not yet been given an id. A
    // multi-process database takes the cross-process write lock and reloads its state in that
    // window, which must not overlap another write transaction but must not hold the state lock
    // either
    write_transaction_starting: bool,
    // Set when the Database was dropped while a write transaction was live. That transaction
    // keeps the database open, and end_write_transaction() hands the close back to it when
    // it ends
    deferred_close: Option<Arc<TransactionalMemory>>,
}

impl State {
    // The oldest transaction this process needs kept alive: read transactions and savepoints both
    // hold a reference here
    fn oldest_pinned(&self) -> Option<TransactionId> {
        self.live_read_transactions.keys().next().copied()
    }
}

pub(crate) struct TransactionTracker {
    state: Mutex<State>,
    live_write_transaction_available: Condvar,
    // Set for a multi-process database: the half of this tracker that other processes can see.
    // Every path that pins a transaction publishes through it, and every path that reclaims pages
    // consults it
    process: Option<Arc<ProcessCoordinator>>,
}

impl TransactionTracker {
    pub(crate) fn new(next_transaction_id: TransactionId) -> Self {
        Self::new_inner(next_transaction_id, None)
    }

    pub(crate) fn new_multi_process(
        next_transaction_id: TransactionId,
        process: Arc<ProcessCoordinator>,
    ) -> Self {
        Self::new_inner(next_transaction_id, Some(process))
    }

    fn new_inner(
        next_transaction_id: TransactionId,
        process: Option<Arc<ProcessCoordinator>>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                next_savepoint_id: SavepointId(0),
                live_read_transactions: BTreeMap::default(),
                next_transaction_id,
                live_write_transaction: None,
                valid_savepoints: BTreeMap::default(),
                persistent_savepoints: BTreeSet::default(),
                pending_non_durable_commits: BTreeMap::default(),
                unprocessed_freed_non_durable_commits: BTreeSet::default(),
                write_transaction_starting: false,
                deferred_close: None,
            }),
            live_write_transaction_available: Condvar::new(),
            process,
        }
    }

    pub(crate) fn process(&self) -> Option<&Arc<ProcessCoordinator>> {
        self.process.as_ref()
    }

    // True when another process may take over the write lock between this process's transactions,
    // so that everything this one leaves in memory has to be assumed lost
    pub(crate) fn write_lock_is_shared(&self) -> bool {
        self.process
            .as_ref()
            .is_some_and(|process| process.shares_write_lock())
    }

    // Every commit must be a quick-repair commit when another process may take over the write
    // lock, since it loads the allocator state this writes rather than rebuilding it
    pub(crate) fn requires_quick_repair(&self) -> bool {
        self.write_lock_is_shared()
    }

    // Every commit must be 2-phase when another process may be reading the file. A 1-phase commit
    // writes the new header and the pages it references in one flush, in no particular order, so a
    // reader in another process can see a header pointing at pages that are not in the file yet.
    // A crash cannot see that intermediate state, which is why 1-phase commits are safe for a
    // single process, but another process reading concurrently can.
    pub(crate) fn requires_two_phase_commit(&self) -> bool {
        self.process.is_some()
    }

    // True when the post-commit free epilogue must be skipped: it publishes its work as a
    // non-durable commit, which is invisible to every other process and is thrown away as soon as
    // one of them takes the write lock
    pub(crate) fn defers_post_commit_free(&self) -> bool {
        self.write_lock_is_shared()
    }

    // False when a non-durable commit would be invisible to, and silently discarded by, the next
    // process to write
    pub(crate) fn allows_non_durable_commit(&self) -> bool {
        self.process
            .as_ref()
            .is_none_or(|process| process.allows_non_durable_commit())
    }

    fn reserve_write_slot(&self) -> MutexGuard<'_, State> {
        let mut state = self.state.lock().unwrap();
        while state.live_write_transaction.is_some() || state.write_transaction_starting {
            state = self.live_write_transaction_available.wait(state).unwrap();
        }
        state
    }

    pub(crate) fn start_write_transaction(&self) -> TransactionId {
        let mut state = self.reserve_write_slot();
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.live_write_transaction = Some(transaction_id);

        transaction_id
    }

    // Starts a write transaction for a multi-process database. `prepare` runs with the write slot
    // reserved but the state lock released -- it blocks on the cross-process write lock, which
    // read transactions in this process must not have to wait for -- and returns the last
    // transaction any process has committed, which numbers the new transaction.
    pub(crate) fn start_write_transaction_prepared(
        &self,
        prepare: impl FnOnce() -> Result<TransactionId>,
    ) -> Result<TransactionId> {
        self.reserve_write_slot().write_transaction_starting = true;

        let prepared = prepare();

        let mut state = self.state.lock()?;
        state.write_transaction_starting = false;
        let last_committed = match prepared {
            Ok(last_committed) => last_committed,
            Err(err) => {
                self.live_write_transaction_available.notify_one();
                return Err(err);
            }
        };
        // Transaction ids come from the file rather than from this process's counter, so that they
        // stay ordered across processes. An aborted transaction's id is reused, exactly as it
        // would be if this process had closed the database and reopened it
        state.next_transaction_id = last_committed;
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.live_write_transaction = Some(transaction_id);

        Ok(transaction_id)
    }

    // Returns the deferred close, if the Database was dropped while this transaction was live.
    // The caller must close the database, now that the write transaction has ended
    pub(crate) fn end_write_transaction(
        &self,
        id: TransactionId,
    ) -> Option<Arc<TransactionalMemory>> {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.live_write_transaction.unwrap(), id);
        state.live_write_transaction = None;
        if let Some(process) = &self.process {
            // Hands the write lock to the next process, if this mode takes it per transaction
            process.end_write();
        }
        self.live_write_transaction_available.notify_one();
        state.deferred_close.take()
    }

    // Defers the database close to the end of the live write transaction, if one exists.
    // Returns false if there is no live write transaction, in which case the caller must close
    // the database itself. The check and the handoff share the state lock, making this atomic
    // with end_write_transaction(), so exactly one side performs the close
    pub(crate) fn defer_close_if_write_transaction_live(
        &self,
        mem: &Arc<TransactionalMemory>,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.live_write_transaction.is_some() {
            state.deferred_close = Some(mem.clone());
            true
        } else {
            false
        }
    }

    pub(crate) fn clear_pending_non_durable_commits(&self) {
        let mut state = self.state.lock().unwrap();
        let ids = mem::take(&mut state.pending_non_durable_commits);
        for (_, durable_ancestor) in ids {
            let ref_count = state
                .live_read_transactions
                .get_mut(&durable_ancestor)
                .unwrap();
            *ref_count -= 1;
            if *ref_count == 0 {
                state.live_read_transactions.remove(&durable_ancestor);
            }
        }
        self.publish_pinned(&state);
    }

    pub(crate) fn is_unprocessed_non_durable_commit(&self, id: TransactionId) -> bool {
        let state = self.state.lock().unwrap();
        state.unprocessed_freed_non_durable_commits.contains(&id)
    }

    pub(crate) fn mark_non_durable_freed_pages_processed(
        &self,
        ids: impl IntoIterator<Item = TransactionId>,
    ) {
        let mut state = self.state.lock().unwrap();
        for id in ids {
            state.unprocessed_freed_non_durable_commits.remove(&id);
        }
    }

    pub(crate) fn oldest_unprocessed_non_durable_commit(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        state
            .unprocessed_freed_non_durable_commits
            .iter()
            .next()
            .copied()
    }

    // `has_unprocessed_freed_pages` is true when a future non-durable commit should scan
    // freed-table entries under this transaction id for pages that can be reclaimed before the
    // id becomes durable.
    pub(crate) fn register_non_durable_commit(
        &self,
        id: TransactionId,
        durable_ancestor: TransactionId,
        has_unprocessed_freed_pages: bool,
    ) {
        let mut state = self.state.lock().unwrap();
        state
            .live_read_transactions
            .entry(durable_ancestor)
            .and_modify(|x| *x += 1)
            .or_insert(1);
        assert!(
            state
                .pending_non_durable_commits
                .insert(id, durable_ancestor)
                .is_none()
        );
        if has_unprocessed_freed_pages {
            state.unprocessed_freed_non_durable_commits.insert(id);
        }
        self.publish_pinned(&state);
    }

    // Reserve a transaction id that was created without starting a new write transaction.
    // The caller must still register the resulting root if it is a non-durable commit.
    pub(crate) fn reserve_transaction_id(
        &self,
        id: TransactionId,
        live_write_transaction: TransactionId,
    ) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.live_write_transaction, Some(live_write_transaction));
        assert_eq!(id, state.next_transaction_id.next());
        state.next_transaction_id = id;
    }

    pub(crate) fn restore_savepoint_counter_state(&self, next_savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        assert!(state.valid_savepoints.is_empty());
        assert!(state.persistent_savepoints.is_empty());
        state.next_savepoint_id = next_savepoint;
    }

    pub(crate) fn register_persistent_savepoint(&self, savepoint: &Savepoint) {
        let mut state = self.state.lock().unwrap();
        state
            .live_read_transactions
            .entry(savepoint.get_transaction_id())
            .and_modify(|x| *x += 1)
            .or_insert(1);
        state
            .valid_savepoints
            .insert(savepoint.get_id(), savepoint.get_transaction_id());
        state.persistent_savepoints.insert(savepoint.get_id());
        // A persistent savepoint pins a transaction older than any live read, so other processes
        // have to be told about it before they can reclaim anything
        self.publish_pinned(&state);
    }

    // Marks an already-registered savepoint as persistent
    pub(crate) fn mark_savepoint_persistent(&self, id: SavepointId) {
        let mut state = self.state.lock().unwrap();
        assert!(state.valid_savepoints.contains_key(&id));
        state.persistent_savepoints.insert(id);
    }

    pub(crate) fn register_read_transaction(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<TransactionId> {
        let mut state = self.state.lock()?;
        let id = if let Some(process) = &self.process {
            // Held across the cross-process registration, so that a writer in this process cannot
            // read the oldest pinned transaction in between publishing the new one and recording
            // it here
            let oldest = state.oldest_pinned();
            process.begin_read(state.live_write_transaction.is_some(), oldest)?
        } else {
            mem.get_last_committed_transaction_id()?
        };
        state
            .live_read_transactions
            .entry(id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok(id)
    }

    pub(crate) fn deallocate_read_transaction(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        let ref_count = state.live_read_transactions.get_mut(&id).unwrap();
        *ref_count -= 1;
        if *ref_count == 0 {
            state.live_read_transactions.remove(&id);
            self.publish_pinned(&state);
        }
    }

    // Tells other processes what this one still needs, after the set of pinned transactions has
    // changed. A failure to publish only leaves a stale, older value in this process's slot, which
    // costs other processes some page reclamation but is never unsafe -- and this runs on drop
    // paths that have nowhere to report an error to.
    fn publish_pinned(&self, state: &State) {
        if let Some(process) = &self.process {
            let _ = process.publish_pinned(state.oldest_pinned());
        }
    }

    pub(crate) fn any_savepoint_exists(&self) -> bool {
        !self.state.lock().unwrap().valid_savepoints.is_empty()
    }

    pub(crate) fn any_persistent_savepoint_exists(&self) -> bool {
        !self.state.lock().unwrap().persistent_savepoints.is_empty()
    }

    // True if an ephemeral (non-persistent) savepoint exists. Unlike persistent ones, it may pin a
    // non-durable transaction whose pages a reload would discard.
    pub(crate) fn any_ephemeral_savepoint_exists(&self) -> bool {
        let state = self.state.lock().unwrap();
        state
            .valid_savepoints
            .keys()
            .any(|id| !state.persistent_savepoints.contains(id))
    }

    // Excludes internal read refs that only pin durable ancestors of pending
    // non-durable commits.
    pub(crate) fn any_user_read_reference_exists(&self) -> bool {
        let state = self.state.lock().unwrap();
        for (id, count) in &state.live_read_transactions {
            let pending_count = state
                .pending_non_durable_commits
                .values()
                .filter(|ancestor| *ancestor == id)
                .count() as u64;
            if *count > pending_count {
                return true;
            }
        }
        false
    }

    pub(crate) fn allocate_savepoint(&self, transaction_id: TransactionId) -> SavepointId {
        let mut state = self.state.lock().unwrap();
        let id = state.next_savepoint_id.next();
        state.next_savepoint_id = id;
        state.valid_savepoints.insert(id, transaction_id);
        id
    }

    // Deallocates the given savepoint and its matching reference count on the transcation
    pub(crate) fn deallocate_savepoint(&self, savepoint: SavepointId, transaction: TransactionId) {
        {
            let mut state = self.state.lock().unwrap();
            state.valid_savepoints.remove(&savepoint);
            state.persistent_savepoints.remove(&savepoint);
        }
        self.deallocate_read_transaction(transaction);
    }

    pub(crate) fn is_valid_savepoint(&self, id: SavepointId) -> bool {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .contains_key(&id)
    }

    pub(crate) fn list_savepoints_after(&self, id: SavepointId) -> Vec<SavepointId> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .range((
                core::ops::Bound::Excluded(id),
                core::ops::Bound::Unbounded::<SavepointId>,
            ))
            .map(|(x, _)| *x)
            .collect()
    }

    // Removes the given savepoints from the in-memory `valid_savepoints` map without touching
    // live_read_transactions refs. The caller is responsible for making sure those refs are
    // released by some other means: ephemeral `Savepoint::drop` or `deallocate_savepoint`
    // (called via `delete_persistent_savepoint`) do that for their respective savepoint kinds.
    //
    // Savepoints that have already been removed (for example, by `deallocate_savepoint` earlier
    // in the same transaction) are silently skipped.
    pub(crate) fn invalidate_savepoints(&self, savepoints: impl IntoIterator<Item = SavepointId>) {
        let mut state = self.state.lock().unwrap();
        for id in savepoints {
            state.valid_savepoints.remove(&id);
            state.persistent_savepoints.remove(&id);
        }
    }

    // The oldest savepoint, ignoring the given savepoint ids. Used by committing transactions
    // to compute the savepoint horizon as it will be after their staged savepoint deletions
    // are applied on commit.
    pub(crate) fn oldest_savepoint_excluding(
        &self,
        exclude: &BTreeSet<SavepointId>,
    ) -> Option<(SavepointId, TransactionId)> {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .iter()
            .find(|(id, _)| !exclude.contains(id))
            .map(|(id, txn_id)| (*id, *txn_id))
    }

    // The oldest transaction that must be kept readable, across every process that has the
    // database open. Nothing freed by a transaction at or after this may be reclaimed.
    //
    // The state lock is held across the cross-process scan, which makes the scan atomic with
    // respect to read transactions registering in this process, exactly as it already was for the
    // in-memory half.
    pub(crate) fn oldest_live_read_transaction(&self) -> Result<Option<TransactionId>> {
        let state = self.state.lock()?;
        let local = state.oldest_pinned();
        match &self.process {
            Some(process) => process.oldest_pinned_globally(local),
            None => Ok(local),
        }
    }

    // The highest free horizon that is safe to use when other processes may be reading, or None
    // when none can.
    //
    // A process that is about to register a read transaction takes its snapshot from the last
    // durable commit in the file, and may do so at any moment before its registration becomes
    // visible here. Bounding the horizon by that snapshot is what makes the window between the two
    // safe, and it is not implied by the horizon this process computes: a commit's non-durable
    // free epilogue, and non-durable commits in general, both leave the in-memory transaction id
    // ahead of the file.
    pub(crate) fn cross_process_free_limit(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<Option<TransactionId>> {
        if self.process.is_none() {
            return Ok(None);
        }
        Ok(Some(mem.get_last_durable_transaction_id()?.next()))
    }

    // Announces that pages freed by transactions older than `horizon` are about to be reclaimed,
    // so that other processes can tell whether their cached pages may have been reused. Must be
    // called before any such page is handed out.
    pub(crate) fn publish_reclaim_horizon(&self, horizon: TransactionId) -> Result<()> {
        match &self.process {
            Some(process) => process.publish_reclaim_horizon(horizon),
            None => Ok(()),
        }
    }

    // Announces a commit that is on disk, so that other processes pick it up.
    pub(crate) fn publish_durable_commit(&self, last_committed: TransactionId) -> Result<()> {
        match &self.process {
            Some(process) => process.publish_durable_commit(last_committed),
            None => Ok(()),
        }
    }

    // Returns the transaction id of the oldest non-durable transaction which has not been processed
    // for freeing, which has live read transactions
    pub(crate) fn oldest_live_read_nondurable_transaction(&self) -> Option<TransactionId> {
        let state = self.state.lock().unwrap();
        for id in state.live_read_transactions.keys() {
            if state.pending_non_durable_commits.contains_key(id) {
                return Some(*id);
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn non_durable_commit_without_freed_pages_is_not_unprocessed() {
        let tracker = TransactionTracker::new(TransactionId::new(0));

        tracker.register_non_durable_commit(TransactionId::new(1), TransactionId::new(0), false);
        assert_eq!(None, tracker.oldest_unprocessed_non_durable_commit());

        tracker.register_non_durable_commit(TransactionId::new(2), TransactionId::new(0), true);
        assert_eq!(
            Some(TransactionId::new(2)),
            tracker.oldest_unprocessed_non_durable_commit()
        );
    }
}
