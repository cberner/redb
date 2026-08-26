#[cfg(not(redb_no_std))]
use crate::multiprocess::{CommitGuard, PreparedWrite, ProcessCoordinator, TransactionPin};
use crate::sync::{Condvar, Mutex, MutexGuard};
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
    // Set while a thread has reserved the local write slot and is waiting for the cross-process
    // writer lock.
    write_transaction_starting: bool,
    #[cfg(not(redb_no_std))]
    process_read_pins: BTreeMap<TransactionId, TransactionPin>,
    #[cfg(not(redb_no_std))]
    write_snapshot_pin: Option<TransactionPin>,
    // Set when the Database was dropped while a write transaction was live. That transaction
    // keeps the database open, and end_write_transaction() hands the close back to it when
    // it ends
    deferred_close: Option<Arc<TransactionalMemory>>,
}

pub(crate) struct TransactionTracker {
    state: Mutex<State>,
    live_write_transaction_available: Condvar,
    #[cfg(not(redb_no_std))]
    process: Option<Arc<ProcessCoordinator>>,
}

impl TransactionTracker {
    pub(crate) fn new(next_transaction_id: TransactionId) -> Self {
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
                #[cfg(not(redb_no_std))]
                process_read_pins: BTreeMap::default(),
                #[cfg(not(redb_no_std))]
                write_snapshot_pin: None,
                deferred_close: None,
            }),
            live_write_transaction_available: Condvar::new(),
            #[cfg(not(redb_no_std))]
            process: None,
        }
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn new_multiprocess(
        next_transaction_id: TransactionId,
        process: Arc<ProcessCoordinator>,
    ) -> Self {
        let mut tracker = Self::new(next_transaction_id);
        tracker.process = Some(process);
        tracker
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
        assert!(state.live_write_transaction.is_none());
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.live_write_transaction = Some(transaction_id);

        transaction_id
    }

    pub(crate) fn start_write_transaction_for(
        &self,
        mem: &Arc<TransactionalMemory>,
    ) -> Result<TransactionId> {
        #[cfg(not(redb_no_std))]
        if let Some(process) = &self.process {
            return self.start_write_transaction_prepared(|| process.begin_write(mem));
        }
        Ok(self.start_write_transaction())
    }

    #[cfg(not(redb_no_std))]
    fn start_write_transaction_prepared(
        &self,
        prepare: impl FnOnce() -> Result<PreparedWrite>,
    ) -> Result<TransactionId> {
        self.reserve_write_slot().write_transaction_starting = true;
        let prepared = prepare();

        let mut state = self.state.lock()?;
        state.write_transaction_starting = false;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.live_write_transaction_available.notify_one();
                return Err(error);
            }
        };
        state.next_transaction_id = prepared.last_committed;
        let transaction_id = state.next_transaction_id.increment();
        state.live_write_transaction = Some(transaction_id);
        state.write_snapshot_pin = prepared.pin;
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
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
        #[cfg(not(redb_no_std))]
        {
            state.write_snapshot_pin = None;
            if let Some(process) = &self.process {
                process.end_write();
            }
        }
        self.live_write_transaction_available.notify_one();
        state.deferred_close.take()
    }

    pub(crate) fn requires_quick_repair(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.process
                .as_ref()
                .is_some_and(|process| process.multiple_writers())
        }
        #[cfg(redb_no_std)]
        {
            false
        }
    }

    pub(crate) fn is_multiprocess(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.process.is_some()
        }
        #[cfg(redb_no_std)]
        {
            false
        }
    }

    pub(crate) fn requires_two_phase_commit(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.process.is_some()
        }
        #[cfg(redb_no_std)]
        {
            false
        }
    }

    pub(crate) fn defers_post_commit_free(&self) -> bool {
        self.requires_two_phase_commit()
    }

    pub(crate) fn allows_non_durable_commit(&self) -> bool {
        !self.requires_two_phase_commit()
    }

    pub(crate) fn allows_ephemeral_savepoint(&self) -> bool {
        #[cfg(not(redb_no_std))]
        {
            self.process
                .as_ref()
                .is_none_or(|process| !process.multiple_writers())
        }
        #[cfg(redb_no_std)]
        {
            true
        }
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

    // Reserves a repair commit's id so it is never issued again: crash recovery orders the
    // commit slots by transaction id, so an id must never be committed twice
    pub(crate) fn reserve_repair_transaction_id(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        assert!(state.live_write_transaction.is_none());
        state.next_transaction_id = state.next_transaction_id.max(id);
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
        #[cfg(not(redb_no_std))]
        let (id, pin) = if let Some(process) = &self.process {
            let local_oldest = state.live_read_transactions.keys().next().copied();
            process.begin_read(mem, state.live_write_transaction.is_some(), local_oldest)?
        } else {
            (mem.get_last_committed_transaction_id()?, None)
        };
        #[cfg(redb_no_std)]
        let id = mem.get_last_committed_transaction_id()?;

        #[cfg(not(redb_no_std))]
        if !state.live_read_transactions.contains_key(&id)
            && let Some(pin) = pin
        {
            state.process_read_pins.insert(id, pin);
        }
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
            #[cfg(not(redb_no_std))]
            state.process_read_pins.remove(&id);
        }
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn adopt_persistent_savepoints(
        &self,
        next_savepoint: Option<SavepointId>,
        savepoints: &[(SavepointId, TransactionId)],
    ) -> Result<()> {
        let mut state = self.state.lock()?;
        let previous: Vec<_> = state
            .persistent_savepoints
            .iter()
            .filter_map(|id| {
                state
                    .valid_savepoints
                    .get(id)
                    .map(|transaction| (*id, *transaction))
            })
            .collect();
        for (id, transaction) in previous {
            state.persistent_savepoints.remove(&id);
            state.valid_savepoints.remove(&id);
            let count = state.live_read_transactions.get_mut(&transaction).unwrap();
            *count -= 1;
            if *count == 0 {
                state.live_read_transactions.remove(&transaction);
                state.process_read_pins.remove(&transaction);
            }
        }

        if let Some(next_savepoint) = next_savepoint {
            state.next_savepoint_id = state.next_savepoint_id.max(next_savepoint);
        }
        for (id, transaction) in savepoints {
            state
                .live_read_transactions
                .entry(*transaction)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            state.valid_savepoints.insert(*id, *transaction);
            state.persistent_savepoints.insert(*id);
        }
        if let Some(oldest) = savepoints.iter().map(|(_, transaction)| *transaction).min()
            && let Some(process) = &self.process
        {
            process.track_cache_floor(oldest)?;
        }
        Ok(())
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

    pub(crate) fn oldest_live_read_transaction(&self) -> Result<Option<TransactionId>> {
        let state = self.state.lock()?;
        let local = state.live_read_transactions.keys().next().copied();
        #[cfg(not(redb_no_std))]
        if let Some(process) = &self.process {
            let local = state
                .write_snapshot_pin
                .as_ref()
                .map_or(local, |write_pin| {
                    let write = write_pin.transaction();
                    Some(local.map_or(write, |read| read.min(write)))
                });
            return process.oldest_active_transaction(local);
        }
        Ok(local)
    }

    #[cfg(not(redb_no_std))]
    pub(crate) fn prepare_commit(
        &self,
        mem: &TransactionalMemory,
        collection_horizon: TransactionId,
    ) -> Result<Option<CommitGuard>> {
        self.process
            .as_ref()
            .map(|process| process.prepare_commit(mem, collection_horizon))
            .transpose()
    }

    pub(crate) fn close(&self, mem: &TransactionalMemory) -> Result<()> {
        #[cfg(not(redb_no_std))]
        if let Some(process) = &self.process {
            return process.close(mem);
        }
        mem.close()
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
