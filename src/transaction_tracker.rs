use crate::multiprocess::{LockedFile, MultiProcessTracker, MultiProcessWriteMode};
use crate::tree_store::TransactionalMemory;
use crate::{DatabaseError, Key, Result, Savepoint, TypeName, Value};
#[cfg(feature = "logging")]
use log::debug;
use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::mem;
use std::mem::size_of;
use std::sync::{Arc, Condvar, Mutex};

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
    write_transaction_pending: bool,
    commit_in_progress: bool,
    valid_savepoints: BTreeMap<SavepointId, TransactionId>,
    // Subset of valid_savepoints that are persistent
    persistent_savepoints: BTreeSet<SavepointId>,
    // Non-durable commits that are still in-memory, and waiting for a durable commit to get flushed
    // We need to make sure that the freed-table does not get processed for these, since they are not durable yet
    // Therefore, we hold a read transaction on their nearest durable ancestor
    //
    // Maps non-durable transaction id -> durable ancestor
    pending_non_durable_commits: HashMap<TransactionId, TransactionId>,
    // Non-durable commits which have NOT been processed in the freed table
    unprocessed_freed_non_durable_commits: BTreeSet<TransactionId>,
    // Set when the Database was dropped while a write transaction was live. That transaction
    // keeps the database open, and end_write_transaction() hands the close back to it when
    // it ends
    deferred_close: Option<Arc<TransactionalMemory>>,
}

pub(crate) struct TransactionTracker {
    state: Mutex<State>,
    live_write_transaction_available: Condvar,
    multi_process: Option<MultiProcessTracker>,
}

pub(crate) struct CommitGuard {
    tracker: Arc<TransactionTracker>,
    lock: Option<LockedFile>,
}

impl Drop for CommitGuard {
    fn drop(&mut self) {
        if self.lock.is_some() {
            self.tracker.state.lock().unwrap().commit_in_progress = false;
            self.lock.take();
        }
    }
}

impl TransactionTracker {
    pub(crate) fn new(next_transaction_id: TransactionId) -> Self {
        Self {
            state: Mutex::new(State {
                next_savepoint_id: SavepointId(0),
                live_read_transactions: BTreeMap::default(),
                next_transaction_id,
                live_write_transaction: None,
                write_transaction_pending: false,
                commit_in_progress: false,
                valid_savepoints: BTreeMap::default(),
                persistent_savepoints: BTreeSet::default(),
                pending_non_durable_commits: HashMap::default(),
                unprocessed_freed_non_durable_commits: BTreeSet::default(),
                deferred_close: None,
            }),
            live_write_transaction_available: Condvar::new(),
            multi_process: None,
        }
    }

    pub(crate) fn new_multiprocess(
        next_transaction_id: TransactionId,
        lock_directory: &std::path::Path,
        write_mode: MultiProcessWriteMode,
    ) -> Result<Self, DatabaseError> {
        let mut tracker = Self::new(next_transaction_id);
        tracker.multi_process = Some(MultiProcessTracker::new(lock_directory, write_mode)?);
        Ok(tracker)
    }

    pub(crate) fn start_write_transaction(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<(TransactionId, bool)> {
        let mut state = self.state.lock().unwrap();
        while state.write_transaction_pending {
            state = self.live_write_transaction_available.wait(state).unwrap();
        }
        state.write_transaction_pending = true;

        if self.multi_process.is_none() {
            let transaction_id = state.next_transaction_id.increment();
            state.live_write_transaction = Some(transaction_id);
            return Ok((transaction_id, false));
        }
        drop(state);

        let multi_process = self.multi_process.as_ref().unwrap();
        let reload_allocator = match multi_process.lock_writer() {
            Ok(reload_allocator) => reload_allocator,
            Err(error) => {
                self.cancel_pending_write();
                return Err(error);
            }
        };
        if reload_allocator {
            let reload_result = (|| {
                let _reader_gate = multi_process.lock_reader_gate_exclusive()?;
                mem.reload_multiprocess_write()
            })();
            if let Err(error) = reload_result {
                let _ = multi_process.abandon_writer();
                self.cancel_pending_write();
                return Err(error);
            }
        }

        let last_committed = match mem.get_last_committed_transaction_id() {
            Ok(id) => id,
            Err(error) => {
                if reload_allocator {
                    let _ = multi_process.abandon_writer();
                } else {
                    let _ = multi_process.end_write_transaction();
                }
                self.cancel_pending_write();
                return Err(error);
            }
        };

        let mut state = self.state.lock().unwrap();
        state.next_transaction_id = last_committed;
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.live_write_transaction = Some(transaction_id);

        Ok((transaction_id, reload_allocator))
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
        state.write_transaction_pending = false;
        if let Some(multi_process) = &self.multi_process {
            multi_process.end_write_transaction().unwrap();
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

    pub(crate) fn multiprocess_writer_initialized(&self) {
        self.multi_process.as_ref().unwrap().writer_initialized();
    }

    pub(crate) fn is_multiprocess(&self) -> bool {
        self.multi_process.is_some()
    }

    pub(crate) fn multiple_writers(&self) -> bool {
        self.multi_process
            .as_ref()
            .is_some_and(MultiProcessTracker::multiple_writers)
    }

    fn cancel_pending_write(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(state.write_transaction_pending);
        assert!(state.live_write_transaction.is_none());
        state.write_transaction_pending = false;
        self.live_write_transaction_available.notify_one();
    }

    pub(crate) fn prepare_commit(self: &Arc<Self>) -> Result<CommitGuard> {
        let lock = self
            .multi_process
            .as_ref()
            .map(MultiProcessTracker::prepare_commit)
            .transpose()?;
        if lock.is_some() {
            let mut state = self.state.lock()?;
            assert!(!state.commit_in_progress);
            state.commit_in_progress = true;
        }
        Ok(CommitGuard {
            tracker: self.clone(),
            lock,
        })
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
        let (mut state, _reader_gate) = loop {
            let mut state = self.state.lock()?;
            while state.write_transaction_pending && state.live_write_transaction.is_none() {
                state = self.live_write_transaction_available.wait(state)?;
            }

            let Some(multi_process) = &self.multi_process else {
                break (state, None);
            };
            drop(state);
            let reader_gate = multi_process.lock_reader_gate_shared()?;
            let state = self.state.lock()?;
            if state.write_transaction_pending && state.live_write_transaction.is_none() {
                drop(state);
                drop(reader_gate);
                continue;
            }
            break (state, Some(reader_gate));
        };
        if self.multi_process.is_some() && state.live_write_transaction.is_none() {
            mem.reload_multiprocess_read()?;
        }
        let id = mem.get_last_committed_transaction_id()?;
        state
            .live_read_transactions
            .entry(id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        if let Some(multi_process) = &self.multi_process
            && let Err(error) = multi_process
                .publish_oldest_reader(state.live_read_transactions.keys().next().copied())
        {
            let ref_count = state.live_read_transactions.get_mut(&id).unwrap();
            *ref_count -= 1;
            if *ref_count == 0 {
                state.live_read_transactions.remove(&id);
            }
            return Err(error);
        }

        Ok(id)
    }

    pub(crate) fn deallocate_read_transaction(&self, id: TransactionId) {
        let commit_in_progress = self.state.lock().unwrap().commit_in_progress;
        let reader_gate = if commit_in_progress {
            None
        } else {
            self.multi_process
                .as_ref()
                .map(MultiProcessTracker::lock_reader_gate_shared)
        };
        let may_publish = reader_gate.as_ref().is_none_or(|result| result.is_ok());
        let _reader_gate = reader_gate.and_then(std::result::Result::ok);
        let mut state = self.state.lock().unwrap();
        let ref_count = state.live_read_transactions.get_mut(&id).unwrap();
        *ref_count -= 1;
        if *ref_count == 0 {
            state.live_read_transactions.remove(&id);
        }
        if may_publish && let Some(multi_process) = &self.multi_process {
            let _ = multi_process
                .publish_oldest_reader(state.live_read_transactions.keys().next().copied());
        }
    }

    pub(crate) fn synchronize_persistent_savepoints(
        &self,
        next_savepoint: Option<SavepointId>,
        savepoints: &[(SavepointId, TransactionId)],
    ) -> Result {
        let _reader_gate = self
            .multi_process
            .as_ref()
            .map(MultiProcessTracker::lock_reader_gate_shared)
            .transpose()?;
        let mut state = self.state.lock()?;

        let previous: Vec<(SavepointId, TransactionId)> = state
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
            }
        }

        if let Some(next_savepoint) = next_savepoint {
            state.next_savepoint_id = next_savepoint;
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
        if let Some(multi_process) = &self.multi_process {
            multi_process
                .publish_oldest_reader(state.live_read_transactions.keys().next().copied())?;
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
                std::ops::Bound::Excluded(id),
                std::ops::Bound::Unbounded::<SavepointId>,
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

    pub(crate) fn oldest_live_read_transaction(&self) -> Option<TransactionId> {
        let local = self
            .state
            .lock()
            .unwrap()
            .live_read_transactions
            .keys()
            .next()
            .copied();
        let external = self
            .multi_process
            .as_ref()
            .and_then(MultiProcessTracker::external_oldest_reader);
        match (local, external) {
            (Some(local), Some(external)) => Some(local.min(external)),
            (Some(local), None) => Some(local),
            (None, Some(external)) => Some(external),
            (None, None) => None,
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
