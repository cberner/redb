use crate::sync::{Condvar, Mutex};
#[cfg(feature = "experimental-multiprocess")]
use crate::tree_store::HeaderGuard;
use crate::tree_store::TransactionalMemory;
#[cfg(feature = "experimental-multiprocess")]
use crate::tree_store::WriterLock;
use crate::{Key, Result, TypeName, Value};
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

// The write slot's state. It is taken ahead of the writer byte, and given its transaction's id
// once the writer byte is held and the file's latest commit is known.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum WriteSlotState {
    Free,
    Taken,
    Live(TransactionId),
}

struct State {
    next_savepoint_id: SavepointId,
    // reference count of read transactions per transaction id
    live_read_transactions: BTreeMap<TransactionId, u64>,
    // Subset of live_read_transactions which are persistent savepoints
    persistent_savepoint_references: BTreeMap<TransactionId, u64>,
    next_transaction_id: TransactionId,
    write_slot: WriteSlotState,
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
    // Set when the Database was dropped while a write transaction was live. That transaction
    // keeps the database open, and end_write_transaction() hands the close back to it when
    // it ends
    deferred_close: Option<Arc<TransactionalMemory>>,
}

impl State {
    // The references to `id` that are reads, which the "active transaction byte" announces to the
    // other processes. Persistent savepoints are excluded
    #[cfg(feature = "experimental-multiprocess")]
    fn active_transaction_lock_references(&self, id: TransactionId) -> u64 {
        self.live_read_transactions.get(&id).copied().unwrap_or(0)
            - self
                .persistent_savepoint_references
                .get(&id)
                .copied()
                .unwrap_or(0)
    }

    // Takes the "active transaction byte" as the first read of `id` appears, and releases it
    // as the last goes away, so a writer in another process sees exactly the transactions this one
    // is still reading. Done under the lock that holds the count, so the two cannot disagree: a
    // reference taken between the count reaching zero and the byte being released would otherwise
    // read a snapshot nothing protects
    fn add_active_transaction_lock_reference(
        &mut self,
        mem: &TransactionalMemory,
        id: TransactionId,
        #[cfg(feature = "experimental-multiprocess")] header: &HeaderGuard<'_>,
    ) -> Result {
        *self.live_read_transactions.entry(id).or_insert(0) += 1;
        #[cfg(feature = "experimental-multiprocess")]
        if self.active_transaction_lock_references(id) == 1
            && let Err(err) = mem.lock_mp_transaction(id, header)
        {
            self.decrement_reference_count(id);
            return Err(err);
        }
        #[cfg(not(feature = "experimental-multiprocess"))]
        let _ = mem;

        Ok(())
    }

    fn remove_active_transaction_lock_reference(
        &mut self,
        mem: &TransactionalMemory,
        id: TransactionId,
    ) {
        self.decrement_reference_count(id);
        // Failing to release only leaves a peer reclaiming less than it could
        #[cfg(feature = "experimental-multiprocess")]
        if self.active_transaction_lock_references(id) == 0 {
            let _ = mem.unlock_mp_transaction(id);
        }
        #[cfg(not(feature = "experimental-multiprocess"))]
        let _ = mem;
    }

    // A persistent savepoint's reference, which takes no byte. See `persistent_savepoint_references`
    fn reference_persistent_savepoint(&mut self, id: TransactionId) {
        *self.live_read_transactions.entry(id).or_insert(0) += 1;
        *self.persistent_savepoint_references.entry(id).or_insert(0) += 1;
    }

    fn dereference_persistent_savepoint(&mut self, id: TransactionId) {
        self.decrement_reference_count(id);
        let count = self.persistent_savepoint_references.get_mut(&id).unwrap();
        *count -= 1;
        if *count == 0 {
            self.persistent_savepoint_references.remove(&id);
        }
    }

    // Hands the read transaction's reference over to the savepoint made from it, releasing the
    // byte with it: from here on the savepoint is protected the way a synced one is
    fn convert_reference_to_persistent_savepoint(
        &mut self,
        mem: &TransactionalMemory,
        id: TransactionId,
    ) {
        *self.persistent_savepoint_references.entry(id).or_insert(0) += 1;
        #[cfg(feature = "experimental-multiprocess")]
        if self.active_transaction_lock_references(id) == 0 {
            let _ = mem.unlock_mp_transaction(id);
        }
        #[cfg(not(feature = "experimental-multiprocess"))]
        let _ = mem;
    }

    fn decrement_reference_count(&mut self, id: TransactionId) {
        let count = self.live_read_transactions.get_mut(&id).unwrap();
        *count -= 1;
        if *count == 0 {
            self.live_read_transactions.remove(&id);
        }
    }
}

pub(crate) struct TransactionTracker {
    state: Mutex<State>,
    live_write_transaction_available: Condvar,
}

impl TransactionTracker {
    pub(crate) fn new(next_transaction_id: TransactionId) -> Self {
        Self {
            state: Mutex::new(State {
                next_savepoint_id: SavepointId(0),
                live_read_transactions: BTreeMap::default(),
                persistent_savepoint_references: BTreeMap::default(),
                next_transaction_id,
                write_slot: WriteSlotState::Free,
                valid_savepoints: BTreeMap::default(),
                persistent_savepoints: BTreeSet::default(),
                pending_non_durable_commits: BTreeMap::default(),
                unprocessed_freed_non_durable_commits: BTreeSet::default(),
                deferred_close: None,
            }),
            live_write_transaction_available: Condvar::new(),
        }
    }

    // Takes the write slot, waiting for the write transaction that holds it to end. The
    // transaction's id is issued separately, by `issue_write_transaction_id()`, once the file's
    // latest commit is known.
    pub(crate) fn take_write_slot(&self) {
        let mut state = self.state.lock().unwrap();
        while state.write_slot != WriteSlotState::Free {
            state = self.live_write_transaction_available.wait(state).unwrap();
        }
        state.write_slot = WriteSlotState::Taken;
    }

    // Issues the slot's transaction an id that follows `last_committed`, the id of the file's
    // latest commit. `writer_lock` proves that the caller holds the writer byte, so that no
    // other process commits between reading that id and issuing this one.
    pub(crate) fn issue_write_transaction_id(
        &self,
        last_committed: TransactionId,
        #[cfg(feature = "experimental-multiprocess")] _writer_lock: &WriterLock,
    ) -> TransactionId {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Taken);
        state.next_transaction_id = state.next_transaction_id.max(last_committed);
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        debug!("Beginning write transaction id={transaction_id:?}");
        state.write_slot = WriteSlotState::Live(transaction_id);

        transaction_id
    }

    // Whether a write transaction holds the write slot in this process
    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn write_transaction_live(&self) -> bool {
        self.state.lock().unwrap().write_slot != WriteSlotState::Free
    }

    // Returns the deferred close, if the Database was dropped while this transaction was live.
    // The caller must close the database, now that the write transaction has ended
    pub(crate) fn end_write_transaction(&self) -> Option<Arc<TransactionalMemory>> {
        let mut state = self.state.lock().unwrap();
        assert_ne!(state.write_slot, WriteSlotState::Free);
        state.write_slot = WriteSlotState::Free;
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
        if state.write_slot != WriteSlotState::Free {
            state.deferred_close = Some(mem.clone());
            true
        } else {
            false
        }
    }

    pub(crate) fn clear_pending_non_durable_commits(&self, memory: &TransactionalMemory) {
        let mut state = self.state.lock().unwrap();
        let ids = mem::take(&mut state.pending_non_durable_commits);
        for (_, durable_ancestor) in ids {
            state.remove_active_transaction_lock_reference(memory, durable_ancestor);
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
        mem: &TransactionalMemory,
        id: TransactionId,
        durable_ancestor: TransactionId,
        has_unprocessed_freed_pages: bool,
    ) -> Result {
        #[cfg(feature = "experimental-multiprocess")]
        let header = mem.lock_header_shared()?;
        let mut state = self.state.lock().unwrap();
        state.add_active_transaction_lock_reference(
            mem,
            durable_ancestor,
            #[cfg(feature = "experimental-multiprocess")]
            &header,
        )?;
        assert!(
            state
                .pending_non_durable_commits
                .insert(id, durable_ancestor)
                .is_none()
        );
        if has_unprocessed_freed_pages {
            state.unprocessed_freed_non_durable_commits.insert(id);
        }

        Ok(())
    }

    // Reserve a transaction id that was created without starting a new write transaction.
    // The caller must still register the resulting root if it is a non-durable commit.
    pub(crate) fn reserve_transaction_id(
        &self,
        id: TransactionId,
        live_write_transaction: TransactionId,
    ) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.write_slot,
            WriteSlotState::Live(live_write_transaction)
        );
        assert_eq!(id, state.next_transaction_id.next());
        state.next_transaction_id = id;
    }

    // Reserves a repair commit's id so it is never issued again: crash recovery orders the
    // commit slots by transaction id, so an id must never be committed twice
    pub(crate) fn reserve_repair_transaction_id(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.write_slot, WriteSlotState::Free);
        state.next_transaction_id = state.next_transaction_id.max(id);
    }

    // Continues savepoint ids from `next_savepoint` where that is past the ones issued here:
    // another process may have created the file's persistent savepoints
    pub(crate) fn restore_savepoint_counter_state(&self, next_savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        state.next_savepoint_id = state.next_savepoint_id.max(next_savepoint);
    }

    // Sync the file's persistent savepoints. `current` must be the current set of persistent savepoints.
    pub(crate) fn sync_persistent_savepoints(
        &self,
        current: &BTreeMap<SavepointId, TransactionId>,
    ) -> Result {
        let mut state = self.state.lock().unwrap();
        let gone: Vec<SavepointId> = state
            .persistent_savepoints
            .iter()
            .filter(|id| !current.contains_key(id))
            .copied()
            .collect();
        for id in gone {
            state.persistent_savepoints.remove(&id);
            let transaction = state.valid_savepoints.remove(&id).unwrap();
            state.dereference_persistent_savepoint(transaction);
        }
        for (&id, &transaction) in current {
            if state.valid_savepoints.contains_key(&id) {
                continue;
            }
            state.reference_persistent_savepoint(transaction);
            assert!(state.valid_savepoints.insert(id, transaction).is_none());
            state.persistent_savepoints.insert(id);
        }

        Ok(())
    }

    // Marks an already-registered savepoint as persistent, which hands it the reference held by
    // the read transaction it was created from
    pub(crate) fn convert_savepoint_to_persistent(
        &self,
        mem: &TransactionalMemory,
        id: SavepointId,
    ) {
        let mut state = self.state.lock().unwrap();
        let transaction = *state.valid_savepoints.get(&id).unwrap();
        state.persistent_savepoints.insert(id);
        state.convert_reference_to_persistent_savepoint(mem, transaction);
    }

    pub(crate) fn register_read_transaction(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<TransactionId> {
        #[cfg(feature = "experimental-multiprocess")]
        let header = mem.lock_header_shared()?;
        let id = mem.latest_committed_transaction_id(
            #[cfg(feature = "experimental-multiprocess")]
            &header,
        )?;
        let mut state = self.state.lock()?;
        state.add_active_transaction_lock_reference(
            mem,
            id,
            #[cfg(feature = "experimental-multiprocess")]
            &header,
        )?;

        Ok(id)
    }

    pub(crate) fn deallocate_read_transaction(&self, mem: &TransactionalMemory, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        state.remove_active_transaction_lock_reference(mem, id);
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

    // Forgets the savepoint, leaving the transaction's reference to whoever owns it
    pub(crate) fn remove_savepoint_registration(&self, savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        state.valid_savepoints.remove(&savepoint);
        state.persistent_savepoints.remove(&savepoint);
    }

    // Deallocates the given persistent savepoint and its matching reference on the transaction
    pub(crate) fn deallocate_persistent_savepoint(
        &self,
        savepoint: SavepointId,
        transaction: TransactionId,
    ) {
        self.remove_savepoint_registration(savepoint);
        self.state
            .lock()
            .unwrap()
            .dereference_persistent_savepoint(transaction);
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
    // released by some other means: ephemeral `Savepoint::drop` or `deallocate_persistent_savepoint`
    // (called via `delete_persistent_savepoint`) do that for their respective savepoint kinds.
    //
    // Savepoints that have already been removed (for example, by `deallocate_persistent_savepoint` earlier
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

    pub(crate) fn oldest_local_referenced_transaction(&self) -> Option<TransactionId> {
        self.state
            .lock()
            .unwrap()
            .live_read_transactions
            .keys()
            .next()
            .copied()
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

    // The tracker takes the "active transaction byte" through this, so it needs somewhere to
    // take it. Opened SingleProcess, where that is a no-op
    fn memory() -> TransactionalMemory {
        use crate::tree_store::{InMemoryBackend, LocklessBackend, PAGE_SIZE};

        let (mem, _writer_lock) = TransactionalMemory::new(
            LocklessBackend::boxed(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::SingleProcess,
        )
        .unwrap();

        mem
    }

    #[test]
    fn non_durable_commit_without_freed_pages_is_not_unprocessed() {
        let mem = memory();
        let tracker = TransactionTracker::new(TransactionId::new(0));

        tracker
            .register_non_durable_commit(&mem, TransactionId::new(1), TransactionId::new(0), false)
            .unwrap();
        assert_eq!(None, tracker.oldest_unprocessed_non_durable_commit());

        tracker
            .register_non_durable_commit(&mem, TransactionId::new(2), TransactionId::new(0), true)
            .unwrap();
        assert_eq!(
            Some(TransactionId::new(2)),
            tracker.oldest_unprocessed_non_durable_commit()
        );
    }
}
