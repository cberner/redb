use crate::tree_store::TransactionalMemory;
use crate::{Key, Result, Savepoint, TypeName, Value};
#[cfg(feature = "logging")]
use log::info;
use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;
use std::mem::size_of;
use std::sync::{Condvar, Mutex};

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct TransactionId(u64);

impl TransactionId {
    pub(crate) fn new(value: u64) -> TransactionId {
        Self(value)
    }

    pub(crate) fn raw_id(&self) -> u64 {
        self.0
    }

    pub(crate) fn next(&self) -> TransactionId {
        TransactionId(self.0 + 1)
    }

    pub(crate) fn increment(&mut self) -> TransactionId {
        let next = self.next();
        *self = next;
        next
    }

    pub(crate) fn parent(&self) -> Option<TransactionId> {
        if self.0 == 0 {
            None
        } else {
            Some(TransactionId(self.0 - 1))
        }
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SavepointId(pub u64);

impl SavepointId {
    pub(crate) fn next(&self) -> SavepointId {
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
        Self: 'a,
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
    valid_savepoints: BTreeSet<SavepointId>,
    // Non-durable commits that are still in-memory, and waiting for a durable commit to get flushed
    // We need to make sure that the freed-table does not get processed for these, since they are not durable yet
    // Therefore, we hold a read transaction on their parent
    pending_non_durable_commits: Vec<TransactionId>,
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
                live_read_transactions: Default::default(),
                next_transaction_id,
                live_write_transaction: None,
                valid_savepoints: Default::default(),
                pending_non_durable_commits: Default::default(),
            }),
            live_write_transaction_available: Condvar::new(),
        }
    }

    pub(crate) fn start_write_transaction(&self) -> TransactionId {
        let mut state = self.state.lock().unwrap();
        while state.live_write_transaction.is_some() {
            state = self.live_write_transaction_available.wait(state).unwrap();
        }
        assert!(state.live_write_transaction.is_none());
        let transaction_id = state.next_transaction_id.increment();
        #[cfg(feature = "logging")]
        info!("Beginning write transaction id={:?}", transaction_id);
        state.live_write_transaction = Some(transaction_id);

        transaction_id
    }

    pub(crate) fn end_write_transaction(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        assert_eq!(state.live_write_transaction.unwrap(), id);
        state.live_write_transaction = None;
        self.live_write_transaction_available.notify_one();
    }

    pub(crate) fn clear_pending_non_durable_commits(&self) {
        let mut state = self.state.lock().unwrap();
        let ids: Vec<TransactionId> = state.pending_non_durable_commits.drain(..).collect();
        for id in ids {
            if let Some(parent) = id.parent() {
                let ref_count = state.live_read_transactions.get_mut(&parent).unwrap();
                *ref_count -= 1;
                if *ref_count == 0 {
                    state.live_read_transactions.remove(&parent);
                }
            }
        }
    }

    pub(crate) fn register_non_durable_commit(&self, id: TransactionId) {
        let mut state = self.state.lock().unwrap();
        if let Some(parent) = id.parent() {
            state
                .live_read_transactions
                .entry(parent)
                .and_modify(|x| *x += 1)
                .or_insert(1);
        }
        state.pending_non_durable_commits.push(id);
    }

    pub(crate) fn restore_savepoint_counter_state(&self, next_savepoint: SavepointId) {
        let mut state = self.state.lock().unwrap();
        assert!(state.valid_savepoints.is_empty());
        state.next_savepoint_id = next_savepoint;
    }

    pub(crate) fn register_persistent_savepoint(&self, savepoint: &Savepoint) {
        let mut state = self.state.lock().unwrap();
        state
            .live_read_transactions
            .entry(savepoint.get_transaction_id())
            .and_modify(|x| *x += 1)
            .or_insert(1);
        state.valid_savepoints.insert(savepoint.get_id());
    }

    pub(crate) fn register_read_transaction(
        &self,
        mem: &TransactionalMemory,
    ) -> Result<TransactionId> {
        let mut state = self.state.lock().unwrap();
        let id = mem.get_last_committed_transaction_id()?;
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
        }
    }

    pub(crate) fn any_savepoint_exists(&self) -> bool {
        !self.state.lock().unwrap().valid_savepoints.is_empty()
    }

    pub(crate) fn allocate_savepoint(&self) -> SavepointId {
        let mut state = self.state.lock().unwrap();
        let id = state.next_savepoint_id.next();
        state.next_savepoint_id = id;
        state.valid_savepoints.insert(id);
        id
    }

    // Deallocates the given savepoint and its matching reference count on the transcation
    pub(crate) fn deallocate_savepoint(&self, savepoint: SavepointId, transaction: TransactionId) {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .remove(&savepoint);
        self.deallocate_read_transaction(transaction);
    }

    pub(crate) fn is_valid_savepoint(&self, id: SavepointId) -> bool {
        self.state.lock().unwrap().valid_savepoints.contains(&id)
    }

    pub(crate) fn invalidate_savepoints_after(&self, id: SavepointId) {
        self.state
            .lock()
            .unwrap()
            .valid_savepoints
            .retain(|x| *x <= id);
    }

    pub(crate) fn oldest_live_read_transaction(&self) -> Option<TransactionId> {
        self.state
            .lock()
            .unwrap()
            .live_read_transactions
            .keys()
            .next()
            .cloned()
    }
}
