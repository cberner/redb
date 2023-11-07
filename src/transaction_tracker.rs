use crate::{RedbKey, RedbValue, Savepoint, TypeName};
use std::cmp::Ordering;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;
use std::mem::size_of;

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

impl RedbValue for SavepointId {
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

impl RedbKey for SavepointId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).0.cmp(&Self::from_bytes(data2).0)
    }
}

pub(crate) struct TransactionTracker {
    next_savepoint_id: SavepointId,
    // reference count of read transactions per transaction id
    live_read_transactions: BTreeMap<TransactionId, u64>,
    valid_savepoints: BTreeSet<SavepointId>,
    // Non-durable commits that are still in-memory, and waiting for a durable commit to get flushed
    // We need to make sure that the freed-table does not get processed for these, since they are not durable yet
    // Therefore, we hold a read transaction on their parent
    pending_non_durable_commits: Vec<TransactionId>,
}

impl TransactionTracker {
    pub(crate) fn new() -> Self {
        Self {
            next_savepoint_id: SavepointId(0),
            live_read_transactions: Default::default(),
            valid_savepoints: Default::default(),
            pending_non_durable_commits: Default::default(),
        }
    }

    pub(crate) fn clear_pending_non_durable_commits(&mut self) {
        for id in self.pending_non_durable_commits.drain(..) {
            if let Some(parent) = id.parent() {
                let ref_count = self.live_read_transactions.get_mut(&parent).unwrap();
                *ref_count -= 1;
                if *ref_count == 0 {
                    self.live_read_transactions.remove(&parent);
                }
            }
        }
    }

    pub(crate) fn register_non_durable_commit(&mut self, id: TransactionId) {
        if let Some(parent) = id.parent() {
            self.live_read_transactions
                .entry(parent)
                .and_modify(|x| *x += 1)
                .or_insert(1);
        }
        self.pending_non_durable_commits.push(id);
    }

    pub(crate) fn restore_savepoint_counter_state(&mut self, next_savepoint: SavepointId) {
        assert!(self.valid_savepoints.is_empty());
        self.next_savepoint_id = next_savepoint;
    }

    pub(crate) fn register_persistent_savepoint(&mut self, savepoint: &Savepoint) {
        self.register_read_transaction(savepoint.get_transaction_id());
        self.valid_savepoints.insert(savepoint.get_id());
    }

    pub(crate) fn register_read_transaction(&mut self, id: TransactionId) {
        self.live_read_transactions
            .entry(id)
            .and_modify(|x| *x += 1)
            .or_insert(1);
    }

    pub(crate) fn deallocate_read_transaction(&mut self, id: TransactionId) {
        let ref_count = self.live_read_transactions.get_mut(&id).unwrap();
        *ref_count -= 1;
        if *ref_count == 0 {
            self.live_read_transactions.remove(&id);
        }
    }

    pub(crate) fn any_savepoint_exists(&self) -> bool {
        !self.valid_savepoints.is_empty()
    }

    pub(crate) fn allocate_savepoint(&mut self) -> SavepointId {
        let id = self.next_savepoint_id.next();
        self.next_savepoint_id = id;
        self.valid_savepoints.insert(id);
        id
    }

    // Deallocates the given savepoint and its matching reference count on the transcation
    pub(crate) fn deallocate_savepoint(
        &mut self,
        savepoint: SavepointId,
        transaction: TransactionId,
    ) {
        self.valid_savepoints.remove(&savepoint);
        self.deallocate_read_transaction(transaction);
    }

    pub(crate) fn is_valid_savepoint(&self, id: SavepointId) -> bool {
        self.valid_savepoints.contains(&id)
    }

    pub(crate) fn invalidate_savepoints_after(&mut self, id: SavepointId) {
        self.valid_savepoints.retain(|x| *x <= id);
    }

    pub(crate) fn oldest_live_read_transaction(&self) -> Option<TransactionId> {
        self.live_read_transactions.keys().next().cloned()
    }
}
