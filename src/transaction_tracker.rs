use crate::Savepoint;
use std::collections::btree_map::BTreeMap;
use std::collections::btree_set::BTreeSet;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct TransactionId(pub u64);

impl TransactionId {
    pub(crate) fn next(&self) -> TransactionId {
        TransactionId(self.0 + 1)
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct SavepointId(pub u64);

impl SavepointId {
    fn next(&self) -> SavepointId {
        SavepointId(self.0 + 1)
    }
}

pub(crate) struct TransactionTracker {
    next_savepoint_id: SavepointId,
    // reference count of read transactions per transaction id
    live_read_transactions: BTreeMap<TransactionId, u64>,
    valid_savepoints: BTreeSet<SavepointId>,
}

impl TransactionTracker {
    pub(crate) fn new() -> Self {
        Self {
            next_savepoint_id: SavepointId(0),
            live_read_transactions: Default::default(),
            valid_savepoints: Default::default(),
        }
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

    pub(crate) fn deallocate_savepoint(&mut self, savepoint: &Savepoint) {
        self.valid_savepoints.remove(&savepoint.get_id());
        self.deallocate_read_transaction(savepoint.get_transaction_id());
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
