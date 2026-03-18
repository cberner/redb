use crate::Error;
use crate::error::StorageError;
use crate::transactions::WriteTransaction;
use std::fmt::{Display, Formatter};
use std::sync::mpsc;
use std::sync::{Condvar, Mutex};

/// Error from a group commit operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum GroupCommitError {
    /// This batch's operations caused an error.
    BatchFailed(Error),
    /// This batch was rolled back because another batch in the group failed.
    /// The caller may retry by resubmitting.
    PeerFailed,
    /// The write transaction could not be acquired.
    TransactionFailed(StorageError),
    /// The commit itself failed (fsync error, etc.).
    CommitFailed(StorageError),
    /// The database is shutting down.
    Shutdown,
}

impl Display for GroupCommitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BatchFailed(e) => write!(f, "Batch operation failed: {e}"),
            Self::PeerFailed => write!(f, "Rolled back: another batch in the group failed"),
            Self::TransactionFailed(e) => write!(f, "Transaction acquisition failed: {e}"),
            Self::CommitFailed(e) => write!(f, "Commit failed: {e}"),
            Self::Shutdown => write!(f, "Database is shutting down"),
        }
    }
}

impl std::error::Error for GroupCommitError {}

type BatchFn =
    Box<dyn FnOnce(&WriteTransaction) -> std::result::Result<(), Error> + Send + 'static>;

/// A batch of write operations submitted to group commit.
///
/// Create a batch from a closure that receives a `&WriteTransaction` and performs
/// mutations (open tables, insert, remove, etc.). The group committer manages the
/// transaction lifecycle -- do not call `commit()` or `abort()` within the closure.
///
/// # Example
///
/// ```ignore
/// use redb::{TableDefinition, WriteBatch};
///
/// const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");
///
/// let batch = WriteBatch::new(|txn| {
///     let mut table = txn.open_table(TABLE)?;
///     table.insert("key", &42)?;
///     Ok(())
/// });
/// db.submit_write_batch(batch)?;
/// ```
pub struct WriteBatch {
    operations: BatchFn,
}

impl WriteBatch {
    /// Create a batch from a closure that receives a shared `&WriteTransaction`.
    ///
    /// The closure should open tables, insert/remove entries, etc.
    /// Do not call `commit()` or `abort()` -- the group committer manages the lifecycle.
    pub fn new<F>(f: F) -> Self
    where
        F: FnOnce(&WriteTransaction) -> std::result::Result<(), Error> + Send + 'static,
    {
        Self {
            operations: Box::new(f),
        }
    }

    pub(crate) fn apply(self, txn: &WriteTransaction) -> std::result::Result<(), Error> {
        (self.operations)(txn)
    }
}

pub(crate) struct PendingBatch {
    pub batch: WriteBatch,
    pub result_tx: mpsc::SyncSender<Result<(), GroupCommitError>>,
}

struct GroupCommitState {
    pending: Vec<PendingBatch>,
    active_leader: bool,
    shutdown: bool,
}

pub(crate) struct GroupCommitter {
    state: Mutex<GroupCommitState>,
    leader_done: Condvar,
}

impl GroupCommitter {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(GroupCommitState {
                pending: Vec::new(),
                active_leader: false,
                shutdown: false,
            }),
            leader_done: Condvar::new(),
        }
    }

    /// Enqueue a batch and determine whether this thread should become the leader.
    /// Returns `(should_lead, result_rx)`.
    pub fn enqueue(
        &self,
        batch: WriteBatch,
    ) -> Result<(bool, mpsc::Receiver<Result<(), GroupCommitError>>), GroupCommitError> {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return Err(GroupCommitError::Shutdown);
        }
        let should_lead = !state.active_leader;
        if should_lead {
            state.active_leader = true;
        }
        state.pending.push(PendingBatch { batch, result_tx });
        Ok((should_lead, result_rx))
    }

    /// Drain all pending batches. Called by the leader.
    pub fn drain_pending(&self) -> Vec<PendingBatch> {
        let mut state = self.state.lock().unwrap();
        std::mem::take(&mut state.pending)
    }

    /// Signal that the leader has finished. Wakes any threads waiting
    /// for the next leader election.
    pub fn finish_leader(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_leader = false;
        self.leader_done.notify_all();
    }

    /// Shut down the group committer, failing all pending batches.
    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        state.shutdown = true;
        let pending = std::mem::take(&mut state.pending);
        drop(state);
        for p in pending {
            let _ = p.result_tx.send(Err(GroupCommitError::Shutdown));
        }
        self.leader_done.notify_all();
    }
}
