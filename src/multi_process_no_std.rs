//! Stands in for [`crate::multi_process`] where there is no file system to coordinate through.
//!
//! A multi-process database is a directory of files and locks, so `no_std` has none. The tracker
//! and the database still carry an `Option<Arc<ProcessCoordinator>>` there, always `None`, rather
//! than putting a `cfg` on the field and on every path that consults it.

use crate::Result;
use crate::transaction_tracker::TransactionId;

/// Uninhabited: a `no_std` build never constructs one, so the methods below are unreachable and
/// exist only so that the paths which consult a coordinator type-check.
pub(crate) enum ProcessCoordinator {}

impl ProcessCoordinator {
    pub(crate) fn shares_write_lock(&self) -> bool {
        match *self {}
    }

    pub(crate) fn allows_non_durable_commit(&self) -> bool {
        match *self {}
    }

    pub(crate) fn begin_read(
        &self,
        _local_write_transaction_live: bool,
        _local_oldest: Option<TransactionId>,
    ) -> Result<TransactionId> {
        match *self {}
    }

    pub(crate) fn publish_pinned(&self, _local_oldest: Option<TransactionId>) -> Result<()> {
        match *self {}
    }

    pub(crate) fn oldest_pinned_globally(
        &self,
        _local_oldest: Option<TransactionId>,
    ) -> Result<Option<TransactionId>> {
        match *self {}
    }

    pub(crate) fn publish_reclaim_horizon(&self, _horizon: TransactionId) -> Result<()> {
        match *self {}
    }

    pub(crate) fn publish_durable_commit(&self, _last_committed: TransactionId) -> Result<()> {
        match *self {}
    }

    pub(crate) fn begin_write(&self) -> Result<TransactionId> {
        match *self {}
    }

    pub(crate) fn end_write(&self) {
        match *self {}
    }
}
