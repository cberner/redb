#![allow(clippy::drop_non_drop)]
#![deny(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::disallowed_methods
)]
// TODO remove this once wasi no longer requires nightly
#![cfg_attr(target_os = "wasi", feature(wasi_ext))]

pub use db::{
    Builder, Database, MultimapTableDefinition, MultimapTableHandle, TableDefinition, TableHandle,
    UntypedMultimapTableHandle, UntypedTableHandle,
};
pub use error::{
    CommitError, CompactionError, DatabaseError, Error, SavepointError, StorageError, TableError,
    TransactionError,
};
pub use multimap_table::{
    MultimapRange, MultimapTable, MultimapValue, ReadOnlyMultimapTable, ReadableMultimapTable,
};
pub use table::{Drain, DrainFilter, Range, ReadOnlyTable, ReadableTable, Table};
pub use transactions::{DatabaseStats, Durability, ReadTransaction, WriteTransaction};
pub use tree_store::{AccessGuard, AccessGuardMut, Savepoint};
pub use types::{RedbKey, RedbValue, TypeName};

type Result<T = (), E = StorageError> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub use crate::python::redb;

mod db;
mod error;
mod multimap_table;
#[cfg(feature = "python")]
mod python;
mod sealed;
mod table;
mod transaction_tracker;
mod transactions;
mod tree_store;
mod tuple_types;
mod types;

#[cfg(test)]
fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}
