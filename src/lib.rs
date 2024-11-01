#![allow(
    clippy::drop_non_drop,
    clippy::default_trait_access,
    clippy::if_not_else,
    clippy::inline_always,
    clippy::iter_not_returning_iterator,
    clippy::let_underscore_drop,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::option_option
)]
#![deny(
    clippy::all,
    clippy::doc_markdown,
    clippy::explicit_iter_loop,
    clippy::cast_lossless,
    clippy::checked_conversions,
    clippy::cloned_instead_of_copied,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::disallowed_methods,
    clippy::map_unwrap_or,
    clippy::match_wildcard_for_single_variants,
    clippy::range_plus_one,
    clippy::type_repetition_in_bounds,
    clippy::uninlined_format_args
)]
// TODO remove this once wasi no longer requires nightly
#![cfg_attr(target_os = "wasi", feature(wasi_ext))]

//! # redb
//!
//! A simple, portable, high-performance, ACID, embedded key-value store.
//!
//! redb is written in pure Rust and is loosely inspired by [lmdb][lmdb]. Data is stored in a collection
//! of copy-on-write B-trees. For more details, see the [design doc][design].
//!
//! # Features
//!
//! - Zero-copy, thread-safe, `BTreeMap` based API
//! - Fully ACID-compliant transactions
//! - MVCC support for concurrent readers & writer, without blocking
//! - Crash-safe by default
//! - Savepoints and rollbacks
//!
//! # Example
//!
//! ```
//! use redb::{Database, Error, ReadableTable, TableDefinition};
//!
//! const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");
//!
//! #[cfg(not(target_os = "wasi"))]
//! fn main() -> Result<(), Error> {
//!     let file = tempfile::NamedTempFile::new().unwrap();
//!     let db = Database::create(file.path())?;
//!     let write_txn = db.begin_write()?;
//!     {
//!         let mut table = write_txn.open_table(TABLE)?;
//!         table.insert("my_key", &123)?;
//!     }
//!     write_txn.commit()?;
//!
//!     let read_txn = db.begin_read()?;
//!     let table = read_txn.open_table(TABLE)?;
//!     assert_eq!(table.get("my_key")?.unwrap().value(), 123);
//!
//!     Ok(())
//! }
//! ```
//!
//! [lmdb]: https://www.lmdb.tech/doc/
//! [design]: https://github.com/cberner/redb/blob/master/docs/design.md

pub use db::{
    Builder, Database, MultimapTableDefinition, MultimapTableHandle, RepairSession, StorageBackend,
    TableDefinition, TableHandle, UntypedMultimapTableHandle, UntypedTableHandle,
};
pub use error::{
    CommitError, CompactionError, DatabaseError, Error, SavepointError, StorageError, TableError,
    TransactionError,
};
pub use multimap_table::{
    MultimapRange, MultimapTable, MultimapValue, ReadOnlyMultimapTable,
    ReadOnlyUntypedMultimapTable, ReadableMultimapTable,
};
pub use table::{
    ExtractIf, Range, ReadOnlyTable, ReadOnlyUntypedTable, ReadableTable, ReadableTableMetadata,
    Table, TableStats,
};
pub use transactions::{DatabaseStats, Durability, ReadTransaction, WriteTransaction};
pub use tree_store::{AccessGuard, AccessGuardMut, Savepoint};
pub use types::{Key, MutInPlaceValue, TypeName, Value};

pub type Result<T = (), E = StorageError> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub use crate::python::redb;

pub mod backends;
mod complex_types;
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
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}
