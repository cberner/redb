#![deny(clippy::all, clippy::pedantic, clippy::disallowed_methods)]
#![allow(
    clippy::if_not_else,
    clippy::iter_not_returning_iterator,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::unreadable_literal
)]

//! # redb
//!
//! A simple, portable, high-performance, ACID, embedded key-value store.
//!
//! redb is written in pure Rust and is loosely inspired by [lmdb][lmdb]. Data is stored in a collection
//! of copy-on-write B+trees. For more details, see the [design doc][design].
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
//! use redb::{Database, Error, ReadableDatabase, ReadableTable, TableDefinition};
//!
//! const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");
//!
//! fn main() -> Result<(), Error> {
//!   # #[cfg(not(target_os = "wasi"))]
//!     let file = tempfile::NamedTempFile::new().unwrap();
//!   # #[cfg(target_os = "wasi")]
//!   # let file = tempfile::NamedTempFile::new_in("/tmp").unwrap();
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
//!     assert_eq!(table.get_owned("my_key")?.unwrap().value(), 123);
//!
//!     Ok(())
//! }
//! ```
//!
//! [lmdb]: https://www.lmdb.tech/doc/
//! [design]: https://github.com/cberner/redb/blob/master/docs/design.md

// `redb_no_std` is set by build.rs when the redb 5 API preview is enabled and "std" is not. It is
// not a complete build mode yet, so it still fails loudly rather than silently producing a build
// that links against the standard library.
#[cfg(redb_no_std)]
compile_error!(
    "redb requires the standard library: no_std is not supported yet. Enable the \"std\" feature; \
     it is on by default, so add features = [\"std\"] if you set default-features = false."
);

// Everything redb needs from the standard library that is not core is imported through `alloc`, so
// that the crate can eventually be built without std. `alloc` is a subset of `std`, so this is a
// no-op for std builds.
extern crate alloc;

#[cfg(not(redb_no_std))]
pub use db::ReadOnlyDatabase;
pub use db::{
    Builder, CacheStats, Database, MultimapTableDefinition, MultimapTableHandle, ReadableDatabase,
    RepairSession, StorageBackend, TableDefinition, TableHandle, UntypedMultimapTableHandle,
    UntypedTableHandle,
};
pub use error::{
    CommitError, CompactionError, DatabaseError, Error, SavepointError, SetDurabilityError,
    StorageError, TableError, TransactionError,
};
#[cfg(feature = "experimental-api-5")]
pub use key_range::KeyRange;
#[cfg(all(feature = "experimental-multiprocess", not(redb_no_std)))]
pub use multi_process::{MultiProcessBuilder, MultiProcessDatabase};
#[cfg(feature = "experimental-api-5")]
pub use multimap_table::MultimapCursor;
pub use multimap_table::{
    MultimapRange, MultimapTable, MultimapValue, OwnedMultimapRange, OwnedMultimapValue,
    ReadOnlyMultimapTable, ReadOnlyUntypedMultimapTable, ReadableMultimapTable,
};
#[cfg(feature = "experimental-api-5")]
pub use table::Cursor;
#[cfg(feature = "experimental_cursor")]
pub use table::CursorMut;
pub use table::{
    Entry, ExtractIf, OccupiedEntry, OwnedAccessGuard, OwnedRange, Range, ReadOnlyTable,
    ReadOnlyUntypedTable, ReadableTable, ReadableTableMetadata, Table, TableStats, VacantEntry,
};
pub use transactions::{DatabaseStats, Durability, ReadTransaction, WriteTransaction};
pub use tree_store::{AccessGuard, AccessGuardMut, AccessGuardMutInPlace, Savepoint};
pub use types::{Key, MutInPlaceValue, TypeName, Value};

pub type Result<T = (), E = StorageError> = core::result::Result<T, E>;

pub mod backends;
mod complex_types;
mod db;
mod error;
// Public only where it is needed: without std a backend author has to be able to name these types
// to implement `StorageBackend`. With std they are re-exports of `std::io`, which the caller
// already has, so the module stays private and redb adds no public surface.
#[cfg(redb_no_std)]
pub mod io;
#[cfg(not(redb_no_std))]
mod io;
#[cfg(feature = "experimental-api-5")]
mod key_range;
// Needs std::fs and std::path, like the rest of the file-backed API
#[cfg(all(feature = "experimental-multiprocess", not(redb_no_std)))]
mod multi_process;
mod multimap_table;
mod sealed;
mod sync;
mod table;
mod transaction_tracker;
mod transactions;
mod tree_store;
mod tuple_types;
mod types;

// Whether the current thread is unwinding from a panic. redb's Drop impls use it to soften
// assertions that would otherwise turn a panic into an abort. A no_std program aborts on panic
// rather than unwinding, so its Drop impls never run while panicking.
#[cfg(not(redb_no_std))]
pub(crate) fn panicking() -> bool {
    std::thread::panicking()
}

#[cfg(redb_no_std)]
pub(crate) fn panicking() -> bool {
    false
}

#[cfg(test)]
fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}
