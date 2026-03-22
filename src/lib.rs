#![cfg_attr(not(feature = "std"), no_std)]
#![deny(clippy::all, clippy::pedantic, clippy::disallowed_methods)]
#![allow(
    clippy::default_trait_access,
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
//! # shodh-redb
//!
//! Multi-modal embedded database for Rust - vectors, blobs, TTL, merge operators,
//! and causal tracking built on ACID B-trees.
//!
//! shodh-redb extends [redb](https://github.com/cberner/redb) with capabilities for
//! AI/ML workloads, edge computing, and multi-modal data. Data is stored in a collection
//! of copy-on-write B-trees with full ACID guarantees.
//!
//! # Core Features
//!
//! - Zero-copy, thread-safe, `BTreeMap` based API
//! - Fully ACID-compliant transactions with MVCC
//! - Crash-safe by default with savepoints and rollbacks
//! - `no_std` compatible (with `std` feature flag, enabled by default)
//!
//! # Extended Features
//!
//! - **Vector types** -- [`FixedVec`], [`DynVec`], [`BinaryQuantized`], [`ScalarQuantized`]
//!   with distance metrics ([`cosine_distance`], [`euclidean_distance_sq`], [`hamming_distance`])
//!   and top-k search ([`nearest_k`])
//! - **Blob store** -- Streaming writes, content-addressable dedup, seekable reads,
//!   causal lineage tracking, and crash-safe compaction
//! - **TTL tables** -- Per-key expiration with lazy filtering and bulk purge
//!   (requires `std` feature)
//! - **Merge operators** -- Atomic read-modify-write via [`MergeOperator`] trait
//!   with built-in [`NumericAdd`], [`NumericMax`], [`BitwiseOr`], and more
//! - **Group commit** -- Batch concurrent writes into a single fsync
//!   (requires `std` feature)
//! - **Hybrid Logical Clock** -- [`HybridLogicalClock`] for causal ordering
//!   in distributed systems
//! - **Memory budget** -- Hard RAM cap with adaptive cache sizing
//!
//! # Example
//!
//! ```
//! use shodh_redb::{Database, Error, ReadableDatabase, ReadableTable, TableDefinition};
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
//!     assert_eq!(table.get("my_key")?.unwrap().value(), 123);
//!
//!     Ok(())
//! }
//! ```

extern crate alloc;

#[cfg(feature = "std")]
pub use db::VerifyReport;
pub use db::{
    Builder, CacheStats, CorruptPageInfo, Database, MultimapTableDefinition, MultimapTableHandle,
    ReadOnlyDatabase, ReadableDatabase, RepairSession, StorageBackend, TableDefinition,
    TableHandle, UntypedMultimapTableHandle, UntypedTableHandle, VerifyLevel,
};
pub use error::{
    BackendError, CommitError, CompactionError, DatabaseError, Error, SavepointError,
    SetDurabilityError, StorageError, TableError, TransactionError,
};
#[cfg(feature = "std")]
pub use group_commit::{GroupCommitError, WriteBatch};
pub use legacy_tuple_types::Legacy;
pub use multimap_table::{
    MultimapRange, MultimapTable, MultimapValue, ReadOnlyMultimapTable,
    ReadOnlyUntypedMultimapTable, ReadableMultimapTable,
};
pub use table::{
    ExtractIf, Range, ReadOnlyTable, ReadOnlyUntypedTable, ReadableTable, ReadableTableMetadata,
    Table, TableStats,
};
pub use transactions::{DatabaseStats, Durability, ReadTransaction, WriteTransaction};
pub use tree_store::{
    AccessGuard, AccessGuardMut, AccessGuardMutInPlace, CompressionConfig, RawEntryGuard,
    RawEntryIter, Savepoint,
};
pub use types::{Key, MutInPlaceValue, TypeName, Value};

pub use cdc::{CdcConfig, ChangeOp, ChangeStream};
pub use blob_store::{
    BlobCompactionReport, BlobId, BlobInput, BlobMeta, BlobReader, BlobRef, BlobStats, BlobWriter,
    CausalEdge, CausalLink, CausalPath, ContentType, DedupStats, MAX_TAGS_PER_BLOB, RelationType,
    StoreOptions,
};
pub use composite::{CompositeQuery, ScoredBlob, SignalScores, SignalWeights};
pub use ivfpq::{
    Codebooks, IndexConfig, IvfPqIndex, IvfPqIndexDefinition, ReadOnlyIvfPqIndex, SearchParams,
};
pub use merge::{
    BitwiseOr, BytesAppend, FnMergeOperator, MergeOperator, NumericAdd, NumericMax, NumericMin,
    merge_fn,
};
pub use temporal::HybridLogicalClock;
#[cfg(feature = "std")]
pub use ttl_table::{ReadOnlyTtlTable, TtlAccessGuard, TtlRange, TtlTable, TtlTableDefinition};
pub use vector::{BinaryQuantized, DynVec, FixedVec, SQVec, ScalarQuantized};
pub use vector_ops::{
    DistanceMetric, Neighbor, cosine_distance, cosine_similarity, dequantize_scalar, dot_product,
    euclidean_distance_sq, hamming_distance, l2_norm, l2_normalize, l2_normalized,
    manhattan_distance, nearest_k, nearest_k_fixed, quantize_binary, quantize_scalar, read_f32_le,
    sq_dot_product, sq_euclidean_distance_sq, write_f32_le,
};

pub type Result<T = (), E = StorageError> = core::result::Result<T, E>;

pub mod backends;
pub mod blob_store;
pub mod cdc;
mod compat;
mod complex_types;
pub mod composite;
mod db;
pub mod error;
#[cfg(feature = "std")]
pub mod group_commit;
pub mod ivfpq;
mod legacy_tuple_types;
pub mod merge;
mod multimap_table;
mod sealed;
mod table;
pub mod temporal;
mod transaction_tracker;
mod transactions;
mod tree_store;
#[cfg(feature = "std")]
pub mod ttl_table;
mod tuple_types;
mod types;
pub mod vector;
pub mod vector_ops;

#[cfg(test)]
fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}
