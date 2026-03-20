//! IVF-PQ vector index — disk-first approximate nearest neighbor search.
//!
//! This module implements an Inverted File Index with Product Quantization
//! (IVF-PQ) built on shodh-redb's B-tree storage. It replaces brute-force
//! `nearest_k` scans with O(sqrt(n)) approximate search that scales to
//! 100M+ vectors.
//!
//! # Architecture
//!
//! - **IVF**: Partitions the vector space into clusters via k-means. At query
//!   time, only the `nprobe` nearest clusters are scanned.
//! - **PQ**: Compresses each vector from `dim * 4` bytes to `num_subvectors`
//!   bytes (one byte per sub-quantizer). Trained codebooks map each sub-vector
//!   to its nearest centroid index.
//! - **ADC**: At search time, a precomputed distance lookup table enables
//!   approximate distance computation via `num_subvectors` table lookups per
//!   candidate.
//!
//! All index data is stored in regular B-tree tables with prefixed names,
//! fully ACID and crash-safe.
//!
//! # Example
//!
//! ```rust,ignore
//! use shodh_redb::{Database, DistanceMetric, IvfPqIndexDefinition, SearchParams};
//!
//! const INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
//!     "embeddings", 384, 256, 48, DistanceMetric::EuclideanSq,
//! ).with_raw_vectors().with_nprobe(16);
//!
//! let db = Database::create("vectors.redb")?;
//!
//! // Train + insert
//! let write_txn = db.begin_write()?;
//! let mut idx = write_txn.open_ivfpq_index(&INDEX)?;
//! idx.train(training_data.into_iter(), 25)?;
//! idx.insert(1, &embedding)?;
//! write_txn.commit()?;
//!
//! // Search
//! let read_txn = db.begin_read()?;
//! let idx = read_txn.open_ivfpq_index(&INDEX)?;
//! let results = idx.search(&read_txn, &query, &SearchParams::top_k(10))?;
//! ```

pub mod adc;
pub mod config;
pub mod index;
pub mod kmeans;
pub mod pq;
pub mod types;

pub use config::{IvfPqIndexDefinition, IndexConfig, SearchParams};
pub use index::{IvfPqIndex, ReadOnlyIvfPqIndex};
pub use pq::Codebooks;
pub use types::PostingKey;
