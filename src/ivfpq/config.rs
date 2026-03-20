use crate::vector_ops::DistanceMetric;
use core::fmt;

// ---------------------------------------------------------------------------
// IndexConfig — persisted index configuration
// ---------------------------------------------------------------------------

/// Persisted configuration for an IVF-PQ index. Stored as a single row in the
/// metadata table and loaded at index-open time.
#[derive(Clone, PartialEq)]
pub struct IndexConfig {
    /// Vector dimensionality (e.g. 384, 768, 1536).
    pub dim: u32,
    /// Number of IVF clusters (centroids). Typical: 256–4096.
    pub num_clusters: u32,
    /// Number of PQ sub-vectors. `dim` must be divisible by this.
    /// Each sub-vector is `dim / num_subvectors` floats.
    pub num_subvectors: u32,
    /// Codewords per sub-quantizer. Always 256 (u8 codes).
    pub num_codewords: u16,
    /// Distance metric used for training, encoding, and search.
    pub metric: DistanceMetric,
    /// Whether to store full-precision vectors for re-ranking.
    pub store_raw_vectors: bool,
    /// Default number of clusters to probe at search time.
    pub default_nprobe: u32,
    /// Training state: 0 = untrained, 1 = trained.
    pub state: u8,
    /// Total number of vectors currently in the index.
    pub num_vectors: u64,
}

/// Training state: index has not been trained yet.
pub const STATE_UNTRAINED: u8 = 0;
/// Training state: index is trained and ready for inserts/queries.
pub const STATE_TRAINED: u8 = 1;

impl IndexConfig {
    /// Returns the dimensionality of each PQ sub-vector.
    pub fn sub_dim(&self) -> usize {
        self.dim as usize / self.num_subvectors as usize
    }

    /// Returns the byte discriminant for the distance metric.
    pub fn metric_byte(&self) -> u8 {
        match self.metric {
            DistanceMetric::Cosine => 0,
            DistanceMetric::EuclideanSq => 1,
            DistanceMetric::DotProduct => 2,
            DistanceMetric::Manhattan => 3,
        }
    }
}

impl fmt::Debug for IndexConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexConfig")
            .field("dim", &self.dim)
            .field("num_clusters", &self.num_clusters)
            .field("num_subvectors", &self.num_subvectors)
            .field("num_codewords", &self.num_codewords)
            .field("metric", &self.metric)
            .field("store_raw", &self.store_raw_vectors)
            .field("nprobe", &self.default_nprobe)
            .field("state", &self.state)
            .field("num_vectors", &self.num_vectors)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// SearchParams — per-query search configuration
// ---------------------------------------------------------------------------

/// Parameters for a single IVF-PQ approximate nearest neighbor query.
#[derive(Debug, Clone)]
pub struct SearchParams {
    /// Number of IVF clusters to probe. Higher = more accurate, slower.
    /// Must be ≥ 1. Clamped to `num_clusters` at search time.
    pub nprobe: u32,
    /// Number of PQ-distance candidates to shortlist before re-ranking.
    /// Only meaningful when `rerank` is true. Defaults to `k * 10`.
    pub candidates: usize,
    /// Number of final results to return.
    pub k: usize,
    /// If true and the index stores raw vectors, re-rank the top
    /// `candidates` with exact distances for higher recall.
    pub rerank: bool,
}

impl SearchParams {
    /// Create search params for a top-k query with default settings.
    pub fn top_k(k: usize) -> Self {
        Self {
            nprobe: 10,
            candidates: k.saturating_mul(10).max(100),
            k,
            rerank: true,
        }
    }
}

// ---------------------------------------------------------------------------
// IvfPqIndexDefinition — user-facing index declaration
// ---------------------------------------------------------------------------

/// Definition for an IVF-PQ vector index.
///
/// Analogous to [`crate::TableDefinition`] — a compile-time description of an
/// index that is passed to `open_ivfpq_index()` to create or open it.
///
/// # Example
///
/// ```rust,ignore
/// use shodh_redb::{DistanceMetric, IvfPqIndexDefinition};
///
/// const INDEX: IvfPqIndexDefinition = IvfPqIndexDefinition::new(
///     "embeddings", 768, 256, 96, DistanceMetric::EuclideanSq,
/// ).with_raw_vectors().with_nprobe(16);
/// ```
pub struct IvfPqIndexDefinition {
    name: &'static str,
    dim: u32,
    num_clusters: u32,
    num_subvectors: u32,
    metric: DistanceMetric,
    store_raw_vectors: bool,
    default_nprobe: u32,
}

impl IvfPqIndexDefinition {
    /// Create a new IVF-PQ index definition.
    ///
    /// `dim` must be divisible by `num_subvectors`.
    pub const fn new(
        name: &'static str,
        dim: u32,
        num_clusters: u32,
        num_subvectors: u32,
        metric: DistanceMetric,
    ) -> Self {
        Self {
            name,
            dim,
            num_clusters,
            num_subvectors,
            metric,
            store_raw_vectors: false,
            default_nprobe: 10,
        }
    }

    /// Enable storage of full-precision vectors for re-ranking.
    #[must_use]
    pub const fn with_raw_vectors(mut self) -> Self {
        self.store_raw_vectors = true;
        self
    }

    /// Set the default number of clusters to probe at search time.
    #[must_use]
    pub const fn with_nprobe(mut self, nprobe: u32) -> Self {
        self.default_nprobe = nprobe;
        self
    }

    /// Returns the index name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the requested number of IVF clusters.
    pub const fn num_clusters(&self) -> u32 {
        self.num_clusters
    }

    /// Convert to a full [`IndexConfig`] (with state=untrained, `num_vectors`=0).
    pub fn to_config(&self) -> IndexConfig {
        IndexConfig {
            dim: self.dim,
            num_clusters: self.num_clusters,
            num_subvectors: self.num_subvectors,
            num_codewords: 256,
            metric: self.metric,
            store_raw_vectors: self.store_raw_vectors,
            default_nprobe: self.default_nprobe,
            state: STATE_UNTRAINED,
            num_vectors: 0,
        }
    }
}

impl fmt::Debug for IvfPqIndexDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IvfPqIndexDefinition({:?}, dim={}, clusters={}, subvecs={}, {:?})",
            self.name, self.dim, self.num_clusters, self.num_subvectors, self.metric,
        )
    }
}
