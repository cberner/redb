use crate::types::{Key, TypeName, Value};
use core::cmp::Ordering;
use core::fmt;

// ---------------------------------------------------------------------------
// PostingKey — composite key for the IVF posting list table
// ---------------------------------------------------------------------------

/// Composite key `(cluster_id, vector_id)` for the IVF posting list.
///
/// Serialized as **big-endian** so that B-tree byte ordering matches the
/// logical `(cluster_id, vector_id)` ordering. This makes range scans over a
/// single cluster a contiguous B-tree region.
///
/// Fixed width: 12 bytes (4 + 8).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PostingKey {
    pub cluster_id: u32,
    pub vector_id: u64,
}

impl PostingKey {
    pub const SERIALIZED_SIZE: usize = 12;

    pub const fn new(cluster_id: u32, vector_id: u64) -> Self {
        Self {
            cluster_id,
            vector_id,
        }
    }

    /// Returns the first possible key for the given cluster (inclusive lower bound).
    pub const fn cluster_start(cluster_id: u32) -> Self {
        Self {
            cluster_id,
            vector_id: 0,
        }
    }

    /// Returns the last possible key for the given cluster (inclusive upper bound).
    pub const fn cluster_end(cluster_id: u32) -> Self {
        Self {
            cluster_id,
            vector_id: u64::MAX,
        }
    }

    #[allow(clippy::big_endian_bytes)]
    pub fn to_be_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[..4].copy_from_slice(&self.cluster_id.to_be_bytes());
        buf[4..12].copy_from_slice(&self.vector_id.to_be_bytes());
        buf
    }

    #[allow(clippy::big_endian_bytes)]
    pub fn from_be_bytes(data: &[u8]) -> Self {
        let cluster_id = u32::from_be_bytes(data[..4].try_into().unwrap());
        let vector_id = u64::from_be_bytes(data[4..12].try_into().unwrap());
        Self {
            cluster_id,
            vector_id,
        }
    }
}

impl PartialOrd for PostingKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostingKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cluster_id
            .cmp(&other.cluster_id)
            .then(self.vector_id.cmp(&other.vector_id))
    }
}

impl fmt::Debug for PostingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PostingKey(cluster={}, vec={})",
            self.cluster_id, self.vector_id
        )
    }
}

impl Value for PostingKey {
    type SelfType<'a>
        = PostingKey
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; PostingKey::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_be_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_be_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("shodh_redb::ivfpq::PostingKey")
    }
}

impl Key for PostingKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // Big-endian serialization means raw byte comparison is correct.
        data1[..Self::SERIALIZED_SIZE].cmp(&data2[..Self::SERIALIZED_SIZE])
    }
}

// ---------------------------------------------------------------------------
// AssignmentValue — u32 cluster_id stored as a value in the assignments table
// ---------------------------------------------------------------------------

/// Thin wrapper so we can use `u32` cluster IDs as values in the assignments
/// table without conflicting with the built-in `u32` Value impl (which uses
/// little-endian). We also use little-endian here for consistency as a value.
pub struct AssignmentValue;

impl fmt::Debug for AssignmentValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AssignmentValue")
    }
}

impl Value for AssignmentValue {
    type SelfType<'a>
        = u32
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 4]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(4)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        u32::from_le_bytes(data[..4].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("shodh_redb::ivfpq::AssignmentValue")
    }
}

// ---------------------------------------------------------------------------
// IndexConfigValue — fixed-width serialisation of IndexConfig for the meta table
// ---------------------------------------------------------------------------

/// Serialised index configuration stored in the metadata table.
///
/// Layout (48 bytes, all little-endian):
/// ```text
/// [dim:4][num_clusters:4][num_subvectors:4][num_codewords:2]
/// [metric:1][store_raw:1][default_nprobe:4][state:1][_pad:3]
/// [num_vectors:8][_reserved:16]
/// ```
pub const INDEX_CONFIG_SIZE: usize = 48;

/// Encode an [`super::config::IndexConfig`] into a fixed-width byte array.
pub fn encode_index_config(cfg: &super::config::IndexConfig) -> [u8; INDEX_CONFIG_SIZE] {
    let mut buf = [0u8; INDEX_CONFIG_SIZE];
    buf[0..4].copy_from_slice(&cfg.dim.to_le_bytes());
    buf[4..8].copy_from_slice(&cfg.num_clusters.to_le_bytes());
    buf[8..12].copy_from_slice(&cfg.num_subvectors.to_le_bytes());
    buf[12..14].copy_from_slice(&cfg.num_codewords.to_le_bytes());
    buf[14] = cfg.metric_byte();
    buf[15] = u8::from(cfg.store_raw_vectors);
    buf[16..20].copy_from_slice(&cfg.default_nprobe.to_le_bytes());
    buf[20] = cfg.state;
    // bytes 21..24 padding
    buf[24..32].copy_from_slice(&cfg.num_vectors.to_le_bytes());
    // bytes 32..48 reserved
    buf
}

/// Decode an [`super::config::IndexConfig`] from a fixed-width byte slice.
pub fn decode_index_config(data: &[u8]) -> super::config::IndexConfig {
    use super::config::IndexConfig;
    use crate::vector_ops::DistanceMetric;

    let dim = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let num_clusters = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let num_subvectors = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let num_codewords = u16::from_le_bytes(data[12..14].try_into().unwrap());
    let metric = match data[14] {
        0 => DistanceMetric::Cosine,
        2 => DistanceMetric::DotProduct,
        3 => DistanceMetric::Manhattan,
        // 1 and any unknown value default to EuclideanSq
        _ => DistanceMetric::EuclideanSq,
    };
    let store_raw_vectors = data[15] != 0;
    let default_nprobe = u32::from_le_bytes(data[16..20].try_into().unwrap());
    let state = data[20];
    let num_vectors = u64::from_le_bytes(data[24..32].try_into().unwrap());

    IndexConfig {
        dim,
        num_clusters,
        num_subvectors,
        num_codewords,
        metric,
        store_raw_vectors,
        default_nprobe,
        state,
        num_vectors,
    }
}
