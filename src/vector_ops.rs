use std::collections::BinaryHeap;
use std::fmt::{self, Debug};

use crate::vector::SQVec;

// ---------------------------------------------------------------------------
// Distance metric enum
// ---------------------------------------------------------------------------

/// Specifies the distance metric for vector similarity search.
///
/// Lower distance values indicate more similar vectors for all metrics.
///
/// # Usage
///
/// ```rust,ignore
/// use redb::{DistanceMetric, FixedVec, TableDefinition, ReadableTable};
///
/// let query = [1.0f32, 0.0, 0.0];
/// let metric = DistanceMetric::Cosine;
///
/// // Scan and rank vectors
/// for entry in table.iter()? {
///     let (key, guard) = entry?;
///     let vec = guard.value();
///     let dist = metric.compute(&query, &vec);
///     println!("{}: {}", key.value(), dist);
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DistanceMetric {
    /// Cosine distance: `1.0 - cosine_similarity(a, b)`. Range `[0.0, 2.0]`.
    Cosine,
    /// Squared Euclidean distance: `sum((a_i - b_i)^2)`. Range `[0.0, inf)`.
    EuclideanSq,
    /// Dot product distance: `-dot_product(a, b)`. Negate so lower = more similar.
    /// Use with L2-normalized vectors for equivalent cosine ranking without sqrt.
    DotProduct,
    /// Manhattan (L1) distance: `sum(|a_i - b_i|)`. Range `[0.0, inf)`.
    Manhattan,
}

impl DistanceMetric {
    /// Computes the distance between two f32 vectors using this metric.
    ///
    /// Lower values indicate more similar vectors for all metrics.
    #[inline]
    pub fn compute(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Self::Cosine => cosine_distance(a, b),
            Self::EuclideanSq => euclidean_distance_sq(a, b),
            Self::DotProduct => -dot_product(a, b),
            Self::Manhattan => manhattan_distance(a, b),
        }
    }
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cosine => f.write_str("cosine"),
            Self::EuclideanSq => f.write_str("euclidean_sq"),
            Self::DotProduct => f.write_str("dot_product"),
            Self::Manhattan => f.write_str("manhattan"),
        }
    }
}

// ---------------------------------------------------------------------------
// Core distance functions
// ---------------------------------------------------------------------------

/// Computes the dot product of two f32 slices.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "dot_product: dimension mismatch ({} vs {})",
        a.len(),
        b.len()
    );
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Computes the squared Euclidean distance between two f32 slices.
///
/// Returns the sum of squared element-wise differences. Take the square root
/// for the actual Euclidean distance, but the squared form is sufficient for
/// nearest-neighbor comparisons and avoids the sqrt cost.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn euclidean_distance_sq(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "euclidean_distance_sq: dimension mismatch ({} vs {})",
        a.len(),
        b.len()
    );
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Computes the cosine similarity between two f32 slices.
///
/// Returns a value in `[-1.0, 1.0]` where 1.0 means identical direction,
/// 0.0 means orthogonal, and -1.0 means opposite direction.
///
/// Returns 0.0 if either vector has zero magnitude.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "cosine_similarity: dimension mismatch ({} vs {})",
        a.len(),
        b.len()
    );
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

/// Computes the cosine distance between two f32 slices.
///
/// Defined as `1.0 - cosine_similarity(a, b)`, returning a value in `[0.0, 2.0]`
/// where 0.0 means identical direction and 2.0 means opposite direction.
/// This is the standard distance metric used in vector search (lower = more similar).
///
/// Returns 1.0 if either vector has zero magnitude.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Computes the Manhattan (L1) distance between two f32 slices.
///
/// Returns the sum of absolute element-wise differences.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "manhattan_distance: dimension mismatch ({} vs {})",
        a.len(),
        b.len()
    );
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum()
}

/// Computes the Hamming distance between two byte slices interpreted as binary vectors.
///
/// Counts the number of bits that differ between `a` and `b`. Useful for binary
/// embeddings (e.g., Cohere binary, Matryoshka quantized vectors).
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    assert_eq!(
        a.len(),
        b.len(),
        "hamming_distance: length mismatch ({} vs {})",
        a.len(),
        b.len()
    );
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x ^ y).count_ones())
        .sum()
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

/// Computes the L2 (Euclidean) norm of a vector.
///
/// Returns `sqrt(sum(x_i^2))`.
#[inline]
pub fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

/// Normalizes a vector to unit length (L2 norm = 1.0) in place.
///
/// After normalization, `dot_product(v, v) ~= 1.0` and `cosine_similarity`
/// reduces to a simple `dot_product`, which is significantly faster.
///
/// If the vector has zero magnitude, it is left unchanged.
#[inline]
pub fn l2_normalize(v: &mut [f32]) {
    let norm = l2_norm(v);
    if norm > 0.0 {
        let inv = 1.0 / norm;
        for x in v.iter_mut() {
            *x *= inv;
        }
    }
}

/// Returns a new L2-normalized copy of the input vector.
///
/// If the input has zero magnitude, returns a zero vector of the same length.
#[inline]
pub fn l2_normalized(v: &[f32]) -> Vec<f32> {
    let mut out = v.to_vec();
    l2_normalize(&mut out);
    out
}

// ---------------------------------------------------------------------------
// Quantization
// ---------------------------------------------------------------------------

/// Converts an f32 vector to a binary quantized representation.
///
/// Each f32 dimension is mapped to a single bit: 1 if positive, 0 otherwise.
/// The result is packed into bytes (MSB-first within each byte), with the
/// output length equal to `ceil(input.len() / 8)`.
///
/// This gives 32x compression over f32 storage. Use
/// [`hamming_distance`] to compare binary vectors.
///
/// # Example
///
/// ```rust,ignore
/// let v = [1.0f32, -0.5, 0.3, -0.1, 0.0, 0.7, -0.2, 0.9];
/// let bq = redb::quantize_binary(&v);
/// // bit pattern: [1,0,1,0, 0,1,0,1] = 0b10100101 = 0xA5
/// assert_eq!(bq, vec![0xA5]);
/// ```
pub fn quantize_binary(v: &[f32]) -> Vec<u8> {
    let byte_count = v.len().div_ceil(8);
    let mut result = vec![0u8; byte_count];
    for (i, &val) in v.iter().enumerate() {
        if val > 0.0 {
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8); // MSB-first
            result[byte_idx] |= 1 << bit_idx;
        }
    }
    result
}

/// Scalar-quantizes an f32 vector to u8 codes with min/max scale factors.
///
/// Maps each f32 value linearly to the `[0, 255]` range based on the vector's
/// min and max values. Returns an [`SQVec`] containing the scale factors and codes.
///
/// This gives approximately 4x compression over f32 storage with bounded
/// quantization error of `(max - min) / 510` per dimension.
///
/// # Example
///
/// ```rust,ignore
/// let v = [0.0f32, 0.5, 1.0, 0.25];
/// let sq: SQVec<4> = redb::quantize_scalar(&v);
/// assert_eq!(sq.min_val, 0.0);
/// assert_eq!(sq.max_val, 1.0);
/// assert_eq!(sq.codes[2], 255); // max maps to 255
/// ```
pub fn quantize_scalar<const N: usize>(v: &[f32; N]) -> SQVec<N> {
    let mut min_val = f32::INFINITY;
    let mut max_val = f32::NEG_INFINITY;
    for &x in v {
        if x < min_val {
            min_val = x;
        }
        if x > max_val {
            max_val = x;
        }
    }

    let mut codes = [0u8; N];
    let range = max_val - min_val;
    if range > 0.0 {
        let inv_range = 255.0 / range;
        for (i, &x) in v.iter().enumerate() {
            // Quantize to [0, 255]: value is guaranteed non-negative and <= 255.5
            // because x is clamped within [min_val, max_val].
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let q = ((x - min_val) * inv_range + 0.5) as u8;
            codes[i] = q;
        }
    }
    // If range == 0 all codes stay 0, and dequantize returns min_val for all

    SQVec {
        min_val,
        max_val,
        codes,
    }
}

/// Dequantizes an [`SQVec`] back to an array of f32 values.
///
/// This is a convenience wrapper around [`SQVec::dequantize`].
#[inline]
pub fn dequantize_scalar<const N: usize>(sq: &SQVec<N>) -> [f32; N] {
    sq.dequantize()
}

/// Computes approximate squared Euclidean distance between an f32 query and
/// a scalar-quantized vector, dequantizing on the fly.
///
/// This avoids materializing the full f32 vector when doing distance
/// comparisons during search, reducing memory bandwidth.
#[inline]
pub fn sq_euclidean_distance_sq<const N: usize>(query: &[f32; N], sq: &SQVec<N>) -> f32 {
    let range = sq.max_val - sq.min_val;
    if range == 0.0 {
        // All codes map to min_val
        let d = query[0] - sq.min_val;
        #[allow(clippy::cast_precision_loss)]
        return d * d * (N as f32);
    }
    let scale = range / 255.0;
    let mut sum = 0.0f32;
    for (i, &q) in query.iter().enumerate() {
        let dequant = sq.min_val + f32::from(sq.codes[i]) * scale;
        let diff = q - dequant;
        sum += diff * diff;
    }
    sum
}

/// Computes approximate dot product between an f32 query and a scalar-quantized
/// vector, dequantizing on the fly.
#[inline]
pub fn sq_dot_product<const N: usize>(query: &[f32; N], sq: &SQVec<N>) -> f32 {
    let range = sq.max_val - sq.min_val;
    if range == 0.0 {
        return query.iter().sum::<f32>() * sq.min_val;
    }
    let scale = range / 255.0;
    let mut sum = 0.0f32;
    for (i, &q) in query.iter().enumerate() {
        let dequant = sq.min_val + f32::from(sq.codes[i]) * scale;
        sum += q * dequant;
    }
    sum
}

// ---------------------------------------------------------------------------
// Top-K scan
// ---------------------------------------------------------------------------

/// A scored result from a nearest-neighbor search.
#[derive(Debug, Clone)]
pub struct Neighbor<K> {
    /// The key of the matching row.
    pub key: K,
    /// The distance from the query vector (lower = more similar).
    pub distance: f32,
}

impl<K> PartialEq for Neighbor<K> {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl<K> Eq for Neighbor<K> {}

impl<K> PartialOrd for Neighbor<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for Neighbor<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; we want the *largest* distance at the top
        // so we can efficiently evict it. This is standard top-k min-heap trick.
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Brute-force top-k nearest neighbor scan over an iterator of `(key, vector)` pairs.
///
/// Returns up to `k` nearest neighbors sorted by ascending distance (closest first).
/// The distance function should return lower values for more similar vectors.
///
/// This is the fundamental building block for vector search. Higher-level index
/// structures (IVF, HNSW) use this for scanning candidate shortlists.
///
/// # Example
///
/// ```rust,ignore
/// use redb::{nearest_k, DistanceMetric, FixedVec, ReadableTable};
///
/// let query = [1.0f32, 0.0, 0.0, 0.0];
/// let metric = DistanceMetric::Cosine;
///
/// let results = nearest_k(
///     table.iter()?.map(|r| {
///         let (k, v) = r.unwrap();
///         (k.value(), v.value())
///     }),
///     &query,
///     10,
///     |a, b| metric.compute(a, b),
/// );
///
/// for neighbor in &results {
///     println!("key={}, distance={}", neighbor.key, neighbor.distance);
/// }
/// ```
pub fn nearest_k<K, I, F>(iter: I, query: &[f32], k: usize, distance_fn: F) -> Vec<Neighbor<K>>
where
    I: Iterator<Item = (K, Vec<f32>)>,
    F: Fn(&[f32], &[f32]) -> f32,
{
    if k == 0 {
        return Vec::new();
    }

    // Max-heap of size k: the root is the worst (largest distance) candidate.
    // When we find something better, we pop the worst and push the new one.
    let mut heap: BinaryHeap<Neighbor<K>> = BinaryHeap::with_capacity(k + 1);

    for (key, vec) in iter {
        let dist = distance_fn(query, &vec);
        if heap.len() < k {
            heap.push(Neighbor {
                key,
                distance: dist,
            });
        } else if heap.peek().is_some_and(|worst| dist < worst.distance) {
            heap.pop();
            heap.push(Neighbor {
                key,
                distance: dist,
            });
        }
    }

    let mut results: Vec<Neighbor<K>> = heap.into_vec();
    results.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

/// Brute-force top-k scan with a fixed-size array query (zero-copy variant).
///
/// Same as [`nearest_k`] but takes `[f32; N]` vectors from the iterator,
/// avoiding the `Vec<f32>` allocation overhead for fixed-dimension tables.
pub fn nearest_k_fixed<K, I, F, const N: usize>(
    iter: I,
    query: &[f32; N],
    k: usize,
    distance_fn: F,
) -> Vec<Neighbor<K>>
where
    I: Iterator<Item = (K, [f32; N])>,
    F: Fn(&[f32], &[f32]) -> f32,
{
    if k == 0 {
        return Vec::new();
    }

    let mut heap: BinaryHeap<Neighbor<K>> = BinaryHeap::with_capacity(k + 1);

    for (key, vec) in iter {
        let dist = distance_fn(query.as_slice(), vec.as_slice());
        if heap.len() < k {
            heap.push(Neighbor {
                key,
                distance: dist,
            });
        } else if heap.peek().is_some_and(|worst| dist < worst.distance) {
            heap.pop();
            heap.push(Neighbor {
                key,
                distance: dist,
            });
        }
    }

    let mut results: Vec<Neighbor<K>> = heap.into_vec();
    results.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

// ---------------------------------------------------------------------------
// LE byte helpers
// ---------------------------------------------------------------------------

/// Writes a slice of f32 values as little-endian bytes into a destination buffer.
///
/// Useful for populating `insert_reserve` buffers when using `FixedVec<N>`.
///
/// # Panics
///
/// Panics if `dest.len() != values.len() * 4`.
#[inline]
pub fn write_f32_le(dest: &mut [u8], values: &[f32]) {
    assert_eq!(
        dest.len(),
        values.len() * 4,
        "write_f32_le: buffer size {} != expected {}",
        dest.len(),
        values.len() * 4
    );
    for (i, val) in values.iter().enumerate() {
        let start = i * 4;
        dest[start..start + 4].copy_from_slice(&val.to_le_bytes());
    }
}

/// Reads little-endian f32 values from a byte slice.
///
/// # Panics
///
/// Panics if `src.len()` is not a multiple of 4.
#[inline]
pub fn read_f32_le(src: &[u8]) -> Vec<f32> {
    assert_eq!(
        src.len() % 4,
        0,
        "read_f32_le: byte length {} is not a multiple of 4",
        src.len()
    );
    let count = src.len() / 4;
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let start = i * 4;
        let bytes: [u8; 4] = src[start..start + 4].try_into().unwrap();
        result.push(f32::from_le_bytes(bytes));
    }
    result
}
