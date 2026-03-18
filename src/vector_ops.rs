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
