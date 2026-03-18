/// Computes the dot product of two f32 slices.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
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

/// Computes the Manhattan (L1) distance between two f32 slices.
///
/// Returns the sum of absolute element-wise differences.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
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

/// Writes a slice of f32 values as little-endian bytes into a destination buffer.
///
/// Useful for populating `insert_reserve` buffers when using `FixedVec<N>`.
///
/// # Panics
///
/// Panics if `dest.len() != values.len() * 4`.
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
