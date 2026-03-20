use alloc::vec::Vec;

use crate::vector_ops::{euclidean_distance_sq, DistanceMetric};

use super::kmeans;

// ---------------------------------------------------------------------------
// Codebooks — PQ codebook storage and encode/decode
// ---------------------------------------------------------------------------

/// Product Quantization codebooks.
///
/// Contains `num_subvectors` codebooks, each with 256 centroids of dimension
/// `sub_dim`. Stored as a single flat `Vec<f32>`:
///
/// ```text
/// codebooks[m][k] starts at offset (m * 256 + k) * sub_dim
/// ```
///
/// Total size: `num_subvectors * 256 * sub_dim` floats.
#[derive(Clone)]
pub struct Codebooks {
    /// Flat storage of all codebook centroids.
    pub data: Vec<f32>,
    /// Number of sub-quantizers (= number of subvector positions).
    pub num_subvectors: usize,
    /// Dimension of each sub-vector (= dim / `num_subvectors`).
    pub sub_dim: usize,
}

impl Codebooks {
    /// Returns the centroid for sub-quantizer `m`, codeword `k`.
    #[inline]
    pub fn centroid(&self, m: usize, k: usize) -> &[f32] {
        let start = (m * 256 + k) * self.sub_dim;
        &self.data[start..start + self.sub_dim]
    }

    /// Encode a full vector into PQ codes.
    ///
    /// The vector must have length `num_subvectors * sub_dim`. Returns a
    /// `Vec<u8>` of length `num_subvectors`, where each byte is the index
    /// of the nearest codebook centroid for that sub-vector position.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = Vec::with_capacity(self.num_subvectors);
        for m in 0..self.num_subvectors {
            let sub = &vector[m * self.sub_dim..(m + 1) * self.sub_dim];
            let mut best_k = 0u8;
            let mut best_dist = f32::INFINITY;
            for k in 0..256usize {
                let centroid = self.centroid(m, k);
                let d = euclidean_distance_sq(sub, centroid);
                if d < best_dist {
                    best_dist = d;
                    #[allow(clippy::cast_possible_truncation)]
                    { best_k = k as u8; }
                }
            }
            codes.push(best_k);
        }
        codes
    }

    /// Decode PQ codes back to an approximate vector.
    ///
    /// Reconstructs the vector by concatenating the codebook centroids
    /// indicated by each code byte.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut vector = Vec::with_capacity(self.num_subvectors * self.sub_dim);
        for (m, &code) in codes.iter().enumerate() {
            vector.extend_from_slice(self.centroid(m, code as usize));
        }
        vector
    }

    /// Total number of f32 elements in the codebook data.
    pub fn data_len(&self) -> usize {
        self.num_subvectors * 256 * self.sub_dim
    }

    /// Serialize all codebooks to bytes (f32 little-endian).
    ///
    /// Each codebook `m` is serialized as `256 * sub_dim * 4` bytes.
    pub fn serialize_codebook(&self, m: usize) -> Vec<u8> {
        let start = m * 256 * self.sub_dim;
        let end = start + 256 * self.sub_dim;
        let floats = &self.data[start..end];
        let mut bytes = Vec::with_capacity(floats.len() * 4);
        for &f in floats {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        bytes
    }

    /// Deserialize a single codebook from bytes.
    pub fn deserialize_codebook(bytes: &[u8], sub_dim: usize) -> Vec<f32> {
        let num_floats = bytes.len() / 4;
        debug_assert_eq!(num_floats, 256 * sub_dim);
        let mut floats = Vec::with_capacity(num_floats);
        for chunk in bytes.chunks_exact(4) {
            floats.push(f32::from_le_bytes(chunk.try_into().unwrap()));
        }
        floats
    }
}

impl core::fmt::Debug for Codebooks {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Codebooks")
            .field("num_subvectors", &self.num_subvectors)
            .field("sub_dim", &self.sub_dim)
            .field("data_len", &self.data.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// PQ training
// ---------------------------------------------------------------------------

/// Train PQ codebooks from a set of training vectors.
///
/// `flat_vectors` contains `n` vectors of dimension `dim`, stored contiguously.
/// `num_subvectors` sub-quantizers are trained, each with 256 codewords.
///
/// For each sub-vector position, k-means with k=256 is run on the corresponding
/// sub-vector slices from all training vectors.
pub fn train_codebooks(
    flat_vectors: &[f32],
    dim: usize,
    num_subvectors: usize,
    max_iter: usize,
    _metric: DistanceMetric,
) -> Codebooks {
    let n = flat_vectors.len() / dim;
    let sub_dim = dim / num_subvectors;
    assert!(
        dim % num_subvectors == 0,
        "dim ({dim}) must be divisible by num_subvectors ({num_subvectors})"
    );

    let k = 256usize.min(n); // Can't have more codewords than training vectors.

    let mut all_data = Vec::with_capacity(num_subvectors * 256 * sub_dim);

    for m in 0..num_subvectors {
        // Extract the m-th sub-vector slice from each training vector into a
        // contiguous buffer for k-means.
        let mut sub_flat = Vec::with_capacity(n * sub_dim);
        for i in 0..n {
            let start = i * dim + m * sub_dim;
            sub_flat.extend_from_slice(&flat_vectors[start..start + sub_dim]);
        }

        // For PQ sub-quantizer training we always use EuclideanSq for
        // codebook construction (independent of the outer metric). This is
        // standard practice as PQ codes represent distortion in Euclidean
        // space, and the ADC table maps this to the requested metric at
        // query time.
        let centroids = kmeans::kmeans(&sub_flat, sub_dim, k, max_iter, DistanceMetric::EuclideanSq);

        // If k < 256, pad remaining codewords with zeros.
        all_data.extend_from_slice(&centroids);
        if k < 256 {
            all_data.resize(all_data.len() + (256 - k) * sub_dim, 0.0);
        }
    }

    Codebooks {
        data: all_data,
        num_subvectors,
        sub_dim,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        // 8-dim vectors, 2 sub-vectors of 4 dims each, 4 training vectors.
        #[rustfmt::skip]
        let training: Vec<f32> = vec![
            1.0, 0.0, 0.0, 0.0,  0.0, 0.0, 0.0, 1.0,
            0.0, 1.0, 0.0, 0.0,  0.0, 0.0, 1.0, 0.0,
            0.0, 0.0, 1.0, 0.0,  0.0, 1.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 1.0,  1.0, 0.0, 0.0, 0.0,
        ];

        let codebooks = train_codebooks(&training, 8, 2, 25, DistanceMetric::EuclideanSq);
        assert_eq!(codebooks.num_subvectors, 2);
        assert_eq!(codebooks.sub_dim, 4);

        // Encode one of the training vectors — should reconstruct closely.
        let original = &training[0..8];
        let codes = codebooks.encode(original);
        assert_eq!(codes.len(), 2);

        let reconstructed = codebooks.decode(&codes);
        assert_eq!(reconstructed.len(), 8);

        // The reconstruction should be close to the original.
        let error: f32 = original
            .iter()
            .zip(reconstructed.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum();
        assert!(
            error < 1.0,
            "reconstruction error too high: {error}, original={original:?}, reconstructed={reconstructed:?}"
        );
    }

    #[test]
    fn codebook_serialize_roundtrip() {
        let codebooks = Codebooks {
            data: [1.0, 2.0, 3.0, 4.0].repeat(256), // 1 subvec, sub_dim=4, 256 codewords
            num_subvectors: 1,
            sub_dim: 4,
        };
        let bytes = codebooks.serialize_codebook(0);
        let floats = Codebooks::deserialize_codebook(&bytes, 4);
        assert_eq!(floats.len(), 256 * 4);
        assert!((floats[0] - 1.0).abs() < 1e-6);
    }
}
