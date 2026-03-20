use alloc::vec::Vec;

use crate::vector_ops::{DistanceMetric, dot_product, euclidean_distance_sq, manhattan_distance};

use super::pq::Codebooks;

// ---------------------------------------------------------------------------
// ADC — Asymmetric Distance Computation
// ---------------------------------------------------------------------------

/// Precomputed lookup table for fast approximate distance computation.
///
/// For each sub-quantizer `m` (`0..num_subvectors`) and each codeword `k`
/// (0..256), stores the distance from the query sub-vector to that codeword's
/// centroid. Total storage: `num_subvectors × 256` f32 values.
///
/// At scan time the approximate distance to a PQ-encoded vector is the sum
/// of `num_subvectors` table lookups — extremely fast.
pub struct AdcTable {
    /// Flat array: `distances[m * 256 + k]`.
    distances: Vec<f32>,
    num_subvectors: usize,
}

impl AdcTable {
    /// Build the ADC lookup table from a query vector and codebooks.
    ///
    /// For Cosine metric the query should already be L2-normalized before
    /// calling this method (normalization is the caller's responsibility at
    /// the index level, matching the normalisation applied at insert time).
    pub fn build(query: &[f32], codebooks: &Codebooks, metric: DistanceMetric) -> Self {
        let m = codebooks.num_subvectors;
        let sub_dim = codebooks.sub_dim;
        let mut distances = Vec::with_capacity(m * 256);

        for sub_idx in 0..m {
            let q_sub = &query[sub_idx * sub_dim..(sub_idx + 1) * sub_dim];
            for k in 0..256 {
                let centroid = codebooks.centroid(sub_idx, k);
                let d = subvector_distance(q_sub, centroid, metric);
                distances.push(d);
            }
        }

        Self {
            distances,
            num_subvectors: m,
        }
    }

    /// Compute the approximate distance from the query to a PQ-encoded vector.
    ///
    /// `pq_codes` must have length `num_subvectors`. Each byte is a codebook
    /// index. The result is the sum of precomputed sub-vector distances.
    #[inline]
    pub fn approximate_distance(&self, pq_codes: &[u8]) -> f32 {
        debug_assert_eq!(pq_codes.len(), self.num_subvectors);

        let mut dist = 0.0f32;
        for (m, &code) in pq_codes.iter().enumerate() {
            // SAFETY: m < num_subvectors and code < 256, so index is always
            // within bounds of the distances array allocated in `build()`.
            unsafe {
                dist += *self.distances.get_unchecked(m * 256 + code as usize);
            }
        }
        dist
    }

    /// Same as `approximate_distance` but with bounds checking (for tests).
    pub fn approximate_distance_checked(&self, pq_codes: &[u8]) -> f32 {
        assert_eq!(pq_codes.len(), self.num_subvectors);
        let mut dist = 0.0f32;
        for (m, &code) in pq_codes.iter().enumerate() {
            dist += self.distances[m * 256 + code as usize];
        }
        dist
    }
}

impl core::fmt::Debug for AdcTable {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AdcTable")
            .field("num_subvectors", &self.num_subvectors)
            .field("table_entries", &self.distances.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Sub-vector distance computation per metric
// ---------------------------------------------------------------------------

/// Compute the distance between a query sub-vector and a codebook centroid.
///
/// The distance function used depends on the metric:
/// - `EuclideanSq`: squared Euclidean (sums of squared diffs — additive over
///   sub-vectors, so the total PQ distance is the sum of sub-vector distances).
/// - `DotProduct`: negative dot product (additive: total = sum of sub-products).
/// - `Cosine`: same as `DotProduct` (query is L2-normalised at index level,
///   stored vectors are L2-normalised at insert time).
/// - `Manhattan`: L1 distance (additive over sub-vectors).
#[inline]
fn subvector_distance(query_sub: &[f32], centroid: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::EuclideanSq => euclidean_distance_sq(query_sub, centroid),
        DistanceMetric::DotProduct | DistanceMetric::Cosine => -dot_product(query_sub, centroid),
        DistanceMetric::Manhattan => manhattan_distance(query_sub, centroid),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivfpq::pq::train_codebooks;

    #[test]
    fn adc_matches_exact_distance() {
        // 8-dim, 2 sub-vectors. Train codebooks on simple data.
        #[rustfmt::skip]
        let training: Vec<f32> = vec![
            1.0, 0.0, 0.0, 0.0,  0.0, 0.0, 0.0, 1.0,
            0.0, 1.0, 0.0, 0.0,  0.0, 0.0, 1.0, 0.0,
            0.0, 0.0, 1.0, 0.0,  0.0, 1.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 1.0,  1.0, 0.0, 0.0, 0.0,
        ];
        let codebooks = train_codebooks(&training, 8, 2, 25, DistanceMetric::EuclideanSq);

        let query = vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0];
        let adc = AdcTable::build(&query, &codebooks, DistanceMetric::EuclideanSq);

        // Encode the first training vector (same as query).
        let codes = codebooks.encode(&training[0..8]);
        let approx_dist = adc.approximate_distance_checked(&codes);

        // Exact distance to self should be 0 or near-zero via PQ approximation.
        assert!(
            approx_dist < 0.5,
            "expected near-zero approx distance for self, got {approx_dist}"
        );
    }

    #[test]
    fn adc_ordering_preserved() {
        // Verify that ADC distances preserve relative ordering for simple cases.
        #[rustfmt::skip]
        let training: Vec<f32> = vec![
            0.0, 0.0,  0.0, 0.0,
            1.0, 1.0,  1.0, 1.0,
            5.0, 5.0,  5.0, 5.0,
            10.0, 10.0, 10.0, 10.0,
        ];
        let codebooks = train_codebooks(&training, 4, 2, 25, DistanceMetric::EuclideanSq);
        let query = vec![0.0, 0.0, 0.0, 0.0];
        let adc = AdcTable::build(&query, &codebooks, DistanceMetric::EuclideanSq);

        let codes_near = codebooks.encode(&[1.0, 1.0, 1.0, 1.0]);
        let codes_far = codebooks.encode(&[10.0, 10.0, 10.0, 10.0]);

        let d_near = adc.approximate_distance_checked(&codes_near);
        let d_far = adc.approximate_distance_checked(&codes_far);

        assert!(
            d_near < d_far,
            "ordering violated: near={d_near}, far={d_far}"
        );
    }
}
