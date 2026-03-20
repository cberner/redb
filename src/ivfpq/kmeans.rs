use alloc::vec;
use alloc::vec::Vec;

use crate::vector_ops::{DistanceMetric, euclidean_distance_sq};

// ---------------------------------------------------------------------------
// Deterministic PRNG — no_std compatible
// ---------------------------------------------------------------------------

/// Xorshift64 PRNG. Deterministic, fast, `no_std`.
/// Quality is sufficient for k-means++ initialisation.
pub(crate) struct Xorshift64(u64);

impl Xorshift64 {
    pub fn new(seed: u64) -> Self {
        // Ensure non-zero state.
        Self(if seed == 0 {
            0x5EED_DEAD_BEEF_CAFE
        } else {
            seed
        })
    }

    /// Seed from a slice of f32 data (hashes the bytes via a simple mix).
    pub fn from_data(data: &[f32]) -> Self {
        let mut h: u64 = 0x517c_c1b7_2722_0a95;
        for &x in data {
            let bits = u64::from(x.to_bits());
            h ^= bits;
            h = h.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            h ^= h >> 30;
        }
        Self::new(h)
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Returns a value in [0.0, 1.0).
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    /// Returns a random index in [0, n).
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn next_usize(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// k-means++ initialisation
// ---------------------------------------------------------------------------

/// Select `k` initial centroids from `vectors` using k-means++ seeding.
///
/// Each vector is `dim` contiguous f32 values. The total number of vectors is
/// `vectors.len() / dim`.
fn kmeans_pp_init(flat_vectors: &[f32], dim: usize, k: usize, rng: &mut Xorshift64) -> Vec<f32> {
    let n = flat_vectors.len() / dim;
    debug_assert!(k > 0 && k <= n);

    // Output: k centroids × dim floats, stored flat.
    let mut centroids = Vec::with_capacity(k * dim);

    // Pick the first centroid uniformly at random.
    let first = rng.next_usize(n);
    centroids.extend_from_slice(&flat_vectors[first * dim..(first + 1) * dim]);

    // Distance from each vector to its nearest already-chosen centroid.
    let mut min_dists = vec![f32::INFINITY; n];

    for c in 1..k {
        // Update min_dists with the most recently added centroid.
        let prev_start = (c - 1) * dim;
        let prev_centroid = &centroids[prev_start..prev_start + dim];
        let mut total: f64 = 0.0;
        for i in 0..n {
            let v = &flat_vectors[i * dim..(i + 1) * dim];
            let d = euclidean_distance_sq(v, prev_centroid);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
            total += f64::from(min_dists[i]);
        }

        // Weighted random selection proportional to min_dists.
        if total <= 0.0 {
            // Degenerate: all remaining points coincide with existing centroids.
            // Pick any unassigned point.
            centroids.extend_from_slice(&flat_vectors[rng.next_usize(n) * dim..][..dim]);
            continue;
        }

        let threshold = rng.next_f64() * total;
        let mut cumulative: f64 = 0.0;
        let mut chosen = n - 1;
        for (i, dist) in min_dists.iter().enumerate().take(n) {
            cumulative += f64::from(*dist);
            if cumulative >= threshold {
                chosen = i;
                break;
            }
        }
        centroids.extend_from_slice(&flat_vectors[chosen * dim..(chosen + 1) * dim]);
    }

    centroids
}

// ---------------------------------------------------------------------------
// Lloyd's k-means iteration
// ---------------------------------------------------------------------------

/// Assign each vector to its nearest centroid.
///
/// Returns a vector of length `n` with cluster indices, and the total
/// intra-cluster distance (for convergence checking).
fn assign_all(
    flat_vectors: &[f32],
    dim: usize,
    centroids: &[f32],
    k: usize,
    assignments: &mut Vec<u32>,
    metric: DistanceMetric,
) -> f64 {
    let n = flat_vectors.len() / dim;
    assignments.clear();
    assignments.reserve(n);

    let mut total_dist: f64 = 0.0;

    for i in 0..n {
        let v = &flat_vectors[i * dim..(i + 1) * dim];
        let mut best_c = 0u32;
        let mut best_d = f32::INFINITY;
        for c in 0..k {
            let centroid = &centroids[c * dim..(c + 1) * dim];
            let d = metric.compute(v, centroid);
            if d < best_d {
                best_d = d;
                #[allow(clippy::cast_possible_truncation)]
                {
                    best_c = c as u32;
                }
            }
        }
        assignments.push(best_c);
        total_dist += f64::from(best_d);
    }

    total_dist
}

/// Recompute centroids as the mean of assigned vectors.
///
/// Uses f64 accumulators to avoid float overflow when summing many large-valued
/// vectors in a single cluster (BUG 8 fix).
fn recompute_centroids(
    flat_vectors: &[f32],
    dim: usize,
    k: usize,
    assignments: &[u32],
    centroids: &mut [f32],
) {
    let n = flat_vectors.len() / dim;

    // Accumulate in f64 to prevent overflow with large clusters / extreme values.
    let mut accum = vec![0.0f64; k * dim];
    let mut counts = vec![0u64; k];

    for i in 0..n {
        let c = assignments[i] as usize;
        counts[c] += 1;
        let v = &flat_vectors[i * dim..(i + 1) * dim];
        let dest = &mut accum[c * dim..(c + 1) * dim];
        for (d, &s) in dest.iter_mut().zip(v.iter()) {
            *d += f64::from(s);
        }
    }

    // Divide by count and convert back to f32.
    for c in 0..k {
        if counts[c] > 0 {
            #[allow(clippy::cast_precision_loss)]
            let scale = 1.0 / counts[c] as f64;
            let src = &accum[c * dim..(c + 1) * dim];
            let dest = &mut centroids[c * dim..(c + 1) * dim];
            #[allow(clippy::cast_possible_truncation)]
            for (d, &s) in dest.iter_mut().zip(src.iter()) {
                *d = (s * scale) as f32;
            }
        } else {
            // Zero out centroids for empty clusters (will be restored from
            // old_centroids by the caller).
            centroids[c * dim..(c + 1) * dim].fill(0.0);
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run k-means clustering on flat vectors and return `k` centroids.
///
/// `flat_vectors` contains `n` vectors of dimension `dim`, stored contiguously.
/// Uses k-means++ initialisation and Lloyd's algorithm.
///
/// Returns a flat `Vec<f32>` of length `k * dim`.
pub fn kmeans(
    flat_vectors: &[f32],
    dim: usize,
    k: usize,
    max_iter: usize,
    metric: DistanceMetric,
) -> Vec<f32> {
    let n = flat_vectors.len() / dim;
    assert!(n > 0, "kmeans: empty input");
    assert!(dim > 0, "kmeans: zero dimension");

    // Clamp k to available points.
    let k = k.min(n);
    if k == 0 {
        return Vec::new();
    }

    let mut rng = Xorshift64::from_data(&flat_vectors[..dim.min(flat_vectors.len())]);
    let mut centroids = kmeans_pp_init(flat_vectors, dim, k, &mut rng);

    let mut assignments = Vec::with_capacity(n);
    let mut prev_dist = f64::INFINITY;

    for iter in 0..max_iter {
        let total_dist = assign_all(flat_vectors, dim, &centroids, k, &mut assignments, metric);

        // Convergence: less than 0.1% relative improvement.
        let improvement = (prev_dist - total_dist) / prev_dist.max(1e-12);
        if improvement < 0.001 && iter > 0 {
            break;
        }
        prev_dist = total_dist;

        // Save current centroids for empty-cluster fallback.
        let old_centroids = centroids.clone();
        recompute_centroids(flat_vectors, dim, k, &assignments, &mut centroids);

        // Restore centroids for any cluster that ended up empty.
        let mut counts = vec![0u32; k];
        for &a in &assignments {
            counts[a as usize] += 1;
        }
        for c in 0..k {
            if counts[c] == 0 {
                centroids[c * dim..(c + 1) * dim]
                    .copy_from_slice(&old_centroids[c * dim..(c + 1) * dim]);
            }
        }
    }

    centroids
}

/// Assign a single vector to its nearest centroid.
///
/// Returns `(cluster_index, distance)`.
pub fn assign_nearest(
    vector: &[f32],
    centroids: &[f32],
    dim: usize,
    num_clusters: usize,
    metric: DistanceMetric,
) -> (u32, f32) {
    let mut best_c = 0u32;
    let mut best_d = f32::INFINITY;
    for c in 0..num_clusters {
        let centroid = &centroids[c * dim..(c + 1) * dim];
        let d = metric.compute(vector, centroid);
        if d < best_d {
            best_d = d;
            #[allow(clippy::cast_possible_truncation)]
            {
                best_c = c as u32;
            }
        }
    }
    (best_c, best_d)
}

/// Find the `nprobe` nearest clusters to a query vector.
///
/// Returns a list of `(cluster_id, distance)` sorted by ascending distance.
pub fn nearest_clusters(
    query: &[f32],
    centroids: &[f32],
    dim: usize,
    num_clusters: usize,
    nprobe: usize,
    metric: DistanceMetric,
) -> Vec<(u32, f32)> {
    #[allow(clippy::cast_possible_truncation)]
    let mut dists: Vec<(u32, f32)> = (0..num_clusters)
        .map(|c| {
            let centroid = &centroids[c * dim..(c + 1) * dim];
            (c as u32, metric.compute(query, centroid))
        })
        .collect();

    // Partial sort: we only need the top `nprobe` closest.
    let nprobe = nprobe.min(num_clusters).max(1);
    dists.select_nth_unstable_by(nprobe - 1, |a, b| {
        a.1.partial_cmp(&b.1).unwrap_or(core::cmp::Ordering::Equal)
    });
    dists.truncate(nprobe);
    dists.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(core::cmp::Ordering::Equal));
    dists
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xorshift_deterministic() {
        let mut rng1 = Xorshift64::new(42);
        let mut rng2 = Xorshift64::new(42);
        for _ in 0..100 {
            assert_eq!(rng1.next_u64(), rng2.next_u64());
        }
    }

    #[test]
    fn xorshift_f64_range() {
        let mut rng = Xorshift64::new(12345);
        for _ in 0..10_000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v));
        }
    }

    #[test]
    fn kmeans_basic_2d() {
        // Two well-separated clusters in 2D.
        #[rustfmt::skip]
        let data: Vec<f32> = vec![
            // Cluster around (0, 0)
            0.1, 0.1,
            -0.1, 0.2,
            0.0, -0.1,
            0.2, 0.0,
            // Cluster around (10, 10)
            10.0, 10.1,
            9.9, 10.0,
            10.1, 9.9,
            10.0, 10.0,
        ];

        let centroids = kmeans(&data, 2, 2, 25, DistanceMetric::EuclideanSq);
        assert_eq!(centroids.len(), 4); // 2 centroids × 2 dims

        let c0 = &centroids[0..2];
        let c1 = &centroids[2..4];

        // One centroid should be near (0,0) and the other near (10,10).
        let near_origin = |c: &[f32]| c[0].abs() < 1.0 && c[1].abs() < 1.0;
        let near_ten = |c: &[f32]| (c[0] - 10.0).abs() < 1.0 && (c[1] - 10.0).abs() < 1.0;

        assert!(
            (near_origin(c0) && near_ten(c1)) || (near_origin(c1) && near_ten(c0)),
            "centroids did not converge: c0={c0:?}, c1={c1:?}"
        );
    }

    #[test]
    fn assign_nearest_basic() {
        let centroids = vec![0.0, 0.0, 10.0, 10.0];
        let (c, _d) = assign_nearest(&[9.0, 9.0], &centroids, 2, 2, DistanceMetric::EuclideanSq);
        assert_eq!(c, 1);
    }

    #[test]
    fn nearest_clusters_ordering() {
        let centroids = vec![0.0, 0.0, 5.0, 5.0, 10.0, 10.0];
        let result = nearest_clusters(
            &[4.0, 4.0],
            &centroids,
            2,
            3,
            2,
            DistanceMetric::EuclideanSq,
        );
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 1); // closest is (5,5)
        assert_eq!(result[1].0, 0); // next is (0,0)
    }
}
