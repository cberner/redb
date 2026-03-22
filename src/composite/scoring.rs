use crate::ReadTransaction;
use crate::blob_store::types::BlobId;
use crate::compat::HashMap;
use alloc::collections::VecDeque;

/// Compute `numerator / denominator` as an f64 in [0.0, 1.0] without lossy
/// casts on the u64 inputs. Scales the ratio into u32 via u128 arithmetic,
/// then uses lossless `f64::from(u32)` for the final division.
fn ratio_u64(numerator: u64, denominator: u64) -> f64 {
    debug_assert!(denominator > 0);
    debug_assert!(numerator <= denominator);
    let wide = u128::from(numerator) * u128::from(u32::MAX) / u128::from(denominator);
    // numerator <= denominator guarantees wide <= u32::MAX, so this never saturates.
    let scaled = u32::try_from(wide).unwrap_or(u32::MAX);
    f64::from(scaled) / f64::from(u32::MAX)
}

/// Min-max normalization on raw distances, inverted so closer = higher score.
///
/// Input: raw distances from IVF-PQ (lower = more similar).
/// Output: normalized scores in [0.0, 1.0] where 1.0 = closest.
///
/// If all distances are identical, all scores are 1.0.
pub(crate) fn normalize_semantic(blob_distances: &[(BlobId, f32)]) -> HashMap<BlobId, f64> {
    if blob_distances.is_empty() {
        return HashMap::new();
    }

    let min_d = blob_distances
        .iter()
        .map(|(_, d)| *d)
        .fold(f32::INFINITY, f32::min);
    let max_d = blob_distances
        .iter()
        .map(|(_, d)| *d)
        .fold(f32::NEG_INFINITY, f32::max);
    let range = max_d - min_d;

    if range <= f32::EPSILON {
        return blob_distances.iter().map(|(id, _)| (*id, 1.0)).collect();
    }

    blob_distances
        .iter()
        .map(|(id, d)| {
            let score = f64::from(1.0 - (d - min_d) / range);
            (*id, score)
        })
        .collect()
}

/// Min-max normalization on wall-clock timestamps. More recent = higher score.
///
/// Computes ratios via `u128` integer arithmetic to avoid precision loss with
/// nanosecond-scale `u64` timestamps.
pub(crate) fn normalize_temporal(blob_timestamps: &[(BlobId, u64)]) -> HashMap<BlobId, f64> {
    if blob_timestamps.is_empty() {
        return HashMap::new();
    }

    let min_t = blob_timestamps.iter().map(|(_, t)| *t).min().unwrap_or(0);
    let max_t = blob_timestamps.iter().map(|(_, t)| *t).max().unwrap_or(0);
    let range = max_t - min_t;

    if range == 0 {
        return blob_timestamps.iter().map(|(id, _)| (*id, 1.0)).collect();
    }

    blob_timestamps
        .iter()
        .map(|(id, t)| {
            let offset = *t - min_t;
            (*id, ratio_u64(offset, range))
        })
        .collect()
}

/// Linear decay normalization on BFS hop distances. Root = 1.0, farthest = 0.0.
///
/// Uses lossless `f64::from(u32)` for intermediate arithmetic since hop counts
/// are always `u32` and `f64` has a 52-bit mantissa (> 32 bits).
pub(crate) fn normalize_causal(hop_distances: &HashMap<BlobId, u32>) -> HashMap<BlobId, f64> {
    if hop_distances.is_empty() {
        return HashMap::new();
    }

    let max_hops = *hop_distances.values().max().unwrap_or(&0);
    if max_hops == 0 {
        return hop_distances.iter().map(|(id, _)| (*id, 1.0)).collect();
    }

    let max_f = f64::from(max_hops);
    hop_distances
        .iter()
        .map(|(id, hops)| {
            let score = 1.0 - f64::from(*hops) / max_f;
            (*id, score)
        })
        .collect()
}

/// Bidirectional BFS from a root blob, returning hop distances for all reachable blobs.
///
/// Traverses both forward (via `causal_children`) and backward (via `causal_parent`
/// in `BlobMeta`) to discover the full causal neighborhood.
///
/// Returns `HashMap<BlobId, u32>` where the root has distance 0.
pub(crate) fn causal_bfs(
    txn: &ReadTransaction,
    root: &BlobId,
    max_hops: usize,
) -> crate::Result<HashMap<BlobId, u32>> {
    let max_depth = u32::try_from(max_hops).unwrap_or(u32::MAX);
    let mut distances: HashMap<BlobId, u32> = HashMap::new();
    let mut queue: VecDeque<(BlobId, u32)> = VecDeque::new();

    distances.insert(*root, 0);
    queue.push_back((*root, 0));

    while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        // Forward: children of current
        let children = txn.causal_children(&current)?;
        for edge in &children {
            if !distances.contains_key(&edge.child) {
                let new_depth = depth + 1;
                distances.insert(edge.child, new_depth);
                queue.push_back((edge.child, new_depth));
            }
        }

        // Backward: parent of current
        if let Some(meta) = txn.get_blob_meta(&current)?
            && let Some(parent) = meta.causal_parent
            && !distances.contains_key(&parent)
        {
            let new_depth = depth + 1;
            distances.insert(parent, new_depth);
            queue.push_back((parent, new_depth));
        }
    }

    Ok(distances)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bid(seq: u64) -> BlobId {
        BlobId::new(seq, 0)
    }

    #[test]
    fn ratio_u64_boundaries() {
        assert!((ratio_u64(0, 100) - 0.0).abs() < 1e-6);
        assert!((ratio_u64(100, 100) - 1.0).abs() < 1e-6);
        assert!((ratio_u64(50, 100) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn ratio_u64_large_values() {
        let range: u64 = 1_000_000_000 * 3600; // 1 hour in ns
        let half = range / 2;
        assert!((ratio_u64(half, range) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn semantic_normalization_basic() {
        let data = vec![(bid(1), 0.0), (bid(2), 1.0), (bid(3), 0.5)];
        let scores = normalize_semantic(&data);
        assert!((scores[&bid(1)] - 1.0).abs() < 1e-6); // closest
        assert!((scores[&bid(2)] - 0.0).abs() < 1e-6); // farthest
        assert!((scores[&bid(3)] - 0.5).abs() < 1e-6); // middle
    }

    #[test]
    fn semantic_normalization_identical() {
        let data = vec![(bid(1), 0.5), (bid(2), 0.5)];
        let scores = normalize_semantic(&data);
        assert!((scores[&bid(1)] - 1.0).abs() < 1e-6);
        assert!((scores[&bid(2)] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn temporal_normalization_basic() {
        let data = vec![(bid(1), 100), (bid(2), 200), (bid(3), 150)];
        let scores = normalize_temporal(&data);
        assert!((scores[&bid(1)] - 0.0).abs() < 1e-6); // oldest
        assert!((scores[&bid(2)] - 1.0).abs() < 1e-6); // newest
        assert!((scores[&bid(3)] - 0.5).abs() < 1e-4); // middle
    }

    #[test]
    fn temporal_normalization_same_timestamp() {
        let data = vec![(bid(1), 100), (bid(2), 100)];
        let scores = normalize_temporal(&data);
        assert!((scores[&bid(1)] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn causal_normalization_basic() {
        let mut hops = HashMap::new();
        hops.insert(bid(1), 0); // root
        hops.insert(bid(2), 1);
        hops.insert(bid(3), 2);
        let scores = normalize_causal(&hops);
        assert!((scores[&bid(1)] - 1.0).abs() < 1e-6); // root
        assert!((scores[&bid(2)] - 0.5).abs() < 1e-6);
        assert!((scores[&bid(3)] - 0.0).abs() < 1e-6); // farthest
    }

    #[test]
    fn causal_normalization_root_only() {
        let mut hops = HashMap::new();
        hops.insert(bid(1), 0);
        let scores = normalize_causal(&hops);
        assert!((scores[&bid(1)] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn empty_inputs() {
        assert!(normalize_semantic(&[]).is_empty());
        assert!(normalize_temporal(&[]).is_empty());
        assert!(normalize_causal(&HashMap::new()).is_empty());
    }
}
