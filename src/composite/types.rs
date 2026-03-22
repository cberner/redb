use crate::blob_store::types::{BlobId, BlobMeta};
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::fmt;

/// Weights for the three scoring signals.
///
/// Each weight controls the relative importance of its signal in the final
/// composite score. Weights are normalized to sum to 1.0 before fusion,
/// so only relative magnitudes matter. A weight of 0.0 disables that signal
/// entirely (no candidates fetched, no computation).
#[derive(Debug, Clone)]
pub struct SignalWeights {
    pub semantic: f32,
    pub temporal: f32,
    pub causal: f32,
}

impl SignalWeights {
    /// Returns normalized weights as f64 that sum to 1.0.
    /// If all weights are zero, returns (0, 0, 0).
    pub(crate) fn normalized_f64(&self) -> (f64, f64, f64) {
        let total = f64::from(self.semantic) + f64::from(self.temporal) + f64::from(self.causal);
        if total <= f64::from(f32::EPSILON) {
            return (0.0, 0.0, 0.0);
        }
        (
            f64::from(self.semantic) / total,
            f64::from(self.temporal) / total,
            f64::from(self.causal) / total,
        )
    }

    pub(crate) fn any_active(&self) -> bool {
        self.semantic > 0.0 || self.temporal > 0.0 || self.causal > 0.0
    }
}

impl Default for SignalWeights {
    fn default() -> Self {
        Self {
            semantic: 0.0,
            temporal: 0.0,
            causal: 0.0,
        }
    }
}

/// Per-signal score breakdown for a single result.
///
/// All scores are in `[0.0, 1.0]` where 1.0 is the best match for that signal.
/// `None` means the signal was disabled (weight = 0).
#[derive(Debug, Clone)]
pub struct SignalScores {
    pub semantic: Option<f64>,
    pub temporal: Option<f64>,
    pub causal: Option<f64>,
}

/// A single fused result from a composite query.
#[derive(Debug, Clone)]
pub struct ScoredBlob {
    pub blob_id: BlobId,
    pub meta: BlobMeta,
    /// Composite score in `[0.0, 1.0]`. Higher = better match.
    pub score: f64,
    /// Per-signal scores for debuggability.
    pub signals: SignalScores,
}

impl fmt::Display for ScoredBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScoredBlob({:?}, score={:.4})", self.blob_id, self.score)
    }
}

/// Internal candidate entry used during query execution.
#[derive(Debug, Clone)]
pub(crate) struct CandidateEntry {
    pub blob_id: BlobId,
    pub meta: Option<BlobMeta>,
    pub raw_distance: Option<f32>,
    pub wall_clock_ns: Option<u64>,
    pub causal_hops: Option<u32>,
}

/// Wrapper for min-heap ordering (lowest score gets evicted first).
pub(crate) struct HeapEntry {
    pub score: f64,
    pub blob_id: BlobId,
    pub meta: BlobMeta,
    pub signals: SignalScores,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: BinaryHeap is max-heap, we want min at top for eviction
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

impl HeapEntry {
    pub fn into_scored_blob(self) -> ScoredBlob {
        ScoredBlob {
            blob_id: self.blob_id,
            meta: self.meta,
            score: self.score,
            signals: self.signals,
        }
    }
}

/// Collect `HeapEntry` items into a sorted `Vec<ScoredBlob>` (highest score first).
pub(crate) fn heap_to_sorted(heap: alloc::collections::BinaryHeap<HeapEntry>) -> Vec<ScoredBlob> {
    let mut results: Vec<ScoredBlob> = heap.into_iter().map(|e| e.into_scored_blob()).collect();
    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    results
}
