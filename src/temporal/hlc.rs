use crate::types::{Key, TypeName, Value};
use std::cmp::Ordering;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Hybrid Logical Clock per Kulkarni et al. 2014.
///
/// Encodes wall-clock time with a logical counter for causal ordering.
/// The encoding guarantees that simple `u64` comparison gives correct
/// causal ordering, because the physical component occupies the high bits.
///
/// Layout (64 bits):
/// - Bits 63..16 (48 bits): physical timestamp in milliseconds since UNIX epoch
///   (covers ~8919 years from epoch, sufficient until year 10889)
/// - Bits 15..0 (16 bits): logical counter (0..65535)
///
/// Properties:
/// - Monotonically increasing within a process
/// - Causally ordered across processes via `merge()`
/// - Natural byte ordering (LE `u64`) = causal ordering
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HybridLogicalClock(u64);

const LOGICAL_BITS: u32 = 16;
const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1; // 0xFFFF
const PHYSICAL_SHIFT: u32 = LOGICAL_BITS;

/// Get the current wall-clock time in milliseconds since UNIX epoch.
fn wall_clock_ms() -> u64 {
    // as_millis() returns u128, but we only use 48 bits (covers ~8919 years).
    // Truncation is intentional and safe for any realistic timestamp.
    #[allow(clippy::cast_possible_truncation)]
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;
    ms
}

impl HybridLogicalClock {
    pub const ZERO: Self = Self(0);
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    /// Create an HLC from the current wall clock.
    /// The logical counter is set to 0 for a fresh timestamp.
    #[must_use]
    pub fn now() -> Self {
        Self(wall_clock_ms() << PHYSICAL_SHIFT)
    }

    /// Create an HLC from a raw `u64` (e.g., restored from persistent storage).
    #[must_use]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the raw `u64` representation (for persistent storage).
    #[must_use]
    pub fn to_raw(self) -> u64 {
        self.0
    }

    /// Extract the physical timestamp in milliseconds since UNIX epoch.
    #[must_use]
    pub fn physical_ms(self) -> u64 {
        self.0 >> PHYSICAL_SHIFT
    }

    /// Extract the logical counter.
    #[must_use]
    pub fn logical(self) -> u16 {
        #[allow(clippy::cast_possible_truncation)]
        let l = (self.0 & LOGICAL_MASK) as u16;
        l
    }

    /// Create from wall-clock nanoseconds. Logical counter set to 0.
    #[must_use]
    pub fn from_wall_ns(ns: u64) -> Self {
        let ms = ns / 1_000_000;
        Self(ms << PHYSICAL_SHIFT)
    }

    /// Create from explicit physical (ms) and logical components.
    #[must_use]
    pub fn from_parts(physical_ms: u64, logical: u16) -> Self {
        Self((physical_ms << PHYSICAL_SHIFT) | u64::from(logical))
    }

    /// Increment the logical counter. Used when multiple events occur
    /// within the same millisecond on the same process.
    ///
    /// If the logical counter would overflow (>65535), the physical
    /// component is incremented by 1ms and the logical counter resets to 0.
    #[must_use]
    pub fn tick(self) -> Self {
        if self.logical() < u16::MAX {
            Self(self.0 + 1)
        } else {
            // Overflow: advance physical by 1ms, reset logical to 0
            Self((self.physical_ms() + 1) << PHYSICAL_SHIFT)
        }
    }

    /// Advance this clock to be at least as recent as the current wall clock.
    /// Returns a new HLC that is strictly greater than `self` and reflects
    /// the current physical time.
    ///
    /// Call this at the start of each operation to maintain monotonicity.
    #[must_use]
    pub fn advance(self) -> Self {
        let now_ms = wall_clock_ms();
        let self_ms = self.physical_ms();

        if now_ms > self_ms {
            Self(now_ms << PHYSICAL_SHIFT)
        } else {
            self.tick()
        }
    }

    /// Merge with an observed HLC from another process/event.
    /// Returns a new HLC that is strictly greater than both `self` and `other`,
    /// and reflects the current wall clock if it has advanced.
    ///
    /// This is the core operation for maintaining causal ordering across
    /// distributed agents or processes.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        let now_ms = wall_clock_ms();
        let self_ms = self.physical_ms();
        let other_ms = other.physical_ms();

        let max_physical = now_ms.max(self_ms).max(other_ms);

        if max_physical == now_ms && now_ms > self_ms && now_ms > other_ms {
            Self(now_ms << PHYSICAL_SHIFT)
        } else if self_ms == other_ms && self_ms == max_physical {
            let max_logical = self.logical().max(other.logical());
            if max_logical < u16::MAX {
                Self::from_parts(max_physical, max_logical + 1)
            } else {
                Self((max_physical + 1) << PHYSICAL_SHIFT)
            }
        } else if self_ms == max_physical {
            self.tick()
        } else if other_ms == max_physical {
            other.tick()
        } else if self_ms == now_ms {
            self.tick()
        } else if other_ms == now_ms {
            other.tick()
        } else {
            Self(now_ms << PHYSICAL_SHIFT)
        }
    }
}

impl fmt::Debug for HybridLogicalClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HLC(physical={}ms, logical={})",
            self.physical_ms(),
            self.logical()
        )
    }
}

impl fmt::Display for HybridLogicalClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.physical_ms(), self.logical())
    }
}

// --- redb Key/Value trait implementations ---

impl Value for HybridLogicalClock {
    type SelfType<'a>
        = HybridLogicalClock
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(u64::from_le_bytes(data[..8].try_into().unwrap()))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::temporal::HLC")
    }
}

impl Key for HybridLogicalClock {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a = u64::from_le_bytes(data1[..8].try_into().unwrap());
        let b = u64::from_le_bytes(data2[..8].try_into().unwrap());
        a.cmp(&b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_physical_and_logical_extraction() {
        let hlc = HybridLogicalClock::from_parts(1000, 42);
        assert_eq!(hlc.physical_ms(), 1000);
        assert_eq!(hlc.logical(), 42);
    }

    #[test]
    fn hlc_tick_increments_logical() {
        let hlc = HybridLogicalClock::from_parts(1000, 0);
        let ticked = hlc.tick();
        assert_eq!(ticked.physical_ms(), 1000);
        assert_eq!(ticked.logical(), 1);
    }

    #[test]
    fn hlc_tick_overflow_advances_physical() {
        let hlc = HybridLogicalClock::from_parts(1000, u16::MAX);
        let ticked = hlc.tick();
        assert_eq!(ticked.physical_ms(), 1001);
        assert_eq!(ticked.logical(), 0);
    }

    #[test]
    fn hlc_ordering_is_causal() {
        let a = HybridLogicalClock::from_parts(100, 0);
        let b = HybridLogicalClock::from_parts(100, 1);
        let c = HybridLogicalClock::from_parts(101, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn hlc_from_wall_ns() {
        let ns = 1_700_000_000_000_000_000u64; // ~2023-11-14
        let hlc = HybridLogicalClock::from_wall_ns(ns);
        assert_eq!(hlc.physical_ms(), 1_700_000_000_000);
        assert_eq!(hlc.logical(), 0);
    }

    #[test]
    fn hlc_now_is_monotonic() {
        let a = HybridLogicalClock::now();
        let b = HybridLogicalClock::now();
        assert!(b >= a);
    }

    #[test]
    fn hlc_advance_monotonic() {
        let old = HybridLogicalClock::from_parts(0, 0);
        let advanced = old.advance();
        assert!(advanced > old);
    }

    #[test]
    fn hlc_merge_produces_greater() {
        let a = HybridLogicalClock::from_parts(1000, 5);
        let b = HybridLogicalClock::from_parts(1000, 10);
        let merged = a.merge(b);
        assert!(merged > a);
        assert!(merged > b);
    }

    #[test]
    fn hlc_raw_roundtrip() {
        let hlc = HybridLogicalClock::from_parts(123456, 789);
        let raw = hlc.to_raw();
        let recovered = HybridLogicalClock::from_raw(raw);
        assert_eq!(hlc, recovered);
    }

    #[test]
    fn hlc_key_trait_roundtrip() {
        let hlc = HybridLogicalClock::from_parts(42000, 7);
        let bytes = HybridLogicalClock::as_bytes(&hlc);
        let recovered = HybridLogicalClock::from_bytes(bytes.as_ref());
        assert_eq!(hlc, recovered);
    }

    #[test]
    fn hlc_key_compare() {
        let a = HybridLogicalClock::from_parts(100, 5);
        let b = HybridLogicalClock::from_parts(200, 0);
        let a_bytes = HybridLogicalClock::as_bytes(&a);
        let b_bytes = HybridLogicalClock::as_bytes(&b);
        assert_eq!(
            HybridLogicalClock::compare(a_bytes.as_ref(), b_bytes.as_ref()),
            Ordering::Less
        );
    }
}
