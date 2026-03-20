use alloc::vec;
use alloc::vec::Vec;
use core::fmt::{self, Debug};

/// Trait for atomic read-modify-write merge operations on raw byte values.
///
/// Merge operators work at the raw byte level, enabling atomic updates without
/// full transaction boilerplate. The caller is responsible for correct serialization
/// of operand bytes to match the value type stored in the table.
///
/// # Return value
///
/// - `Some(bytes)` -- the merged value to store
/// - `None` -- delete the key
pub trait MergeOperator: Send + Sync {
    /// Merge `operand` into the `existing` value (if present), returning the new value.
    ///
    /// `key` is provided for context (e.g., per-key merge logic).
    /// `existing` is `None` when the key does not yet exist in the table.
    /// Return `None` to delete the key.
    fn merge(&self, key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>>;
}

/// Adds two little-endian encoded numeric values.
///
/// Supports 1, 2, 4, and 8-byte widths (u8/i8 through u64/i64, f32, f64).
/// If the key does not exist, the operand is used as the initial value.
///
/// # Panics
///
/// Panics if `existing` and `operand` have different lengths, or if the length
/// is not 1, 2, 4, or 8.
#[derive(Clone, Copy)]
pub struct NumericAdd;

impl Debug for NumericAdd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NumericAdd")
    }
}

impl MergeOperator for NumericAdd {
    fn merge(&self, _key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let Some(existing) = existing else {
            return Some(operand.to_vec());
        };
        assert_eq!(
            existing.len(),
            operand.len(),
            "NumericAdd: existing ({}) and operand ({}) byte widths must match",
            existing.len(),
            operand.len()
        );
        let result = match operand.len() {
            1 => {
                let a = existing[0];
                let b = operand[0];
                vec![a.wrapping_add(b)]
            }
            2 => {
                let a = u16::from_le_bytes(existing.try_into().unwrap());
                let b = u16::from_le_bytes(operand.try_into().unwrap());
                a.wrapping_add(b).to_le_bytes().to_vec()
            }
            4 => {
                let a = u32::from_le_bytes(existing.try_into().unwrap());
                let b = u32::from_le_bytes(operand.try_into().unwrap());
                a.wrapping_add(b).to_le_bytes().to_vec()
            }
            8 => {
                let a = u64::from_le_bytes(existing.try_into().unwrap());
                let b = u64::from_le_bytes(operand.try_into().unwrap());
                a.wrapping_add(b).to_le_bytes().to_vec()
            }
            n => panic!("NumericAdd: unsupported byte width {n} (expected 1, 2, 4, or 8)"),
        };
        Some(result)
    }
}

/// Keeps the maximum of two little-endian encoded unsigned numeric values.
///
/// Supports 1, 2, 4, and 8-byte widths. Comparison is unsigned.
/// If the key does not exist, the operand is used as the initial value.
///
/// # Panics
///
/// Panics if byte widths don't match or width is not 1, 2, 4, or 8.
#[derive(Clone, Copy)]
pub struct NumericMax;

impl Debug for NumericMax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NumericMax")
    }
}

impl MergeOperator for NumericMax {
    fn merge(&self, _key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let Some(existing) = existing else {
            return Some(operand.to_vec());
        };
        assert_eq!(
            existing.len(),
            operand.len(),
            "NumericMax: existing ({}) and operand ({}) byte widths must match",
            existing.len(),
            operand.len()
        );
        let use_operand = match operand.len() {
            1 => operand[0] > existing[0],
            2 => {
                let a = u16::from_le_bytes(existing.try_into().unwrap());
                let b = u16::from_le_bytes(operand.try_into().unwrap());
                b > a
            }
            4 => {
                let a = u32::from_le_bytes(existing.try_into().unwrap());
                let b = u32::from_le_bytes(operand.try_into().unwrap());
                b > a
            }
            8 => {
                let a = u64::from_le_bytes(existing.try_into().unwrap());
                let b = u64::from_le_bytes(operand.try_into().unwrap());
                b > a
            }
            n => panic!("NumericMax: unsupported byte width {n} (expected 1, 2, 4, or 8)"),
        };
        if use_operand {
            Some(operand.to_vec())
        } else {
            Some(existing.to_vec())
        }
    }
}

/// Keeps the minimum of two little-endian encoded unsigned numeric values.
///
/// Supports 1, 2, 4, and 8-byte widths. Comparison is unsigned.
/// If the key does not exist, the operand is used as the initial value.
///
/// # Panics
///
/// Panics if byte widths don't match or width is not 1, 2, 4, or 8.
#[derive(Clone, Copy)]
pub struct NumericMin;

impl Debug for NumericMin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NumericMin")
    }
}

impl MergeOperator for NumericMin {
    fn merge(&self, _key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let Some(existing) = existing else {
            return Some(operand.to_vec());
        };
        assert_eq!(
            existing.len(),
            operand.len(),
            "NumericMin: existing ({}) and operand ({}) byte widths must match",
            existing.len(),
            operand.len()
        );
        let use_operand = match operand.len() {
            1 => operand[0] < existing[0],
            2 => {
                let a = u16::from_le_bytes(existing.try_into().unwrap());
                let b = u16::from_le_bytes(operand.try_into().unwrap());
                b < a
            }
            4 => {
                let a = u32::from_le_bytes(existing.try_into().unwrap());
                let b = u32::from_le_bytes(operand.try_into().unwrap());
                b < a
            }
            8 => {
                let a = u64::from_le_bytes(existing.try_into().unwrap());
                let b = u64::from_le_bytes(operand.try_into().unwrap());
                b < a
            }
            n => panic!("NumericMin: unsupported byte width {n} (expected 1, 2, 4, or 8)"),
        };
        if use_operand {
            Some(operand.to_vec())
        } else {
            Some(existing.to_vec())
        }
    }
}

/// Bitwise OR of fixed-width byte slices.
///
/// Both existing and operand must have the same length.
/// If the key does not exist, the operand is used as the initial value.
///
/// # Panics
///
/// Panics if existing and operand have different lengths.
#[derive(Clone, Copy)]
pub struct BitwiseOr;

impl Debug for BitwiseOr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BitwiseOr")
    }
}

impl MergeOperator for BitwiseOr {
    fn merge(&self, _key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let Some(existing) = existing else {
            return Some(operand.to_vec());
        };
        assert_eq!(
            existing.len(),
            operand.len(),
            "BitwiseOr: existing ({}) and operand ({}) byte lengths must match",
            existing.len(),
            operand.len()
        );
        let result: Vec<u8> = existing
            .iter()
            .zip(operand.iter())
            .map(|(a, b)| a | b)
            .collect();
        Some(result)
    }
}

/// Appends operand bytes to the existing value.
///
/// If the key does not exist, the operand is used as the initial value.
#[derive(Clone, Copy)]
pub struct BytesAppend;

impl Debug for BytesAppend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BytesAppend")
    }
}

impl MergeOperator for BytesAppend {
    fn merge(&self, _key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let Some(existing) = existing else {
            return Some(operand.to_vec());
        };
        let mut result = Vec::with_capacity(existing.len() + operand.len());
        result.extend_from_slice(existing);
        result.extend_from_slice(operand);
        Some(result)
    }
}

/// A merge operator backed by a closure.
///
/// Created via [`merge_fn()`].
pub struct FnMergeOperator<F>
where
    F: Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>> + Send + Sync,
{
    f: F,
}

impl<F> Debug for FnMergeOperator<F>
where
    F: Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>> + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FnMergeOperator")
    }
}

impl<F> MergeOperator for FnMergeOperator<F>
where
    F: Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>> + Send + Sync,
{
    fn merge(&self, key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        (self.f)(key, existing, operand)
    }
}

/// Creates a [`MergeOperator`] from a closure.
///
/// # Example
///
/// ```rust,ignore
/// use shodh_redb::merge_fn;
///
/// let op = merge_fn(|_key, existing, operand| {
///     // Custom merge: multiply existing by operand
///     let a = existing.map_or(1u64, |b| u64::from_le_bytes(b.try_into().unwrap()));
///     let b = u64::from_le_bytes(operand.try_into().unwrap());
///     Some((a * b).to_le_bytes().to_vec())
/// });
/// ```
pub fn merge_fn<F>(f: F) -> FnMergeOperator<F>
where
    F: Fn(&[u8], Option<&[u8]>, &[u8]) -> Option<Vec<u8>> + Send + Sync,
{
    FnMergeOperator { f }
}
