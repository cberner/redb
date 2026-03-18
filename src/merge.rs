use std::fmt::{self, Debug};

/// Trait for atomic read-modify-write merge operations on raw byte values.
///
/// Merge operators work at the raw byte level, enabling atomic updates without
/// full transaction boilerplate. The caller is responsible for correct serialization
/// of operand bytes to match the value type stored in the table.
///
/// # Return value
///
/// - `Some(bytes)` — the merged value to store
/// - `None` — delete the key
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
