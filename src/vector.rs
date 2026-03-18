use std::fmt::{self, Debug};

use crate::types::{MutInPlaceValue, TypeName, Value};

/// Fixed-dimension vector type for storing embeddings and feature vectors.
///
/// `FixedVec<N>` stores exactly `N` contiguous little-endian `f32` values with no
/// length header, giving `fixed_width() = Some(N * 4)` for optimal B-tree page layout.
///
/// # Usage
///
/// ```rust,ignore
/// use redb::{Database, TableDefinition, FixedVec, ReadableTable};
///
/// const EMBEDDINGS: TableDefinition<u64, FixedVec<384>> = TableDefinition::new("embeddings");
///
/// let db = Database::create("vectors.redb")?;
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_table(EMBEDDINGS)?;
///     let embedding: [f32; 384] = compute_embedding();
///     table.insert(&1u64, &embedding)?;
/// }
/// write_txn.commit()?;
/// ```
pub struct FixedVec<const N: usize>;

impl<const N: usize> Debug for FixedVec<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedVec<{N}>")
    }
}

impl<const N: usize> Value for FixedVec<N> {
    type SelfType<'a>
        = [f32; N]
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(N * std::mem::size_of::<f32>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> [f32; N]
    where
        Self: 'a,
    {
        assert_eq!(
            data.len(),
            N * std::mem::size_of::<f32>(),
            "FixedVec<{N}>: expected {} bytes, got {}",
            N * std::mem::size_of::<f32>(),
            data.len()
        );
        let mut result = [0.0f32; N];
        for (i, val) in result.iter_mut().enumerate() {
            let start = i * std::mem::size_of::<f32>();
            let bytes: [u8; 4] = data[start..start + 4].try_into().unwrap();
            *val = f32::from_le_bytes(bytes);
        }
        result
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a [f32; N]) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = Vec::with_capacity(N * std::mem::size_of::<f32>());
        for val in value {
            result.extend_from_slice(&val.to_le_bytes());
        }
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("redb::vec::FixedVec<{N}>"))
    }
}

impl<const N: usize> MutInPlaceValue for FixedVec<N> {
    type BaseRefType = [u8];

    fn initialize(data: &mut [u8]) {
        data.fill(0);
    }

    fn from_bytes_mut(data: &mut [u8]) -> &mut [u8] {
        data
    }
}

/// Dynamic-dimension vector type for storing variable-length vectors.
///
/// `DynVec` stores contiguous little-endian `f32` values with the dimension
/// inferred from the byte length (`dim = data.len() / 4`). This is useful when
/// different rows may have different vector dimensions.
///
/// # Usage
///
/// ```rust,ignore
/// use redb::{Database, TableDefinition, DynVec, ReadableTable};
///
/// const VECTORS: TableDefinition<u64, DynVec> = TableDefinition::new("vectors");
///
/// let db = Database::create("dynamic.redb")?;
/// let write_txn = db.begin_write()?;
/// {
///     let mut table = write_txn.open_table(VECTORS)?;
///     let vec3: Vec<f32> = vec![1.0, 2.0, 3.0];
///     table.insert(&1u64, &vec3)?;
/// }
/// write_txn.commit()?;
/// ```
pub struct DynVec;

impl Debug for DynVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DynVec")
    }
}

impl Value for DynVec {
    type SelfType<'a>
        = Vec<f32>
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Vec<f32>
    where
        Self: 'a,
    {
        assert_eq!(
            data.len() % std::mem::size_of::<f32>(),
            0,
            "DynVec: byte length {} is not a multiple of 4",
            data.len()
        );
        let dim = data.len() / std::mem::size_of::<f32>();
        let mut result = Vec::with_capacity(dim);
        for i in 0..dim {
            let start = i * std::mem::size_of::<f32>();
            let bytes: [u8; 4] = data[start..start + 4].try_into().unwrap();
            result.push(f32::from_le_bytes(bytes));
        }
        result
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Vec<f32>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = Vec::with_capacity(value.len() * std::mem::size_of::<f32>());
        for val in value {
            result.extend_from_slice(&val.to_le_bytes());
        }
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::vec::DynVec")
    }
}

/// Binary quantized vector type for ultra-compact storage.
///
/// `BinaryQuantized<N>` stores `N` bytes representing `N * 8` binary dimensions.
/// Each bit encodes whether the corresponding f32 dimension was positive (1) or
/// negative/zero (0). This gives 32x compression over full f32 vectors.
///
/// Use [`quantize_binary`] to convert f32 vectors to binary, and
/// [`hamming_distance`](crate::hamming_distance) to compare.
///
/// # Storage layout
///
/// `fixed_width() = Some(N)` -- exactly `N` bytes, no header.
///
/// # Usage
///
/// ```rust,ignore
/// use redb::{Database, TableDefinition, BinaryQuantized};
/// use redb::vector_ops::quantize_binary;
///
/// // 384 f32 dims -> 48 bytes (384/8)
/// const BQ_TABLE: TableDefinition<u64, BinaryQuantized<48>> = TableDefinition::new("bq_emb");
///
/// let embedding: [f32; 384] = compute_embedding();
/// let binary = quantize_binary(&embedding); // Vec<u8>, len=48
/// let bytes: [u8; 48] = binary.try_into().unwrap();
/// table.insert(&1u64, &bytes)?;
/// ```
pub struct BinaryQuantized<const N: usize>;

impl<const N: usize> Debug for BinaryQuantized<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BinaryQuantized<{N}>")
    }
}

impl<const N: usize> Value for BinaryQuantized<N> {
    type SelfType<'a>
        = [u8; N]
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; N]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(N)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> [u8; N]
    where
        Self: 'a,
    {
        data.try_into().unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a [u8; N]) -> [u8; N]
    where
        Self: 'b,
    {
        *value
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("redb::vec::BinaryQuantized<{N}>"))
    }
}

impl<const N: usize> MutInPlaceValue for BinaryQuantized<N> {
    type BaseRefType = [u8];

    fn initialize(data: &mut [u8]) {
        data.fill(0);
    }

    fn from_bytes_mut(data: &mut [u8]) -> &mut [u8] {
        data
    }
}

/// Scalar quantized vector with per-vector min/max scale factors.
///
/// `ScalarQuantized<N>` stores `N` dimensions as `u8` values (0..255) plus two
/// `f32` scale factors (`min_val`, `max_val`), giving approximately 4x compression
/// over full f32 storage with bounded quantization error.
///
/// # Storage layout
///
/// ```text
/// [min_val: f32 LE][max_val: f32 LE][q0: u8][q1: u8]...[q_{N-1}: u8]
/// ```
///
/// `fixed_width() = Some(8 + N)` -- 8 bytes header + N quantized values.
///
/// # Quantization formula
///
/// Encode: `q_i = round(255 * (x_i - min) / (max - min))`
/// Decode: `x_i = min + q_i * (max - min) / 255`
///
/// Use [`quantize_scalar`] and [`dequantize_scalar`] for conversion.
pub struct ScalarQuantized<const N: usize>;

impl<const N: usize> Debug for ScalarQuantized<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScalarQuantized<{N}>")
    }
}

/// Header size for scalar quantized vectors: `min_val` (4 bytes) + `max_val` (4 bytes).
const SQ_HEADER: usize = 8;

impl<const N: usize> Value for ScalarQuantized<N> {
    type SelfType<'a>
        = SQVec<N>
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(SQ_HEADER + N)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> SQVec<N>
    where
        Self: 'a,
    {
        assert_eq!(data.len(), SQ_HEADER + N);
        let min_val = f32::from_le_bytes(data[0..4].try_into().unwrap());
        let max_val = f32::from_le_bytes(data[4..8].try_into().unwrap());
        let mut codes = [0u8; N];
        codes.copy_from_slice(&data[SQ_HEADER..]);
        SQVec {
            min_val,
            max_val,
            codes,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a SQVec<N>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = Vec::with_capacity(SQ_HEADER + N);
        result.extend_from_slice(&value.min_val.to_le_bytes());
        result.extend_from_slice(&value.max_val.to_le_bytes());
        result.extend_from_slice(&value.codes);
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("redb::vec::ScalarQuantized<{N}>"))
    }
}

/// The decoded representation of a [`ScalarQuantized<N>`] value.
#[derive(Debug, Clone, PartialEq)]
pub struct SQVec<const N: usize> {
    /// Minimum value of the original f32 vector (scale floor).
    pub min_val: f32,
    /// Maximum value of the original f32 vector (scale ceiling).
    pub max_val: f32,
    /// Quantized codes: each u8 maps linearly to `[min_val, max_val]`.
    pub codes: [u8; N],
}

impl<const N: usize> SQVec<N> {
    /// Dequantizes the codes back to f32 values.
    #[inline]
    pub fn dequantize(&self) -> [f32; N] {
        let mut result = [0.0f32; N];
        let range = self.max_val - self.min_val;
        if range == 0.0 {
            result.fill(self.min_val);
        } else {
            let scale = range / 255.0;
            for (i, &code) in self.codes.iter().enumerate() {
                result[i] = self.min_val + f32::from(code) * scale;
            }
        }
        result
    }
}
