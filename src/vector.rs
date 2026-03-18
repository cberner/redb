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
