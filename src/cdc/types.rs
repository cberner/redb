use crate::types::{Key, TypeName, Value};
use alloc::string::String;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::fmt;

// ---------------------------------------------------------------------------
// CdcConfig
// ---------------------------------------------------------------------------

/// Configuration for Change Data Capture.
///
/// Set on the database [`Builder`](crate::Builder) via `set_cdc()`.
/// When disabled (the default), CDC has zero overhead.
#[derive(Debug, Clone, Default)]
pub struct CdcConfig {
    /// Whether CDC is enabled.
    pub enabled: bool,
    /// Maximum number of committed transactions to retain in the CDC log.
    /// 0 means unlimited (entries are never pruned automatically).
    pub retention_max_txns: u64,
}

// ---------------------------------------------------------------------------
// ChangeOp
// ---------------------------------------------------------------------------

/// The type of mutation captured by a CDC event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChangeOp {
    /// A new key was inserted (no previous value existed).
    Insert = 0,
    /// An existing key was overwritten with a new value.
    Update = 1,
    /// A key was removed.
    Delete = 2,
}

impl ChangeOp {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Insert,
            1 => Self::Update,
            2 => Self::Delete,
            other => unreachable!("invalid ChangeOp discriminant: {other}"),
        }
    }
}

// ---------------------------------------------------------------------------
// CdcEvent -- in-memory accumulator (not persisted directly)
// ---------------------------------------------------------------------------

/// In-memory change event accumulated during a write transaction.
///
/// These are flushed to the CDC system table on commit.
pub(crate) struct CdcEvent {
    pub table_name: String,
    pub op: ChangeOp,
    pub key: Vec<u8>,
    pub new_value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// CdcKey -- system table key (fixed-width, 12 bytes)
// ---------------------------------------------------------------------------

/// Key for the CDC log system table.
///
/// Encoded as 12 bytes little-endian: `[transaction_id: u64][sequence: u32]`.
/// Ordering is handled by the [`Key::compare`] implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CdcKey {
    pub transaction_id: u64,
    pub sequence: u32,
}

impl CdcKey {
    pub const SERIALIZED_SIZE: usize = 12;

    pub fn new(transaction_id: u64, sequence: u32) -> Self {
        Self {
            transaction_id,
            sequence,
        }
    }

    fn to_le_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[..8].copy_from_slice(&self.transaction_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.sequence.to_le_bytes());
        buf
    }

    fn from_le_bytes(data: &[u8]) -> Self {
        let transaction_id = u64::from_le_bytes(data[..8].try_into().unwrap());
        let sequence = u32::from_le_bytes(data[8..12].try_into().unwrap());
        Self {
            transaction_id,
            sequence,
        }
    }
}

impl PartialOrd for CdcKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CdcKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.transaction_id
            .cmp(&other.transaction_id)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl Value for CdcKey {
    type SelfType<'a>
        = CdcKey
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; CdcKey::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::cdc::CdcKey")
    }
}

impl Key for CdcKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a = Self::from_le_bytes(data1);
        let b = Self::from_le_bytes(data2);
        a.cmp(&b)
    }
}

// ---------------------------------------------------------------------------
// CdcRecord -- system table value (variable-width)
// ---------------------------------------------------------------------------

const NONE_SENTINEL: u32 = u32::MAX;

/// Serialized CDC change record stored in the system table.
///
/// Binary layout:
/// ```text
/// [op: u8]
/// [table_name_len: u16 LE][table_name: N bytes]
/// [key_len: u32 LE][key: N bytes]
/// [new_val_len: u32 LE][new_val: N bytes]    -- 0xFFFFFFFF if None
/// [old_val_len: u32 LE][old_val: N bytes]    -- 0xFFFFFFFF if None
/// ```
#[derive(Clone)]
pub(crate) struct CdcRecord {
    pub op: ChangeOp,
    pub table_name: String,
    pub key: Vec<u8>,
    pub new_value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
}

impl fmt::Debug for CdcRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdcRecord")
            .field("op", &self.op)
            .field("table_name", &self.table_name)
            .field("key", &self.key)
            .field("new_value", &self.new_value)
            .field("old_value", &self.old_value)
            .finish()
    }
}

impl CdcRecord {
    pub fn from_event(event: &CdcEvent) -> Self {
        Self {
            op: event.op,
            table_name: event.table_name.clone(),
            key: event.key.clone(),
            new_value: event.new_value.clone(),
            old_value: event.old_value.clone(),
        }
    }

    fn serialized_size(&self) -> usize {
        1 // op
        + 2 + self.table_name.len() // table_name_len + table_name
        + 4 + self.key.len() // key_len + key
        + 4 + self.new_value.as_ref().map_or(0, Vec::len) // new_val_len + new_val
        + 4 + self.old_value.as_ref().map_or(0, Vec::len) // old_val_len + old_val
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.serialized_size());

        buf.push(self.op as u8);

        let name_len = u16::try_from(self.table_name.len()).unwrap_or(u16::MAX);
        buf.extend_from_slice(&name_len.to_le_bytes());
        buf.extend_from_slice(&self.table_name.as_bytes()[..usize::from(name_len)]);

        let key_len = u32::try_from(self.key.len()).unwrap_or(NONE_SENTINEL - 1);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&self.key[..key_len as usize]);

        match &self.new_value {
            Some(v) => {
                let len = u32::try_from(v.len()).unwrap_or(NONE_SENTINEL - 1);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&v[..len as usize]);
            }
            None => {
                buf.extend_from_slice(&NONE_SENTINEL.to_le_bytes());
            }
        }

        match &self.old_value {
            Some(v) => {
                let len = u32::try_from(v.len()).unwrap_or(NONE_SENTINEL - 1);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&v[..len as usize]);
            }
            None => {
                buf.extend_from_slice(&NONE_SENTINEL.to_le_bytes());
            }
        }

        buf
    }

    fn deserialize(data: &[u8]) -> Self {
        let mut pos = 0;

        let op = ChangeOp::from_u8(data[pos]);
        pos += 1;

        let name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;
        let table_name =
            String::from_utf8_lossy(&data[pos..pos + usize::from(name_len)]).into_owned();
        pos += usize::from(name_len);

        let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let key = data[pos..pos + key_len as usize].to_vec();
        pos += key_len as usize;

        let new_val_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let new_value = if new_val_len == NONE_SENTINEL {
            None
        } else {
            let v = data[pos..pos + new_val_len as usize].to_vec();
            pos += new_val_len as usize;
            Some(v)
        };

        let old_val_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let old_value = if old_val_len == NONE_SENTINEL {
            None
        } else {
            let v = data[pos..pos + old_val_len as usize].to_vec();
            // pos += old_val_len as usize; // not needed, last field
            let _ = v.len(); // suppress unused assignment warning
            Some(v)
        };

        Self {
            op,
            table_name,
            key,
            new_value,
            old_value,
        }
    }
}

impl Value for CdcRecord {
    type SelfType<'a>
        = CdcRecord
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::deserialize(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.serialize()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::cdc::CdcRecord")
    }
}

// ---------------------------------------------------------------------------
// ChangeStream -- public query result type
// ---------------------------------------------------------------------------

/// A single change record from the CDC log, returned by
/// [`ReadTransaction::read_cdc_since()`](crate::ReadTransaction::read_cdc_since).
#[derive(Debug, Clone)]
pub struct ChangeStream {
    /// Transaction that produced this change.
    pub transaction_id: u64,
    /// Sequence number within the transaction (0-based).
    pub sequence: u32,
    /// Type of mutation.
    pub op: ChangeOp,
    /// Name of the table that was modified.
    pub table_name: String,
    /// Serialized key bytes.
    pub key: Vec<u8>,
    /// Serialized new value bytes (`None` for [`ChangeOp::Delete`]).
    pub new_value: Option<Vec<u8>>,
    /// Serialized old value bytes (`None` for [`ChangeOp::Insert`]).
    pub old_value: Option<Vec<u8>>,
}

impl ChangeStream {
    pub(crate) fn from_key_record(key: CdcKey, record: CdcRecord) -> Self {
        Self {
            transaction_id: key.transaction_id,
            sequence: key.sequence,
            op: record.op,
            table_name: record.table_name,
            key: record.key,
            new_value: record.new_value,
            old_value: record.old_value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdc_key_round_trip() {
        let key = CdcKey::new(42, 7);
        let bytes = key.to_le_bytes();
        let decoded = CdcKey::from_le_bytes(&bytes);
        assert_eq!(key, decoded);
    }

    #[test]
    fn cdc_key_ordering() {
        let a = CdcKey::new(1, 0);
        let b = CdcKey::new(1, 1);
        let c = CdcKey::new(2, 0);
        assert!(a < b);
        assert!(b < c);

        // Key::compare handles ordering via deserialization
        let ab = a.to_le_bytes();
        let bb = b.to_le_bytes();
        let cb = c.to_le_bytes();
        assert_eq!(CdcKey::compare(&ab, &bb), core::cmp::Ordering::Less);
        assert_eq!(CdcKey::compare(&bb, &cb), core::cmp::Ordering::Less);
    }

    #[test]
    fn cdc_record_round_trip_insert() {
        let record = CdcRecord {
            op: ChangeOp::Insert,
            table_name: String::from("my_table"),
            key: vec![1, 2, 3],
            new_value: Some(vec![4, 5, 6]),
            old_value: None,
        };
        let bytes = record.serialize();
        let decoded = CdcRecord::deserialize(&bytes);
        assert_eq!(decoded.op, ChangeOp::Insert);
        assert_eq!(decoded.table_name, "my_table");
        assert_eq!(decoded.key, vec![1, 2, 3]);
        assert_eq!(decoded.new_value, Some(vec![4, 5, 6]));
        assert!(decoded.old_value.is_none());
    }

    #[test]
    fn cdc_record_round_trip_update() {
        let record = CdcRecord {
            op: ChangeOp::Update,
            table_name: String::from("t"),
            key: vec![10],
            new_value: Some(vec![20]),
            old_value: Some(vec![30]),
        };
        let bytes = record.serialize();
        let decoded = CdcRecord::deserialize(&bytes);
        assert_eq!(decoded.op, ChangeOp::Update);
        assert_eq!(decoded.new_value, Some(vec![20]));
        assert_eq!(decoded.old_value, Some(vec![30]));
    }

    #[test]
    fn cdc_record_round_trip_delete() {
        let record = CdcRecord {
            op: ChangeOp::Delete,
            table_name: String::from("x"),
            key: vec![99],
            new_value: None,
            old_value: Some(vec![100]),
        };
        let bytes = record.serialize();
        let decoded = CdcRecord::deserialize(&bytes);
        assert_eq!(decoded.op, ChangeOp::Delete);
        assert!(decoded.new_value.is_none());
        assert_eq!(decoded.old_value, Some(vec![100]));
    }

    #[test]
    fn cdc_record_empty_values() {
        let record = CdcRecord {
            op: ChangeOp::Insert,
            table_name: String::new(),
            key: vec![],
            new_value: Some(vec![]),
            old_value: None,
        };
        let bytes = record.serialize();
        let decoded = CdcRecord::deserialize(&bytes);
        assert_eq!(decoded.table_name, "");
        assert!(decoded.key.is_empty());
        assert_eq!(decoded.new_value, Some(vec![]));
    }
}
