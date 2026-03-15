use crate::sealed::Sealed;
use crate::types::{Key, TypeName, Value};
use crate::TableHandle;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

const EXPIRY_HEADER_SIZE: usize = 8;

fn now_millis() -> u64 {
    // Truncation is safe: u64 millis covers ~584 million years from epoch
    #[allow(clippy::cast_possible_truncation)]
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64;
    ms
}

fn is_expired(expires_at_ms: u64) -> bool {
    expires_at_ms != 0 && expires_at_ms <= now_millis()
}

fn read_expiry(data: &[u8]) -> u64 {
    let bytes: [u8; 8] = data[..EXPIRY_HEADER_SIZE]
        .try_into()
        .expect("TTL value too short");
    u64::from_le_bytes(bytes)
}

// ---------------------------------------------------------------------------
// TtlValueOf<V> — internal Value wrapper: [u64 LE expiry][V bytes]
// ---------------------------------------------------------------------------

pub(crate) struct TtlValueOf<V: Value> {
    _phantom: PhantomData<V>,
}

impl<V: Value> Debug for TtlValueOf<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TtlValueOf<{:?}>", V::type_name())
    }
}

/// Reference type returned by `TtlValueOf::from_bytes`.
/// Holds the raw slice so we can hand out both expiry and inner value.
#[derive(Debug)]
pub(crate) struct TtlValueRef<'a> {
    pub(crate) data: &'a [u8],
}

impl TtlValueRef<'_> {
    pub(crate) fn expires_at_ms(&self) -> u64 {
        read_expiry(self.data)
    }
}

impl<V: Value> Value for TtlValueOf<V> {
    type SelfType<'a>
        = TtlValueRef<'a>
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        V::fixed_width().map(|w| w + EXPIRY_HEADER_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> TtlValueRef<'a>
    where
        Self: 'a,
    {
        TtlValueRef { data }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        value.data.to_vec()
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("ttl::{}", V::type_name().name()))
    }
}

// ---------------------------------------------------------------------------
// TtlTableDefinition
// ---------------------------------------------------------------------------

/// Definition for a TTL-enabled table. Use with `open_ttl_table()`.
///
/// # Example
///
/// ```ignore
/// use redb::TtlTableDefinition;
///
/// const CACHE: TtlTableDefinition<&str, &[u8]> = TtlTableDefinition::new("cache");
/// ```
pub struct TtlTableDefinition<K: Key + 'static, V: Value + 'static> {
    name: &'static str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> TtlTableDefinition<K, V> {
    pub const fn new(name: &'static str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the internal `TableDefinition` used for storage.
    pub(crate) fn inner_def(&self) -> crate::TableDefinition<'static, K, TtlValueOf<V>> {
        crate::TableDefinition::new(self.name)
    }
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for TtlTableDefinition<K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key + 'static, V: Value + 'static> Sealed for TtlTableDefinition<K, V> {}

impl<K: Key + 'static, V: Value + 'static> Clone for TtlTableDefinition<K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Value + 'static> Copy for TtlTableDefinition<K, V> {}

impl<K: Key + 'static, V: Value + 'static> std::fmt::Display for TtlTableDefinition<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TtlTable<{}, {}>(\"{}\")",
            K::type_name().name(),
            V::type_name().name(),
            self.name,
        )
    }
}
