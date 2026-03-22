use crate::sealed::Sealed;
use crate::table::{Range, ReadableTable, ReadableTableMetadata, TableStats};
use crate::tree_store::AccessGuard;
use crate::types::{Key, TypeName, Value};
use crate::{ReadOnlyTable, Result, Table, TableHandle};
use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const EXPIRY_HEADER_SIZE: usize = 8;

fn now_millis() -> u64 {
    // Truncation is safe: u64 millis covers ~584 million years from epoch
    #[allow(clippy::cast_possible_truncation)]
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64;
    ms
}

fn is_expired(expires_at_ms: u64) -> bool {
    expires_at_ms != 0 && expires_at_ms <= now_millis()
}

fn read_expiry(data: &[u8]) -> u64 {
    let bytes: [u8; 8] = data
        .get(..EXPIRY_HEADER_SIZE)
        .and_then(|s| s.try_into().ok())
        .unwrap_or([0u8; 8]);
    u64::from_le_bytes(bytes)
}

// ---------------------------------------------------------------------------
// TtlValueOf<V> -- internal Value wrapper: [u64 LE expiry][V bytes]
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
/// use shodh_redb::TtlTableDefinition;
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

// ---------------------------------------------------------------------------
// TtlAccessGuard -- strips the 8-byte expiry header
// ---------------------------------------------------------------------------

/// Access guard for TTL table values. Transparently strips the expiry header.
pub struct TtlAccessGuard<'a, V: Value + 'static> {
    /// Raw bytes: [u64 LE expiry][value_bytes]
    raw: Vec<u8>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<V: Value + 'static> TtlAccessGuard<'_, V> {
    fn from_raw(raw: Vec<u8>) -> Self {
        Self {
            raw,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    /// Returns the deserialized value (without the expiry header).
    pub fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.raw[EXPIRY_HEADER_SIZE..])
    }

    /// Returns the expiry timestamp in milliseconds since UNIX epoch, or 0 for no expiry.
    pub fn expires_at_ms(&self) -> u64 {
        read_expiry(&self.raw)
    }
}

/// Build a `TtlAccessGuard` from an inner `AccessGuard<TtlValueOf<V>>`, consuming it.
fn extract_ttl_guard<'a, V: Value + 'static>(
    inner: AccessGuard<'_, TtlValueOf<V>>,
) -> TtlAccessGuard<'a, V> {
    let val_ref: TtlValueRef<'_> = inner.value();
    let raw = val_ref.data.to_vec();
    TtlAccessGuard::from_raw(raw)
}

/// Build a `TtlAccessGuard` from an inner guard, returning `None` if expired.
fn extract_ttl_guard_if_alive<'a, V: Value + 'static>(
    inner: AccessGuard<'_, TtlValueOf<V>>,
) -> Option<TtlAccessGuard<'a, V>> {
    let val_ref: TtlValueRef<'_> = inner.value();
    let expires = val_ref.expires_at_ms();
    if is_expired(expires) {
        None
    } else {
        let raw = val_ref.data.to_vec();
        Some(TtlAccessGuard::from_raw(raw))
    }
}

// ---------------------------------------------------------------------------
// Encode helpers
// ---------------------------------------------------------------------------

fn encode_ttl_value<V: Value>(expires_at_ms: u64, value: &V::SelfType<'_>) -> Vec<u8> {
    let val_bytes = V::as_bytes(value);
    let val_ref = val_bytes.as_ref();
    let mut buf = Vec::with_capacity(EXPIRY_HEADER_SIZE + val_ref.len());
    buf.extend_from_slice(&expires_at_ms.to_le_bytes());
    buf.extend_from_slice(val_ref);
    buf
}

// ---------------------------------------------------------------------------
// TtlTable (writable)
// ---------------------------------------------------------------------------

/// A writable table with per-key TTL support.
///
/// Obtained from `WriteTransaction::open_ttl_table()`.
pub struct TtlTable<'txn, K: Key + 'static, V: Value + 'static> {
    inner: Table<'txn, K, TtlValueOf<V>>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for TtlTable<'_, K, V> {
    fn name(&self) -> &str {
        self.inner.name()
    }
}

impl<K: Key + 'static, V: Value + 'static> Sealed for TtlTable<'_, K, V> {}

impl<'txn, K: Key + 'static, V: Value + 'static> TtlTable<'txn, K, V> {
    pub(crate) fn new(inner: Table<'txn, K, TtlValueOf<V>>) -> Self {
        Self { inner }
    }

    /// Insert a key-value pair that never expires.
    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<Option<TtlAccessGuard<'_, V>>> {
        let encoded = encode_ttl_value::<V>(0, value.borrow());
        let ttl_ref = TtlValueRef { data: &encoded };
        let old = self.inner.insert(key, &ttl_ref)?;
        Ok(old.map(extract_ttl_guard))
    }

    /// Insert a key-value pair with a time-to-live duration.
    ///
    /// After `ttl` elapses, the entry will be treated as expired:
    /// - `get()` returns `None`
    /// - Iterators skip it
    /// - `purge_expired()` removes it from disk
    pub fn insert_with_ttl<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
        ttl: Duration,
    ) -> Result<Option<TtlAccessGuard<'_, V>>> {
        // Truncation is safe: u64 millis covers ~584 million years
        #[allow(clippy::cast_possible_truncation)]
        let expires_at = now_millis() + ttl.as_millis() as u64;
        let encoded = encode_ttl_value::<V>(expires_at, value.borrow());
        let ttl_ref = TtlValueRef { data: &encoded };
        let old = self.inner.insert(key, &ttl_ref)?;
        Ok(old.map(extract_ttl_guard))
    }

    /// Returns the value for the given key, or `None` if absent or expired.
    pub fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<TtlAccessGuard<'_, V>>> {
        match self.inner.get(key)? {
            Some(guard) => Ok(extract_ttl_guard_if_alive(guard)),
            None => Ok(None),
        }
    }

    /// Removes the given key, returning the old value if it existed (even if expired).
    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<TtlAccessGuard<'_, V>>> {
        let old = self.inner.remove(key)?;
        Ok(old.map(extract_ttl_guard))
    }

    /// Remove all expired entries from disk.
    ///
    /// Returns the number of entries removed.
    pub fn purge_expired(&mut self) -> Result<u64> {
        let before = self.inner.len()?;
        self.inner
            .retain(|_key, value: TtlValueRef<'_>| !is_expired(value.expires_at_ms()))?;
        let after = self.inner.len()?;
        Ok(before - after)
    }

    /// Returns a double-ended iterator over non-expired entries in the given range.
    pub fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<TtlRange<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let inner = self.inner.range(range)?;
        Ok(TtlRange {
            inner,
            _value_type: PhantomData,
        })
    }

    /// Returns an iterator over all non-expired entries.
    pub fn iter(&self) -> Result<TtlRange<'_, K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }

    /// Returns the total number of entries (including expired but not yet purged).
    pub fn len_with_expired(&self) -> Result<u64> {
        self.inner.len()
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTableMetadata for TtlTable<'_, K, V> {
    fn stats(&self) -> Result<TableStats> {
        self.inner.stats()
    }

    /// Returns the total number of entries including expired ones.
    /// Use `len_with_expired()` explicitly, or iterate to count live entries.
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
}

// ---------------------------------------------------------------------------
// TtlRange -- iterator that skips expired entries
// ---------------------------------------------------------------------------

/// Iterator over non-expired entries in a TTL table.
pub struct TtlRange<'a, K: Key + 'static, V: Value + 'static> {
    inner: Range<'a, K, TtlValueOf<V>>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> Iterator for TtlRange<'a, K, V> {
    type Item = Result<(AccessGuard<'a, K>, TtlAccessGuard<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Err(e) => return Some(Err(e)),
                Ok((key_guard, val_guard)) => {
                    let val_ref: TtlValueRef<'_> = val_guard.value();
                    if is_expired(val_ref.expires_at_ms()) {
                        continue;
                    }
                    let raw = val_ref.data.to_vec();
                    let ttl_guard = TtlAccessGuard::from_raw(raw);
                    return Some(Ok((key_guard, ttl_guard)));
                }
            }
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> DoubleEndedIterator for TtlRange<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next_back()? {
                Err(e) => return Some(Err(e)),
                Ok((key_guard, val_guard)) => {
                    let val_ref: TtlValueRef<'_> = val_guard.value();
                    if is_expired(val_ref.expires_at_ms()) {
                        continue;
                    }
                    let raw = val_ref.data.to_vec();
                    let ttl_guard = TtlAccessGuard::from_raw(raw);
                    return Some(Ok((key_guard, ttl_guard)));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ReadOnlyTtlTable
// ---------------------------------------------------------------------------

/// A read-only TTL table, obtained from `ReadTransaction::open_ttl_table()`.
pub struct ReadOnlyTtlTable<K: Key + 'static, V: Value + 'static> {
    inner: ReadOnlyTable<K, TtlValueOf<V>>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for ReadOnlyTtlTable<K, V> {
    fn name(&self) -> &str {
        self.inner.name()
    }
}

impl<K: Key + 'static, V: Value + 'static> Sealed for ReadOnlyTtlTable<K, V> {}

impl<K: Key + 'static, V: Value + 'static> ReadOnlyTtlTable<K, V> {
    pub(crate) fn new(inner: ReadOnlyTable<K, TtlValueOf<V>>) -> Self {
        Self { inner }
    }

    /// Returns the value for the given key, or `None` if absent or expired.
    pub fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<TtlAccessGuard<'_, V>>> {
        match self.inner.get(key)? {
            Some(guard) => Ok(extract_ttl_guard_if_alive(guard)),
            None => Ok(None),
        }
    }

    /// Returns a double-ended iterator over non-expired entries in the given range.
    pub fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<TtlRange<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        let inner = self.inner.range(range)?;
        Ok(TtlRange {
            inner,
            _value_type: PhantomData,
        })
    }

    /// Returns an iterator over all non-expired entries.
    pub fn iter(&self) -> Result<TtlRange<'_, K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }

    /// Returns the total number of entries (including expired but not yet purged).
    pub fn len_with_expired(&self) -> Result<u64> {
        self.inner.len()
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTableMetadata for ReadOnlyTtlTable<K, V> {
    fn stats(&self) -> Result<TableStats> {
        self.inner.stats()
    }

    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
}
