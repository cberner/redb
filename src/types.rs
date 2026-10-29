use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::string::ToString;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::convert::TryInto;
use core::fmt::Debug;
use core::mem::size_of;
use core::num::{
    NonZeroI8, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI128, NonZeroU8, NonZeroU16, NonZeroU32,
    NonZeroU64, NonZeroU128,
};
#[cfg(feature = "chrono_v0_4")]
mod chrono_v0_4;
#[cfg(feature = "uuid")]
mod uuid;

#[derive(Eq, PartialEq, Clone, Debug)]
enum TypeClassification {
    Internal,
    UserDefined,
    // Used by variable width tuple encoding in version 3.0 and newer. This differentiates the encoding
    // from the old encoding used previously
    Internal2,
    // Used by composite types written by version 4.2 and newer
    Internal3,
}

impl TypeClassification {
    fn to_byte(&self) -> u8 {
        match self {
            TypeClassification::Internal => 1,
            TypeClassification::UserDefined => 2,
            TypeClassification::Internal2 => 3,
            TypeClassification::Internal3 => 4,
        }
    }

    fn from_byte(value: u8) -> Self {
        match value {
            1 => TypeClassification::Internal,
            2 => TypeClassification::UserDefined,
            3 => TypeClassification::Internal2,
            4 => TypeClassification::Internal3,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypeName {
    classification: TypeClassification,
    name: String,
    // In-memory only; never serialized. For a composite (`Option`, `Vec`, tuple, array), this
    // records the classification that older redb versions stored, so that their tables still
    // match on open. That old spelling is ambiguous for colliding user and built-in composites.
    // `None` for leaf types, flat user-defined types, and deserialized names.
    legacy_classification: Option<TypeClassification>,
}

// Equality is on-disk identity: the auxiliary `legacy_classification` never participates, so a
// freshly built name compares equal to the same name after a serialize/deserialize round trip.
impl PartialEq for TypeName {
    fn eq(&self, other: &Self) -> bool {
        self.classification == other.classification && self.name == other.name
    }
}

impl Eq for TypeName {}

impl TypeName {
    /// It is recommended that `name` be prefixed with the crate name to minimize the chance of
    /// it coliding with another user defined type
    pub fn new(name: &str) -> Self {
        Self {
            classification: TypeClassification::UserDefined,
            name: name.to_string(),
            legacy_classification: None,
        }
    }

    pub(crate) fn internal(name: &str) -> Self {
        Self {
            classification: TypeClassification::Internal,
            name: name.to_string(),
            legacy_classification: None,
        }
    }

    pub(crate) fn internal2(name: &str) -> Self {
        Self {
            classification: TypeClassification::Internal2,
            name: name.to_string(),
            legacy_classification: None,
        }
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.name.len() + 1);
        result.push(self.classification.to_byte());
        result.extend_from_slice(self.name.as_bytes());
        result
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let classification = TypeClassification::from_byte(bytes[0]);
        let name = core::str::from_utf8(&bytes[1..]).unwrap().to_string();

        Self {
            classification,
            name,
            legacy_classification: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn is_user_defined(&self) -> bool {
        matches!(self.classification, TypeClassification::UserDefined)
    }

    // Classify a composite as user-defined when it wraps a user-defined type, and Internal3
    // otherwise. The previous natural classification is retained so older databases still match.
    pub(crate) fn into_composite(mut self, user_defined: bool) -> Self {
        debug_assert!(matches!(
            &self.classification,
            TypeClassification::Internal | TypeClassification::Internal2
        ));
        self.legacy_classification = Some(self.classification.clone());
        if user_defined {
            self.classification = TypeClassification::UserDefined;
        } else {
            self.classification = TypeClassification::Internal3;
        }
        self
    }

    // Whether `stored` (read from an existing database) is the spelling an older redb version
    // wrote for this same composite. Composites were classified `Internal` (or `Internal2` for
    // variable-width tuples); accepting that spelling keeps those databases readable, but cannot
    // distinguish legacy colliding user composites from built-in composites. A flat user-defined
    // type has no recorded legacy classification.
    pub(crate) fn matches_legacy(&self, stored: &TypeName) -> bool {
        if let Some(natural) = &self.legacy_classification {
            *natural == stored.classification && self.name == stored.name
        } else {
            false
        }
    }
}

/// Failure to decode bytes with [`Value::try_from_bytes`].
pub type ValueError = Box<dyn core::error::Error + Send + Sync>;

#[derive(Debug)]
struct ValueDecodeError {
    message: String,
}

impl core::fmt::Display for ValueDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl core::error::Error for ValueDecodeError {}

#[cold]
pub(crate) fn value_error(message: impl core::fmt::Display) -> ValueError {
    Box::new(ValueDecodeError {
        message: message.to_string(),
    })
}

/// Types that implement this trait can be used as values in a redb table
pub trait Value: Debug {
    /// `SelfType<'a>` must be the same type as Self with all lifetimes replaced with 'a
    type SelfType<'a>: Debug + 'a
    where
        Self: 'a;

    type AsBytes<'a>: AsRef<[u8]> + 'a
    where
        Self: 'a;

    /// A byte string that [`as_bytes()`](Self::as_bytes) never returns, if the type declares one.
    ///
    /// Implementations must ensure that `as_bytes()` never returns it, for any value. A fixed
    /// width type's niche must be exactly [`fixed_width()`](Self::fixed_width) bytes long; a
    /// variable width type's may be any length, including empty. The default, `None`, declares
    /// no niche.
    ///
    /// Container types may use it as an encoding that no value of `Self` occupies: `Option<Self>`,
    /// for example, encodes `None` as it, in place of a tag byte. Declaring a niche changes the
    /// type name of `Option<Self>`, so it is not equivalent to `Option` of the type without one.
    const NICHE: Option<&'static [u8]> = None;

    /// Width of a fixed type, or None for variable width
    fn fixed_width() -> Option<usize>;

    /// Deserializes data.
    /// Implementations may return a view over data, or an owned type.
    ///
    /// This method may panic when `data` is not a valid encoding. It will be removed in redb
    /// 5.0. Implement [`try_from_bytes`](Self::try_from_bytes), and decode with that method when
    /// a panic is not acceptable.
    ///
    /// Note: Implementations may assume that `data` is the return value of `as_bytes(&v)` for some `v` of type `Self`.
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a;

    /// Deserializes data, returning an error when `data` is not a valid encoding of `Self`.
    ///
    /// The default calls [`from_bytes`](Self::from_bytes), so it panics when that method panics.
    /// Override it for panic-safe decoding. It will be required in redb 5.0, when `from_bytes`
    /// is removed.
    ///
    /// `as_bytes` and `try_from_bytes` must be inverses for every value `as_bytes` produces.
    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Self::SelfType<'a>, ValueError>
    where
        Self: 'a,
    {
        Ok(Self::from_bytes(data))
    }

    /// Serialize the value to a slice
    ///
    /// Note: Implementations must ensure that `as_bytes()` and `from_bytes()` are inverses.
    /// Specifically, for any value `v` and `data` returned from `as_bytes(&v)`,
    /// `as_bytes(from_bytes(data))` must equal `data`.
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b;

    /// Globally unique identifier for this type
    fn type_name() -> TypeName;
}

/// Implementing this trait indicates that the type can be mutated in-place as a &mut [u8].
/// This enables the `.insert_reserve()` method on Table
pub trait MutInPlaceValue: Value {
    /// The base type such that &mut [u8] can be safely transmuted to `&mut BaseRefType`
    type BaseRefType: Debug + ?Sized;

    /// Initialize `data` to a valid value. This method will be called (at some point, not necessarily immediately)
    /// before `from_bytes_mut()` is called on a slice.
    ///
    /// Note: There must exist a value `v` of type `Self` such that `as_bytes(&v)` equals `data`.
    fn initialize(data: &mut [u8]);

    fn from_bytes_mut(data: &mut [u8]) -> &mut Self::BaseRefType;
}

impl MutInPlaceValue for &[u8] {
    type BaseRefType = [u8];

    fn initialize(_data: &mut [u8]) {
        // no-op. All values are valid.
    }

    fn from_bytes_mut(data: &mut [u8]) -> &mut Self::BaseRefType {
        data
    }
}

/// Trait which allows the type to be used as a key in a redb table
pub trait Key: Value {
    /// Compare data1 with data2.
    ///
    /// The implementation must ensure there is a total order.
    ///
    /// This method may panic when either encoding is invalid. It will be removed in redb 5.0.
    /// Implement [`try_compare`](Self::try_compare), and compare with that method when a panic
    /// is not acceptable.
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;

    /// Compares the two encodings, returning an error when either is not a valid encoding of `Self`.
    ///
    /// The default calls [`compare`](Self::compare), so it panics when that method panics.
    /// Override it for panic-safe comparison. It will be required in redb 5.0, when `compare`
    /// is removed.
    fn try_compare(data1: &[u8], data2: &[u8]) -> core::result::Result<Ordering, ValueError> {
        Ok(Self::compare(data1, data2))
    }

    /// Returns a separator between `left` and `right`.
    ///
    /// `left` and `right` are encoded values for which [`compare()`](Self::compare) returns
    /// [`Ordering::Less`]. The result must be an encoding of `Self` that is greater than or equal
    /// to `left`, and less than `right`, under [`compare()`](Self::compare).
    ///
    /// The result is stored in the btree's internal nodes to route lookups. The shorter it is,
    /// the more children fit in each internal node, which makes the tree shallower and cheaper to
    /// search. It should never be longer than `left`, which is always available to fall back to.
    ///
    /// Returning [`Cow::Owned`] allows separators synthesized from the inputs, rather than
    /// borrowed from them.
    ///
    /// The default implementation returns `left`, which is always valid.
    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        debug_assert!(Self::compare(left, right).is_lt());
        Cow::Borrowed(left)
    }

    /// The encoding of the smallest value of `Self`, or `None` when it has no smallest value.
    ///
    /// Providing this is optional. It lets container types holding `Self` shorten their
    /// separators.
    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        None
    }
}

impl Value for () {
    type SelfType<'a>
        = ()
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(0)
    }

    #[allow(clippy::unused_unit, clippy::semicolon_if_nothing_returned)]
    fn from_bytes<'a>(data: &'a [u8]) -> ()
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<(), ValueError>
    where
        Self: 'a,
    {
        if data.is_empty() {
            Ok(())
        } else {
            Err(value_error("invalid () encoding"))
        }
    }

    #[allow(clippy::ignored_unit_patterns)]
    fn as_bytes<'a, 'b: 'a>(_: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'b,
    {
        &[]
    }

    fn type_name() -> TypeName {
        TypeName::internal("()")
    }
}

impl Key for () {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::try_compare(data1, data2).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_compare(data1: &[u8], data2: &[u8]) -> core::result::Result<Ordering, ValueError> {
        Self::try_from_bytes(data1)?;
        Self::try_from_bytes(data2)?;
        Ok(Ordering::Equal)
    }
}

impl Value for bool {
    type SelfType<'a>
        = bool
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    // `false` and `true` encode to 0 and 1, so nothing encodes to 0xff
    #[cfg(feature = "experimental-niches")]
    const NICHE: Option<&'static [u8]> = Some(&[0xff]);

    fn fixed_width() -> Option<usize> {
        Some(1)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> bool
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<bool, ValueError>
    where
        Self: 'a,
    {
        match data {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(value_error("invalid bool encoding")),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'b,
    {
        match value {
            true => &[1],
            false => &[0],
        }
    }

    fn type_name() -> TypeName {
        TypeName::internal("bool")
    }
}

impl Key for bool {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // `false` and `true` encode to 0 and 1, so byte order is value order. A malformed
        // encoding still has a total order and does not panic.
        data1.cmp(data2)
    }
}

// `None` without a niche is a `0` tag and, for a fixed-width `T`, that many zero bytes.
// Anything else with a `0` tag is not an encoding `as_bytes` produces.
fn untagged_option_payload<T: Value>(
    data: &[u8],
) -> core::result::Result<Option<&[u8]>, ValueError> {
    match data.first().copied() {
        Some(0) if is_untagged_option_none::<T>(data) => Ok(None),
        Some(1) => Ok(Some(&data[1..])),
        _ => Err(value_error("invalid Option encoding")),
    }
}

fn is_untagged_option_none<T: Value>(data: &[u8]) -> bool {
    if data.first() != Some(&0) {
        return false;
    }
    match T::fixed_width() {
        Some(width) => data.len() == width + 1 && data[1..].iter().all(|byte| *byte == 0),
        None => data.len() == 1,
    }
}

impl<T: Value> Value for Option<T> {
    type SelfType<'a>
        = Option<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        if T::NICHE.is_some() {
            // `None` is encoded as the niche, so there is no tag to make room for
            T::fixed_width()
        } else {
            T::fixed_width().map(|x| x + 1)
        }
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Option<T::SelfType<'a>>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(
        data: &'a [u8],
    ) -> core::result::Result<Option<T::SelfType<'a>>, ValueError>
    where
        Self: 'a,
    {
        if let Some(niche) = T::NICHE {
            return if data == niche {
                Ok(None)
            } else {
                T::try_from_bytes(data).map(Some)
            };
        }
        match data.first().copied() {
            Some(0) if is_untagged_option_none::<T>(data) => Ok(None),
            Some(1) => T::try_from_bytes(&data[1..]).map(Some),
            _ => Err(value_error("invalid Option encoding")),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        if let Some(niche) = T::NICHE {
            let Some(x) = value else {
                debug_assert!(
                    T::fixed_width().is_none_or(|width| width == niche.len()),
                    "the niche of a fixed width type is not that wide"
                );
                return niche.to_vec();
            };
            let encoded = T::as_bytes(x);
            debug_assert!(
                encoded.as_ref() != niche,
                "a value encodes to the niche of its type"
            );
            return encoded.as_ref().to_vec();
        }
        let mut result = vec![0];
        if let Some(x) = value {
            result[0] = 1;
            result.extend_from_slice(T::as_bytes(x).as_ref());
        } else if let Some(fixed_width) = T::fixed_width() {
            result.extend_from_slice(&vec![0; fixed_width]);
        }
        result
    }

    fn type_name() -> TypeName {
        let inner = T::type_name();
        let name = if T::NICHE.is_some() {
            format!("niche::Option<{}>", inner.name())
        } else {
            format!("Option<{}>", inner.name())
        };
        TypeName::internal(&name).into_composite(inner.is_user_defined())
    }
}

impl<T: Key> Key for Option<T> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::try_compare(data1, data2).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_compare(data1: &[u8], data2: &[u8]) -> core::result::Result<Ordering, ValueError> {
        // Both sides are validated before the tag decides the order. A `None` compared with a
        // malformed `Some` is an error, not `Less`.
        if let Some(niche) = T::NICHE {
            // `None` sorts below every `Some`, as it does when encoded with a tag
            return match (data1 == niche, data2 == niche) {
                (true, true) => Ok(Ordering::Equal),
                (true, false) => {
                    T::try_from_bytes(data2)?;
                    Ok(Ordering::Less)
                }
                (false, true) => {
                    T::try_from_bytes(data1)?;
                    Ok(Ordering::Greater)
                }
                (false, false) => T::try_compare(data1, data2),
            };
        }
        match (
            untagged_option_payload::<T>(data1)?,
            untagged_option_payload::<T>(data2)?,
        ) {
            (None, None) => Ok(Ordering::Equal),
            (None, Some(payload)) => {
                T::try_from_bytes(payload)?;
                Ok(Ordering::Less)
            }
            (Some(payload), None) => {
                T::try_from_bytes(payload)?;
                Ok(Ordering::Greater)
            }
            (Some(left), Some(right)) => T::try_compare(left, right),
        }
    }

    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        // A fixed width `T` makes `Option<T>` fixed width too, and its encodings are compared at
        // that width, so nothing shorter than `left` may be returned
        if T::fixed_width().is_some() {
            return Cow::Borrowed(left);
        }
        if let Some(niche) = T::NICHE {
            // `None` sorts below every `Some`, and keeps its own encoding, as it does when
            // encoded with a tag
            if left == niche {
                return Cow::Borrowed(left);
            }
            // Both are `Some`, encoded exactly as `T`, so they separate as `T` does. The result
            // is an encoding of `T`, which the niche never is.
            let separator = T::separator(left, right);
            debug_assert!(separator.as_ref() != niche);
            return separator;
        }
        // `None` sorts below every `Some` and encodes as the tag alone, so nothing is shorter
        if left.first().copied().unwrap_or(0) == 0 || right.len() < 2 {
            return Cow::Borrowed(left);
        }
        // Both are `Some`, so a separator between the payloads, behind the same tag, separates
        // the two. Which input the payload came from is not visible here, so the result is
        // assembled rather than borrowed.
        let payload = T::separator(&left[1..], &right[1..]);
        if payload.len() + 1 >= left.len() {
            return Cow::Borrowed(left);
        }
        let mut separator = Vec::with_capacity(payload.len() + 1);
        separator.push(1);
        separator.extend_from_slice(&payload);
        Cow::Owned(separator)
    }

    // `None` sorts below every `Some`
    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        if let Some(niche) = T::NICHE {
            return Some(Cow::Borrowed(niche));
        }
        Some(match T::fixed_width() {
            // A fixed width `T` pads the tag out to the width of a `Some`
            Some(width) => Cow::Owned(vec![0; width + 1]),
            None => Cow::Borrowed(&[0]),
        })
    }
}

impl Value for &[u8] {
    type SelfType<'a>
        = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal("&[u8]")
    }
}

impl Key for &[u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }

    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        debug_assert!(left < right);
        // Truncating `right` just past the first byte where it exceeds `left` leaves something
        // greater than `left`, and less than `right` as long as bytes were dropped. When that is
        // no shorter than `left`, `left` itself is the shortest: anything shorter shares fewer
        // bytes with `left` than `left` and `right` do, so it sorts either below `left` or above
        // `right`.
        let separator_len = left.iter().zip(right).take_while(|(x, y)| x == y).count() + 1;
        if separator_len < left.len() && separator_len < right.len() {
            Cow::Borrowed(&right[..separator_len])
        } else {
            Cow::Borrowed(left)
        }
    }

    // Encodings order as their bytes, so the empty one sorts below them all
    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl<const N: usize> Value for &[u8; N] {
    type SelfType<'a>
        = &'a [u8; N]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8; N]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(N)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8; N]
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<&'a [u8; N], ValueError>
    where
        Self: 'a,
    {
        data.try_into()
            .map_err(|_| value_error(format_args!("expected {N} bytes, got {}", data.len())))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8; N]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("[u8;{N}]")).into_composite(false)
    }
}

impl<const N: usize> Key for &[u8; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl<const N: usize, T: Value> Value for [T; N] {
    type SelfType<'a>
        = [T::SelfType<'a>; N]
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width().map(|x| x * N)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> [T::SelfType<'a>; N]
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<[T::SelfType<'a>; N], ValueError>
    where
        Self: 'a,
    {
        let mut result = Vec::with_capacity(N);
        if let Some(fixed) = T::fixed_width() {
            let Some(expected) = fixed.checked_mul(N) else {
                return Err(value_error("encoded array is too large"));
            };
            if data.len() != expected {
                return Err(value_error(format_args!(
                    "expected {expected} bytes, got {}",
                    data.len()
                )));
            }
            for i in 0..N {
                result.push(T::try_from_bytes(&data[fixed * i..fixed * (i + 1)])?);
            }
        } else {
            if data.len() < size_of::<u32>().saturating_mul(N) {
                return Err(value_error("encoded array is too short"));
            }
            let mut start = size_of::<u32>() * N;
            for i in 0..N {
                let range = size_of::<u32>() * i..size_of::<u32>() * (i + 1);
                let end = u32::from_le_bytes(data[range].try_into().unwrap()) as usize;
                if end < start || end > data.len() {
                    return Err(value_error("encoded array is too short"));
                }
                result.push(T::try_from_bytes(&data[start..end])?);
                start = end;
            }
            if start != data.len() {
                return Err(value_error("encoded array has trailing bytes"));
            }
        }
        Ok(result.try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        if let Some(fixed) = T::fixed_width() {
            let mut result = Vec::with_capacity(fixed * N);
            for item in value {
                result.extend_from_slice(T::as_bytes(item).as_ref());
            }
            result
        } else {
            // Reserve space for the end offsets
            let mut result = vec![0u8; size_of::<u32>() * N];
            for i in 0..N {
                result.extend_from_slice(T::as_bytes(&value[i]).as_ref());
                let end: u32 = result.len().try_into().unwrap();
                result[size_of::<u32>() * i..size_of::<u32>() * (i + 1)]
                    .copy_from_slice(&end.to_le_bytes());
            }
            result
        }
    }

    fn type_name() -> TypeName {
        // Uses the same type name as [T;N] so that tables are compatible with [u8;N] and &[u8;N] types
        // This requires that the binary encoding be the same
        let inner = T::type_name();
        TypeName::internal(&format!("[{};{N}]", inner.name()))
            .into_composite(inner.is_user_defined())
    }
}

impl<const N: usize, T: Key> Key for [T; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::try_compare(data1, data2).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_compare(data1: &[u8], data2: &[u8]) -> core::result::Result<Ordering, ValueError> {
        if let Some(fixed) = T::fixed_width() {
            let width = fixed
                .checked_mul(N)
                .ok_or_else(|| value_error("invalid array encoding"))?;
            if data1.len() != width || data2.len() != width {
                return Err(value_error("invalid array encoding"));
            }
            let mut order = Ordering::Equal;
            for i in 0..N {
                let range = fixed * i..fixed * (i + 1);
                let comparison = T::try_compare(&data1[range.clone()], &data2[range])?;
                if order.is_eq() {
                    order = comparison;
                }
            }
            return Ok(order);
        }
        // Set offset to the first data item
        let mut start1 = size_of::<u32>() * N;
        let mut start2 = size_of::<u32>() * N;
        if data1.len() < start1 || data2.len() < start2 {
            return Err(value_error("invalid array encoding"));
        }
        let mut order = Ordering::Equal;
        for i in 0..N {
            let range = size_of::<u32>() * i..size_of::<u32>() * (i + 1);
            let end1 = u32::from_le_bytes(data1[range.clone()].try_into().unwrap()) as usize;
            let end2 = u32::from_le_bytes(data2[range].try_into().unwrap()) as usize;
            if end1 < start1 || end1 > data1.len() || end2 < start2 || end2 > data2.len() {
                return Err(value_error("invalid array encoding"));
            }
            let comparison = T::try_compare(&data1[start1..end1], &data2[start2..end2])?;
            if order.is_eq() {
                order = comparison;
            }
            start1 = end1;
            start2 = end2;
        }
        if start1 != data1.len() || start2 != data2.len() {
            return Err(value_error("invalid array encoding"));
        }
        Ok(order)
    }

    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        // A fixed width element type makes the array fixed width too, and its encodings are
        // compared at that width, so nothing shorter than `left` may be returned
        if T::fixed_width().is_some() {
            return Cow::Borrowed(left);
        }
        // Only the element the two first differ in can be shortened: the elements before it are
        // needed for the result to sort below `right`. The elements after it can be replaced by
        // the smallest encoding, but only once the shortened element sorts strictly above
        // `left`'s, so that both comparisons resolve there and never read what follows.
        let header = size_of::<u32>() * N;

        for i in 0..N {
            let left_element = array_element::<N>(left, i);
            let right_element = array_element::<N>(right, i);
            if T::compare(left_element, right_element).is_eq() {
                continue;
            }
            let separator = T::separator(left_element, right_element);
            // There is a tail to replace only when this is not the last element, and it may only
            // be replaced once this element sorts strictly above `left`'s. `min_encoded_key()`
            // may allocate, so it is asked only when the answer will be used.
            let replaces_tail = i + 1 < N && T::compare(left_element, &separator).is_lt();
            let min = replaces_tail.then(T::min_encoded_key).flatten();
            let tail = min.as_deref();
            let mut elements: Vec<&[u8]> = Vec::with_capacity(N);
            elements.extend((0..i).map(|j| array_element::<N>(left, j)));
            elements.push(&separator);
            elements
                .extend(((i + 1)..N).map(|j| tail.unwrap_or_else(|| array_element::<N>(left, j))));

            // A separator longer than the whole key is no use, however it was assembled. A
            // smallest encoding longer than the element it replaces can make it longer, so this
            // is checked before building it, which also keeps every end offset within a `u32`.
            let total = elements
                .iter()
                .map(|x| x.len())
                .fold(header, usize::saturating_add);
            if total >= left.len() {
                return Cow::Borrowed(left);
            }
            let mut result = Vec::with_capacity(total);
            let mut end = header;
            for element in &elements {
                end += element.len();
                result.extend_from_slice(&u32::try_from(end).unwrap().to_le_bytes());
            }
            for element in &elements {
                result.extend_from_slice(element);
            }
            return Cow::Owned(result);
        }
        // Every element compared equal, so `left` does not sort below `right`
        Cow::Borrowed(left)
    }
}

// The `index`th element of an array encoding of `N` variable width elements
fn array_element<const N: usize>(data: &[u8], index: usize) -> &[u8] {
    let start = if index == 0 {
        size_of::<u32>() * N
    } else {
        array_end_offset(data, index - 1)
    };
    &data[start..array_end_offset(data, index)]
}

// The end offset an array encoding stores for its `index`th element
fn array_end_offset(data: &[u8], index: usize) -> usize {
    let range = size_of::<u32>() * index..size_of::<u32>() * (index + 1);
    u32::from_le_bytes(data[range].try_into().unwrap()) as usize
}

// The niche of `&str` and `String`: no UTF-8 string contains the byte 0xff
#[cfg(feature = "experimental-niches")]
const STR_NICHE: &[u8] = &[0xff];

impl Value for &str {
    type SelfType<'a>
        = &'a str
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a str
    where
        Self: 'a;

    #[cfg(feature = "experimental-niches")]
    const NICHE: Option<&'static [u8]> = Some(STR_NICHE);

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a str
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<&'a str, ValueError>
    where
        Self: 'a,
    {
        core::str::from_utf8(data).map_err(|_| value_error("invalid UTF-8 in &str encoding"))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a str
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal("&str")
    }
}

// `index` advanced to the next character boundary, or the end. Only a character's trailing bytes
// match `10xx_xxxx`, so this needs no deserialization, unlike `str::is_char_boundary()`.
fn round_up_to_char_boundary(utf8: &[u8], mut index: usize) -> usize {
    while index < utf8.len() && utf8[index] & 0b1100_0000 == 0b1000_0000 {
        index += 1;
    }
    index
}

impl Key for &str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // Valid UTF-8 orders as its bytes. Comparing the bytes also orders a malformed
        // encoding without panicking.
        data1.cmp(data2)
    }

    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        debug_assert!(left < right);
        // `str` orders as its bytes, but `compare()` deserializes, so the cut must keep it valid
        let common_bytes = left.iter().zip(right).take_while(|(x, y)| x == y).count();
        let separator_len = round_up_to_char_boundary(right, common_bytes + 1);
        if separator_len < left.len() && separator_len < right.len() {
            Cow::Borrowed(&right[..separator_len])
        } else {
            Cow::Borrowed(left)
        }
    }

    // The empty string is valid UTF-8, and sorts below every other one
    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl Value for String {
    type SelfType<'a>
        = String
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a str
    where
        Self: 'a;

    #[cfg(feature = "experimental-niches")]
    const NICHE: Option<&'static [u8]> = Some(STR_NICHE);

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> String
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<String, ValueError>
    where
        Self: 'a,
    {
        core::str::from_utf8(data)
            .map(ToString::to_string)
            .map_err(|_| value_error("invalid UTF-8 in String encoding"))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a str
    where
        Self: 'b,
    {
        value.as_str()
    }

    fn type_name() -> TypeName {
        TypeName::internal("String")
    }
}

impl Key for String {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // Encoded the same way as `&str`, so it orders the same way
        <&str as Key>::compare(data1, data2)
    }

    // Encoded the same way as `&str`, so it separates the same way
    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        <&str as Key>::separator(left, right)
    }

    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        <&str as Key>::min_encoded_key()
    }
}

impl Value for char {
    type SelfType<'a> = char;
    type AsBytes<'a>
        = [u8; 3]
    where
        Self: 'a;

    // A code point is at most 0x10ffff, so nothing encodes to 0xffffff
    #[cfg(feature = "experimental-niches")]
    const NICHE: Option<&'static [u8]> = Some(&[0xff; 3]);

    fn fixed_width() -> Option<usize> {
        Some(3)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> char
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<char, ValueError>
    where
        Self: 'a,
    {
        let bytes: &[u8; 3] = data
            .try_into()
            .map_err(|_| value_error("invalid char encoding"))?;
        let [a, b, c] = *bytes;
        char::from_u32(u32::from_le_bytes([a, b, c, 0]))
            .ok_or_else(|| value_error("invalid char encoding"))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 3]
    where
        Self: 'b,
    {
        let bytes = u32::from(*value).to_le_bytes();
        [bytes[0], bytes[1], bytes[2]]
    }

    fn type_name() -> TypeName {
        TypeName::internal(stringify!(char))
    }
}

impl Key for char {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // The code point is stored little-endian, so the last byte is the most
        // significant. Forward byte order is not `char` order.
        data1.iter().rev().cmp(data2.iter().rev())
    }
}

macro_rules! le_value {
    ($t:ty) => {
        impl Value for $t {
            type SelfType<'a> = $t;
            type AsBytes<'a>
                = [u8; core::mem::size_of::<$t>()]
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                Some(core::mem::size_of::<$t>())
            }

            fn from_bytes<'a>(data: &'a [u8]) -> $t
            where
                Self: 'a,
            {
                Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
            }

            fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<$t, ValueError>
            where
                Self: 'a,
            {
                let bytes: [u8; core::mem::size_of::<$t>()] = data.try_into().map_err(|_| {
                    value_error(format_args!(
                        "expected {} bytes, got {}",
                        core::mem::size_of::<$t>(),
                        data.len()
                    ))
                })?;
                Ok(<$t>::from_le_bytes(bytes))
            }

            fn as_bytes<'a, 'b: 'a>(
                value: &'a Self::SelfType<'b>,
            ) -> [u8; core::mem::size_of::<$t>()]
            where
                Self: 'a,
                Self: 'b,
            {
                value.to_le_bytes()
            }

            fn type_name() -> TypeName {
                TypeName::internal(stringify!($t))
            }
        }
    };
}

macro_rules! le_impl {
    ($t:ty) => {
        le_value!($t);

        impl Key for $t {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                Self::try_compare(data1, data2).unwrap_or_else(|err| panic!("{err}"))
            }

            fn try_compare(
                data1: &[u8],
                data2: &[u8],
            ) -> core::result::Result<Ordering, ValueError> {
                Ok(Self::try_from_bytes(data1)?.cmp(&Self::try_from_bytes(data2)?))
            }
        }
    };
}

le_impl!(u8);
le_impl!(u16);
le_impl!(u32);
le_impl!(u64);
le_impl!(u128);
le_impl!(i8);
le_impl!(i16);
le_impl!(i32);
le_impl!(i64);
le_impl!(i128);
le_value!(f32);
le_value!(f64);

// `NonZero*` is encoded and ordered as the primitive it wraps. Zero is the one encoding of that
// primitive it never produces, which `Option<NonZero*>` uses for `None`. `NonZeroUsize` and
// `NonZeroIsize` are left out, as `usize` and `isize` are: their width varies between platforms.
macro_rules! nonzero_impl {
    ($t:ident, $prim:ty) => {
        impl Value for $t {
            type SelfType<'a> = $t;
            type AsBytes<'a>
                = <$prim as Value>::AsBytes<'a>
            where
                Self: 'a;

            const NICHE: Option<&'static [u8]> = Some(&[0; size_of::<$prim>()]);

            fn fixed_width() -> Option<usize> {
                <$prim as Value>::fixed_width()
            }

            fn from_bytes<'a>(data: &'a [u8]) -> $t
            where
                Self: 'a,
            {
                Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
            }

            fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<$t, ValueError>
            where
                Self: 'a,
            {
                let value = <$prim as Value>::try_from_bytes(data)?;
                <$t>::new(value)
                    .ok_or_else(|| value_error(concat!("invalid ", stringify!($t), " encoding")))
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                <$prim as Value>::as_bytes(&value.get())
            }

            fn type_name() -> TypeName {
                TypeName::internal(stringify!($t))
            }
        }

        impl Key for $t {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                Self::try_compare(data1, data2).unwrap_or_else(|err| panic!("{err}"))
            }

            fn try_compare(
                data1: &[u8],
                data2: &[u8],
            ) -> core::result::Result<Ordering, ValueError> {
                Ok(Self::try_from_bytes(data1)?
                    .get()
                    .cmp(&Self::try_from_bytes(data2)?.get()))
            }
        }
    };
}

nonzero_impl!(NonZeroU8, u8);
nonzero_impl!(NonZeroU16, u16);
nonzero_impl!(NonZeroU32, u32);
nonzero_impl!(NonZeroU64, u64);
nonzero_impl!(NonZeroU128, u128);
nonzero_impl!(NonZeroI8, i8);
nonzero_impl!(NonZeroI16, i16);
nonzero_impl!(NonZeroI32, i32);
nonzero_impl!(NonZeroI64, i64);
nonzero_impl!(NonZeroI128, i128);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "expected 4 bytes")]
    fn malformed_u32_compare_panics() {
        let _ = <u32 as Key>::compare(&[1, 2], &1u32.to_le_bytes());
    }

    #[test]
    fn malformed_builtin_values_return_errors() {
        assert!(<&str as Value>::try_from_bytes(&[0xff]).is_err());
        assert!(<String as Value>::try_from_bytes(&[0xff]).is_err());
        assert!(<bool as Value>::try_from_bytes(&[2]).is_err());
        assert!(<bool as Value>::try_from_bytes(&[]).is_err());
        assert!(<u32 as Value>::try_from_bytes(&[1, 2]).is_err());
        assert!(<u32 as Key>::try_compare(&[1, 2], &1u32.to_le_bytes()).is_err());
        assert_eq!(
            <u32 as Key>::try_compare(&1u32.to_le_bytes(), &2u32.to_le_bytes()).unwrap(),
            Ordering::Less
        );
        assert!(<Option<u32> as Value>::try_from_bytes(&[2, 0, 0, 0, 0]).is_err());
        assert!(<Option<u32> as Value>::try_from_bytes(&[]).is_err());
        assert!(
            <Option<u32> as Value>::try_from_bytes(&[0, 0, 0, 0, 0])
                .unwrap()
                .is_none()
        );
        assert!(<Option<u32> as Value>::try_from_bytes(&[0, 0xff, 0, 0, 0]).is_err());
        assert!(
            <Option<Vec<u8>> as Value>::try_from_bytes(&[0])
                .unwrap()
                .is_none()
        );
        assert!(<Option<Vec<u8>> as Value>::try_from_bytes(&[0, 0xff]).is_err());
        let none = <Option<u32> as Value>::as_bytes(&None);
        assert!(<Option<u32> as Key>::try_compare(none.as_ref(), &[2, 0, 0, 0, 0]).is_err());
        let left = [NonZeroU32::new(1).unwrap(), NonZeroU32::new(2).unwrap()];
        let right = [NonZeroU32::new(3).unwrap(), NonZeroU32::new(4).unwrap()];
        let mut bad_array = <[NonZeroU32; 2] as Value>::as_bytes(&left);
        bad_array[4..].fill(0);
        assert!(
            <[NonZeroU32; 2] as Key>::try_compare(
                bad_array.as_ref(),
                <[NonZeroU32; 2] as Value>::as_bytes(&right).as_ref()
            )
            .is_err()
        );
        let mut bad_variable = <(String, NonZeroU32) as Value>::as_bytes(&(
            String::from("a"),
            NonZeroU32::new(1).unwrap(),
        ));
        let variable_len = bad_variable.len();
        bad_variable[variable_len - 4..].fill(0);
        assert!(
            <(String, NonZeroU32) as Key>::try_compare(
                bad_variable.as_ref(),
                <(String, NonZeroU32) as Value>::as_bytes(&(
                    String::from("b"),
                    NonZeroU32::new(2).unwrap(),
                ))
                .as_ref()
            )
            .is_err()
        );
        let mut bad_tuple = <(NonZeroU32, NonZeroU32) as Value>::as_bytes(&(left[0], left[1]));
        bad_tuple[4..].fill(0);
        assert!(
            <(NonZeroU32, NonZeroU32) as Key>::try_compare(
                bad_tuple.as_ref(),
                <(NonZeroU32, NonZeroU32) as Value>::as_bytes(&(right[0], right[1])).as_ref()
            )
            .is_err()
        );
        assert!(<char as Value>::try_from_bytes(&[0xff, 0xff, 0xff]).is_err());
        assert!(<NonZeroU32 as Value>::try_from_bytes(&[0, 0, 0, 0]).is_err());
        assert!(<Vec<u8> as Value>::try_from_bytes(&[]).is_err());
        assert!(<Vec<u8> as Value>::try_from_bytes(&[0]).unwrap().is_empty());
        // 254 means a u16 length follows. Zero fits in the one-byte form, `[0]`.
        assert!(<Vec<u8> as Value>::try_from_bytes(&[254, 0, 0]).is_err());
        assert!(<() as Value>::try_from_bytes(&[]).is_ok());
        assert!(<() as Value>::try_from_bytes(b"x").is_err());
        assert_eq!(<() as Key>::try_compare(&[], &[]).unwrap(), Ordering::Equal);
        assert!(<() as Key>::try_compare(b"x", &[]).is_err());
        assert!(<(u8, u8) as Value>::try_from_bytes(&[1]).is_err());
        assert!(<bool as Value>::try_from_bytes(&[1]).unwrap());
        assert_eq!(<&str as Value>::try_from_bytes(b"ok").unwrap(), "ok");

        let guard = crate::AccessGuard::<&str>::with_owned_value(vec![0xff]);
        assert!(matches!(
            guard.try_value(),
            Err(crate::StorageError::Corrupted(_))
        ));
        let guard = crate::AccessGuard::<&str>::with_owned_value(b"ok".to_vec());
        assert_eq!(guard.try_value().unwrap(), "ok");
    }

    #[test]
    fn classification_is_readable_through_reconstruction() {
        // The derive macro reads a field type's classification by rebuilding its `TypeName`
        // through `TypeName::new` and comparing, because `is_user_defined()` is crate-private.
        // Equality must keep agreeing with `is_user_defined()` for every classification.
        let samples = [
            TypeName::new("Plain"),
            TypeName::internal("Plain"),
            TypeName::internal2("Plain"),
            TypeName::internal("Composite").into_composite(true),
            TypeName::internal("Composite").into_composite(false),
            TypeName::internal2("Composite").into_composite(true),
            TypeName::internal2("Composite").into_composite(false),
        ];
        for type_name in samples {
            assert_eq!(
                type_name.is_user_defined(),
                type_name == TypeName::new(type_name.name())
            );
        }
    }

    #[test]
    fn byte_slice_separator() {
        // (left, right, the shortest slice that separates them)
        let cases: &[(&[u8], &[u8], &[u8])] = &[
            (b"abc0suffix", b"abc1suffix", b"abc1"),
            // Nothing is shorter than `left` and still above it
            (b"abc", b"abd-suffix", b"abc"),
            (b"abc", b"abc-suffix", b"abc"),
            (b"\x01\xff", b"\x02\x00", b"\x02"),
            // Truncating `right` at all would reach `right` itself
            (b"\x01\xff", b"\x02", b"\x01\xff"),
            (b"", b"\x00", b""),
        ];

        for &(left, right, expected) in cases {
            let separator = <&[u8] as Key>::separator(left, right);
            assert_eq!(separator, expected);
            assert!(<&[u8] as Key>::compare(left, &separator).is_le());
            assert!(<&[u8] as Key>::compare(&separator, right).is_lt());
        }
    }

    // The byte test must agree with the standard library's definition, at every index
    #[test]
    fn round_up_to_char_boundary_matches_std() {
        let sample: String = [
            'a',
            '\u{0}',
            '\u{7f}',
            '\u{80}',
            '\u{e9}',
            '\u{7ff}',
            '\u{800}',
            'b',
            '\u{ffff}',
            '\u{1d11e}',
            '\u{10ffff}',
            'c',
        ]
        .iter()
        .collect();
        let bytes = sample.as_bytes();
        for index in 0..=bytes.len() {
            let rounded = round_up_to_char_boundary(bytes, index);
            assert!(
                sample.is_char_boundary(rounded),
                "index {index} rounded to {rounded}"
            );
            assert!(rounded >= index);
            // Nothing between `index` and `rounded` may be a boundary, or it was not the next one
            for skipped in index..rounded {
                assert!(
                    !sample.is_char_boundary(skipped),
                    "skipped boundary {skipped}"
                );
            }
        }
    }

    #[test]
    fn str_separator() {
        // (left, right, the shortest separator that is still valid UTF-8)
        let cases: &[(&str, &str, &str)] = &[
            ("abc0suffix", "abc1suffix", "abc1"),
            // Nothing is shorter than `left` and still above it
            ("abc", "abd-suffix", "abc"),
            ("abc", "abc-suffix", "abc"),
            // A cut inside a two byte character keeps the whole character
            ("aaaaaa", "a\u{e9}zz", "a\u{e9}"),
            // ...and inside a four byte one
            ("aaaaaaaa", "a\u{1d11e}zz", "a\u{1d11e}"),
            // Keeping the whole character would reach `right` itself, so `left` wins
            ("a\u{e9}", "a\u{ea}", "a\u{e9}"),
            ("", "\u{0}", ""),
        ];

        for &(left, right, expected) in cases {
            let separator = <&str as Key>::separator(left.as_bytes(), right.as_bytes());
            assert_eq!(separator, expected.as_bytes());
            // A separator is passed back through `compare()`, which deserializes it
            assert!(core::str::from_utf8(&separator).is_ok());
            assert!(<&str as Key>::compare(left.as_bytes(), &separator).is_le());
            assert!(<&str as Key>::compare(&separator, right.as_bytes()).is_lt());
            assert_eq!(
                <String as Key>::separator(left.as_bytes(), right.as_bytes()),
                separator
            );
        }
    }

    #[test]
    fn option_separator() {
        // (left, right, the shortest separator)
        #[cfg(not(feature = "experimental-niches"))]
        let cases: &[(Option<&str>, Option<&str>, &[u8])] = &[
            // The payloads separate as `&str` does, behind the `Some` tag
            (Some("abc0suffix"), Some("abc1suffix"), b"\x01abc1"),
            // `None` encodes as the tag alone, so nothing is shorter
            (None, Some("abc"), b"\x00"),
            // Nothing shorter than `left` sorts above it
            (Some("abc"), Some("abd-suffix"), b"\x01abc"),
        ];
        // `&str` declares a niche, so a `Some` is encoded as its payload and `None` as the niche
        #[cfg(feature = "experimental-niches")]
        let cases: &[(Option<&str>, Option<&str>, &[u8])] = &[
            (Some("abc0suffix"), Some("abc1suffix"), b"abc1"),
            (None, Some("abc"), b"\xff"),
            (Some("abc"), Some("abd-suffix"), b"abc"),
        ];

        for &(left, right, expected) in cases {
            let left = <Option<&str> as Value>::as_bytes(&left);
            let right = <Option<&str> as Value>::as_bytes(&right);
            let separator = <Option<&str> as Key>::separator(&left, &right);
            assert_eq!(separator, expected);
            assert!(<Option<&str> as Key>::compare(&left, &separator).is_le());
            assert!(<Option<&str> as Key>::compare(&separator, &right).is_lt());
        }
    }

    // `Option<T>` of a fixed width `T` is itself fixed width, so it must not shorten
    #[test]
    fn fixed_width_option_separator_returns_full_key() {
        let left = <Option<u64> as Value>::as_bytes(&Some(1));
        let right = <Option<u64> as Value>::as_bytes(&Some(2));
        assert_eq!(<Option<u64> as Key>::separator(&left, &right), left);
    }

    // Types that declare no niche keep the tag, so their existing tables stay readable
    #[test]
    fn option_of_type_without_niche_keeps_the_tag() {
        assert_eq!(<u32 as Value>::NICHE, None);
        assert_eq!(<() as Value>::NICHE, None);
        assert_eq!(<Option<u32> as Value>::fixed_width(), Some(5));
        assert_eq!(<Option<u32> as Value>::as_bytes(&None), [0; 5]);
        assert_eq!(<Option<u32> as Value>::as_bytes(&Some(1)), [1, 1, 0, 0, 0]);
        assert_eq!(<Option<()> as Value>::as_bytes(&None), [0]);
        assert_eq!(<Option<()> as Value>::as_bytes(&Some(())), [1]);
    }

    // `bool` keeps the tag without the feature, and drops it with the feature for the 0xff
    // niche, which neither of its values encodes to
    #[test]
    fn option_bool_encoding() {
        #[cfg(not(feature = "experimental-niches"))]
        {
            assert_eq!(<bool as Value>::NICHE, None);
            assert_eq!(<Option<bool> as Value>::fixed_width(), Some(2));
            assert_eq!(<Option<bool> as Value>::as_bytes(&None), [0, 0]);
            assert_eq!(<Option<bool> as Value>::as_bytes(&Some(false)), [1, 0]);
            assert_eq!(<Option<bool> as Value>::type_name().name(), "Option<bool>");
        }
        #[cfg(feature = "experimental-niches")]
        {
            assert_eq!(<bool as Value>::NICHE, Some([0xff].as_slice()));
            assert_eq!(<Option<bool> as Value>::fixed_width(), Some(1));
            assert_eq!(<Option<bool> as Value>::as_bytes(&None), [0xff]);
            assert_eq!(<Option<bool> as Value>::as_bytes(&Some(false)), [0]);
            assert_eq!(<Option<bool> as Value>::as_bytes(&Some(true)), [1]);
            for value in [None, Some(false), Some(true)] {
                let encoded = <Option<bool> as Value>::as_bytes(&value);
                assert_eq!(<Option<bool> as Value>::from_bytes(&encoded), value);
            }
            assert_eq!(
                <Option<bool> as Value>::type_name().name(),
                "niche::Option<bool>"
            );
        }
    }

    fn check_comparison(a: char, b: char) {
        let a_bytes = <char as Value>::as_bytes(&a);
        let b_bytes = <char as Value>::as_bytes(&b);
        assert_eq!(
            <char as Key>::compare(a_bytes.as_slice(), b_bytes.as_slice()),
            a.cmp(&b),
            "{a:?} <=> {b:?}"
        );
    }

    #[test]
    fn char_compare_follows_code_point_order() {
        let chars = [
            '\0',
            'a',
            char::from_u32(0x00ff).unwrap(),
            char::from_u32(0x0100).unwrap(),
            char::from_u32(0x1f600).unwrap(),
            char::MAX,
        ];
        for &a in &chars {
            for &b in &chars {
                check_comparison(a, b);
            }
        }
    }

    // `char` keeps the tag without the feature, and drops it with the feature for the 0xffffff
    // niche, which lies beyond the last code point
    #[test]
    fn option_char_encoding() {
        #[cfg(not(feature = "experimental-niches"))]
        {
            assert_eq!(<char as Value>::NICHE, None);
            assert_eq!(<Option<char> as Value>::fixed_width(), Some(4));
            assert_eq!(<Option<char> as Value>::as_bytes(&None), [0; 4]);
            assert_eq!(
                <Option<char> as Value>::as_bytes(&Some('a')),
                [1, b'a', 0, 0]
            );
            assert_eq!(<Option<char> as Value>::type_name().name(), "Option<char>");
        }
        #[cfg(feature = "experimental-niches")]
        {
            assert_eq!(<char as Value>::NICHE, Some([0xff; 3].as_slice()));
            assert_eq!(<Option<char> as Value>::fixed_width(), Some(3));
            assert_eq!(<Option<char> as Value>::as_bytes(&None), [0xff; 3]);
            assert_eq!(<Option<char> as Value>::as_bytes(&Some('a')), [b'a', 0, 0]);
            for value in [None, Some('\0'), Some('a'), Some(char::MAX)] {
                let encoded = <Option<char> as Value>::as_bytes(&value);
                assert_eq!(<Option<char> as Value>::from_bytes(&encoded), value);
            }
            assert_eq!(
                <Option<char> as Value>::type_name().name(),
                "niche::Option<char>"
            );
        }
    }

    // `&str` and `String` keep the tag without the feature, and drop it with the feature for the
    // 0xff niche, which no UTF-8 string contains
    #[test]
    fn option_str_encoding() {
        assert_eq!(<Option<&str> as Value>::fixed_width(), None);
        assert_eq!(<Option<String> as Value>::fixed_width(), None);
        #[cfg(not(feature = "experimental-niches"))]
        {
            assert_eq!(<&str as Value>::NICHE, None);
            assert_eq!(<String as Value>::NICHE, None);
            assert_eq!(<Option<&str> as Value>::as_bytes(&None), [0]);
            assert_eq!(<Option<&str> as Value>::as_bytes(&Some("a")), [1, b'a']);
            assert_eq!(<Option<String> as Value>::as_bytes(&None), [0]);
            assert_eq!(
                <Option<String> as Value>::as_bytes(&Some("a".to_string())),
                [1, b'a']
            );
        }
        #[cfg(feature = "experimental-niches")]
        {
            assert_eq!(<&str as Value>::NICHE, Some([0xff].as_slice()));
            assert_eq!(<String as Value>::NICHE, Some([0xff].as_slice()));
            assert_eq!(<Option<&str> as Value>::as_bytes(&None), [0xff]);
            assert_eq!(<Option<&str> as Value>::as_bytes(&Some("a")), [b'a']);
            assert!(<Option<&str> as Value>::as_bytes(&Some("")).is_empty());
            assert_eq!(<Option<String> as Value>::as_bytes(&None), [0xff]);
            assert_eq!(
                <Option<String> as Value>::as_bytes(&Some("a".to_string())),
                [b'a']
            );
            for value in [None, Some(""), Some("a"), Some("\u{ff}\u{10ffff}")] {
                let encoded = <Option<&str> as Value>::as_bytes(&value);
                assert_eq!(<Option<&str> as Value>::from_bytes(&encoded), value);
                let owned = value.map(str::to_string);
                assert_eq!(<Option<String> as Value>::as_bytes(&owned), encoded);
                assert_eq!(<Option<String> as Value>::from_bytes(&encoded), owned);
            }
        }
    }

    // `Option` of a type with a niche has a name of its own, so a table written with the tag
    // encoding, before the type declared the niche, keeps its identity
    #[test]
    fn option_type_name_records_the_niche() {
        assert_eq!(<Option<u32> as Value>::type_name().name(), "Option<u32>");
        assert_eq!(
            <Option<NonZeroU32> as Value>::type_name().name(),
            "niche::Option<NonZeroU32>"
        );
        assert!(!<Option<NonZeroU32> as Value>::type_name().is_user_defined());
        // Through the types that forward a niche, and into the composites that embed the name
        assert_eq!(
            <Option<(NonZeroU32,)> as Value>::type_name().name(),
            "niche::Option<(NonZeroU32,)>"
        );
        assert_eq!(
            <Vec<Option<NonZeroU32>> as Value>::type_name().name(),
            "Vec<niche::Option<NonZeroU32>>"
        );
        // `Option` itself declares none, so the outer one keeps the plain name
        assert_eq!(
            <Option<Option<NonZeroU32>> as Value>::type_name().name(),
            "Option<niche::Option<NonZeroU32>>"
        );
        let user = <Option<NonMaxU16> as Value>::type_name();
        assert_eq!(user.name(), "niche::Option<test::NonMaxU16>");
        assert!(user.is_user_defined());

        #[cfg(feature = "experimental-niches")]
        {
            assert_eq!(
                <Option<&str> as Value>::type_name().name(),
                "niche::Option<&str>"
            );
            assert_eq!(
                <Option<String> as Value>::type_name().name(),
                "niche::Option<String>"
            );
            // The legacy type has the name and the encoding `&str` had without the niche
            assert_eq!(
                <crate::Legacy<&str> as Value>::type_name(),
                <&str as Value>::type_name()
            );
            assert_eq!(
                <Option<crate::Legacy<&str>> as Value>::type_name(),
                TypeName::internal("Option<&str>").into_composite(false)
            );
            assert_eq!(<Option<crate::Legacy<&str>> as Value>::as_bytes(&None), [0]);
            assert_eq!(
                <Option<crate::Legacy<&str>> as Value>::as_bytes(&Some("a")),
                [1, b'a']
            );
        }
        #[cfg(not(feature = "experimental-niches"))]
        {
            assert_eq!(<Option<&str> as Value>::type_name().name(), "Option<&str>");
            assert_eq!(
                <Option<String> as Value>::type_name().name(),
                "Option<String>"
            );
        }
    }

    // A fixed width key type with a niche: a `u16` that is never `u16::MAX`, which frees that
    // encoding. It is the bytewise largest one, so nothing may rely on a niche sorting first.
    #[derive(Debug)]
    struct NonMaxU16;

    impl Value for NonMaxU16 {
        type SelfType<'a> = u16;
        type AsBytes<'a> = [u8; 2];

        const NICHE: Option<&'static [u8]> = Some(&[0xff, 0xff]);

        fn fixed_width() -> Option<usize> {
            Some(2)
        }

        fn from_bytes<'a>(data: &'a [u8]) -> u16
        where
            Self: 'a,
        {
            // The niche is `Option`'s to decode, never this type's
            assert_ne!(data, Self::NICHE.unwrap());
            u16::from_le_bytes(data.try_into().unwrap())
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a u16) -> [u8; 2]
        where
            Self: 'b,
        {
            assert_ne!(*value, u16::MAX);
            value.to_le_bytes()
        }

        fn type_name() -> TypeName {
            TypeName::new("test::NonMaxU16")
        }
    }

    impl Key for NonMaxU16 {
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
        }
    }

    // `Option` of a fixed width type with a niche is that width too: the niche encodes `None`,
    // and everything else is an encoding of the type itself
    #[test]
    fn fixed_width_option_niche() {
        type Opt = Option<NonMaxU16>;
        assert_eq!(<Opt as Value>::fixed_width(), Some(2));
        assert_eq!(<Opt as Value>::as_bytes(&None), [0xff, 0xff]);
        assert_eq!(<Opt as Value>::as_bytes(&Some(1)), [1, 0]);
        assert_eq!(
            <Opt as Key>::min_encoded_key().as_deref(),
            Some([0xff, 0xff].as_slice())
        );

        let values = [None, Some(0), Some(1), Some(u16::MAX - 1)];
        for value in values {
            let encoded = <Opt as Value>::as_bytes(&value);
            assert_eq!(encoded.len(), 2);
            assert_eq!(<Opt as Value>::from_bytes(&encoded), value);
        }
        // `None` sorts below every `Some`, as it does with a tag, although its bytes are the
        // largest. A fixed width `Option` is compared at that width, so `left` is the separator.
        for left in values {
            for right in values {
                let a = <Opt as Value>::as_bytes(&left);
                let b = <Opt as Value>::as_bytes(&right);
                assert_eq!(<Opt as Key>::compare(&a, &b), left.cmp(&right));
                if left < right {
                    assert_eq!(<Opt as Key>::separator(&a, &b), a);
                }
            }
        }

        // Composites see the same width: a `Vec` stores no element lengths, and an outer
        // `Option` needs a tag, since `Option` declares no niche of its own
        assert_eq!(
            <Vec<Opt> as Value>::as_bytes(&vec![None, Some(1)]),
            [2, 0xff, 0xff, 1, 0]
        );
        assert_eq!(<Option<Opt> as Value>::fixed_width(), Some(3));
        assert_eq!(<Option<Opt> as Value>::as_bytes(&None), [0, 0, 0]);
        assert_eq!(
            <Option<Opt> as Value>::as_bytes(&Some(None)),
            [1, 0xff, 0xff]
        );
        assert_eq!(<Option<Opt> as Value>::as_bytes(&Some(Some(1))), [1, 1, 0]);
    }

    // A variable width key type with a niche of `N` bytes, so that empty and multi-byte niches
    // can be tried. Encoded as its bytes, which are never `N` bytes of `0xff`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct NicheBytes<'a, const N: usize>(&'a [u8]);

    impl<const N: usize> Value for NicheBytes<'_, N> {
        type SelfType<'a>
            = NicheBytes<'a, N>
        where
            Self: 'a;
        type AsBytes<'a>
            = &'a [u8]
        where
            Self: 'a;

        const NICHE: Option<&'static [u8]> = Some(&[0xff; N]);

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> NicheBytes<'a, N>
        where
            Self: 'a,
        {
            // The niche is `Option`'s to decode, never this type's
            assert_ne!(data, Self::NICHE.unwrap());
            NicheBytes(data)
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a NicheBytes<'b, N>) -> &'a [u8]
        where
            Self: 'b,
        {
            assert_ne!(value.0, Self::NICHE.unwrap());
            value.0
        }

        fn type_name() -> TypeName {
            TypeName::new(&format!("test::NicheBytes<{N}>"))
        }
    }

    impl<const N: usize> Key for NicheBytes<'_, N> {
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            assert_ne!(data1, Self::NICHE.unwrap());
            assert_ne!(data2, Self::NICHE.unwrap());
            data1.cmp(data2)
        }

        // A prefix of `right` separates, as it does for `&[u8]`, unless it is the niche, which
        // is not an encoding and so may not be returned
        fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
            let separator = <&[u8] as Key>::separator(left, right);
            if separator.as_ref() == Self::NICHE.unwrap() {
                Cow::Borrowed(left)
            } else {
                separator
            }
        }
    }

    // `Option` of a variable width type with a niche encodes `None` as the niche, whatever its
    // length, and a `Some` exactly as the type itself
    #[test]
    fn variable_width_option_niche() {
        fn check<const N: usize>() {
            type Opt<'a, const N: usize> = Option<NicheBytes<'a, N>>;
            type Nested<'a, const N: usize> = Option<Opt<'a, N>>;
            let niche = NicheBytes::<N>::NICHE.unwrap();
            assert_eq!(<Opt<N> as Value>::fixed_width(), None);
            assert_eq!(<Opt<N> as Value>::as_bytes(&None), niche);
            assert_eq!(<Opt<N> as Value>::from_bytes(niche), None);
            assert_eq!(<Opt<N> as Key>::min_encoded_key().as_deref(), Some(niche));

            // Every other byte string is a `Some`: the empty one, and prefixes of the niche
            let samples: &[&[u8]] = &[
                b"",
                b"\0",
                b"\0\0",
                b"aaaa-long",
                b"bbbb-long",
                &[0xff],
                &[0xff; 2],
                &[0xff; 3],
                &[0xff; 4],
            ];
            let mut values = vec![None];
            for &bytes in samples {
                if bytes != niche {
                    let value = Some(NicheBytes::<N>(bytes));
                    assert_eq!(<Opt<N> as Value>::as_bytes(&value), bytes);
                    assert_eq!(<Opt<N> as Value>::from_bytes(bytes), value);
                    values.push(value);
                }
            }
            // `None` sorts first, then the values as their bytes, which is how the `Option`
            // orders in memory
            for &left in &values {
                for &right in &values {
                    let a = <Opt<N> as Value>::as_bytes(&left);
                    let b = <Opt<N> as Value>::as_bytes(&right);
                    assert_eq!(<Opt<N> as Key>::compare(&a, &b), left.cmp(&right));
                    if left < right {
                        // A separator is no longer than `left`, sorts between the two, and is
                        // an encoding
                        let separator = <Opt<N> as Key>::separator(&a, &b);
                        assert!(separator.len() <= a.len());
                        assert!(<Opt<N> as Key>::compare(&a, &separator).is_le());
                        assert!(<Opt<N> as Key>::compare(&separator, &b).is_lt());
                        let decoded = <Opt<N> as Value>::from_bytes(&separator);
                        assert_eq!(<Opt<N> as Value>::as_bytes(&decoded), separator.as_ref());
                    }
                }
            }
            // The payloads separate as `&[u8]` does, with no tag in front of them
            assert_eq!(
                <Opt<N> as Key>::separator(b"aaaa-long", b"bbbb-long").as_ref(),
                b"b"
            );

            // An outer `Option` needs a tag, since `Option` declares no niche of its own
            let cases = [None, Some(None), Some(Some(NicheBytes::<N>(b"\0")))];
            for &value in &cases {
                let bytes = <Nested<N> as Value>::as_bytes(&value);
                assert_eq!(<Nested<N> as Value>::from_bytes(&bytes), value);
                for &other in &cases {
                    let other_bytes = <Nested<N> as Value>::as_bytes(&other);
                    assert_eq!(
                        <Nested<N> as Key>::compare(&bytes, &other_bytes),
                        value.cmp(&other)
                    );
                }
            }
        }

        check::<0>();
        check::<1>();
        check::<3>();
    }

    // Each `NonZero*` encodes as the primitive it wraps, and `Option` of it at the same width,
    // with zero as `None`
    macro_rules! nonzero_encoding_test {
        ($name:ident, $t:ty, $prim:ty, [$($value:expr),+]) => {
            #[test]
            fn $name() {
                let zero = [0; size_of::<$prim>()];
                assert_eq!(<$t as Value>::NICHE, Some(zero.as_slice()));
                assert_eq!(<$t as Value>::fixed_width(), <$prim as Value>::fixed_width());
                assert_eq!(
                    <Option<$t> as Value>::fixed_width(),
                    <$prim as Value>::fixed_width()
                );
                assert_eq!(<Option<$t> as Value>::as_bytes(&None), zero);
                assert_eq!(<Option<$t> as Value>::from_bytes(&zero), None);
                assert_eq!(
                    <Option<$t> as Key>::min_encoded_key().as_deref(),
                    Some(zero.as_slice())
                );
                // A table of the primitive may hold zeros, so it must not open as this type
                assert_ne!(<$t as Value>::type_name(), <$prim as Value>::type_name());

                let mut options = vec![None];
                for primitive in [$($value),+] {
                    let value = <$t>::new(primitive).unwrap();
                    let encoded = <$prim as Value>::as_bytes(&primitive);
                    assert_eq!(<$t as Value>::as_bytes(&value), encoded);
                    assert_eq!(<$t as Value>::from_bytes(&encoded), value);
                    assert_eq!(<Option<$t> as Value>::as_bytes(&Some(value)), encoded);
                    assert_eq!(<Option<$t> as Value>::from_bytes(&encoded), Some(value));
                    options.push(Some(value));
                }
                // Both order as the primitive does, with `None` first, which is how they order
                // in memory. A fixed width `Option` is compared at that width, so `left` is the
                // separator.
                for &left in &options {
                    for &right in &options {
                        let a = <Option<$t> as Value>::as_bytes(&left);
                        let b = <Option<$t> as Value>::as_bytes(&right);
                        assert_eq!(<Option<$t> as Key>::compare(&a, &b), left.cmp(&right));
                        if let (Some(left), Some(right)) = (left, right) {
                            assert_eq!(<$t as Key>::compare(&a, &b), left.cmp(&right));
                        }
                        if left < right {
                            assert_eq!(<Option<$t> as Key>::separator(&a, &b), a);
                        }
                    }
                }
            }
        };
    }

    nonzero_encoding_test!(nonzero_u8_encoding, NonZeroU8, u8, [1, 128, u8::MAX]);
    nonzero_encoding_test!(nonzero_u16_encoding, NonZeroU16, u16, [1, 256, u16::MAX]);
    nonzero_encoding_test!(nonzero_u32_encoding, NonZeroU32, u32, [1, 256, u32::MAX]);
    nonzero_encoding_test!(nonzero_u64_encoding, NonZeroU64, u64, [1, 256, u64::MAX]);
    nonzero_encoding_test!(
        nonzero_u128_encoding,
        NonZeroU128,
        u128,
        [1, 256, u128::MAX]
    );
    nonzero_encoding_test!(
        nonzero_i8_encoding,
        NonZeroI8,
        i8,
        [i8::MIN, -1, 1, i8::MAX]
    );
    nonzero_encoding_test!(
        nonzero_i16_encoding,
        NonZeroI16,
        i16,
        [i16::MIN, -256, -1, 1, 256, i16::MAX]
    );
    nonzero_encoding_test!(
        nonzero_i32_encoding,
        NonZeroI32,
        i32,
        [i32::MIN, -256, -1, 1, 256, i32::MAX]
    );
    nonzero_encoding_test!(
        nonzero_i64_encoding,
        NonZeroI64,
        i64,
        [i64::MIN, -256, -1, 1, 256, i64::MAX]
    );
    nonzero_encoding_test!(
        nonzero_i128_encoding,
        NonZeroI128,
        i128,
        [i128::MIN, -256, -1, 1, 256, i128::MAX]
    );

    #[test]
    fn array_separator() {
        // (left, right, the shortest separator)
        let cases: &[([&str; 2], [&str; 2], [&str; 2])] = &[
            // The shortened element sorts strictly above `left`'s, so the tail is discarded
            (
                ["abc0suffix", "left"],
                ["abc1suffix", "right"],
                ["abc1", ""],
            ),
            // Shortening the last element leaves the elements before it alone
            (
                ["same", "abc0suffix"],
                ["same", "abc1suffix"],
                ["same", "abc1"],
            ),
            // Nothing sorts between these first elements, so the tail has to stay: a separator
            // equal to `left`'s element does not resolve the comparison against `left`
            (["abc", "tail"], ["abc-suffix", "x"], ["abc", "tail"]),
        ];

        for &(left, right, expected) in cases {
            let left = <[&str; 2] as Value>::as_bytes(&left);
            let right = <[&str; 2] as Value>::as_bytes(&right);
            let separator = <[&str; 2] as Key>::separator(&left, &right);
            assert_eq!(separator, <[&str; 2] as Value>::as_bytes(&expected));
            assert!(<[&str; 2] as Key>::compare(&left, &separator).is_le());
            assert!(<[&str; 2] as Key>::compare(&separator, &right).is_lt());
            assert!(separator.len() <= left.len());
        }
    }

    // A key type whose smallest value encodes to more bytes than an ordinary one. Nothing stops
    // an implementation from being shaped this way, so substituting the minimum into a separator
    // has to be able to make the result longer rather than shorter.
    #[derive(Debug)]
    struct WideMinimum;

    const WIDE_MINIMUM: [u8; 64] = [0; 64];

    impl Value for WideMinimum {
        type SelfType<'a> = &'a [u8];
        type AsBytes<'a>
            = &'a [u8]
        where
            Self: 'a;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
        where
            Self: 'a,
        {
            data
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a &'b [u8]) -> &'a [u8]
        where
            Self: 'b,
        {
            value
        }

        fn type_name() -> TypeName {
            TypeName::new("test::WideMinimum")
        }
    }

    impl Key for WideMinimum {
        // Sorts as its bytes, except that the smallest value sorts below everything
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            match (data1 == WIDE_MINIMUM, data2 == WIDE_MINIMUM) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => data1.cmp(data2),
            }
        }

        // A prefix of `right` separates any two values that are not the smallest one, which is
        // all this type is used with
        fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
            <&[u8] as Key>::separator(left, right)
        }

        fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
            Some(Cow::Borrowed(&WIDE_MINIMUM))
        }
    }

    // Discarding an element replaces it with the smallest encoding, which can be longer than the
    // element it replaces. The whole key is kept when that would make the separator longer.
    #[test]
    fn separator_is_not_grown_by_a_wide_minimum() {
        type Key3 = [WideMinimum; 3];

        // It really is a minimum: at or below every value, including itself
        assert!(WideMinimum::compare(&WIDE_MINIMUM, b"a").is_lt());
        assert!(WideMinimum::compare(b"a", &WIDE_MINIMUM).is_gt());
        assert!(WideMinimum::compare(&WIDE_MINIMUM, &WIDE_MINIMUM).is_eq());

        let left = <Key3 as Value>::as_bytes(&[b"aaa", b"x", b"y"]);
        let right = <Key3 as Value>::as_bytes(&[b"bbb", b"x", b"y"]);
        // Shortening the first element saves two bytes, but the two the tail would discard to
        // cost 64 each
        let separator = <Key3 as Key>::separator(&left, &right);
        assert_eq!(separator, left);
        // ...and it is an encoding of the key type, as every separator must be
        let value = <Key3 as Value>::from_bytes(&separator);
        assert_eq!(<Key3 as Value>::as_bytes(&value), separator.as_ref());
    }

    // An array of a fixed width element type is itself fixed width, so it must not shorten
    #[test]
    fn fixed_width_array_separator_returns_full_key() {
        let left = <[u64; 2] as Value>::as_bytes(&[1, 1]);
        let right = <[u64; 2] as Value>::as_bytes(&[2, 2]);
        assert_eq!(<[u64; 2] as Key>::separator(&left, &right), left);
    }

    // A discarded element is replaced by its own smallest encoding, not by nothing. A lookup key
    // that matches the separator up to the discarded element is compared against it, and `Option`
    // reads a tag byte, so an empty element would leave `compare()` indexing past the end.
    #[test]
    fn discarded_array_elements_stay_readable_by_compare() {
        type Key2 = [Option<&'static str>; 2];
        let left = <Key2 as Value>::as_bytes(&[Some("aaaa"), Some("zzzz")]);
        let right = <Key2 as Value>::as_bytes(&[Some("bbbb"), Some("yyyy")]);
        let separator = <Key2 as Key>::separator(&left, &right);
        assert_eq!(
            separator,
            // The `Some("b")` the first element shortened to, then `None` in place of the second
            <Key2 as Value>::as_bytes(&[Some("b"), None])
        );
        assert!(<Key2 as Key>::compare(&left, &separator).is_le());
        assert!(<Key2 as Key>::compare(&separator, &right).is_lt());

        // The comparison a lookup makes once its first element matches the separator's
        let probe = <Key2 as Value>::as_bytes(&[Some("b"), Some("x")]);
        assert!(<Key2 as Key>::compare(&probe, &separator).is_gt());
    }

    fn assert_least<K: Key>(value: &K::SelfType<'_>) {
        let encoded = K::as_bytes(value);
        let min = K::min_encoded_key().unwrap();
        assert!(K::compare(&min, encoded.as_ref()).is_le());
        assert!(K::compare(&min, &min).is_eq());
        // It is an encoding like any other, so a fixed width type's is that wide
        if let Some(width) = K::fixed_width() {
            assert_eq!(min.len(), width);
        }
    }

    #[test]
    fn min_encoded_keys() {
        // Types whose encodings order as their bytes bottom out at the empty one
        assert_eq!(
            <&[u8] as Key>::min_encoded_key().as_deref(),
            Some(b"".as_slice())
        );
        assert_eq!(
            <&str as Key>::min_encoded_key().as_deref(),
            Some(b"".as_slice())
        );
        assert_eq!(
            <String as Key>::min_encoded_key().as_deref(),
            Some(b"".as_slice())
        );
        // `Option` needs its tag, which is also what `None` encodes to
        #[cfg(not(feature = "experimental-niches"))]
        assert_eq!(
            <Option<&str> as Key>::min_encoded_key().as_deref(),
            Some([0].as_slice())
        );
        // Unless the type declares a niche, which `None` then encodes to
        #[cfg(feature = "experimental-niches")]
        assert_eq!(
            <Option<&str> as Key>::min_encoded_key().as_deref(),
            Some([0xff].as_slice())
        );
        // A fixed width `T` pads that tag out to the width of a `Some`
        assert_eq!(
            <Option<u64> as Key>::min_encoded_key().as_deref(),
            Some([0; 9].as_slice())
        );
        // A niche is what `None` encodes to, so it is the smallest encoding of that `Option`
        assert_eq!(
            <Option<NonMaxU16> as Key>::min_encoded_key().as_deref(),
            Some([0xff, 0xff].as_slice())
        );
        assert_eq!(
            <Option<NicheBytes<1>> as Key>::min_encoded_key().as_deref(),
            Some([0xff].as_slice())
        );
        // A one element tuple is encoded exactly as its element
        assert_eq!(
            <(&str,) as Key>::min_encoded_key().as_deref(),
            Some(b"".as_slice())
        );
        // Types that do not implement it keep the default
        assert_eq!(<u64 as Key>::min_encoded_key(), None);
        assert_eq!(<[&str; 2] as Key>::min_encoded_key(), None);
        assert_eq!(<(&str, &str) as Key>::min_encoded_key(), None);

        // Every one of them must be something `compare()` accepts, and sort at or below the
        // encoding of any value
        assert_least::<&[u8]>(&b"anything".as_slice());
        assert_least::<&str>(&"anything");
        assert_least::<String>(&String::from("anything"));
        assert_least::<Option<&str>>(&Some("anything"));
        assert_least::<Option<&str>>(&None);
        assert_least::<Option<u64>>(&Some(1));
        assert_least::<Option<u64>>(&None);
        assert_least::<Option<NonMaxU16>>(&Some(0));
        assert_least::<Option<NonMaxU16>>(&None);
        assert_least::<Option<NicheBytes<1>>>(&Some(NicheBytes(b"")));
        assert_least::<Option<NicheBytes<1>>>(&None);
        assert_least::<Option<NonZeroI32>>(&NonZeroI32::new(i32::MIN));
        assert_least::<Option<NonZeroI32>>(&None);
    }

    #[test]
    fn default_separator_returns_full_key() {
        let left = 1u64.to_le_bytes();
        let right = 2u64.to_le_bytes();
        assert_eq!(<u64 as Key>::separator(&left, &right).as_ref(), left);
    }

    // A user-defined type whose name deliberately collides with the built-in `u32`, and whose
    // fixed width matches it. Before composite classifications bubbled up, `Option<FakeU32>` and
    // `Option<u32>` produced byte-identical `TypeName`s, letting one silently open as the other.
    #[derive(Debug)]
    struct FakeU32;

    impl Value for FakeU32 {
        type SelfType<'a> = u32;
        type AsBytes<'a> = [u8; 4];

        fn fixed_width() -> Option<usize> {
            Some(4)
        }

        fn from_bytes<'a>(data: &'a [u8]) -> u32
        where
            Self: 'a,
        {
            u32::from_le_bytes(data.try_into().unwrap())
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a u32) -> [u8; 4]
        where
            Self: 'b,
        {
            value.to_le_bytes()
        }

        fn type_name() -> TypeName {
            TypeName::new("u32")
        }
    }

    impl Key for FakeU32 {
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            data1.cmp(data2)
        }
    }

    #[test]
    fn builtin_composites_use_internal3_and_match_legacy() {
        fn assert_internal3(name: TypeName) {
            assert!(matches!(name.classification, TypeClassification::Internal3));
        }

        assert_internal3(<Option<u32> as Value>::type_name());
        assert_internal3(<Option<NonZeroU32> as Value>::type_name());
        assert_internal3(<Vec<u32> as Value>::type_name());
        assert_internal3(<[u32; 3] as Value>::type_name());
        assert_internal3(<&[u8; 3] as Value>::type_name());
        assert_internal3(<(u32,) as Value>::type_name());
        assert_internal3(<(u32, u64) as Value>::type_name());
        assert_internal3(<(u32, &str) as Value>::type_name());
        assert_internal3(<Vec<Option<u32>> as Value>::type_name());

        // The owned and borrowed byte-array types remain compatible.
        assert_eq!(
            <[u8; 3] as Value>::type_name(),
            <&[u8; 3] as Value>::type_name()
        );

        // Built-in composites accept their pre-4.2 classifications. Variable-width tuples only
        // accept Internal2, preserving the encoding distinction introduced in version 3.0.
        assert!(
            <Option<u32> as Value>::type_name().matches_legacy(&TypeName::internal("Option<u32>"))
        );
        assert!(
            <(u32, u64) as Value>::type_name().matches_legacy(&TypeName::internal("(u32,u64)"))
        );
        assert!(
            <(u32, &str) as Value>::type_name().matches_legacy(&TypeName::internal2("(u32,&str)"))
        );
        assert!(
            !<(u32, &str) as Value>::type_name().matches_legacy(&TypeName::internal("(u32,&str)"))
        );
    }

    #[test]
    fn composite_of_user_type_no_longer_collides_with_builtin() {
        // The bug: these pairs were equal before the fix.
        assert_ne!(
            <Option<u32> as Value>::type_name(),
            <Option<FakeU32> as Value>::type_name()
        );
        assert_ne!(
            <Vec<u32> as Value>::type_name(),
            <Vec<FakeU32> as Value>::type_name()
        );
        assert_ne!(
            <[u32; 2] as Value>::type_name(),
            <[FakeU32; 2] as Value>::type_name()
        );
        assert_ne!(
            <(u32, u64) as Value>::type_name(),
            <(FakeU32, u64) as Value>::type_name()
        );
        assert_ne!(
            <(u32, &str) as Value>::type_name(),
            <(FakeU32, &str) as Value>::type_name()
        );

        // Only the classification differs; the human-readable name string is unchanged, and the
        // user composite is classified user-defined.
        assert_eq!(
            <Option<u32> as Value>::type_name().name(),
            <Option<FakeU32> as Value>::type_name().name()
        );
        assert!(<Option<FakeU32> as Value>::type_name().is_user_defined());
        // Bubbling is transitive through nesting.
        assert!(<Vec<Option<FakeU32>> as Value>::type_name().is_user_defined());
    }

    #[test]
    fn legacy_matching_accepts_pre_bubbling_spelling() {
        // Option/Vec/array/(T,) were stored as Internal in every prior version.
        assert!(
            <Option<FakeU32> as Value>::type_name()
                .matches_legacy(&TypeName::internal("Option<u32>"))
        );
        assert!(
            <Vec<FakeU32> as Value>::type_name().matches_legacy(&TypeName::internal("Vec<u32>"))
        );
        assert!(<(FakeU32,) as Value>::type_name().matches_legacy(&TypeName::internal("(u32,)")));
        // Fixed-width tuple: legacy spelling is Internal.
        assert!(
            <(FakeU32, u64) as Value>::type_name().matches_legacy(&TypeName::internal("(u32,u64)"))
        );
        // Variable-width tuple: legacy spelling is Internal2, NOT Internal. redb 2.6 used a
        // different variable-width tuple encoding (it had no Internal2), so its Internal-tagged
        // tables are deliberately not re-accepted.
        assert!(
            <(FakeU32, &str) as Value>::type_name()
                .matches_legacy(&TypeName::internal2("(u32,&str)"))
        );
        assert!(
            !<(FakeU32, &str) as Value>::type_name()
                .matches_legacy(&TypeName::internal("(u32,&str)"))
        );

        // A legacy match must still agree on the name.
        assert!(
            !<Option<FakeU32> as Value>::type_name()
                .matches_legacy(&TypeName::internal("Option<u64>"))
        );

        // Crucially, a flat user-defined type carries no legacy classification, so it never
        // matches an `Internal` stored name -- this is what keeps a user type named "u32" from
        // aliasing the built-in `u32` at the top level.
        assert!(!<FakeU32 as Value>::type_name().matches_legacy(&TypeName::internal("u32")));
    }
}
