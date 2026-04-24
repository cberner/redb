use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;
use std::mem::size_of;
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
}

impl TypeClassification {
    fn to_byte(&self) -> u8 {
        match self {
            TypeClassification::Internal => 1,
            TypeClassification::UserDefined => 2,
            TypeClassification::Internal2 => 3,
        }
    }

    fn from_byte(value: u8) -> Self {
        match value {
            1 => TypeClassification::Internal,
            2 => TypeClassification::UserDefined,
            3 => TypeClassification::Internal2,
            _ => unreachable!(),
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct TypeName {
    classification: TypeClassification,
    name: String,
}

impl TypeName {
    /// It is recommended that `name` be prefixed with the crate name to minimize the chance of
    /// it coliding with another user defined type
    pub fn new(name: &str) -> Self {
        Self {
            classification: TypeClassification::UserDefined,
            name: name.to_string(),
        }
    }

    pub(crate) fn internal(name: &str) -> Self {
        Self {
            classification: TypeClassification::Internal,
            name: name.to_string(),
        }
    }

    pub(crate) fn internal2(name: &str) -> Self {
        Self {
            classification: TypeClassification::Internal2,
            name: name.to_string(),
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
        let name = std::str::from_utf8(&bytes[1..]).unwrap().to_string();

        Self {
            classification,
            name,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
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

    /// Width of a fixed type, or None for variable width
    fn fixed_width() -> Option<usize>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    ///
    /// Note: Implementations may assume that `data` is the return value of `as_bytes(&v)` for some `v` of type `Self`.
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a;

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
    /// The implementation must ensure there is a total order
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;

    /// Compute a short separator key S where `left <= S < right` according
    /// to this type's ordering. The separator is stored in B-tree branch nodes
    /// to route searches; shorter separators increase branch fan-out and
    /// improve read performance.
    ///
    /// The returned bytes must be a valid serialized form for comparisons via
    /// [`Key::compare`]. The result S must satisfy:
    /// - `Self::compare(left, &S)` is `Equal` or `Less`
    /// - `Self::compare(&S, right)` is `Less`
    ///
    /// The default returns the full `left` key (always correct, no compression).
    ///
    /// For types whose byte serialization uses lexicographic ordering,
    /// override this using the provided [`byte_key_shortest_separator`] helper
    /// to get significant B-tree performance improvements.
    fn shortest_separator(left: &[u8], _right: &[u8]) -> Vec<u8> {
        left.to_vec()
    }
}

/// Compute the shortest separator for key types with lexicographic byte ordering.
///
/// Use this in your [`Key::shortest_separator`] implementation when the type's
/// serialized byte form is compared lexicographically (e.g., raw byte slices).
///
/// Returns the shortest byte string S such that `left <= S < right`.
pub fn byte_key_shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    let common_len = left
        .iter()
        .zip(right.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(left.len().min(right.len()));

    if common_len >= left.len() {
        return left.to_vec();
    }
    if common_len + 1 < right.len() {
        return right[..common_len + 1].to_vec();
    }
    if left[common_len] + 1 < right[common_len] {
        let mut sep = left[..common_len].to_vec();
        sep.push(left[common_len] + 1);
        return sep;
    }
    left.to_vec()
}

/// Compute the shortest separator for UTF-8 string key types.
///
/// Like [`byte_key_shortest_separator`], but ensures the result is valid UTF-8.
/// UTF-8 strings compare lexicographically by byte value, but the separator must
/// remain valid UTF-8 since [`Key::compare`] will decode it.
pub fn str_key_shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    let common_len = left
        .iter()
        .zip(right.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(left.len().min(right.len()));

    if common_len >= left.len() {
        return left.to_vec();
    }

    // For valid UTF-8, the divergence point is always at a character boundary.
    // Include the full first differing character from `right` so the result
    // remains valid UTF-8.
    let char_len = match right[common_len] {
        0..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1,
    };
    let end = common_len + char_len;
    if end < right.len() {
        return right[..end].to_vec();
    }

    left.to_vec()
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
    fn from_bytes<'a>(_data: &'a [u8]) -> ()
    where
        Self: 'a,
    {
        ()
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
    fn compare(_data1: &[u8], _data2: &[u8]) -> Ordering {
        Ordering::Equal
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

    fn fixed_width() -> Option<usize> {
        Some(1)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> bool
    where
        Self: 'a,
    {
        match data[0] {
            0 => false,
            1 => true,
            _ => unreachable!(),
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
        let value1 = Self::from_bytes(data1);
        let value2 = Self::from_bytes(data2);
        value1.cmp(&value2)
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
        T::fixed_width().map(|x| x + 1)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Option<T::SelfType<'a>>
    where
        Self: 'a,
    {
        match data[0] {
            0 => None,
            1 => Some(T::from_bytes(&data[1..])),
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
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
        TypeName::internal(&format!("Option<{}>", T::type_name().name()))
    }
}

impl<T: Key> Key for Option<T> {
    #[allow(clippy::collapsible_else_if)]
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        if data1[0] == 0 {
            if data2[0] == 0 {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        } else {
            if data2[0] == 0 {
                Ordering::Greater
            } else {
                T::compare(&data1[1..], &data2[1..])
            }
        }
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

    fn shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
        byte_key_shortest_separator(left, right)
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
        data.try_into().unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8; N]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("[u8;{N}]"))
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
        let mut result = Vec::with_capacity(N);
        if let Some(fixed) = T::fixed_width() {
            for i in 0..N {
                result.push(T::from_bytes(&data[fixed * i..fixed * (i + 1)]));
            }
        } else {
            // Set offset to the first data item
            let mut start = size_of::<u32>() * N;
            for i in 0..N {
                let range = size_of::<u32>() * i..size_of::<u32>() * (i + 1);
                let end = u32::from_le_bytes(data[range].try_into().unwrap()) as usize;
                result.push(T::from_bytes(&data[start..end]));
                start = end;
            }
        }
        result.try_into().unwrap()
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
        TypeName::internal(&format!("[{};{N}]", T::type_name().name()))
    }
}

impl<const N: usize, T: Key> Key for [T; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        if let Some(fixed) = T::fixed_width() {
            for i in 0..N {
                let range = fixed * i..fixed * (i + 1);
                let comparison = T::compare(&data1[range.clone()], &data2[range]);
                if !comparison.is_eq() {
                    return comparison;
                }
            }
        } else {
            // Set offset to the first data item
            let mut start1 = size_of::<u32>() * N;
            let mut start2 = size_of::<u32>() * N;
            for i in 0..N {
                let range = size_of::<u32>() * i..size_of::<u32>() * (i + 1);
                let end1 = u32::from_le_bytes(data1[range.clone()].try_into().unwrap()) as usize;
                let end2 = u32::from_le_bytes(data2[range].try_into().unwrap()) as usize;
                let comparison = T::compare(&data1[start1..end1], &data2[start2..end2]);
                if !comparison.is_eq() {
                    return comparison;
                }
                start1 = end1;
                start2 = end2;
            }
        }
        Ordering::Equal
    }
}

impl Value for &str {
    type SelfType<'a>
        = &'a str
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a str
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a str
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap()
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

impl Key for &str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let str1 = Self::from_bytes(data1);
        let str2 = Self::from_bytes(data2);
        str1.cmp(str2)
    }

    fn shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
        str_key_shortest_separator(left, right)
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

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> String
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap().to_string()
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
        let str1 = std::str::from_utf8(data1).unwrap();
        let str2 = std::str::from_utf8(data2).unwrap();
        str1.cmp(str2)
    }

    fn shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
        str_key_shortest_separator(left, right)
    }
}

impl Value for char {
    type SelfType<'a> = char;
    type AsBytes<'a>
        = [u8; 3]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(3)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> char
    where
        Self: 'a,
    {
        char::from_u32(u32::from_le_bytes([data[0], data[1], data[2], 0])).unwrap()
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
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}

macro_rules! le_value {
    ($t:ty) => {
        impl Value for $t {
            type SelfType<'a> = $t;
            type AsBytes<'a>
                = [u8; std::mem::size_of::<$t>()]
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                Some(std::mem::size_of::<$t>())
            }

            fn from_bytes<'a>(data: &'a [u8]) -> $t
            where
                Self: 'a,
            {
                <$t>::from_le_bytes(data.try_into().unwrap())
            }

            fn as_bytes<'a, 'b: 'a>(
                value: &'a Self::SelfType<'b>,
            ) -> [u8; std::mem::size_of::<$t>()]
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
                Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
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
