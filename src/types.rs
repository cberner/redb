use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;

pub(crate) const MAX_ALIGNMENT: usize = 16;

#[derive(Clone)]
pub struct AlignedVec<const N: usize> {
    aligned_start: usize,
    data: Vec<u8>,
}

impl<const N: usize> AlignedVec<N> {
    pub fn new() -> Self {
        Self::from_slice(&[])
    }

    /// Construct a new AlignedVec from the given slice
    pub fn from_slice(slice: &[u8]) -> Self {
        let mut data = Vec::with_capacity(N + slice.len());
        let remainder = data.as_ptr() as usize % N;
        let aligned_start = if remainder > 0 {
            let padding = N - remainder;
            data.resize(padding, 0);
            padding
        } else {
            0
        };
        data.extend_from_slice(slice);

        Self {
            aligned_start,
            data,
        }
    }

    /// The length in bytes
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if vec has length 0
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<const N: usize> Default for AlignedVec<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> AsAlignedSlice<N> for AlignedVec<N> {
    fn as_aligned(&self) -> AlignedSlice<N> {
        AlignedSlice::new(&self.data[self.aligned_start..])
    }
}

#[derive(Copy, Clone)]
pub struct AlignedSlice<'a, const N: usize> {
    data: &'a [u8],
}

impl<'a, const N: usize> AlignedSlice<'a, N> {
    /// Constructs a new AlignedSlice. `data` must start and end at a multiple of `N` bytes
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(0, data.as_ptr() as usize % N);
        assert_eq!(0, data.len() % N);
        Self { data }
    }

    /// Returns a slice which is aligned to a multiple of `N` bytes
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// The length in bytes
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if slice has length 0
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns an owned `AlignedVec` with the same alignment
    pub fn to_vec(&self) -> AlignedVec<N> {
        AlignedVec::from_slice(self.data)
    }
}

pub trait AsAlignedSlice<const N: usize> {
    fn as_aligned(&self) -> AlignedSlice<N>;
}

impl AsAlignedSlice<1> for &[u8] {
    fn as_aligned(&self) -> AlignedSlice<1> {
        AlignedSlice::new(self)
    }
}

impl AsAlignedSlice<1> for Vec<u8> {
    fn as_aligned(&self) -> AlignedSlice<1> {
        AlignedSlice::new(self.as_slice())
    }
}

impl<const N: usize> AsAlignedSlice<1> for [u8; N] {
    fn as_aligned(&self) -> AlignedSlice<1> {
        AlignedSlice::new(self.as_slice())
    }
}

impl<const N: usize> AsAlignedSlice<1> for &[u8; N] {
    fn as_aligned(&self) -> AlignedSlice<1> {
        AlignedSlice::new(self.as_slice())
    }
}

impl AsAlignedSlice<1> for &str {
    fn as_aligned(&self) -> AlignedSlice<1> {
        AlignedSlice::new(self.as_bytes())
    }
}

#[derive(Eq, PartialEq, Clone, Debug)]
enum TypeClassification {
    Internal,
    UserDefined,
}

impl TypeClassification {
    fn to_byte(&self) -> u8 {
        match self {
            TypeClassification::Internal => 1,
            TypeClassification::UserDefined => 2,
        }
    }

    fn from_byte(value: u8) -> Self {
        match value {
            1 => TypeClassification::Internal,
            2 => TypeClassification::UserDefined,
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

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.name.as_bytes().len() + 1);
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

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

pub trait RedbValue: Debug {
    /// Required alignment of the byte representation of this type. Must be a power of 2.
    /// Note: Currently only ALIGNMENT = 1 is supported
    const ALIGNMENT: usize = 1;

    /// SelfType<'a> must be the same type as Self with all lifetimes replaced with 'a
    type SelfType<'a>: Debug + 'a
    where
        Self: 'a;

    type AsBytes<'a>: AsAlignedSlice<1> + 'a
    where
        Self: 'a;

    /// Width of a fixed type, or None for variable width
    fn fixed_width() -> Option<usize>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    // TODO: implement guarantee that data is aligned to Self::ALIGNMENT
    fn from_bytes<'a>(data: AlignedSlice<'a, 1>) -> Self::SelfType<'a>
    where
        Self: 'a;

    /// Serialize the value to a slice
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b;

    /// Globally unique identifier for this type
    fn type_name() -> TypeName;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: AlignedSlice<1>, data2: AlignedSlice<1>) -> Ordering;
}

impl RedbValue for () {
    type SelfType<'a> = ()
    where
        Self: 'a;
    type AsBytes<'a> = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(0)
    }

    #[allow(clippy::unused_unit)]
    fn from_bytes<'a>(_data: AlignedSlice<'a, 1>) -> ()
    where
        Self: 'a,
    {
        ()
    }

    fn as_bytes<'a, 'b: 'a>(_: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'a,
        Self: 'b,
    {
        &[]
    }

    fn type_name() -> TypeName {
        TypeName::internal("()")
    }
}

impl<T: RedbValue> RedbValue for Option<T> {
    const ALIGNMENT: usize = T::ALIGNMENT;
    type SelfType<'a> = Option<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width().map(|x| x + T::ALIGNMENT)
    }

    fn from_bytes<'a>(data: AlignedSlice<'a, 1>) -> Option<T::SelfType<'a>>
    where
        Self: 'a,
    {
        match data.data()[0] {
            0 => None,
            1 => Some(T::from_bytes(AlignedSlice::new(
                &data.data()[T::ALIGNMENT..],
            ))),
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut result = vec![0; T::ALIGNMENT];
        if let Some(x) = value {
            result[0] = 1;
            result.extend_from_slice(T::as_bytes(x).as_aligned().data());
        }
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("Option<{}>", T::type_name().name()))
    }
}

impl RedbValue for &[u8] {
    type SelfType<'a> = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a> = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: AlignedSlice<'a, 1>) -> &'a [u8]
    where
        Self: 'a,
    {
        data.data()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal("&[u8]")
    }
}

impl RedbKey for &[u8] {
    fn compare(data1: AlignedSlice<1>, data2: AlignedSlice<1>) -> Ordering {
        data1.data().cmp(data2.data())
    }
}

impl<const N: usize> RedbValue for &[u8; N] {
    type SelfType<'a> = &'a [u8; N]
    where
        Self: 'a;
    type AsBytes<'a> = &'a [u8; N]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(N)
    }

    fn from_bytes<'a>(data: AlignedSlice<'a, 1>) -> &'a [u8; N]
    where
        Self: 'a,
    {
        data.data().try_into().unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8; N]
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("[u8;{N}]"))
    }
}

impl<const N: usize> RedbKey for &[u8; N] {
    fn compare(data1: AlignedSlice<1>, data2: AlignedSlice<1>) -> Ordering {
        data1.data().cmp(data2.data())
    }
}

impl RedbValue for &str {
    type SelfType<'a> = &'a str
    where
        Self: 'a;
    type AsBytes<'a> = &'a str
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: AlignedSlice<'a, 1>) -> &'a str
    where
        Self: 'a,
    {
        std::str::from_utf8(data.data()).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a str
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal("&str")
    }
}

impl RedbKey for &str {
    fn compare(data1: AlignedSlice<1>, data2: AlignedSlice<1>) -> Ordering {
        let str1 = Self::from_bytes(data1);
        let str2 = Self::from_bytes(data2);
        str1.cmp(str2)
    }
}

macro_rules! be_value {
    ($t:ty) => {
        impl RedbValue for $t {
            type SelfType<'a> = $t;
            type AsBytes<'a> = [u8; std::mem::size_of::<$t>()] where Self: 'a;

            fn fixed_width() -> Option<usize> {
                Some(std::mem::size_of::<$t>())
            }

            fn from_bytes<'a>(data: AlignedSlice<'a, 1>) -> $t
            where
                Self: 'a,
            {
                <$t>::from_le_bytes(data.data().try_into().unwrap())
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

macro_rules! be_impl {
    ($t:ty) => {
        be_value!($t);

        impl RedbKey for $t {
            fn compare(data1: AlignedSlice<1>, data2: AlignedSlice<1>) -> Ordering {
                Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
            }
        }
    };
}

be_impl!(u8);
be_impl!(u16);
be_impl!(u32);
be_impl!(u64);
be_impl!(u128);
be_impl!(i8);
be_impl!(i16);
be_impl!(i32);
be_impl!(i64);
be_impl!(i128);
be_value!(f32);
be_value!(f64);
