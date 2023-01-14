use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;

pub trait Sealed {}

pub trait RedbValue: Debug + Sealed {
    /// SelfType<'a> must be the same type as Self with all lifetimes replaced with 'a
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
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a;

    /// Serialize the value to a slice
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b;

    /// Globally unique identifier for this type
    fn redb_type_name() -> String;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
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
    fn from_bytes<'a>(_data: &'a [u8]) -> ()
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

    fn redb_type_name() -> String {
        "()".to_string()
    }
}

impl Sealed for () {}

impl<T: RedbValue> RedbValue for Option<T> {
    type SelfType<'a> = Option<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
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
        Self: 'a,
        Self: 'b,
    {
        let mut result = vec![];
        if let Some(x) = value {
            result.push(1);
            result.extend_from_slice(T::as_bytes(x).as_ref());
        } else {
            result.push(0);
        }
        result
    }

    fn redb_type_name() -> String {
        format!("Option<{}>", T::redb_type_name())
    }
}

impl<T: RedbValue> Sealed for Option<T> {}

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

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn redb_type_name() -> String {
        "[u8]".to_string()
    }
}

impl Sealed for &[u8] {}

impl RedbKey for &[u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
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

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8; N]
    where
        Self: 'a,
    {
        data.try_into().unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8; N]
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn redb_type_name() -> String {
        format!("[u8;{}]", N)
    }
}

impl<const N: usize> Sealed for &[u8; N] {}

impl<const N: usize> RedbKey for &[u8; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
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

    fn from_bytes<'a>(data: &'a [u8]) -> &'a str
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a str
    where
        Self: 'a,
        Self: 'b,
    {
        value
    }

    fn redb_type_name() -> String {
        "str".to_string()
    }
}

impl Sealed for &str {}

impl RedbKey for &str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
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

            fn redb_type_name() -> String {
                stringify!($t).to_string()
            }
        }

        impl Sealed for $t {}
    };
}

macro_rules! be_impl {
    ($t:ty) => {
        be_value!($t);

        impl RedbKey for $t {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
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
