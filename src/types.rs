use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;

pub trait RedbValue: Debug {
    type View<'a>: Debug + 'a
    where
        Self: 'a;
    type AsBytes<'a>: AsRef<[u8]> + 'a
    where
        Self: 'a;

    /// Width of a fixed type, or None for variable width
    fn fixed_width() -> Option<usize>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    fn from_bytes<'a>(data: &'a [u8]) -> Self::View<'a>
    where
        Self: 'a;

    /// Serialize the key to a slice
    fn as_bytes(&self) -> Self::AsBytes<'_>;

    /// Globally unique identifier for this type
    fn redb_type_name() -> String;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl RedbValue for () {
    type View<'a> = ()
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

    fn as_bytes(&self) -> &[u8] {
        &[]
    }

    fn redb_type_name() -> String {
        "()".to_string()
    }
}

impl RedbValue for [u8] {
    type View<'a> = &'a [u8]
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

    fn as_bytes(&self) -> &[u8] {
        self
    }

    fn redb_type_name() -> String {
        "[u8]".to_string()
    }
}

impl RedbKey for [u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl<const N: usize> RedbValue for [u8; N] {
    type View<'a> = &'a [u8; N]
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

    fn as_bytes(&self) -> &[u8; N] {
        self
    }

    fn redb_type_name() -> String {
        format!("[u8;{}]", N)
    }
}

impl<const N: usize> RedbKey for [u8; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl RedbValue for str {
    type View<'a> = &'a str
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

    fn as_bytes(&self) -> &str {
        self
    }

    fn redb_type_name() -> String {
        "str".to_string()
    }
}

impl RedbKey for str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let str1 = str::from_bytes(data1);
        let str2 = str::from_bytes(data2);
        str1.cmp(str2)
    }
}

macro_rules! be_value {
    ($t:ty) => {
        impl RedbValue for $t {
            type View<'a> = $t;
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

            fn as_bytes(&self) -> [u8; std::mem::size_of::<$t>()] {
                self.to_le_bytes()
            }

            fn redb_type_name() -> String {
                stringify!($t).to_string()
            }
        }
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
