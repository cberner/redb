use std::cmp::Ordering;
use std::convert::TryInto;
use std::marker::PhantomData;

pub trait AsBytesWithLifetime<'a> {
    type Out: AsRef<[u8]>;
}

pub struct RefAsBytesLifetime<T: AsRef<[u8]> + ?Sized>(PhantomData<T>);
impl<'a, T: 'a + AsRef<[u8]> + ?Sized> AsBytesWithLifetime<'a> for RefAsBytesLifetime<T> {
    type Out = &'a T;
}

pub struct OwnedAsBytesLifetime<T: AsRef<[u8]>>(PhantomData<T>);
impl<'a, T: AsRef<[u8]> + 'a> AsBytesWithLifetime<'a> for OwnedAsBytesLifetime<T> {
    type Out = T;
}

pub trait WithLifetime<'a> {
    type Out;
}

pub struct RefLifetime<T: ?Sized>(PhantomData<T>);
impl<'a, T: 'a + ?Sized> WithLifetime<'a> for RefLifetime<T> {
    type Out = &'a T;
}

pub struct OwnedLifetime<T>(PhantomData<T>);
impl<'a, T: 'a> WithLifetime<'a> for OwnedLifetime<T> {
    type Out = T;
}

pub trait RedbValue {
    // TODO: need GATs, so that we can replace all this HRTB stuff
    type View: for<'a> WithLifetime<'a>;
    type ToBytes: for<'a> AsBytesWithLifetime<'a>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out;

    /// Serialize the key to a slice
    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl RedbValue for [u8] {
    type View = RefLifetime<[u8]>;
    type ToBytes = RefAsBytesLifetime<[u8]>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self
    }
}

impl RedbKey for [u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl RedbValue for u64 {
    type View = OwnedLifetime<u64>;
    type ToBytes = OwnedAsBytesLifetime<[u8; 8]>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        u64::from_be_bytes(data.try_into().unwrap())
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self.to_be_bytes()
    }
}

impl RedbKey for u64 {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}
