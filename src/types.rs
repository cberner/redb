use std::cmp::Ordering;
use std::marker::PhantomData;

pub trait WithLifetime<'a> {
    type Out;
}

pub struct RefLifetime<T: ?Sized>(PhantomData<T>);
impl<'a, T: 'a + ?Sized> WithLifetime<'a> for RefLifetime<T> {
    type Out = &'a T;
}

pub trait RedbValue {
    // TODO: need GATs, so that we can replace all this HRTB stuff
    type View: for<'a> WithLifetime<'a>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out;

    /// Serialize the key to a slice
    fn as_bytes(&self) -> &[u8];
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl RedbValue for [u8] {
    type View = RefLifetime<[u8]>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data
    }

    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl RedbKey for [u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}
