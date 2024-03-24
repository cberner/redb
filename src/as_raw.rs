use super::*;
use std::fmt;

pub trait Name {
    const NAME: &'static str;
}

pub trait AsRaw:
    for<'a> AsRef<<Self::Raw as Value>::SelfType<'a>>
    + for<'a> From<<Self::Raw as Value>::SelfType<'a>>
    + Name
{
    type Raw: Value;
}

impl<T> Value for T
where
    T: AsRaw + fmt::Debug + 'static,
{
    type SelfType<'a> = Self;

    type AsBytes<'a> = <<T as AsRaw>::Raw as Value>::AsBytes<'a>;

    fn fixed_width() -> Option<usize> {
        <T as AsRaw>::Raw::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        T::from(<<T as AsRaw>::Raw>::from_bytes(data))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        <T as AsRaw>::Raw::as_bytes(value.as_ref())
    }

    fn type_name() -> TypeName {
        TypeName::new(<T as Name>::NAME)
    }
}
