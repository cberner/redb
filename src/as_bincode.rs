use super::*;

use std::fmt;

#[derive(Debug)]
pub struct AsBincode<T>(T);

impl<T> Value for AsBincode<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Name + fmt::Debug + 'static,
{
    type SelfType<'a> = T;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::deserialize(data).expect("bincode deserialization error")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::serialize(value).expect("bincode serialization error")
    }

    fn type_name() -> TypeName {
        TypeName::new(<T as Name>::NAME)
    }
}
