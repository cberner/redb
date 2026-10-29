use crate::types::{TypeName, Value, ValueError, value_error};
use alloc::format;
use alloc::vec::Vec;

// Encode len as a varint and store it at the end of output
pub(super) fn encode_varint_len(len: usize, output: &mut Vec<u8>) {
    if len < 254 {
        output.push(len.try_into().unwrap());
    } else if len <= u16::MAX.into() {
        let u16_len: u16 = len.try_into().unwrap();
        output.push(254);
        output.extend_from_slice(&u16_len.to_le_bytes());
    } else {
        let u32_len: u32 = len.try_into().unwrap();
        output.push(255);
        output.extend_from_slice(&u32_len.to_le_bytes());
    }
}

// Decode a variable length int starting at the beginning of data.
// Returns (decoded length, length consumed of `data`).
pub(super) fn try_decode_varint_len(
    data: &[u8],
) -> core::result::Result<(usize, usize), ValueError> {
    let Some(&tag) = data.first() else {
        return Err(value_error("encoded value is too short"));
    };
    match tag {
        0..=253 => Ok((tag as usize, 1)),
        254 => {
            let bytes = data
                .get(1..3)
                .ok_or_else(|| value_error("encoded value is too short"))?;
            let len = u16::from_le_bytes(bytes.try_into().unwrap()) as usize;
            // Lengths below 254 are stored in the tag byte itself.
            if len < 254 {
                return Err(value_error("non-minimal length encoding"));
            }
            Ok((len, 3))
        }
        255 => {
            let bytes = data
                .get(1..5)
                .ok_or_else(|| value_error("encoded value is too short"))?;
            let len = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
            // Lengths up to u16::MAX use the three-byte form.
            if u16::try_from(len).is_ok() {
                return Err(value_error("non-minimal length encoding"));
            }
            Ok((len, 5))
        }
    }
}

impl<T: Value> Value for Vec<T> {
    type SelfType<'a>
        = Vec<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Vec<T::SelfType<'a>>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Vec<T::SelfType<'a>>, ValueError>
    where
        Self: 'a,
    {
        let (elements, mut offset) = try_decode_varint_len(data)?;
        // The declared length is untrusted, so it is not used as the allocation size
        let mut result = Vec::new();
        for _ in 0..elements {
            let element_len = if let Some(len) = T::fixed_width() {
                len
            } else {
                let (len, consumed) =
                    try_decode_varint_len(data.get(offset..).unwrap_or_default())?;
                offset += consumed;
                len
            };
            let Some(end) = offset.checked_add(element_len) else {
                return Err(value_error("encoded Vec is too short"));
            };
            if end > data.len() {
                return Err(value_error("encoded Vec is too short"));
            }
            result.push(T::try_from_bytes(&data[offset..end])?);
            offset = end;
        }
        if offset != data.len() {
            return Err(value_error("encoded Vec has trailing bytes"));
        }
        Ok(result)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Vec<T::SelfType<'b>>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = if let Some(width) = T::fixed_width() {
            Vec::with_capacity(value.len() * width + 5)
        } else {
            Vec::with_capacity(value.len() * 2 + 5)
        };
        encode_varint_len(value.len(), &mut result);

        for element in value {
            let serialized = T::as_bytes(element);
            if T::fixed_width().is_none() {
                encode_varint_len(serialized.as_ref().len(), &mut result);
            }
            result.extend_from_slice(serialized.as_ref());
        }
        result
    }

    fn type_name() -> TypeName {
        let inner = T::type_name();
        TypeName::internal(&format!("Vec<{}>", inner.name()))
            .into_composite(inner.is_user_defined())
    }
}
