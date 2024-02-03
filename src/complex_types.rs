use crate::types::{TypeName, Value};

// Encode len as a varint and store it at the end of output
fn encode_varint_len(len: usize, output: &mut Vec<u8>) {
    if len < 254 {
        output.push(len.try_into().unwrap());
    } else if len <= u16::MAX.into() {
        let u16_len: u16 = len.try_into().unwrap();
        output.push(254);
        output.extend_from_slice(&u16_len.to_le_bytes())
    } else {
        assert!(len <= u32::MAX as usize);
        let u32_len: u32 = len.try_into().unwrap();
        output.push(255);
        output.extend_from_slice(&u32_len.to_le_bytes())
    }
}

// Decode a variable length int starting at the beginning of data
// Returns (decoded length, length consumed of `data`)
fn decode_varint_len(data: &[u8]) -> (usize, usize) {
    match data[0] {
        0..=253 => (data[0] as usize, 1),
        254 => (
            u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize,
            3,
        ),
        255 => (
            u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize,
            5,
        ),
    }
}

impl<T: Value> Value for Vec<T> {
    type SelfType<'a> = Vec<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a> = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Vec<T::SelfType<'a>>
    where
        Self: 'a,
    {
        let (elements, mut offset) = decode_varint_len(data);
        let mut result = Vec::with_capacity(elements);
        for _ in 0..elements {
            let element_len = if let Some(len) = T::fixed_width() {
                len
            } else {
                let (len, consumed) = decode_varint_len(&data[offset..]);
                offset += consumed;
                len
            };
            result.push(T::from_bytes(&data[offset..(offset + element_len)]));
            offset += element_len;
        }
        assert_eq!(offset, data.len());
        result
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Vec<T::SelfType<'b>>) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut result = if let Some(width) = T::fixed_width() {
            Vec::with_capacity(value.len() * width + 5)
        } else {
            Vec::with_capacity(value.len() * 2 + 5)
        };
        encode_varint_len(value.len(), &mut result);

        for element in value.iter() {
            let serialized = T::as_bytes(element);
            if T::fixed_width().is_none() {
                encode_varint_len(serialized.as_ref().len(), &mut result);
            }
            result.extend_from_slice(serialized.as_ref());
        }
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("Vec<{}>", T::type_name().name()))
    }
}
