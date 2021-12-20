use crate::tree_store::PageNumber;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(in crate) struct NodeHandle {
    page_number: PageNumber,
    valid_message_bytes: u32,
}

impl NodeHandle {
    pub(crate) fn new(page_number: PageNumber, message_bytes: u32) -> Self {
        Self {
            page_number,
            valid_message_bytes: message_bytes,
        }
    }

    pub(crate) fn get_page_number(&self) -> PageNumber {
        self.page_number
    }

    pub(crate) fn get_valid_message_bytes(&self) -> u32 {
        self.valid_message_bytes
    }

    pub(crate) fn to_be_bytes(self) -> [u8; 12] {
        let mut result = [0u8; 12];
        result[0..8].copy_from_slice(&self.page_number.to_be_bytes());
        result[8..12].copy_from_slice(&self.valid_message_bytes.to_be_bytes());

        result
    }

    pub(crate) fn from_be_bytes(bytes: [u8; 12]) -> Self {
        let page_number = PageNumber(u64::from_be_bytes(bytes[..8].try_into().unwrap()));
        let valid_message_bytes = u32::from_be_bytes(bytes[8..12].try_into().unwrap());

        Self::new(page_number, valid_message_bytes)
    }
}
