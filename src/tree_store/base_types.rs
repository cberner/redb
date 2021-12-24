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

    #[inline(always)]
    pub(crate) fn to_be_bytes(self) -> [u8; Self::serialized_size()] {
        let mut temp = self.page_number.0;
        temp |= (self.valid_message_bytes as u64) << 48;

        temp.to_be_bytes()
    }

    #[inline(always)]
    pub(crate) fn from_be_bytes(bytes: [u8; Self::serialized_size()]) -> Self {
        let temp = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let page_number = PageNumber(temp & 0x0000_FFFF_FFFF_FFFF);
        let valid_message_bytes = (temp >> 48) as u32;

        Self::new(page_number, valid_message_bytes)
    }

    #[inline(always)]
    pub(crate) const fn serialized_size() -> usize {
        8
    }
}
