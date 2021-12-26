use crate::tree_store::PageNumber;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(in crate) struct NodeHandle {
    page_number: PageNumber,
    messages: u8,
}

impl NodeHandle {
    pub(crate) fn new(page_number: PageNumber, messages: u32) -> Self {
        // TODO: change messages to a u8
        assert!(messages <= u8::MAX as u32);
        Self {
            page_number,
            messages: messages as u8,
        }
    }

    pub(crate) fn get_page_number(&self) -> PageNumber {
        self.page_number
    }

    // Messages are btree node defined, but must form a strict ordering of subsets of page memory.
    // That is, if m1 & m2 are counts of messages and m1 < m2, then the bytes in a page p,
    // used in the representation of m1 is a strict subset of the bytes used for m2
    pub(crate) fn get_valid_messages(&self) -> u32 {
        self.messages as u32
    }

    #[inline(always)]
    pub(crate) fn to_be_bytes(self) -> [u8; Self::serialized_size()] {
        let mut temp = u64::from_be_bytes(self.page_number.to_be_bytes());
        // Page number must not use the top byte, since we use that for the messages field
        assert_eq!(0xFF00_0000_0000_0000 & temp, 0);
        temp |= (self.messages as u64) << 56;

        temp.to_be_bytes()
    }

    #[inline(always)]
    pub(crate) fn from_be_bytes(bytes: [u8; Self::serialized_size()]) -> Self {
        let page_number = PageNumber::from_be_bytes(bytes);
        let temp = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let messages = (temp >> 56) as u32;

        Self::new(page_number, messages)
    }

    #[inline(always)]
    pub(crate) const fn serialized_size() -> usize {
        8
    }
}
