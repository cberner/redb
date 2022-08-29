use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use std::mem::size_of;

const REGION_FORMAT_VERSION: u8 = 1;
const ALLOCATOR_LENGTH_OFFSET: usize = 4;
const ALLOCATOR_OFFSET: usize = ALLOCATOR_LENGTH_OFFSET + size_of::<u32>();

// Region header
// 1 byte: region format version
// 3 bytes: padding
// 4 bytes: length of the allocator state in bytes
// n bytes: the allocator state
pub(super) struct RegionHeaderMutator<'a> {
    mem: &'a mut [u8],
}

impl<'a> RegionHeaderMutator<'a> {
    pub(crate) fn new(data: &'a mut [u8]) -> Self {
        Self { mem: data }
    }

    pub(crate) fn initialize(
        &mut self,
        num_pages: usize,
        max_page_capacity: usize,
        max_order: usize,
    ) {
        self.mem[0] = REGION_FORMAT_VERSION;
        let allocator_len = BuddyAllocator::required_space(max_page_capacity, max_order);
        self.mem[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(allocator_len as u32).to_le_bytes());
        BuddyAllocator::init_new(
            &mut self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + allocator_len)],
            num_pages,
            max_page_capacity,
            max_order,
        );
    }

    fn get_allocator_len(&self) -> usize {
        u32::from_le_bytes(
            self.mem[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(crate) fn allocator_state_mut(&mut self) -> &mut [u8] {
        let len = self.get_allocator_len();
        &mut self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + len)]
    }
}

pub(super) struct RegionHeaderAccessor<'a> {
    mem: &'a [u8],
}

impl<'a> RegionHeaderAccessor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        assert_eq!(data[0], REGION_FORMAT_VERSION);
        Self { mem: data }
    }

    fn get_allocator_len(&self) -> usize {
        u32::from_le_bytes(
            self.mem[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(crate) fn allocator_state(&self) -> &[u8] {
        &self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + self.get_allocator_len())]
    }
}
