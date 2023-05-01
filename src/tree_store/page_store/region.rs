use crate::tree_store::page_store::buddy_allocator::{BuddyAllocator, BuddyAllocatorMut};
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

    pub(crate) fn initialize(&mut self, num_pages: u32, max_page_capacity: u32) {
        self.mem[0] = REGION_FORMAT_VERSION;
        let allocator_len = BuddyAllocatorMut::required_space(max_page_capacity);
        self.mem[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
            .copy_from_slice(&u32::try_from(allocator_len).unwrap().to_le_bytes());
        BuddyAllocatorMut::init_new(
            &mut self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + allocator_len)],
            num_pages,
            max_page_capacity,
        );
    }

    fn get_allocator_len(&self) -> usize {
        u32::from_le_bytes(
            self.mem[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(crate) fn allocator_mut(&mut self) -> BuddyAllocatorMut {
        let len = self.get_allocator_len();
        BuddyAllocatorMut::new(&mut self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + len)])
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

    pub(crate) fn allocator_raw(&self) -> Vec<u8> {
        self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + self.get_allocator_len())].to_vec()
    }

    pub(crate) fn allocator(&self) -> BuddyAllocator {
        BuddyAllocator::new(
            &self.mem[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + self.get_allocator_len())],
        )
    }
}
