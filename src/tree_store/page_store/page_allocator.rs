use crate::Error;
use crate::Result;
use std::mem::size_of;

const ELEMENTS_OFFSET: usize = 0;
const CAPACITY_OFFSET: usize = ELEMENTS_OFFSET + size_of::<u32>();
const HEIGHT_OFFSET: usize = CAPACITY_OFFSET + size_of::<u32>();
const END_OFFSETS: usize = HEIGHT_OFFSET + size_of::<u32>();

fn get_level_start(data: &[u8], i: u32) -> usize {
    if i == 0 {
        get_data_start(data)
    } else {
        get_level_end(data, i - 1)
    }
}

fn get_level_end(data: &[u8], i: u32) -> usize {
    if i == 0 {
        get_data_start(data) + U64GroupedBitMapMut::required_bytes(64)
    } else {
        let index = END_OFFSETS + ((i - 1) as usize) * size_of::<u32>();
        u32::from_le_bytes(data[index..(index + size_of::<u32>())].try_into().unwrap())
            .try_into()
            .unwrap()
    }
}

fn get_height(data: &[u8]) -> u32 {
    u32::from_le_bytes(
        data[HEIGHT_OFFSET..(HEIGHT_OFFSET + size_of::<u32>())]
            .try_into()
            .unwrap(),
    )
}

fn get_elements(data: &[u8]) -> u32 {
    u32::from_le_bytes(
        data[ELEMENTS_OFFSET..(ELEMENTS_OFFSET + size_of::<u32>())]
            .try_into()
            .unwrap(),
    )
}

fn get_data_start(data: &[u8]) -> usize {
    END_OFFSETS + (get_height(data) as usize - 1) * size_of::<u32>()
}

pub(crate) struct PageAllocator<'a> {
    data: &'a [u8],
}

// Stores a 64-way bit-tree of allocated pages.
// Does not hold a reference to the data, so that this structure can be initialized once, without
// borrowing the data array
//
// Data structure format:
// elements: u32
// capacity: u32
// height: u32
// layer_ends: array of u32, ending offset in bytes of layers. Does not include the root layer
// root: u64
// subtree layer: 2-64 u64s
// ...consecutive layers. Except for the last level, all sub-trees of the root must be complete
impl<'a> PageAllocator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub(crate) fn count_free_pages(&self) -> usize {
        self.get_level(self.get_height() - 1).count_unset()
    }

    pub(crate) fn has_free_pages(&self) -> bool {
        self.get_level(self.get_height() - 1).any_unset()
    }

    pub(crate) fn get_num_pages(&self) -> u64 {
        get_elements(self.data).into()
    }

    pub(crate) fn is_allocated(&self, page_number: u64) -> bool {
        self.get_level(self.get_height() - 1)
            .get(page_number as usize)
    }

    /// data must have been initialized by Self::init_new(). Returns the first free id, after (inclusive) of start
    pub(crate) fn find_free(&self) -> Result<u64> {
        if let Some(mut entry) = self.get_level(0).first_unset(0, 64) {
            let mut height = 0;

            while height < self.get_height() - 1 {
                height += 1;
                entry *= 64;
                entry = self
                    .get_level(height)
                    .first_unset(entry, entry + 64)
                    .unwrap();
            }

            assert!(entry < self.get_num_pages() as usize);
            Ok(entry as u64)
        } else {
            Err(Error::OutOfSpace)
        }
    }

    fn get_level(&self, i: u32) -> U64GroupedBitMap<'a> {
        assert!(i < self.get_height());
        let start = get_level_start(self.data, i);
        let end = get_level_end(self.data, i);
        U64GroupedBitMap::new(&self.data[start..end])
    }

    fn get_height(&self) -> u32 {
        get_height(self.data)
    }
}

pub(crate) struct PageAllocatorMut<'a> {
    data: &'a mut [u8],
}

impl<'a> PageAllocatorMut<'a> {
    /// data must have been initialized by Self::init_new()
    pub(super) fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    // Initializes a new allocator, with no pages free
    pub(crate) fn init_new(data: &'a mut [u8], num_pages: usize, capacity: usize) -> Self {
        assert!(data.len() >= Self::required_space(num_pages));
        // TODO: there should be a resize() method to change elements
        data[ELEMENTS_OFFSET..(ELEMENTS_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(num_pages as u32).to_le_bytes());
        data[CAPACITY_OFFSET..(CAPACITY_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(capacity as u32).to_le_bytes());
        let height = Self::required_tree_height(capacity);
        data[HEIGHT_OFFSET..(HEIGHT_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(height as u32).to_le_bytes());
        // Initialize the memory, so that all pages are allocated
        U64GroupedBitMapMut::init_full(&mut data[Self::tree_data_start(capacity)..]);

        let mut tree_level_offsets = vec![];
        let mut offset = get_data_start(data);
        // root level -- not stored
        offset += size_of::<u64>();

        // Intermediate levels
        if Self::required_tree_height(capacity) > 2 {
            for i in 1..(Self::required_tree_height(capacity) - 1) {
                let len = Self::required_subtrees(capacity) * 64usize.pow(i as u32) / 8;
                tree_level_offsets.push(offset + len);
                offset += len;
            }
        }

        // Leaf level
        if Self::required_tree_height(capacity) > 1 {
            let len = (capacity + 63) / 64 * size_of::<u64>();
            tree_level_offsets.push(offset + len);
            offset += len;
        }

        assert_eq!(
            tree_level_offsets.len(),
            Self::required_tree_height(capacity) - 1
        );
        assert_eq!(offset, Self::required_space(capacity));

        let mut index = END_OFFSETS;
        for end in tree_level_offsets {
            data[index..(index + size_of::<u32>())].copy_from_slice(&(end as u32).to_le_bytes());
            index += size_of::<u32>();
        }

        Self::new(data)
    }

    pub(crate) fn find_free(&self) -> Result<u64> {
        PageAllocator::new(self.data).find_free()
    }

    pub(crate) fn is_allocated(&self, page_number: u64) -> bool {
        PageAllocator::new(self.data).is_allocated(page_number)
    }

    pub(crate) fn alloc(&mut self) -> Result<u64> {
        let entry = self.find_free()?;
        self.record_alloc(entry as u64);
        Ok(entry)
    }

    pub(crate) fn record_alloc(&mut self, page_number: u64) {
        assert!(page_number < self.get_num_pages());
        let full = self
            .get_level_mut(self.get_height() - 1)
            .set(page_number as usize);
        self.update_to_root(page_number as usize, full);
    }

    pub(crate) fn free(&mut self, page_number: u64) {
        assert!(page_number < self.get_num_pages());
        self.get_level_mut(self.get_height() - 1)
            .clear(page_number as usize);
        self.update_to_root(page_number as usize, false);
    }

    pub(crate) fn get_num_pages(&self) -> u64 {
        get_elements(self.data).into()
    }

    pub(crate) fn required_space(capacity: usize) -> usize {
        let tree_space = if Self::required_tree_height(capacity) == 1 {
            assert!(capacity <= 64);
            // Space for root
            size_of::<u64>()
        } else if Self::required_tree_height(capacity) == 2 {
            // Space for root
            size_of::<u64>() +
                // Space for the leaves
                U64GroupedBitMapMut::required_bytes(capacity)
        } else {
            // Space for root
            size_of::<u64>() +
                // Space for the subtrees
                Self::required_subtrees(capacity) * Self::required_interior_bytes_per_subtree(capacity) +
                // Space for the leaves
                U64GroupedBitMapMut::required_bytes(capacity)
        };

        Self::tree_data_start(capacity) + tree_space
    }

    fn tree_data_start(capacity: usize) -> usize {
        END_OFFSETS + (Self::required_tree_height(capacity) - 1) * size_of::<u32>()
    }

    fn required_interior_bytes_per_subtree(capacity: usize) -> usize {
        let subtree_height = Self::required_tree_height(capacity) - 1;
        (1..subtree_height)
            .map(|i| 64usize.pow(i as u32))
            .sum::<usize>()
            / 8
    }

    fn required_subtrees(capacity: usize) -> usize {
        let height = Self::required_tree_height(capacity);
        let pages_per_subtree = 64usize.pow((height - 1) as u32);

        (capacity + pages_per_subtree - 1) / pages_per_subtree
    }

    fn required_tree_height(capacity: usize) -> usize {
        let mut height = 1;
        let mut storable = 64;
        while capacity > storable {
            storable *= 64;
            height += 1;
        }

        height
    }

    fn get_height(&self) -> u32 {
        get_height(self.data)
    }

    fn get_level_mut(&mut self, i: u32) -> U64GroupedBitMapMut {
        assert!(i < self.get_height());
        let start = get_level_start(self.data, i);
        let end = get_level_end(self.data, i);
        U64GroupedBitMapMut::new(&mut self.data[start..end])
    }

    // Recursively update to the root, starting at the given entry in the given height
    // full parameter must be set if all bits in the entry's group of u64 are full
    fn update_to_root(&mut self, page_number: usize, mut full: bool) {
        if self.get_height() == 1 {
            return;
        }

        let mut parent_height = self.get_height() - 2;
        let mut parent_entry = page_number / 64;
        loop {
            full = if full {
                self.get_level_mut(parent_height).set(parent_entry)
            } else {
                self.get_level_mut(parent_height).clear(parent_entry);
                false
            };

            if parent_height == 0 {
                break;
            }
            parent_height -= 1;
            parent_entry /= 64;
        }
    }
}

// A bitmap which groups consecutive groups of 64bits together
struct U64GroupedBitMap<'a> {
    data: &'a [u8],
}

impl<'a> U64GroupedBitMap<'a> {
    fn new(data: &'a [u8]) -> Self {
        assert_eq!(data.len() % 8, 0);
        Self { data }
    }

    fn data_index_of(&self, bit: usize) -> (usize, usize) {
        ((bit / 64) as usize * size_of::<u64>(), (bit % 64) as usize)
    }

    fn count_unset(&self) -> usize {
        self.data.iter().map(|x| x.count_ones() as usize).sum()
    }

    fn any_unset(&self) -> bool {
        self.data.iter().any(|x| x.count_ones() > 0)
    }

    fn first_unset(&self, start_bit: usize, end_bit: usize) -> Option<usize> {
        assert_eq!(end_bit, (start_bit - start_bit % 64) + 64);

        let (index, bit) = self.data_index_of(start_bit);
        let mask = !((1 << bit) - 1);
        let group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        let group = group & mask;
        match group.trailing_zeros() {
            64 => None,
            x => Some(start_bit + x as usize - bit),
        }
    }

    fn get(&self, bit: usize) -> bool {
        let (index, bit_index) = self.data_index_of(bit);
        let group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group & U64GroupedBitMapMut::select_mask(bit_index) == 0
    }
}

// Note bits are set in the opposite of what may be intuitive: they are 0 when set and 1 when unset.
// This is so that the data structure can be efficiently initialized to be all set.
struct U64GroupedBitMapMut<'a> {
    data: &'a mut [u8],
}

impl<'a> U64GroupedBitMapMut<'a> {
    fn required_bytes(elements: usize) -> usize {
        let words = (elements + 63) / 64;
        words * size_of::<u64>()
    }

    fn init_full(data: &mut [u8]) {
        for value in data.iter_mut() {
            *value = 0;
        }
    }

    fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(data.len() % 8, 0);
        Self { data }
    }

    // Returns true iff the bit's group is all set
    fn set(&mut self, bit: usize) -> bool {
        let (index, bit_index) = self.data_index_of(bit);
        let mut group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group &= !Self::select_mask(bit_index);
        self.data[index..(index + 8)].copy_from_slice(&group.to_le_bytes());

        group == 0
    }

    fn clear(&mut self, bit: usize) {
        let (index, bit_index) = self.data_index_of(bit);
        let mut group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group |= Self::select_mask(bit_index);
        self.data[index..(index + 8)].copy_from_slice(&group.to_le_bytes());
    }

    fn data_index_of(&self, bit: usize) -> (usize, usize) {
        ((bit / 64) as usize * size_of::<u64>(), (bit % 64) as usize)
    }

    fn select_mask(bit: usize) -> u64 {
        1u64 << (bit as u64)
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::page_allocator::PageAllocatorMut;
    use crate::Error;
    use rand::prelude::IteratorRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;
    use std::convert::TryInto;

    #[test]
    fn alloc() {
        let num_pages = 2;
        let mut data = vec![0; PageAllocatorMut::required_space(num_pages)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, num_pages, num_pages);
        for i in 0..num_pages {
            allocator.free(i as u64);
        }
        for i in 0..num_pages {
            assert_eq!(i as u64, allocator.alloc().unwrap());
        }
        assert!(matches!(allocator.alloc().unwrap_err(), Error::OutOfSpace));
    }

    #[test]
    fn record_alloc() {
        let mut data = vec![0; PageAllocatorMut::required_space(2)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, 2, 2);
        allocator.free(0);
        allocator.free(1);
        allocator.record_alloc(0);
        assert_eq!(1, allocator.alloc().unwrap());
        assert!(matches!(allocator.alloc().unwrap_err(), Error::OutOfSpace));
    }

    #[test]
    fn free() {
        let mut data = vec![0; PageAllocatorMut::required_space(1)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, 1, 1);
        allocator.free(0);
        assert_eq!(0, allocator.alloc().unwrap());
        assert!(matches!(allocator.alloc().unwrap_err(), Error::OutOfSpace));
        allocator.free(0);
        assert_eq!(0, allocator.alloc().unwrap());
    }

    #[test]
    fn reuse_lowest() {
        let num_pages = 65;
        let mut data = vec![0; PageAllocatorMut::required_space(num_pages)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, num_pages, num_pages);
        for i in 0..num_pages {
            allocator.free(i as u64);
        }
        for i in 0..num_pages {
            assert_eq!(i as u64, allocator.alloc().unwrap());
        }
        allocator.free(5);
        allocator.free(15);
        assert_eq!(5, allocator.alloc().unwrap());
        assert_eq!(15, allocator.alloc().unwrap());
        assert!(matches!(allocator.alloc().unwrap_err(), Error::OutOfSpace));
    }

    #[test]
    fn all_space_used() {
        let num_pages = 65;
        let mut data = vec![0; PageAllocatorMut::required_space(num_pages)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, num_pages, num_pages);
        for i in 0..num_pages {
            allocator.free(i as u64);
        }
        // Allocate everything
        while allocator.alloc().is_ok() {}
        // The last u64 must be used, since the leaf layer is compact
        let l = data.len();
        assert_ne!(
            u64::MAX,
            u64::from_le_bytes(data[(l - 8)..].try_into().unwrap())
        );
    }

    #[test]
    fn find_free() {
        let num_pages = 129;
        let mut data = vec![0; PageAllocatorMut::required_space(num_pages)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, num_pages, num_pages);
        assert!(matches!(
            allocator.find_free().unwrap_err(),
            Error::OutOfSpace
        ));
        allocator.free(128);
        assert_eq!(allocator.find_free().unwrap(), 128);
        allocator.free(65);
        assert_eq!(allocator.find_free().unwrap(), 65);
        allocator.free(8);
        assert_eq!(allocator.find_free().unwrap(), 8);
        allocator.free(0);
        assert_eq!(allocator.find_free().unwrap(), 0);
    }

    #[test]
    fn random_pattern() {
        let seed = rand::thread_rng().gen();
        // Print the seed to debug for reproducibility, in case this test fails
        println!("seed={}", seed);
        let mut rng = StdRng::seed_from_u64(seed);

        let num_pages = rng.gen_range(2..10000);
        let mut data = vec![0; PageAllocatorMut::required_space(num_pages)];
        let mut allocator = PageAllocatorMut::init_new(&mut data, num_pages, num_pages);
        for i in 0..num_pages {
            allocator.free(i as u64);
        }
        let mut allocated = HashSet::new();

        for _ in 0..(num_pages * 2) {
            if rng.gen_bool(0.75) {
                if let Ok(page) = allocator.alloc() {
                    allocated.insert(page);
                } else {
                    assert_eq!(allocated.len(), num_pages);
                }
            } else if let Some(to_free) = allocated.iter().choose(&mut rng).cloned() {
                allocator.free(to_free);
                allocated.remove(&to_free);
            }
        }

        for _ in allocated.len()..num_pages {
            allocator.alloc().unwrap();
        }
        assert!(matches!(allocator.alloc().unwrap_err(), Error::OutOfSpace));

        for i in 0..num_pages {
            allocator.free(i as u64);
        }

        for _ in 0..num_pages {
            allocator.alloc().unwrap();
        }
        assert!(matches!(allocator.alloc().unwrap_err(), Error::OutOfSpace));
    }
}
