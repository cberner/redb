use crate::tree_store::page_store::xxh3_checksum;
use std::mem::size_of;

const HEIGHT_OFFSET: usize = 0;
const END_OFFSETS: usize = HEIGHT_OFFSET + size_of::<u32>();

pub(crate) struct BtreeBitmap {
    heights: Vec<U64GroupedBitmap>,
}

// Stores a 64-way bit-tree of allocated ids.
//
// Data structure format:
// height: u32
// layer_ends: array of u32, ending offset in bytes of layers.
// layer data: u64s
// ...consecutive layers. Except for the last level, all sub-trees of the root must be complete
impl BtreeBitmap {
    pub(crate) fn count_unset(&self) -> u32 {
        self.get_level(self.get_height() - 1).count_unset()
    }

    pub(crate) fn has_unset(&self) -> bool {
        self.get_level(self.get_height() - 1).any_unset()
    }

    pub(crate) fn get(&self, i: u32) -> bool {
        self.get_level(self.get_height() - 1).get(i)
    }

    pub(crate) fn len(&self) -> u32 {
        self.get_level(self.get_height() - 1).len()
    }

    pub(crate) fn find_first_unset(&self) -> Option<u32> {
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

            Some(entry)
        } else {
            None
        }
    }

    fn get_level(&self, i: u32) -> &U64GroupedBitmap {
        assert!(i < self.get_height());
        &self.heights[i as usize]
    }

    fn get_height(&self) -> u32 {
        self.heights.len().try_into().unwrap()
    }

    pub(crate) fn xxh3_hash(&self) -> u128 {
        let mut result = 0;
        for height in &self.heights {
            result ^= height.xxh3_hash();
        }
        result
    }

    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![];
        let height: u32 = self.heights.len().try_into().unwrap();
        result.extend(height.to_le_bytes());

        let vecs: Vec<Vec<u8>> = self.heights.iter().map(|x| x.to_vec()).collect();
        let mut data_offset = END_OFFSETS + self.heights.len() * size_of::<u32>();
        let end_metadata = data_offset;
        for serialized in &vecs {
            data_offset += serialized.len();
            let offset_u32: u32 = data_offset.try_into().unwrap();
            result.extend(offset_u32.to_le_bytes());
        }

        assert_eq!(end_metadata, result.len());
        for serialized in &vecs {
            result.extend(serialized);
        }

        result
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Self {
        let height = u32::from_le_bytes(
            data[HEIGHT_OFFSET..(HEIGHT_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        let mut metadata = END_OFFSETS;
        let mut data_start = END_OFFSETS + (height as usize) * size_of::<u32>();

        let mut heights = vec![];
        for _ in 0..height {
            let data_end = u32::from_le_bytes(
                data[metadata..(metadata + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            heights.push(U64GroupedBitmap::from_bytes(&data[data_start..data_end]));
            data_start = data_end;
            metadata += size_of::<u32>();
        }

        Self { heights }
    }

    // Initializes a new allocator, with no ids free
    pub(crate) fn new(mut num_pages: u32, mut capacity: u32) -> Self {
        let mut heights = vec![];

        // Build from the leaf to root
        loop {
            heights.push(U64GroupedBitmap::new_full(num_pages, capacity));
            if capacity <= 64 {
                break;
            }
            capacity = capacity.div_ceil(64);
            num_pages = num_pages.div_ceil(64);
        }

        // Reverse so that the root as index 0
        heights.reverse();

        Self { heights }
    }

    pub(crate) fn resize(&mut self, mut new_len: u32, full: bool) {
        for height in self.heights.iter_mut().rev() {
            height.resize(new_len, full);
            new_len = new_len.div_ceil(64);
        }
        assert!(self.get_level(0).len() <= 64);
    }

    // Returns the first unset id, and sets it
    pub(crate) fn alloc(&mut self) -> Option<u32> {
        let entry = self.find_first_unset()?;
        self.set(entry);
        Some(entry)
    }

    pub(crate) fn set(&mut self, i: u32) {
        let full = self.get_level_mut(self.get_height() - 1).set(i);
        self.update_to_root(i, full);
    }

    pub(crate) fn clear(&mut self, i: u32) {
        self.get_level_mut(self.get_height() - 1).clear(i);
        self.update_to_root(i, false);
    }

    fn get_level_mut(&mut self, i: u32) -> &mut U64GroupedBitmap {
        assert!(i < self.get_height());
        &mut self.heights[i as usize]
    }

    // Recursively update to the root, starting at the given entry in the given height
    // full parameter must be set if all bits in the entry's group of u64 are full
    fn update_to_root(&mut self, i: u32, mut full: bool) {
        if self.get_height() == 1 {
            return;
        }

        let mut parent_height = self.get_height() - 2;
        let mut parent_entry = i / 64;
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
pub(crate) struct U64GroupedBitmap {
    len: u32,
    data: Vec<u64>,
}

impl U64GroupedBitmap {
    fn required_words(elements: u32) -> usize {
        let words = elements.div_ceil(64);
        words as usize
    }

    pub fn new_full(len: u32, capacity: u32) -> Self {
        let data = vec![u64::MAX; Self::required_words(capacity)];
        Self { len, data }
    }

    pub fn xxh3_hash(&self) -> u128 {
        if self.len == 0 {
            return 0;
        }
        let mut bytes = vec![];
        bytes.extend(self.len.to_le_bytes());
        // Hash all the whole words
        for x in &self.data[0..Self::required_words(self.len) - 1] {
            bytes.extend(x.to_le_bytes());
        }
        let (index, bit) = Self::data_index_of(self.len - 1);
        // Select the bit and all lower ones
        let mask = ((1 << bit) - 1) | (1 << bit);
        let group = self.data[index];
        let group = group & mask;
        bytes.extend(group.to_le_bytes());

        xxh3_checksum(&bytes)
    }

    // Format:
    // 4 bytes: number of elements
    // n bytes: serialized groups
    pub fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(self.len.to_le_bytes());
        for x in &self.data[..Self::required_words(self.len)] {
            result.extend(x.to_le_bytes());
        }
        result
    }

    pub fn from_bytes(serialized: &[u8]) -> Self {
        assert_eq!(0, (serialized.len() - size_of::<u32>()) % size_of::<u64>());
        let mut data = vec![];
        let len = u32::from_le_bytes(serialized[..size_of::<u32>()].try_into().unwrap());
        let words = (serialized.len() - size_of::<u32>()) / size_of::<u64>();
        for i in 0..words {
            let start = size_of::<u32>() + i * size_of::<u64>();
            let value = u64::from_le_bytes(
                serialized[start..(start + size_of::<u64>())]
                    .try_into()
                    .unwrap(),
            );
            data.push(value);
        }

        Self { len, data }
    }

    fn data_index_of(bit: u32) -> (usize, usize) {
        ((bit as usize) / 64, (bit as usize) % 64)
    }

    fn select_mask(bit: usize) -> u64 {
        1u64 << (bit as u64)
    }

    fn count_unset(&self) -> u32 {
        self.data.iter().map(|x| x.count_zeros()).sum()
    }

    fn any_unset(&self) -> bool {
        self.data.iter().any(|x| x.count_zeros() > 0)
    }

    fn first_unset(&self, start_bit: u32, end_bit: u32) -> Option<u32> {
        assert_eq!(end_bit, (start_bit - start_bit % 64) + 64);
        if self.len == 0 {
            return None;
        }

        let (index, bit) = Self::data_index_of(start_bit);
        let mask = (1 << bit) - 1;
        let group = self.data[index];
        let group = group | mask;
        match group.trailing_ones() {
            64 => None,
            x => Some(start_bit + x - u32::try_from(bit).unwrap()),
        }
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn resize(&mut self, new_len: u32, full: bool) {
        if self.data.len() < Self::required_words(new_len) {
            let default_value = if full { u64::MAX } else { 0 };
            self.data
                .resize(Self::required_words(new_len), default_value);
        }
        let old_len = self.len;
        self.len = new_len;
        if old_len < new_len {
            // TODO: optimize this loop to set whole words at a time
            for i in old_len..new_len {
                if full {
                    self.set(i);
                } else {
                    self.clear(i);
                }
            }
        }
    }

    pub fn get(&self, bit: u32) -> bool {
        assert!(bit < self.len);
        let (index, bit_index) = Self::data_index_of(bit);
        let group = self.data[index];
        group & U64GroupedBitmap::select_mask(bit_index) != 0
    }

    // Returns true iff the bit's group is all set
    pub fn set(&mut self, bit: u32) -> bool {
        assert!(bit < self.len);
        let (index, bit_index) = Self::data_index_of(bit);
        let mut group = self.data[index];
        group |= Self::select_mask(bit_index);
        self.data[index] = group;

        group == u64::MAX
    }

    pub fn clear(&mut self, bit: u32) {
        assert!(bit < self.len, "{bit} must be less than {}", self.len);
        let (index, bit_index) = Self::data_index_of(bit);
        self.data[index] &= !Self::select_mask(bit_index);
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::bitmap::BtreeBitmap;
    use rand::prelude::IteratorRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;

    #[test]
    fn alloc() {
        let num_pages = 2;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        for i in 0..num_pages {
            assert_eq!(i, allocator.alloc().unwrap());
        }
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn record_alloc() {
        let mut allocator = BtreeBitmap::new(2, 2);
        allocator.clear(0);
        allocator.clear(1);
        allocator.set(0);
        assert_eq!(1, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn free() {
        let mut allocator = BtreeBitmap::new(1, 1);
        allocator.clear(0);
        assert_eq!(0, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
        allocator.clear(0);
        assert_eq!(0, allocator.alloc().unwrap());
    }

    #[test]
    fn reuse_lowest() {
        let num_pages = 65;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        for i in 0..num_pages {
            assert_eq!(i, allocator.alloc().unwrap());
        }
        allocator.clear(5);
        allocator.clear(15);
        assert_eq!(5, allocator.alloc().unwrap());
        assert_eq!(15, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn all_space_used() {
        let num_pages = 65;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        // Allocate everything
        while allocator.alloc().is_some() {}
        // The last u64 must be used, since the leaf layer is compact
        assert_eq!(
            u64::MAX,
            *allocator.heights.last().unwrap().data.last().unwrap()
        );
    }

    #[test]
    fn find_free() {
        let num_pages = 129;
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        assert!(allocator.find_first_unset().is_none());
        allocator.clear(128);
        assert_eq!(allocator.find_first_unset().unwrap(), 128);
        allocator.clear(65);
        assert_eq!(allocator.find_first_unset().unwrap(), 65);
        allocator.clear(8);
        assert_eq!(allocator.find_first_unset().unwrap(), 8);
        allocator.clear(0);
        assert_eq!(allocator.find_first_unset().unwrap(), 0);
    }

    #[test]
    fn random_pattern() {
        let seed = rand::rng().random();
        // Print the seed to debug for reproducibility, in case this test fails
        println!("seed={seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let num_pages = rng.random_range(2..10000);
        let mut allocator = BtreeBitmap::new(num_pages, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        let mut allocated = HashSet::new();

        for _ in 0..(num_pages * 2) {
            if rng.random_bool(0.75) {
                if let Some(page) = allocator.alloc() {
                    allocated.insert(page);
                } else {
                    assert_eq!(allocated.len(), num_pages as usize);
                }
            } else if let Some(to_free) = allocated.iter().choose(&mut rng).copied() {
                allocator.clear(to_free);
                allocated.remove(&to_free);
            }
        }

        for _ in allocated.len()..(num_pages as usize) {
            allocator.alloc().unwrap();
        }
        assert!(allocator.alloc().is_none());

        for i in 0..num_pages {
            allocator.clear(i);
        }

        for _ in 0..num_pages {
            allocator.alloc().unwrap();
        }
        assert!(allocator.alloc().is_none());
    }
}
