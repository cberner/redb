use std::mem::size_of;

const HEIGHT_OFFSET: usize = 0;
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
        get_data_start(data) + U64GroupedBitmapMut::required_bytes(64)
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

fn get_data_start(data: &[u8]) -> usize {
    END_OFFSETS + (get_height(data) as usize - 1) * size_of::<u32>()
}

pub(crate) struct BtreeBitmap<'a> {
    data: &'a [u8],
}

// Stores a 64-way bit-tree of allocated ids.
// Does not hold a reference to the data, so that this structure can be initialized once, without
// borrowing the data array
//
// Data structure format:
// height: u32
// layer_ends: array of u32, ending offset in bytes of layers. Does not include the root layer
// root: u64
// subtree layer: 2-64 u64s
// ...consecutive layers. Except for the last level, all sub-trees of the root must be complete
impl<'a> BtreeBitmap<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

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

    /// data must have been initialized by Self::init_new(). Returns the first free id, after (inclusive) of start
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

    fn get_level(&self, i: u32) -> U64GroupedBitmap<'a> {
        assert!(i < self.get_height());
        let start = get_level_start(self.data, i);
        let end = get_level_end(self.data, i);
        U64GroupedBitmap::new(&self.data[start..end])
    }

    fn get_height(&self) -> u32 {
        get_height(self.data)
    }
}

pub(crate) struct BtreeBitmapMut<'a> {
    data: &'a mut [u8],
}

impl<'a> BtreeBitmapMut<'a> {
    /// data must have been initialized by Self::init_new()
    pub(super) fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    // Initializes a new allocator, with no ids free
    pub(crate) fn init_new(data: &'a mut [u8], elements: u32) -> Self {
        assert!(data.len() >= Self::required_space(elements));
        let height = Self::required_tree_height(elements);
        data[HEIGHT_OFFSET..(HEIGHT_OFFSET + size_of::<u32>())]
            .copy_from_slice(&u32::try_from(height).unwrap().to_le_bytes());
        // Initialize the memory, so that all ids are allocated
        U64GroupedBitmapMut::init_full(&mut data[Self::tree_data_start(elements)..]);

        let mut tree_level_offsets = vec![];
        let mut offset = get_data_start(data);
        // root level -- not stored
        offset += size_of::<u64>();

        // Intermediate levels
        if Self::required_tree_height(elements) > 2 {
            for i in 1..(Self::required_tree_height(elements) - 1) {
                let len =
                    Self::required_subtrees(elements) * 64usize.pow(i.try_into().unwrap()) / 8;
                tree_level_offsets.push(offset + len);
                offset += len;
            }
        }

        // Leaf level
        if Self::required_tree_height(elements) > 1 {
            let len = (elements as usize + 63) / 64 * size_of::<u64>();
            tree_level_offsets.push(offset + len);
            offset += len;
        }

        assert_eq!(
            tree_level_offsets.len(),
            Self::required_tree_height(elements) - 1
        );
        assert_eq!(offset, Self::required_space(elements));

        let mut index = END_OFFSETS;
        for end in tree_level_offsets {
            data[index..(index + size_of::<u32>())]
                .copy_from_slice(&u32::try_from(end).unwrap().to_le_bytes());
            index += size_of::<u32>();
        }

        Self::new(data)
    }

    pub(crate) fn find_first_unset(&self) -> Option<u32> {
        BtreeBitmap::new(self.data).find_first_unset()
    }

    pub(crate) fn get(&self, i: u32) -> bool {
        BtreeBitmap::new(self.data).get(i)
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

    pub(crate) fn required_space(capacity: u32) -> usize {
        let tree_space = if Self::required_tree_height(capacity) == 1 {
            assert!(capacity <= 64);
            // Space for root
            size_of::<u64>()
        } else if Self::required_tree_height(capacity) == 2 {
            // Space for root
            size_of::<u64>() +
                // Space for the leaves
                U64GroupedBitmapMut::required_bytes(capacity)
        } else {
            // Space for root
            size_of::<u64>() +
                // Space for the subtrees
                Self::required_subtrees(capacity) * Self::required_interior_bytes_per_subtree(capacity) +
                // Space for the leaves
                U64GroupedBitmapMut::required_bytes(capacity)
        };

        Self::tree_data_start(capacity) + tree_space
    }

    fn tree_data_start(capacity: u32) -> usize {
        END_OFFSETS + (Self::required_tree_height(capacity) - 1) * size_of::<u32>()
    }

    fn required_interior_bytes_per_subtree(capacity: u32) -> usize {
        let subtree_height = Self::required_tree_height(capacity) - 1;
        (1..subtree_height)
            .map(|i| 64usize.pow(i.try_into().unwrap()))
            .sum::<usize>()
            / 8
    }

    fn required_subtrees(capacity: u32) -> usize {
        let height = Self::required_tree_height(capacity);
        let values_per_subtree = 64usize.pow((height - 1).try_into().unwrap());

        (capacity as usize + values_per_subtree - 1) / values_per_subtree
    }

    fn required_tree_height(capacity: u32) -> usize {
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

    fn get_level_mut(&mut self, i: u32) -> U64GroupedBitmapMut {
        assert!(i < self.get_height());
        let start = get_level_start(self.data, i);
        let end = get_level_end(self.data, i);
        U64GroupedBitmapMut::new(&mut self.data[start..end])
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

pub(crate) struct U64GroupedBitmapIter<'a> {
    data: &'a [u8],
    data_index: usize,
    current: u64,
}

impl<'a> U64GroupedBitmapIter<'a> {
    fn new(data: &'a [u8]) -> Self {
        let base = u64::from_le_bytes(data[..8].try_into().unwrap());
        Self {
            data,
            data_index: 0,
            current: base,
        }
    }
}

impl<'a> Iterator for U64GroupedBitmapIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current != 0 {
            let mut result: u32 = self.data_index.try_into().unwrap();
            result *= u8::BITS;
            let bit = self.current.trailing_zeros();
            result += bit;
            self.current &= !U64GroupedBitmapMut::select_mask(bit as usize);
            return Some(result);
        }
        self.data_index += size_of::<u64>();
        while self.data_index + size_of::<u64>() <= self.data.len() {
            let start = self.data_index;
            let end = start + size_of::<u64>();
            let next = u64::from_le_bytes(self.data[start..end].try_into().unwrap());
            if next != 0 {
                self.current = next;
                return self.next();
            }
            self.data_index += size_of::<u64>();
        }
        None
    }
}

pub(crate) struct U64GroupedBitmapDifference<'a, 'b> {
    data: &'a [u8],
    exclusion_data: &'b [u8],
    data_index: usize,
    current: u64,
}

impl<'a, 'b> U64GroupedBitmapDifference<'a, 'b> {
    fn new(data: &'a [u8], exclusion_data: &'b [u8]) -> Self {
        assert_eq!(data.len(), exclusion_data.len());
        let base = u64::from_le_bytes(data[..8].try_into().unwrap());
        let exclusion = u64::from_le_bytes(exclusion_data[..8].try_into().unwrap());
        Self {
            data,
            exclusion_data,
            data_index: 0,
            current: base & (!exclusion),
        }
    }
}

impl<'a, 'b> Iterator for U64GroupedBitmapDifference<'a, 'b> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current != 0 {
            let mut result: u32 = self.data_index.try_into().unwrap();
            result *= u8::BITS;
            let bit = self.current.trailing_zeros();
            result += bit;
            self.current &= !U64GroupedBitmapMut::select_mask(bit as usize);
            return Some(result);
        }
        self.data_index += size_of::<u64>();
        while self.data_index + size_of::<u64>() <= self.data.len() {
            let start = self.data_index;
            let end = start + size_of::<u64>();
            let next = u64::from_le_bytes(self.data[start..end].try_into().unwrap());
            let exclusion = u64::from_le_bytes(self.exclusion_data[start..end].try_into().unwrap());
            let next = next & (!exclusion);
            if next != 0 {
                self.current = next;
                return self.next();
            }
            self.data_index += size_of::<u64>();
        }
        None
    }
}

// A bitmap which groups consecutive groups of 64bits together
pub(crate) struct U64GroupedBitmap<'a> {
    data: &'a [u8],
}

impl<'a> U64GroupedBitmap<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        assert_eq!(data.len() % 8, 0);
        Self { data }
    }

    fn data_index_of(&self, bit: u32) -> (usize, usize) {
        (
            ((bit as usize) / 64) * size_of::<u64>(),
            ((bit as usize) % 64),
        )
    }

    fn count_unset(&self) -> u32 {
        self.data.iter().map(|x| x.count_zeros()).sum()
    }

    pub fn difference<'a0, 'b0>(
        &'a0 self,
        exclusion: U64GroupedBitmap<'b0>,
    ) -> U64GroupedBitmapDifference<'a0, 'b0> {
        U64GroupedBitmapDifference::new(self.data, exclusion.data)
    }

    pub fn iter(&self) -> U64GroupedBitmapIter {
        U64GroupedBitmapIter::new(self.data)
    }

    pub fn len(&self) -> u32 {
        let len: u32 = self.data.len().try_into().unwrap();
        len * u8::BITS
    }

    fn any_unset(&self) -> bool {
        self.data.iter().any(|x| x.count_zeros() > 0)
    }

    fn first_unset(&self, start_bit: u32, end_bit: u32) -> Option<u32> {
        assert_eq!(end_bit, (start_bit - start_bit % 64) + 64);

        let (index, bit) = self.data_index_of(start_bit);
        let mask = (1 << bit) - 1;
        let group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        let group = group | mask;
        match group.trailing_ones() {
            64 => None,
            x => Some(start_bit + x - u32::try_from(bit).unwrap()),
        }
    }

    pub fn get(&self, bit: u32) -> bool {
        let (index, bit_index) = self.data_index_of(bit);
        let group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group & U64GroupedBitmapMut::select_mask(bit_index) != 0
    }
}

pub(crate) struct U64GroupedBitmapMut<'a> {
    data: &'a mut [u8],
}

impl<'a> U64GroupedBitmapMut<'a> {
    pub fn required_bytes(elements: u32) -> usize {
        let words = (elements + 63) / 64;
        (words as usize) * size_of::<u64>()
    }

    pub fn init_full(data: &mut [u8]) {
        data.fill(0xFF);
    }

    pub fn init_empty(data: &mut [u8]) {
        data.fill(0);
    }

    pub fn new(data: &'a mut [u8]) -> Self {
        assert_eq!(data.len() % 8, 0);
        Self { data }
    }

    // Returns true iff the bit's group is all set
    pub fn set(&mut self, bit: u32) -> bool {
        let (index, bit_index) = self.data_index_of(bit as usize);
        let mut group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group |= Self::select_mask(bit_index);
        self.data[index..(index + 8)].copy_from_slice(&group.to_le_bytes());

        group == u64::MAX
    }

    pub fn clear(&mut self, bit: u32) {
        let (index, bit_index) = self.data_index_of(bit as usize);
        let mut group = u64::from_le_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group &= !Self::select_mask(bit_index);
        self.data[index..(index + 8)].copy_from_slice(&group.to_le_bytes());
    }

    fn data_index_of(&self, bit: usize) -> (usize, usize) {
        ((bit / 64) * size_of::<u64>(), (bit % 64))
    }

    fn select_mask(bit: usize) -> u64 {
        1u64 << (bit as u64)
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::bitmap::{
        BtreeBitmapMut, U64GroupedBitmap, U64GroupedBitmapMut,
    };
    use rand::prelude::IteratorRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;
    use std::convert::TryInto;

    #[test]
    fn alloc() {
        let num_pages = 2;
        let mut data = vec![0; BtreeBitmapMut::required_space(num_pages)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, num_pages);
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
        let mut data = vec![0; BtreeBitmapMut::required_space(2)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, 2);
        allocator.clear(0);
        allocator.clear(1);
        allocator.set(0);
        assert_eq!(1, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
    }

    #[test]
    fn free() {
        let mut data = vec![0; BtreeBitmapMut::required_space(1)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, 1);
        allocator.clear(0);
        assert_eq!(0, allocator.alloc().unwrap());
        assert!(allocator.alloc().is_none());
        allocator.clear(0);
        assert_eq!(0, allocator.alloc().unwrap());
    }

    #[test]
    fn reuse_lowest() {
        let num_pages = 65;
        let mut data = vec![0; BtreeBitmapMut::required_space(num_pages)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, num_pages);
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
        let mut data = vec![0; BtreeBitmapMut::required_space(num_pages)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        // Allocate everything
        while allocator.alloc().is_some() {}
        // The last u64 must be used, since the leaf layer is compact
        let l = data.len();
        assert_eq!(
            u64::MAX,
            u64::from_le_bytes(data[(l - 8)..].try_into().unwrap())
        );
    }

    #[test]
    fn find_free() {
        let num_pages = 129;
        let mut data = vec![0; BtreeBitmapMut::required_space(num_pages)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, num_pages);
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
    fn iter() {
        let num_pages = 129;
        let mut data = vec![0; U64GroupedBitmapMut::required_bytes(num_pages)];
        U64GroupedBitmapMut::init_empty(&mut data);
        let mut bitmap = U64GroupedBitmapMut::new(&mut data);
        let values = [0, 1, 33, 63, 64, 65, 90, 126, 127, 128];
        for x in values {
            bitmap.set(x);
        }
        drop(bitmap);
        let bitmap = U64GroupedBitmap::new(&data);
        for (i, x) in bitmap.iter().enumerate() {
            assert_eq!(values[i], x);
        }
        assert_eq!(bitmap.iter().count(), values.len());
    }

    #[test]
    fn random_pattern() {
        let seed = rand::thread_rng().gen();
        // Print the seed to debug for reproducibility, in case this test fails
        println!("seed={seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let num_pages = rng.gen_range(2..10000);
        let mut data = vec![0; BtreeBitmapMut::required_space(num_pages)];
        let mut allocator = BtreeBitmapMut::init_new(&mut data, num_pages);
        for i in 0..num_pages {
            allocator.clear(i);
        }
        let mut allocated = HashSet::new();

        for _ in 0..(num_pages * 2) {
            if rng.gen_bool(0.75) {
                if let Some(page) = allocator.alloc() {
                    allocated.insert(page);
                } else {
                    assert_eq!(allocated.len(), num_pages as usize);
                }
            } else if let Some(to_free) = allocated.iter().choose(&mut rng).cloned() {
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
