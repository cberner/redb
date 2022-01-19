use crate::tree_store::page_store::{Page, PageMut};
use crate::tree_store::PageNumber;
use crate::types::RedbKey;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::size_of;

pub(in crate::tree_store) const BTREE_ORDER: usize = 40;
pub(in crate::tree_store) const LEAF: u8 = 1;
pub(in crate::tree_store) const INTERNAL: u8 = 2;

pub trait BtreeEntry<'a: 'b, 'b> {
    fn key(&'b self) -> &'a [u8];
    fn value(&'b self) -> &'a [u8];
}

pub struct AccessGuardMut<'a> {
    page: PageMut<'a>,
    offset: usize,
    len: usize,
}

impl<'a> AccessGuardMut<'a> {
    pub(in crate::tree_store) fn new(page: PageMut<'a>, offset: usize, len: usize) -> Self {
        AccessGuardMut { page, offset, len }
    }
}

impl<'a> AsMut<[u8]> for AccessGuardMut<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.page.memory_mut()[self.offset..(self.offset + self.len)]
    }
}

// Provides a simple zero-copy way to access entries
pub struct EntryAccessor<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> EntryAccessor<'a> {
    fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        EntryAccessor { key, value }
    }
}

impl<'a: 'b, 'b> BtreeEntry<'a, 'b> for EntryAccessor<'a> {
    fn key(&'b self) -> &'a [u8] {
        self.key
    }

    fn value(&'b self) -> &'a [u8] {
        self.value
    }
}

// Provides a simple zero-copy way to access a leaf page
pub(in crate::tree_store) struct LeafAccessor<'a: 'b, 'b, T: Page + 'a> {
    page: &'b T,
    _page_lifetime: PhantomData<&'a ()>,
}

impl<'a: 'b, 'b, T: Page + 'a> LeafAccessor<'a, 'b, T> {
    pub(in crate::tree_store) fn new(page: &'b T) -> Self {
        LeafAccessor {
            page,
            _page_lifetime: Default::default(),
        }
    }

    pub(in crate::tree_store) fn position<K: RedbKey + ?Sized>(
        &self,
        query: &[u8],
    ) -> (usize, bool) {
        // inclusive
        let mut min_entry = 0;
        // inclusive. Start past end, since it might be positioned beyond the end of the leaf
        let mut max_entry = self.num_pairs();
        while min_entry < max_entry {
            let mid = (min_entry + max_entry) / 2;
            let key = self.entry(mid).unwrap().key();
            match K::compare(query, key) {
                Ordering::Less => {
                    max_entry = mid;
                }
                Ordering::Equal => {
                    return (mid, true);
                }
                Ordering::Greater => {
                    min_entry = mid + 1;
                }
            }
        }
        debug_assert_eq!(min_entry, max_entry);
        (min_entry, false)
    }

    pub(in crate::tree_store) fn find_key<K: RedbKey + ?Sized>(
        &self,
        query: &[u8],
    ) -> Option<usize> {
        let (entry, found) = self.position::<K>(query);
        if found {
            Some(entry)
        } else {
            None
        }
    }

    fn key_start(&self, n: usize) -> Option<usize> {
        if n == 0 {
            Some(4 + 2 * size_of::<u32>() * self.num_pairs())
        } else {
            self.value_end(n - 1)
        }
    }

    fn key_end(&self, n: usize) -> Option<usize> {
        if n >= self.num_pairs() {
            None
        } else {
            let offset = 4 + 2 * size_of::<u32>() * n;
            let end = u32::from_be_bytes(
                self.page.memory()[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            Some(end)
        }
    }

    fn value_end(&self, n: usize) -> Option<usize> {
        if n >= self.num_pairs() {
            None
        } else {
            let offset = 4 + 2 * size_of::<u32>() * n + size_of::<u32>();
            let end = u32::from_be_bytes(
                self.page.memory()[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            Some(end)
        }
    }

    pub(in crate::tree_store) fn num_pairs(&self) -> usize {
        u16::from_be_bytes(self.page.memory()[2..4].try_into().unwrap()) as usize
    }

    pub(in crate::tree_store) fn offset_of_first_value(&self) -> usize {
        self.key_end(0).unwrap()
    }

    pub(in crate::tree_store) fn offset_of_value(&self, n: usize) -> Option<usize> {
        self.key_end(n)
    }

    // Returns the length of all keys and values between [start, end)
    pub(in crate::tree_store) fn length_of_pairs(&self, start: usize, end: usize) -> usize {
        let end_offset = self.value_end(end - 1).unwrap();
        let start_offset = self.key_start(start).unwrap();
        end_offset - start_offset
    }

    pub(in crate::tree_store) fn entry(&self, n: usize) -> Option<EntryAccessor<'b>> {
        let key = &self.page.memory()[self.key_start(n)?..self.key_end(n)?];
        let value = &self.page.memory()[self.key_end(n)?..self.value_end(n)?];
        Some(EntryAccessor::new(key, value))
    }

    pub(in crate::tree_store) fn first_entry(&self) -> EntryAccessor<'b> {
        self.entry(0).unwrap()
    }

    pub(in crate::tree_store) fn last_entry(&self) -> EntryAccessor<'b> {
        self.entry(self.num_pairs() - 1).unwrap()
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
// Layout is:
// 1 byte: type
// 1 byte: reserved (padding to 32bits aligned)
// 2 bytes: num_entries (number of pairs)
// TODO: try separating key & value data for better lookup performance
// repeating num_entries times:
// 4 bytes: key_end
// 4 bytes: value_end
// repeating (num_entries times):
// * n bytes: key data
// * n bytes: value data
pub(in crate::tree_store) struct LeafBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
    num_pairs: usize,
    pairs_written: usize, // used for debugging
}

impl<'a: 'b, 'b> LeafBuilder<'a, 'b> {
    pub(in crate::tree_store) fn required_bytes(
        num_pairs: usize,
        keys_values_bytes: usize,
    ) -> usize {
        // Page id & header;
        let mut result = 4;
        // key & value lengths
        result += num_pairs * 2 * size_of::<u32>();
        result += keys_values_bytes;

        result
    }

    pub(in crate::tree_store) fn new(page: &'b mut PageMut<'a>, num_pairs: usize) -> Self {
        page.memory_mut()[0] = LEAF;
        page.memory_mut()[2..4].copy_from_slice(&(num_pairs as u16).to_be_bytes());
        #[cfg(debug_assertions)]
        {
            // Poison all the key & value offsets, in case the caller forgets to write them
            let last = 4 + 2 * size_of::<u32>() * num_pairs;
            for x in &mut page.memory_mut()[4..last] {
                *x = 0xFF;
            }
        }
        LeafBuilder {
            page,
            num_pairs,
            pairs_written: 0,
        }
    }

    fn pair_end(&self, n: usize) -> usize {
        let offset = 4 + 2 * size_of::<u32>() * n + size_of::<u32>();
        u32::from_be_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(in crate::tree_store) fn append(&mut self, key: &[u8], value: &[u8]) {
        let key_offset = if self.pairs_written == 0 {
            4 + 2 * size_of::<u32>() * self.num_pairs
        } else {
            self.pair_end(self.pairs_written - 1)
        };

        let n = self.pairs_written;
        let offset = 4 + 2 * size_of::<u32>() * n;
        self.page.memory_mut()[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&((key_offset + key.len()) as u32).to_be_bytes());
        self.page.memory_mut()[key_offset..(key_offset + key.len())].copy_from_slice(key);

        let offset = 4 + 2 * size_of::<u32>() * n + size_of::<u32>();
        let value_offset = key_offset + key.len();
        self.page.memory_mut()[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&((value_offset + value.len()) as u32).to_be_bytes());
        self.page.memory_mut()[value_offset..(value_offset + value.len())].copy_from_slice(value);
        self.pairs_written += 1;
    }
}

impl<'a: 'b, 'b> Drop for LeafBuilder<'a, 'b> {
    fn drop(&mut self) {
        assert_eq!(self.pairs_written, self.num_pairs);
    }
}

// Provides a simple zero-copy way to access an index page
pub(in crate::tree_store) struct InternalAccessor<'a: 'b, 'b, T: Page + 'a> {
    page: &'b T,
    _page_lifetime: PhantomData<&'a ()>,
}

impl<'a: 'b, 'b, T: Page + 'a> InternalAccessor<'a, 'b, T> {
    pub(in crate::tree_store) fn new(page: &'b T) -> Self {
        debug_assert_eq!(page.memory()[0], INTERNAL);
        InternalAccessor {
            page,
            _page_lifetime: Default::default(),
        }
    }

    pub(in crate::tree_store) fn child_for_key<K: RedbKey + ?Sized>(
        &self,
        query: &[u8],
    ) -> (usize, PageNumber) {
        let mut min_child = 0; // inclusive
        let mut max_child = BTREE_ORDER - 1; // inclusive
        while min_child < max_child {
            let mid = (min_child + max_child) / 2;
            if let Some(key) = self.key(mid) {
                match K::compare(query, key) {
                    Ordering::Less => {
                        max_child = mid;
                    }
                    Ordering::Equal => {
                        return (mid, self.child_page(mid).unwrap());
                    }
                    Ordering::Greater => {
                        min_child = mid + 1;
                    }
                }
            } else {
                max_child = mid;
            }
        }
        debug_assert_eq!(min_child, max_child);

        (min_child, self.child_page(min_child).unwrap())
    }

    fn key_offset(&self, n: usize) -> usize {
        if n == 0 {
            4 + PageNumber::serialized_size() * self.count_children()
                + size_of::<u32>() * self.num_keys()
        } else {
            self.key_end(n - 1)
        }
    }

    fn key_end(&self, n: usize) -> usize {
        let offset =
            4 + PageNumber::serialized_size() * self.count_children() + size_of::<u32>() * n;
        u32::from_be_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(in crate::tree_store) fn key(&self, n: usize) -> Option<&[u8]> {
        debug_assert!(n < BTREE_ORDER - 1);
        if n >= self.num_keys() {
            return None;
        }
        let offset = self.key_offset(n);
        let end = self.key_end(n);
        Some(&self.page.memory()[offset..end])
    }

    pub(in crate::tree_store) fn count_children(&self) -> usize {
        self.num_keys() + 1
    }

    pub(in crate::tree_store) fn child_page(&self, n: usize) -> Option<PageNumber> {
        debug_assert!(n < BTREE_ORDER);
        if n >= self.count_children() {
            return None;
        }

        let offset = 4 + PageNumber::serialized_size() * n;
        Some(PageNumber::from_be_bytes(
            self.page.memory()[offset..(offset + PageNumber::serialized_size())]
                .try_into()
                .unwrap(),
        ))
    }

    pub(in crate::tree_store) fn total_key_length(&self) -> usize {
        self.key_end(self.num_keys() - 1)
    }

    fn num_keys(&self) -> usize {
        u16::from_be_bytes(self.page.memory()[2..4].try_into().unwrap()) as usize
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
// Layout is:
// 1 byte: type
// 1 byte: reserved (padding to 32bits aligned)
// 2 bytes: num_keys (number of keys)
// repeating (num_keys + 1 times):
// 8 bytes: page number
// repeating (num_keys times):
// * 4 bytes: key end. Ending offset of the key, exclusive
// repeating (num_keys times):
// * n bytes: key data
pub(in crate::tree_store) struct InternalBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
    num_keys: usize,
    keys_written: usize, // used for debugging
}

impl<'a: 'b, 'b> InternalBuilder<'a, 'b> {
    pub(in crate::tree_store) fn required_bytes(num_keys: usize, size_of_keys: usize) -> usize {
        let fixed_size =
            4 + PageNumber::serialized_size() * (num_keys + 1) + size_of::<u32>() * num_keys;
        size_of_keys + fixed_size
    }

    // Caller MUST write num_keys values
    pub(in crate::tree_store) fn new(page: &'b mut PageMut<'a>, num_keys: usize) -> Self {
        page.memory_mut()[0] = INTERNAL;
        page.memory_mut()[2..4].copy_from_slice(&(num_keys as u16).to_be_bytes());
        #[cfg(debug_assertions)]
        {
            // Poison all the child pointers & key offsets, in case the caller forgets to write them
            let last =
                4 + PageNumber::serialized_size() * (num_keys + 1) + size_of::<u32>() * num_keys;
            for x in &mut page.memory_mut()[4..last] {
                *x = 0xFF;
            }
        }
        InternalBuilder {
            page,
            num_keys,
            keys_written: 0,
        }
    }

    pub(in crate::tree_store) fn write_first_page(&mut self, page_number: PageNumber) {
        let offset = 4;
        self.page.memory_mut()[offset..(offset + PageNumber::serialized_size())]
            .copy_from_slice(&page_number.to_be_bytes());
    }

    fn key_end(&self, n: usize) -> usize {
        let offset = 4 + PageNumber::serialized_size() * (self.num_keys + 1) + size_of::<u32>() * n;
        u32::from_be_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    // Write the nth key and page of values greater than this key, but less than or equal to the next
    // Caller must write keys & pages in increasing order
    pub(in crate::tree_store) fn write_nth_key(
        &mut self,
        key: &[u8],
        page_number: PageNumber,
        n: usize,
    ) {
        assert!(n < self.num_keys as usize);
        assert_eq!(n, self.keys_written);
        self.keys_written += 1;
        let offset = 4 + PageNumber::serialized_size() * (n + 1);
        self.page.memory_mut()[offset..(offset + PageNumber::serialized_size())]
            .copy_from_slice(&page_number.to_be_bytes());

        let data_offset = if n > 0 {
            self.key_end(n - 1)
        } else {
            4 + PageNumber::serialized_size() * (self.num_keys + 1)
                + size_of::<u32>() * self.num_keys
        };
        let offset = 4 + PageNumber::serialized_size() * (self.num_keys + 1) + size_of::<u32>() * n;
        self.page.memory_mut()[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&((data_offset + key.len()) as u32).to_be_bytes());

        debug_assert!(data_offset > offset);
        self.page.memory_mut()[data_offset..(data_offset + key.len())].copy_from_slice(key);
    }
}

impl<'a: 'b, 'b> Drop for InternalBuilder<'a, 'b> {
    fn drop(&mut self) {
        assert_eq!(self.keys_written, self.num_keys);
    }
}

pub(in crate::tree_store) struct InternalMutator<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> InternalMutator<'a, 'b> {
    pub(in crate::tree_store) fn new(page: &'b mut PageMut<'a>) -> Self {
        assert_eq!(page.memory()[0], INTERNAL);
        Self { page }
    }

    fn num_keys(&self) -> usize {
        u16::from_be_bytes(self.page.memory()[2..4].try_into().unwrap()) as usize
    }

    pub(in crate::tree_store) fn write_child_page(&mut self, i: usize, page_number: PageNumber) {
        debug_assert!(i <= self.num_keys());
        let offset = 4 + PageNumber::serialized_size() * i;
        self.page.memory_mut()[offset..(offset + PageNumber::serialized_size())]
            .copy_from_slice(&page_number.to_be_bytes());
    }
}
