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
//
// Entry format is:
// TODO: use 4 bytes instead of 8
// * (8 bytes) key_size
// * (key_size bytes) key_data
// * (8 bytes) value_size
// * (value_size bytes) value_data
pub struct EntryAccessor<'a> {
    raw: &'a [u8],
}

impl<'a> EntryAccessor<'a> {
    pub(in crate::tree_store) fn new(raw: &'a [u8]) -> Self {
        EntryAccessor { raw }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.raw[0..8].try_into().unwrap()) as usize
    }

    pub(in crate::tree_store) fn value_offset(&self) -> usize {
        8 + self.key_len() + 8
    }

    fn value_len(&self) -> usize {
        let key_len = self.key_len();
        u64::from_be_bytes(
            self.raw[(8 + key_len)..(8 + key_len + 8)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn raw_len(&self) -> usize {
        8 + self.key_len() + 8 + self.value_len()
    }
}

impl<'a: 'b, 'b> BtreeEntry<'a, 'b> for EntryAccessor<'a> {
    fn key(&'b self) -> &'a [u8] {
        &self.raw[8..(8 + self.key_len())]
    }

    fn value(&'b self) -> &'a [u8] {
        &self.raw[self.value_offset()..(self.value_offset() + self.value_len())]
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct EntryMutator<'a> {
    raw: &'a mut [u8],
}

impl<'a> EntryMutator<'a> {
    fn new(raw: &'a mut [u8]) -> Self {
        EntryMutator { raw }
    }

    fn write_key(&mut self, key: &[u8]) {
        self.raw[0..8].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.raw[8..(8 + key.len())].copy_from_slice(key);
    }

    fn write_value(&mut self, value: &[u8]) {
        let value_offset = EntryAccessor::new(self.raw).value_offset();
        self.raw[(value_offset - 8)..value_offset]
            .copy_from_slice(&(value.len() as u64).to_be_bytes());
        self.raw[value_offset..(value_offset + value.len())].copy_from_slice(value);
    }
}

// TODO: support more than 2 entries in a leaf
// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 1 = LEAF
// * (n bytes) lesser_entry
// * (n bytes) greater_entry: optional
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

    pub(in crate::tree_store) fn offset_of_lesser(&self) -> usize {
        1
    }

    pub(in crate::tree_store) fn offset_of_greater(&self) -> usize {
        1 + self.lesser().raw_len()
    }

    pub(in crate::tree_store) fn lesser(&self) -> EntryAccessor<'b> {
        EntryAccessor::new(&self.page.memory()[self.offset_of_lesser()..])
    }

    pub(in crate::tree_store) fn greater(&self) -> Option<EntryAccessor<'b>> {
        let entry = EntryAccessor::new(&self.page.memory()[self.offset_of_greater()..]);
        if entry.key_len() == 0 {
            None
        } else {
            Some(entry)
        }
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
pub(in crate::tree_store) struct LeafBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> LeafBuilder<'a, 'b> {
    pub(in crate::tree_store) fn required_bytes(keys_values: &[&[u8]]) -> usize {
        assert_eq!(keys_values.len() % 2, 0);
        // Page id;
        let mut result = 1;
        // key & value lengths
        result += keys_values.len() * size_of::<u64>();
        result += keys_values.iter().map(|x| x.len()).sum::<usize>();

        result
    }

    pub(in crate::tree_store) fn new(page: &'b mut PageMut<'a>) -> Self {
        page.memory_mut()[0] = LEAF;
        LeafBuilder { page }
    }

    pub(in crate::tree_store) fn write_lesser(&mut self, key: &[u8], value: &[u8]) {
        let mut entry = EntryMutator::new(&mut self.page.memory_mut()[1..]);
        entry.write_key(key);
        entry.write_value(value);
    }

    pub(in crate::tree_store) fn write_greater(&mut self, entry: Option<(&[u8], &[u8])>) {
        let offset = 1 + EntryAccessor::new(&self.page.memory()[1..]).raw_len();
        let mut writer = EntryMutator::new(&mut self.page.memory_mut()[offset..]);
        if let Some((key, value)) = entry {
            writer.write_key(key);
            writer.write_value(value);
        } else {
            writer.write_key(&[]);
        }
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
