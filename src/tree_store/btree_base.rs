use crate::tree_store::page_store::{ChecksumType, Page, PageImpl, PageMut, TransactionalMemory};
use crate::tree_store::{page_store, PageNumber};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Result;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::size_of;
use std::thread;

pub(crate) const LEAF: u8 = 1;
pub(crate) const BRANCH: u8 = 2;

pub(crate) type Checksum = u128;

pub(super) fn leaf_checksum<T: Page>(
    page: &T,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    checksum: ChecksumType,
) -> Checksum {
    let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
    // TODO: during verification, the page could be corrupted, so this needs to be safe on
    // arbitrary data
    let end = accessor.value_end(accessor.num_pairs() - 1).unwrap();
    match checksum {
        ChecksumType::Zero => 0,
        ChecksumType::XXH3_128 => page_store::hash128_with_seed(&page.memory()[..end], 0),
    }
}

pub(super) fn branch_checksum<T: Page>(
    page: &T,
    fixed_key_size: Option<usize>,
    checksum: ChecksumType,
) -> Checksum {
    let accessor = BranchAccessor::new(page, fixed_key_size);
    // TODO: during verification, the page could be corrupted, so this needs to be safe on
    // arbitrary data
    let end = accessor.key_end(accessor.num_keys() - 1);
    match checksum {
        ChecksumType::Zero => 0,
        ChecksumType::XXH3_128 => page_store::hash128_with_seed(&page.memory()[..end], 0),
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum FreePolicy {
    // Never free pages during the operation. Defer until commit
    Never,
    // Free uncommitted pages immediately
    Uncommitted,
}

impl FreePolicy {
    // Safety: Caller must ensure there are no references to page, unless self is FreePolicy::Never
    pub(crate) unsafe fn conditional_free(
        &self,
        page: PageNumber,
        freed: &mut Vec<PageNumber>,
        mem: &TransactionalMemory,
    ) -> Result {
        match self {
            FreePolicy::Never => {
                freed.push(page);
            }
            FreePolicy::Uncommitted => {
                if !mem.free_if_uncommitted(page)? {
                    freed.push(page);
                }
            }
        }

        Ok(())
    }

    pub(crate) fn free_on_drop(&self, page: PageNumber, mem: &TransactionalMemory) -> bool {
        match self {
            FreePolicy::Never => false,
            FreePolicy::Uncommitted => mem.uncommitted(page),
        }
    }
}

enum OnDrop {
    None,
    Free(PageNumber),
    RemoveEntry {
        position: usize,
        fixed_key_size: Option<usize>,
    },
}

enum EitherPage<'a> {
    Immutable(PageImpl<'a>),
    Mutable(PageMut<'a>),
    OwnedMemory(Vec<u8>),
}

impl<'a> EitherPage<'a> {
    fn memory(&self) -> &[u8] {
        match self {
            EitherPage::Immutable(page) => page.memory(),
            EitherPage::Mutable(page) => page.memory(),
            EitherPage::OwnedMemory(mem) => mem.as_slice(),
        }
    }
}

pub struct AccessGuard<'a, V: RedbValue + ?Sized> {
    page: EitherPage<'a>,
    offset: usize,
    len: usize,
    on_drop: OnDrop,
    mem: &'a TransactionalMemory,
    _value_type: PhantomData<V>,
}

impl<'a, V: RedbValue + ?Sized> AccessGuard<'a, V> {
    // Safety: if free_on_drop is true, caller must guarantee that no other references to page exist,
    // and that no references will be created until this AccessGuard is dropped
    pub(super) unsafe fn new(
        page: PageImpl<'a>,
        offset: usize,
        len: usize,
        free_on_drop: bool,
        mem: &'a TransactionalMemory,
    ) -> Self {
        let page_number = page.get_page_number();
        Self {
            page: EitherPage::Immutable(page),
            offset,
            len,
            on_drop: if free_on_drop {
                OnDrop::Free(page_number)
            } else {
                OnDrop::None
            },
            mem,
            _value_type: Default::default(),
        }
    }

    pub(super) fn with_owned_value(value: Vec<u8>, mem: &'a TransactionalMemory) -> Self {
        let len = value.len();
        Self {
            page: EitherPage::OwnedMemory(value),
            offset: 0,
            len,
            on_drop: OnDrop::None,
            mem,
            _value_type: Default::default(),
        }
    }

    // Safety: if free_on_drop is true, caller must guarantee that no other references to page exist,
    // and that no references will be created until this AccessGuard is dropped
    pub(super) unsafe fn remove_on_drop(
        page: PageMut<'a>,
        offset: usize,
        len: usize,
        position: usize,
        fixed_key_size: Option<usize>,
        mem: &'a TransactionalMemory,
    ) -> Self {
        Self {
            page: EitherPage::Mutable(page),
            offset,
            len,
            on_drop: OnDrop::RemoveEntry {
                position,
                fixed_key_size,
            },
            mem,
            _value_type: Default::default(),
        }
    }

    // TODO: implement Deref instead of this to_value() method, when GAT is stable
    pub fn to_value(&self) -> <<V as RedbValue>::View as WithLifetime>::Out {
        V::from_bytes(&self.page.memory()[self.offset..(self.offset + self.len)])
    }
}

impl<'a, V: RedbValue + ?Sized> Drop for AccessGuard<'a, V> {
    fn drop(&mut self) {
        match self.on_drop {
            OnDrop::None => {}
            OnDrop::Free(page_number) => {
                // Safety: caller to new() guaranteed that no other references to this page exist
                unsafe {
                    self.mem.free(page_number).unwrap();
                }
            }
            OnDrop::RemoveEntry {
                position,
                fixed_key_size,
            } => {
                if let EitherPage::Mutable(ref mut mut_page) = self.page {
                    let mut mutator = LeafMutator::new(mut_page, fixed_key_size, V::fixed_width());
                    mutator.remove(position);
                } else if !thread::panicking() {
                    unreachable!();
                }
            }
        }
    }
}

pub struct AccessGuardMut<'a> {
    page: PageMut<'a>,
    offset: usize,
    len: usize,
}

impl<'a> AccessGuardMut<'a> {
    pub(crate) fn new(page: PageMut<'a>, offset: usize, len: usize) -> Self {
        AccessGuardMut { page, offset, len }
    }
}

// TODO: this should return a RedbValue typed reference
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

impl<'a: 'b, 'b> EntryAccessor<'a> {
    pub(crate) fn key(&'b self) -> &'a [u8] {
        self.key
    }

    pub(crate) fn value(&'b self) -> &'a [u8] {
        self.value
    }
}

// Provides a simple zero-copy way to access a leaf page
pub(crate) struct LeafAccessor<'a> {
    page: &'a [u8],
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    num_pairs: usize,
}

impl<'a> LeafAccessor<'a> {
    pub(crate) fn new(
        page: &'a [u8],
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
    ) -> Self {
        debug_assert_eq!(page[0], LEAF);
        let num_pairs = u16::from_le_bytes(page[2..4].try_into().unwrap()) as usize;
        LeafAccessor {
            page,
            fixed_key_size,
            fixed_value_size,
            num_pairs,
        }
    }

    pub(super) fn print_node<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        include_value: bool,
    ) {
        let mut i = 0;
        while let Some(entry) = self.entry(i) {
            eprint!(" key_{}={:?}", i, K::from_bytes(entry.key()));
            if include_value {
                eprint!(" value_{}={:?}", i, V::from_bytes(entry.value()));
            }
            i += 1;
        }
    }

    pub(crate) fn position<K: RedbKey + ?Sized>(&self, query: &[u8]) -> (usize, bool) {
        // inclusive
        let mut min_entry = 0;
        // inclusive. Start past end, since it might be positioned beyond the end of the leaf
        let mut max_entry = self.num_pairs();
        while min_entry < max_entry {
            let mid = (min_entry + max_entry) / 2;
            let key = self.key_unchecked(mid);
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

    pub(crate) fn find_key<K: RedbKey + ?Sized>(&self, query: &[u8]) -> Option<usize> {
        let (entry, found) = self.position::<K>(query);
        if found {
            Some(entry)
        } else {
            None
        }
    }

    fn key_section_start(&self) -> usize {
        let mut offset = 4;
        if self.fixed_key_size.is_none() {
            offset += size_of::<u32>() * self.num_pairs;
        }
        if self.fixed_value_size.is_none() {
            offset += size_of::<u32>() * self.num_pairs;
        }

        offset
    }

    fn key_start(&self, n: usize) -> Option<usize> {
        if n == 0 {
            Some(self.key_section_start())
        } else {
            self.key_end(n - 1)
        }
    }

    fn key_end(&self, n: usize) -> Option<usize> {
        if n >= self.num_pairs() {
            None
        } else {
            if let Some(fixed) = self.fixed_key_size {
                return Some(self.key_section_start() + fixed * (n + 1));
            }
            let offset = 4 + size_of::<u32>() * n;
            let end = u32::from_le_bytes(
                self.page[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            Some(end)
        }
    }

    fn value_start(&self, n: usize) -> Option<usize> {
        if n == 0 {
            self.key_end(self.num_pairs() - 1)
        } else {
            self.value_end(n - 1)
        }
    }

    fn value_end(&self, n: usize) -> Option<usize> {
        if n >= self.num_pairs() {
            None
        } else {
            if let Some(fixed) = self.fixed_value_size {
                return Some(self.key_end(self.num_pairs - 1).unwrap() + fixed * (n + 1));
            }
            let mut offset = 4 + size_of::<u32>() * n;
            if self.fixed_key_size.is_none() {
                offset += size_of::<u32>() * self.num_pairs;
            }
            let end = u32::from_le_bytes(
                self.page[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            Some(end)
        }
    }

    pub(crate) fn num_pairs(&self) -> usize {
        self.num_pairs
    }

    pub(super) fn offset_of_first_value(&self) -> usize {
        self.offset_of_value(0).unwrap()
    }

    pub(super) fn offset_of_value(&self, n: usize) -> Option<usize> {
        self.value_start(n)
    }

    pub(super) fn value_range(&self, n: usize) -> Option<(usize, usize)> {
        Some((self.value_start(n)?, self.value_end(n)?))
    }

    // Returns the length of all keys and values between [start, end)
    pub(crate) fn length_of_pairs(&self, start: usize, end: usize) -> usize {
        self.length_of_values(start, end) + self.length_of_keys(start, end)
    }

    fn length_of_values(&self, start: usize, end: usize) -> usize {
        if end == 0 {
            return 0;
        }
        let end_offset = self.value_end(end - 1).unwrap();
        let start_offset = self.value_start(start).unwrap();
        end_offset - start_offset
    }

    // Returns the length of all keys between [start, end)
    pub(crate) fn length_of_keys(&self, start: usize, end: usize) -> usize {
        if end == 0 {
            return 0;
        }
        let end_offset = self.key_end(end - 1).unwrap();
        let start_offset = self.key_start(start).unwrap();
        end_offset - start_offset
    }

    pub(crate) fn total_length(&self) -> usize {
        // Values are stored last
        self.value_end(self.num_pairs() - 1).unwrap()
    }

    fn key_unchecked(&self, n: usize) -> &[u8] {
        &self.page[self.key_start(n).unwrap()..self.key_end(n).unwrap()]
    }

    pub(crate) fn entry(&self, n: usize) -> Option<EntryAccessor<'a>> {
        let key = &self.page[self.key_start(n)?..self.key_end(n)?];
        let value = &self.page[self.value_start(n)?..self.value_end(n)?];
        Some(EntryAccessor::new(key, value))
    }

    pub(super) fn last_entry(&self) -> EntryAccessor<'a> {
        self.entry(self.num_pairs() - 1).unwrap()
    }
}

pub(crate) struct LeafKeyIter {
    data: Vec<u8>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    start_entry: isize, // inclusive
    end_entry: isize,   // inclusive
    reverse: bool,
}

impl LeafKeyIter {
    pub(crate) fn new(
        data: Vec<u8>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
    ) -> Self {
        let accessor = LeafAccessor::new(&data, fixed_key_size, fixed_value_size);
        let end_entry = accessor.num_pairs() as isize - 1;
        Self {
            data,
            fixed_key_size,
            fixed_value_size,
            start_entry: 0,
            end_entry,
            reverse: false,
        }
    }

    pub(crate) fn next_key(&mut self) -> Option<&[u8]> {
        if self.end_entry < self.start_entry {
            return None;
        }
        let accessor = LeafAccessor::new(&self.data, self.fixed_key_size, self.fixed_value_size);
        if self.reverse {
            self.end_entry -= 1;
            accessor
                .entry((self.end_entry + 1) as usize)
                .map(|e| e.key())
        } else {
            self.start_entry += 1;
            accessor
                .entry((self.start_entry - 1) as usize)
                .map(|e| e.key())
        }
    }

    pub(crate) fn reverse(self) -> Self {
        Self {
            data: self.data,
            fixed_key_size: self.fixed_key_size,
            fixed_value_size: self.fixed_value_size,
            start_entry: self.start_entry,
            end_entry: self.end_entry,
            reverse: !self.reverse,
        }
    }
}

pub(super) struct LeafBuilder<'a, 'b> {
    pairs: Vec<(&'a [u8], &'a [u8])>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    total_key_bytes: usize,
    total_value_bytes: usize,
    mem: &'b TransactionalMemory,
}

impl<'a, 'b> LeafBuilder<'a, 'b> {
    pub(super) fn required_bytes(num_pairs: usize, keys_values_bytes: usize) -> usize {
        RawLeafBuilder::required_bytes(num_pairs, keys_values_bytes)
    }

    pub(super) fn new(
        mem: &'b TransactionalMemory,
        capacity: usize,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
    ) -> Self {
        Self {
            pairs: Vec::with_capacity(capacity),
            fixed_key_size,
            fixed_value_size,
            total_key_bytes: 0,
            total_value_bytes: 0,
            mem,
        }
    }

    pub(super) fn push(&mut self, key: &'a [u8], value: &'a [u8]) {
        self.total_key_bytes += key.len();
        self.total_value_bytes += value.len();
        self.pairs.push((key, value))
    }

    pub(super) fn push_all_except(
        &mut self,
        accessor: &'a LeafAccessor<'_>,
        except: Option<usize>,
    ) {
        for i in 0..accessor.num_pairs() {
            if let Some(except) = except {
                if except == i {
                    continue;
                }
            }
            let entry = accessor.entry(i).unwrap();
            self.push(entry.key(), entry.value());
        }
    }

    pub(super) fn should_split(&self) -> bool {
        let required_size = Self::required_bytes(
            self.pairs.len(),
            self.total_key_bytes + self.total_value_bytes,
        );
        required_size > self.mem.get_page_size() && self.pairs.len() > 1
    }

    pub(super) fn build_split(self) -> Result<(PageMut<'b>, &'a [u8], PageMut<'b>)> {
        let total_size = self.total_key_bytes + self.total_value_bytes;
        let mut division = 0;
        let mut first_split_key_bytes = 0;
        let mut first_split_value_bytes = 0;
        for (key, value) in self.pairs.iter().take(self.pairs.len() - 1) {
            first_split_key_bytes += key.len();
            first_split_value_bytes += value.len();
            division += 1;
            if first_split_key_bytes + first_split_value_bytes >= total_size / 2 {
                break;
            }
        }

        let required_size =
            Self::required_bytes(division, first_split_key_bytes + first_split_value_bytes);
        let mut page1 = self.mem.allocate(required_size)?;
        let mut builder = RawLeafBuilder::new(
            page1.memory_mut(),
            division,
            self.fixed_key_size,
            self.fixed_value_size,
            first_split_key_bytes,
        );
        for (key, value) in self.pairs.iter().take(division) {
            builder.append(key, value);
        }
        drop(builder);

        let required_size = Self::required_bytes(
            self.pairs.len() - division,
            self.total_key_bytes + self.total_value_bytes
                - first_split_key_bytes
                - first_split_value_bytes,
        );
        let mut page2 = self.mem.allocate(required_size)?;
        let mut builder = RawLeafBuilder::new(
            page2.memory_mut(),
            self.pairs.len() - division,
            self.fixed_key_size,
            self.fixed_value_size,
            self.total_key_bytes - first_split_key_bytes,
        );
        for (key, value) in self.pairs[division..].iter() {
            builder.append(key, value);
        }
        drop(builder);

        Ok((page1, self.pairs[division - 1].0, page2))
    }

    pub(super) fn build(self) -> Result<PageMut<'b>> {
        let required_size = Self::required_bytes(
            self.pairs.len(),
            self.total_key_bytes + self.total_value_bytes,
        );
        let mut page = self.mem.allocate(required_size)?;
        let mut builder = RawLeafBuilder::new(
            page.memory_mut(),
            self.pairs.len(),
            self.fixed_key_size,
            self.fixed_value_size,
            self.total_key_bytes,
        );
        for (key, value) in self.pairs {
            builder.append(key, value);
        }
        drop(builder);
        Ok(page)
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
// Layout is:
// 1 byte: type
// 1 byte: reserved (padding to 32bits aligned)
// 2 bytes: num_entries (number of pairs)
// (optional) repeating (num_entries times):
// 4 bytes: key_end
// (optional) repeating (num_entries times):
// 4 bytes: value_end
// repeating (num_entries times):
// * n bytes: key data
// repeating (num_entries times):
// * n bytes: value data
pub(crate) struct RawLeafBuilder<'a> {
    page: &'a mut [u8],
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    num_pairs: usize,
    provisioned_key_bytes: usize,
    pairs_written: usize, // used for debugging
}

impl<'a> RawLeafBuilder<'a> {
    pub(crate) fn required_bytes(num_pairs: usize, keys_values_bytes: usize) -> usize {
        // Page id & header;
        let mut result = 4;
        // key & value lengths
        result += num_pairs * 2 * size_of::<u32>();
        result += keys_values_bytes;

        result
    }

    pub(crate) fn new(
        page: &'a mut [u8],
        num_pairs: usize,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        key_bytes: usize,
    ) -> Self {
        page[0] = LEAF;
        page[2..4].copy_from_slice(&(num_pairs as u16).to_le_bytes());
        #[cfg(debug_assertions)]
        {
            // Poison all the key & value offsets, in case the caller forgets to write them
            let last = 4 + 2 * size_of::<u32>() * num_pairs;
            for x in &mut page[4..last] {
                *x = 0xFF;
            }
        }
        RawLeafBuilder {
            page,
            fixed_key_size,
            fixed_value_size,
            num_pairs,
            provisioned_key_bytes: key_bytes,
            pairs_written: 0,
        }
    }

    fn value_end(&self, n: usize) -> usize {
        if let Some(fixed) = self.fixed_value_size {
            return self.key_section_start() + self.provisioned_key_bytes + fixed * (n + 1);
        }
        let mut offset = 4 + size_of::<u32>() * n;
        if self.fixed_key_size.is_none() {
            offset += size_of::<u32>() * self.num_pairs;
        }
        u32::from_le_bytes(
            self.page[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn key_section_start(&self) -> usize {
        let mut offset = 4;
        if self.fixed_key_size.is_none() {
            offset += size_of::<u32>() * self.num_pairs;
        }
        if self.fixed_value_size.is_none() {
            offset += size_of::<u32>() * self.num_pairs;
        }

        offset
    }

    fn key_end(&self, n: usize) -> usize {
        if let Some(fixed) = self.fixed_key_size {
            return self.key_section_start() + fixed * (n + 1);
        }
        let offset = 4 + size_of::<u32>() * n;
        u32::from_le_bytes(
            self.page[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(crate) fn append(&mut self, key: &[u8], value: &[u8]) {
        let key_offset = if self.pairs_written == 0 {
            self.key_section_start()
        } else {
            self.key_end(self.pairs_written - 1)
        };
        let value_offset = if self.pairs_written == 0 {
            self.key_section_start() + self.provisioned_key_bytes
        } else {
            self.value_end(self.pairs_written - 1)
        };

        let n = self.pairs_written;
        if self.fixed_key_size.is_none() {
            let offset = 4 + size_of::<u32>() * n;
            self.page[offset..(offset + size_of::<u32>())]
                .copy_from_slice(&((key_offset + key.len()) as u32).to_le_bytes());
        }
        self.page[key_offset..(key_offset + key.len())].copy_from_slice(key);
        let written_key_len = key_offset + key.len() - self.key_section_start();
        assert!(written_key_len <= self.provisioned_key_bytes);

        if self.fixed_value_size.is_none() {
            let mut offset = 4 + size_of::<u32>() * n;
            if self.fixed_key_size.is_none() {
                offset += size_of::<u32>() * self.num_pairs;
            }
            self.page[offset..(offset + size_of::<u32>())]
                .copy_from_slice(&((value_offset + value.len()) as u32).to_le_bytes());
        }
        self.page[value_offset..(value_offset + value.len())].copy_from_slice(value);
        self.pairs_written += 1;
    }
}

impl<'a> Drop for RawLeafBuilder<'a> {
    fn drop(&mut self) {
        if !thread::panicking() {
            assert_eq!(self.pairs_written, self.num_pairs);
            assert_eq!(
                self.key_section_start() + self.provisioned_key_bytes,
                self.key_end(self.num_pairs - 1)
            );
        }
    }
}

pub(super) struct LeafMutator<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
}

impl<'a: 'b, 'b> LeafMutator<'a, 'b> {
    pub(super) fn new(
        page: &'b mut PageMut<'a>,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
    ) -> Self {
        assert_eq!(page.memory_mut()[0], LEAF);
        Self {
            page,
            fixed_key_size,
            fixed_value_size,
        }
    }

    pub(super) fn sufficient_insert_inplace_space(
        page: &'_ PageImpl<'_>,
        position: usize,
        overwrite: bool,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        new_key: &[u8],
        new_value: &[u8],
    ) -> bool {
        let accessor = LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
        if overwrite {
            let remaining = page.memory().len() - accessor.total_length();
            let required_delta = (new_key.len() + new_value.len()) as isize
                - accessor.length_of_pairs(position, position + 1) as isize;
            required_delta <= remaining as isize
        } else {
            let remaining = page.memory().len() - accessor.total_length();
            let mut required_delta = new_key.len() + new_value.len();
            if fixed_key_size.is_none() {
                required_delta += size_of::<u32>();
            }
            if fixed_value_size.is_none() {
                required_delta += size_of::<u32>();
            }
            required_delta <= remaining
        }
    }

    // Insert the given key, value pair at index i and shift all following pairs to the right
    pub(super) fn insert(&mut self, i: usize, overwrite: bool, key: &[u8], value: &[u8]) {
        let accessor = LeafAccessor::new(
            self.page.memory(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        let required_delta = if overwrite {
            (key.len() + value.len()) as isize - accessor.length_of_pairs(i, i + 1) as isize
        } else {
            let mut delta = key.len() + value.len();
            if self.fixed_key_size.is_none() {
                delta += size_of::<u32>();
            }
            if self.fixed_value_size.is_none() {
                delta += size_of::<u32>();
            }
            delta as isize
        };
        assert!(
            accessor.total_length() as isize + required_delta <= self.page.memory().len() as isize
        );

        let num_pairs = accessor.num_pairs();
        let last_key_end = accessor.key_end(accessor.num_pairs() - 1).unwrap();
        let last_value_end = accessor.value_end(accessor.num_pairs() - 1).unwrap();
        let shift_index = if overwrite { i + 1 } else { i };
        let shift_key_start = accessor.key_start(shift_index).unwrap_or(last_key_end);
        let shift_value_start = accessor.value_start(shift_index).unwrap_or(last_value_end);
        let existing_value_len = accessor
            .value_range(i)
            .map(|(start, end)| end - start)
            .unwrap_or_default();
        drop(accessor);

        let value_delta = if overwrite {
            value.len() as isize - existing_value_len as isize
        } else {
            value.len() as isize
        };

        // Update all the pointers
        let key_ptr_size = if self.fixed_key_size.is_none() { 4 } else { 0 } as usize;
        let value_ptr_size = if self.fixed_value_size.is_none() {
            4
        } else {
            0
        } as usize;
        if !overwrite {
            for j in 0..i {
                self.update_key_end(j, (key_ptr_size + value_ptr_size) as isize);
                let value_delta = (key_ptr_size + value_ptr_size + key.len()) as isize;
                self.update_value_end(j, value_delta);
            }
        }
        for j in i..num_pairs {
            if overwrite {
                self.update_value_end(j, value_delta);
            } else {
                let key_delta = (key_ptr_size + value_ptr_size + key.len()) as isize;
                self.update_key_end(j, key_delta);
                let value_delta = key_delta + value.len() as isize;
                self.update_value_end(j, value_delta);
            }
        }

        let new_num_pairs = if overwrite { num_pairs } else { num_pairs + 1 };
        self.page.memory_mut()[2..4].copy_from_slice(&(new_num_pairs as u16).to_le_bytes());

        // Right shift the trailing values
        let mut dest = if overwrite {
            (shift_value_start as isize + value_delta) as usize
        } else {
            shift_value_start + key_ptr_size + value_ptr_size + key.len() + value.len()
        };
        let start = shift_value_start;
        let end = last_value_end;
        self.page.memory_mut().copy_within(start..end, dest);

        // Insert the value
        let inserted_value_end = dest as u32;
        dest -= value.len();
        self.page.memory_mut()[dest..(dest + value.len())].copy_from_slice(value);

        if !overwrite {
            // Right shift the trailing key data & preceding value data
            let start = shift_key_start;
            let end = shift_value_start;
            dest -= end - start;
            self.page.memory_mut().copy_within(start..end, dest);

            // Insert the key
            let inserted_key_end = dest as u32;
            dest -= key.len();
            self.page.memory_mut()[dest..(dest + key.len())].copy_from_slice(key);

            // Right shift the trailing value pointers & preceding key data
            let start = 4 + key_ptr_size * num_pairs + value_ptr_size * i;
            let end = shift_key_start;
            dest -= end - start;
            debug_assert_eq!(
                dest,
                4 + key_ptr_size * new_num_pairs + value_ptr_size * (i + 1)
            );
            self.page.memory_mut().copy_within(start..end, dest);

            // Insert the value pointer
            if self.fixed_value_size.is_none() {
                dest -= size_of::<u32>();
                self.page.memory_mut()[dest..(dest + size_of::<u32>())]
                    .copy_from_slice(&inserted_value_end.to_le_bytes());
            }

            // Right shift the trailing key pointers & preceding value pointers
            let start = 4 + key_ptr_size * i;
            let end = 4 + key_ptr_size * num_pairs + value_ptr_size * i;
            dest -= end - start;
            debug_assert_eq!(dest, 4 + key_ptr_size * (i + 1));
            self.page.memory_mut().copy_within(start..end, dest);

            // Insert the key pointer
            if self.fixed_key_size.is_none() {
                dest -= size_of::<u32>();
                self.page.memory_mut()[dest..(dest + size_of::<u32>())]
                    .copy_from_slice(&inserted_key_end.to_le_bytes());
            }
            debug_assert_eq!(dest, 4 + key_ptr_size * i);
        }
    }

    pub(super) fn remove(&mut self, i: usize) {
        let accessor = LeafAccessor::new(
            self.page.memory(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        let num_pairs = accessor.num_pairs();
        assert!(i < num_pairs);
        assert!(num_pairs > 1);
        let key_start = accessor.key_start(i).unwrap();
        let key_end = accessor.key_end(i).unwrap();
        let value_start = accessor.value_start(i).unwrap();
        let value_end = accessor.value_end(i).unwrap();
        let last_value_end = accessor.value_end(accessor.num_pairs() - 1).unwrap();
        drop(accessor);

        // Update all the pointers
        let key_ptr_size = if self.fixed_key_size.is_none() {
            size_of::<u32>()
        } else {
            0
        };
        let value_ptr_size = if self.fixed_value_size.is_none() {
            size_of::<u32>()
        } else {
            0
        };
        for j in 0..i {
            self.update_key_end(j, -((key_ptr_size + value_ptr_size) as isize));
            let value_delta =
                -((key_ptr_size + value_ptr_size) as isize) - (key_end - key_start) as isize;
            self.update_value_end(j, value_delta);
        }
        for j in (i + 1)..num_pairs {
            let key_delta =
                -((key_ptr_size + value_ptr_size) as isize) - (key_end - key_start) as isize;
            self.update_key_end(j, key_delta);
            let value_delta = key_delta - (value_end - value_start) as isize;
            self.update_value_end(j, value_delta);
        }

        // Left shift all the pointers & data

        let new_num_pairs = num_pairs - 1;
        self.page.memory_mut()[2..4].copy_from_slice(&(new_num_pairs as u16).to_le_bytes());
        // Left shift the trailing key pointers & preceding value pointers
        let mut dest = 4 + key_ptr_size * i;
        // First trailing key pointer
        let start = 4 + key_ptr_size * (i + 1);
        // Last preceding value pointer
        let end = 4 + key_ptr_size * num_pairs + value_ptr_size * i;
        self.page
            .memory_mut()
            .copy_within(start..end, dest as usize);
        dest += end - start;
        debug_assert_eq!(dest, 4 + key_ptr_size * new_num_pairs + value_ptr_size * i);

        // Left shift the trailing value pointers & preceding key data
        let start = 4 + key_ptr_size * num_pairs + value_ptr_size * (i + 1);
        let end = key_start;
        self.page
            .memory_mut()
            .copy_within(start..end, dest as usize);
        dest += end - start;

        let preceding_key_len = key_start - (4 + (key_ptr_size + value_ptr_size) * num_pairs);
        debug_assert_eq!(
            dest,
            4 + (key_ptr_size + value_ptr_size) * new_num_pairs + preceding_key_len
        );

        // Left shift the trailing key data & preceding value data
        let start = key_end;
        let end = value_start;
        self.page
            .memory_mut()
            .copy_within(start..end, dest as usize);
        dest += end - start;

        // Left shift the trailing value data
        let preceding_data_len =
            value_start - (4 + (key_ptr_size + value_ptr_size) * num_pairs) - (key_end - key_start);
        debug_assert_eq!(
            dest,
            4 + (key_ptr_size + value_ptr_size) * new_num_pairs + preceding_data_len
        );
        let start = value_end;
        let end = last_value_end;
        self.page
            .memory_mut()
            .copy_within(start..end, dest as usize);
    }

    fn update_key_end(&mut self, i: usize, delta: isize) {
        if self.fixed_key_size.is_some() {
            return;
        }
        let offset = 4 + size_of::<u32>() * i;
        let mut ptr = u32::from_le_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        ptr = (ptr as isize + delta) as u32;
        self.page.memory_mut()[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&ptr.to_le_bytes());
    }

    fn update_value_end(&mut self, i: usize, delta: isize) {
        if self.fixed_value_size.is_some() {
            return;
        }
        let accessor = LeafAccessor::new(
            self.page.memory(),
            self.fixed_key_size,
            self.fixed_value_size,
        );
        let num_pairs = accessor.num_pairs();
        drop(accessor);
        let mut offset = 4 + size_of::<u32>() * i;
        if self.fixed_key_size.is_none() {
            offset += size_of::<u32>() * num_pairs;
        }
        let mut ptr = u32::from_le_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        ptr = (ptr as isize + delta) as u32;
        self.page.memory_mut()[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&ptr.to_le_bytes());
    }
}

// Provides a simple zero-copy way to access a branch page
pub(super) struct BranchAccessor<'a: 'b, 'b, T: Page + 'a> {
    page: &'b T,
    num_keys: usize,
    fixed_key_size: Option<usize>,
    _page_lifetime: PhantomData<&'a ()>,
}

impl<'a: 'b, 'b, T: Page + 'a> BranchAccessor<'a, 'b, T> {
    pub(super) fn new(page: &'b T, fixed_key_size: Option<usize>) -> Self {
        debug_assert_eq!(page.memory()[0], BRANCH);
        let num_keys = u16::from_le_bytes(page.memory()[2..4].try_into().unwrap()) as usize;
        BranchAccessor {
            page,
            num_keys,
            fixed_key_size,
            _page_lifetime: Default::default(),
        }
    }

    pub(super) fn print_node<K: RedbKey + ?Sized>(&self) {
        eprint!(
            "Internal[ (page={:?}), child_0={:?}",
            self.page.get_page_number(),
            self.child_page(0).unwrap()
        );
        for i in 0..(self.count_children() - 1) {
            if let Some(child) = self.child_page(i + 1) {
                let key = self.key(i).unwrap();
                eprint!(" key_{}={:?}", i, K::from_bytes(key));
                eprint!(" child_{}={:?}", i + 1, child);
            }
        }
        eprint!("]");
    }

    pub(crate) fn total_length(&self) -> usize {
        // Keys are stored at the end
        self.key_end(self.num_keys() - 1)
    }

    pub(super) fn child_for_key<K: RedbKey + ?Sized>(&self, query: &[u8]) -> (usize, PageNumber) {
        let mut min_child = 0; // inclusive
        let mut max_child = self.num_keys(); // inclusive
        while min_child < max_child {
            let mid = (min_child + max_child) / 2;
            match K::compare(query, self.key(mid).unwrap()) {
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
        }
        debug_assert_eq!(min_child, max_child);

        (min_child, self.child_page(min_child).unwrap())
    }

    fn key_section_start(&self) -> usize {
        if self.fixed_key_size.is_none() {
            8 + (PageNumber::serialized_size() + size_of::<Checksum>()) * self.count_children()
                + size_of::<u32>() * self.num_keys()
        } else {
            8 + (PageNumber::serialized_size() + size_of::<Checksum>()) * self.count_children()
        }
    }

    fn key_offset(&self, n: usize) -> usize {
        if n == 0 {
            self.key_section_start()
        } else {
            self.key_end(n - 1)
        }
    }

    fn key_end(&self, n: usize) -> usize {
        if let Some(fixed) = self.fixed_key_size {
            return self.key_section_start() + fixed * (n + 1);
        }
        let offset = 8
            + (PageNumber::serialized_size() + size_of::<Checksum>()) * self.count_children()
            + size_of::<u32>() * n;
        u32::from_le_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub(super) fn key(&self, n: usize) -> Option<&[u8]> {
        if n >= self.num_keys() {
            return None;
        }
        let offset = self.key_offset(n);
        let end = self.key_end(n);
        Some(&self.page.memory()[offset..end])
    }

    pub(super) fn count_children(&self) -> usize {
        self.num_keys() + 1
    }

    pub(super) fn child_checksum(&self, n: usize) -> Option<Checksum> {
        if n >= self.count_children() {
            return None;
        }

        let offset = 8 + size_of::<Checksum>() * n;
        Some(Checksum::from_le_bytes(
            self.page.memory()[offset..(offset + size_of::<Checksum>())]
                .try_into()
                .unwrap(),
        ))
    }

    pub(super) fn child_page(&self, n: usize) -> Option<PageNumber> {
        if n >= self.count_children() {
            return None;
        }

        let offset =
            8 + size_of::<Checksum>() * self.count_children() + PageNumber::serialized_size() * n;
        Some(PageNumber::from_le_bytes(
            self.page.memory()[offset..(offset + PageNumber::serialized_size())]
                .try_into()
                .unwrap(),
        ))
    }

    fn num_keys(&self) -> usize {
        self.num_keys
    }
}

pub(super) struct BranchBuilder<'a, 'b> {
    children: Vec<(PageNumber, Checksum)>,
    keys: Vec<&'a [u8]>,
    total_key_bytes: usize,
    fixed_key_size: Option<usize>,
    mem: &'b TransactionalMemory,
}

impl<'a, 'b> BranchBuilder<'a, 'b> {
    pub(super) fn new(
        mem: &'b TransactionalMemory,
        child_capacity: usize,
        fixed_key_size: Option<usize>,
    ) -> Self {
        Self {
            children: Vec::with_capacity(child_capacity),
            keys: Vec::with_capacity(child_capacity - 1),
            total_key_bytes: 0,
            fixed_key_size,
            mem,
        }
    }

    pub(super) fn replace_child(&mut self, index: usize, child: PageNumber, checksum: Checksum) {
        self.children[index] = (child, checksum);
    }

    pub(super) fn push_child(&mut self, child: PageNumber, checksum: Checksum) {
        self.children.push((child, checksum));
    }

    pub(super) fn push_key(&mut self, key: &'a [u8]) {
        self.keys.push(key);
        self.total_key_bytes += key.len();
    }

    pub(super) fn push_all<T: Page>(&mut self, accessor: &'a BranchAccessor<'_, '_, T>) {
        for i in 0..accessor.count_children() {
            let child = accessor.child_page(i).unwrap();
            let checksum = accessor.child_checksum(i).unwrap();
            self.push_child(child, checksum);
        }
        for i in 0..(accessor.count_children() - 1) {
            self.push_key(accessor.key(i).unwrap());
        }
    }

    pub(super) fn to_single_child(&self) -> Option<(PageNumber, Checksum)> {
        if self.children.len() > 1 {
            None
        } else {
            Some(self.children[0])
        }
    }

    pub(super) fn build(self) -> Result<PageMut<'b>> {
        assert_eq!(self.children.len(), self.keys.len() + 1);
        let size = RawBranchBuilder::required_bytes(
            self.keys.len(),
            self.total_key_bytes,
            self.fixed_key_size,
        );
        let mut page = self.mem.allocate(size)?;
        let mut builder = RawBranchBuilder::new(&mut page, self.keys.len(), self.fixed_key_size);
        builder.write_first_page(self.children[0].0, self.children[0].1);
        for i in 1..self.children.len() {
            let key = &self.keys[i - 1];
            builder.write_nth_key(key.as_ref(), self.children[i].0, self.children[i].1, i - 1);
        }
        drop(builder);

        Ok(page)
    }

    pub(super) fn should_split(&self) -> bool {
        let size = RawBranchBuilder::required_bytes(
            self.keys.len(),
            self.total_key_bytes,
            self.fixed_key_size,
        );
        size > self.mem.get_page_size() && self.keys.len() >= 3
    }

    pub(super) fn build_split(self) -> Result<(PageMut<'b>, &'a [u8], PageMut<'b>)> {
        assert_eq!(self.children.len(), self.keys.len() + 1);
        assert!(self.keys.len() >= 3);
        let division = self.keys.len() / 2;
        let first_split_key_len: usize = self.keys.iter().take(division).map(|k| k.len()).sum();
        let division_key = self.keys[division];
        let second_split_key_len = self.total_key_bytes - first_split_key_len - division_key.len();

        let size =
            RawBranchBuilder::required_bytes(division, first_split_key_len, self.fixed_key_size);
        let mut page1 = self.mem.allocate(size)?;
        let mut builder = RawBranchBuilder::new(&mut page1, division, self.fixed_key_size);
        builder.write_first_page(self.children[0].0, self.children[0].1);
        for i in 0..division {
            let key = &self.keys[i];
            builder.write_nth_key(
                key.as_ref(),
                self.children[i + 1].0,
                self.children[i + 1].1,
                i,
            );
        }
        drop(builder);

        let size = RawBranchBuilder::required_bytes(
            self.keys.len() - division - 1,
            second_split_key_len,
            self.fixed_key_size,
        );
        let mut page2 = self.mem.allocate(size)?;
        let mut builder = RawBranchBuilder::new(
            &mut page2,
            self.keys.len() - division - 1,
            self.fixed_key_size,
        );
        builder.write_first_page(self.children[division + 1].0, self.children[division + 1].1);
        for i in (division + 1)..self.keys.len() {
            let key = &self.keys[i];
            builder.write_nth_key(
                key.as_ref(),
                self.children[i + 1].0,
                self.children[i + 1].1,
                i - division - 1,
            );
        }
        drop(builder);

        Ok((page1, division_key, page2))
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
// Layout is:
// 1 byte: type
// 1 byte: padding (padding to 16bits aligned)
// 2 bytes: num_keys (number of keys)
// 4 byte: padding (padding to 64bits aligned)
// repeating (num_keys + 1 times):
// 16 bytes: child page checksum
// repeating (num_keys + 1 times):
// 8 bytes: page number
// (optional) repeating (num_keys times):
// * 4 bytes: key end. Ending offset of the key, exclusive
// repeating (num_keys times):
// * n bytes: key data
pub(super) struct RawBranchBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
    fixed_key_size: Option<usize>,
    num_keys: usize,
    keys_written: usize, // used for debugging
}

impl<'a: 'b, 'b> RawBranchBuilder<'a, 'b> {
    pub(super) fn required_bytes(
        num_keys: usize,
        size_of_keys: usize,
        fixed_key_size: Option<usize>,
    ) -> usize {
        if fixed_key_size.is_none() {
            let fixed_size = 8
                + (PageNumber::serialized_size() + size_of::<Checksum>()) * (num_keys + 1)
                + size_of::<u32>() * num_keys;
            size_of_keys + fixed_size
        } else {
            let fixed_size =
                8 + (PageNumber::serialized_size() + size_of::<Checksum>()) * (num_keys + 1);
            size_of_keys + fixed_size
        }
    }

    // Caller MUST write num_keys values
    pub(super) fn new(
        page: &'b mut PageMut<'a>,
        num_keys: usize,
        fixed_key_size: Option<usize>,
    ) -> Self {
        assert!(num_keys > 0);
        page.memory_mut()[0] = BRANCH;
        page.memory_mut()[2..4].copy_from_slice(&(num_keys as u16).to_le_bytes());
        #[cfg(debug_assertions)]
        {
            // Poison all the child pointers & key offsets, in case the caller forgets to write them
            let start = 8 + size_of::<Checksum>() * (num_keys + 1);
            let last = 8
                + (PageNumber::serialized_size() + size_of::<Checksum>()) * (num_keys + 1)
                + size_of::<u32>() * num_keys;
            for x in &mut page.memory_mut()[start..last] {
                *x = 0xFF;
            }
        }
        RawBranchBuilder {
            page,
            fixed_key_size,
            num_keys,
            keys_written: 0,
        }
    }

    pub(super) fn write_first_page(&mut self, page_number: PageNumber, checksum: Checksum) {
        let offset = 8;
        self.page.memory_mut()[offset..(offset + size_of::<Checksum>())]
            .copy_from_slice(&checksum.to_le_bytes());
        let offset = 8 + size_of::<Checksum>() * (self.num_keys + 1);
        self.page.memory_mut()[offset..(offset + PageNumber::serialized_size())]
            .copy_from_slice(&page_number.to_le_bytes());
    }

    fn key_section_start(&self) -> usize {
        let mut offset =
            8 + (PageNumber::serialized_size() + size_of::<Checksum>()) * (self.num_keys + 1);
        if self.fixed_key_size.is_none() {
            offset += size_of::<u32>() * self.num_keys;
        }

        offset
    }

    fn key_end(&self, n: usize) -> usize {
        if let Some(fixed) = self.fixed_key_size {
            return self.key_section_start() + fixed * (n + 1);
        }
        let offset = 8
            + (PageNumber::serialized_size() + size_of::<Checksum>()) * (self.num_keys + 1)
            + size_of::<u32>() * n;
        u32::from_le_bytes(
            self.page.memory()[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    // Write the nth key and page of values greater than this key, but less than or equal to the next
    // Caller must write keys & pages in increasing order
    pub(super) fn write_nth_key(
        &mut self,
        key: &[u8],
        page_number: PageNumber,
        checksum: Checksum,
        n: usize,
    ) {
        assert!(n < self.num_keys as usize);
        assert_eq!(n, self.keys_written);
        self.keys_written += 1;
        let offset = 8 + size_of::<Checksum>() * (n + 1);
        self.page.memory_mut()[offset..(offset + size_of::<Checksum>())]
            .copy_from_slice(&checksum.to_le_bytes());
        let offset = 8
            + size_of::<Checksum>() * (self.num_keys + 1)
            + PageNumber::serialized_size() * (n + 1);
        self.page.memory_mut()[offset..(offset + PageNumber::serialized_size())]
            .copy_from_slice(&page_number.to_le_bytes());

        let data_offset = if n > 0 {
            self.key_end(n - 1)
        } else {
            self.key_section_start()
        };
        if self.fixed_key_size.is_none() {
            let offset = 8
                + (PageNumber::serialized_size() + size_of::<Checksum>()) * (self.num_keys + 1)
                + size_of::<u32>() * n;
            self.page.memory_mut()[offset..(offset + size_of::<u32>())]
                .copy_from_slice(&((data_offset + key.len()) as u32).to_le_bytes());
        }

        debug_assert!(data_offset > offset);
        self.page.memory_mut()[data_offset..(data_offset + key.len())].copy_from_slice(key);
    }
}

impl<'a: 'b, 'b> Drop for RawBranchBuilder<'a, 'b> {
    fn drop(&mut self) {
        if !thread::panicking() {
            assert_eq!(self.keys_written, self.num_keys);
        }
    }
}

pub(super) struct BranchMutator<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> BranchMutator<'a, 'b> {
    pub(super) fn new(page: &'b mut PageMut<'a>) -> Self {
        assert_eq!(page.memory()[0], BRANCH);
        Self { page }
    }

    fn num_keys(&self) -> usize {
        u16::from_le_bytes(self.page.memory()[2..4].try_into().unwrap()) as usize
    }

    pub(super) fn write_child_page(
        &mut self,
        i: usize,
        page_number: PageNumber,
        checksum: Checksum,
    ) {
        debug_assert!(i <= self.num_keys());
        let offset = 8 + size_of::<Checksum>() * i;
        self.page.memory_mut()[offset..(offset + size_of::<Checksum>())]
            .copy_from_slice(&checksum.to_le_bytes());
        let offset =
            8 + size_of::<Checksum>() * (self.num_keys() + 1) + PageNumber::serialized_size() * i;
        self.page.memory_mut()[offset..(offset + PageNumber::serialized_size())]
            .copy_from_slice(&page_number.to_le_bytes());
    }
}
