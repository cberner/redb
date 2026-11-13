use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::fmt::{Debug, Formatter};
use core::ops::Deref;

// Pages are allocated from the system allocator rather than the global one, because the cache
// budget is counted in page bytes. Size-class allocators, such as jemalloc and mimalloc, round a
// page plus an `Arc` header (4096 + 16 bytes) up to a 5120 byte class, so a cache of such pages
// would hold at least 25% more memory than its budget. The reference counts are kept in a separate
// allocation, so that the page's own is exactly the page size.
#[cfg(not(redb_no_std))]
use std::alloc::System as BufferAllocator;
// Without std, the global allocator is the only one available
#[cfg(redb_no_std)]
use alloc::alloc::Global as BufferAllocator;

/// The shared, immutable memory of a page: what the page cache stores and hands out to readers.
#[derive(Clone)]
pub(crate) struct PageBuffer(Arc<Box<[u8], BufferAllocator>>);

impl PageBuffer {
    pub(crate) fn zeroed(len: usize) -> Self {
        let mut buffer = Vec::with_capacity_in(len, BufferAllocator);
        buffer.resize(len, 0);
        Self::from_vec(buffer)
    }

    pub(crate) fn copy_from_slice(data: &[u8]) -> Self {
        let mut buffer = Vec::with_capacity_in(data.len(), BufferAllocator);
        buffer.extend_from_slice(data);
        Self::from_vec(buffer)
    }

    // The Vec must be full, so that converting it does not reallocate
    fn from_vec(buffer: Vec<u8, BufferAllocator>) -> Self {
        debug_assert_eq!(buffer.len(), buffer.capacity());
        Self(Arc::new(buffer.into_boxed_slice()))
    }

    /// The page's memory, if no other reference to it exists
    pub(crate) fn get_mut(&mut self) -> Option<&mut [u8]> {
        Arc::get_mut(&mut self.0).map(|buffer| &mut **buffer)
    }
}

impl Deref for PageBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for PageBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PageBuffer")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::PageBuffer;

    #[test]
    fn zeroed() {
        let buffer = PageBuffer::zeroed(4096);
        assert_eq!(buffer.len(), 4096);
        assert!(buffer.iter().all(|&b| b == 0));
    }

    #[test]
    fn copy_from_slice() {
        let buffer = PageBuffer::copy_from_slice(&[1, 2, 3]);
        assert_eq!(&*buffer, &[1, 2, 3]);
    }

    #[test]
    fn get_mut_requires_unique_reference() {
        let mut buffer = PageBuffer::zeroed(16);
        buffer.get_mut().unwrap().fill(0xAB);
        let shared = buffer.clone();
        assert!(buffer.get_mut().is_none());
        assert!(shared.iter().all(|&b| b == 0xAB));
        drop(shared);
        assert!(buffer.get_mut().is_some());
    }
}
