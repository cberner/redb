use crate::Error;
use std::cell::UnsafeCell;
use std::ops::Range;
use std::slice;

pub(crate) struct Mmap {
    inner: UnsafeCell<memmap2::MmapMut>,
    data_ptr: *mut u8,
    len: usize,
}

impl Mmap {
    pub(crate) fn new(mut mmap: memmap2::MmapMut) -> Self {
        let len = mmap.len();
        let data_ptr = mmap.as_mut_ptr();
        Self {
            inner: UnsafeCell::new(mmap),
            data_ptr,
            len,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn flush(&self) -> Result<(), Error> {
        // Safety: .flush() does not construct any references to the memory of the mmap
        // and the only mutable references we construct are to that memory
        unsafe {
            (*(self.inner.get() as *const memmap2::MmapMut)).flush()?;
        }

        Ok(())
    }

    pub(crate) fn get_memory(&self, range: Range<usize>) -> &[u8] {
        unsafe {
            // Avoid calling self.inner.deref() because that would create a reference to all of the
            // mmap'ed memory
            let ptr = (self.data_ptr as *const u8).add(range.start);
            slice::from_raw_parts(ptr, range.len())
        }
    }

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .get_memory() or .get_memory_mut()
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_memory_mut(&self, range: Range<usize>) -> &mut [u8] {
        // Avoid calling self.inner.deref_mut() because that would create a reference to all of the
        // mmap'ed memory, and require a mutable reference to .inner
        let ptr = self.data_ptr.add(range.start);
        slice::from_raw_parts_mut(ptr, range.len())
    }
}
