use crate::Error;
use std::ops::Range;
use std::slice;

pub(crate) struct Mmap {
    inner: memmap2::MmapRaw,
}

impl Mmap {
    pub(crate) fn new(mmap: memmap2::MmapRaw) -> Self {
        Self { inner: mmap }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    pub(crate) fn flush(&self) -> Result<(), Error> {
        self.inner.flush()?;

        Ok(())
    }

    pub(crate) fn get_memory(&self, range: Range<usize>) -> &[u8] {
        assert!(range.end <= self.inner.len());
        unsafe {
            let ptr = self.inner.as_ptr().add(range.start);
            slice::from_raw_parts(ptr, range.len())
        }
    }

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .get_memory() or .get_memory_mut()
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_memory_mut(&self, range: Range<usize>) -> &mut [u8] {
        assert!(range.end <= self.inner.len());
        let ptr = self.inner.as_mut_ptr().add(range.start);
        slice::from_raw_parts_mut(ptr, range.len())
    }
}
