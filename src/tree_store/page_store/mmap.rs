use crate::Result;
use errno::errno;
use memmap2::MmapRaw;
use std::fs::File;
use std::io;
use std::ops::Range;
use std::os::unix::io::AsRawFd;
use std::slice;

pub(crate) struct Mmap {
    file: File,
    inner: memmap2::MmapRaw,
}

impl Mmap {
    pub(crate) fn new(file: File) -> Result<Self> {
        Ok(Self {
            inner: MmapRaw::map_raw(&file)?,
            file,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn flush(&self) -> Result {
        let code = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_BARRIERFSYNC) };
        if code == -1 {
            return Err(io::Error::new(io::ErrorKind::Other, errno()).into());
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) fn flush(&self) -> Result {
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
