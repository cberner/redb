use crate::Result;
use std::fs::File;
use std::io;
use std::ops::Range;
use std::os::unix::io::AsRawFd;
use std::{ptr, slice};

pub(crate) struct Mmap {
    #[allow(dead_code)]
    file: File,
    mmap: *mut u8,
    len: usize,
}

// mmap() is documented as being multi-thread safe
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    pub(crate) fn new(file: File) -> Result<Self> {
        let len = file.metadata()?.len();
        let mmap = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            ) as *mut u8
        };
        Ok(Self {
            mmap,
            file,
            len: len as usize,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) fn flush(&self) -> Result {
        let result = unsafe {
            libc::msync(
                self.mmap as *mut libc::c_void,
                self.len as libc::size_t,
                libc::MS_SYNC,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error().into())
        }
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn flush(&self) -> Result {
        let code = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_FULLFSYNC) };
        if code == -1 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    pub(crate) fn eventual_flush(&self) -> Result {
        self.flush()
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn eventual_flush(&self) -> Result {
        // TODO: It may be unsafe to mix F_BARRIERFSYNC with writes to the mmap.
        //       Investigate switching to `write()`
        let code = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_BARRIERFSYNC) };
        if code == -1 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .get_memory_mut()
    pub(crate) unsafe fn get_memory(&self, range: Range<usize>) -> &[u8] {
        assert!(range.end <= self.len);
        let ptr = self.mmap.add(range.start);
        slice::from_raw_parts(ptr, range.len())
    }

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .get_memory() or .get_memory_mut()
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_memory_mut(&self, range: Range<usize>) -> &mut [u8] {
        assert!(range.end <= self.len);
        let ptr = self.mmap.add(range.start);
        slice::from_raw_parts_mut(ptr, range.len())
    }
}
