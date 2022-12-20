use crate::{Error, Result};
use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

pub(crate) struct FileLock {
    fd: libc::c_int,
}

impl FileLock {
    pub(crate) fn new(file: &File) -> Result<Self> {
        let fd = file.as_raw_fd();
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                Err(Error::DatabaseAlreadyOpen)
            } else {
                Err(Error::Io(err))
            }
        } else {
            Ok(Self { fd })
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        unsafe { libc::flock(self.fd, libc::LOCK_UN) };
    }
}
