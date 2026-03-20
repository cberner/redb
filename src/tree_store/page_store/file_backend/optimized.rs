use crate::error::BackendError;
use crate::{DatabaseError, Result, StorageBackend};
use std::fs::{File, TryLockError};
use std::io;

#[cfg(feature = "logging")]
use log::warn;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    lock_supported: bool,
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, false)
    }

    pub(crate) fn new_internal(file: File, read_only: bool) -> Result<Self, DatabaseError> {
        let result = if read_only {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };

        match result {
            Ok(()) => Ok(Self {
                file,
                lock_supported: true,
            }),
            Err(TryLockError::WouldBlock) => Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) if err.kind() == io::ErrorKind::Unsupported => {
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file, at a time"
                );

                Ok(Self {
                    file,
                    lock_supported: false,
                })
            }
            Err(TryLockError::Error(err)) => Err(err.into()),
        }
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, BackendError> {
        Ok(self.file.metadata().map_err(BackendError::Io)?.len())
    }

    #[cfg(unix)]
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        self.file
            .read_exact_at(out, offset)
            .map_err(BackendError::Io)?;
        Ok(())
    }

    #[cfg(target_os = "wasi")]
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        read_exact_at(&self.file, out, offset).map_err(BackendError::Io)?;
        Ok(())
    }

    #[cfg(windows)]
    fn read(&self, mut offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        let mut data_offset = 0;
        while data_offset < out.len() {
            let read = self
                .file
                .seek_read(&mut out[data_offset..], offset)
                .map_err(BackendError::Io)?;
            offset += read as u64;
            data_offset += read;
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), BackendError> {
        self.file.set_len(len).map_err(BackendError::Io)
    }

    fn sync_data(&self) -> Result<(), BackendError> {
        self.file.sync_data().map_err(BackendError::Io)
    }

    #[cfg(unix)]
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendError> {
        self.file
            .write_all_at(data, offset)
            .map_err(BackendError::Io)
    }

    #[cfg(target_os = "wasi")]
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendError> {
        write_all_at(&self.file, data, offset).map_err(BackendError::Io)
    }

    #[cfg(windows)]
    fn write(&self, mut offset: u64, data: &[u8]) -> Result<(), BackendError> {
        let mut data_offset = 0;
        while data_offset < data.len() {
            let written = self
                .file
                .seek_write(&data[data_offset..], offset)
                .map_err(BackendError::Io)?;
            offset += written as u64;
            data_offset += written;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), BackendError> {
        if self.lock_supported {
            self.file.unlock().map_err(BackendError::Io)?;
        }

        Ok(())
    }
}

// TODO: replace these with wasi::FileExt when https://github.com/rust-lang/rust/issues/71213
// is stable
#[cfg(target_os = "wasi")]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    while !buf.is_empty() {
        let nbytes = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buf.as_mut_ptr() as _,
                core::cmp::min(buf.len(), libc::ssize_t::MAX as _),
                offset as _,
            )
        };
        match nbytes {
            0 => break,
            -1 => match io::Error::last_os_error() {
                err if err.kind() == io::ErrorKind::Interrupted => {}
                err => return Err(err),
            },
            n => {
                let tmp = buf;
                buf = &mut tmp[n as usize..];
                offset += n as u64;
            }
        }
    }
    if !buf.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        ))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "wasi")]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    while !buf.is_empty() {
        let nbytes = unsafe {
            libc::pwrite(
                file.as_raw_fd(),
                buf.as_ptr() as _,
                core::cmp::min(buf.len(), libc::ssize_t::MAX as _),
                offset as _,
            )
        };
        match nbytes {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            -1 => match io::Error::last_os_error() {
                err if err.kind() == io::ErrorKind::Interrupted => {}
                err => return Err(err),
            },
            n => {
                buf = &buf[n as usize..];
                offset += n as u64
            }
        }
    }
    Ok(())
}
