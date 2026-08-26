use crate::{DatabaseError, Result, StorageBackend};
use std::fs::{File, TryLockError};
use std::io;

#[cfg(feature = "logging")]
use log::warn;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

// The multi-process locking protocol (see docs/design.md) coordinates processes through
// byte-range locks on the database file, at offsets far past any possible end of file. On
// Unix those are a separate namespace from the whole-file flock() that std's file locks
// take, so the two kinds of lock cannot see each other; a single-process or read-only
// handle must hold the entire byte range as well, so that it and a multi-process handle
// refuse each other. Windows needs nothing extra: std's whole-file lock is itself a range
// lock over every byte.
//
// Open file description locks, never POSIX record locks: a record lock belongs to the
// process, and is released when any descriptor for the file is closed anywhere in it --
// which a library embedded in an arbitrary program cannot rule out.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
mod range_lock {
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    // The lock-type constants are c_int on Linux and already c_short on the Apple platforms
    #[cfg(target_os = "linux")]
    fn lock_type(kind: libc::c_int) -> libc::c_short {
        kind.try_into().unwrap()
    }

    #[cfg(target_vendor = "apple")]
    fn lock_type(kind: libc::c_short) -> libc::c_short {
        kind
    }

    fn flock_struct(kind: libc::c_short, start: u64, len: u64) -> libc::flock {
        // Zeroed rather than written field by field: the layout of struct flock differs
        // between Linux and the Apple platforms, and libc carries the right one for each
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = kind;
        lock.l_whence = libc::SEEK_SET.try_into().unwrap();
        lock.l_start = libc::off_t::try_from(start).unwrap();
        lock.l_len = libc::off_t::try_from(len).unwrap();
        lock
    }

    /// `Ok(true)`: acquired. `Ok(false)`: a conflicting lock is held elsewhere.
    fn try_lock(file: &File, start: u64, len: u64, exclusive: bool) -> io::Result<bool> {
        let kind = lock_type(if exclusive {
            libc::F_WRLCK
        } else {
            libc::F_RDLCK
        });
        let mut lock = flock_struct(kind, start, len);
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &raw mut lock) };
        if rc == 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EAGAIN | libc::EACCES) => Ok(false),
            _ => Err(err),
        }
    }

    fn unlock(file: &File, start: u64, len: u64) -> io::Result<()> {
        let mut lock = flock_struct(lock_type(libc::F_UNLCK), start, len);
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &raw mut lock) };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Locks every offset the file can address. A length of zero means "to the largest
    /// possible offset", and is the only way to say it: an explicit length cannot reach the
    /// last addressable byte, because the offsets fcntl takes are signed.
    pub(super) fn try_lock_all(file: &File, exclusive: bool) -> io::Result<bool> {
        try_lock(file, 0, 0, exclusive)
    }

    pub(super) fn unlock_all(file: &File) -> io::Result<()> {
        unlock(file, 0, 0)
    }

    /// The filesystem does not support range locks (fcntl on a kernel or filesystem
    /// without them reports `EINVAL` or a not-supported error)
    pub(super) fn unsupported(err: &io::Error) -> bool {
        matches!(err.raw_os_error(), Some(code) if code == libc::EINVAL
            || code == libc::ENOTSUP
            || code == libc::EOPNOTSUPP)
    }

    #[cfg(test)]
    pub(super) fn try_lock_byte(file: &File, offset: u64, exclusive: bool) -> io::Result<bool> {
        try_lock(file, offset, 1, exclusive)
    }

    #[cfg(test)]
    pub(super) fn unlock_byte(file: &File, offset: u64) -> io::Result<()> {
        unlock(file, offset, 1)
    }
}

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    lock_supported: bool,
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    range_lock_supported: bool,
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, false)
    }

    pub(crate) fn new_internal(file: File, read_only: bool) -> Result<Self, DatabaseError> {
        // On the Apple platforms the whole-file flock and the fcntl range locks share one
        // lock table and conflict with each other, even taken through the same file
        // description, so the two cannot be held together the way they are on Linux. Only
        // the range lock is taken there: sharing the table is exactly what makes it
        // conflict with everything else -- the ranges of the multi-process protocol and
        // the whole-file locks of older redb versions alike. The whole-file lock remains
        // as the fallback for a filesystem that does not support range locks, where the
        // conflict cannot arise
        #[cfg(target_vendor = "apple")]
        let mut lock_supported = false;

        #[cfg(not(target_vendor = "apple"))]
        let lock_supported = Self::try_whole_file_lock(&file, read_only)?;

        // The byte ranges of the multi-process locking protocol, held exclusively by a
        // writable handle, the way it locks the file itself, and shared by a read-only
        // one. On Linux the flock above cannot conflict with them -- flock() and fcntl()
        // locks ignore each other there -- so they are held in addition. A conflict means
        // a multi-process handle has the database open
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        let range_lock_supported = match range_lock::try_lock_all(&file, !read_only) {
            Ok(true) => true,
            Ok(false) => return Err(DatabaseError::DatabaseAlreadyOpen),
            Err(err) if range_lock::unsupported(&err) => {
                #[cfg(feature = "logging")]
                warn!(
                    "Byte-range locks not supported by this filesystem. You must ensure that the database file is not opened for multi-process access"
                );

                // Nothing is held yet on an Apple platform, so a database on such a
                // filesystem must fall back to the whole-file lock, keeping two ordinary
                // handles excluding each other the way they always have
                #[cfg(target_vendor = "apple")]
                {
                    lock_supported = Self::try_whole_file_lock(&file, read_only)?;
                }

                false
            }
            Err(err) => return Err(err.into()),
        };

        Ok(Self {
            lock_supported,
            #[cfg(any(target_os = "linux", target_vendor = "apple"))]
            range_lock_supported,
            file,
        })
    }

    /// Takes the whole-file lock, shared or exclusive. `Ok(false)` means the platform does
    /// not support file locks at all, which is reported once rather than failing the open.
    fn try_whole_file_lock(file: &File, read_only: bool) -> Result<bool, DatabaseError> {
        let result = if read_only {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };

        match result {
            Ok(()) => Ok(true),
            Err(TryLockError::WouldBlock) => Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(err)) if err.kind() == io::ErrorKind::Unsupported => {
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file, at a time"
                );

                Ok(false)
            }
            Err(TryLockError::Error(err)) => Err(err.into()),
        }
    }
}

impl StorageBackend for FileBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.metadata()?.len())
    }

    #[cfg(unix)]
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.file.read_exact_at(out, offset)?;
        Ok(())
    }

    #[cfg(target_os = "wasi")]
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        read_exact_at(&self.file, out, offset)?;
        Ok(())
    }

    #[cfg(windows)]
    fn read(&self, mut offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let mut data_offset = 0;
        while data_offset < out.len() {
            let read = self.file.seek_read(&mut out[data_offset..], offset)?;
            // seek_read returns Ok(0) at EOF; treat a short read as an error so that reading
            // past the end of the file fails instead of looping forever.
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            offset += read as u64;
            data_offset += read;
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.file.sync_data()
    }

    #[cfg(unix)]
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.file.write_all_at(data, offset)
    }

    #[cfg(target_os = "wasi")]
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        write_all_at(&self.file, data, offset)
    }

    #[cfg(windows)]
    fn write(&self, mut offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut data_offset = 0;
        while data_offset < data.len() {
            let written = self.file.seek_write(&data[data_offset..], offset)?;
            // seek_write can report zero bytes written; treat that as an error so the write
            // fails instead of looping forever, like the read loop above.
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            offset += written as u64;
            data_offset += written;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), io::Error> {
        #[cfg(any(target_os = "linux", target_vendor = "apple"))]
        if self.range_lock_supported {
            range_lock::unlock_all(&self.file)?;
        }
        if self.lock_supported {
            self.file.unlock()?;
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

#[cfg(all(test, any(target_os = "linux", target_vendor = "apple")))]
mod range_lock_tests {
    use super::{FileBackend, range_lock};
    use crate::StorageBackend;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    // The offsets docs/design.md assigns: the header lock over the super-header bytes, the
    // fixed coordination bytes at BASE, the active transaction range above TXN_BASE, and
    // the last byte the range can address. Holding "the entire file" must cover every one
    const BASE: u64 = 1 << 62;
    const PROTOCOL_OFFSETS: [u64; 6] = [0, 319, BASE, BASE + 2, BASE + 1024 + 12345, (1 << 63) - 1];

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    /// A shared probe answers "could a reader-side lock land here", an exclusive probe
    /// "could a writer-side lock land here". The probe's own lock is released right away.
    fn byte_is_free(file: &File, offset: u64, exclusive: bool) -> bool {
        let acquired = range_lock::try_lock_byte(file, offset, exclusive).unwrap();
        if acquired {
            range_lock::unlock_byte(file, offset).unwrap();
        }
        acquired
    }

    #[test]
    fn a_writable_backend_holds_every_protocol_byte_exclusively() {
        let tmpfile = crate::create_tempfile();
        let backend = FileBackend::new_internal(reopen(tmpfile.path()), false).unwrap();

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(!byte_is_free(&observer, offset, false), "offset {offset}");
            assert!(!byte_is_free(&observer, offset, true), "offset {offset}");
        }

        // ... and releases them all at close
        backend.close().unwrap();
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, true), "offset {offset}");
        }
    }

    #[test]
    fn a_read_only_backend_shares_the_protocol_bytes() {
        let tmpfile = crate::create_tempfile();
        let backend = FileBackend::new_internal(reopen(tmpfile.path()), true).unwrap();

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, false), "offset {offset}");
            assert!(!byte_is_free(&observer, offset, true), "offset {offset}");
        }
        backend.close().unwrap();
    }

    #[test]
    fn a_held_protocol_byte_reads_as_already_open() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(range_lock::try_lock_byte(&holder, BASE, true).unwrap());

        // Both kinds of handle are refused while any part of the range is held, which is
        // what stands in for a multi-process handle having the database open
        assert!(matches!(
            FileBackend::new_internal(reopen(tmpfile.path()), false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(matches!(
            FileBackend::new_internal(reopen(tmpfile.path()), true),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));

        range_lock::unlock_byte(&holder, BASE).unwrap();
        let backend = FileBackend::new_internal(reopen(tmpfile.path()), false).unwrap();
        backend.close().unwrap();
    }

    #[test]
    fn read_only_backends_share_with_each_other() {
        let tmpfile = crate::create_tempfile();
        let first = FileBackend::new_internal(reopen(tmpfile.path()), true).unwrap();
        let second = FileBackend::new_internal(reopen(tmpfile.path()), true).unwrap();
        first.close().unwrap();
        second.close().unwrap();
    }
}
