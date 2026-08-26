use crate::{DatabaseError, Result, StorageBackend};
use std::fs::{File, TryLockError};
use std::io;
use std::ops::Range;

#[cfg(feature = "logging")]
use log::warn;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

use super::range_lock::RangeLock;

/// Below the coordination bytes of the multi-process locking protocol (docs/design.md) and
/// above every offset it assigns, and left unlocked by every participant, so that a lock
/// found there can only be a whole-file lock the range locks conflict with
const NAMESPACE_PROBE_BYTE: u64 = (1 << 62) - 2;

/// Every offset the protocol assigns, in the pieces the probe byte punctures it into
const PROTOCOL_RANGES: [Range<u64>; 2] =
    [0..NAMESPACE_PROBE_BYTE, NAMESPACE_PROBE_BYTE + 1..u64::MAX];

/// A lock a failing open could not release, which is logged rather than reported: the open
/// has an error of its own, and reporting this one in its place would turn a database that
/// is merely open elsewhere into an I/O failure.
fn report_failed_release(result: io::Result<()>) {
    #[cfg(feature = "logging")]
    if let Err(err) = result {
        warn!("Failed to release a file lock after an open failed: {err}");
    }
    #[cfg(not(feature = "logging"))]
    let _ = result;
}

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    lock_supported: bool,
    range_lock_supported: bool,
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, false)
    }

    pub(crate) fn new_internal(file: File, read_only: bool) -> Result<Self, DatabaseError> {
        let lock_supported = Self::try_whole_file_lock(&file, read_only)?;

        // Where the whole-file lock conflicts with the range locks it excludes their holders
        // as well, and no range is taken: on Windows, where the locks are mandatory, they
        // would block this handle's own reads and writes. Where the answer belongs to the
        // filesystem it is asked of the lock just taken, at the byte no range covers; a
        // question the filesystem refuses is answered by holding both.
        let conflicts = lock_supported
            && match File::CONFLICTS_WITH_STD_FILE_LOCK {
                Some(known) => known,
                None => file
                    .query_lock(NAMESPACE_PROBE_BYTE..NAMESPACE_PROBE_BYTE + 1)
                    .unwrap_or(false),
            };

        let range_lock_supported = if conflicts {
            false
        } else {
            match Self::try_lock_protocol_ranges(&file, read_only) {
                Ok(true) => true,
                // A multi-process handle has the database open
                Ok(false) => {
                    Self::release_whole_file_lock(&file, lock_supported);
                    return Err(DatabaseError::DatabaseAlreadyOpen);
                }
                Err(err) if err.kind() == io::ErrorKind::Unsupported => {
                    #[cfg(feature = "logging")]
                    warn!(
                        "Byte-range locks not supported by this filesystem. You must ensure that the database file is not opened for multi-process access"
                    );

                    false
                }
                Err(err) => {
                    Self::release_whole_file_lock(&file, lock_supported);
                    return Err(err.into());
                }
            }
        };

        Ok(Self {
            lock_supported,
            range_lock_supported,
            file,
        })
    }

    /// Exclusively for a writable open, shared for a read-only one, matching the whole-file
    /// lock the same open would take. Pieces taken before one conflicts are released here:
    /// the locks belong to the open file description, which a caller holding a `try_clone()`
    /// keeps alive past the drop.
    fn try_lock_protocol_ranges(file: &File, read_only: bool) -> io::Result<bool> {
        for (taken, range) in PROTOCOL_RANGES.into_iter().enumerate() {
            let acquired = if read_only {
                file.try_lock_shared_range(range)
            } else {
                file.try_lock_range(range)
            };
            if !matches!(acquired, Ok(true)) {
                Self::release_protocol_ranges(file, taken);
                return acquired;
            }
        }
        Ok(true)
    }

    /// The first `taken` ranges of an open that is failing.
    fn release_protocol_ranges(file: &File, taken: usize) {
        for range in PROTOCOL_RANGES.into_iter().take(taken) {
            report_failed_release(file.unlock_range(range));
        }
    }

    /// For the open paths that fail while holding it.
    fn release_whole_file_lock(file: &File, held: bool) {
        if held {
            report_failed_release(file.unlock());
        }
    }

    /// `Ok(false)` means the platform does not support file locks at all, which is reported
    /// once rather than failing the open.
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
        // Every lock is released even where one fails: a lock left behind outlives this
        // backend wherever the description is shared
        let mut result = Ok(());
        if self.range_lock_supported {
            for range in PROTOCOL_RANGES {
                result = result.and(self.file.unlock_range(range));
            }
        }
        if self.lock_supported {
            result = result.and(self.file.unlock());
        }

        result
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
    use super::{FileBackend, NAMESPACE_PROBE_BYTE, RangeLock};
    use crate::StorageBackend;
    use std::fs::{File, OpenOptions, TryLockError};
    use std::path::Path;

    // Offsets docs/design.md assigns: the header lock, the coordination bytes at BASE, the
    // transaction range, and the last addressable byte
    const BASE: u64 = 1 << 62;
    const PROTOCOL_OFFSETS: [u64; 6] = [0, 319, BASE, BASE + 2, BASE + 1024 + 12345, (1 << 63) - 1];

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    fn byte_is_free(file: &File, offset: u64, exclusive: bool) -> bool {
        let byte = offset..offset + 1;
        let acquired = if exclusive {
            file.try_lock_range(byte.clone()).unwrap()
        } else {
            file.try_lock_shared_range(byte.clone()).unwrap()
        };
        if acquired {
            file.unlock_range(byte).unwrap();
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
    fn read_only_backends_share_the_protocol_bytes_with_each_other() {
        let tmpfile = crate::create_tempfile();
        let first = FileBackend::new_internal(reopen(tmpfile.path()), true).unwrap();

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, false), "offset {offset}");
            assert!(!byte_is_free(&observer, offset, true), "offset {offset}");
        }

        let second = FileBackend::new_internal(reopen(tmpfile.path()), true).unwrap();
        first.close().unwrap();
        second.close().unwrap();
    }

    #[test]
    fn a_held_protocol_byte_reads_as_already_open() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(BASE..BASE + 1).unwrap());

        // A held byte anywhere in the range stands in for a multi-process handle having
        // the database open
        assert!(matches!(
            FileBackend::new_internal(reopen(tmpfile.path()), false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(matches!(
            FileBackend::new_internal(reopen(tmpfile.path()), true),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));

        holder.unlock_range(BASE..BASE + 1).unwrap();
        let backend = FileBackend::new_internal(reopen(tmpfile.path()), false).unwrap();
        backend.close().unwrap();
    }

    /// Dropping the file does not suffice: a caller holding a `try_clone()` of the file it
    /// handed over keeps the open file description, and so the locks, alive
    #[test]
    fn a_refused_open_releases_what_it_took() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        // In the second piece only, so that the first is taken before the conflict
        assert!(holder.try_lock_range(BASE..BASE + 1).unwrap());

        let file = reopen(tmpfile.path());
        let kept_by_the_caller = file.try_clone().unwrap();
        assert!(matches!(
            FileBackend::new_internal(file, false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));

        holder.unlock_range(BASE..BASE + 1).unwrap();
        let observer = reopen(tmpfile.path());
        assert!(byte_is_free(&observer, 0, true));
        observer.try_lock().unwrap();
        drop(kept_by_the_caller);
    }

    /// The question the namespace answer is asked with. An implementation blind to the locks
    /// another handle holds would report every filesystem as keeping the two kinds apart.
    #[test]
    fn a_query_reports_the_locks_another_handle_holds() {
        let tmpfile = crate::create_tempfile();
        let byte = NAMESPACE_PROBE_BYTE..NAMESPACE_PROBE_BYTE + 1;
        let asking = reopen(tmpfile.path());
        assert!(!asking.query_lock(byte.clone()).unwrap());

        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(byte.clone()).unwrap());
        assert!(asking.query_lock(byte.clone()).unwrap());

        holder.unlock_range(byte.clone()).unwrap();
        assert!(!asking.query_lock(byte).unwrap());
    }

    /// An ordinary Linux filesystem keeps the whole-file lock out of the range locks' table,
    /// so an open holds both, and leaves the byte it asked at free for the next one to ask
    #[cfg(target_os = "linux")]
    #[test]
    fn an_ordinary_filesystem_answers_that_the_two_kinds_are_separate() {
        assert!(File::CONFLICTS_WITH_STD_FILE_LOCK.is_none());
        let tmpfile = crate::create_tempfile();
        let backend = FileBackend::new_internal(reopen(tmpfile.path()), false).unwrap();

        let observer = reopen(tmpfile.path());
        assert!(matches!(observer.try_lock(), Err(TryLockError::WouldBlock)));
        assert!(!byte_is_free(&observer, BASE, true));
        assert!(byte_is_free(&observer, NAMESPACE_PROBE_BYTE, true));

        backend.close().unwrap();
    }

    /// The whole-file lock is all an older version of redb takes, so an open must keep
    /// excluding it however the namespace question is answered
    #[test]
    fn the_whole_file_lock_is_refused_while_a_backend_is_open() {
        let tmpfile = crate::create_tempfile();
        let observer = reopen(tmpfile.path());

        let writable = FileBackend::new_internal(reopen(tmpfile.path()), false).unwrap();
        assert!(matches!(
            observer.try_lock_shared(),
            Err(TryLockError::WouldBlock)
        ));
        writable.close().unwrap();

        let reader = FileBackend::new_internal(reopen(tmpfile.path()), true).unwrap();
        assert!(matches!(observer.try_lock(), Err(TryLockError::WouldBlock)));
        reader.close().unwrap();

        observer.try_lock().unwrap();
    }
}
