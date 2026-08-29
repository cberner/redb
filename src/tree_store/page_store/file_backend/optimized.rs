use crate::db::{FULL_RANGE, InternalStorageBackend};
use crate::{DatabaseError, Result, StorageBackend};
use std::fs::{File, TryLockError};
use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "logging")]
use log::warn;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

use super::range_lock::RangeLock;

// An offset that is not used in the multi-process locking protocol. Used to detect whether flock()
// and range locks share the same namespace.
const NAMESPACE_PROBE_BYTE: u64 = (1 << 62) - 2;

/// Every offset the multi-process locking protocol uses
const PROTOCOL_RANGES: [Range<u64>; 2] =
    [0..NAMESPACE_PROBE_BYTE, NAMESPACE_PROBE_BYTE + 1..u64::MAX];

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
    whole_file_locked: AtomicBool,
    ranges_locked: AtomicBool,
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Ok(Self {
            whole_file_locked: AtomicBool::new(false),
            ranges_locked: AtomicBool::new(false),
            file,
        })
    }

    /// The whole storage, locked with the protocol ranges and -- where those are a namespace of
    /// their own -- the whole-file lock an older redb takes as well.
    fn lock_whole_storage(&self, shared: bool) -> io::Result<bool> {
        // Prefer range locks
        if File::CONFLICTS_WITH_STD_FILE_LOCK == Some(true) {
            return match self.lock_protocol_ranges(shared) {
                // Windows, whose range locks are not implemented yet and whose whole-file lock is
                // itself one over every byte
                Err(err) if err.kind() == io::ErrorKind::Unsupported => {
                    self.lock_whole_file(shared)
                }
                result => result,
            };
        }

        if !self.lock_whole_file(shared)? {
            return Ok(false);
        }

        if File::CONFLICTS_WITH_STD_FILE_LOCK.is_none()
            && self
                .file
                .query_lock(NAMESPACE_PROBE_BYTE..NAMESPACE_PROBE_BYTE + 1)
                .unwrap_or(false)
        {
            // Prefer range locks, so release the whole-file lock which maps to the range lock
            self.release_whole_storage()?;
            return self.lock_protocol_ranges(shared);
        }

        let acquired = self.lock_protocol_ranges(shared);
        match acquired {
            Ok(true) => Ok(true),
            // Without range locks there is no multi-process access to exclude, and the whole-file
            // lock already keeps every other process out
            Err(ref err) if err.kind() == io::ErrorKind::Unsupported => Ok(true),
            // A multi-process handle has the database open
            _ => {
                report_failed_release(self.release_whole_storage());
                acquired
            }
        }
    }

    /// The ranges alone, which is what a namespace shared with the whole-file lock calls for
    fn lock_protocol_ranges(&self, shared: bool) -> io::Result<bool> {
        for (taken, range) in PROTOCOL_RANGES.into_iter().enumerate() {
            let acquired = if shared {
                self.file.try_lock_shared_range(range)
            } else {
                self.file.try_lock_range(range)
            };
            // Another handle has the database open, holding either kind of lock
            if !matches!(acquired, Ok(true)) {
                for range in PROTOCOL_RANGES.into_iter().take(taken) {
                    report_failed_release(self.file.unlock_range(range));
                }
                return acquired;
            }
        }
        self.ranges_locked.store(true, Ordering::Release);

        Ok(true)
    }

    fn release_whole_storage(&self) -> io::Result<()> {
        // Every lock is released even where one fails: a lock left behind outlives this
        // backend wherever the description is shared
        let mut result = Ok(());
        if self.ranges_locked.swap(false, Ordering::AcqRel) {
            for range in PROTOCOL_RANGES {
                result = result.and(self.file.unlock_range(range));
            }
        }
        if self.whole_file_locked.swap(false, Ordering::AcqRel) {
            result = result.and(self.file.unlock());
        }

        result
    }

    fn lock_whole_file(&self, shared: bool) -> io::Result<bool> {
        let result = if shared {
            self.file.try_lock_shared()
        } else {
            self.file.try_lock()
        };

        match result {
            Ok(()) => {
                self.whole_file_locked.store(true, Ordering::Release);
                Ok(true)
            }
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(TryLockError::Error(err)) => Err(err),
        }
    }
}

/// The whole storage is what a single-process open locks, and the whole-file lock an older redb
/// takes covers exactly that, so both kinds are held for that one range. Any other range is a
/// byte-range lock alone.
impl InternalStorageBackend for FileBackend {
    fn try_lock_range(&self, range: Range<u64>) -> io::Result<bool> {
        if range == FULL_RANGE {
            self.lock_whole_storage(false)
        } else {
            self.file.try_lock_range(range)
        }
    }

    fn try_lock_shared_range(&self, range: Range<u64>) -> io::Result<bool> {
        if range == FULL_RANGE {
            self.lock_whole_storage(true)
        } else {
            self.file.try_lock_shared_range(range)
        }
    }

    fn unlock_range(&self, range: Range<u64>) -> io::Result<()> {
        if range == FULL_RANGE {
            self.release_whole_storage()
        } else {
            self.file.unlock_range(range)
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
    use super::{FULL_RANGE, FileBackend, InternalStorageBackend, NAMESPACE_PROBE_BYTE, RangeLock};
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

    /// The whole-storage lock the core takes at open, mapped as the core maps it
    fn open_file(file: File, read_only: bool) -> Result<FileBackend, crate::DatabaseError> {
        let backend = FileBackend::new(file).unwrap();
        let acquired = if read_only {
            backend.try_lock_shared_range(FULL_RANGE)
        } else {
            backend.try_lock_range(FULL_RANGE)
        };
        match acquired {
            Ok(true) => Ok(backend),
            Ok(false) => Err(crate::DatabaseError::DatabaseAlreadyOpen),
            Err(err) => Err(err.into()),
        }
    }

    fn open(path: &Path, read_only: bool) -> Result<FileBackend, crate::DatabaseError> {
        open_file(reopen(path), read_only)
    }

    /// ... and releases at close
    fn close(backend: &FileBackend) {
        backend.unlock_range(FULL_RANGE).unwrap();
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

    /// Only the whole storage is locked with both kinds. A range short of it is the byte-range
    /// lock alone, which is what the concurrency protocols take
    #[test]
    fn a_range_short_of_the_whole_storage_is_a_byte_range_lock_alone() {
        let tmpfile = crate::create_tempfile();
        let backend = open(tmpfile.path(), false).unwrap();
        let byte = BASE..BASE + 1;
        // Held by the open already, so it is released first: the same file description's locks
        // replace each other rather than conflicting
        backend.unlock_range(byte.clone()).unwrap();

        let observer = reopen(tmpfile.path());
        assert!(backend.try_lock_range(byte.clone()).unwrap());
        assert!(!byte_is_free(&observer, BASE, false));
        backend.unlock_range(byte.clone()).unwrap();
        assert!(byte_is_free(&observer, BASE, true));

        assert!(backend.try_lock_shared_range(byte.clone()).unwrap());
        assert!(byte_is_free(&observer, BASE, false));
        assert!(!byte_is_free(&observer, BASE, true));
        backend.unlock_range(byte).unwrap();

        close(&backend);
    }

    #[test]
    fn a_writable_backend_holds_every_protocol_byte_exclusively() {
        let tmpfile = crate::create_tempfile();
        let backend = open(tmpfile.path(), false).unwrap();

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(!byte_is_free(&observer, offset, false), "offset {offset}");
            assert!(!byte_is_free(&observer, offset, true), "offset {offset}");
        }

        // ... and releases them all at close
        close(&backend);
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, true), "offset {offset}");
        }
    }

    #[test]
    fn read_only_backends_share_the_protocol_bytes_with_each_other() {
        let tmpfile = crate::create_tempfile();
        let first = open(tmpfile.path(), true).unwrap();

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, false), "offset {offset}");
            assert!(!byte_is_free(&observer, offset, true), "offset {offset}");
        }

        let second = open(tmpfile.path(), true).unwrap();
        close(&first);
        close(&second);
    }

    #[test]
    fn a_held_protocol_byte_reads_as_already_open() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(BASE..BASE + 1).unwrap());

        // A held byte anywhere in the range stands in for a multi-process handle having
        // the database open
        assert!(matches!(
            open(tmpfile.path(), false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(matches!(
            open(tmpfile.path(), true),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));

        holder.unlock_range(BASE..BASE + 1).unwrap();
        let backend = open(tmpfile.path(), false).unwrap();
        close(&backend);
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
            open_file(file, false),
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
    /// so an open holds both: only the whole-file lock itself can refuse the observer's
    #[cfg(target_os = "linux")]
    #[test]
    fn an_ordinary_filesystem_answers_that_the_two_kinds_are_separate() {
        assert!(File::CONFLICTS_WITH_STD_FILE_LOCK.is_none());
        let tmpfile = crate::create_tempfile();
        let backend = open(tmpfile.path(), false).unwrap();

        let observer = reopen(tmpfile.path());
        assert!(matches!(observer.try_lock(), Err(TryLockError::WouldBlock)));
        assert!(!byte_is_free(&observer, BASE, true));

        close(&backend);
    }

    /// The open a platform whose two kinds of lock conflict takes, holding the ranges in place of
    /// a whole-file lock that is never taken -- which only a separate namespace can observe, so
    /// the check belongs here rather than where the open is actually reached
    #[cfg(target_os = "linux")]
    #[test]
    fn an_open_holding_the_ranges_takes_no_whole_file_lock() {
        use super::AtomicBool;

        let tmpfile = crate::create_tempfile();
        let file = reopen(tmpfile.path());
        let backend = FileBackend {
            whole_file_locked: AtomicBool::new(false),
            ranges_locked: AtomicBool::new(false),
            file,
        };
        assert!(backend.lock_protocol_ranges(false).unwrap());

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(!byte_is_free(&observer, offset, false), "offset {offset}");
        }
        assert!(byte_is_free(&observer, NAMESPACE_PROBE_BYTE, true));
        // Here the whole-file lock is a namespace of its own, so taking it proves it is unheld
        observer.try_lock().unwrap();
        observer.unlock().unwrap();

        close(&backend);
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, true), "offset {offset}");
        }
    }

    /// The byte the namespace probe asks at is left free by every open, whichever lock it holds:
    /// where the whole-file lock would cover it, the ranges are held in its place
    #[test]
    fn the_probe_byte_is_free_while_a_backend_is_open() {
        let tmpfile = crate::create_tempfile();
        let observer = reopen(tmpfile.path());

        let writable = open(tmpfile.path(), false).unwrap();
        assert!(byte_is_free(&observer, NAMESPACE_PROBE_BYTE, true));
        close(&writable);

        let reader = open(tmpfile.path(), true).unwrap();
        assert!(byte_is_free(&observer, NAMESPACE_PROBE_BYTE, true));
        close(&reader);
    }

    /// The whole-file lock is all an older version of redb takes, so an open must keep
    /// excluding it however the namespace question is answered
    #[test]
    fn the_whole_file_lock_is_refused_while_a_backend_is_open() {
        let tmpfile = crate::create_tempfile();
        let observer = reopen(tmpfile.path());

        let writable = open(tmpfile.path(), false).unwrap();
        assert!(matches!(
            observer.try_lock_shared(),
            Err(TryLockError::WouldBlock)
        ));
        close(&writable);

        let reader = open(tmpfile.path(), true).unwrap();
        assert!(matches!(observer.try_lock(), Err(TryLockError::WouldBlock)));
        close(&reader);

        observer.try_lock().unwrap();
    }

    /// ... and be excluded by one: the same older version, having opened the database first
    #[test]
    fn a_whole_file_lock_holder_reads_as_already_open() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        holder.try_lock().unwrap();

        assert!(matches!(
            open(tmpfile.path(), false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));
        assert!(matches!(
            open(tmpfile.path(), true),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));

        // Held shared it is an older read-only holder, which only a writable open conflicts with
        holder.unlock().unwrap();
        holder.try_lock_shared().unwrap();
        assert!(matches!(
            open(tmpfile.path(), false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));
        let reader = open(tmpfile.path(), true).unwrap();
        close(&reader);
        holder.unlock().unwrap();
    }
}
