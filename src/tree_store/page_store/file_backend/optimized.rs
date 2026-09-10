use crate::BackendError;
#[cfg(all(target_os = "linux", not(feature = "experimental-api-5")))]
use crate::db::{SHARED_WRITER_BYTE, byte_range};
use crate::{DatabaseError, Result, StorageBackend};
use std::collections::HashSet;
use std::fs::{File, TryLockError};
use std::io;
use std::ops::Bound;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

use super::range_lock::RangeLock;

fn is_whole_storage(range: (Bound<u64>, Bound<u64>)) -> bool {
    matches!(range.0, Bound::Unbounded | Bound::Included(0)) && range.1 == Bound::Unbounded
}

#[cfg(all(target_os = "linux", not(feature = "experimental-api-5")))]
fn needs_legacy_lock(range: (Bound<u64>, Bound<u64>)) -> bool {
    range == (Bound::Excluded(SHARED_WRITER_BYTE), Bound::Unbounded)
}

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    whole_file_locked: AtomicBool,
    // UnlockFile requires exactly the range that was locked, so retain each acquisition.
    locked_ranges: Mutex<HashSet<(Bound<u64>, Bound<u64>)>>,
    file: File,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Ok(Self {
            whole_file_locked: AtomicBool::new(false),
            locked_ranges: Mutex::new(HashSet::new()),
            file,
        })
    }

    // Best-effort 4.x compatibility: when a single-process open takes its suffix range,
    // also take the flock used by older redb, unless it shares the range-lock namespace.
    #[cfg(all(target_os = "linux", not(feature = "experimental-api-5")))]
    fn lock_legacy_file(&self, shared: bool) -> io::Result<bool> {
        match self.lock_whole_file(shared) {
            Ok(false) => return Ok(false),
            Ok(true) => {
                // Immutable opens leave this byte free, even when other immutable readers
                // hold the rest of the file.
                if !matches!(
                    self.file.query_lock(byte_range(SHARED_WRITER_BYTE)),
                    Ok(false)
                ) {
                    self.unlock_whole_file()?;
                }
            }
            Err(_) => {}
        }
        Ok(true)
    }

    fn try_lock(&self, range: (Bound<u64>, Bound<u64>), shared: bool) -> io::Result<bool> {
        #[cfg(all(target_os = "linux", not(feature = "experimental-api-5")))]
        if needs_legacy_lock(range) && !self.lock_legacy_file(shared)? {
            return Ok(false);
        }

        let acquired = if shared {
            self.file.try_lock_shared_range(range)
        } else {
            self.file.try_lock_range(range)
        };
        if matches!(acquired, Ok(true)) {
            self.locked_ranges.lock().unwrap().insert(range);
            return acquired;
        }

        #[cfg(all(target_os = "linux", not(feature = "experimental-api-5")))]
        if needs_legacy_lock(range) {
            self.unlock_whole_file()?;
        }
        match acquired {
            Err(err) if err.kind() == io::ErrorKind::Unsupported && is_whole_storage(range) => {
                self.lock_whole_file(shared)
            }
            result => result,
        }
    }

    fn lock(&self, range: (Bound<u64>, Bound<u64>), shared: bool) -> io::Result<()> {
        let result = if shared {
            self.file.lock_shared_range(range)
        } else {
            self.file.lock_range(range)
        };
        match result {
            Ok(()) => {
                self.locked_ranges.lock().unwrap().insert(range);
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::Unsupported && is_whole_storage(range) => {
                if shared {
                    self.file.lock_shared()?;
                } else {
                    self.file.lock()?;
                }
                self.whole_file_locked.store(true, Ordering::Release);
                Ok(())
            }
            result => result,
        }
    }

    fn release_all_locks(&self) -> io::Result<()> {
        // A lock left behind outlives this backend wherever the description is shared.
        let mut result = Ok(());
        for range in self.locked_ranges.lock().unwrap().drain() {
            result = result.and(self.file.unlock_range(range));
        }
        result.and(self.unlock_whole_file())
    }

    fn unlock_whole_file(&self) -> io::Result<()> {
        if self.whole_file_locked.load(Ordering::Acquire) {
            self.file.unlock()?;
            self.whole_file_locked.store(false, Ordering::Release);
        }
        Ok(())
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

impl StorageBackend for FileBackend {
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.try_lock((start, end), false)
            .map_err(BackendError::from)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.try_lock((start, end), true)
            .map_err(BackendError::from)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.lock((start, end), false).map_err(BackendError::from)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.lock((start, end), true).map_err(BackendError::from)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        let range = (start, end);
        if is_whole_storage(range)
            && self.whole_file_locked.load(Ordering::Acquire)
            && !self.locked_ranges.lock().unwrap().contains(&range)
        {
            return self.unlock_whole_file().map_err(BackendError::from);
        }

        // Retain failed unlocks for close() to retry.
        self.file.unlock_range(range)?;
        self.locked_ranges.lock().unwrap().remove(&range);
        #[cfg(all(target_os = "linux", not(feature = "experimental-api-5")))]
        if needs_legacy_lock(range) {
            self.unlock_whole_file()?;
        }
        Ok(())
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.file
            .query_lock((start, end))
            .map_err(BackendError::from)
    }

    fn close(&self) -> Result<(), io::Error> {
        self.release_all_locks()
    }

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

#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod range_lock_tests {
    use super::{Bound, FileBackend, RangeLock, StorageBackend};
    use crate::db::FULL_RANGE;
    #[cfg(not(target_os = "linux"))]
    use std::fs::TryLockError;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    // Offsets docs/design.md assigns: the header lock, the coordination bytes at BASE, the
    // transaction range, and the last addressable byte
    const BASE: u64 = 1 << 62;
    const PROTOCOL_OFFSETS: [u64; 7] = [
        0,
        319,
        BASE,
        BASE + 1,
        BASE + 2,
        BASE + 1024 + 12345,
        (1 << 63) - 1,
    ];

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    /// Exercise the public whole-storage primitive, including the shared writer byte.
    fn open_file(file: File, read_only: bool) -> Result<FileBackend, crate::DatabaseError> {
        let backend = FileBackend::new(file).unwrap();
        let acquired = if read_only {
            backend.try_lock_shared_range(FULL_RANGE.0, FULL_RANGE.1)
        } else {
            backend.try_lock_range(FULL_RANGE.0, FULL_RANGE.1)
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

    /// ... and releases at close, which is what the core relies on
    fn close(backend: &FileBackend) {
        crate::StorageBackend::close(backend).unwrap();
    }

    /// `close()` has to release rather than leaving it to the file being dropped: a read
    /// transaction outliving the database keeps the backend, and so the file, alive past it
    #[test]
    fn close_releases_while_the_backend_is_still_alive() {
        let tmpfile = crate::create_tempfile();
        let backend = open(tmpfile.path(), false).unwrap();
        close(&backend);

        let observer = reopen(tmpfile.path());
        for offset in PROTOCOL_OFFSETS {
            assert!(byte_is_free(&observer, offset, false), "offset {offset}");
        }
        assert!(observer.try_lock().is_ok());
        drop(backend);
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

    /// A bounded range takes only the native byte-range lock.
    #[test]
    fn a_range_short_of_the_whole_storage_is_a_byte_range_lock_alone() {
        let tmpfile = crate::create_tempfile();
        // No whole-storage lock: on the platforms whose locks do not split, a range it covered
        // could not be locked or released on its own
        let backend = FileBackend::new(reopen(tmpfile.path())).unwrap();
        let byte = BASE..BASE + 1;

        let observer = reopen(tmpfile.path());
        assert!(
            backend
                .try_lock_range(Bound::Included(byte.start), Bound::Excluded(byte.end))
                .unwrap()
        );
        assert!(!byte_is_free(&observer, BASE, false));
        backend
            .unlock_range(Bound::Included(byte.start), Bound::Excluded(byte.end))
            .unwrap();
        assert!(byte_is_free(&observer, BASE, true));

        assert!(
            backend
                .try_lock_shared_range(Bound::Included(byte.start), Bound::Excluded(byte.end))
                .unwrap()
        );
        assert!(byte_is_free(&observer, BASE, false));
        assert!(!byte_is_free(&observer, BASE, true));
        backend
            .unlock_range(Bound::Included(byte.start), Bound::Excluded(byte.end))
            .unwrap();
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
        assert!(holder.try_lock_range(BASE..=BASE).unwrap());

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

        holder.unlock_range(BASE..=BASE).unwrap();
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
        assert!(holder.try_lock_range(BASE..=BASE).unwrap());

        let file = reopen(tmpfile.path());
        let kept_by_the_caller = file.try_clone().unwrap();
        assert!(matches!(
            open_file(file, false),
            Err(crate::DatabaseError::DatabaseAlreadyOpen)
        ));

        holder.unlock_range(BASE..=BASE).unwrap();
        let observer = reopen(tmpfile.path());
        assert!(byte_is_free(&observer, 0, true));
        observer.try_lock().unwrap();
        drop(kept_by_the_caller);
    }

    /// The question the protocol's probes are asked with. An implementation blind to the locks
    /// another handle holds would answer every one of them free.
    #[test]
    fn a_query_reports_the_locks_another_handle_holds() {
        let tmpfile = crate::create_tempfile();
        let byte = BASE + 4..BASE + 5;
        let asking = reopen(tmpfile.path());
        assert!(!asking.query_lock(byte.clone()).unwrap());

        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(byte.clone()).unwrap());
        assert!(asking.query_lock(byte.clone()).unwrap());

        holder.unlock_range(byte.clone()).unwrap();
        assert!(!asking.query_lock(byte).unwrap());
    }

    #[test]
    fn large_lock_offsets_do_not_alias_lower_bytes() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        let observer = reopen(tmpfile.path());

        for offset in PROTOCOL_OFFSETS
            .into_iter()
            .filter(|offset| *offset >= BASE)
        {
            let byte = offset..offset + 1;
            let low_offset = offset & u64::from(u32::MAX);
            let low_byte = low_offset..low_offset + 1;
            for exclusive in [false, true] {
                let acquired = if exclusive {
                    holder.try_lock_range(byte.clone())
                } else {
                    holder.try_lock_shared_range(byte.clone())
                };
                assert!(acquired.unwrap());
                assert!(observer.query_lock(byte.clone()).unwrap());
                assert!(!observer.try_lock_range(byte.clone()).unwrap());
                assert!(!observer.query_lock(low_byte.clone()).unwrap());
                assert!(observer.try_lock_range(low_byte.clone()).unwrap());
                observer.unlock_range(low_byte.clone()).unwrap();

                holder.unlock_range(byte.clone()).unwrap();
                assert!(!observer.query_lock(byte.clone()).unwrap());
            }
        }
    }

    #[test]
    fn large_lock_lengths_preserve_range_boundaries() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        let observer = reopen(tmpfile.path());

        for range in [0..BASE, BASE..(1 << 63) - 1] {
            let last_byte = range.end - 1..range.end;
            assert!(holder.try_lock_shared_range(range.clone()).unwrap());
            assert!(observer.query_lock(last_byte.clone()).unwrap());
            assert!(!observer.try_lock_range(last_byte.clone()).unwrap());
            assert!(observer.try_lock_shared_range(last_byte.clone()).unwrap());
            observer.unlock_range(last_byte.clone()).unwrap();
            assert!(!observer.query_lock(range.end..=range.end).unwrap());

            holder.unlock_range(range).unwrap();
            assert!(!observer.query_lock(last_byte).unwrap());
        }
    }

    #[test]
    fn blocking_locks_use_large_offsets() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        let observer = reopen(tmpfile.path());
        let byte = BASE + 4..BASE + 5;

        holder.lock_shared_range(byte.clone()).unwrap();
        assert!(observer.query_lock(byte.clone()).unwrap());
        assert!(!observer.try_lock_range(byte.clone()).unwrap());
        holder.unlock_range(byte.clone()).unwrap();

        holder.lock_range(byte.clone()).unwrap();
        assert!(observer.query_lock(byte.clone()).unwrap());
        assert!(!observer.try_lock_shared_range(byte.clone()).unwrap());
        holder.unlock_range(byte.clone()).unwrap();
        assert!(!observer.query_lock(byte).unwrap());
    }

    #[test]
    fn blocking_whole_storage_locks_have_no_protocol_holes() {
        let tmpfile = crate::create_tempfile();
        let backend = FileBackend::new(reopen(tmpfile.path())).unwrap();
        let observer = reopen(tmpfile.path());
        for shared in [false, true] {
            if shared {
                backend
                    .lock_shared_range(FULL_RANGE.0, FULL_RANGE.1)
                    .unwrap();
            } else {
                backend.lock_range(FULL_RANGE.0, FULL_RANGE.1).unwrap();
            }
            for offset in PROTOCOL_OFFSETS {
                assert!(!byte_is_free(&observer, offset, true));
            }
            backend.unlock_range(FULL_RANGE.0, FULL_RANGE.1).unwrap();
            for offset in PROTOCOL_OFFSETS {
                assert!(byte_is_free(&observer, offset, true));
            }
        }
        close(&backend);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn ordinary_unbounded_ranges_do_not_take_legacy_locks() {
        let tmpfile = crate::create_tempfile();
        let observer = reopen(tmpfile.path());
        observer.try_lock().unwrap();
        let file = reopen(tmpfile.path());
        // Some filesystems emulate flock with range locks, making exclusion unavoidable.
        if !byte_is_free(&file, 0, true) {
            return;
        }
        observer.unlock().unwrap();
        let backend = FileBackend::new(file).unwrap();

        for range in [
            (Bound::Included(BASE + 1024), Bound::Unbounded),
            (Bound::Included(100), Bound::Unbounded),
            (Bound::Included(0), Bound::Unbounded),
            FULL_RANGE,
        ] {
            for shared in [false, true] {
                let try_lock = || {
                    if shared {
                        backend.try_lock_shared_range(range.0, range.1)
                    } else {
                        backend.try_lock_range(range.0, range.1)
                    }
                };
                observer.try_lock().unwrap();
                assert!(try_lock().unwrap());
                backend.unlock_range(range.0, range.1).unwrap();
                observer.unlock().unwrap();

                assert!(try_lock().unwrap());
                observer.try_lock().unwrap();
                observer.unlock().unwrap();
                assert!(!byte_is_free(&observer, BASE + 1024, true));
                backend.unlock_range(range.0, range.1).unwrap();
            }
        }

        close(&backend);
    }

    /// These platforms use the same lock namespace for ranges and whole files.
    #[cfg(not(target_os = "linux"))]
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
    #[cfg(not(target_os = "linux"))]
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
