use crate::transaction_tracker::TransactionId;
use crate::{Result, StorageError};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const DATABASE_FILE_NAME: &str = "database.redb";
pub(crate) const LOCK_DIRECTORY_NAME: &str = "locks";
pub(crate) const INITIALIZATION_LOCK_FILE_NAME: &str = "initialization.lock";
pub(crate) const WRITER_LOCK_FILE_NAME: &str = "writer.lock";
pub(crate) const READER_GATE_FILE_NAME: &str = "reader-gate.lock";
pub(crate) const PROTOCOL_FILE_NAME: &str = "protocol-v1";
pub(crate) const PROTOCOL_TEMP_FILE_NAME: &str = "protocol-v1.tmp";
const SINGLE_WRITER_PROTOCOL_CONTENTS: &[u8] = b"redb-multiprocess-1\nwriter-mode=single\n";
const MULTIPLE_WRITER_PROTOCOL_CONTENTS: &[u8] = b"redb-multiprocess-1\nwriter-mode=multiple\n";
const READER_FILE_PREFIX: &str = "reader-slot-";
const INACTIVE_READER: u64 = u64::MAX;

static NEXT_READER_FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum MultiProcessWriteMode {
    SingleWriter,
    MultipleWriters,
}

impl MultiProcessWriteMode {
    pub(crate) fn protocol_contents(self) -> &'static [u8] {
        match self {
            Self::SingleWriter => SINGLE_WRITER_PROTOCOL_CONTENTS,
            Self::MultipleWriters => MULTIPLE_WRITER_PROTOCOL_CONTENTS,
        }
    }

    pub(crate) fn multiple_writers(self) -> bool {
        self == Self::MultipleWriters
    }

    pub(crate) fn from_protocol_contents(contents: &[u8]) -> Option<Self> {
        match contents {
            SINGLE_WRITER_PROTOCOL_CONTENTS => Some(Self::SingleWriter),
            MULTIPLE_WRITER_PROTOCOL_CONTENTS => Some(Self::MultipleWriters),
            _ => None,
        }
    }
}

pub(crate) struct LockedFile {
    file: File,
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(crate) struct MultiProcessTracker {
    lock_directory: PathBuf,
    reader_gate_path: PathBuf,
    reader_file: Mutex<File>,
    reader_path: PathBuf,
    writer: File,
    write_mode: MultiProcessWriteMode,
    single_writer_state: Mutex<SingleWriterState>,
    external_oldest_reader: Mutex<Option<TransactionId>>,
}

#[derive(Default)]
struct SingleWriterState {
    owned: bool,
    initialized: bool,
}

impl MultiProcessTracker {
    pub(crate) fn new(lock_directory: &Path, write_mode: MultiProcessWriteMode) -> Result<Self> {
        let reader_gate_path = lock_directory.join(READER_GATE_FILE_NAME);
        let reader_gate = open_lock_file(&reader_gate_path)?;
        let writer = open_lock_file(&lock_directory.join(WRITER_LOCK_FILE_NAME))?;

        reader_gate.lock_shared()?;
        let (reader_file, reader_path) = create_reader_file(lock_directory)?;
        reader_gate.unlock()?;

        Ok(Self {
            lock_directory: lock_directory.to_path_buf(),
            reader_gate_path,
            reader_file: Mutex::new(reader_file),
            reader_path,
            writer,
            write_mode,
            single_writer_state: Mutex::new(SingleWriterState::default()),
            external_oldest_reader: Mutex::new(None),
        })
    }

    // Returns whether allocator state must be reloaded after acquiring writer ownership.
    pub(crate) fn lock_writer(&self) -> Result<bool> {
        if self.write_mode.multiple_writers() {
            self.writer.lock()?;
            return Ok(true);
        }

        let mut state = self.single_writer_state.lock()?;
        if !state.owned {
            self.writer.lock()?;
            state.owned = true;
            state.initialized = false;
        }
        Ok(!state.initialized)
    }

    pub(crate) fn writer_initialized(&self) {
        if !self.write_mode.multiple_writers() {
            self.single_writer_state.lock().unwrap().initialized = true;
        }
    }

    pub(crate) fn abandon_writer(&self) -> Result {
        if !self.write_mode.multiple_writers() {
            let mut state = self.single_writer_state.lock()?;
            self.writer.unlock()?;
            state.owned = false;
            state.initialized = false;
            return Ok(());
        }
        self.writer.unlock().map_err(StorageError::from)
    }

    pub(crate) fn end_write_transaction(&self) -> Result {
        if self.write_mode.multiple_writers() {
            self.writer.unlock().map_err(StorageError::from)
        } else {
            Ok(())
        }
    }

    pub(crate) fn multiple_writers(&self) -> bool {
        self.write_mode.multiple_writers()
    }

    pub(crate) fn lock_reader_gate_shared(&self) -> Result<LockedFile> {
        let file = open_lock_file(&self.reader_gate_path)?;
        file.lock_shared()?;
        Ok(LockedFile { file })
    }

    pub(crate) fn lock_reader_gate_exclusive(&self) -> Result<LockedFile> {
        let file = open_lock_file(&self.reader_gate_path)?;
        file.lock()?;
        Ok(LockedFile { file })
    }

    pub(crate) fn prepare_commit(&self) -> Result<LockedFile> {
        let gate = open_lock_file(&self.reader_gate_path)?;
        gate.lock()?;

        let scan_result = self.scan_oldest_reader();
        match scan_result {
            Ok(oldest) => {
                *self.external_oldest_reader.lock()? = oldest;
                Ok(LockedFile { file: gate })
            }
            Err(error) => {
                let _ = gate.unlock();
                Err(error)
            }
        }
    }

    pub(crate) fn external_oldest_reader(&self) -> Option<TransactionId> {
        *self.external_oldest_reader.lock().unwrap()
    }

    pub(crate) fn publish_oldest_reader(&self, oldest: Option<TransactionId>) -> Result {
        let value = oldest.map_or(INACTIVE_READER, TransactionId::raw_id);
        let mut file = self.reader_file.lock()?;
        write_reader_record(&mut file, value)?;
        Ok(())
    }

    fn scan_oldest_reader(&self) -> Result<Option<TransactionId>> {
        let mut oldest: Option<TransactionId> = None;
        for entry in std::fs::read_dir(&self.lock_directory)? {
            let entry = entry?;
            let file_name = entry.file_name();
            if !file_name.to_string_lossy().starts_with(READER_FILE_PREFIX) {
                continue;
            }

            let path = entry.path();
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            match file.try_lock() {
                Ok(()) => {
                    file.unlock()?;
                    drop(file);
                    let _ = std::fs::remove_file(path);
                }
                Err(TryLockError::WouldBlock) => {
                    let value = read_reader_record(&mut file)?;
                    if value != INACTIVE_READER {
                        let id = TransactionId::new(value);
                        oldest = Some(oldest.map_or(id, |current| current.min(id)));
                    }
                }
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
        Ok(oldest)
    }
}

impl Drop for MultiProcessTracker {
    fn drop(&mut self) {
        let Ok(gate) = open_lock_file(&self.reader_gate_path) else {
            return;
        };
        if gate.lock_shared().is_err() {
            return;
        }

        if let Ok(file) = self.reader_file.get_mut() {
            let _ = write_reader_record(file, INACTIVE_READER);
            let _ = file.unlock();
        }
        let _ = std::fs::remove_file(&self.reader_path);
        let _ = gate.unlock();
    }
}

pub(crate) fn open_lock_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn create_reader_file(lock_directory: &Path) -> Result<(File, PathBuf)> {
    loop {
        let sequence = NEXT_READER_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path = lock_directory.join(format!(
            "{READER_FILE_PREFIX}{}-{sequence}.lock",
            std::process::id()
        ));
        let open_result = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path);
        let mut file = match open_result {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        };
        file.lock()?;
        write_reader_record(&mut file, INACTIVE_READER)?;
        return Ok((file, path));
    }
}

fn write_reader_record(file: &mut File, value: u64) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&value.to_le_bytes())?;
    file.set_len(size_of::<u64>() as u64)
}

fn read_reader_record(file: &mut File) -> Result<u64> {
    let mut bytes = [0; size_of::<u64>()];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut bytes).map_err(|error| {
        StorageError::Corrupted(format!("invalid multi-process reader record: {error}"))
    })?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_contents_round_trip() {
        for mode in [
            MultiProcessWriteMode::SingleWriter,
            MultiProcessWriteMode::MultipleWriters,
        ] {
            assert_eq!(
                MultiProcessWriteMode::from_protocol_contents(mode.protocol_contents()),
                Some(mode)
            );
        }
        assert_eq!(
            MultiProcessWriteMode::from_protocol_contents(b"redb-multiprocess-2\n"),
            None
        );
    }

    #[test]
    fn active_and_stale_reader_records() {
        let directory = tempfile::tempdir().unwrap();
        let tracker1 =
            MultiProcessTracker::new(directory.path(), MultiProcessWriteMode::SingleWriter)
                .unwrap();
        let tracker2 =
            MultiProcessTracker::new(directory.path(), MultiProcessWriteMode::SingleWriter)
                .unwrap();

        let reader_gate = tracker1.lock_reader_gate_shared().unwrap();
        tracker1
            .publish_oldest_reader(Some(TransactionId::new(11)))
            .unwrap();
        tracker2
            .publish_oldest_reader(Some(TransactionId::new(7)))
            .unwrap();
        drop(reader_gate);

        let gate = tracker1.prepare_commit().unwrap();
        assert_eq!(
            tracker1.external_oldest_reader(),
            Some(TransactionId::new(7))
        );
        drop(gate);

        let stale_path = tracker2.reader_path.clone();
        drop(tracker2);
        let mut stale = open_lock_file(&stale_path).unwrap();
        write_reader_record(&mut stale, 1).unwrap();

        let gate = tracker1.prepare_commit().unwrap();
        assert_eq!(
            tracker1.external_oldest_reader(),
            Some(TransactionId::new(11))
        );
        drop(gate);
        assert!(!stale_path.exists());
    }

    #[test]
    fn reader_gate_locks_have_independent_lifetimes() {
        let directory = tempfile::tempdir().unwrap();
        let tracker =
            MultiProcessTracker::new(directory.path(), MultiProcessWriteMode::SingleWriter)
                .unwrap();
        let gate1 = tracker.lock_reader_gate_shared().unwrap();
        let gate2 = tracker.lock_reader_gate_shared().unwrap();
        drop(gate1);

        let probe = open_lock_file(&directory.path().join(READER_GATE_FILE_NAME)).unwrap();
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        drop(gate2);
        probe.lock().unwrap();
        probe.unlock().unwrap();
    }

    #[test]
    fn writer_lock_lifetime_follows_mode() {
        let directory = tempfile::tempdir().unwrap();
        let writer_path = directory.path().join(WRITER_LOCK_FILE_NAME);

        let single_writer =
            MultiProcessTracker::new(directory.path(), MultiProcessWriteMode::SingleWriter)
                .unwrap();
        assert!(single_writer.lock_writer().unwrap());
        single_writer.writer_initialized();
        single_writer.end_write_transaction().unwrap();
        assert!(!single_writer.lock_writer().unwrap());
        let probe = open_lock_file(&writer_path).unwrap();
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        drop(single_writer);
        probe.lock().unwrap();
        probe.unlock().unwrap();

        let multiple_writers =
            MultiProcessTracker::new(directory.path(), MultiProcessWriteMode::MultipleWriters)
                .unwrap();
        assert!(multiple_writers.lock_writer().unwrap());
        assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
        multiple_writers.end_write_transaction().unwrap();
        probe.lock().unwrap();
        probe.unlock().unwrap();
    }
}
