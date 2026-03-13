use crate::Result;
use crate::tree_store::TransactionalMemory;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

/// A seekable reader for blob data stored in the database.
///
/// `BlobReader` implements [`std::io::Read`] and [`std::io::Seek`], allowing
/// partial and streaming reads of blob data without loading the entire blob
/// into memory.
///
/// Range reads bypass checksum verification since the stored checksum covers
/// the entire blob. Use [`crate::WriteTransaction::get_blob`] or
/// [`crate::ReadTransaction::get_blob`] for full-blob reads with integrity
/// verification.
pub struct BlobReader {
    mem: Arc<TransactionalMemory>,
    /// Absolute file offset of the first byte of this blob
    file_offset: u64,
    /// Total length of the blob in bytes
    blob_length: u64,
    /// Current read cursor position within the blob
    position: u64,
}

impl BlobReader {
    pub(crate) fn new(mem: Arc<TransactionalMemory>, file_offset: u64, blob_length: u64) -> Self {
        Self {
            mem,
            file_offset,
            blob_length,
            position: 0,
        }
    }

    /// Returns the total length of the blob in bytes.
    pub fn len(&self) -> u64 {
        self.blob_length
    }

    /// Returns `true` if the blob has zero length.
    pub fn is_empty(&self) -> bool {
        self.blob_length == 0
    }

    /// Returns the current read cursor position within the blob.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns the number of bytes remaining from the current position.
    pub fn remaining(&self) -> u64 {
        self.blob_length.saturating_sub(self.position)
    }

    /// Read a specific range of bytes from the blob.
    ///
    /// This is a convenience method that seeks to `offset` and reads `length` bytes.
    /// Returns the data as a `Vec<u8>`. The cursor position is advanced past the
    /// read range.
    pub fn read_range(&mut self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end =
            offset
                .checked_add(length as u64)
                .ok_or(crate::StorageError::BlobRangeOutOfBounds {
                    blob_length: self.blob_length,
                    requested_offset: offset,
                    requested_length: length as u64,
                })?;
        if end > self.blob_length {
            return Err(crate::StorageError::BlobRangeOutOfBounds {
                blob_length: self.blob_length,
                requested_offset: offset,
                requested_length: length as u64,
            });
        }

        let data = self.mem.blob_read(self.file_offset + offset, length)?;
        self.position = end;
        Ok(data)
    }
}

impl Read for BlobReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.blob_length {
            return Ok(0);
        }

        #[allow(clippy::cast_possible_truncation)]
        let remaining = (self.blob_length - self.position) as usize;
        let to_read = buf.len().min(remaining);
        if to_read == 0 {
            return Ok(0);
        }

        let data = self
            .mem
            .blob_read(self.file_offset + self.position, to_read)
            .map_err(|e| io::Error::other(e.to_string()))?;

        buf[..to_read].copy_from_slice(&data);
        self.position += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for BlobReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => i64::try_from(offset).ok(),
            SeekFrom::End(offset) => i64::try_from(self.blob_length)
                .ok()
                .and_then(|len| len.checked_add(offset)),
            SeekFrom::Current(offset) => i64::try_from(self.position)
                .ok()
                .and_then(|pos| pos.checked_add(offset)),
        };

        match new_pos {
            Some(pos) if pos >= 0 => {
                #[allow(clippy::cast_sign_loss)]
                let unsigned = pos as u64;
                self.position = unsigned;
                Ok(self.position)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for BlobReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReader")
            .field("blob_length", &self.blob_length)
            .field("position", &self.position)
            .finish()
    }
}
