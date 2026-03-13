use crate::WriteTransaction;
use crate::blob_store::types::{BlobId, BlobMeta, BlobRef, CausalLink, ContentType};
use crate::tree_store::{Xxh3StreamHasher, hash64_with_seed};
use std::io;
use std::sync::atomic::Ordering;

/// Streaming blob writer that writes data in arbitrary-sized chunks with
/// constant memory overhead, regardless of total blob size.
///
/// Created via [`WriteTransaction::blob_writer`]. Data is written directly to
/// the append-only blob region as each chunk arrives. At [`finish`](Self::finish),
/// the xxh3 checksums are finalized and the blob is indexed in the system tables.
///
/// Implements [`std::io::Write`] for interoperability with the standard library.
///
/// # Drop behavior
///
/// If the writer is dropped without calling `finish()`, the blob data already
/// written to the blob region becomes dead space (it is not indexed). The
/// active-writer guard is released so subsequent blob operations can proceed.
pub struct BlobWriter<'txn> {
    txn: &'txn WriteTransaction,
    sequence: u64,
    content_type: ContentType,
    label: String,
    causal_link: Option<CausalLink>,
    /// Absolute file offset where this blob's data starts.
    blob_file_offset: u64,
    /// Offset within the blob region where this blob starts.
    blob_region_start: u64,
    bytes_written: u64,
    /// First 4096 bytes of blob data, for computing the content prefix hash.
    prefix_buf: Vec<u8>,
    /// Incremental xxh3-128 hasher for the full blob checksum.
    /// Wrapped in Option so `finish()` can take ownership despite Drop impl.
    hasher: Option<Xxh3StreamHasher>,
    finished: bool,
}

const PREFIX_HASH_LEN: usize = 4096;

impl<'txn> BlobWriter<'txn> {
    pub(crate) fn new(
        txn: &'txn WriteTransaction,
        sequence: u64,
        content_type: ContentType,
        label: &str,
        causal_link: Option<CausalLink>,
        blob_file_offset: u64,
        blob_region_start: u64,
    ) -> Self {
        Self {
            txn,
            sequence,
            content_type,
            label: label.to_string(),
            causal_link,
            blob_file_offset,
            blob_region_start,
            bytes_written: 0,
            prefix_buf: Vec::with_capacity(PREFIX_HASH_LEN),
            hasher: Some(Xxh3StreamHasher::new(0)),
            finished: false,
        }
    }

    /// Write a chunk of blob data. Can be called any number of times.
    pub fn write(&mut self, data: &[u8]) -> crate::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        // Buffer prefix bytes (up to 4096) for the content prefix hash
        let prefix_remaining = PREFIX_HASH_LEN.saturating_sub(self.prefix_buf.len());
        if prefix_remaining > 0 {
            let copy_len = data.len().min(prefix_remaining);
            self.prefix_buf.extend_from_slice(&data[..copy_len]);
        }

        // Write data to the blob region
        let file_offset = self.blob_file_offset + self.bytes_written;
        self.txn.blob_write_raw(file_offset, data)?;

        // Feed the streaming hasher
        self.hasher.as_mut().expect("hasher taken").update(data);
        self.bytes_written += data.len() as u64;

        Ok(())
    }

    /// Finalize the blob: compute checksums, index in system tables, and
    /// return the assigned `BlobId`.
    pub fn finish(mut self) -> crate::Result<BlobId> {
        self.finished = true;

        // Compute content prefix hash (xxh3-64 of first min(4096, blob_len) bytes)
        let content_prefix_hash = hash64_with_seed(&self.prefix_buf, 0);
        let blob_id = BlobId::new(self.sequence, content_prefix_hash);

        // Finalize full checksum (xxh3-128)
        let hasher = self.hasher.take().expect("hasher taken");
        let checksum = hasher.finish_128();

        // Build BlobRef and BlobMeta
        let blob_ref = BlobRef {
            offset: self.blob_region_start,
            length: self.bytes_written,
            checksum,
            ref_count: 1,
            content_type: self.content_type.as_byte(),
            compression: 0,
        };

        #[allow(clippy::cast_possible_truncation)]
        let wall_clock_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos() as u64;

        let causal_parent = self.causal_link.as_ref().map(|l| l.parent);
        let meta = BlobMeta::new(
            blob_ref,
            wall_clock_ns,
            0, // HLC placeholder — set by finalize_blob_writer
            causal_parent,
            &self.label,
        );

        // Delegate indexing and state updates to WriteTransaction
        let causal_link = self.causal_link.take();
        self.txn
            .finalize_blob_writer(blob_id, meta, self.bytes_written, causal_link)?;

        Ok(blob_id)
    }

    /// Total bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl Drop for BlobWriter<'_> {
    fn drop(&mut self) {
        self.txn
            .blob_writer_active()
            .store(false, Ordering::Release);
    }
}

impl io::Write for BlobWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        BlobWriter::write(self, buf).map_err(io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
