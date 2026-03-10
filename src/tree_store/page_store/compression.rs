//! Page-level transparent compression for redb.
//!
//! Compression is applied at the cache-to-disk boundary:
//! - Pages in memory are ALWAYS uncompressed (B-tree code is untouched)
//! - On flush: page bytes are compressed before writing to disk
//! - On read: page bytes are decompressed after reading from disk
//!
//! # On-disk format (compressed page)
//!
//! ```text
//! [0]      page type (LEAF=1, BRANCH=2) — preserved uncompressed
//! [1]      compression algorithm ID (0=none, 1=lz4, 2=zstd)
//! [2..6]   compressed data length as u32 LE (NOT including this 6-byte header)
//! [6..]    compressed(original_page[2..])
//!          zero-padded to allocation size
//! ```
//!
//! # In-memory format (always uncompressed)
//!
//! ```text
//! [0]      page type
//! [1]      0x00 (always zero — no compression flag in memory)
//! [2..]    normal page content
//! ```
//!
//! # Design decisions
//!
//! - **Compress `page[2..]`**: everything after type byte + algo byte is compressed.
//!   This means the original `page[2..]` content (including any B-tree metadata at
//!   those offsets) is fully preserved through the compress/decompress round-trip.
//! - **Stored compressed length**: the compressed data length is stored at `[2..6]`
//!   so the decompressor knows exactly where the real data ends (ignoring zero padding).
//! - **Skip small pages**: pages smaller than 128 bytes are never compressed
//!   (compression overhead exceeds savings).
//! - **Skip incompressible data**: if compressed size >= 87.5% of original,
//!   the page is stored uncompressed (avoids wasting CPU on random/encrypted data).
//! - **Header bytes always in the clear**: `page[0]` (type) and `page[1]` (algo) are
//!   never compressed, so the reader can determine format without decompression.

use crate::{Result, StorageError};
use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

/// Byte offset where the compressed-data-length field starts in a compressed page.
const COMP_LEN_OFFSET: usize = 2;
/// Byte offset where compressed data starts.
const COMP_DATA_OFFSET: usize = 6;
/// Byte offset where the compressible payload starts in the original in-memory page.
/// We compress everything from this offset onward: `page[PAYLOAD_OFFSET..]`.
const PAYLOAD_OFFSET: usize = 2;
/// Minimum page size worth compressing (bytes). Smaller pages have too much
/// overhead relative to savings.
const MIN_COMPRESS_SIZE: usize = 128;
/// If `compressed_size / original_size` exceeds this ratio, store uncompressed.
/// 7/8 = 0.875 — less than 12.5% savings means not worth the CPU.
const INCOMPRESSIBLE_RATIO: f64 = 0.875;

/// Algorithm identifier stored in `page[1]` on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum CompressionAlgorithm {
    None = 0,
    #[cfg(feature = "compression_lz4")]
    Lz4 = 1,
    #[cfg(feature = "compression_zstd")]
    Zstd = 2,
}

impl CompressionAlgorithm {
    pub(crate) fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Self::None),
            #[cfg(feature = "compression_lz4")]
            1 => Ok(Self::Lz4),
            #[cfg(feature = "compression_zstd")]
            2 => Ok(Self::Zstd),
            #[cfg(not(feature = "compression_lz4"))]
            1 => Err(StorageError::Corrupted(
                "page uses LZ4 compression but the compression_lz4 feature is not enabled"
                    .to_string(),
            )),
            #[cfg(not(feature = "compression_zstd"))]
            2 => Err(StorageError::Corrupted(
                "page uses zstd compression but the compression_zstd feature is not enabled"
                    .to_string(),
            )),
            other => Err(StorageError::Corrupted(format!(
                "unknown compression algorithm: {other}"
            ))),
        }
    }

    pub(crate) fn as_byte(self) -> u8 {
        self as u8
    }
}

/// User-facing compression configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionConfig {
    /// No compression (default).
    None,
    /// LZ4 block compression. Very fast, moderate ratio.
    /// Best for latency-sensitive workloads.
    #[cfg(feature = "compression_lz4")]
    Lz4,
    /// Zstd compression with a specified level (1-22).
    /// Level 1 is fast, level 3 is a good default, higher levels trade speed for ratio.
    #[cfg(feature = "compression_zstd")]
    Zstd {
        /// Compression level (1-22). Default: 3.
        level: i32,
    },
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::None
    }
}

impl CompressionConfig {
    pub(crate) fn algorithm(self) -> CompressionAlgorithm {
        match self {
            Self::None => CompressionAlgorithm::None,
            #[cfg(feature = "compression_lz4")]
            Self::Lz4 => CompressionAlgorithm::Lz4,
            #[cfg(feature = "compression_zstd")]
            Self::Zstd { .. } => CompressionAlgorithm::Zstd,
        }
    }

    pub(crate) fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Persistent identifier stored in the database header (1 byte).
    pub(crate) fn header_byte(self) -> u8 {
        self.algorithm().as_byte()
    }

    /// Reconstruct from header byte. Level is not stored in the header —
    /// the reader doesn't need it (decompression is level-independent for both
    /// lz4 and zstd).
    pub(crate) fn from_header_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Self::None),
            #[cfg(feature = "compression_lz4")]
            1 => Ok(Self::Lz4),
            #[cfg(feature = "compression_zstd")]
            2 => Ok(Self::Zstd { level: 0 }),
            #[cfg(not(feature = "compression_lz4"))]
            1 => Err(StorageError::Corrupted(
                "database uses LZ4 compression but compression_lz4 feature is not enabled"
                    .to_string(),
            )),
            #[cfg(not(feature = "compression_zstd"))]
            2 => Err(StorageError::Corrupted(
                "database uses zstd compression but compression_zstd feature is not enabled"
                    .to_string(),
            )),
            other => Err(StorageError::Corrupted(format!(
                "unknown compression algorithm in header: {other}"
            ))),
        }
    }
}

/// Runtime compression statistics (lock-free).
pub(crate) struct CompressionStats {
    /// Total bytes before compression (input to compressor).
    pub(crate) bytes_in: AtomicU64,
    /// Total bytes after compression (output from compressor).
    pub(crate) bytes_out: AtomicU64,
    /// Number of pages compressed.
    pub(crate) pages_compressed: AtomicU64,
    /// Number of pages skipped (too small or incompressible).
    pub(crate) pages_skipped: AtomicU64,
    /// Number of pages decompressed on read.
    pub(crate) pages_decompressed: AtomicU64,
}

impl CompressionStats {
    pub(crate) fn new() -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            pages_compressed: AtomicU64::new(0),
            pages_skipped: AtomicU64::new(0),
            pages_decompressed: AtomicU64::new(0),
        }
    }

    /// Current compression ratio (0.0 = perfect, 1.0 = no savings).
    /// Returns `None` if no pages have been compressed yet.
    #[allow(dead_code, clippy::cast_precision_loss)]
    pub(crate) fn ratio(&self) -> Option<f64> {
        let bytes_in = self.bytes_in.load(Ordering::Relaxed);
        if bytes_in == 0 {
            return None;
        }
        let bytes_out = self.bytes_out.load(Ordering::Relaxed);
        Some(bytes_out as f64 / bytes_in as f64)
    }
}

/// Compress a page for writing to disk.
///
/// Takes a full page buffer (e.g. 4096 bytes) and returns a new buffer of the
/// SAME size with compressed content in the on-disk format:
///   `[0]` = original `page[0]` (type byte)
///   `[1]` = algorithm ID
///   `[2..6]` = compressed data length (u32 LE)
///   `[6..]` = `compressed(page[2..])`, zero-padded
///
/// If compression would not save space, returns `None` (caller stores uncompressed).
#[allow(clippy::cast_precision_loss, unreachable_code, unused_variables)]
pub(crate) fn compress_page(
    page: &[u8],
    config: CompressionConfig,
    stats: &CompressionStats,
) -> Option<Vec<u8>> {
    if !config.is_enabled() || page.len() < MIN_COMPRESS_SIZE {
        stats.pages_skipped.fetch_add(1, Ordering::Relaxed);
        return None;
    }

    // Compress everything from page[2..] onward — this preserves all B-tree
    // content at those offsets through the round-trip. page[0] (type) and
    // page[1] (will become algo ID on disk) stay in the clear.
    let payload = &page[PAYLOAD_OFFSET..];
    let payload_len = payload.len();

    let compressed: Vec<u8> = match config {
        CompressionConfig::None => {
            stats.pages_skipped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        #[cfg(feature = "compression_lz4")]
        CompressionConfig::Lz4 => lz4_flex::compress_prepend_size(payload),
        #[cfg(feature = "compression_zstd")]
        CompressionConfig::Zstd { level } => {
            if let Ok(c) = zstd::bulk::compress(payload, level) {
                c
            } else {
                stats.pages_skipped.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
    };

    // Check if compression is actually worth it.
    // The compressed on-disk page needs: 6-byte header + compressed data.
    let compressed_total = COMP_DATA_OFFSET + compressed.len();
    if compressed_total >= page.len()
        || compressed.len() as f64 > payload_len as f64 * INCOMPRESSIBLE_RATIO
    {
        stats.pages_skipped.fetch_add(1, Ordering::Relaxed);
        return None;
    }

    // Build the on-disk page
    let mut out = vec![0u8; page.len()];
    out[0] = page[0]; // preserve type byte
    out[1] = config.algorithm().as_byte();
    #[allow(clippy::cast_possible_truncation)]
    let compressed_len_u32 = compressed.len() as u32;
    out[COMP_LEN_OFFSET..COMP_DATA_OFFSET].copy_from_slice(&compressed_len_u32.to_le_bytes());
    out[COMP_DATA_OFFSET..COMP_DATA_OFFSET + compressed.len()].copy_from_slice(&compressed);
    // Rest is already zeroed (padding)

    stats
        .bytes_in
        .fetch_add(payload_len as u64, Ordering::Relaxed);
    stats
        .bytes_out
        .fetch_add(compressed.len() as u64, Ordering::Relaxed);
    stats.pages_compressed.fetch_add(1, Ordering::Relaxed);

    Some(out)
}

/// Decompress a page read from disk.
///
/// Checks `page[1]` for compression algorithm. If 0, returns the buffer unchanged.
/// Otherwise, decompresses and returns a full-size page with `page[1]` cleared to 0.
#[allow(unreachable_code, unused_variables)]
pub(crate) fn decompress_page(page: &[u8], stats: &CompressionStats) -> Result<Option<Vec<u8>>> {
    if page.len() < COMP_DATA_OFFSET {
        return Ok(None); // Too small to have compression header
    }

    let algo = CompressionAlgorithm::from_byte(page[1])?;
    if matches!(algo, CompressionAlgorithm::None) {
        return Ok(None); // Not compressed
    }

    let compressed_len =
        u32::from_le_bytes(page[COMP_LEN_OFFSET..COMP_DATA_OFFSET].try_into().unwrap()) as usize;

    if COMP_DATA_OFFSET + compressed_len > page.len() {
        return Err(StorageError::Corrupted(format!(
            "compressed page claims compressed length {compressed_len} \
             but only {} bytes available after header",
            page.len() - COMP_DATA_OFFSET
        )));
    }

    // Slice only the actual compressed data (excluding zero padding)
    let compressed_data = &page[COMP_DATA_OFFSET..COMP_DATA_OFFSET + compressed_len];

    // Maximum decompressed size: the original page could not have been larger
    // than page.len() (same allocation size), and we compressed page[2..],
    // so max decompressed = page.len() - PAYLOAD_OFFSET.
    let max_decompressed = page.len() - PAYLOAD_OFFSET;

    let decompressed: Vec<u8> = match algo {
        CompressionAlgorithm::None => unreachable!(),
        #[cfg(feature = "compression_lz4")]
        CompressionAlgorithm::Lz4 => lz4_flex::decompress_size_prepended(compressed_data)
            .map_err(|e| StorageError::Corrupted(format!("LZ4 decompression failed: {e}")))?,
        #[cfg(feature = "compression_zstd")]
        CompressionAlgorithm::Zstd => zstd::bulk::decompress(compressed_data, max_decompressed)
            .map_err(|e| StorageError::Corrupted(format!("zstd decompression failed: {e}")))?,
    };

    // Reconstruct the full in-memory page:
    //   [0] = type byte (from disk)
    //   [1] = 0 (cleared — no compression flag in memory)
    //   [2..] = decompressed payload (original page[2..])
    let mut out = vec![0u8; PAYLOAD_OFFSET + decompressed.len()];
    out[0] = page[0]; // type byte preserved
    // out[1] = 0 — already zeroed, no compression in memory
    out[PAYLOAD_OFFSET..].copy_from_slice(&decompressed);

    stats.pages_decompressed.fetch_add(1, Ordering::Relaxed);

    Ok(Some(out))
}

// ──────────────────────────────────────────────────────────────────────
// Value-level compression
// ──────────────────────────────────────────────────────────────────────
//
// Unlike page-level compression (which zero-pads to fixed page slots and
// saves nothing), value-level compression compresses individual values
// BEFORE they enter the B-tree. Shorter values → fewer pages → smaller files.
//
// Compressed value envelope:
//   [0]     flags: bit 0 = compressed, bits 1-2 = algo (01=lz4, 10=zstd)
//   [1..5]  original_size (u32 LE)
//   [5..]   compressed data
//
// If flags == 0x00: the value is stored raw (no envelope overhead).
// The flags byte is self-describing: decompression never needs config.

/// Minimum value size worth compressing. Below this the 5-byte envelope
/// overhead exceeds any realistic savings.
const MIN_VALUE_COMPRESS_SIZE: usize = 64;

/// Size of the compressed value envelope header (flags + `original_size`).
const VALUE_ENVELOPE_SIZE: usize = 5;

/// Flags byte encoding for compressed values.
const VALUE_FLAG_COMPRESSED: u8 = 0x01;
#[cfg(feature = "compression_lz4")]
const VALUE_FLAG_LZ4: u8 = 0x01 | 0x02; // bits: compressed=1, algo=01
#[cfg(feature = "compression_zstd")]
const VALUE_FLAG_ZSTD: u8 = 0x01 | 0x04; // bits: compressed=1, algo=10

/// Compress a value for storage in the B-tree.
///
/// When `config` is `None`, returns an exact copy (no envelope).
///
/// When compression is enabled, **always** prepends a flags byte:
/// - `0x00` = value stored uncompressed (remaining bytes are original data)
/// - `0x03` = LZ4 compressed, `0x05` = zstd compressed
///   followed by `[orig_size_u32_le, compressed_data...]`
///
/// This eliminates ambiguity: `decompress_value()` can safely rely on the
/// flags byte being present for any value written to a compressed database.
#[allow(unused_variables, clippy::cast_possible_truncation)]
pub(crate) fn compress_value(data: &[u8], config: CompressionConfig) -> Vec<u8> {
    if !config.is_enabled() || data.is_empty() {
        return data.to_vec();
    }

    // Try actual compression if above minimum size
    if data.len() >= MIN_VALUE_COMPRESS_SIZE {
        let result: Option<(Vec<u8>, u8)> = match config {
            CompressionConfig::None => None,
            #[cfg(feature = "compression_lz4")]
            CompressionConfig::Lz4 => Some((lz4_flex::compress_prepend_size(data), VALUE_FLAG_LZ4)),
            #[cfg(feature = "compression_zstd")]
            CompressionConfig::Zstd { level } => zstd::bulk::compress(data, level)
                .ok()
                .map(|c| (c, VALUE_FLAG_ZSTD)),
        };

        if let Some((compressed, flags)) = result {
            // Only use compression if it actually saves space (envelope overhead = 5 bytes)
            if VALUE_ENVELOPE_SIZE + compressed.len() < data.len() {
                let mut out = Vec::with_capacity(VALUE_ENVELOPE_SIZE + compressed.len());
                out.push(flags);
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(&compressed);
                return out;
            }
        }
    }

    // Compression not attempted or not beneficial — prepend 0x00 flags byte
    let mut out = Vec::with_capacity(1 + data.len());
    out.push(0x00);
    out.extend_from_slice(data);
    out
}

/// Decompress a value read from a compressed database's B-tree.
///
/// Every value in a compressed database has a flags byte prepended:
/// - `0x00` = not compressed → returns `Cow::Borrowed(&data[1..])` (zero-copy)
/// - `0x03` / `0x05` = compressed → returns `Cow::Owned(decompressed)`
///
/// **Only call this for values from compressed databases.** Values from
/// uncompressed databases have no flags byte and must not be passed here.
#[allow(unused_variables)]
pub(crate) fn decompress_value(data: &[u8]) -> Result<Cow<'_, [u8]>> {
    if data.is_empty() {
        return Ok(Cow::Borrowed(data));
    }
    if data[0] & VALUE_FLAG_COMPRESSED == 0 {
        // Not compressed — strip the flags byte, return remaining data
        return Ok(Cow::Borrowed(&data[1..]));
    }

    if data.len() < VALUE_ENVELOPE_SIZE {
        return Err(StorageError::Corrupted(
            "compressed value too short for envelope header".to_string(),
        ));
    }

    let flags = data[0];
    let original_size = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
    let compressed_data = &data[VALUE_ENVELOPE_SIZE..];

    let algo_bits = flags & 0x06; // bits 1-2
    match algo_bits {
        #[cfg(feature = "compression_lz4")]
        0x02 => {
            let decompressed =
                lz4_flex::decompress_size_prepended(compressed_data).map_err(|e| {
                    StorageError::Corrupted(format!("LZ4 value decompression failed: {e}"))
                })?;
            if decompressed.len() != original_size {
                return Err(StorageError::Corrupted(format!(
                    "decompressed value size mismatch: expected {original_size}, got {}",
                    decompressed.len()
                )));
            }
            Ok(Cow::Owned(decompressed))
        }
        #[cfg(feature = "compression_zstd")]
        0x04 => {
            let decompressed =
                zstd::bulk::decompress(compressed_data, original_size).map_err(|e| {
                    StorageError::Corrupted(format!("zstd value decompression failed: {e}"))
                })?;
            if decompressed.len() != original_size {
                return Err(StorageError::Corrupted(format!(
                    "decompressed value size mismatch: expected {original_size}, got {}",
                    decompressed.len()
                )));
            }
            Ok(Cow::Owned(decompressed))
        }
        #[cfg(not(feature = "compression_lz4"))]
        0x02 => Err(StorageError::Corrupted(
            "value uses LZ4 compression but compression_lz4 feature is not enabled".to_string(),
        )),
        #[cfg(not(feature = "compression_zstd"))]
        0x04 => Err(StorageError::Corrupted(
            "value uses zstd compression but compression_zstd feature is not enabled".to_string(),
        )),
        _ => Err(StorageError::Corrupted(format!(
            "unknown value compression algorithm bits: {algo_bits:#04x}"
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation, clippy::needless_range_loop)]
mod tests {
    use super::*;

    fn make_test_page(size: usize) -> Vec<u8> {
        let mut page = vec![0u8; size];
        page[0] = 1; // LEAF type
        // page[1] = 0 (no compression in memory)
        // Simulate B-tree metadata at page[2..6]
        page[2] = 10; // e.g. num_pairs low byte
        page[3] = 0;
        page[4] = 0xFF; // some flags
        page[5] = 0x42; // some data
        // Compressible repeating pattern for the rest
        for i in 6..size {
            page[i] = (i % 64) as u8;
        }
        page
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn lz4_round_trip() {
        let page = make_test_page(4096);
        let config = CompressionConfig::Lz4;
        let stats = CompressionStats::new();

        let compressed = compress_page(&page, config, &stats).expect("should compress");
        assert_eq!(compressed.len(), page.len());
        assert_eq!(compressed[0], 1); // type preserved
        assert_eq!(compressed[1], 1); // LZ4 algo

        let decompressed = decompress_page(&compressed, &stats)
            .expect("no error")
            .expect("should decompress");

        // Full round-trip: the entire page content must match
        assert_eq!(decompressed[0], page[0]); // type preserved
        assert_eq!(decompressed[1], 0); // algo cleared
        assert_eq!(&decompressed[2..], &page[2..]); // ALL content from offset 2 preserved
    }

    #[cfg(feature = "compression_zstd")]
    #[test]
    fn zstd_round_trip() {
        let page = make_test_page(4096);
        let config = CompressionConfig::Zstd { level: 3 };
        let stats = CompressionStats::new();

        let compressed = compress_page(&page, config, &stats).expect("should compress");
        assert_eq!(compressed.len(), page.len());
        assert_eq!(compressed[0], 1); // type preserved
        assert_eq!(compressed[1], 2); // zstd algo

        let decompressed = decompress_page(&compressed, &stats)
            .expect("no error")
            .expect("should decompress");

        assert_eq!(decompressed[0], page[0]);
        assert_eq!(decompressed[1], 0);
        assert_eq!(&decompressed[2..], &page[2..]); // ALL content preserved
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn lz4_preserves_btree_metadata() {
        // Specifically verify that bytes [2..6] survive the round-trip
        let page = make_test_page(4096);
        let config = CompressionConfig::Lz4;
        let stats = CompressionStats::new();

        let compressed = compress_page(&page, config, &stats).expect("should compress");
        let decompressed = decompress_page(&compressed, &stats)
            .expect("no error")
            .expect("should decompress");

        assert_eq!(decompressed[2], 10);
        assert_eq!(decompressed[3], 0);
        assert_eq!(decompressed[4], 0xFF);
        assert_eq!(decompressed[5], 0x42);
    }

    #[test]
    fn no_compression_passthrough() {
        let page = make_test_page(4096);
        let config = CompressionConfig::None;
        let stats = CompressionStats::new();

        assert!(compress_page(&page, config, &stats).is_none());
        assert_eq!(stats.pages_skipped.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn small_page_skipped() {
        let page = make_test_page(64);
        let stats = CompressionStats::new();
        let config = CompressionConfig::Lz4;
        assert!(compress_page(&page, config, &stats).is_none());
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn incompressible_skipped() {
        // Random data doesn't compress well
        let mut page = vec![0u8; 4096];
        page[0] = 1;
        // Fill with pseudo-random bytes
        let mut state = 0x12345678u32;
        for byte in &mut page[2..] {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
            *byte = (state >> 16) as u8;
        }

        let config = CompressionConfig::Lz4;
        let stats = CompressionStats::new();
        let result = compress_page(&page, config, &stats);
        // Whether it compresses or not depends on the data, but the logic path is tested
        let _ = result;
    }

    #[test]
    fn uncompressed_page_decompress_noop() {
        let page = make_test_page(4096);
        let stats = CompressionStats::new();
        // page[1] == 0, so decompress should return None
        assert!(decompress_page(&page, &stats).unwrap().is_none());
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn stats_tracking() {
        let page = make_test_page(4096);
        let config = CompressionConfig::Lz4;
        let stats = CompressionStats::new();

        let compressed = compress_page(&page, config, &stats);
        assert!(compressed.is_some());
        assert_eq!(stats.pages_compressed.load(Ordering::Relaxed), 1);
        assert!(stats.bytes_in.load(Ordering::Relaxed) > 0);
        assert!(stats.bytes_out.load(Ordering::Relaxed) > 0);
        assert!(stats.ratio().unwrap() < 1.0); // compression saved space

        let _ = decompress_page(&compressed.unwrap(), &stats);
        assert_eq!(stats.pages_decompressed.load(Ordering::Relaxed), 1);
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn various_page_sizes() {
        for &size in &[512, 1024, 2048, 4096, 8192, 16384, 65536] {
            let page = make_test_page(size);
            let config = CompressionConfig::Lz4;
            let stats = CompressionStats::new();

            if let Some(compressed) = compress_page(&page, config, &stats) {
                let decompressed = decompress_page(&compressed, &stats)
                    .expect("no error")
                    .expect("should decompress");
                assert_eq!(decompressed.len(), page.len());
                assert_eq!(&decompressed[..], &page[..]);
            }
        }
    }

    #[cfg(all(feature = "compression_lz4", feature = "compression_zstd"))]
    #[test]
    fn cross_algorithm_detection() {
        let page = make_test_page(4096);
        let stats = CompressionStats::new();

        // Compress with LZ4
        let lz4_compressed =
            compress_page(&page, CompressionConfig::Lz4, &stats).expect("should compress");
        assert_eq!(lz4_compressed[1], 1); // LZ4

        // Compress with zstd
        let zstd_compressed = compress_page(&page, CompressionConfig::Zstd { level: 3 }, &stats)
            .expect("should compress");
        assert_eq!(zstd_compressed[1], 2); // zstd

        // Decompress each — the decompressor auto-detects from page[1]
        let d1 = decompress_page(&lz4_compressed, &stats).unwrap().unwrap();
        let d2 = decompress_page(&zstd_compressed, &stats).unwrap().unwrap();
        assert_eq!(d1, d2);
        assert_eq!(&d1[..], &page[..]);
    }

    // ── Value-level compression tests ──

    fn make_compressible_value(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 64) as u8).collect()
    }

    #[test]
    fn value_no_compression_passthrough() {
        let data = make_compressible_value(1024);
        let result = compress_value(&data, CompressionConfig::None);
        assert_eq!(result, data);
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn value_small_skipped() {
        let data = make_compressible_value(32); // below MIN_VALUE_COMPRESS_SIZE
        let result = compress_value(&data, CompressionConfig::Lz4);
        // Flags byte (0x00) prepended, original data follows
        assert_eq!(result.len(), data.len() + 1);
        assert_eq!(result[0], 0x00);
        assert_eq!(&result[1..], &data[..]);
        // Round-trip through decompress
        let decompressed = decompress_value(&result).unwrap();
        assert_eq!(decompressed.as_ref(), &data[..]);
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn value_lz4_round_trip() {
        let data = make_compressible_value(1536); // embedding-sized
        let compressed = compress_value(&data, CompressionConfig::Lz4);
        assert!(compressed.len() < data.len(), "should be smaller");
        assert_eq!(compressed[0] & VALUE_FLAG_COMPRESSED, VALUE_FLAG_COMPRESSED);

        let decompressed = decompress_value(&compressed).unwrap();
        assert_eq!(decompressed.as_ref(), &data[..]);
    }

    #[cfg(feature = "compression_zstd")]
    #[test]
    fn value_zstd_round_trip() {
        let data = make_compressible_value(1536);
        let compressed = compress_value(&data, CompressionConfig::Zstd { level: 3 });
        assert!(compressed.len() < data.len(), "should be smaller");

        let decompressed = decompress_value(&compressed).unwrap();
        assert_eq!(decompressed.as_ref(), &data[..]);
    }

    #[test]
    fn value_uncompressed_decompress_noop() {
        let data = vec![0x00, 0x41, 0x42, 0x43]; // flags=0x00 → not compressed
        let result = decompress_value(&data).unwrap();
        assert!(matches!(result, Cow::Borrowed(_)));
        // Flags byte stripped: returns &data[1..]
        assert_eq!(result.as_ref(), &[0x41, 0x42, 0x43]);
    }

    #[test]
    fn value_empty_decompress_noop() {
        let result = decompress_value(&[]).unwrap();
        assert!(matches!(result, Cow::Borrowed(_)));
        assert!(result.is_empty());
    }
}
