//! Compression support for redb.
//!
//! Value-level compression compresses individual values BEFORE they enter
//! the B-tree. Shorter values -> fewer pages -> smaller files.
//!
//! # Compressed value envelope
//!
//! ```text
//! [0]     flags: bit 0 = compressed, bits 1-2 = algo (01=lz4, 10=zstd)
//! [1..5]  original_size (u32 LE)
//! [5..]   compressed data
//! ```
//!
//! If flags == 0x00: the value is stored raw (no envelope overhead).
//! The flags byte is self-describing: decompression never needs config.

use crate::{Result, StorageError};
use alloc::borrow::Cow;
use alloc::format;
use alloc::string::ToString;
use alloc::vec::Vec;

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
    pub(crate) fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }

    /// Persistent identifier stored in the database header (1 byte).
    pub(crate) fn header_byte(self) -> u8 {
        match self {
            Self::None => 0,
            #[cfg(feature = "compression_lz4")]
            Self::Lz4 => 1,
            #[cfg(feature = "compression_zstd")]
            Self::Zstd { .. } => 2,
        }
    }

    /// Reconstruct from header byte. Level is not stored in the header --
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

// ----------------------------------------------------------------------
// Value-level compression
// ----------------------------------------------------------------------

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

    // Compression not attempted or not beneficial -- prepend 0x00 flags byte
    let mut out = Vec::with_capacity(1 + data.len());
    out.push(0x00);
    out.extend_from_slice(data);
    out
}

/// Decompress a value read from a compressed database's B-tree.
///
/// Every value in a compressed database has a flags byte prepended:
/// - `0x00` = not compressed -> returns `Cow::Borrowed(&data[1..])` (zero-copy)
/// - `0x03` / `0x05` = compressed -> returns `Cow::Owned(decompressed)`
///
/// **Only call this for values from compressed databases.** Values from
/// uncompressed databases have no flags byte and must not be passed here.
#[allow(unused_variables)]
pub(crate) fn decompress_value(data: &[u8]) -> Result<Cow<'_, [u8]>> {
    if data.is_empty() {
        return Ok(Cow::Borrowed(data));
    }
    if data[0] & VALUE_FLAG_COMPRESSED == 0 {
        // Not compressed -- strip the flags byte, return remaining data
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
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

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
        let data = vec![0x00, 0x41, 0x42, 0x43]; // flags=0x00 -> not compressed
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
