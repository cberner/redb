use crate::temporal::HybridLogicalClock;
use crate::types::{Key, TypeName, Value};
use std::cmp::Ordering;
use std::fmt;
use std::mem::size_of;

// ---------------------------------------------------------------------------
// ContentType
// ---------------------------------------------------------------------------

/// MIME-like content type tag for blobs. Stored as a single byte discriminant.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ContentType {
    OctetStream = 0,
    ImagePng = 1,
    ImageJpeg = 2,
    AudioWav = 3,
    AudioOgg = 4,
    VideoMp4 = 5,
    PointCloudLas = 6,
    SensorImu = 7,
    Embedding = 8,
    Metadata = 9,
}

impl ContentType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => Self::ImagePng,
            2 => Self::ImageJpeg,
            3 => Self::AudioWav,
            4 => Self::AudioOgg,
            5 => Self::VideoMp4,
            6 => Self::PointCloudLas,
            7 => Self::SensorImu,
            8 => Self::Embedding,
            9 => Self::Metadata,
            _ => Self::OctetStream,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }

    pub fn mime_str(self) -> &'static str {
        match self {
            Self::OctetStream => "application/octet-stream",
            Self::ImagePng => "image/png",
            Self::ImageJpeg => "image/jpeg",
            Self::AudioWav => "audio/wav",
            Self::AudioOgg => "audio/ogg",
            Self::VideoMp4 => "video/mp4",
            Self::PointCloudLas => "application/vnd.las",
            Self::SensorImu => "application/x-imu",
            Self::Embedding => "application/x-embedding",
            Self::Metadata => "application/json",
        }
    }
}

impl fmt::Debug for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentType({})", self.mime_str())
    }
}

// ---------------------------------------------------------------------------
// BlobId — 16-byte unique identifier for a blob
// ---------------------------------------------------------------------------

/// Unique identifier for a blob in the store.
///
/// Layout (16 bytes):
/// - `sequence` (`u64`): monotonic counter, incremented per blob write
/// - `content_prefix_hash` (`u64`): xxh3-64 of the first min(4096, `blob_len`) bytes
///
/// Ordering: sequence first (temporal), then `content_prefix_hash` (tiebreaker).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobId {
    pub sequence: u64,
    pub content_prefix_hash: u64,
}

impl BlobId {
    pub const SERIALIZED_SIZE: usize = 16;

    pub const MIN: Self = Self {
        sequence: 0,
        content_prefix_hash: 0,
    };

    pub const MAX: Self = Self {
        sequence: u64::MAX,
        content_prefix_hash: u64::MAX,
    };

    pub fn new(sequence: u64, content_prefix_hash: u64) -> Self {
        Self {
            sequence,
            content_prefix_hash,
        }
    }

    pub fn to_le_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[..8].copy_from_slice(&self.sequence.to_le_bytes());
        buf[8..16].copy_from_slice(&self.content_prefix_hash.to_le_bytes());
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        Self {
            sequence: u64::from_le_bytes(data[..8].try_into().unwrap()),
            content_prefix_hash: u64::from_le_bytes(data[8..16].try_into().unwrap()),
        }
    }
}

impl PartialOrd for BlobId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlobId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then(self.content_prefix_hash.cmp(&other.content_prefix_hash))
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlobId(seq={}, hash={:016x})",
            self.sequence, self.content_prefix_hash
        )
    }
}

impl Value for BlobId {
    type SelfType<'a>
        = BlobId
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; BlobId::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::BlobId")
    }
}

impl Key for BlobId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a = Self::from_bytes(data1);
        let b = Self::from_bytes(data2);
        a.cmp(&b)
    }
}

// ---------------------------------------------------------------------------
// BlobRef — 40-byte on-disk pointer to blob data in the append-only region
// ---------------------------------------------------------------------------

/// On-disk reference to blob data stored in the append-only blob region.
/// This is stored as the inner component of `BlobMeta` in the B-tree.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BlobRef {
    /// Byte offset from the start of the blob region
    pub offset: u64,
    /// Length of the blob data in bytes
    pub length: u64,
    /// xxh3-128 checksum of the entire blob
    pub checksum: u128,
    /// Reference count for content-addressable dedup (initialized to 1)
    pub ref_count: u32,
    /// Content type discriminant
    pub content_type: u8,
    /// Compression applied: 0=none, 1=lz4, 2=zstd
    pub compression: u8,
}

impl BlobRef {
    pub const SERIALIZED_SIZE: usize = 8 + 8 + 16 + 4 + 1 + 1 + 2; // 40 bytes

    pub fn to_le_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        let mut pos = 0;
        buf[pos..pos + 8].copy_from_slice(&self.offset.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.length.to_le_bytes());
        pos += 8;
        buf[pos..pos + 16].copy_from_slice(&self.checksum.to_le_bytes());
        pos += 16;
        buf[pos..pos + 4].copy_from_slice(&self.ref_count.to_le_bytes());
        pos += 4;
        buf[pos] = self.content_type;
        pos += 1;
        buf[pos] = self.compression;
        // remaining 2 bytes are reserved (zeros)
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let mut pos = 0;
        let offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let length = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let checksum = u128::from_le_bytes(data[pos..pos + 16].try_into().unwrap());
        pos += 16;
        let ref_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let content_type = data[pos];
        pos += 1;
        let compression = data[pos];
        Self {
            offset,
            length,
            checksum,
            ref_count,
            content_type,
            compression,
        }
    }

    pub fn content_type_enum(&self) -> ContentType {
        ContentType::from_byte(self.content_type)
    }
}

impl fmt::Debug for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobRef")
            .field("offset", &self.offset)
            .field("length", &self.length)
            .field("checksum", &format_args!("{:032x}", self.checksum))
            .field("ref_count", &self.ref_count)
            .field("content_type", &ContentType::from_byte(self.content_type))
            .field("compression", &self.compression)
            .finish()
    }
}

impl Value for BlobRef {
    type SelfType<'a>
        = BlobRef
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; BlobRef::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::BlobRef")
    }
}

// ---------------------------------------------------------------------------
// BlobMeta — full metadata per blob (137 bytes, fixed-width)
// ---------------------------------------------------------------------------

/// Complete metadata for a stored blob, combining the on-disk reference
/// with temporal and causal information.
///
/// Stored as the value in the primary `BLOB_TABLE` system table (`BlobId` → `BlobMeta`).
#[derive(Clone)]
pub struct BlobMeta {
    pub blob_ref: BlobRef,
    pub wall_clock_ns: u64,
    pub hlc: u64, // HybridLogicalClock stored as raw u64
    /// If set, this blob was produced as a result of processing `causal_parent`.
    pub causal_parent: Option<BlobId>,
    /// User-provided label (up to 63 bytes, UTF-8).
    pub label_len: u8,
    pub label: [u8; 63],
}

impl BlobMeta {
    // 40 (BlobRef) + 8 (wall_clock) + 8 (hlc) + 1 (has_parent) + 16 (parent) + 1 (label_len) + 63 (label)
    pub const SERIALIZED_SIZE: usize = 40 + 8 + 8 + 1 + 16 + 1 + 63; // = 137

    pub fn new(
        blob_ref: BlobRef,
        wall_clock_ns: u64,
        hlc: u64,
        causal_parent: Option<BlobId>,
        label: &str,
    ) -> Self {
        let label_bytes = label.as_bytes();
        // Safe: .min(63) guarantees value fits in u8 (max 63 < 256).
        #[allow(clippy::cast_possible_truncation)]
        let label_len = label_bytes.len().min(63) as u8;
        let mut label_buf = [0u8; 63];
        label_buf[..label_len as usize].copy_from_slice(&label_bytes[..label_len as usize]);
        Self {
            blob_ref,
            wall_clock_ns,
            hlc,
            causal_parent,
            label_len,
            label: label_buf,
        }
    }

    pub fn label_str(&self) -> &str {
        std::str::from_utf8(&self.label[..self.label_len as usize]).unwrap_or("")
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        let mut pos = 0;

        // BlobRef (40 bytes)
        let ref_bytes = self.blob_ref.to_le_bytes();
        buf[pos..pos + BlobRef::SERIALIZED_SIZE].copy_from_slice(&ref_bytes);
        pos += BlobRef::SERIALIZED_SIZE;

        // wall_clock_ns (8 bytes)
        buf[pos..pos + 8].copy_from_slice(&self.wall_clock_ns.to_le_bytes());
        pos += 8;

        // hlc (8 bytes)
        buf[pos..pos + 8].copy_from_slice(&self.hlc.to_le_bytes());
        pos += 8;

        // causal_parent: 1 byte tag + 16 bytes data
        if let Some(parent) = self.causal_parent {
            buf[pos] = 1;
            pos += 1;
            buf[pos..pos + BlobId::SERIALIZED_SIZE].copy_from_slice(&parent.to_le_bytes());
            pos += BlobId::SERIALIZED_SIZE;
        } else {
            buf[pos] = 0;
            pos += 1;
            // 16 zero bytes for absent parent (buf is already zeroed)
            pos += BlobId::SERIALIZED_SIZE;
        }

        // label_len (1 byte) + label (63 bytes)
        buf[pos] = self.label_len;
        pos += 1;
        buf[pos..pos + 63].copy_from_slice(&self.label);

        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let mut pos = 0;

        let blob_ref = BlobRef::from_le_bytes(
            data[pos..pos + BlobRef::SERIALIZED_SIZE]
                .try_into()
                .unwrap(),
        );
        pos += BlobRef::SERIALIZED_SIZE;

        let wall_clock_ns = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let hlc = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let has_parent = data[pos];
        pos += 1;
        let causal_parent = if has_parent != 0 {
            Some(BlobId::from_le_bytes(
                data[pos..pos + BlobId::SERIALIZED_SIZE].try_into().unwrap(),
            ))
        } else {
            None
        };
        pos += BlobId::SERIALIZED_SIZE;

        let label_len = data[pos];
        pos += 1;
        let mut label = [0u8; 63];
        label.copy_from_slice(&data[pos..pos + 63]);

        Self {
            blob_ref,
            wall_clock_ns,
            hlc,
            causal_parent,
            label_len,
            label,
        }
    }
}

impl fmt::Debug for BlobMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobMeta")
            .field("blob_ref", &self.blob_ref)
            .field("wall_clock_ns", &self.wall_clock_ns)
            .field("hlc", &self.hlc)
            .field("causal_parent", &self.causal_parent)
            .field("label", &self.label_str())
            .finish_non_exhaustive()
    }
}

impl Value for BlobMeta {
    type SelfType<'a>
        = BlobMeta
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; BlobMeta::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::BlobMeta")
    }
}

// ---------------------------------------------------------------------------
// RelationType — describes the semantic relationship in a causal edge
// ---------------------------------------------------------------------------

/// Semantic relationship type for a causal edge between two blobs.
///
/// Stored as a single byte discriminant within [`CausalEdge`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RelationType {
    /// Child was produced by processing the parent (default).
    Derived = 0,
    /// Child is semantically similar to the parent.
    Similar = 1,
    /// Child contradicts or invalidates the parent.
    Contradicts = 2,
    /// Child provides supporting evidence for the parent.
    Supports = 3,
    /// Child replaces the parent entirely.
    Supersedes = 4,
}

impl RelationType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => Self::Similar,
            2 => Self::Contradicts,
            3 => Self::Supports,
            4 => Self::Supersedes,
            _ => Self::Derived,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Derived => "derived",
            Self::Similar => "similar",
            Self::Contradicts => "contradicts",
            Self::Supports => "supports",
            Self::Supersedes => "supersedes",
        }
    }
}

impl fmt::Debug for RelationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelationType({})", self.label())
    }
}

// ---------------------------------------------------------------------------
// CausalEdge — 80-byte fixed-width edge metadata in causal graph
// ---------------------------------------------------------------------------

/// Rich metadata for a causal edge between two blobs, stored as the value
/// in the `blob_causal_edges` system table.
///
/// Layout (80 bytes):
/// - `child` (`BlobId`, 16 bytes): the child blob in this edge
/// - `relation` (`u8`, 1 byte): [`RelationType`] discriminant
/// - `context_len` (`u8`, 1 byte): length of the context string (max 62)
/// - `context` (`[u8; 62]`): UTF-8 description of why this edge exists
#[derive(Clone)]
pub struct CausalEdge {
    /// The child blob in this directed edge.
    pub child: BlobId,
    /// Semantic relationship from parent to child.
    pub relation: RelationType,
    /// Length of the context string (0–62).
    pub context_len: u8,
    /// UTF-8 context describing the reason for this causal link.
    pub context: [u8; 62],
}

impl CausalEdge {
    pub const SERIALIZED_SIZE: usize = BlobId::SERIALIZED_SIZE + 1 + 1 + 62; // 80

    pub fn new(child: BlobId, relation: RelationType, context: &str) -> Self {
        let ctx_bytes = context.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let context_len = ctx_bytes.len().min(62) as u8;
        let mut context_buf = [0u8; 62];
        context_buf[..context_len as usize].copy_from_slice(&ctx_bytes[..context_len as usize]);
        Self {
            child,
            relation,
            context_len,
            context: context_buf,
        }
    }

    /// Synthesize a legacy edge with [`RelationType::Derived`] and empty context.
    pub fn legacy(child: BlobId) -> Self {
        Self::new(child, RelationType::Derived, "")
    }

    pub fn context_str(&self) -> &str {
        std::str::from_utf8(&self.context[..self.context_len as usize]).unwrap_or("")
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        let mut pos = 0;
        buf[pos..pos + BlobId::SERIALIZED_SIZE].copy_from_slice(&self.child.to_le_bytes());
        pos += BlobId::SERIALIZED_SIZE;
        buf[pos] = self.relation.as_byte();
        pos += 1;
        buf[pos] = self.context_len;
        pos += 1;
        buf[pos..pos + 62].copy_from_slice(&self.context);
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let mut pos = 0;
        let child =
            BlobId::from_le_bytes(data[pos..pos + BlobId::SERIALIZED_SIZE].try_into().unwrap());
        pos += BlobId::SERIALIZED_SIZE;
        let relation = RelationType::from_byte(data[pos]);
        pos += 1;
        let context_len = data[pos];
        pos += 1;
        let mut context = [0u8; 62];
        context.copy_from_slice(&data[pos..pos + 62]);
        Self {
            child,
            relation,
            context_len,
            context,
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for CausalEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CausalEdge")
            .field("child", &self.child)
            .field("relation", &self.relation)
            .field("context_len", &self.context_len)
            .field("context", &self.context_str())
            .finish()
    }
}

impl Value for CausalEdge {
    type SelfType<'a>
        = CausalEdge
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; CausalEdge::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::CausalEdge")
    }
}

// ---------------------------------------------------------------------------
// CausalLink — API input for creating causal edges
// ---------------------------------------------------------------------------

/// User-facing input for specifying a causal link when storing a blob.
///
/// This is the API-level struct that replaces `Option<BlobId>` in write methods.
/// It carries the parent reference along with rich edge metadata.
#[derive(Clone, Debug)]
pub struct CausalLink {
    /// The parent blob that this new blob is causally linked to.
    pub parent: BlobId,
    /// Semantic relationship from parent to child.
    pub relation: RelationType,
    /// Human-readable context describing why this link exists (truncated to 62 bytes).
    pub context: String,
}

impl CausalLink {
    pub fn new(parent: BlobId, relation: RelationType, context: &str) -> Self {
        Self {
            parent,
            relation,
            context: context.to_string(),
        }
    }

    /// Shorthand for a [`RelationType::Derived`] link with empty context.
    pub fn derived(parent: BlobId) -> Self {
        Self::new(parent, RelationType::Derived, "")
    }
}

/// A causal path is a sequence of blobs with optional edge metadata.
/// Each element is `(BlobId, Option<CausalEdge>)` where the edge describes
/// the link from the previous node to this one (`None` for the path origin).
pub type CausalPath = Vec<(BlobId, Option<CausalEdge>)>;

// ---------------------------------------------------------------------------
// TagKey — 49-byte composite key for the blob tag index
// ---------------------------------------------------------------------------

/// Composite key for the `BLOB_TAG_INDEX` B-tree.
///
/// Layout (49 bytes):
/// - `tag` (`[u8; 32]`): zero-padded tag string
/// - `tag_len` (`u8`): actual length of the tag (max 32)
/// - `blob_id` (`BlobId`, 16 bytes): the tagged blob
///
/// Ordering: tag bytes (lexicographic) first, then `blob_id`, enabling
/// efficient prefix range scans for "all blobs with tag X".
#[derive(Clone, PartialEq, Eq)]
pub struct TagKey {
    pub tag: [u8; 32],
    pub tag_len: u8,
    pub blob_id: BlobId,
}

impl TagKey {
    pub const SERIALIZED_SIZE: usize = 32 + 1 + BlobId::SERIALIZED_SIZE; // 49

    pub fn new(tag: &str, blob_id: BlobId) -> Self {
        let tag_bytes = tag.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let tag_len = tag_bytes.len().min(32) as u8;
        let mut tag_buf = [0u8; 32];
        tag_buf[..tag_len as usize].copy_from_slice(&tag_bytes[..tag_len as usize]);
        Self {
            tag: tag_buf,
            tag_len,
            blob_id,
        }
    }

    /// Lower bound for scanning all blobs with a given tag.
    pub fn range_start(tag: &str) -> Self {
        Self::new(tag, BlobId::MIN)
    }

    /// Upper bound for scanning all blobs with a given tag.
    pub fn range_end(tag: &str) -> Self {
        Self::new(tag, BlobId::MAX)
    }

    pub fn tag_str(&self) -> &str {
        std::str::from_utf8(&self.tag[..self.tag_len as usize]).unwrap_or("")
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[..32].copy_from_slice(&self.tag);
        buf[32] = self.tag_len;
        buf[33..49].copy_from_slice(&self.blob_id.to_le_bytes());
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let mut tag = [0u8; 32];
        tag.copy_from_slice(&data[..32]);
        let tag_len = data[32];
        let blob_id = BlobId::from_le_bytes(data[33..49].try_into().unwrap());
        Self {
            tag,
            tag_len,
            blob_id,
        }
    }
}

impl PartialOrd for TagKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TagKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag
            .cmp(&other.tag)
            .then(self.tag_len.cmp(&other.tag_len))
            .then(self.blob_id.cmp(&other.blob_id))
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for TagKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TagKey")
            .field("tag", &self.tag_str())
            .field("blob_id", &self.blob_id)
            .finish()
    }
}

impl Value for TagKey {
    type SelfType<'a>
        = TagKey
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; TagKey::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::TagKey")
    }
}

impl Key for TagKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a = Self::from_bytes(data1);
        let b = Self::from_bytes(data2);
        a.cmp(&b)
    }
}

// ---------------------------------------------------------------------------
// NamespaceKey — 80-byte composite key for the namespace index
// ---------------------------------------------------------------------------

/// Composite key for the `BLOB_NAMESPACE_INDEX` B-tree.
///
/// Layout (80 bytes):
/// - `namespace` (`[u8; 63]`): zero-padded namespace string
/// - `ns_len` (`u8`): actual length (max 63)
/// - `blob_id` (`BlobId`, 16 bytes): the blob in this namespace
///
/// Ordering: namespace bytes first (lexicographic), then `blob_id`.
#[derive(Clone, PartialEq, Eq)]
pub struct NamespaceKey {
    pub namespace: [u8; 63],
    pub ns_len: u8,
    pub blob_id: BlobId,
}

impl NamespaceKey {
    pub const SERIALIZED_SIZE: usize = 63 + 1 + BlobId::SERIALIZED_SIZE; // 80

    pub fn new(namespace: &str, blob_id: BlobId) -> Self {
        let ns_bytes = namespace.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let ns_len = ns_bytes.len().min(63) as u8;
        let mut ns_buf = [0u8; 63];
        ns_buf[..ns_len as usize].copy_from_slice(&ns_bytes[..ns_len as usize]);
        Self {
            namespace: ns_buf,
            ns_len,
            blob_id,
        }
    }

    pub fn range_start(namespace: &str) -> Self {
        Self::new(namespace, BlobId::MIN)
    }

    pub fn range_end(namespace: &str) -> Self {
        Self::new(namespace, BlobId::MAX)
    }

    pub fn namespace_str(&self) -> &str {
        std::str::from_utf8(&self.namespace[..self.ns_len as usize]).unwrap_or("")
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[..63].copy_from_slice(&self.namespace);
        buf[63] = self.ns_len;
        buf[64..80].copy_from_slice(&self.blob_id.to_le_bytes());
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let mut namespace = [0u8; 63];
        namespace.copy_from_slice(&data[..63]);
        let ns_len = data[63];
        let blob_id = BlobId::from_le_bytes(data[64..80].try_into().unwrap());
        Self {
            namespace,
            ns_len,
            blob_id,
        }
    }
}

impl PartialOrd for NamespaceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NamespaceKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.namespace
            .cmp(&other.namespace)
            .then(self.ns_len.cmp(&other.ns_len))
            .then(self.blob_id.cmp(&other.blob_id))
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for NamespaceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceKey")
            .field("namespace", &self.namespace_str())
            .field("blob_id", &self.blob_id)
            .finish()
    }
}

impl Value for NamespaceKey {
    type SelfType<'a>
        = NamespaceKey
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; NamespaceKey::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::NamespaceKey")
    }
}

impl Key for NamespaceKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a = Self::from_bytes(data1);
        let b = Self::from_bytes(data2);
        a.cmp(&b)
    }
}

// ---------------------------------------------------------------------------
// NamespaceVal — 64-byte value storing namespace for a blob
// ---------------------------------------------------------------------------

/// Namespace value stored per blob in the `BLOB_NAMESPACE` table.
///
/// Layout (64 bytes):
/// - `ns_len` (`u8`): actual length (max 63)
/// - `namespace` (`[u8; 63]`): zero-padded namespace string
#[derive(Clone, PartialEq, Eq)]
pub struct NamespaceVal {
    pub ns_len: u8,
    pub namespace: [u8; 63],
}

impl NamespaceVal {
    pub const SERIALIZED_SIZE: usize = 1 + 63; // 64

    pub fn new(namespace: &str) -> Self {
        let ns_bytes = namespace.as_bytes();
        #[allow(clippy::cast_possible_truncation)]
        let ns_len = ns_bytes.len().min(63) as u8;
        let mut ns_buf = [0u8; 63];
        ns_buf[..ns_len as usize].copy_from_slice(&ns_bytes[..ns_len as usize]);
        Self {
            ns_len,
            namespace: ns_buf,
        }
    }

    pub fn namespace_str(&self) -> &str {
        std::str::from_utf8(&self.namespace[..self.ns_len as usize]).unwrap_or("")
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[0] = self.ns_len;
        buf[1..64].copy_from_slice(&self.namespace);
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let ns_len = data[0];
        let mut namespace = [0u8; 63];
        namespace.copy_from_slice(&data[1..64]);
        Self { ns_len, namespace }
    }
}

impl fmt::Debug for NamespaceVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NamespaceVal(\"{}\")", self.namespace_str())
    }
}

impl Value for NamespaceVal {
    type SelfType<'a>
        = NamespaceVal
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; NamespaceVal::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::NamespaceVal")
    }
}

// ---------------------------------------------------------------------------
// StoreOptions — API input for blob storage with tags and namespace
// ---------------------------------------------------------------------------

/// Options for storing a blob, grouping causal link, tags, and namespace.
///
/// Used by [`WriteTransaction::store_blob`] and [`WriteTransaction::blob_writer`].
///
/// # Example
/// ```rust,ignore
/// let opts = StoreOptions {
///     causal_link: Some(CausalLink::derived(parent_id)),
///     namespace: Some("agent-session-42".into()),
///     tags: vec!["lidar".into(), "outdoor".into()],
/// };
/// write_txn.store_blob(data, ContentType::PointCloudLas, "scan", opts)?;
/// ```
#[derive(Clone, Debug, Default)]
pub struct StoreOptions {
    /// Optional causal link to a parent blob.
    pub causal_link: Option<CausalLink>,
    /// Optional namespace for session/domain isolation (max 63 bytes).
    pub namespace: Option<String>,
    /// Tags for categorization (max 8 tags, each max 32 bytes).
    pub tags: Vec<String>,
}

impl StoreOptions {
    pub fn with_causal_link(link: CausalLink) -> Self {
        Self {
            causal_link: Some(link),
            ..Default::default()
        }
    }

    pub fn with_namespace(namespace: &str) -> Self {
        Self {
            namespace: Some(namespace.to_string()),
            ..Default::default()
        }
    }

    pub fn with_tags(tags: &[&str]) -> Self {
        Self {
            tags: tags.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        }
    }
}

/// Maximum number of tags per blob.
pub const MAX_TAGS_PER_BLOB: usize = 8;

// ---------------------------------------------------------------------------
// BlobInput — convenience struct for batch blob storage
// ---------------------------------------------------------------------------

/// Input for `store_blob_batch`. Groups blob data with its metadata.
pub struct BlobInput<'a> {
    pub data: &'a [u8],
    pub content_type: ContentType,
    pub label: &'a str,
    pub opts: StoreOptions,
}

// ---------------------------------------------------------------------------
// TemporalKey — 32-byte composite key for temporal blob indexing
// ---------------------------------------------------------------------------

/// Composite key for the temporal index B-tree.
///
/// Layout (32 bytes):
/// - `wall_clock_ns` (u64): nanosecond wall-clock timestamp for human-readable range queries
/// - `hlc` (HybridLogicalClock/u64): causal ordering within the same wall-clock window
/// - `blob_id` (BlobId/16 bytes): the blob this entry refers to
///
/// The B-tree ordering is `(wall_clock_ns, hlc, blob_id)` which means
/// temporal range scans ("everything between T1 and T2") are a single
/// sequential B-tree scan with no post-filtering.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TemporalKey {
    pub wall_clock_ns: u64,
    pub hlc: HybridLogicalClock,
    pub blob_id: BlobId,
}

impl TemporalKey {
    pub const SERIALIZED_SIZE: usize = 8 + 8 + BlobId::SERIALIZED_SIZE; // 32

    pub fn new(wall_clock_ns: u64, hlc: HybridLogicalClock, blob_id: BlobId) -> Self {
        Self {
            wall_clock_ns,
            hlc,
            blob_id,
        }
    }

    /// Lower bound for a temporal range query starting at `start_ns`.
    pub fn range_start(start_ns: u64) -> Self {
        Self {
            wall_clock_ns: start_ns,
            hlc: HybridLogicalClock::MIN,
            blob_id: BlobId::MIN,
        }
    }

    /// Upper bound for a temporal range query ending at `end_ns` (inclusive).
    pub fn range_end(end_ns: u64) -> Self {
        Self {
            wall_clock_ns: end_ns,
            hlc: HybridLogicalClock::MAX,
            blob_id: BlobId::MAX,
        }
    }

    pub fn to_le_bytes(self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut buf = [0u8; Self::SERIALIZED_SIZE];
        buf[..8].copy_from_slice(&self.wall_clock_ns.to_le_bytes());
        buf[8..16].copy_from_slice(&self.hlc.to_raw().to_le_bytes());
        buf[16..32].copy_from_slice(&self.blob_id.to_le_bytes());
        buf
    }

    pub fn from_le_bytes(data: [u8; Self::SERIALIZED_SIZE]) -> Self {
        let wall_clock_ns = u64::from_le_bytes(data[..8].try_into().unwrap());
        let hlc_raw = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let blob_id = BlobId::from_le_bytes(data[16..32].try_into().unwrap());
        Self {
            wall_clock_ns,
            hlc: HybridLogicalClock::from_raw(hlc_raw),
            blob_id,
        }
    }
}

impl PartialOrd for TemporalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TemporalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wall_clock_ns
            .cmp(&other.wall_clock_ns)
            .then(self.hlc.cmp(&other.hlc))
            .then(self.blob_id.cmp(&other.blob_id))
    }
}

impl fmt::Debug for TemporalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TemporalKey")
            .field("wall_clock_ns", &self.wall_clock_ns)
            .field("hlc", &self.hlc)
            .field("blob_id", &self.blob_id)
            .finish()
    }
}

impl Value for TemporalKey {
    type SelfType<'a>
        = TemporalKey
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; TemporalKey::SERIALIZED_SIZE]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(Self::SERIALIZED_SIZE)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::from_le_bytes(data[..Self::SERIALIZED_SIZE].try_into().unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::blob::TemporalKey")
    }
}

impl Key for TemporalKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let a = Self::from_bytes(data1);
        let b = Self::from_bytes(data2);
        a.cmp(&b)
    }
}

// ---------------------------------------------------------------------------
// Compile-time size assertions
// ---------------------------------------------------------------------------

const _: () = {
    assert!(size_of::<u64>() * 2 == BlobId::SERIALIZED_SIZE);
    assert!(BlobRef::SERIALIZED_SIZE == 40);
    assert!(BlobMeta::SERIALIZED_SIZE == 137);
    assert!(TemporalKey::SERIALIZED_SIZE == 32);
    assert!(CausalEdge::SERIALIZED_SIZE == 80);
    assert!(TagKey::SERIALIZED_SIZE == 49);
    assert!(NamespaceKey::SERIALIZED_SIZE == 80);
    assert!(NamespaceVal::SERIALIZED_SIZE == 64);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_id_roundtrip() {
        let id = BlobId::new(42, 0xDEAD_BEEF_CAFE_BABE);
        let bytes = id.to_le_bytes();
        let recovered = BlobId::from_le_bytes(bytes);
        assert_eq!(id, recovered);
    }

    #[test]
    fn blob_id_ordering() {
        let a = BlobId::new(1, 100);
        let b = BlobId::new(2, 50);
        let c = BlobId::new(1, 200);
        assert!(a < b); // sequence takes priority
        assert!(a < c); // same sequence, hash tiebreaker
    }

    #[test]
    fn blob_id_key_trait() {
        let a = BlobId::new(1, 100);
        let b = BlobId::new(2, 50);
        let a_bytes = BlobId::as_bytes(&a);
        let b_bytes = BlobId::as_bytes(&b);
        assert_eq!(
            BlobId::compare(a_bytes.as_ref(), b_bytes.as_ref()),
            Ordering::Less
        );
    }

    #[test]
    fn blob_ref_roundtrip() {
        let r = BlobRef {
            offset: 1024,
            length: 65536,
            checksum: 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210,
            ref_count: 1,
            content_type: ContentType::ImagePng.as_byte(),
            compression: 0,
        };
        let bytes = r.to_le_bytes();
        let recovered = BlobRef::from_le_bytes(bytes);
        assert_eq!(r, recovered);
    }

    #[test]
    fn blob_meta_roundtrip() {
        let parent = BlobId::new(10, 0xFF);
        let meta = BlobMeta::new(
            BlobRef {
                offset: 0,
                length: 1000,
                checksum: 12345,
                ref_count: 1,
                content_type: ContentType::AudioWav.as_byte(),
                compression: 0,
            },
            1_700_000_000_000_000_000, // wall clock ns
            0x0001_0000_0000_0042,     // hlc
            Some(parent),
            "test-audio",
        );
        let bytes = meta.to_le_bytes();
        let recovered = BlobMeta::from_le_bytes(bytes);
        assert_eq!(recovered.blob_ref, meta.blob_ref);
        assert_eq!(recovered.wall_clock_ns, meta.wall_clock_ns);
        assert_eq!(recovered.hlc, meta.hlc);
        assert_eq!(recovered.causal_parent, Some(parent));
        assert_eq!(recovered.label_str(), "test-audio");
    }

    #[test]
    fn blob_meta_no_parent_roundtrip() {
        let meta = BlobMeta::new(
            BlobRef {
                offset: 0,
                length: 100,
                checksum: 0,
                ref_count: 1,
                content_type: ContentType::OctetStream.as_byte(),
                compression: 0,
            },
            0,
            0,
            None,
            "",
        );
        let bytes = meta.to_le_bytes();
        let recovered = BlobMeta::from_le_bytes(bytes);
        assert!(recovered.causal_parent.is_none());
        assert_eq!(recovered.label_str(), "");
    }

    #[test]
    fn blob_meta_label_truncation() {
        let long_label = "a".repeat(100);
        let meta = BlobMeta::new(
            BlobRef {
                offset: 0,
                length: 0,
                checksum: 0,
                ref_count: 1,
                content_type: 0,
                compression: 0,
            },
            0,
            0,
            None,
            &long_label,
        );
        assert_eq!(meta.label_len, 63);
        assert_eq!(meta.label_str().len(), 63);
    }

    #[test]
    fn content_type_roundtrip() {
        for b in 0..=9 {
            let ct = ContentType::from_byte(b);
            assert_eq!(ct.as_byte(), b);
        }
        // Unknown maps to OctetStream
        assert_eq!(ContentType::from_byte(255), ContentType::OctetStream);
    }

    #[test]
    fn temporal_key_roundtrip() {
        let tk = TemporalKey::new(
            1_700_000_000_000_000_000,
            crate::temporal::HybridLogicalClock::from_parts(1_700_000_000_000, 42),
            BlobId::new(7, 0xCAFE),
        );
        let bytes = tk.to_le_bytes();
        let recovered = TemporalKey::from_le_bytes(bytes);
        assert_eq!(tk, recovered);
    }

    #[test]
    fn temporal_key_ordering_by_wall_clock() {
        let hlc = crate::temporal::HybridLogicalClock::from_parts(100, 0);
        let blob = BlobId::new(1, 0);
        let a = TemporalKey::new(1000, hlc, blob);
        let b = TemporalKey::new(2000, hlc, blob);
        assert!(a < b);
    }

    #[test]
    fn temporal_key_ordering_by_hlc_within_same_wall_clock() {
        let blob = BlobId::new(1, 0);
        let a = TemporalKey::new(
            1000,
            crate::temporal::HybridLogicalClock::from_parts(100, 0),
            blob,
        );
        let b = TemporalKey::new(
            1000,
            crate::temporal::HybridLogicalClock::from_parts(100, 1),
            blob,
        );
        assert!(a < b);
    }

    #[test]
    fn temporal_key_range_bounds() {
        let start = TemporalKey::range_start(1000);
        let end = TemporalKey::range_end(2000);
        let mid = TemporalKey::new(
            1500,
            crate::temporal::HybridLogicalClock::from_parts(150, 5),
            BlobId::new(99, 0),
        );
        assert!(start <= mid);
        assert!(mid <= end);
    }

    #[test]
    fn relation_type_roundtrip() {
        for b in 0..=4 {
            let rt = RelationType::from_byte(b);
            assert_eq!(rt.as_byte(), b);
        }
        // Unknown maps to Derived
        assert_eq!(RelationType::from_byte(255), RelationType::Derived);
    }

    #[test]
    fn causal_edge_roundtrip() {
        let child = BlobId::new(42, 0xCAFE);
        let edge = CausalEdge::new(child, RelationType::Contradicts, "inference conflict");
        let bytes = edge.to_le_bytes();
        let recovered = CausalEdge::from_le_bytes(bytes);
        assert_eq!(recovered.child, child);
        assert_eq!(recovered.relation, RelationType::Contradicts);
        assert_eq!(recovered.context_str(), "inference conflict");
    }

    #[test]
    fn causal_edge_empty_context() {
        let child = BlobId::new(1, 0);
        let edge = CausalEdge::new(child, RelationType::Derived, "");
        assert_eq!(edge.context_len, 0);
        assert_eq!(edge.context_str(), "");
        let bytes = edge.to_le_bytes();
        let recovered = CausalEdge::from_le_bytes(bytes);
        assert_eq!(recovered.context_str(), "");
    }

    #[test]
    fn causal_edge_context_truncation() {
        let child = BlobId::new(1, 0);
        let long_ctx = "x".repeat(200);
        let edge = CausalEdge::new(child, RelationType::Supports, &long_ctx);
        assert_eq!(edge.context_len, 62);
        assert_eq!(edge.context_str().len(), 62);
    }

    #[test]
    fn causal_edge_legacy() {
        let child = BlobId::new(99, 0xFF);
        let edge = CausalEdge::legacy(child);
        assert_eq!(edge.relation, RelationType::Derived);
        assert_eq!(edge.context_str(), "");
        assert_eq!(edge.child, child);
    }

    #[test]
    fn causal_edge_value_trait() {
        let edge = CausalEdge::new(BlobId::new(1, 2), RelationType::Similar, "test");
        let bytes = CausalEdge::as_bytes(&edge);
        let recovered = CausalEdge::from_bytes(bytes.as_ref());
        assert_eq!(recovered.child, edge.child);
        assert_eq!(recovered.relation, edge.relation);
        assert_eq!(recovered.context_str(), edge.context_str());
    }

    #[test]
    fn tag_key_roundtrip() {
        let tk = TagKey::new("lidar", BlobId::new(42, 0xCAFE));
        let bytes = tk.to_le_bytes();
        let recovered = TagKey::from_le_bytes(bytes);
        assert_eq!(recovered.tag_str(), "lidar");
        assert_eq!(recovered.blob_id, BlobId::new(42, 0xCAFE));
    }

    #[test]
    fn tag_key_ordering() {
        let a = TagKey::new("alpha", BlobId::new(1, 0));
        let b = TagKey::new("beta", BlobId::new(1, 0));
        let c = TagKey::new("alpha", BlobId::new(2, 0));
        assert!(a < b); // tag first
        assert!(a < c); // same tag, blob_id tiebreaker
    }

    #[test]
    fn tag_key_truncation() {
        let long_tag = "x".repeat(100);
        let tk = TagKey::new(&long_tag, BlobId::new(1, 0));
        assert_eq!(tk.tag_len, 32);
        assert_eq!(tk.tag_str().len(), 32);
    }

    #[test]
    fn namespace_key_roundtrip() {
        let nk = NamespaceKey::new("agent-session-42", BlobId::new(7, 0xFF));
        let bytes = nk.to_le_bytes();
        let recovered = NamespaceKey::from_le_bytes(bytes);
        assert_eq!(recovered.namespace_str(), "agent-session-42");
        assert_eq!(recovered.blob_id, BlobId::new(7, 0xFF));
    }

    #[test]
    fn namespace_val_roundtrip() {
        let nv = NamespaceVal::new("my-namespace");
        let bytes = nv.to_le_bytes();
        let recovered = NamespaceVal::from_le_bytes(bytes);
        assert_eq!(recovered.namespace_str(), "my-namespace");
    }

    #[test]
    fn store_options_default() {
        let opts = StoreOptions::default();
        assert!(opts.causal_link.is_none());
        assert!(opts.namespace.is_none());
        assert!(opts.tags.is_empty());
    }

    #[test]
    fn temporal_key_key_trait() {
        let a = TemporalKey::new(
            100,
            crate::temporal::HybridLogicalClock::from_parts(10, 0),
            BlobId::new(1, 0),
        );
        let b = TemporalKey::new(
            200,
            crate::temporal::HybridLogicalClock::from_parts(20, 0),
            BlobId::new(2, 0),
        );
        let a_bytes = TemporalKey::as_bytes(&a);
        let b_bytes = TemporalKey::as_bytes(&b);
        assert_eq!(
            TemporalKey::compare(a_bytes.as_ref(), b_bytes.as_ref()),
            Ordering::Less
        );
    }
}
