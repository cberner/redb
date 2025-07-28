mod backends;
mod base;
mod bitmap;
mod buddy_allocator;
mod cached_file;
mod fast_hash;
pub mod file_backend;
mod header;
mod layout;
mod lru_cache;
mod page_manager;
mod region;
mod savepoint;
#[allow(clippy::pedantic, dead_code)]
mod xxh3;

pub use backends::InMemoryBackend;
pub(crate) use backends::ReadOnlyBackend;
pub(crate) use base::{
    MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, Page, PageHint, PageNumber, PageTrackerPolicy,
};
pub(crate) use header::PAGE_SIZE;
pub(crate) use page_manager::{
    FILE_FORMAT_VERSION3, ShrinkPolicy, TransactionalMemory, xxh3_checksum,
};
pub use savepoint::Savepoint;
pub(crate) use savepoint::SerializedSavepoint;

pub(super) use base::{PageImpl, PageMut};
pub(super) use xxh3::hash128_with_seed;
