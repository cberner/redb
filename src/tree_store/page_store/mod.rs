#[cfg(feature = "experimental-multiprocess")]
mod active_transactions;
mod backends;
mod base;
mod bitmap;
mod buddy_allocator;
mod cached_file;
mod fast_hash;
#[cfg(not(redb_no_std))]
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
pub(crate) use backends::LocklessBackend;
#[cfg(not(redb_no_std))]
pub(crate) use backends::ReadOnlyBackend;
pub(crate) use base::{MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, Page, PageHint, PageNumber, PageTracker};
pub(crate) use fast_hash::{PageNumberHashMap, PageNumberHashSet};
pub(crate) use header::PAGE_SIZE;
#[cfg(feature = "experimental-multiprocess")]
pub(crate) use page_manager::HeaderGuard;
#[cfg(feature = "experimental-multiprocess")]
pub(crate) use page_manager::MultiProcessWriterGuard;
pub(crate) use page_manager::{
    AccessMode, AllocationPolicy, FILE_FORMAT_VERSION3, PageAllocator, PageResolver, ShrinkPolicy,
    TransactionalMemory, xxh3_checksum,
};
pub use savepoint::Savepoint;
pub(crate) use savepoint::SerializedSavepoint;

pub(super) use base::{PageImpl, PageMut};
pub(super) use xxh3::hash128_with_seed;
