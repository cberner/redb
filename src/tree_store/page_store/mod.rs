mod base;
mod buddy_allocator;
mod grouped_bitmap;
mod mmap;
mod page_allocator;
mod page_manager;
mod utils;

pub(crate) use base::PageNumber;
pub(crate) use page_manager::{get_db_size, TransactionalMemory};

pub(in crate::tree_store) use base::{Page, PageImpl, PageMut};
