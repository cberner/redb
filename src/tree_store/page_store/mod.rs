mod page_allocator;
mod page_manager;
mod utils;

pub(in crate) use page_manager::PageNumber;

pub(in crate::tree_store) use page_manager::{Page, PageImpl, PageMut, TransactionalMemory};
