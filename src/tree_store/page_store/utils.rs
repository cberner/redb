#[cfg(unix)]
pub(crate) fn get_page_size() -> usize {
    use libc::{sysconf, _SC_PAGESIZE};

    unsafe { sysconf(_SC_PAGESIZE) as usize }
}

#[cfg(not(unix))]
pub(crate) fn get_page_size() -> usize {
    4096
}
