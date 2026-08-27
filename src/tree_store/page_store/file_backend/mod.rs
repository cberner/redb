// On Windows the ordinary open modes rely on std's whole-file lock, which already spans
// every offset a range lock could, so range locks exist there only for the multi-process
// protocol
#[cfg(any(
    target_os = "linux",
    target_vendor = "apple",
    all(windows, redb_multiprocess)
))]
pub(crate) mod range_lock;

#[cfg(any(windows, unix, target_os = "wasi"))]
mod optimized;
#[cfg(any(windows, unix, target_os = "wasi"))]
pub use optimized::FileBackend;

#[cfg(not(any(windows, unix, target_os = "wasi")))]
mod fallback;
#[cfg(not(any(windows, unix, target_os = "wasi")))]
pub use fallback::FileBackend;
