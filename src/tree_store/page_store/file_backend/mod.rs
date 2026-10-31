#[cfg(any(windows, unix, target_os = "wasi"))]
mod optimized;
#[cfg(any(windows, unix, target_os = "wasi"))]
pub use optimized::FileBackend;

// Beside the backend that uses it: the fallback backend below has no file to lock
#[cfg(any(windows, unix, target_os = "wasi"))]
pub(crate) mod range_lock;

#[cfg(not(any(windows, unix, target_os = "wasi")))]
mod fallback;
#[cfg(not(any(windows, unix, target_os = "wasi")))]
pub use fallback::FileBackend;
