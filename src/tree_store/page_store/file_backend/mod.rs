#[cfg(any(windows, unix, target_os = "wasi"))]
mod optimized;
#[cfg(any(windows, unix, target_os = "wasi"))]
pub use optimized::FileBackend;

#[cfg(not(any(windows, unix, target_os = "wasi")))]
mod fallback;
#[cfg(not(any(windows, unix, target_os = "wasi")))]
pub use fallback::FileBackend;
