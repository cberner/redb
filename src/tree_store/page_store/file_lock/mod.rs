#[cfg(any(unix, target_os = "wasi"))]
mod unix;
#[cfg(any(unix, target_os = "wasi"))]
pub(super) use unix::LockedFile;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::LockedFile;
