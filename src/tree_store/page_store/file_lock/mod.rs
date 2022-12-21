#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::LockedFile;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::LockedFile;
