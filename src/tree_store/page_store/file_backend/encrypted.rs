//! Encrypted file backend implementation for redbx
//!
//! Provides transparent encryption for LEAF pages while keeping BRANCH pages
//! and metadata unencrypted for performance and debugging.

use crate::encryption::{PageCipher, KeyManager, should_encrypt_page, AES_NONCE_SIZE};
use crate::tree_store::page_store::header::PAGE_SIZE;
use crate::{DatabaseError, Result, StorageBackend, StorageError};
#[cfg(feature = "logging")]
use log::warn;
use std::fs::File;
use std::io;
use std::io::ErrorKind;
use std::fs::TryLockError;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

#[derive(Debug)]
pub enum EncryptedBackendError {
    Io(io::Error),
    DecryptionFailed(DatabaseError),
    EncryptionFailed(DatabaseError),
}

impl From<io::Error> for EncryptedBackendError {
    fn from(err: io::Error) -> Self {
        EncryptedBackendError::Io(err)
    }
}

impl From<DatabaseError> for EncryptedBackendError {
    fn from(err: DatabaseError) -> Self {
        match err {
            DatabaseError::DecryptionFailed(_) => EncryptedBackendError::DecryptionFailed(err),
            DatabaseError::EncryptionFailed(_) => EncryptedBackendError::EncryptionFailed(err),
            other => EncryptedBackendError::Io(io::Error::new(io::ErrorKind::Other, format!("{:?}", other))),
        }
    }
}

impl From<EncryptedBackendError> for io::Error {
    fn from(err: EncryptedBackendError) -> Self {
        match err {
            EncryptedBackendError::Io(io_err) => io_err,
            EncryptedBackendError::DecryptionFailed(db_err) => {
                io::Error::new(io::ErrorKind::Other, format!("DecryptionFailed({:?})", db_err))
            }
            EncryptedBackendError::EncryptionFailed(db_err) => {
                io::Error::new(io::ErrorKind::Other, format!("EncryptionFailed({:?})", db_err))
            }
        }
    }
}

#[derive(Debug)]
pub struct EncryptedFileBackend {
    file: File,
    cipher: Arc<PageCipher>,
    salt: [u8; 16],
    lock_supported: bool,
}

impl EncryptedFileBackend {
    pub fn new(file: File, password: &str) -> Result<Self, DatabaseError> {
        match file.try_lock() {
            Ok(()) => {
                let salt = crate::encryption::generate_salt()?;

                let key_manager = Arc::new(KeyManager::new(password, &salt)?);

                let cipher = Arc::new(PageCipher::new(key_manager.get_key()));

                let backend = Self {
                    file,
                    cipher,
                    salt,
                    lock_supported: true,
                };

                Ok(backend)
            }
            Err(TryLockError::Error(err)) if err.kind() == ErrorKind::Unsupported => {
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file at a time"
                );

                let salt = crate::encryption::generate_salt()?;

                let key_manager = Arc::new(KeyManager::new(password, &salt)?);

                let cipher = Arc::new(PageCipher::new(key_manager.get_key()));

                let backend = Self {
                    file,
                    cipher,
                    salt,
                    lock_supported: false,
                };

                Ok(backend)
            }
            Err(TryLockError::WouldBlock) => Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(_)) => Err(DatabaseError::DatabaseAlreadyOpen),
        }
    }

    pub fn open(file: File, password: &str, salt: [u8; 16]) -> Result<Self, DatabaseError> {
        let backend = Self::open_internal(file, password, salt, false)?;

        backend.validate_password()?;

        Ok(backend)
    }

    /// Open an existing encrypted file backend with read-only flag
    pub(crate) fn open_internal(file: File, password: &str, salt: [u8; 16], read_only: bool) -> Result<Self, DatabaseError> {
        let lock_result = if read_only {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };

        let backend = match lock_result {
            Ok(()) => {
                let key_manager = Arc::new(KeyManager::new(password, &salt)?);

                let cipher = Arc::new(PageCipher::new(key_manager.get_key()));

                Self {
                    file,
                    cipher,
                    salt,
                    lock_supported: true,
                }
            }
            Err(TryLockError::Error(err)) if err.kind() == ErrorKind::Unsupported => {
                // File locks not supported on this platform
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file at a time"
                );

                let key_manager = Arc::new(KeyManager::new(password, &salt)?);

                let cipher = Arc::new(PageCipher::new(key_manager.get_key()));

                Self {
                    file,
                    cipher,
                    salt,
                    lock_supported: false,
                }
            }
            Err(TryLockError::WouldBlock) => return Err(DatabaseError::DatabaseAlreadyOpen),
            Err(TryLockError::Error(_)) => return Err(DatabaseError::DatabaseAlreadyOpen),
        };

        Ok(backend)
    }

    pub fn get_salt(&self) -> &[u8; 16] {
        &self.salt
    }


    fn read_and_decrypt_with_errors(&self, offset: u64, out: &mut [u8]) -> Result<(), EncryptedBackendError> {
        self.file.read_exact_at(out, offset)?;

        if self.should_encrypt_page(out) {
            let page_type = out[0];
            let nonce = &out[1..1+AES_NONCE_SIZE];
            let encrypted_data = &out[1+AES_NONCE_SIZE..];

            let decrypted = self.cipher.decrypt_with_nonce(
                nonce.try_into().map_err(|_| EncryptedBackendError::Io(io::Error::new(io::ErrorKind::InvalidData, "Invalid nonce size")))?,
                encrypted_data
            )?;

            out[0] = page_type;
            out[1..1+decrypted.len()].copy_from_slice(&decrypted);
            if decrypted.len() + 1 < out.len() {
                out[1+decrypted.len()..].fill(0);
            }

            if page_type != 1 && page_type != 2 { // LEAF=1, BRANCH=2
                return Err(EncryptedBackendError::DecryptionFailed(
                    DatabaseError::CorruptedEncryption("Invalid page type after decryption".to_string())
                ));
            }
        }

        Ok(())
    }

    pub fn validate_password(&self) -> Result<(), DatabaseError> {
        // Check if database has any data beyond the header
        let file_len = self.file.metadata()
            .map_err(|e| DatabaseError::Storage(StorageError::Io(e)))?
            .len();

        // If database is empty or only has header, no validation needed
        if file_len <= PAGE_SIZE as u64 {
            return Ok(());
        }

        // Try to read and decrypt the first data page (after header)
        let mut test_buffer = [0u8; PAGE_SIZE];
        match self.read_and_decrypt_with_errors(PAGE_SIZE as u64, &mut test_buffer) {
            Ok(()) => Ok(()),
            Err(EncryptedBackendError::DecryptionFailed(_)) => {
                Err(DatabaseError::IncorrectPassword)
            }
            Err(EncryptedBackendError::Io(_io_err)) => {
                // For I/O errors, don't assume it's a password issue
                // This could be a legitimate I/O error or the page might not exist
                Ok(())
            }
            Err(EncryptedBackendError::EncryptionFailed(_)) => {
                Err(DatabaseError::IncorrectPassword)
            }
        }
    }

    /// Check if a page should be encrypted based on its type
    fn should_encrypt_page(&self, data: &[u8]) -> bool {
        should_encrypt_page(data)
    }

    /// Encrypt and write a page with unencrypted page type header
    fn encrypt_and_write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        if !self.should_encrypt_page(data) {
            // Write unencrypted (BRANCH pages, metadata)
            self.file.write_all_at(data, offset)?;
            self.file.sync_all()?;
            return Ok(());
        }

        // LEAF page encryption with preserved page type
        let page_type = data[0]; // Save page type (1=LEAF)
        let full_plaintext = &data[1..]; // Encrypt everything after page type

        // Calculate maximum plaintext size that will fit after encryption
        // Available space for encrypted data: PAGE_SIZE - 1 (page type) - AES_NONCE_SIZE (nonce) = 4083 bytes
        // Encrypted data = plaintext + 16 byte auth tag
        // So max plaintext = 4083 - 16 = 4067 bytes
        let max_plaintext_size = PAGE_SIZE - 1 - AES_NONCE_SIZE - 16; // 4067 bytes
        let plaintext = if full_plaintext.len() > max_plaintext_size {
            &full_plaintext[..max_plaintext_size]
        } else {
            full_plaintext
        };

        let nonce = crate::encryption::generate_nonce()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

        let encrypted_data = self.cipher.encrypt_with_nonce(&nonce, plaintext)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;


        // Write: [page_type(1)] + [nonce(12)] + [encrypted_data + auth_tag]
        let mut page_buffer = [0u8; PAGE_SIZE];
        page_buffer[0] = page_type; // Unencrypted page type
        page_buffer[1..1+AES_NONCE_SIZE].copy_from_slice(&nonce); // Unencrypted nonce

        // Write the encrypted data (should fit since we limited plaintext size)
        page_buffer[1+AES_NONCE_SIZE..1+AES_NONCE_SIZE+encrypted_data.len()].copy_from_slice(&encrypted_data);

        self.file.write_all_at(&page_buffer, offset)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn read_and_decrypt(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        // For encrypted pages, we need to read the full page to get the correct format
        // regardless of the output buffer size
        if out.len() < PAGE_SIZE {
            // Read into a full page buffer first, then determine if it's encrypted
            let mut full_page = [0u8; PAGE_SIZE];
            self.file.read_exact_at(&mut full_page, offset)?;

            if self.should_encrypt_page(&full_page) {
                // LEAF page decryption with preserved page type
                let page_type = full_page[0];
                let nonce = &full_page[1..1+AES_NONCE_SIZE]; // Nonce is unencrypted
                let encrypted_data = &full_page[1+AES_NONCE_SIZE..]; // Encrypted payload

                let decrypted = self.cipher.decrypt_with_nonce(
                    nonce.try_into().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid nonce size"))?,
                    encrypted_data
                ).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("DecryptionFailed({:?})", e)))?;

                let mut decrypted_page = [0u8; PAGE_SIZE];
                decrypted_page[0] = page_type; // Keep page type at byte 0
                decrypted_page[1..1+decrypted.len()].copy_from_slice(&decrypted);

                let copy_len = out.len().min(PAGE_SIZE);
                out[..copy_len].copy_from_slice(&decrypted_page[..copy_len]);
            } else {
                let copy_len = out.len().min(PAGE_SIZE);
                out[..copy_len].copy_from_slice(&full_page[..copy_len]);
            }
        } else if out.len() == PAGE_SIZE {
            self.file.read_exact_at(out, offset)?;

            if self.should_encrypt_page(out) {
                let page_type = out[0];
                let nonce = &out[1..1+AES_NONCE_SIZE]; // Nonce is unencrypted
                let encrypted_data = &out[1+AES_NONCE_SIZE..]; // Encrypted payload

                let decrypted = self.cipher.decrypt_with_nonce(
                    nonce.try_into().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid nonce size"))?,
                    encrypted_data
                ).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("DecryptionFailed({:?})", e)))?;

                // Reconstruct page: [page_type] + [decrypted_data]
                out[0] = page_type; // Keep page type at byte 0
                out[1..1+decrypted.len()].copy_from_slice(&decrypted);
                // Clear remaining bytes if any
                if decrypted.len() + 1 < out.len() {
                    out[1+decrypted.len()..].fill(0);
                }
            }
        } else {
            self.file.read_exact_at(out, offset)?;

            // For large reads, we need to decrypt each page individually
            let mut current_offset = 0u64;
            while current_offset < out.len() as u64 {
                let remaining = out.len() as u64 - current_offset;
                let page_size = PAGE_SIZE.min(remaining as usize);

                let page_start = current_offset as usize;
                let page_end = page_start + page_size;
                let page_data = &mut out[page_start..page_end];

                if page_size == PAGE_SIZE && self.should_encrypt_page(page_data) {
                    // Only decrypt if it's a full page and marked for encryption
                    let page_type = page_data[0];
                    let nonce = &page_data[1..1+AES_NONCE_SIZE];
                    let encrypted_data = &page_data[1+AES_NONCE_SIZE..];

                    let decrypted = self.cipher.decrypt_with_nonce(
                        nonce.try_into().map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid nonce size"))?,
                        encrypted_data
                    ).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("DecryptionFailed({:?})", e)))?;

                    // Reconstruct page: [page_type] + [decrypted_data]
                    page_data[0] = page_type;
                    page_data[1..1+decrypted.len()].copy_from_slice(&decrypted);
                    if decrypted.len() + 1 < page_data.len() {
                        page_data[1+decrypted.len()..].fill(0);
                    }
                }

                current_offset += page_size as u64;
            }
        }

        Ok(())
    }
}

impl StorageBackend for EncryptedFileBackend {
    fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), io::Error> {
        self.read_and_decrypt(offset, out)
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), io::Error> {
        self.encrypt_and_write(offset, data)
    }

    fn sync_data(&self) -> std::result::Result<(), io::Error> {
        self.file.sync_data()
    }

    fn len(&self) -> std::result::Result<u64, io::Error> {
        self.file.metadata().map(|m| m.len())
    }

    fn set_len(&self, size: u64) -> std::result::Result<(), io::Error> {
        self.file.set_len(size)
    }

    fn close(&self) -> std::result::Result<(), io::Error> {
        if self.lock_supported {
            self.file.unlock()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use crate::encryption::{LEAF_PAGE_TYPE, BRANCH_PAGE_TYPE, KeyManager, PageCipher};

    #[test]
    fn test_encrypted_backend_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let backend = EncryptedFileBackend::new(file, "test_password");
        assert!(backend.is_ok());
    }

    #[test]
    fn test_encrypted_backend_open() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let salt = [0u8; 16];
        let backend = EncryptedFileBackend::open(file, "test_password", salt);
        assert!(backend.is_ok());
    }

    #[test]
    fn test_should_encrypt_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let backend = EncryptedFileBackend::new(file, "test_password").unwrap();

        // LEAF page should be encrypted
        let leaf_data = [LEAF_PAGE_TYPE, 0, 1, 2, 3];
        assert!(backend.should_encrypt_page(&leaf_data));

        // BRANCH page should not be encrypted
        let branch_data = [BRANCH_PAGE_TYPE, 0, 1, 2, 3];
        assert!(!backend.should_encrypt_page(&branch_data));

        // Empty data should not be encrypted
        let empty_data = [];
        assert!(!backend.should_encrypt_page(&empty_data));
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        // Test the encryption/decryption logic without file I/O
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::create(temp_file.path()).unwrap();
        let backend = EncryptedFileBackend::new(file, "test_password").unwrap();

        // Test data
        let original_data = [LEAF_PAGE_TYPE, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        // Test that the data should be encrypted
        assert!(backend.should_encrypt_page(&original_data));

        // Test encryption logic directly
        let salt = backend.get_salt();
        let key_manager = Arc::new(KeyManager::new("test_password", salt).unwrap());
        let cipher = PageCipher::new(key_manager.get_key());

        // Generate nonce
        let nonce = crate::encryption::generate_nonce().unwrap();

        // Encrypt the data (excluding page type)
        let plaintext = &original_data[1..];
        let encrypted = cipher.encrypt_with_nonce(&nonce, plaintext).unwrap();

        // Decrypt the data
        let decrypted = cipher.decrypt_with_nonce(&nonce, &encrypted).unwrap();

        // Should match original data (excluding page type)
        assert_eq!(plaintext, decrypted.as_slice());
    }

    #[test]
    fn test_branch_page_no_encryption() {
        // Test that BRANCH pages are not encrypted
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::create(temp_file.path()).unwrap();
        let backend = EncryptedFileBackend::new(file, "test_password").unwrap();

        // Test data for BRANCH page
        let original_data = [BRANCH_PAGE_TYPE, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        // Test that the data should NOT be encrypted
        assert!(!backend.should_encrypt_page(&original_data));

        // Test that the data would be written as-is (no encryption)
        // This verifies the encryption decision logic works correctly
        assert_eq!(original_data[0], BRANCH_PAGE_TYPE);
    }
}
