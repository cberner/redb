//! AES-GCM encryption/decryption implementation

use crate::DatabaseError;
use super::{AES_KEY_SIZE, AES_NONCE_SIZE};
use aes_gcm::{Aes256Gcm, Key, Nonce, aead::{Aead, KeyInit}};

pub struct PageCipher {
    cipher: Aes256Gcm,
}

impl std::fmt::Debug for PageCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCipher")
            .field("cipher", &"<Aes256Gcm>")
            .finish()
    }
}

impl PageCipher {
    pub fn new(key: &[u8; AES_KEY_SIZE]) -> Self {
        let key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(&key);

        Self { cipher }
    }

    pub fn encrypt_with_nonce(&self, nonce: &[u8; AES_NONCE_SIZE], data: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        let nonce = Nonce::from_slice(nonce);

        self.cipher.encrypt(nonce, data)
            .map_err(|e| DatabaseError::EncryptionFailed(format!("AES-GCM encryption failed: {}", e)))
    }

    pub fn decrypt_with_nonce(&self, nonce: &[u8; AES_NONCE_SIZE], ciphertext: &[u8]) -> Result<Vec<u8>, DatabaseError> {
        let nonce = Nonce::from_slice(nonce);

        self.cipher.decrypt(nonce, ciphertext)
            .map_err(|e| DatabaseError::DecryptionFailed(format!("AES-GCM decryption failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_roundtrip() {
        let key = [0u8; AES_KEY_SIZE];
        let cipher = PageCipher::new(&key);
        let data = b"Hello, World!";
        let nonce = [0u8; AES_NONCE_SIZE];

        let encrypted = cipher.encrypt_with_nonce(&nonce, data).unwrap();
        assert!(encrypted.len() > data.len()); // Should include auth tag

        let decrypted = cipher.decrypt_with_nonce(&nonce, &encrypted).unwrap();
        assert_eq!(data.as_slice(), decrypted);
    }

    #[test]
    fn test_encryption_with_specific_nonce() {
        let key = [0u8; AES_KEY_SIZE];
        let cipher = PageCipher::new(&key);
        let data = b"Test data";
        let nonce = [1u8; AES_NONCE_SIZE];

        let encrypted = cipher.encrypt_with_nonce(&nonce, data).unwrap();
        let decrypted = cipher.decrypt_with_nonce(&nonce, &encrypted).unwrap();

        assert_eq!(data.as_slice(), decrypted);
    }

    #[test]
    fn test_different_nonces_produce_different_ciphertext() {
        let key = [0u8; AES_KEY_SIZE];
        let cipher = PageCipher::new(&key);
        let data = b"Same data";
        let nonce1 = [1u8; AES_NONCE_SIZE];
        let nonce2 = [2u8; AES_NONCE_SIZE];

        let encrypted1 = cipher.encrypt_with_nonce(&nonce1, data).unwrap();
        let encrypted2 = cipher.encrypt_with_nonce(&nonce2, data).unwrap();

        assert_ne!(encrypted1, encrypted2);

        let decrypted1 = cipher.decrypt_with_nonce(&nonce1, &encrypted1).unwrap();
        let decrypted2 = cipher.decrypt_with_nonce(&nonce2, &encrypted2).unwrap();

        assert_eq!(decrypted1, decrypted2);
        assert_eq!(decrypted1, data);
    }

    #[test]
    fn test_wrong_nonce_fails() {
        let key = [0u8; AES_KEY_SIZE];
        let cipher = PageCipher::new(&key);
        let data = b"Test data";
        let nonce = [1u8; AES_NONCE_SIZE];

        let encrypted = cipher.encrypt_with_nonce(&nonce, data).unwrap();
        let wrong_nonce = [2u8; AES_NONCE_SIZE];

        let result = cipher.decrypt_with_nonce(&wrong_nonce, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_corrupted_ciphertext_fails() {
        let key = [0u8; AES_KEY_SIZE];
        let cipher = PageCipher::new(&key);
        let data = b"Test data";
        let nonce = [1u8; AES_NONCE_SIZE];

        let mut encrypted = cipher.encrypt_with_nonce(&nonce, data).unwrap();
        // Corrupt the ciphertext
        encrypted[5] ^= 0xFF;

        let result = cipher.decrypt_with_nonce(&nonce, &encrypted);
        assert!(result.is_err());
    }
}
