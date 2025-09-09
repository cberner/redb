//! Key management for redbx encryption

use crate::DatabaseError;
use super::{SecurePassword, SALT_SIZE, AES_KEY_SIZE};
use std::time::Instant;
use zeroize::Zeroize;

#[derive(Debug)]
pub struct KeyManager {
    key: [u8; AES_KEY_SIZE],
    #[allow(dead_code)]
    created_at: Instant,
}

impl KeyManager {
    pub fn new(password: &str, salt: &[u8; SALT_SIZE]) -> Result<Self, DatabaseError> {
        let secure_password = SecurePassword::new(password);
        let key = secure_password.derive_key(salt)?;

        Ok(KeyManager {
            key,
            created_at: Instant::now(),
        })
    }

    pub fn get_key(&self) -> &[u8; AES_KEY_SIZE] {
        &self.key
    }

    #[allow(dead_code)]
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
}

impl Drop for KeyManager {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

#[derive(ZeroizeOnDrop)]
#[allow(dead_code)]
pub struct SecureKey {
    data: [u8; AES_KEY_SIZE],
}

impl SecureKey {
    #[allow(dead_code)]
    pub fn new(key: [u8; AES_KEY_SIZE]) -> Self {
        Self { data: key }
    }

    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8; AES_KEY_SIZE] {
        &self.data
    }
}

use zeroize::ZeroizeOnDrop;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_manager_creation() {
        let password = "test_password";
        let salt = [0u8; SALT_SIZE];
        let key_manager = KeyManager::new(password, &salt).unwrap();

        let key = key_manager.get_key();
        assert_eq!(key.len(), AES_KEY_SIZE);

        assert!(key_manager.age().as_millis() < 100);
    }

    #[test]
    fn test_secure_key() {
        let key_data = [1u8; AES_KEY_SIZE];
        let secure_key = SecureKey::new(key_data);
        assert_eq!(secure_key.as_bytes(), &key_data);
    }

    #[test]
    fn test_key_manager_zeroization() {
        let password = "test_password";
        let salt = [0u8; SALT_SIZE];
        let key_manager = KeyManager::new(password, &salt).unwrap();
        let _key = *key_manager.get_key();

        drop(key_manager);
    }
}
