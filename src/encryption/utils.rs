//! Utility functions for encryption operations

use crate::DatabaseError;
use super::{SALT_SIZE, AES_KEY_SIZE, PBKDF2_ITERATIONS};
use getrandom::getrandom;
use pbkdf2::pbkdf2;
use hmac::Hmac;
use sha2::Sha256;

pub fn generate_salt() -> Result<[u8; SALT_SIZE], DatabaseError> {
    let mut salt = [0u8; SALT_SIZE];
    getrandom(&mut salt)
        .map_err(|_e| DatabaseError::SaltGenerationFailed)?;
    Ok(salt)
}

pub fn derive_key(password: &[u8], salt: &[u8; SALT_SIZE]) -> Result<[u8; AES_KEY_SIZE], DatabaseError> {
    let mut key = [0u8; AES_KEY_SIZE];

    pbkdf2::<Hmac<Sha256>>(password, salt, PBKDF2_ITERATIONS, &mut key)
        .map_err(|_| DatabaseError::KeyDerivationFailed)?;

    Ok(key)
}

pub fn generate_nonce() -> Result<[u8; super::AES_NONCE_SIZE], DatabaseError> {
    let mut nonce = [0u8; super::AES_NONCE_SIZE];
    getrandom(&mut nonce)
        .map_err(|_| DatabaseError::EncryptionFailed("Failed to generate nonce".to_string()))?;
    Ok(nonce)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_salt_generation() {
        let salt1 = generate_salt().unwrap();
        let salt2 = generate_salt().unwrap();
        assert_ne!(salt1, salt2); // Should be unique
        assert_eq!(salt1.len(), SALT_SIZE);
        assert_eq!(salt2.len(), SALT_SIZE);
    }

    #[test]
    fn test_key_derivation_consistency() {
        let password = b"test_password";
        let salt = [0u8; SALT_SIZE];
        let key1 = derive_key(password, &salt).unwrap();
        let key2 = derive_key(password, &salt).unwrap();
        assert_eq!(key1, key2); // Same inputs -> same output
        assert_eq!(key1.len(), AES_KEY_SIZE);
    }

    #[test]
    fn test_key_derivation_different_salts() {
        let password = b"test_password";
        let salt1 = [0u8; SALT_SIZE];
        let salt2 = [1u8; SALT_SIZE];
        let key1 = derive_key(password, &salt1).unwrap();
        let key2 = derive_key(password, &salt2).unwrap();
        assert_ne!(key1, key2); // Different salts -> different keys
    }

    #[test]
    fn test_nonce_generation() {
        let nonce1 = generate_nonce().unwrap();
        let nonce2 = generate_nonce().unwrap();
        assert_ne!(nonce1, nonce2); // Should be unique
        assert_eq!(nonce1.len(), crate::encryption::AES_NONCE_SIZE);
        assert_eq!(nonce2.len(), crate::encryption::AES_NONCE_SIZE);
    }
}
