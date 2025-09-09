//! Encryption module for redbx
//! 
//! Provides transparent encryption for user data while keeping metadata unencrypted
//! for performance and debugging purposes.

pub mod cipher;
pub mod key_manager;
pub mod utils;

pub use cipher::PageCipher;
pub use key_manager::KeyManager;
pub use utils::{generate_salt, derive_key, generate_nonce};

use crate::DatabaseError;

/// Encryption configuration constants
pub const AES_KEY_SIZE: usize = 32; // 256 bits
pub const AES_NONCE_SIZE: usize = 12; // 96 bits for GCM
pub const AES_TAG_SIZE: usize = 16; // 128 bits
pub const SALT_SIZE: usize = 16; // 128 bits
pub const PBKDF2_ITERATIONS: u32 = 100_000;

/// Total encryption overhead per page
pub const ENCRYPTION_OVERHEAD: usize = AES_NONCE_SIZE + AES_TAG_SIZE; // 28 bytes

/// Metadata overhead for encrypted pages (page type + nonce)
pub const LEAF_METADATA_SIZE: usize = 1 + AES_NONCE_SIZE; // 13 bytes

/// Effective capacity for LEAF pages after encryption overhead
pub const EFFECTIVE_LEAF_PAGE_SIZE: usize = crate::tree_store::PAGE_SIZE 
    - LEAF_METADATA_SIZE - ENCRYPTION_OVERHEAD; // 4055 bytes

/// Page type constants for encryption decisions
pub const LEAF_PAGE_TYPE: u8 = 1;
pub const BRANCH_PAGE_TYPE: u8 = 2;

/// Determines if a page should be encrypted based on its type
pub fn should_encrypt_page(data: &[u8]) -> bool {
    if data.is_empty() {
        return false;
    }
    
    match data[0] {
        LEAF_PAGE_TYPE => true,      // Encrypt user data pages
        BRANCH_PAGE_TYPE => false,   // Keep navigation pages unencrypted
        _ => false,                  // Keep metadata pages unencrypted
    }
}

/// Secure password wrapper that zeroizes on drop
#[derive(ZeroizeOnDrop)]
pub struct SecurePassword {
    data: Vec<u8>,
}

impl SecurePassword {
    /// Create a new secure password from string
    pub fn new(password: &str) -> Self {
        Self {
            data: password.as_bytes().to_vec(),
        }
    }
    
    /// Derive encryption key from password and salt
    pub fn derive_key(&self, salt: &[u8; SALT_SIZE]) -> Result<[u8; AES_KEY_SIZE], DatabaseError> {
        derive_key(&self.data, salt)
    }
}

use zeroize::ZeroizeOnDrop;