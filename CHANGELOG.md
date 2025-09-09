# redbx - Changelog

## 3.0.1 - 2025-09-10

### Major Transformation: redb → redbx
Complete transformation of redb to redbx with built-in AES-256-GCM encryption for all user data.

### 🔐 Encryption Features
* **Built-in AES-256-GCM encryption** - Transparent encryption for all user data at rest
* **PBKDF2-SHA256 key derivation** - 100,000 iterations for secure key generation
* **Selective encryption architecture** - Encrypts user data while keeping metadata unencrypted for performance
* **Password-based database creation** - `Database::create(path, password)` and `Database::open(path, password)`
* **Transparent encryption/decryption** - No changes required to existing redb code patterns

### 🏗️ Architecture Changes
* **Encrypted storage backend** - New `EncryptedFileBackend` for transparent data encryption
* **Selective encryption** - LEAF pages and system table contents encrypted, BRANCH pages and metadata remain unencrypted
* **Key management system** - Secure key derivation and management with PBKDF2
* **Encryption utilities** - Comprehensive encryption/decryption utilities with proper error handling

### 📦 Package Restructure
* **Project rename** - Complete migration from `redb` to `redbx` namespace
* **Crate reorganization** - Updated all sub-crates: `redbx-derive`, `redbx-python`, `redbx-bench`
* **Benchmark improvements** - New encryption overhead benchmarks in `redbx-bench`
* **Documentation** - New design document explaining encryption architecture

### 🔧 API Enhancements
* **Password-required constructors** - All database creation/opening now requires encryption password
* **Backward compatibility** - Maintains existing redb API patterns with encryption layer
* **Error handling** - Enhanced error types for encryption-specific failures
* **Performance optimization** - Selective encryption minimizes performance impact

### 🧪 Testing & Quality
* **Comprehensive encryption tests** - New test suite validating encryption functionality
* **Security validation** - Tests ensure proper encryption/decryption of user data
* **Performance benchmarking** - Encryption overhead measurement and optimization
* **Integration testing** - All existing redb functionality verified with encryption

### 📚 Documentation
* **Complete README update** - New examples showing encrypted database usage
* **Design documentation** - Detailed encryption architecture and security model
* **API documentation** - Updated examples and usage patterns for encrypted databases

### 🎯 Key Benefits
* **Zero-trust security** - All user data encrypted at rest with strong cryptography
* **Performance optimized** - Selective encryption maintains high performance
* **Drop-in replacement** - Minimal code changes required from redb
* **Production ready** - Built on proven redb foundation with added security

### 🔄 Migration from redb
```rust
// Before (redb)
let db = redb::Database::create("my_db.redb")?;

// After (redbx) 
let db = redbx::Database::create("my_db.redbx", "my_secure_password")?;
```

---

**Note**: This version maintains full compatibility with redb 3.0.1 features while adding comprehensive encryption support. All redb functionality remains available with the added security of AES-256-GCM encryption for user data.