# redbx design

--------------------------------------------------------------------------------------------------

redbx is a simple, portable, high-performance, ACID, embedded key-value store with built-in AES encryption.

redbx is a fork of redb that provides transparent encryption for all user data. Each redbx database contains a collection of encrypted tables, and supports a single writer and multiple concurrent readers. redbx uses MVCC to provide isolation, and provides a single isolation level: serializable, in which all writes are applied sequentially.

Each table is a key-value mapping providing an interface similar to `BTreeMap`, with all user data transparently encrypted at rest.

# Encryption Architecture

## Overview
redbx implements **selective encryption** at the storage backend level. This approach encrypts user data while keeping metadata unencrypted for performance and debugging purposes.

### What Gets Encrypted
- **LEAF pages**: All user key-value data stored in B-tree leaf nodes
- **System table contents**: Internal system tables containing user-related metadata
- **User table definitions**: Names and schemas of user-created tables

### What Remains Unencrypted
- **Database headers**: File metadata, magic numbers, version information
- **BRANCH pages**: B-tree internal nodes containing only navigation metadata
- **Region headers**: Page allocation and management metadata
- **Transaction metadata**: Commit information and MVCC structures
- **Page allocation data**: Free page tracking and region management

## Encryption Specifications

### Cryptographic Algorithms
- **Symmetric Encryption**: AES-256-GCM (Galois/Counter Mode)
  - Provides both confidentiality and authenticity
  - Resistant to padding oracle attacks
  - Hardware acceleration available on modern CPUs
- **Key Derivation**: PBKDF2-SHA256 with 100,000 iterations
  - Industry standard for password-based key derivation
  - Configurable iteration count for future-proofing
  - Salt-based to prevent rainbow table attacks

### Key Management
- **Master Key**: 256-bit AES key derived from user password + salt
- **Salt Storage**: 16-byte cryptographically secure random salt stored in database header
- **Key Derivation**: `PBKDF2(SHA-256, password, salt, 100000 iterations) → 32-byte key`
- **Key Lifecycle**: Key derived on database open, held in memory during session, zeroized on close

### Encryption Parameters
- **Nonce Size**: 12 bytes (96 bits) for AES-GCM
- **Authentication Tag**: 16 bytes (128 bits)
- **Total Overhead**: 28 bytes per encrypted page (12 + 16)
- **Nonce Generation**: Cryptographically secure random for each encryption operation

# File Format

The redbx file format is based on redb with extensions for encryption support.

## Magic Number
redbx uses a different magic number to distinguish from redb files:
- **redbx**: `[b'r', b'd', b'b', b'x', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A]`
- **redb**: `[b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A]`

This prevents accidental opening of encrypted databases with unencrypted redb and vice versa.

## Database Header Extensions

The database header is extended to include encryption metadata:

```
<-------------------------------------------- 8 bytes ------------------------------------------->
========================================== Super header ==========================================
---------------------------------------- Header (64 bytes) ---------------------------------------
| magic number (rdbx + signature)                                                               |
| magic con.| god byte  | padding               | page size                                      |
| region header pages                           | region max data pages                          |
| encryption salt (16 bytes)                                                                    |
| encryption salt (continued)                                                                   |
| number of full regions                        | data pages in trailing region                  |
| padding                                                                                        |
| padding                                                                                        |
------------------------------------ Commit slot 0 (128 bytes) -----------------------------------
| version   | user nn   | sys nn    | f-root nn | padding                                        |
| user root page number                                                                          |
| user root checksum                                                                             |
| user root checksum (cont.)                                                                     |
| user root length                                                                               |
| system root page number                                                                        |
| system root checksum                                                                           |
| system root checksum (cont.)                                                                   |
| system root length                                                                             |
| padding                                                                                        |
| padding                                                                                        |
| padding                                                                                        |
| padding                                                                                        |
| transaction id                                                                                 |
| slot checksum                                                                                  |
| slot checksum (cont.)                                                                          |
----------------------------------------- Commit slot 1 ------------------------------------------
|                                 Same layout as commit slot 0                                   |
---------------------------------- footer padding (192+ bytes) -----------------------------------
==================================================================================================
```

### God Byte Extensions
The god byte is extended with an encryption flag:
- **Bit 0**: `primary_bit` - indicates active commit slot
- **Bit 1**: `recovery_required` - database needs repair
- **Bit 2**: `two_phase_commit` - using 2-phase commit
- **Bit 3**: `encrypted` - database uses encryption (always 1 for redbx)

### Encryption Salt Field
- **Location**: Bytes 24-39 in database header
- **Size**: 16 bytes
- **Purpose**: Unique salt for PBKDF2 key derivation
- **Generation**: Cryptographically secure random bytes

## Encrypted Page Format

**CRITICAL ARCHITECTURAL DECISIONS**:
1. **Encryption Overhead**: Reduce LEAF Page Capacity to fit within fixed 4096-byte pages
2. **Page Type Detection**: Store page type in unencrypted header to maintain btree compatibility

### Page Type Detection Challenge
The btree code expects `page.memory()[0]` to contain the page type (1=LEAF, 2=BRANCH), but encryption makes this impossible for LEAF pages since the first byte becomes encrypted data.

### Solution: Unencrypted Page Type Metadata
Store page type in the first byte before encryption, preserving btree compatibility:

**For LEAF Pages (Encrypted)**:
- **Byte 0**: Page type (1=LEAF) - **UNENCRYPTED**
- **Bytes 1-12**: Nonce (12 bytes) - **UNENCRYPTED**
- **Bytes 13-4095**: Encrypted data + auth tag (4083 bytes max) - **ENCRYPTED**

**For BRANCH Pages (Unencrypted)**:
- **Byte 0**: Page type (2=BRANCH) - **UNENCRYPTED**
- **Bytes 1-4095**: Page data (4095 bytes) - **UNENCRYPTED**

### Page Size Management
- **Disk Pages**: Fixed 4096 bytes for all page types (maintains redb file layout compatibility)
- **LEAF Pages**: Usable capacity reduced to 4055 bytes (4096 - 1 - 12 - 28 = 4055 bytes)
- **BRANCH Pages**: Full 4095 bytes available (minus 1 byte for page type)
- **Metadata Overhead**: 1 byte page type + 12 bytes nonce + 16 bytes auth tag = 29 bytes total

### LEAF Pages (Encrypted with Unencrypted Header)
LEAF pages with page type preserved for btree compatibility:

```
On Disk (Fixed 4096 bytes):
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| type(1)  | Nonce (12 bytes)                                | Encrypted Data + Auth Tag      |
|          |                                                  | (Max 4055 bytes + 16 byte tag) |
==================================================================================================

Decrypted In-Memory (Usable: 4055 bytes):
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| type(1)  | padding   | number of entries      | (optional) key end (repeated entries times)    |
--------------------------------------------------------------------------------------------------
| (optional) value end (repeated entries times) | (optional) key alignment padding               |
==================================================================================================
| Key data                                      | (optional) value alignment padding             |
==================================================================================================
| Value data (capacity limited to fit within 4055 bytes)                                       |
==================================================================================================
```

### BRANCH Pages (Unencrypted with Page Type)
BRANCH pages remain unencrypted for performance, with page type preserved:

```
<-------------------------------------------- 8 bytes ------------------------------------------->
==================================================================================================
| type(2)  | padding   | number of keys        | padding                                         |
--------------------------------------------------------------------------------------------------
| child page checksum (repeated num_keys + 1 times)                                              |
|                                                                                                |
--------------------------------------------------------------------------------------------------
| child page number (repeated num_keys + 1 times)                                                |
--------------------------------------------------------------------------------------------------
| (optional) key end (repeated num_keys times) | alignment padding                               |
==================================================================================================
| Key data (navigation keys only, no user values) - 4095 bytes available                       |
==================================================================================================
```

# Storage Backend Architecture

## EncryptedFileBackend
The `EncryptedFileBackend` implements the `StorageBackend` trait and provides transparent encryption with preserved page type detection.

### Structure
- **File Handle**: Direct file I/O operations
- **Key Manager**: Thread-safe key management with Arc<KeyManager>
- **Salt Storage**: 16-byte encryption salt
- **Lock Support**: Platform-dependent file locking capability

### Core Methods
- **Creation**: Creates new encrypted database with password
- **Opening**: Opens existing database with password validation
- **Encryption Decision**: Determines which pages to encrypt based on page type
- **Read/Write Operations**: Transparent encryption/decryption during I/O

### Encryption Implementation
The encryption system handles page encryption transparently:

**Write Operations**:
- Non-encrypted pages (BRANCH, metadata) are written directly to disk
- LEAF pages have their page type preserved in byte 0 (unencrypted)
- A unique nonce is generated for each encryption operation
- Page content (excluding page type) is encrypted with AES-256-GCM
- Layout: [page_type(1)] + [nonce(12)] + [encrypted_data + auth_tag]

**Read Operations**:
- All pages are read from disk first
- Page type in byte 0 determines if decryption is needed
- For LEAF pages, nonce is extracted from bytes 1-12
- Encrypted payload is decrypted and reconstructed with original page type
- BRANCH pages and metadata are used as-is without decryption

### Encryption Decision Logic
The encryption decision is based on page type analysis:

- **LEAF Pages** (type 1): Always encrypted as they contain user data
- **BRANCH Pages** (type 2): Never encrypted for performance (navigation only)
- **Metadata Pages**: Never encrypted for debugging and performance
- **Empty Pages**: Never encrypted (safety check)

This selective approach reduces encryption overhead while protecting sensitive user data.

## Performance Considerations

### Selective Encryption Benefits
1. **Reduced Overhead**: Only ~30-50% of pages encrypted in typical workloads
2. **Fast Navigation**: B-tree traversal uses unencrypted BRANCH pages
3. **Debugging**: Metadata remains readable for troubleshooting
4. **Cache Efficiency**: Unencrypted pages can be cached without security concerns

### Encryption Overhead
- **CPU**: ~10-20% overhead for encrypted pages (AES-GCM is hardware-accelerated)
- **Storage**: No overhead per page (encryption fits within fixed 4096-byte pages)
- **LEAF Capacity**: ~1% reduced capacity (4055/4096 = 98.9% of original, 13 bytes metadata overhead)
- **BRANCH Capacity**: Minimal impact (4095/4096 = 99.9% of original, 1 byte page type)
- **Memory**: Master key held in memory during database session
- **I/O**: No significant impact due to selective encryption and fixed page sizes
- **Btree Compatibility**: 100% compatible (page type detection unchanged)

# Security Model

## Threat Model
redbx protects against:
- **Data at Rest Attacks**: Database files are encrypted on disk
- **Backup Compromise**: Database backups are useless without password
- **Memory Dumps**: Keys are zeroized when database closes
- **File System Access**: Direct file access reveals no user data

redbx does NOT protect against:
- **Memory Attacks**: Keys and decrypted data visible in process memory
- **Application-Level Attacks**: Compromised application can access all data
- **Side Channel Attacks**: Timing and power analysis (use HSM if needed)
- **Quantum Attacks**: AES-256 security is reduced to ~128-bit equivalent against quantum computers, but remains practically secure for foreseeable future

## Key Management Security

### Password Handling
- User passwords never stored permanently
- Passwords cleared from memory after key derivation
- Strong password requirements recommended but not enforced

### Salt Management
- 16-byte cryptographically secure random salt per database
- Salt stored unencrypted in database header
- Unique salt prevents rainbow table attacks
- Salt cannot be changed after database creation

### Key Derivation
- PBKDF2-SHA256 with 100,000 iterations (configurable)
- High iteration count slows brute force attacks
- Industry standard algorithm with wide support
- Resistant to GPU/FPGA acceleration

## Database Lifecycle Security

### Creation
1. Generate cryptographically secure 16-byte salt
2. Derive master key using PBKDF2(password, salt, 100000)
3. Store salt in database header
4. Begin encrypting user data pages

### Opening
1. Read salt from database header
2. Derive master key using PBKDF2(password, salt, 100000)
3. Verify key correctness by attempting to decrypt a test page
4. Begin normal database operations

### Closing
1. Complete any pending transactions
2. Flush all data to disk
3. Zeroize master key from memory
4. Close file handles

# API Changes from redb

## Always-Encrypted API
Unlike redb, redbx is always encrypted and requires passwords for all database operations:

- **Database Creation**: Always requires a password parameter
- **Database Opening**: Always requires the same password used during creation
- **No Unencrypted Mode**: All user data is automatically encrypted
- **Password Validation**: Incorrect passwords result in clear error messages

## API Signatures

### Database Creation and Opening
The primary API methods require password parameters:

- **Database::create()**: Creates new encrypted database with password
- **Database::open()**: Opens existing database with password
- **DatabaseBuilder**: Builder pattern supporting encrypted database configuration

### Builder Pattern Support
- **Cache Configuration**: Configurable page cache size
- **Page Size**: Configurable page size (affects encryption overhead)
- **Encrypted Creation/Opening**: All builder methods require passwords

### Password Security
Internal password handling provides secure memory management:

- **SecurePassword Wrapper**: Wraps password data with automatic zeroization
- **Memory Safety**: Password data is cleared from memory after use
- **Key Derivation**: Secure PBKDF2 key derivation from password and salt
- **No Storage**: Passwords are never stored permanently

## Comprehensive Error Handling

### Error Types
redbx extends redb's error handling with encryption-specific errors:

**Inherited from redb**:
- Database corruption, file access, and transaction errors
- Table management and data operation errors

**New Encryption Errors**:
- **IncorrectPassword**: Wrong password provided during opening
- **CorruptedEncryption**: Encrypted data appears corrupted
- **KeyDerivationFailed**: PBKDF2 key derivation failed
- **EncryptionFailed**: AES encryption operation failed
- **DecryptionFailed**: AES decryption operation failed
- **SaltGenerationFailed**: Cryptographic salt generation failed
- **IncompatibleFileFormat**: File format version incompatibility

### Error Recovery Strategies
- **IncorrectPassword**: Prompt for password re-entry, no automatic retry
- **CorruptedEncryption**: Attempt partial recovery, log corruption details
- **KeyDerivationFailed**: Check PBKDF2 parameters, validate salt integrity
- **EncryptionFailed**: Retry with fresh nonce, check hardware support
- **DecryptionFailed**: Verify file integrity, attempt backup recovery

# Compatibility and Migration

## Backward Compatibility
- redbx cannot open redb files (different magic number)
- redb cannot open redbx files (different magic number)
- Clear error messages for format mismatches

## Migration from redb
Migration requires creating a new redbx database and copying data:

1. **Open Source Database**: Open existing redb database
2. **Create Target Database**: Create new redbx database with password
3. **Copy Table Schemas**: Recreate all table definitions
4. **Transfer Data**: Copy all key-value pairs from source to target
5. **Verify Migration**: Ensure data integrity after transfer
6. **Update Applications**: Modify application code to use redbx API with passwords

# Concurrency and Threading

## Thread Safety Model
- **Key Management**: Master key shared across all threads via Arc<>
- **Encryption Context**: Thread-local nonce generation for parallel encryption
- **Reader Isolation**: Multiple readers can decrypt pages independently
- **Writer Coordination**: Single writer serializes encrypted page writes
- **Memory Safety**: Zeroize sensitive data when threads terminate

## Lock-Free Operations
- **Page Decryption**: No locks required, read-only master key access
- **Nonce Generation**: Thread-local RNG prevents contention
- **Salt Access**: Immutable after database creation, lock-free reads

# Memory Management

## Secure Memory Handling
The key management system ensures secure handling of cryptographic material:

**Key Lifecycle**:
- **Creation**: Master key derived from password and salt using PBKDF2
- **Storage**: Key held in memory during database session
- **Access**: Thread-safe access through Arc<KeyManager>
- **Cleanup**: Automatic zeroization when database closes

**Memory Security**:
- **Zeroization**: All sensitive data cleared on drop
- **Stack Allocation**: Keys stored on stack to prevent heap fragmentation
- **Secure Deletion**: Cryptographic material overwritten with zeros

## Memory Layout Considerations
- **Page Buffer Alignment**: 4KB alignment for optimal encryption performance
- **Key Storage**: Stack allocation to prevent heap fragmentation
- **Nonce Pool**: Pre-allocated pool to avoid allocation during encryption
- **Decrypted Page Cache**: Clear pages on eviction to prevent memory leaks

# Performance Characteristics

## Detailed Benchmarks (Target Metrics)
- **Read Latency**: <1ms for cached pages, <5ms for encrypted page load
- **Write Latency**: <2ms for encrypted page write with fsync
- **Throughput**: >50MB/s sequential reads, >30MB/s sequential writes
- **Memory Overhead**: <10MB base + 1KB per cached encrypted page
- **CPU Usage**: <20% overhead on AES-NI enabled systems
- **Storage Overhead**: No per-page overhead (fixed 4096-byte pages with metadata reserved within pages)

## Performance Thresholds
- **Acceptable**: <20% performance degradation vs redb
- **Warning**: 20-40% degradation indicates optimization needed
- **Critical**: >40% degradation requires architectural changes

## Optimization Strategies
1. **Hardware AES**: Leverage CPU AES-NI instructions
2. **Batch Operations**: Encrypt/decrypt multiple pages together
3. **Key Caching**: Reuse derived key for database session
4. **Selective Encryption**: Only encrypt user data, not metadata
5. **Page Alignment**: Optimize for CPU cache line alignment

# Testing Strategy

## Unit Tests
- Encryption/decryption correctness
- Key derivation with various inputs
- Salt generation and storage
- Error handling for wrong passwords

## Integration Tests
- Full database lifecycle with encryption
- Multiple tables and data types
- Concurrent access patterns
- Recovery after unclean shutdown

## Security Tests
- Password brute force resistance
- Side channel attack resistance (basic)
- Memory leak detection for sensitive data
- Cryptographic algorithm compliance

## Performance Tests
- Benchmark against redb baseline
- Memory usage profiling
- Encryption overhead measurement
- Scalability with database size

# Future Enhancements

## Potential Features
1. **Key Rotation**: Support for changing database passwords
2. **Multi-Key**: Support for different keys per table
3. **Compression**: Pre-encryption compression for better space efficiency
4. **Key Stretching**: Configurable PBKDF2 iteration count

## Security Improvements
1. **Memory Protection**: Use OS memory protection features
2. **Secure Deletion**: Cryptographically secure deletion of encrypted pages

This design provides a foundation for a secure, performant, always-encrypted embedded database while maintaining the simplicity and reliability that makes redb successful.
