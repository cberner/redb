//! Comprehensive tests for redbx encryption functionality

use redbx::{Database, DatabaseError, TableDefinition, ReadableDatabase, ReadableTable};
use tempfile::NamedTempFile;

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("test_data");

#[test]
fn test_encrypted_database_creation() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "test_password_123";
    
    // Create encrypted database
    let db = Database::create(temp_file.path(), password);
    assert!(db.is_ok(), "Failed to create encrypted database: {:?}", db.err());
    
    let db = db.unwrap();
    
    // Test basic operations
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert("key1", &123u64).unwrap();
        table.insert("key2", &456u64).unwrap();
    }
    write_txn.commit().unwrap();
    
    // Test reading data
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("key1").unwrap().unwrap().value(), 123);
    assert_eq!(table.get("key2").unwrap().unwrap().value(), 456);
}

#[test]
fn test_encrypted_database_reopen() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "test_password_456";
    
    // Create and populate database
    {
        let db = Database::create(temp_file.path(), password).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("persistent_key", &789u64).unwrap();
        }
        write_txn.commit().unwrap();
    }
    
    // Reopen database
    let db = Database::open(temp_file.path(), password).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("persistent_key").unwrap().unwrap().value(), 789);
}

#[test]
fn test_wrong_password_fails() {
    let temp_file = NamedTempFile::new().unwrap();
    let correct_password = "correct_password";
    let wrong_password = "wrong_password";
    
    // Create database with correct password
    {
        let db = Database::create(temp_file.path(), correct_password).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("test_key", &42u64).unwrap();
        }
        write_txn.commit().unwrap();
    }
    
    // Try to open with wrong password
    let result = Database::open(temp_file.path(), wrong_password);
    assert!(result.is_err(), "Opening with wrong password should fail");
    
    // Verify it's the correct error type
    match result.unwrap_err() {
        DatabaseError::IncorrectPassword => {}, // Expected
        other => panic!("Expected IncorrectPassword error, got: {:?}", other),
    }
}

#[test]
fn test_multiple_tables_encryption() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "multi_table_password";
    
    const TABLE1: TableDefinition<&str, u64> = TableDefinition::new("table1");
    const TABLE2: TableDefinition<&str, String> = TableDefinition::new("table2");
    
    let db = Database::create(temp_file.path(), password).unwrap();
    let write_txn = db.begin_write().unwrap();
    
    // Insert data into multiple tables
    {
        let mut table1 = write_txn.open_table(TABLE1).unwrap();
        table1.insert("key1", &100u64).unwrap();
        table1.insert("key2", &200u64).unwrap();
    }
    
    {
        let mut table2 = write_txn.open_table(TABLE2).unwrap();
        table2.insert("str_key1", &"value1".to_string()).unwrap();
        table2.insert("str_key2", &"value2".to_string()).unwrap();
    }
    
    write_txn.commit().unwrap();
    
    // Verify data in both tables
    let read_txn = db.begin_read().unwrap();
    {
        let table1 = read_txn.open_table(TABLE1).unwrap();
        assert_eq!(table1.get("key1").unwrap().unwrap().value(), 100);
        assert_eq!(table1.get("key2").unwrap().unwrap().value(), 200);
    }
    
    {
        let table2 = read_txn.open_table(TABLE2).unwrap();
        assert_eq!(table2.get("str_key1").unwrap().unwrap().value(), "value1");
        assert_eq!(table2.get("str_key2").unwrap().unwrap().value(), "value2");
    }
}

#[test]
fn test_large_data_encryption() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "large_data_password";
    
    const LARGE_TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("large_data");
    
    let db = Database::create(temp_file.path(), password).unwrap();
    let write_txn = db.begin_write().unwrap();
    
    // Insert large data (1MB)
    let large_data = vec![0x42u8; 1024 * 1024]; // 1MB of data
    {
        let mut table = write_txn.open_table(LARGE_TABLE).unwrap();
        table.insert("large_key", &large_data).unwrap();
    }
    
    write_txn.commit().unwrap();
    
    // Verify large data
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(LARGE_TABLE).unwrap();
    let retrieved_data = table.get("large_key").unwrap().unwrap().value();
    assert_eq!(retrieved_data.len(), 1024 * 1024);
    assert_eq!(retrieved_data[0], 0x42);
    assert_eq!(retrieved_data[retrieved_data.len() - 1], 0x42);
}

// Note: Encryption module tests are in the internal test modules
// since the encryption module is private to the crate

#[test]
fn test_database_builder_with_encryption() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "builder_test_password";
    
    // Test builder pattern with encryption
    let db = Database::builder()
        .set_cache_size(1024 * 1024) // 1MB cache
        .create(temp_file.path(), password)
        .unwrap();
    
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert("builder_test", &999u64).unwrap();
    }
    write_txn.commit().unwrap();
    
    // Verify data
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("builder_test").unwrap().unwrap().value(), 999);
}

#[test]
fn test_proactive_password_validation() {
    let temp_file = NamedTempFile::new().unwrap();
    let correct_password = "correct_password_123";
    let wrong_password = "wrong_password_456";
    
    // Create database with some data
    {
        let db = Database::create(temp_file.path(), correct_password).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("validation_test", &42u64).unwrap();
        }
        write_txn.commit().unwrap();
    }
    
    // Test that wrong password is detected immediately during open
    let result = Database::open(temp_file.path(), wrong_password);
    assert!(result.is_err(), "Opening with wrong password should fail immediately");
    
    // Verify it's the correct error type
    match result.unwrap_err() {
        DatabaseError::IncorrectPassword => {}, // Expected
        other => panic!("Expected IncorrectPassword error, got: {:?}", other),
    }
    
    // Test that correct password still works
    let db = Database::open(temp_file.path(), correct_password).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("validation_test").unwrap().unwrap().value(), 42);
}

#[test]
fn test_read_only_password_validation() {
    let temp_file = NamedTempFile::new().unwrap();
    let correct_password = "readonly_correct_password";
    let wrong_password = "readonly_wrong_password";
    
    // Create database with some data
    {
        let db = Database::create(temp_file.path(), correct_password).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("readonly_test", &99u64).unwrap();
        }
        write_txn.commit().unwrap();
    }
    
    // Test that wrong password is detected immediately during read-only open
    let result = Database::builder().open_read_only(temp_file.path(), wrong_password);
    assert!(result.is_err(), "Opening read-only with wrong password should fail immediately");
    
    // Verify it's the correct error type
    if let Err(DatabaseError::IncorrectPassword) = result {
        // Expected error type
    } else {
        panic!("Expected IncorrectPassword error, got different error");
    }
    
    // Test that correct password still works for read-only
    let db = Database::builder().open_read_only(temp_file.path(), correct_password).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("readonly_test").unwrap().unwrap().value(), 99);
}

#[test]
fn test_read_only_encrypted_database_comprehensive() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "read_only_comprehensive_test";
    
    // Create and populate database
    {
        let db = Database::create(temp_file.path(), password).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("read_only_key1", &100u64).unwrap();
            table.insert("read_only_key2", &200u64).unwrap();
            table.insert("read_only_key3", &300u64).unwrap();
        }
        write_txn.commit().unwrap();
    }
    
    // Open as read-only
    let db = Database::builder().open_read_only(temp_file.path(), password).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    
    // Test reading all data
    assert_eq!(table.get("read_only_key1").unwrap().unwrap().value(), 100);
    assert_eq!(table.get("read_only_key2").unwrap().unwrap().value(), 200);
    assert_eq!(table.get("read_only_key3").unwrap().unwrap().value(), 300);
    
    // Verify write operations fail - ReadOnlyDatabase doesn't have begin_write method
    // This is enforced at compile time, so we just need to verify the database is read-only
    // by checking that we can read from it successfully
    
    // Test iteration
    let mut count = 0;
    for item in table.iter().unwrap() {
        let (key, value) = item.unwrap();
        assert!(key.value() == "read_only_key1" || key.value() == "read_only_key2" || key.value() == "read_only_key3");
        assert!(value.value() == 100 || value.value() == 200 || value.value() == 300);
        count += 1;
    }
    assert_eq!(count, 3, "Should have 3 items in the table");
}

#[test]
fn test_read_only_wrong_password_comprehensive() {
    let temp_file = NamedTempFile::new().unwrap();
    let correct_password = "correct_readonly_password";
    let wrong_password = "wrong_readonly_password";
    
    // Create database
    {
        let db = Database::create(temp_file.path(), correct_password).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("test_key", &123u64).unwrap();
        }
        write_txn.commit().unwrap();
    }
    
    // Try to open read-only with wrong password
    let result = Database::builder().open_read_only(temp_file.path(), wrong_password);
    assert!(result.is_err(), "Should fail with wrong password");
    
    match result {
        Err(DatabaseError::IncorrectPassword) => {}, // Expected
        Err(other) => panic!("Expected IncorrectPassword error, got: {:?}", other),
        Ok(_) => panic!("Expected error but got success"),
    }
}

#[test]
fn test_read_only_empty_database() {
    let temp_file = NamedTempFile::new().unwrap();
    let password = "empty_readonly_test";
    
    // Create empty database
    {
        let _db = Database::create(temp_file.path(), password).unwrap();
        // Don't add any data
    }
    
    // Open as read-only
    let db = Database::builder().open_read_only(temp_file.path(), password).unwrap();
    let read_txn = db.begin_read().unwrap();
    
    // Test that we can't open a table that doesn't exist
    let table_result = read_txn.open_table(TABLE);
    assert!(table_result.is_err(), "Opening non-existent table should fail");
    
    // This is expected behavior - in an empty database, no tables exist yet
}