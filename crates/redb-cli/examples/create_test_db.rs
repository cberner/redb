use redb::{Database, MultimapTableDefinition, TableDefinition};

const USERS: TableDefinition<&str, &str> = TableDefinition::new("users");
const EMBEDDINGS: TableDefinition<u64, &[u8]> = TableDefinition::new("embeddings");
const TAGS: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("tags");

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "test.redb".to_string());
    let db = Database::create(&path).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(USERS).unwrap();
        for i in 0..1000 {
            let key = format!("user_{i:04}");
            let val = format!("{{\"name\":\"User {i}\",\"email\":\"user{i}@example.com\"}}");
            t.insert(key.as_str(), val.as_str()).unwrap();
        }
    }
    {
        let mut t = txn.open_table(EMBEDDINGS).unwrap();
        let data = vec![0u8; 384];
        for i in 0..500 {
            t.insert(i, data.as_slice()).unwrap();
        }
    }
    {
        let mut t = txn.open_multimap_table(TAGS).unwrap();
        t.insert("rust", "memory").unwrap();
        t.insert("rust", "database").unwrap();
        t.insert("rust", "embedded").unwrap();
        t.insert("ai", "agents").unwrap();
        t.insert("ai", "memory").unwrap();
    }
    txn.commit().unwrap();

    println!("Created test database at: {path}");
}
