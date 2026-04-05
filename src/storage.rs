use std::sync::Arc;

use rocksdb::{DB, Options, WriteBatch};

use crate::models::TelemetryEvent;

const COUNTER_KEY: &str = "__counter";

pub fn open_db(path: &str) -> Result<Arc<DB>, Box<dyn std::error::Error + Send + Sync>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    let db = DB::open(&opts, path)?;
    Ok(Arc::new(db))
}

#[allow(dead_code)]
pub fn get_counter(db: &DB) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    match db.get(COUNTER_KEY.as_bytes())? {
        Some(bytes) => {
            let arr: [u8; 8] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| "invalid counter value in rocksdb")?;
            Ok(u64::from_le_bytes(arr))
        }
        None => Ok(0),
    }
}

#[allow(dead_code)]
pub fn write_event(
    db: &DB,
    event: &TelemetryEvent,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let counter = get_counter(db)? + 1;
    let key = format!("event_{:010}", counter);
    let value = serde_json::to_vec(event)?;

    let mut batch = WriteBatch::default();
    batch.put(key.as_bytes(), &value);
    batch.put(COUNTER_KEY.as_bytes(), counter.to_le_bytes());
    db.write(batch)?;

    Ok(())
}

pub fn read_batch(
    db: &DB,
    start_index: u64,
    size: usize,
) -> Result<Vec<TelemetryEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let mut events = Vec::with_capacity(size);
    let prefix = b"event_".to_vec();

    let start_key = format!("event_{:010}", start_index);
    let iter = db.iterator(rocksdb::IteratorMode::From(
        start_key.as_bytes(),
        rocksdb::Direction::Forward,
    ));

    for item in iter {
        if events.len() >= size {
            break;
        }
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            continue;
        }
        let event: TelemetryEvent = serde_json::from_slice(&value)?;
        events.push(event);
    }

    Ok(events)
}

pub fn delete_batch(
    db: &DB,
    start_index: u64,
    size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut batch = WriteBatch::default();
    for i in start_index..start_index + size as u64 {
        let key = format!("event_{:010}", i);
        batch.delete(key.as_bytes());
    }
    db.write(batch)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_event(name: &str) -> TelemetryEvent {
        TelemetryEvent {
            session_id: "test-session".into(),
            path: format!("/test/{name}"),
            event_name: name.into(),
            timestamp: 1_000_000,
        }
    }

    #[test]
    fn test_open_db_creates_directory() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(get_counter(&db).unwrap(), 0);
    }

    #[test]
    fn test_write_and_read_single_event() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path().to_str().unwrap()).unwrap();
        let event = test_event("click");
        write_event(&db, &event).unwrap();
        assert_eq!(get_counter(&db).unwrap(), 1);

        let batch = read_batch(&db, 0, 10).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_name, "click");
    }

    #[test]
    fn test_write_batch_of_events() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path().to_str().unwrap()).unwrap();

        for i in 0..50 {
            write_event(&db, &test_event(&format!("e{i}"))).unwrap();
        }
        assert_eq!(get_counter(&db).unwrap(), 50);

        let batch = read_batch(&db, 0, 50).unwrap();
        assert_eq!(batch.len(), 50);
        assert_eq!(batch[0].event_name, "e0");
        assert_eq!(batch[49].event_name, "e49");
    }

    #[test]
    fn test_delete_batch_removes_keys() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path().to_str().unwrap()).unwrap();

        for i in 0..10 {
            write_event(&db, &test_event(&format!("e{i}"))).unwrap();
        }

        // Keys are 1-indexed: event_0000000001..event_0000000010
        delete_batch(&db, 1, 10).unwrap();

        let remaining = read_batch(&db, 0, 100).unwrap();
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_read_batch_partial_from_middle() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path().to_str().unwrap()).unwrap();

        for i in 0..20 {
            write_event(&db, &test_event(&format!("e{i}"))).unwrap();
        }

        // Keys are 1-indexed: event_0000000001..event_0000000020
        // start_index=6 reads from key event_0000000006 = e5
        let partial = read_batch(&db, 6, 5).unwrap();
        assert_eq!(partial.len(), 5);
        assert_eq!(partial[0].event_name, "e5");
    }
}
