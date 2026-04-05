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
    batch.delete(COUNTER_KEY.as_bytes());
    db.write(batch)?;
    Ok(())
}
