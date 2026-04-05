use std::sync::Arc;

use rocksdb::{Options, WriteBatch, DB};

use crate::models::TelemetryEvent;

const NEXT_EVENT_ID_KEY: &str = "__meta_next_event_id";
const NEXT_BATCH_START_ID_KEY: &str = "__meta_next_batch_start_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub next_event_id: u64,
    pub next_batch_start_id: u64,
}

pub fn open_db(path: &str) -> Result<Arc<DB>, Box<dyn std::error::Error + Send + Sync>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    let db = DB::open(&opts, path)?;
    Ok(Arc::new(db))
}

fn event_key(event_id: u64) -> String {
    format!("event_{event_id:010}")
}

fn metadata_bytes(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn read_metadata_value(
    db: &DB,
    key: &[u8],
) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
    match db.get(key)? {
        Some(bytes) => {
            let arr: [u8; 8] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| "invalid metadata value in rocksdb")?;
            Ok(Some(u64::from_le_bytes(arr)))
        }
        None => Ok(None),
    }
}

fn parse_event_id(key: &[u8]) -> Option<u64> {
    let key = std::str::from_utf8(key).ok()?;
    let suffix = key.strip_prefix("event_")?;
    suffix.parse().ok()
}

fn discover_event_range(
    db: &DB,
) -> Result<Option<(u64, u64)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut min_id: Option<u64> = None;
    let mut max_id: Option<u64> = None;

    for item in db.iterator(rocksdb::IteratorMode::Start) {
        let (key, _) = item?;
        if let Some(event_id) = parse_event_id(&key) {
            min_id = Some(match min_id {
                Some(current) => current.min(event_id),
                None => event_id,
            });
            max_id = Some(match max_id {
                Some(current) => current.max(event_id),
                None => event_id,
            });
        }
    }

    Ok(match (min_id, max_id) {
        (Some(min_id), Some(max_id)) => Some((min_id, max_id)),
        _ => None,
    })
}

fn persist_metadata(
    db: &DB,
    metadata: Metadata,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut batch = WriteBatch::default();
    batch.put(
        NEXT_EVENT_ID_KEY.as_bytes(),
        metadata_bytes(metadata.next_event_id),
    );
    batch.put(
        NEXT_BATCH_START_ID_KEY.as_bytes(),
        metadata_bytes(metadata.next_batch_start_id),
    );
    db.write(batch)?;
    Ok(())
}

pub fn load_or_initialize_metadata(
    db: &DB,
) -> Result<Metadata, Box<dyn std::error::Error + Send + Sync>> {
    let next_event_id = read_metadata_value(db, NEXT_EVENT_ID_KEY.as_bytes())?;
    let next_batch_start_id = read_metadata_value(db, NEXT_BATCH_START_ID_KEY.as_bytes())?;

    if let (Some(next_event_id), Some(next_batch_start_id)) = (next_event_id, next_batch_start_id) {
        return Ok(Metadata {
            next_event_id,
            next_batch_start_id,
        });
    }

    let metadata = match discover_event_range(db)? {
        Some((min_event_id, max_event_id)) => Metadata {
            next_event_id: max_event_id + 1,
            next_batch_start_id: min_event_id,
        },
        None => Metadata {
            next_event_id: 1,
            next_batch_start_id: 1,
        },
    };

    persist_metadata(db, metadata)?;

    Ok(metadata)
}

#[allow(dead_code)]
pub fn get_counter(db: &DB) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    Ok(load_or_initialize_metadata(db)?
        .next_event_id
        .saturating_sub(1))
}

pub fn append_event(
    db: &DB,
    event_id: u64,
    event: &TelemetryEvent,
    next_event_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let value = serde_json::to_vec(event)?;

    let mut batch = WriteBatch::default();
    batch.put(event_key(event_id).as_bytes(), &value);
    batch.put(NEXT_EVENT_ID_KEY.as_bytes(), metadata_bytes(next_event_id));
    db.write(batch)?;

    Ok(())
}

#[allow(dead_code)]
pub fn write_event(
    db: &DB,
    event: &TelemetryEvent,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metadata = load_or_initialize_metadata(db)?;
    append_event(
        db,
        metadata.next_event_id,
        event,
        metadata.next_event_id + 1,
    )
}

pub fn read_batch(
    db: &DB,
    start_event_id: u64,
    size: usize,
) -> Result<Vec<TelemetryEvent>, Box<dyn std::error::Error + Send + Sync>> {
    if size == 0 {
        return Ok(Vec::new());
    }

    let mut events = Vec::with_capacity(size);
    let prefix = b"event_".to_vec();

    let start_key = event_key(start_event_id);
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
    start_event_id: u64,
    size: usize,
    next_batch_start_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut batch = WriteBatch::default();
    for event_id in start_event_id..start_event_id + size as u64 {
        batch.delete(event_key(event_id).as_bytes());
    }
    batch.put(
        NEXT_BATCH_START_ID_KEY.as_bytes(),
        metadata_bytes(next_batch_start_id),
    );
    db.write(batch)?;
    Ok(())
}
