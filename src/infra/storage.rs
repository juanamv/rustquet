use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Error, ErrorKind};
use std::sync::Arc;

use rocksdb::{DB, Options, WriteBatch};

use crate::domain::models::TelemetryEvent;
use crate::domain::schema::{self, IndexedColumn, SchemaSpec};

const NEXT_EVENT_ID_KEY: &str = "__meta_next_event_id";
const NEXT_BATCH_START_ID_KEY: &str = "__meta_next_batch_start_id";
const ACTIVE_BATCH_START_ID_KEY: &str = "__meta_active_batch_start_id";
const ACTIVE_BATCH_LEN_KEY: &str = "__meta_active_batch_len";
const ACTIVE_BATCH_STATUS_KEY: &str = "__meta_active_batch_status";
const ACTIVE_BATCH_SCHEMA_VERSION_KEY: &str = "__meta_active_batch_schema_version";
const ACTIVE_BATCH_TIMESTAMP_MIN_KEY: &str = "__meta_active_batch_timestamp_min";
const ACTIVE_BATCH_TIMESTAMP_MAX_KEY: &str = "__meta_active_batch_timestamp_max";
const ACTIVE_BATCH_UPLOADED_PUSHES_KEY: &str = "__meta_active_batch_uploaded_pushes";
const CURRENT_SCHEMA_VERSION_KEY: &str = "__schema_current_version";
const SCHEMA_VERSION_PREFIX: &str = "__schema_version_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub next_event_id: u64,
    pub next_batch_start_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveBatchStatus {
    Writing,
    Written,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveBatch {
    pub start_event_id: u64,
    pub len: usize,
    pub status: ActiveBatchStatus,
    pub schema_version: u32,
    pub timestamp_min: i64,
    pub timestamp_max: i64,
}

impl ActiveBatchStatus {
    fn as_byte(self) -> u8 {
        match self {
            Self::Writing => 1,
            Self::Written => 2,
        }
    }

    fn from_byte(value: u8) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        match value {
            1 => Ok(Self::Writing),
            2 => Ok(Self::Written),
            _ => Err(invalid_data("invalid active batch status in rocksdb").into()),
        }
    }
}

pub fn open_db(path: &str) -> Result<Arc<DB>, Box<dyn std::error::Error + Send + Sync>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    let db = DB::open(&opts, path)?;
    Ok(Arc::new(db))
}

fn invalid_data(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn format_column(column: &IndexedColumn) -> String {
    format!(
        "{} <- metadata.{} ({})",
        column.name,
        column.path.join("."),
        match column.kind {
            schema::ColumnType::Bool => "boolean",
            schema::ColumnType::String => "string",
            schema::ColumnType::Number => "number",
        }
    )
}

fn format_schema_columns(schema_spec: &SchemaSpec) -> String {
    if schema_spec.columns.is_empty() {
        return "  - (no indexed columns)".to_string();
    }

    schema_spec
        .columns
        .iter()
        .map(|column| format!("  - {}", format_column(column)))
        .collect::<Vec<_>>()
        .join("\n")
}

fn schema_definition_conflict_message(existing: &SchemaSpec, requested: &SchemaSpec) -> String {
    let mut differences = Vec::new();
    let mut requested_columns = HashMap::with_capacity(requested.columns.len());
    for column in &requested.columns {
        requested_columns.insert(column.name.as_str(), column);
    }

    for existing_column in &existing.columns {
        match requested_columns.get(existing_column.name.as_str()) {
            Some(requested_column) if *requested_column == existing_column => {}
            Some(requested_column) => differences.push(format!(
                "  - changed '{}' from {} to {}",
                existing_column.name,
                format_column(existing_column),
                format_column(requested_column)
            )),
            None => differences.push(format!(
                "  - removed '{}' ({})",
                existing_column.name,
                format_column(existing_column)
            )),
        }
    }

    for requested_column in &requested.columns {
        if existing
            .columns
            .iter()
            .all(|column| column.name != requested_column.name)
        {
            differences.push(format!(
                "  - added '{}' ({})",
                requested_column.name,
                format_column(requested_column)
            ));
        }
    }

    format!(
        "schema version {} already exists with a different definition\n\
stored schema:\n{}\n\
requested schema:\n{}\n\
differences:\n{}\n\
action: increment schema.schema_version for the new definition",
        existing.version,
        format_schema_columns(existing),
        format_schema_columns(requested),
        differences.join("\n")
    )
}

fn schema_downgrade_message(current_version: u32, requested_version: u32) -> String {
    format!(
        "schema version downgrade is not allowed: requested version {}, current stored version {}\n\
action: keep the current schema version or bump schema.schema_version to a newer version",
        requested_version, current_version
    )
}

fn append_only_schema_message(
    current_schema: &SchemaSpec,
    new_schema: &SchemaSpec,
    detail: String,
) -> String {
    format!(
        "new schema version {} must preserve existing indexed columns from version {}\n\
current schema:\n{}\n\
requested schema:\n{}\n\
details:\n{}\n\
action: keep existing indexed columns unchanged and only append new columns in a higher schema version",
        new_schema.version,
        current_schema.version,
        format_schema_columns(current_schema),
        format_schema_columns(new_schema),
        detail
    )
}

fn historical_redefinition_message(
    existing: &IndexedColumn,
    new_schema: &SchemaSpec,
    requested: &IndexedColumn,
) -> String {
    format!(
        "new schema version {} cannot redefine indexed column '{}'\n\
existing definition: {}\n\
requested definition: {}\n\
action: preserve the historical column definition and add a new column with a different name if needed",
        new_schema.version,
        existing.name,
        format_column(existing),
        format_column(requested)
    )
}

fn event_key(event_id: u64) -> String {
    format!("event_{event_id:010}")
}

fn metadata_bytes(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn timestamp_bytes(value: i64) -> [u8; 8] {
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
                .map_err(|_| invalid_data("invalid metadata value in rocksdb"))?;
            Ok(Some(u64::from_le_bytes(arr)))
        }
        None => Ok(None),
    }
}

fn read_timestamp_value(
    db: &DB,
    key: &[u8],
) -> Result<Option<i64>, Box<dyn std::error::Error + Send + Sync>> {
    match db.get(key)? {
        Some(bytes) => {
            let arr: [u8; 8] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| invalid_data("invalid timestamp value in rocksdb"))?;
            Ok(Some(i64::from_le_bytes(arr)))
        }
        None => Ok(None),
    }
}

fn read_active_batch_status(
    db: &DB,
) -> Result<Option<ActiveBatchStatus>, Box<dyn std::error::Error + Send + Sync>> {
    match db.get(ACTIVE_BATCH_STATUS_KEY.as_bytes())? {
        Some(bytes) => {
            let [value]: [u8; 1] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| invalid_data("invalid active batch status bytes in rocksdb"))?;
            Ok(Some(ActiveBatchStatus::from_byte(value)?))
        }
        None => Ok(None),
    }
}

fn read_active_batch_uploaded_pushes(
    db: &DB,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    match db.get(ACTIVE_BATCH_UPLOADED_PUSHES_KEY.as_bytes())? {
        Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
        None => Ok(BTreeSet::new()),
    }
}

fn active_batch_identity(batch: ActiveBatch) -> (u64, usize, u32, i64, i64) {
    (
        batch.start_event_id,
        batch.len,
        batch.schema_version,
        batch.timestamp_min,
        batch.timestamp_max,
    )
}

fn parse_event_id(key: &[u8]) -> Option<u64> {
    let key = std::str::from_utf8(key).ok()?;
    let suffix = key.strip_prefix("event_")?;
    suffix.parse().ok()
}

fn parse_schema_version(key: &[u8]) -> Option<u32> {
    let key = std::str::from_utf8(key).ok()?;
    let suffix = key.strip_prefix(SCHEMA_VERSION_PREFIX)?;
    suffix.parse().ok()
}

fn schema_version_key(version: u32) -> String {
    format!("{SCHEMA_VERSION_PREFIX}{version:010}")
}

fn read_u32_metadata_value(
    db: &DB,
    key: &[u8],
) -> Result<Option<u32>, Box<dyn std::error::Error + Send + Sync>> {
    read_metadata_value(db, key)?.map_or(Ok(None), |value| {
        Ok(Some(u32::try_from(value).map_err(|_| {
            invalid_data("schema version does not fit in u32")
        })?))
    })
}

fn validate_schema_shape(
    schema_spec: &SchemaSpec,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut names = HashMap::with_capacity(schema_spec.columns.len());

    for column in &schema_spec.columns {
        if column.path.is_empty() || column.path.iter().any(|segment| segment.is_empty()) {
            return Err(invalid_data("schema column path cannot be empty").into());
        }
        if schema::is_reserved_column_name(&column.name) {
            return Err(invalid_data("schema column name is reserved").into());
        }
        if names.insert(column.name.as_str(), column).is_some() {
            return Err(invalid_data("schema contains duplicate column names").into());
        }
    }

    Ok(())
}

fn historical_columns(
    schemas: &BTreeMap<u32, SchemaSpec>,
) -> Result<HashMap<&str, &IndexedColumn>, Box<dyn std::error::Error + Send + Sync>> {
    let mut columns = HashMap::new();

    for schema in schemas.values() {
        validate_schema_shape(schema)?;
        for column in &schema.columns {
            match columns.get(column.name.as_str()) {
                Some(existing) if *existing != column => {
                    return Err(
                        invalid_data("stored schema history redefines an indexed column").into(),
                    );
                }
                None => {
                    columns.insert(column.name.as_str(), column);
                }
                _ => {}
            }
        }
    }

    Ok(columns)
}

fn validate_append_only_schema(
    current_schema: &SchemaSpec,
    new_schema: &SchemaSpec,
    schemas: &BTreeMap<u32, SchemaSpec>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let historical = historical_columns(schemas)?;
    let mut new_columns = HashMap::with_capacity(new_schema.columns.len());
    for column in &new_schema.columns {
        new_columns.insert(column.name.as_str(), column);
    }

    for current_column in &current_schema.columns {
        match new_columns.get(current_column.name.as_str()) {
            Some(column) if *column == current_column => {}
            Some(column) => {
                return Err(invalid_data(&append_only_schema_message(
                    current_schema,
                    new_schema,
                    format!(
                        "  - column '{}' changed from {} to {}",
                        current_column.name,
                        format_column(current_column),
                        format_column(column)
                    ),
                ))
                .into());
            }
            _ => {
                return Err(invalid_data(&append_only_schema_message(
                    current_schema,
                    new_schema,
                    format!(
                        "  - column '{}' is missing from the requested schema",
                        current_column.name
                    ),
                ))
                .into());
            }
        }
    }

    for column in &new_schema.columns {
        if let Some(existing) = historical.get(column.name.as_str())
            && *existing != column
        {
            return Err(invalid_data(&historical_redefinition_message(
                existing, new_schema, column,
            ))
            .into());
        }
    }

    Ok(())
}

fn persist_current_schema_version(
    db: &DB,
    version: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.put(
        CURRENT_SCHEMA_VERSION_KEY.as_bytes(),
        metadata_bytes(u64::from(version)),
    )?;
    Ok(())
}

fn persist_schema(
    db: &DB,
    schema_spec: &SchemaSpec,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    db.put(
        schema_version_key(schema_spec.version).as_bytes(),
        serde_json::to_vec(schema_spec)?,
    )?;
    Ok(())
}

pub fn load_current_schema_version(
    db: &DB,
) -> Result<Option<u32>, Box<dyn std::error::Error + Send + Sync>> {
    read_u32_metadata_value(db, CURRENT_SCHEMA_VERSION_KEY.as_bytes())
}

pub fn load_all_schemas(
    db: &DB,
) -> Result<BTreeMap<u32, SchemaSpec>, Box<dyn std::error::Error + Send + Sync>> {
    let mut schemas = BTreeMap::new();

    for item in db.iterator(rocksdb::IteratorMode::Start) {
        let (key, value) = item?;
        let Some(version) = parse_schema_version(&key) else {
            continue;
        };
        let schema_spec: SchemaSpec = serde_json::from_slice(&value)?;
        if schema_spec.version != version {
            return Err(invalid_data("stored schema version key does not match payload").into());
        }
        validate_schema_shape(&schema_spec)?;
        schemas.insert(version, schema_spec);
    }

    Ok(schemas)
}

pub fn ensure_schema(
    db: &DB,
    schema_spec: &SchemaSpec,
) -> Result<BTreeMap<u32, SchemaSpec>, Box<dyn std::error::Error + Send + Sync>> {
    validate_schema_shape(schema_spec)?;

    let mut schemas = load_all_schemas(db)?;
    let current_version = load_current_schema_version(db)?;

    if let Some(version) = current_version {
        if !schemas.contains_key(&version) {
            return Err(invalid_data("current schema version is missing from rocksdb").into());
        }
        if schema_spec.version < version {
            return Err(
                invalid_data(&schema_downgrade_message(version, schema_spec.version)).into(),
            );
        }
    } else if !schemas.is_empty() {
        return Err(invalid_data("stored schemas exist without a current schema version").into());
    }

    match schemas.get(&schema_spec.version) {
        Some(existing) if existing != schema_spec => {
            return Err(
                invalid_data(&schema_definition_conflict_message(existing, schema_spec)).into(),
            );
        }
        Some(_) => {
            if current_version != Some(schema_spec.version) {
                persist_current_schema_version(db, schema_spec.version)?;
            }
            return Ok(schemas);
        }
        None => {}
    }

    if let Some(version) = current_version {
        if schema_spec.version <= version {
            return Err(invalid_data(
                "new schema version must be greater than the current version",
            )
            .into());
        }
        validate_append_only_schema(
            schemas
                .get(&version)
                .ok_or_else(|| invalid_data("current schema version is missing from rocksdb"))?,
            schema_spec,
            &schemas,
        )?;
    }

    persist_schema(db, schema_spec)?;
    persist_current_schema_version(db, schema_spec.version)?;
    schemas.insert(schema_spec.version, schema_spec.clone());
    Ok(schemas)
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

fn clear_active_batch_metadata(batch: &mut WriteBatch) {
    batch.delete(ACTIVE_BATCH_START_ID_KEY.as_bytes());
    batch.delete(ACTIVE_BATCH_LEN_KEY.as_bytes());
    batch.delete(ACTIVE_BATCH_STATUS_KEY.as_bytes());
    batch.delete(ACTIVE_BATCH_SCHEMA_VERSION_KEY.as_bytes());
    batch.delete(ACTIVE_BATCH_TIMESTAMP_MIN_KEY.as_bytes());
    batch.delete(ACTIVE_BATCH_TIMESTAMP_MAX_KEY.as_bytes());
    batch.delete(ACTIVE_BATCH_UPLOADED_PUSHES_KEY.as_bytes());
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

pub fn load_active_batch(
    db: &DB,
) -> Result<Option<ActiveBatch>, Box<dyn std::error::Error + Send + Sync>> {
    let start_event_id = read_metadata_value(db, ACTIVE_BATCH_START_ID_KEY.as_bytes())?;
    let len = read_metadata_value(db, ACTIVE_BATCH_LEN_KEY.as_bytes())?;
    let status = read_active_batch_status(db)?;
    let schema_version = read_u32_metadata_value(db, ACTIVE_BATCH_SCHEMA_VERSION_KEY.as_bytes())?;
    let timestamp_min = read_timestamp_value(db, ACTIVE_BATCH_TIMESTAMP_MIN_KEY.as_bytes())?;
    let timestamp_max = read_timestamp_value(db, ACTIVE_BATCH_TIMESTAMP_MAX_KEY.as_bytes())?;

    match (start_event_id, len, status) {
        (None, None, None) => Ok(None),
        (Some(start_event_id), Some(len), Some(status)) => {
            let len = usize::try_from(len)
                .map_err(|_| invalid_data("active batch length does not fit in usize"))?;
            let (timestamp_min, timestamp_max) = match (timestamp_min, timestamp_max) {
                (Some(timestamp_min), Some(timestamp_max)) => (timestamp_min, timestamp_max),
                (None, None) => batch_timestamp_range(db, start_event_id, len)?,
                _ => {
                    return Err(invalid_data(
                        "incomplete active batch timestamp metadata in rocksdb",
                    )
                    .into());
                }
            };

            Ok(Some(ActiveBatch {
                start_event_id,
                len,
                status,
                schema_version: schema_version
                    .or(load_current_schema_version(db)?)
                    .unwrap_or(schema::DEFAULT_SCHEMA_VERSION),
                timestamp_min,
                timestamp_max,
            }))
        }
        _ => Err(invalid_data("incomplete active batch metadata in rocksdb").into()),
    }
}

pub fn load_active_batch_uploaded_pushes(
    db: &DB,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    read_active_batch_uploaded_pushes(db)
}

pub fn store_active_batch(
    db: &DB,
    active_batch: ActiveBatch,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut batch = WriteBatch::default();
    batch.put(
        ACTIVE_BATCH_START_ID_KEY.as_bytes(),
        metadata_bytes(active_batch.start_event_id),
    );
    batch.put(
        ACTIVE_BATCH_LEN_KEY.as_bytes(),
        metadata_bytes(active_batch.len as u64),
    );
    batch.put(
        ACTIVE_BATCH_STATUS_KEY.as_bytes(),
        [active_batch.status.as_byte()],
    );
    batch.put(
        ACTIVE_BATCH_SCHEMA_VERSION_KEY.as_bytes(),
        metadata_bytes(u64::from(active_batch.schema_version)),
    );
    batch.put(
        ACTIVE_BATCH_TIMESTAMP_MIN_KEY.as_bytes(),
        timestamp_bytes(active_batch.timestamp_min),
    );
    batch.put(
        ACTIVE_BATCH_TIMESTAMP_MAX_KEY.as_bytes(),
        timestamp_bytes(active_batch.timestamp_max),
    );
    batch.delete(ACTIVE_BATCH_UPLOADED_PUSHES_KEY.as_bytes());
    db.write(batch)?;
    Ok(())
}

pub fn mark_active_batch_push_uploaded(
    db: &DB,
    active_batch: ActiveBatch,
    push_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let push_name = push_name.trim();
    if push_name.is_empty() {
        return Err(invalid_data("uploaded push name cannot be empty").into());
    }

    let loaded = load_active_batch(db)?;
    if loaded.map(active_batch_identity) != Some(active_batch_identity(active_batch)) {
        return Err(invalid_data("active batch metadata does not match expected batch").into());
    }

    let mut uploaded_pushes = read_active_batch_uploaded_pushes(db)?;
    uploaded_pushes.insert(push_name.to_string());

    let mut batch = WriteBatch::default();
    batch.put(
        ACTIVE_BATCH_UPLOADED_PUSHES_KEY.as_bytes(),
        serde_json::to_vec(&uploaded_pushes)?,
    );
    db.write(batch)?;
    Ok(())
}

pub fn mark_active_batch_written(
    db: &DB,
    active_batch: ActiveBatch,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let loaded = load_active_batch(db)?;
    if loaded.map(active_batch_identity) != Some(active_batch_identity(active_batch)) {
        return Err(invalid_data("active batch metadata does not match expected batch").into());
    }

    let mut batch = WriteBatch::default();
    batch.put(
        ACTIVE_BATCH_STATUS_KEY.as_bytes(),
        [ActiveBatchStatus::Written.as_byte()],
    );
    db.write(batch)?;
    Ok(())
}

pub fn finalize_active_batch(
    db: &DB,
    active_batch: ActiveBatch,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let next_batch_start_id = active_batch.start_event_id + active_batch.len as u64;
    let mut batch = WriteBatch::default();

    for event_id in active_batch.start_event_id..next_batch_start_id {
        batch.delete(event_key(event_id).as_bytes());
    }

    batch.put(
        NEXT_BATCH_START_ID_KEY.as_bytes(),
        metadata_bytes(next_batch_start_id),
    );
    clear_active_batch_metadata(&mut batch);
    db.write(batch)?;

    Ok(next_batch_start_id)
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

pub fn batch_timestamp_range(
    db: &DB,
    start_event_id: u64,
    size: usize,
) -> Result<(i64, i64), Box<dyn std::error::Error + Send + Sync>> {
    if size == 0 {
        return Err(invalid_data("cannot read timestamp range for an empty batch").into());
    }

    let start_key = event_key(start_event_id);
    let iter = db.iterator(rocksdb::IteratorMode::From(
        start_key.as_bytes(),
        rocksdb::Direction::Forward,
    ));
    let mut timestamp_min = i64::MAX;
    let mut timestamp_max = i64::MIN;
    let mut seen = 0usize;

    for item in iter {
        if seen >= size {
            break;
        }

        let (key, value) = item?;
        if !key.starts_with(b"event_") {
            continue;
        }

        let event: TelemetryEvent = serde_json::from_slice(&value)?;
        timestamp_min = timestamp_min.min(event.timestamp);
        timestamp_max = timestamp_max.max(event.timestamp);
        seen += 1;
    }

    if seen != size {
        return Err(
            invalid_data("active batch is missing events required to compute timestamps").into(),
        );
    }

    Ok((timestamp_min, timestamp_max))
}

#[allow(dead_code)]
pub fn delete_batch(
    db: &DB,
    start_event_id: u64,
    size: usize,
    next_batch_start_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let loaded_batch = load_active_batch(db)?;
    let (schema_version, timestamp_min, timestamp_max) = match loaded_batch {
        Some(batch) => (
            batch.schema_version,
            batch.timestamp_min,
            batch.timestamp_max,
        ),
        None => {
            let (timestamp_min, timestamp_max) = batch_timestamp_range(db, start_event_id, size)?;
            (
                load_current_schema_version(db)?.unwrap_or(schema::DEFAULT_SCHEMA_VERSION),
                timestamp_min,
                timestamp_max,
            )
        }
    };
    let active_batch = ActiveBatch {
        start_event_id,
        len: size,
        status: ActiveBatchStatus::Written,
        schema_version,
        timestamp_min,
        timestamp_max,
    };

    let mut batch = WriteBatch::default();
    for event_id in start_event_id..start_event_id + size as u64 {
        batch.delete(event_key(event_id).as_bytes());
    }
    batch.put(
        NEXT_BATCH_START_ID_KEY.as_bytes(),
        metadata_bytes(next_batch_start_id),
    );
    if loaded_batch == Some(active_batch) {
        clear_active_batch_metadata(&mut batch);
    }
    db.write(batch)?;
    Ok(())
}
