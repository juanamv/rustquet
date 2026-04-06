use std::io::Cursor;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

use crate::domain::models::TelemetryEvent;
use crate::domain::schema::{self, ColumnType, SchemaSpec};
use crate::infra::layout;
use crate::infra::storage::ActiveBatch;

enum IndexedBuilder {
    Bool(BooleanBuilder),
    String(StringBuilder),
    Number(Float64Builder),
}

impl IndexedBuilder {
    fn new(kind: ColumnType) -> Self {
        match kind {
            ColumnType::Bool => Self::Bool(BooleanBuilder::new()),
            ColumnType::String => Self::String(StringBuilder::new()),
            ColumnType::Number => Self::Number(Float64Builder::new()),
        }
    }

    fn field(&self, name: &str) -> Field {
        match self {
            Self::Bool(_) => Field::new(name, DataType::Boolean, true),
            Self::String(_) => Field::new(name, DataType::Utf8, true),
            Self::Number(_) => Field::new(name, DataType::Float64, true),
        }
    }

    fn append(&mut self, value: Option<&Value>) {
        match (self, value) {
            (Self::Bool(builder), Some(Value::Bool(value))) => builder.append_value(*value),
            (Self::Bool(builder), _) => builder.append_null(),
            (Self::String(builder), Some(Value::String(value))) => builder.append_value(value),
            (Self::String(builder), _) => builder.append_null(),
            (Self::Number(builder), Some(Value::Number(value))) => {
                if let Some(value) = value.as_f64() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            (Self::Number(builder), _) => builder.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut builder) => Arc::new(builder.finish()),
            Self::String(mut builder) => Arc::new(builder.finish()),
            Self::Number(mut builder) => Arc::new(builder.finish()),
        }
    }
}

fn metadata_value<'a>(metadata: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut value = metadata;
    for segment in path {
        value = value.get(segment)?;
    }
    Some(value)
}

fn invalid_data(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn event_timestamp_range(
    events: &[TelemetryEvent],
) -> Result<(i64, i64), Box<dyn std::error::Error + Send + Sync>> {
    let Some(first) = events.first() else {
        return Err(invalid_data("cannot write parquet for an empty batch").into());
    };

    let mut timestamp_min = first.timestamp;
    let mut timestamp_max = first.timestamp;
    for event in &events[1..] {
        timestamp_min = timestamp_min.min(event.timestamp);
        timestamp_max = timestamp_max.max(event.timestamp);
    }

    Ok((timestamp_min, timestamp_max))
}

pub fn parquet_file_path(output_dir: &str, batch_start_id: u64) -> PathBuf {
    layout::find_batch_file(Path::new(output_dir), batch_start_id, "parquet").unwrap_or_else(|| {
        PathBuf::from(output_dir).join(format!("batch_{batch_start_id:010}.parquet"))
    })
}

pub fn parquet_file_path_for_batch(output_dir: &str, batch: ActiveBatch) -> PathBuf {
    layout::partition_directory(
        Path::new(output_dir),
        batch.timestamp_min,
        batch.timestamp_max,
    )
    .join(layout::batch_file_name(
        batch.start_event_id,
        batch.timestamp_min,
        batch.timestamp_max,
        "parquet",
    ))
}

pub fn parquet_temp_file_path_for_batch(output_dir: &str, batch: ActiveBatch) -> PathBuf {
    let final_path = parquet_file_path_for_batch(output_dir, batch);
    final_path.with_extension("parquet.tmp")
}

pub fn write_parquet(
    events: &[TelemetryEvent],
    batch_start_id: u64,
    output_dir: &str,
) -> Result<(String, usize), Box<dyn std::error::Error + Send + Sync>> {
    let schema_spec = schema::load_default_schema()?;
    write_parquet_with_schema(events, batch_start_id, output_dir, &schema_spec)
}

pub fn write_parquet_with_schema(
    events: &[TelemetryEvent],
    batch_start_id: u64,
    output_dir: &str,
    schema_spec: &SchemaSpec,
) -> Result<(String, usize), Box<dyn std::error::Error + Send + Sync>> {
    let (timestamp_min, timestamp_max) = event_timestamp_range(events)?;
    write_parquet_with_layout(
        events,
        batch_start_id,
        timestamp_min,
        timestamp_max,
        output_dir,
        schema_spec,
    )
}

pub fn write_parquet_batch_with_schema(
    events: &[TelemetryEvent],
    batch: ActiveBatch,
    output_dir: &str,
    schema_spec: &SchemaSpec,
) -> Result<(String, usize), Box<dyn std::error::Error + Send + Sync>> {
    if events.len() != batch.len {
        return Err(invalid_data("batch metadata does not match parquet row count").into());
    }

    write_parquet_with_layout(
        events,
        batch.start_event_id,
        batch.timestamp_min,
        batch.timestamp_max,
        output_dir,
        schema_spec,
    )
}

fn write_parquet_with_layout(
    events: &[TelemetryEvent],
    batch_start_id: u64,
    timestamp_min: i64,
    timestamp_max: i64,
    output_dir: &str,
    schema_spec: &SchemaSpec,
) -> Result<(String, usize), Box<dyn std::error::Error + Send + Sync>> {
    let partition_dir =
        layout::partition_directory(Path::new(output_dir), timestamp_min, timestamp_max);
    std::fs::create_dir_all(&partition_dir)?;

    let batch = ActiveBatch {
        start_event_id: batch_start_id,
        len: events.len(),
        status: crate::infra::storage::ActiveBatchStatus::Writing,
        schema_version: schema_spec.version,
        timestamp_min,
        timestamp_max,
    };
    let final_path = parquet_file_path_for_batch(output_dir, batch);
    if final_path.exists() {
        return Ok((final_path.to_string_lossy().into_owned(), events.len()));
    }

    let temp_path = parquet_temp_file_path_for_batch(output_dir, batch);
    if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }

    let mut indexed_builders: Vec<_> = schema_spec
        .columns
        .iter()
        .map(|column| IndexedBuilder::new(column.kind))
        .collect();
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("metadata", DataType::Utf8, false),
    ];
    fields.extend(
        schema_spec
            .columns
            .iter()
            .zip(indexed_builders.iter())
            .map(|(column, builder)| builder.field(&column.name)),
    );
    let schema = Arc::new(Schema::new(fields));

    let mut id_builder = StringBuilder::new();
    let mut path_builder = StringBuilder::new();
    let mut event_name_builder = StringBuilder::new();
    let mut timestamp_builder = Int64Builder::new();
    let mut metadata_builder = StringBuilder::new();

    for event in events {
        id_builder.append_value(&event.id);
        path_builder.append_value(&event.path);
        event_name_builder.append_value(&event.event_name);
        timestamp_builder.append_value(event.timestamp);
        metadata_builder.append_value(event.metadata.to_string());
        for (column, builder) in schema_spec.columns.iter().zip(indexed_builders.iter_mut()) {
            builder.append(metadata_value(&event.metadata, &column.path));
        }
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(id_builder.finish()),
        Arc::new(path_builder.finish()),
        Arc::new(event_name_builder.finish()),
        Arc::new(timestamp_builder.finish()),
        Arc::new(metadata_builder.finish()),
    ];
    columns.extend(indexed_builders.into_iter().map(IndexedBuilder::finish));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(Cursor::new(&mut buffer), schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }

    std::fs::write(&temp_path, &buffer)?;
    std::fs::rename(&temp_path, &final_path)?;

    Ok((final_path.to_string_lossy().into_owned(), events.len()))
}
