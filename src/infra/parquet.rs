use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

use crate::domain::models::TelemetryEvent;
use crate::domain::schema::{self, ColumnType, SchemaSpec};

enum IndexedBuilder {
    Bool(BooleanBuilder),
    String(StringBuilder),
}

impl IndexedBuilder {
    fn new(kind: ColumnType) -> Self {
        match kind {
            ColumnType::Bool => Self::Bool(BooleanBuilder::new()),
            ColumnType::String => Self::String(StringBuilder::new()),
        }
    }

    fn field(&self, name: &str) -> Field {
        match self {
            Self::Bool(_) => Field::new(name, DataType::Boolean, true),
            Self::String(_) => Field::new(name, DataType::Utf8, true),
        }
    }

    fn append(&mut self, value: Option<&Value>) {
        match (self, value) {
            (Self::Bool(builder), Some(Value::Bool(value))) => builder.append_value(*value),
            (Self::Bool(builder), _) => builder.append_null(),
            (Self::String(builder), Some(Value::String(value))) => builder.append_value(value),
            (Self::String(builder), _) => builder.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut builder) => Arc::new(builder.finish()),
            Self::String(mut builder) => Arc::new(builder.finish()),
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

pub fn parquet_file_path(output_dir: &str, batch_start_id: u64) -> PathBuf {
    PathBuf::from(output_dir).join(format!("batch_{batch_start_id:010}.parquet"))
}

fn parquet_temp_file_path(output_dir: &str, batch_start_id: u64) -> PathBuf {
    PathBuf::from(output_dir).join(format!("batch_{batch_start_id:010}.parquet.tmp"))
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
    std::fs::create_dir_all(output_dir)?;

    let final_path = parquet_file_path(output_dir, batch_start_id);
    if final_path.exists() {
        return Ok((final_path.to_string_lossy().into_owned(), events.len()));
    }

    let temp_path = parquet_temp_file_path(output_dir, batch_start_id);
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
