use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::models::TelemetryEvent;

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
    std::fs::create_dir_all(output_dir)?;

    let final_path = parquet_file_path(output_dir, batch_start_id);
    if final_path.exists() {
        return Ok((final_path.to_string_lossy().into_owned(), events.len()));
    }

    let temp_path = parquet_temp_file_path(output_dir, batch_start_id);
    if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let mut session_builder = StringBuilder::new();
    let mut path_builder = StringBuilder::new();
    let mut event_name_builder = StringBuilder::new();
    let mut timestamp_builder = Int64Builder::new();

    for event in events {
        session_builder.append_value(&event.session_id);
        path_builder.append_value(&event.path);
        event_name_builder.append_value(&event.event_name);
        timestamp_builder.append_value(event.timestamp);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(session_builder.finish()),
            Arc::new(path_builder.finish()),
            Arc::new(event_name_builder.finish()),
            Arc::new(timestamp_builder.finish()),
        ],
    )?;

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

#[cfg(test)]
mod tests;
