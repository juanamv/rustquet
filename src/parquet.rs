use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::config::PARQUET_OUTPUT_DIR;
use crate::models::TelemetryEvent;

pub fn write_parquet(
    events: &[TelemetryEvent],
    batch_index: u64,
) -> Result<(String, usize), Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(PARQUET_OUTPUT_DIR)?;

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

    let file_name = format!(
        "{}/batch_{:010}_{}.parquet",
        PARQUET_OUTPUT_DIR,
        batch_index,
        uuid::Uuid::now_v7()
    );

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(Cursor::new(&mut buf), schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }

    std::fs::write(&file_name, &buf)?;

    Ok((file_name, events.len()))
}
