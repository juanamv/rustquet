use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::models::TelemetryEvent;

pub fn write_parquet(
    events: &[TelemetryEvent],
    batch_index: u64,
    output_dir: &str,
) -> Result<(String, usize), Box<dyn std::error::Error + Send + Sync>> {
    std::fs::create_dir_all(output_dir)?;

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
        output_dir,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TelemetryEvent;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    fn test_events(n: usize) -> Vec<TelemetryEvent> {
        (0..n)
            .map(|i| TelemetryEvent {
                session_id: format!("sess-{i}"),
                path: format!("/page/{i}"),
                event_name: "test_click".into(),
                timestamp: 1_000_000 + i as i64,
            })
            .collect()
    }

    #[test]
    fn test_write_and_read_back_parquet() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let events = test_events(10);
        let (path, count) = write_parquet(&events, 0, dir).unwrap();
        assert_eq!(count, 10);
        assert!(std::path::Path::new(&path).exists());

        let file = std::fs::File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        let mut total_rows = 0usize;
        for _ in reader.get_row_iter(None).unwrap() {
            total_rows += 1;
        }
        assert_eq!(total_rows, 10);
    }

    #[test]
    fn test_parquet_schema_has_four_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let events = test_events(1);
        let (path, _) = write_parquet(&events, 99, dir).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema();
        assert_eq!(schema.get_fields().len(), 4);
    }
}
