use std::io::Write;

use tempfile::NamedTempFile;

use super::schema::{ColumnType, IndexedColumn, load_default_schema, load_schema_from_path};

#[test]
fn test_load_default_schema_contains_expected_indexed_columns() {
    let schema = load_default_schema().unwrap();
    assert_eq!(schema.version, 1);
    assert_eq!(
        schema.columns,
        vec![
            IndexedColumn::new("h2o", &["h2o"], ColumnType::Bool),
            IndexedColumn::new("slot", &["slot"], ColumnType::String),
        ]
    );
}

#[test]
fn test_load_schema_from_path_rejects_non_metadata_source() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"schema_version\":1,\"columns\":[{{\"name\":\"slot\",\"source\":\"slot\",\"type\":\"string\"}}]}}"
    )
    .unwrap();

    let error = load_schema_from_path(file.path()).unwrap_err();
    assert!(error.to_string().contains("metadata"));
}
