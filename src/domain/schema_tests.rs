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

#[test]
fn test_load_schema_from_path_reads_versioned_fixture() {
    let schema = load_schema_from_path("config_v2.json").unwrap();

    assert_eq!(schema.version, 2);
    assert_eq!(
        schema.columns,
        vec![
            IndexedColumn::new("h2o", &["h2o"], ColumnType::Bool),
            IndexedColumn::new("slot", &["slot"], ColumnType::String),
            IndexedColumn::new("region", &["region"], ColumnType::String),
        ]
    );
}

#[test]
fn test_load_schema_from_path_reads_nested_fixture() {
    let schema = load_schema_from_path("config_v3_nested.json").unwrap();

    assert_eq!(schema.version, 3);
    assert_eq!(
        schema.columns[3],
        IndexedColumn::new("country", &["geo", "country"], ColumnType::String)
    );
}

#[test]
fn test_load_schema_from_path_rejects_invalid_json() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "{{\"schema_version\":").unwrap();

    let error = load_schema_from_path(file.path()).unwrap_err();
    assert!(error.to_string().contains("EOF") || error.to_string().contains("expected"));
}

#[test]
fn test_load_schema_from_path_rejects_empty_metadata_source() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"schema_version\":1,\"columns\":[{{\"name\":\"slot\",\"source\":\"metadata.\",\"type\":\"string\"}}]}}"
    )
    .unwrap();

    let error = load_schema_from_path(file.path()).unwrap_err();
    assert!(error.to_string().contains("metadata field"));
}

#[test]
fn test_load_schema_from_path_rejects_invalid_column_type() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"schema_version\":1,\"columns\":[{{\"name\":\"slot\",\"source\":\"metadata.slot\",\"type\":\"int\"}}]}}"
    )
    .unwrap();

    let error = load_schema_from_path(file.path()).unwrap_err();
    assert!(error.to_string().contains("unknown variant"));
}
