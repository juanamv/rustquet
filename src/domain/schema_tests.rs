use std::io::Write;

use tempfile::NamedTempFile;

use super::schema::{
    ColumnType, ConfigSpec, DatasetSpec, IndexedColumn, load_default_config, load_default_schema,
    load_schema_from_path,
};

fn config_with_schema(schema_body: &str) -> String {
    format!("{{\"dataset\":{{\"write_manifest\":true}},\"schema\":{schema_body}}}")
}

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
fn test_load_default_config_contains_schema_and_dataset() {
    let config = load_default_config().unwrap();

    assert_eq!(
        config,
        ConfigSpec {
            schema: load_default_schema().unwrap(),
            dataset: DatasetSpec {
                write_manifest: true
            },
        }
    );
}

#[test]
fn test_load_schema_from_path_rejects_non_metadata_source() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{}",
        config_with_schema(
            "{\"schema_version\":1,\"columns\":[{\"name\":\"slot\",\"source\":\"slot\",\"type\":\"string\"}]}"
        )
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
    writeln!(file, "{{\"dataset\":{{}},\"schema\":").unwrap();

    assert!(load_schema_from_path(file.path()).is_err());
}

#[test]
fn test_load_schema_from_path_rejects_empty_metadata_source() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{}",
        config_with_schema(
            "{\"schema_version\":1,\"columns\":[{\"name\":\"slot\",\"source\":\"metadata.\",\"type\":\"string\"}]}"
        )
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
        "{}",
        config_with_schema(
            "{\"schema_version\":1,\"columns\":[{\"name\":\"slot\",\"source\":\"metadata.slot\",\"type\":\"int\"}]}"
        )
    )
    .unwrap();

    let error = load_schema_from_path(file.path()).unwrap_err();
    assert!(error.to_string().contains("unknown variant"));
}

#[test]
fn test_load_schema_from_path_accepts_number_column_type() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{}",
        config_with_schema(
            "{\"schema_version\":1,\"columns\":[{\"name\":\"price\",\"source\":\"metadata.price\",\"type\":\"number\"}]}"
        )
    )
    .unwrap();

    let schema = load_schema_from_path(file.path()).unwrap();

    assert_eq!(schema.version, 1);
    assert_eq!(
        schema.columns,
        vec![IndexedColumn::new("price", &["price"], ColumnType::Number)]
    );
}
