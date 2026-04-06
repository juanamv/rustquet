use std::io::Write;

use tempfile::NamedTempFile;

use super::schema::{
    ColumnType, ConfigSpec, DatasetSpec, IndexedColumn, PushEnvSpec, PushKind, PushSpec,
    load_config_from_path, load_default_config, load_default_schema, load_schema_from_path,
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
            push: Vec::new(),
        }
    );
}

#[test]
fn test_load_config_from_path_reads_push_targets() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"dataset\":{{\"write_manifest\":true}},\"push\":[{{\"name\":\"lake_primary\",\"kind\":\"s3_compatible\",\"env\":{{\"bucket\":\"LAKE_BUCKET\",\"prefix\":\"LAKE_PREFIX\",\"endpoint\":\"LAKE_ENDPOINT\",\"region\":\"LAKE_REGION\",\"access_key_id\":\"LAKE_ACCESS_KEY_ID\",\"secret_access_key\":\"LAKE_SECRET_ACCESS_KEY\",\"allow_http\":\"LAKE_ALLOW_HTTP\"}}}}],\"schema\":{{\"schema_version\":1,\"columns\":[]}}}}"
    )
    .unwrap();

    let config = load_config_from_path(file.path()).unwrap();

    assert_eq!(
        config.push,
        vec![PushSpec {
            name: "lake_primary".to_string(),
            kind: PushKind::S3Compatible,
            artifacts: None,
            env: PushEnvSpec {
                bucket: "LAKE_BUCKET".to_string(),
                prefix: Some("LAKE_PREFIX".to_string()),
                endpoint: "LAKE_ENDPOINT".to_string(),
                region: "LAKE_REGION".to_string(),
                access_key_id: "LAKE_ACCESS_KEY_ID".to_string(),
                secret_access_key: "LAKE_SECRET_ACCESS_KEY".to_string(),
                session_token: None,
                allow_http: Some("LAKE_ALLOW_HTTP".to_string()),
                virtual_hosted_style: None,
            },
        }]
    );
}

#[test]
fn test_load_config_from_path_rejects_duplicate_push_names() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"dataset\":{{\"write_manifest\":true}},\"push\":[{{\"name\":\"lake\",\"kind\":\"s3_compatible\",\"env\":{{\"bucket\":\"A_BUCKET\",\"endpoint\":\"A_ENDPOINT\",\"region\":\"A_REGION\",\"access_key_id\":\"A_KEY\",\"secret_access_key\":\"A_SECRET\"}}}},{{\"name\":\"lake\",\"kind\":\"s3_compatible\",\"env\":{{\"bucket\":\"B_BUCKET\",\"endpoint\":\"B_ENDPOINT\",\"region\":\"B_REGION\",\"access_key_id\":\"B_KEY\",\"secret_access_key\":\"B_SECRET\"}}}}],\"schema\":{{\"schema_version\":1,\"columns\":[]}}}}"
    )
    .unwrap();

    let error = load_config_from_path(file.path()).unwrap_err();

    assert!(error.to_string().contains("duplicate push name"));
}

#[test]
fn test_load_config_from_path_rejects_manifest_push_when_manifest_disabled() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"dataset\":{{\"write_manifest\":false}},\"push\":[{{\"name\":\"lake\",\"kind\":\"s3_compatible\",\"artifacts\":[\"manifest\"],\"env\":{{\"bucket\":\"LAKE_BUCKET\",\"endpoint\":\"LAKE_ENDPOINT\",\"region\":\"LAKE_REGION\",\"access_key_id\":\"LAKE_KEY\",\"secret_access_key\":\"LAKE_SECRET\"}}}}],\"schema\":{{\"schema_version\":1,\"columns\":[]}}}}"
    )
    .unwrap();

    let error = load_config_from_path(file.path()).unwrap_err();

    assert!(error.to_string().contains("dataset.write_manifest"));
}

#[test]
fn test_load_config_from_path_rejects_duplicate_push_artifacts() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"dataset\":{{\"write_manifest\":true}},\"push\":[{{\"name\":\"lake\",\"kind\":\"s3_compatible\",\"artifacts\":[\"parquet\",\"parquet\"],\"env\":{{\"bucket\":\"LAKE_BUCKET\",\"endpoint\":\"LAKE_ENDPOINT\",\"region\":\"LAKE_REGION\",\"access_key_id\":\"LAKE_KEY\",\"secret_access_key\":\"LAKE_SECRET\"}}}}],\"schema\":{{\"schema_version\":1,\"columns\":[]}}}}"
    )
    .unwrap();

    let error = load_config_from_path(file.path()).unwrap_err();

    assert!(error.to_string().contains("duplicate artifact"));
}

#[test]
fn test_load_config_from_path_rejects_reused_push_env_names() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        "{{\"dataset\":{{\"write_manifest\":true}},\"push\":[{{\"name\":\"lake\",\"kind\":\"s3_compatible\",\"env\":{{\"bucket\":\"LAKE_ENV\",\"endpoint\":\"LAKE_ENV\",\"region\":\"LAKE_REGION\",\"access_key_id\":\"LAKE_KEY\",\"secret_access_key\":\"LAKE_SECRET\"}}}}],\"schema\":{{\"schema_version\":1,\"columns\":[]}}}}"
    )
    .unwrap();

    let error = load_config_from_path(file.path()).unwrap_err();

    assert!(error.to_string().contains("reuses env var"));
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
