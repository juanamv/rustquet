use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG_PATH: &str = "config.json";
pub const DEFAULT_SCHEMA_VERSION: u32 = 1;
pub const FIXED_COLUMNS: [&str; 5] = ["id", "path", "event_name", "timestamp", "metadata"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    #[serde(alias = "boolean")]
    Bool,
    String,
    Number,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedColumn {
    pub name: String,
    pub path: Vec<String>,
    pub kind: ColumnType,
}

impl IndexedColumn {
    pub fn new(name: &str, path: &[&str], kind: ColumnType) -> Self {
        Self {
            name: name.into(),
            path: path.iter().map(|segment| (*segment).into()).collect(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSpec {
    pub version: u32,
    pub columns: Vec<IndexedColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSpec {
    #[serde(default = "default_write_manifest")]
    pub write_manifest: bool,
}

fn default_write_manifest() -> bool {
    true
}

impl Default for DatasetSpec {
    fn default() -> Self {
        Self {
            write_manifest: default_write_manifest(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigSpec {
    pub schema: SchemaSpec,
    pub dataset: DatasetSpec,
}

#[derive(Deserialize)]
struct ConfigFile {
    schema: SchemaFile,
    #[serde(default)]
    dataset: DatasetSpec,
}

#[derive(Deserialize)]
struct SchemaFile {
    schema_version: u32,
    columns: Vec<SchemaFileColumn>,
}

#[derive(Deserialize)]
struct SchemaFileColumn {
    name: String,
    source: String,
    #[serde(rename = "type")]
    kind: ColumnType,
}

fn invalid_data(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn parse_source_path(
    source: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let path = source
        .strip_prefix("metadata.")
        .ok_or_else(|| invalid_data("schema column source must start with 'metadata.'"))?
        .split('.')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if path.is_empty() {
        return Err(invalid_data("schema column source must point to a metadata field").into());
    }

    Ok(path)
}

pub fn load_schema_from_path(
    path: impl AsRef<Path>,
) -> Result<SchemaSpec, Box<dyn std::error::Error + Send + Sync>> {
    Ok(load_config_from_path(path)?.schema)
}

pub fn load_config_from_path(
    path: impl AsRef<Path>,
) -> Result<ConfigSpec, Box<dyn std::error::Error + Send + Sync>> {
    let raw = fs::read_to_string(path)?;
    let file: ConfigFile = serde_json::from_str(&raw)?;

    Ok(ConfigSpec {
        schema: SchemaSpec {
            version: file.schema.schema_version,
            columns: file
                .schema
                .columns
                .into_iter()
                .map(|column| {
                    Ok(IndexedColumn {
                        name: column.name,
                        path: parse_source_path(&column.source)?,
                        kind: column.kind,
                    })
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?,
        },
        dataset: file.dataset,
    })
}

pub fn load_default_config() -> Result<ConfigSpec, Box<dyn std::error::Error + Send + Sync>> {
    load_config_from_path(DEFAULT_CONFIG_PATH)
}

pub fn load_default_schema() -> Result<SchemaSpec, Box<dyn std::error::Error + Send + Sync>> {
    load_default_config().map(|config| config.schema)
}

pub fn is_reserved_column_name(name: &str) -> bool {
    FIXED_COLUMNS.contains(&name)
}
