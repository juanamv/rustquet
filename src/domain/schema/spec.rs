use std::collections::BTreeSet;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushArtifact {
    Parquet,
    Manifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushKind {
    S3Compatible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushEnvSpec {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub virtual_hosted_style: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushSpec {
    pub name: String,
    pub kind: PushKind,
    #[serde(default)]
    pub artifacts: Option<Vec<PushArtifact>>,
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    pub endpoint: String,
    pub region: String,
    #[serde(default)]
    pub allow_http: Option<bool>,
    pub env: PushEnvSpec,
}

impl PushSpec {
    pub fn effective_artifacts(&self, write_manifest: bool) -> Vec<PushArtifact> {
        match &self.artifacts {
            Some(artifacts) => artifacts.clone(),
            None => {
                let mut artifacts = vec![PushArtifact::Parquet];
                if write_manifest {
                    artifacts.push(PushArtifact::Manifest);
                }
                artifacts
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigSpec {
    pub schema: SchemaSpec,
    pub dataset: DatasetSpec,
    pub push: Vec<PushSpec>,
}

#[derive(Deserialize)]
struct ConfigFile {
    schema: SchemaFile,
    #[serde(default)]
    dataset: DatasetSpec,
    #[serde(default)]
    push: Vec<PushSpec>,
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

fn validate_non_empty_field(
    value: &str,
    field_name: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(invalid_data(&format!("{field_name} cannot be empty")).into());
    }

    Ok(trimmed.to_string())
}

fn validate_optional_non_empty_field(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    value
        .map(|value| validate_non_empty_field(&value, field_name))
        .transpose()
}

fn register_push_env_name(
    names: &mut BTreeSet<String>,
    push_name: &str,
    field_name: &str,
    env_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if names.insert(env_name.to_string()) {
        return Ok(());
    }

    Err(invalid_data(&format!(
        "push '{push_name}' reuses env var '{env_name}' for multiple fields, including {field_name}"
    ))
    .into())
}

fn validate_push_spec(
    push: PushSpec,
    write_manifest: bool,
    names: &mut BTreeSet<String>,
) -> Result<PushSpec, Box<dyn std::error::Error + Send + Sync>> {
    let name = validate_non_empty_field(&push.name, "push.name")?;
    if !names.insert(name.clone()) {
        return Err(invalid_data(&format!("duplicate push name: {name}")).into());
    }

    let artifacts = if let Some(artifacts) = push.artifacts {
        if artifacts.is_empty() {
            return Err(invalid_data(&format!(
                "push '{name}' artifacts cannot be empty when provided"
            ))
            .into());
        }

        let mut seen_artifacts = BTreeSet::new();
        for artifact in &artifacts {
            if !seen_artifacts.insert(*artifact) {
                return Err(invalid_data(&format!(
                    "push '{name}' contains duplicate artifact '{artifact:?}'"
                ))
                .into());
            }
        }

        if !write_manifest && artifacts.contains(&PushArtifact::Manifest) {
            return Err(invalid_data(&format!(
                "push '{name}' cannot upload manifests when dataset.write_manifest is false"
            ))
            .into());
        }

        Some(artifacts)
    } else {
        None
    };

    let bucket = validate_non_empty_field(&push.bucket, &format!("push '{name}' bucket"))?;
    let prefix = validate_optional_non_empty_field(push.prefix, &format!("push '{name}' prefix"))?;
    let endpoint = validate_non_empty_field(&push.endpoint, &format!("push '{name}' endpoint"))?;
    let region = validate_non_empty_field(&push.region, &format!("push '{name}' region"))?;

    let mut seen_env_names = BTreeSet::new();
    let access_key_id = validate_non_empty_field(
        &push.env.access_key_id,
        &format!("push '{name}' env.access_key_id"),
    )?;
    register_push_env_name(
        &mut seen_env_names,
        &name,
        "env.access_key_id",
        &access_key_id,
    )?;

    let secret_access_key = validate_non_empty_field(
        &push.env.secret_access_key,
        &format!("push '{name}' env.secret_access_key"),
    )?;
    register_push_env_name(
        &mut seen_env_names,
        &name,
        "env.secret_access_key",
        &secret_access_key,
    )?;

    let session_token = validate_optional_non_empty_field(
        push.env.session_token,
        &format!("push '{name}' env.session_token"),
    )?;
    if let Some(session_token) = &session_token {
        register_push_env_name(
            &mut seen_env_names,
            &name,
            "env.session_token",
            session_token,
        )?;
    }

    let virtual_hosted_style = validate_optional_non_empty_field(
        push.env.virtual_hosted_style,
        &format!("push '{name}' env.virtual_hosted_style"),
    )?;
    if let Some(virtual_hosted_style) = &virtual_hosted_style {
        register_push_env_name(
            &mut seen_env_names,
            &name,
            "env.virtual_hosted_style",
            virtual_hosted_style,
        )?;
    }

    Ok(PushSpec {
        name,
        kind: push.kind,
        artifacts,
        bucket,
        prefix,
        endpoint,
        region,
        allow_http: push.allow_http,
        env: PushEnvSpec {
            access_key_id,
            secret_access_key,
            session_token,
            virtual_hosted_style,
        },
    })
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
    let mut push_names = BTreeSet::new();

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
        push: file
            .push
            .into_iter()
            .map(|push| validate_push_spec(push, file.dataset.write_manifest, &mut push_names))
            .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?,
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
