use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
#[cfg(test)]
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::runtime::Handle;

use crate::domain::schema::{PushArtifact, PushKind, PushSpec};
use crate::infra::{layout, manifest, parquet};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UploadedBatchArtifacts {
    pub parquet_uris: Vec<String>,
    pub manifest_uri: Option<String>,
}

pub struct PushTarget {
    name: String,
    kind: PushKind,
    artifacts: Vec<PushArtifact>,
    bucket: String,
    prefix: String,
    endpoint: String,
    region: String,
    store: Arc<dyn ObjectStore>,
}

fn invalid_input(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, message.into())
}

fn normalize_object_prefix(value: &str) -> String {
    value
        .trim()
        .replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

fn resolve_required_env_value(
    env_getter: &dyn Fn(&str) -> Option<String>,
    push_name: &str,
    field_name: &str,
    env_name: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let value = env_getter(env_name).ok_or_else(|| {
        invalid_input(format!(
            "push '{push_name}' requires env var '{env_name}' for {field_name}"
        ))
    })?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(invalid_input(format!(
            "push '{push_name}' env var '{env_name}' for {field_name} cannot be empty"
        ))
        .into());
    }

    Ok(trimmed.to_string())
}

fn resolve_optional_env_value(
    env_getter: &dyn Fn(&str) -> Option<String>,
    push_name: &str,
    field_name: &str,
    env_name: Option<&str>,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    env_name
        .map(|env_name| resolve_required_env_value(env_getter, push_name, field_name, env_name))
        .transpose()
}

fn parse_bool_env_value(
    push_name: &str,
    field_name: &str,
    env_name: &str,
    value: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    value.parse::<bool>().map_err(|error| {
        invalid_input(format!(
            "push '{push_name}' env var '{env_name}' for {field_name} must be 'true' or 'false': {error}"
        ))
        .into()
    })
}

impl PushTarget {
    fn from_spec(
        spec: &PushSpec,
        write_manifest: bool,
        env_getter: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bucket =
            resolve_required_env_value(env_getter, &spec.name, "bucket", &spec.env.bucket)?;
        let endpoint =
            resolve_required_env_value(env_getter, &spec.name, "endpoint", &spec.env.endpoint)?;
        let region =
            resolve_required_env_value(env_getter, &spec.name, "region", &spec.env.region)?;
        let access_key_id = resolve_required_env_value(
            env_getter,
            &spec.name,
            "access_key_id",
            &spec.env.access_key_id,
        )?;
        let secret_access_key = resolve_required_env_value(
            env_getter,
            &spec.name,
            "secret_access_key",
            &spec.env.secret_access_key,
        )?;
        let prefix = resolve_optional_env_value(
            env_getter,
            &spec.name,
            "prefix",
            spec.env.prefix.as_deref(),
        )?
        .map(|value| normalize_object_prefix(&value))
        .unwrap_or_default();
        let session_token = resolve_optional_env_value(
            env_getter,
            &spec.name,
            "session_token",
            spec.env.session_token.as_deref(),
        )?;
        let allow_http = resolve_optional_env_value(
            env_getter,
            &spec.name,
            "allow_http",
            spec.env.allow_http.as_deref(),
        )?
        .as_deref()
        .map(|value| {
            parse_bool_env_value(
                &spec.name,
                "allow_http",
                spec.env.allow_http.as_deref().unwrap_or_default(),
                value,
            )
        })
        .transpose()?
        .unwrap_or(false);
        let virtual_hosted_style = resolve_optional_env_value(
            env_getter,
            &spec.name,
            "virtual_hosted_style",
            spec.env.virtual_hosted_style.as_deref(),
        )?
        .as_deref()
        .map(|value| {
            parse_bool_env_value(
                &spec.name,
                "virtual_hosted_style",
                spec.env.virtual_hosted_style.as_deref().unwrap_or_default(),
                value,
            )
        })
        .transpose()?
        .unwrap_or(false);

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_endpoint(&endpoint)
            .with_region(&region)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .with_allow_http(allow_http)
            .with_virtual_hosted_style_request(virtual_hosted_style);

        if let Some(session_token) = session_token {
            builder = builder.with_token(session_token);
        }

        let store = builder.build()?;

        Ok(Self {
            name: spec.name.clone(),
            kind: spec.kind,
            artifacts: spec.effective_artifacts(write_manifest),
            bucket,
            prefix,
            endpoint,
            region,
            store: Arc::new(store),
        })
    }

    #[cfg(test)]
    pub(crate) fn mock_s3_compatible(
        name: &str,
        artifacts: Vec<PushArtifact>,
        bucket: &str,
        prefix: &str,
    ) -> Self {
        Self {
            name: name.to_string(),
            kind: PushKind::S3Compatible,
            artifacts,
            bucket: bucket.to_string(),
            prefix: normalize_object_prefix(prefix),
            endpoint: "memory://mock".to_string(),
            region: "test-region-1".to_string(),
            store: Arc::new(InMemory::new()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> PushKind {
        self.kind
    }

    pub fn artifacts(&self) -> &[PushArtifact] {
        &self.artifacts
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    fn uploads_parquet(&self) -> bool {
        self.artifacts.contains(&PushArtifact::Parquet)
    }

    fn uploads_manifest(&self) -> bool {
        self.artifacts.contains(&PushArtifact::Manifest)
    }

    fn object_path(&self, dataset_path: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(dataset_path.to_string())
        } else {
            ObjectPath::from(format!("{}/{dataset_path}", self.prefix))
        }
    }

    fn object_uri(&self, location: &ObjectPath) -> String {
        format!("s3://{}/{}", self.bucket, location)
    }

    fn upload_local_file(
        &self,
        runtime_handle: &Handle,
        local_path: &Path,
        dataset_path: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let location = self.object_path(dataset_path);
        let payload = std::fs::read(local_path)?;
        let _ = runtime_handle.block_on(self.store.put(&location, payload.into()))?;

        Ok(self.object_uri(&location))
    }

    pub fn upload_batch_artifacts(
        &self,
        runtime_handle: &Handle,
        output_dir: &str,
        parquet_files: &[parquet::ParquetDataFile],
        manifest_path: Option<&Path>,
    ) -> Result<UploadedBatchArtifacts, Box<dyn std::error::Error + Send + Sync>> {
        let parquet_uris = if self.uploads_parquet() {
            parquet_files
                .iter()
                .map(|file| {
                    let parquet_path = Path::new(&file.path);
                    let parquet_dataset_path =
                        layout::dataset_file_path(Path::new(output_dir), parquet_path)?;
                    self.upload_local_file(runtime_handle, parquet_path, &parquet_dataset_path)
                })
                .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?
        } else {
            Vec::new()
        };

        let manifest_uri = if self.uploads_manifest() {
            let manifest_path = manifest_path.ok_or_else(|| {
                invalid_input(format!(
                    "push '{}' requires a manifest artifact but dataset.write_manifest is disabled",
                    self.name
                ))
            })?;
            let manifest_dataset_path =
                layout::dataset_file_path(&manifest::manifest_root(output_dir), manifest_path)?;
            Some(self.upload_local_file(runtime_handle, manifest_path, &manifest_dataset_path)?)
        } else {
            None
        };

        Ok(UploadedBatchArtifacts {
            parquet_uris,
            manifest_uri,
        })
    }
}

pub fn resolve_push_targets_from_env<F>(
    push_specs: &[PushSpec],
    write_manifest: bool,
    env_getter: F,
) -> Result<Vec<PushTarget>, Box<dyn std::error::Error + Send + Sync>>
where
    F: Fn(&str) -> Option<String>,
{
    push_specs
        .iter()
        .map(|spec| PushTarget::from_spec(spec, write_manifest, &env_getter))
        .collect()
}
