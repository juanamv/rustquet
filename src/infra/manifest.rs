use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::infra::layout;
use crate::infra::storage::ActiveBatch;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub batch_id: u64,
    pub schema_version: u32,
    pub file_path: String,
    pub row_count: u64,
    pub event_id_min: u64,
    pub event_id_max: u64,
    pub timestamp_min: i64,
    pub timestamp_max: i64,
    pub date_start: String,
    pub date_end: String,
    pub created_at: i64,
}

fn invalid_data(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

fn manifest_root(output_dir: &str) -> PathBuf {
    Path::new(output_dir)
        .parent()
        .map(|parent| parent.join("manifests"))
        .unwrap_or_else(|| PathBuf::from("manifests"))
}

pub fn manifest_file_path_for_batch(output_dir: &str, batch: ActiveBatch) -> PathBuf {
    layout::partition_directory(
        &manifest_root(output_dir),
        batch.timestamp_min,
        batch.timestamp_max,
    )
    .join(layout::batch_file_name(
        batch.start_event_id,
        batch.timestamp_min,
        batch.timestamp_max,
        "json",
    ))
}

pub fn manifest_file_path(output_dir: &str, batch_start_id: u64) -> PathBuf {
    let root = manifest_root(output_dir);
    layout::find_batch_file(&root, batch_start_id, "json")
        .unwrap_or_else(|| root.join(format!("batch_{batch_start_id:010}.json")))
}

pub fn write_manifest(
    output_dir: &str,
    batch: ActiveBatch,
    parquet_path: &Path,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if batch.len == 0 {
        return Err(invalid_data("cannot write manifest for an empty batch").into());
    }

    let final_path = manifest_file_path_for_batch(output_dir, batch);
    if final_path.exists() {
        return Ok(final_path.to_string_lossy().into_owned());
    }

    let temp_path = final_path.with_extension("json.tmp");
    if temp_path.exists() {
        std::fs::remove_file(&temp_path)?;
    }
    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let created_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    let entry = ManifestEntry {
        batch_id: batch.start_event_id,
        schema_version: batch.schema_version,
        file_path: parquet_path.to_string_lossy().into_owned(),
        row_count: batch.len as u64,
        event_id_min: batch.start_event_id,
        event_id_max: batch.start_event_id + batch.len as u64 - 1,
        timestamp_min: batch.timestamp_min,
        timestamp_max: batch.timestamp_max,
        date_start: layout::format_date(batch.timestamp_min),
        date_end: layout::format_date(batch.timestamp_max),
        created_at,
    };

    std::fs::write(&temp_path, serde_json::to_vec_pretty(&entry)?)?;
    std::fs::rename(&temp_path, &final_path)?;

    Ok(final_path.to_string_lossy().into_owned())
}

pub fn load_manifest(
    path: &Path,
) -> Result<ManifestEntry, Box<dyn std::error::Error + Send + Sync>> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}
