use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use rocksdb::DB;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::domain::models::TelemetryEvent;
use crate::domain::schema::SchemaSpec;
use crate::infra::storage::{self, ActiveBatch, ActiveBatchStatus};
use crate::infra::uploader::PushTarget;

// ── Command types ──────────────────────────────────────────────────

pub enum IngestCmd {
    /// Persist a new event to RocksDB and acknowledge the HTTP handler.
    WriteEvent {
        event: TelemetryEvent,
        ack: oneshot::Sender<()>,
    },
    /// Sent by the ParquetActor after the batch was fully finalized.
    BatchDone { next_batch_start_id: u64 },
    /// Graceful shutdown: flush the remaining partial batch.
    #[allow(dead_code)]
    Shutdown,
}

pub enum ParquetCmd {
    ProcessBatch { batch: ActiveBatch },
}

// ── IngestActor ────────────────────────────────────────────────────
// Single writer to RocksDB. It persists incoming events and only sends
// a compact batch reference to the ParquetActor. Batch state is stored
// in RocksDB so the service can resume after restart.

async fn dispatch_batch(parquet_tx: &mpsc::Sender<ParquetCmd>, batch: ActiveBatch) -> bool {
    if let Err(error) = parquet_tx.send(ParquetCmd::ProcessBatch { batch }).await {
        error!(error = %error, "failed to send batch to parquet actor");
        return false;
    }

    true
}

fn pending_batch_len(
    next_event_id: u64,
    next_batch_start_id: u64,
    batch_size: u64,
    shutting_down: bool,
) -> usize {
    let pending_events = next_event_id.saturating_sub(next_batch_start_id);

    if shutting_down {
        pending_events as usize
    } else if pending_events >= batch_size {
        batch_size as usize
    } else {
        0
    }
}

pub async fn run_ingest_actor(
    db: Arc<DB>,
    mut rx: mpsc::Receiver<IngestCmd>,
    parquet_tx: mpsc::Sender<ParquetCmd>,
    batch_size: u64,
    schema_version: u32,
) {
    let metadata =
        storage::load_or_initialize_metadata(&db).expect("failed to load metadata from rocksdb");
    let mut next_event_id = metadata.next_event_id;
    let mut next_batch_start_id = metadata.next_batch_start_id;
    let mut active_batch = storage::load_active_batch(&db).expect("failed to load active batch");
    let mut parquet_in_flight = false;
    let mut shutting_down = false;

    loop {
        if !parquet_in_flight {
            let batch_to_dispatch = if let Some(batch) = active_batch {
                Some(batch)
            } else {
                let batch_len = pending_batch_len(
                    next_event_id,
                    next_batch_start_id,
                    batch_size,
                    shutting_down,
                );

                if batch_len > 0 {
                    let (timestamp_min, timestamp_max) =
                        match storage::batch_timestamp_range(&db, next_batch_start_id, batch_len) {
                            Ok(range) => range,
                            Err(error) => {
                                error!(error = %error, "failed to compute batch timestamp range");
                                continue;
                            }
                        };

                    let batch = ActiveBatch {
                        start_event_id: next_batch_start_id,
                        len: batch_len,
                        status: ActiveBatchStatus::Writing,
                        schema_version,
                        timestamp_min,
                        timestamp_max,
                    };

                    match storage::store_active_batch(&db, batch) {
                        Ok(()) => {
                            active_batch = Some(batch);
                            Some(batch)
                        }
                        Err(error) => {
                            error!(error = %error, "failed to persist active batch");
                            None
                        }
                    }
                } else {
                    None
                }
            };

            if let Some(batch) = batch_to_dispatch {
                parquet_in_flight = dispatch_batch(&parquet_tx, batch).await;
            } else if shutting_down {
                break;
            }
        }

        let Some(cmd) = rx.recv().await else {
            shutting_down = true;
            continue;
        };

        match cmd {
            IngestCmd::WriteEvent { event, ack } => {
                if let Err(error) =
                    storage::append_event(&db, next_event_id, &event, next_event_id + 1)
                {
                    error!(error = %error, event_id = next_event_id, "failed to append event");
                    continue;
                }

                next_event_id += 1;
                let _ = ack.send(());
            }
            IngestCmd::BatchDone {
                next_batch_start_id: committed_next_batch_start_id,
            } => {
                next_batch_start_id = committed_next_batch_start_id;
                active_batch = None;
                parquet_in_flight = false;
            }
            IngestCmd::Shutdown => {
                shutting_down = true;
            }
        }
    }
}

// ── ParquetActor ───────────────────────────────────────────────────
// Receives a compact batch reference, loads the rows from RocksDB,
// writes the Parquet file if needed, and then atomically finalizes the
// batch inside RocksDB.

struct ParquetBatchContext<'a> {
    ingest_tx: &'a mpsc::Sender<IngestCmd>,
    output_dir: &'a str,
    schemas: &'a BTreeMap<u32, SchemaSpec>,
    write_manifest: bool,
    runtime_handle: &'a tokio::runtime::Handle,
    push_targets: &'a [PushTarget],
}

fn finalize_batch(db: &DB, ingest_tx: &mpsc::Sender<IngestCmd>, batch: ActiveBatch) {
    let next_batch_start_id = match storage::finalize_active_batch(db, batch) {
        Ok(next_batch_start_id) => next_batch_start_id,
        Err(error) => {
            error!(error = %error, batch_id = batch.start_event_id, "failed to finalize batch");
            return;
        }
    };

    let _ = ingest_tx.blocking_send(IngestCmd::BatchDone {
        next_batch_start_id,
    });
}

fn all_push_targets_uploaded(
    push_targets: &[PushTarget],
    uploaded_pushes: &BTreeSet<String>,
) -> bool {
    push_targets
        .iter()
        .all(|push_target| uploaded_pushes.contains(push_target.name()))
}

fn remove_file_if_exists(path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn cleanup_local_batch_artifacts(
    output_dir: &str,
    batch: ActiveBatch,
    parquet_files: Option<&[crate::infra::parquet::ParquetDataFile]>,
    manifest_path: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let parquet_paths = match parquet_files {
        Some(files) => files
            .iter()
            .map(|file| Path::new(&file.path).to_path_buf())
            .collect::<Vec<_>>(),
        None => crate::infra::parquet::parquet_file_paths(output_dir, batch.start_event_id),
    };

    for parquet_path in parquet_paths {
        remove_file_if_exists(&parquet_path)?;
    }
    if let Some(manifest_path) = manifest_path {
        remove_file_if_exists(manifest_path)?;
    }

    Ok(())
}

fn process_batch(db: &DB, batch: ActiveBatch, context: ParquetBatchContext<'_>) {
    let ParquetBatchContext {
        ingest_tx,
        output_dir,
        schemas,
        write_manifest,
        runtime_handle,
        push_targets,
    } = context;

    let Some(schema_spec) = schemas.get(&batch.schema_version) else {
        error!(
            schema_version = batch.schema_version,
            batch_id = batch.start_event_id,
            "missing schema version for batch"
        );
        return;
    };
    let manifest_lookup_path = write_manifest
        .then(|| crate::infra::manifest::manifest_file_path(output_dir, batch.start_event_id));
    let existing_manifest = manifest_lookup_path
        .as_ref()
        .filter(|path| path.exists())
        .and_then(|path| match crate::infra::manifest::load_manifest(path) {
            Ok(manifest) => Some(manifest),
            Err(error) => {
                warn!(
                    error = %error,
                    batch_id = batch.start_event_id,
                    manifest_path = %path.display(),
                    "ignoring invalid manifest"
                );
                let _ = std::fs::remove_file(path);
                None
            }
        });

    let uploaded_pushes = match storage::load_active_batch_uploaded_pushes(db) {
        Ok(uploaded_pushes) => uploaded_pushes,
        Err(error) => {
            error!(error = %error, batch_id = batch.start_event_id, "failed to load uploaded push state");
            return;
        }
    };

    if batch.status == ActiveBatchStatus::Written {
        if push_targets.is_empty() {
            if existing_manifest.as_ref().is_some_and(|manifest| {
                crate::infra::manifest::manifest_files_exist(output_dir, manifest)
            }) {
                finalize_batch(db, ingest_tx, batch);
                return;
            }
        } else {
            if let Err(error) = cleanup_local_batch_artifacts(
                output_dir,
                batch,
                None,
                manifest_lookup_path.as_deref(),
            ) {
                error!(error = %error, batch_id = batch.start_event_id, "failed to clean local batch artifacts");
                return;
            }

            finalize_batch(db, ingest_tx, batch);
            return;
        }
    }

    if !push_targets.is_empty() && all_push_targets_uploaded(push_targets, &uploaded_pushes) {
        if let Err(error) =
            cleanup_local_batch_artifacts(output_dir, batch, None, manifest_lookup_path.as_deref())
        {
            error!(error = %error, batch_id = batch.start_event_id, "failed to clean local batch artifacts");
            return;
        }
        if let Err(error) = storage::mark_active_batch_written(db, batch) {
            error!(error = %error, batch_id = batch.start_event_id, "failed to mark batch as written");
            return;
        }

        finalize_batch(db, ingest_tx, batch);
        return;
    }

    let events = match storage::read_batch(db, batch.start_event_id, batch.len) {
        Ok(events) => events,
        Err(error) => {
            error!(error = %error, batch_id = batch.start_event_id, "failed to read batch from rocksdb");
            return;
        }
    };

    if events.len() != batch.len {
        error!(
            expected_events = batch.len,
            actual_events = events.len(),
            batch_id = batch.start_event_id,
            "batch length does not match stored events"
        );
        return;
    }

    let write_result = match crate::infra::parquet::write_parquet_batch_with_schema(
        &events,
        batch,
        output_dir,
        schema_spec,
    ) {
        Ok(result) => {
            let file_list = result
                .files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            info!(
                batch_id = batch.start_event_id,
                row_count = result.row_count,
                files = %file_list,
                "wrote parquet batch"
            );
            result
        }
        Err(error) => {
            error!(error = %error, batch_id = batch.start_event_id, "failed to write files");
            return;
        }
    };

    let manifest_path = if write_manifest {
        match crate::infra::manifest::write_manifest(output_dir, batch, &write_result.files) {
            Ok(path) => Some(path),
            Err(error) => {
                error!(error = %error, batch_id = batch.start_event_id, "failed to write manifest");
                return;
            }
        }
    } else {
        None
    };

    if batch.status != ActiveBatchStatus::Written {
        let mut uploaded_pushes = uploaded_pushes;

        for push_target in push_targets {
            if uploaded_pushes.contains(push_target.name()) {
                continue;
            }

            match push_target.upload_batch_artifacts(
                runtime_handle,
                output_dir,
                &write_result.files,
                manifest_path.as_deref().map(std::path::Path::new),
            ) {
                Ok(uploaded) => {
                    for parquet_uri in uploaded.parquet_uris {
                        info!(
                            push_name = push_target.name(),
                            uri = %parquet_uri,
                            batch_id = batch.start_event_id,
                            "uploaded parquet artifact"
                        );
                    }
                    if let Some(manifest_uri) = uploaded.manifest_uri {
                        info!(
                            push_name = push_target.name(),
                            uri = %manifest_uri,
                            batch_id = batch.start_event_id,
                            "uploaded manifest artifact"
                        );
                    }
                    if let Err(error) =
                        storage::mark_active_batch_push_uploaded(db, batch, push_target.name())
                    {
                        error!(
                            error = %error,
                            push_name = push_target.name(),
                            batch_id = batch.start_event_id,
                            "failed to persist uploaded push state"
                        );
                        return;
                    }
                    uploaded_pushes.insert(push_target.name().to_string());
                }
                Err(error) => {
                    error!(
                        error = %error,
                        push_name = push_target.name(),
                        batch_id = batch.start_event_id,
                        "failed to upload batch artifacts"
                    );
                    return;
                }
            }
        }

        if !push_targets.is_empty()
            && let Err(error) = cleanup_local_batch_artifacts(
                output_dir,
                batch,
                Some(&write_result.files),
                manifest_path.as_deref().map(Path::new),
            )
        {
            error!(error = %error, batch_id = batch.start_event_id, "failed to clean local batch artifacts");
            return;
        }

        if let Err(error) = storage::mark_active_batch_written(db, batch) {
            error!(error = %error, batch_id = batch.start_event_id, "failed to mark batch as written");
            return;
        }
    }

    finalize_batch(db, ingest_tx, batch);
}

pub async fn run_parquet_actor(
    db: Arc<DB>,
    mut rx: mpsc::Receiver<ParquetCmd>,
    ingest_tx: mpsc::Sender<IngestCmd>,
    output_dir: String,
    schemas: Arc<BTreeMap<u32, SchemaSpec>>,
    write_manifest: bool,
    push_targets: Arc<Vec<PushTarget>>,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ParquetCmd::ProcessBatch { batch } => {
                let db_clone = db.clone();
                let ingest_tx_clone = ingest_tx.clone();
                let dir = output_dir.clone();
                let schemas_clone = schemas.clone();
                let runtime_handle = tokio::runtime::Handle::current();
                let push_targets_clone = push_targets.clone();

                tokio::task::spawn_blocking(move || {
                    process_batch(
                        &db_clone,
                        batch,
                        ParquetBatchContext {
                            ingest_tx: &ingest_tx_clone,
                            output_dir: &dir,
                            schemas: &schemas_clone,
                            write_manifest,
                            runtime_handle: &runtime_handle,
                            push_targets: push_targets_clone.as_slice(),
                        },
                    );
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;
    use crate::domain::schema::{self, PushArtifact};
    use crate::infra::{manifest, parquet};

    fn test_event(timestamp: i64) -> TelemetryEvent {
        TelemetryEvent {
            id: "evt-1".to_string(),
            path: "/page".to_string(),
            event_name: "click".to_string(),
            metadata: serde_json::json!({"slot": "hero"}),
            timestamp,
        }
    }

    fn test_push_targets(write_manifest: bool) -> Vec<PushTarget> {
        let mut artifacts = vec![PushArtifact::Parquet];
        if write_manifest {
            artifacts.push(PushArtifact::Manifest);
        }

        vec![PushTarget::mock_s3_compatible(
            "lake_primary",
            artifacts,
            "bucket",
            "dev",
        )]
    }

    fn test_schemas() -> BTreeMap<u32, SchemaSpec> {
        let schema_spec = schema::load_default_schema().unwrap();
        BTreeMap::from([(schema_spec.version, schema_spec)])
    }

    #[test]
    fn test_process_batch_cleans_local_files_after_uploaded_pushes_complete() {
        let temp_dir = TempDir::new().unwrap();
        let db = storage::open_db(temp_dir.path().to_str().unwrap()).unwrap();
        let output_dir = temp_dir.path().join("parquet_output");
        let output_dir = output_dir.to_string_lossy().to_string();
        let schema_spec = schema::load_default_schema().unwrap();
        let event = test_event(1_700_000_000);
        let batch = ActiveBatch {
            start_event_id: 1,
            len: 1,
            status: ActiveBatchStatus::Writing,
            schema_version: schema_spec.version,
            timestamp_min: event.timestamp,
            timestamp_max: event.timestamp,
        };

        storage::write_event(&db, &event).unwrap();
        storage::store_active_batch(&db, batch).unwrap();
        let write_result = parquet::write_parquet_batch_with_schema(
            std::slice::from_ref(&event),
            batch,
            &output_dir,
            &schema_spec,
        )
        .unwrap();
        let manifest_path =
            manifest::write_manifest(&output_dir, batch, &write_result.files).unwrap();
        storage::mark_active_batch_push_uploaded(&db, batch, "lake_primary").unwrap();

        let push_targets = test_push_targets(true);
        let schemas = test_schemas();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let runtime_handle = runtime.handle().clone();
        let (ingest_tx, mut ingest_rx) = mpsc::channel(1);

        process_batch(
            &db,
            batch,
            ParquetBatchContext {
                ingest_tx: &ingest_tx,
                output_dir: &output_dir,
                schemas: &schemas,
                write_manifest: true,
                runtime_handle: &runtime_handle,
                push_targets: &push_targets,
            },
        );

        runtime.block_on(async {
            match ingest_rx.recv().await {
                Some(IngestCmd::BatchDone {
                    next_batch_start_id,
                }) => {
                    assert_eq!(next_batch_start_id, 2);
                }
                _ => panic!("expected BatchDone after cleanup"),
            }
        });

        assert!(!Path::new(&write_result.files[0].path).exists());
        assert!(!Path::new(&manifest_path).exists());
        assert_eq!(storage::load_active_batch(&db).unwrap(), None);
        assert!(storage::read_batch(&db, 1, 1).unwrap().is_empty());
        assert!(
            storage::load_active_batch_uploaded_pushes(&db)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_process_batch_finalizes_written_remote_batch_without_local_files() {
        let temp_dir = TempDir::new().unwrap();
        let db = storage::open_db(temp_dir.path().to_str().unwrap()).unwrap();
        let output_dir = temp_dir.path().join("parquet_output");
        let output_dir = output_dir.to_string_lossy().to_string();
        let schema_spec = schema::load_default_schema().unwrap();
        let event = test_event(1_700_000_001);
        let batch = ActiveBatch {
            start_event_id: 1,
            len: 1,
            status: ActiveBatchStatus::Written,
            schema_version: schema_spec.version,
            timestamp_min: event.timestamp,
            timestamp_max: event.timestamp,
        };

        storage::write_event(&db, &event).unwrap();
        storage::store_active_batch(&db, batch).unwrap();

        let push_targets = test_push_targets(false);
        let schemas = test_schemas();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let runtime_handle = runtime.handle().clone();
        let (ingest_tx, mut ingest_rx) = mpsc::channel(1);

        process_batch(
            &db,
            batch,
            ParquetBatchContext {
                ingest_tx: &ingest_tx,
                output_dir: &output_dir,
                schemas: &schemas,
                write_manifest: false,
                runtime_handle: &runtime_handle,
                push_targets: &push_targets,
            },
        );

        runtime.block_on(async {
            match ingest_rx.recv().await {
                Some(IngestCmd::BatchDone {
                    next_batch_start_id,
                }) => {
                    assert_eq!(next_batch_start_id, 2);
                }
                _ => panic!("expected BatchDone for written remote batch"),
            }
        });

        assert_eq!(storage::load_active_batch(&db).unwrap(), None);
        assert!(storage::read_batch(&db, 1, 1).unwrap().is_empty());
        assert!(
            crate::infra::parquet::parquet_file_paths(&output_dir, batch.start_event_id).is_empty()
        );
    }
}
