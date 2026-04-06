use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

enum IngestActorEvent {
    Command(Option<IngestCmd>),
    TimeoutExpired,
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
    timeout_expired: bool,
) -> usize {
    let pending_events = next_event_id.saturating_sub(next_batch_start_id);

    if shutting_down {
        pending_events as usize
    } else if batch_size > 0 && pending_events >= batch_size {
        batch_size as usize
    } else if timeout_expired {
        pending_events as usize
    } else {
        0
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn undispatched_tail_start(next_batch_start_id: u64, active_batch: Option<ActiveBatch>) -> u64 {
    active_batch
        .map(|batch| batch.start_event_id + batch.len as u64)
        .unwrap_or(next_batch_start_id)
}

fn sync_pending_batch_opened_at_ms(
    db: &DB,
    pending_batch_opened_at_ms: &mut Option<u64>,
    desired: Option<u64>,
) {
    if *pending_batch_opened_at_ms == desired {
        return;
    }

    match storage::set_pending_batch_opened_at_ms(db, desired) {
        Ok(()) => *pending_batch_opened_at_ms = desired,
        Err(error) => {
            error!(error = %error, "failed to persist pending batch timeout metadata");
        }
    }
}

fn timeout_expired(
    batch_max_age_ms: u64,
    pending_batch_opened_at_ms: Option<u64>,
    next_event_id: u64,
    next_batch_start_id: u64,
) -> bool {
    if batch_max_age_ms == 0 || next_event_id == next_batch_start_id {
        return false;
    }

    let Some(opened_at_ms) = pending_batch_opened_at_ms else {
        return false;
    };

    now_unix_ms().saturating_sub(opened_at_ms) >= batch_max_age_ms
}

fn next_timeout_wait(
    batch_max_age_ms: u64,
    pending_batch_opened_at_ms: Option<u64>,
    next_event_id: u64,
    next_batch_start_id: u64,
) -> Option<Duration> {
    if batch_max_age_ms == 0 || next_event_id == next_batch_start_id {
        return None;
    }

    let opened_at_ms = pending_batch_opened_at_ms?;
    let elapsed_ms = now_unix_ms().saturating_sub(opened_at_ms);
    if elapsed_ms >= batch_max_age_ms {
        None
    } else {
        Some(Duration::from_millis(batch_max_age_ms - elapsed_ms))
    }
}

pub async fn run_ingest_actor(
    db: Arc<DB>,
    mut rx: mpsc::Receiver<IngestCmd>,
    parquet_tx: mpsc::Sender<ParquetCmd>,
    batch_size: u64,
    batch_max_age_ms: u64,
    schema_version: u32,
) {
    let metadata =
        storage::load_or_initialize_metadata(&db).expect("failed to load metadata from rocksdb");
    let mut next_event_id = metadata.next_event_id;
    let mut next_batch_start_id = metadata.next_batch_start_id;
    let mut pending_batch_opened_at_ms = metadata.pending_batch_opened_at_ms;
    let mut active_batch = storage::load_active_batch(&db).expect("failed to load active batch");
    let mut parquet_in_flight = false;
    let mut shutting_down = false;

    let initial_tail_start = undispatched_tail_start(next_batch_start_id, active_batch);
    if next_event_id == initial_tail_start {
        sync_pending_batch_opened_at_ms(&db, &mut pending_batch_opened_at_ms, None);
    } else if pending_batch_opened_at_ms.is_none() {
        sync_pending_batch_opened_at_ms(&db, &mut pending_batch_opened_at_ms, Some(now_unix_ms()));
    }

    loop {
        if !parquet_in_flight {
            let batch_to_dispatch = if let Some(batch) = active_batch {
                Some(batch)
            } else {
                let batch_timeout_expired = timeout_expired(
                    batch_max_age_ms,
                    pending_batch_opened_at_ms,
                    next_event_id,
                    next_batch_start_id,
                );
                let batch_len = pending_batch_len(
                    next_event_id,
                    next_batch_start_id,
                    batch_size,
                    shutting_down,
                    batch_timeout_expired,
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
                            let remaining_tail_start = batch.start_event_id + batch.len as u64;
                            let desired_opened_at_ms = if next_event_id > remaining_tail_start {
                                Some(now_unix_ms())
                            } else {
                                None
                            };
                            sync_pending_batch_opened_at_ms(
                                &db,
                                &mut pending_batch_opened_at_ms,
                                desired_opened_at_ms,
                            );
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

        let next_event = if !parquet_in_flight && active_batch.is_none() {
            match next_timeout_wait(
                batch_max_age_ms,
                pending_batch_opened_at_ms,
                next_event_id,
                next_batch_start_id,
            ) {
                Some(wait) => {
                    tokio::select! {
                        cmd = rx.recv() => IngestActorEvent::Command(cmd),
                        _ = tokio::time::sleep(wait) => IngestActorEvent::TimeoutExpired,
                    }
                }
                None => IngestActorEvent::Command(rx.recv().await),
            }
        } else {
            IngestActorEvent::Command(rx.recv().await)
        };

        let cmd = match next_event {
            IngestActorEvent::TimeoutExpired => continue,
            IngestActorEvent::Command(Some(cmd)) => cmd,
            IngestActorEvent::Command(None) => {
                shutting_down = true;
                continue;
            }
        };

        match cmd {
            IngestCmd::WriteEvent { event, ack } => {
                let tail_start = undispatched_tail_start(next_batch_start_id, active_batch);
                if let Err(error) =
                    storage::append_event(&db, next_event_id, &event, next_event_id + 1)
                {
                    error!(error = %error, event_id = next_event_id, "failed to append event");
                    continue;
                }

                if pending_batch_opened_at_ms.is_none() && next_event_id == tail_start {
                    sync_pending_batch_opened_at_ms(
                        &db,
                        &mut pending_batch_opened_at_ms,
                        Some(now_unix_ms()),
                    );
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

                if next_event_id == next_batch_start_id {
                    sync_pending_batch_opened_at_ms(&db, &mut pending_batch_opened_at_ms, None);
                } else if pending_batch_opened_at_ms.is_none() {
                    sync_pending_batch_opened_at_ms(
                        &db,
                        &mut pending_batch_opened_at_ms,
                        Some(now_unix_ms()),
                    );
                }
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

#[cfg(test)]
pub(super) fn process_batch_for_test(
    db: &DB,
    batch: ActiveBatch,
    ingest_tx: &mpsc::Sender<IngestCmd>,
    output_dir: &str,
    schemas: &BTreeMap<u32, SchemaSpec>,
    write_manifest: bool,
    runtime_handle: &tokio::runtime::Handle,
    push_targets: &[PushTarget],
) {
    process_batch(
        db,
        batch,
        ParquetBatchContext {
            ingest_tx,
            output_dir,
            schemas,
            write_manifest,
            runtime_handle,
            push_targets,
        },
    );
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
