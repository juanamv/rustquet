use std::collections::BTreeMap;
use std::sync::Arc;

use rocksdb::DB;
use tokio::sync::{mpsc, oneshot};

use crate::domain::models::TelemetryEvent;
use crate::domain::schema::schema::SchemaSpec;
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
        eprintln!("[ingest] failed to send batch to parquet actor: {error}");
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
                                eprintln!(
                                    "[ingest] failed to compute batch timestamp range: {error}"
                                );
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
                            eprintln!("[ingest] failed to persist active batch: {error}");
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
                    eprintln!("[ingest] failed to append event: {error}");
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
            eprintln!("[parquet] failed to finalize batch: {error}");
            return;
        }
    };

    let _ = ingest_tx.blocking_send(IngestCmd::BatchDone {
        next_batch_start_id,
    });
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
        eprintln!(
            "[parquet] missing schema version {} for batch {}",
            batch.schema_version, batch.start_event_id
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
                eprintln!(
                    "[parquet] ignoring invalid manifest for batch {}: {error}",
                    batch.start_event_id
                );
                let _ = std::fs::remove_file(path);
                None
            }
        });

    if batch.status == ActiveBatchStatus::Written
        && existing_manifest.as_ref().is_some_and(|manifest| {
            crate::infra::manifest::manifest_files_exist(output_dir, manifest)
        })
    {
        finalize_batch(db, ingest_tx, batch);
        return;
    }

    let events = match storage::read_batch(db, batch.start_event_id, batch.len) {
        Ok(events) => events,
        Err(error) => {
            eprintln!("[parquet] failed to read batch from rocksdb: {error}");
            return;
        }
    };

    if events.len() != batch.len {
        eprintln!(
            "[parquet] expected {} events for batch {}, got {}",
            batch.len,
            batch.start_event_id,
            events.len()
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
            println!(
                "[parquet] wrote {} events -> {}",
                result.row_count, file_list
            );
            result
        }
        Err(error) => {
            eprintln!("[parquet] failed to write files: {error}");
            return;
        }
    };

    let manifest_path = if write_manifest {
        match crate::infra::manifest::write_manifest(output_dir, batch, &write_result.files) {
            Ok(path) => Some(path),
            Err(error) => {
                eprintln!("[parquet] failed to write manifest: {error}");
                return;
            }
        }
    } else {
        None
    };

    if batch.status != ActiveBatchStatus::Written {
        for push_target in push_targets {
            match push_target.upload_batch_artifacts(
                runtime_handle,
                output_dir,
                &write_result.files,
                manifest_path.as_deref().map(std::path::Path::new),
            ) {
                Ok(uploaded) => {
                    for parquet_uri in uploaded.parquet_uris {
                        println!(
                            "[parquet] push '{}' uploaded parquet -> {}",
                            push_target.name(),
                            parquet_uri
                        );
                    }
                    if let Some(manifest_uri) = uploaded.manifest_uri {
                        println!(
                            "[parquet] push '{}' uploaded manifest -> {}",
                            push_target.name(),
                            manifest_uri
                        );
                    }
                }
                Err(error) => {
                    eprintln!(
                        "[parquet] push '{}' failed to upload batch artifacts: {error}",
                        push_target.name()
                    );
                    return;
                }
            }
        }

        if let Err(error) = storage::mark_active_batch_written(db, batch) {
            eprintln!("[parquet] failed to mark batch as written: {error}");
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
