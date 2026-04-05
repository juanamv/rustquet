use std::sync::Arc;

use rocksdb::DB;
use tokio::sync::{mpsc, oneshot};

use crate::models::TelemetryEvent;
use crate::storage::{self, ActiveBatch, ActiveBatchStatus};

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
                    let batch = ActiveBatch {
                        start_event_id: next_batch_start_id,
                        len: batch_len,
                        status: ActiveBatchStatus::Writing,
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

fn process_batch(
    db: &DB,
    ingest_tx: &mpsc::Sender<IngestCmd>,
    output_dir: &str,
    batch: ActiveBatch,
) {
    let final_path = crate::parquet::parquet_file_path(output_dir, batch.start_event_id);

    if !final_path.exists() {
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

        match crate::parquet::write_parquet(&events, batch.start_event_id, output_dir) {
            Ok((file, count)) => {
                println!("[parquet] wrote {count} events -> {file}");
            }
            Err(error) => {
                eprintln!("[parquet] failed to write file: {error}");
                return;
            }
        }
    }

    if let Err(error) = storage::mark_active_batch_written(db, batch) {
        eprintln!("[parquet] failed to mark batch as written: {error}");
        return;
    }

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

pub async fn run_parquet_actor(
    db: Arc<DB>,
    mut rx: mpsc::Receiver<ParquetCmd>,
    ingest_tx: mpsc::Sender<IngestCmd>,
    output_dir: String,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ParquetCmd::ProcessBatch { batch } => {
                let db_clone = db.clone();
                let ingest_tx_clone = ingest_tx.clone();
                let dir = output_dir.clone();

                tokio::task::spawn_blocking(move || {
                    process_batch(&db_clone, &ingest_tx_clone, &dir, batch);
                });
            }
        }
    }
}
