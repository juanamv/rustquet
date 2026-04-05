use std::sync::Arc;

use rocksdb::DB;
use tokio::sync::{mpsc, oneshot};

use crate::models::TelemetryEvent;
use crate::storage::{self, ActiveBatch, ActiveBatchStatus};

// ── Command types ──────────────────────────────────────────────────

pub enum IngestCmd {
    /// Write an event to RocksDB. The `oneshot::Sender` is used to
    /// notify the HTTP handler that the write completed.
    WriteEvent {
        event: TelemetryEvent,
        ack: oneshot::Sender<()>,
    },
    /// Sent by the ParquetActor after a batch is fully processed.
    BatchDone { next_batch_start_id: u64 },
    /// Graceful shutdown: flush remaining events to Parquet.
    #[allow(dead_code)]
    Shutdown,
}

pub enum ParquetCmd {
    WriteBatch {
        batch: ActiveBatch,
        events: Vec<TelemetryEvent>,
    },
    RecoverBatch {
        batch: ActiveBatch,
    },
}

// ── IngestActor ────────────────────────────────────────────────────
// Single writer to RocksDB. Receives events from the HTTP handler,
// persists them, and uses persisted metadata to recover batch state
// after restart.

fn dispatch_write_batch(
    db: &DB,
    parquet_tx: &mpsc::UnboundedSender<ParquetCmd>,
    batch: ActiveBatch,
) -> bool {
    let events = match storage::read_batch(db, batch.start_event_id, batch.len) {
        Ok(events) => events,
        Err(error) => {
            eprintln!("[ingest] failed to read batch: {error}");
            return false;
        }
    };

    if events.len() != batch.len {
        eprintln!(
            "[ingest] expected {} events for batch {}, got {}",
            batch.len,
            batch.start_event_id,
            events.len()
        );
        return false;
    }

    if let Err(error) = parquet_tx.send(ParquetCmd::WriteBatch { batch, events }) {
        eprintln!("[ingest] failed to send batch to parquet actor: {error}");
        return false;
    }

    true
}

fn dispatch_recovery_batch(
    parquet_tx: &mpsc::UnboundedSender<ParquetCmd>,
    batch: ActiveBatch,
) -> bool {
    if let Err(error) = parquet_tx.send(ParquetCmd::RecoverBatch { batch }) {
        eprintln!("[ingest] failed to send recovery batch to parquet actor: {error}");
        return false;
    }

    true
}

pub async fn run_ingest_actor(
    db: Arc<DB>,
    mut rx: mpsc::UnboundedReceiver<IngestCmd>,
    parquet_tx: mpsc::UnboundedSender<ParquetCmd>,
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
            if let Some(batch) = active_batch {
                parquet_in_flight = dispatch_recovery_batch(&parquet_tx, batch);
            } else {
                let pending_events = next_event_id.saturating_sub(next_batch_start_id);
                let batch_len = if shutting_down {
                    pending_events as usize
                } else if pending_events >= batch_size {
                    batch_size as usize
                } else {
                    0
                };

                if batch_len > 0 {
                    let batch = ActiveBatch {
                        start_event_id: next_batch_start_id,
                        len: batch_len,
                        status: ActiveBatchStatus::Writing,
                    };

                    match storage::store_active_batch(&db, batch) {
                        Ok(()) => {
                            active_batch = Some(batch);
                            parquet_in_flight = dispatch_write_batch(&db, &parquet_tx, batch);
                        }
                        Err(error) => {
                            eprintln!("[ingest] failed to persist active batch: {error}");
                        }
                    }
                } else if shutting_down {
                    break;
                }
            }
        }

        let Some(cmd) = rx.recv().await else {
            break;
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
// Receives a batch, writes or recovers the Parquet file, then atomically
// advances RocksDB metadata and deletes the processed keys.

fn process_batch(
    db: &DB,
    ingest_tx: &mpsc::UnboundedSender<IngestCmd>,
    output_dir: &str,
    batch: ActiveBatch,
    events: Option<Vec<TelemetryEvent>>,
) {
    let final_path = crate::parquet::parquet_file_path(output_dir, batch.start_event_id);

    if !final_path.exists() {
        let events = match events {
            Some(events) => events,
            None => match storage::read_batch(db, batch.start_event_id, batch.len) {
                Ok(events) => events,
                Err(error) => {
                    eprintln!("[parquet] failed to reload batch from rocksdb: {error}");
                    return;
                }
            },
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

    let _ = ingest_tx.send(IngestCmd::BatchDone {
        next_batch_start_id,
    });
}

pub async fn run_parquet_actor(
    db: Arc<DB>,
    mut rx: mpsc::UnboundedReceiver<ParquetCmd>,
    ingest_tx: mpsc::UnboundedSender<IngestCmd>,
    output_dir: String,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ParquetCmd::WriteBatch { batch, events } => {
                let db_clone = db.clone();
                let ingest_tx_clone = ingest_tx.clone();
                let dir = output_dir.clone();

                tokio::task::spawn_blocking(move || {
                    process_batch(&db_clone, &ingest_tx_clone, &dir, batch, Some(events));
                });
            }
            ParquetCmd::RecoverBatch { batch } => {
                let db_clone = db.clone();
                let ingest_tx_clone = ingest_tx.clone();
                let dir = output_dir.clone();

                tokio::task::spawn_blocking(move || {
                    process_batch(&db_clone, &ingest_tx_clone, &dir, batch, None);
                });
            }
        }
    }
}
