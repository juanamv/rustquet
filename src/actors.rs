use std::sync::Arc;

use rocksdb::DB;
use tokio::sync::{mpsc, oneshot};

use crate::models::TelemetryEvent;
use crate::storage;

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
    ProcessBatch {
        events: Vec<TelemetryEvent>,
        batch_start: u64,
    },
}

// ── IngestActor ────────────────────────────────────────────────────
// Single writer to RocksDB.  Receives events from the HTTP handler,
// persists them, tracks the batch size, and dispatches full batches
// to the ParquetActor.
//
// Both the write cursor and the earliest unprocessed event id are
// persisted in RocksDB metadata so the actor can recover after restart.

fn dispatch_batch(
    db: &DB,
    parquet_tx: &mpsc::UnboundedSender<ParquetCmd>,
    batch_start_id: u64,
    batch_len: usize,
) -> bool {
    let events = match storage::read_batch(db, batch_start_id, batch_len) {
        Ok(events) => events,
        Err(error) => {
            eprintln!("[ingest] failed to read batch: {error}");
            return false;
        }
    };

    if events.is_empty() {
        return false;
    }

    if let Err(error) = parquet_tx.send(ParquetCmd::ProcessBatch {
        events,
        batch_start: batch_start_id,
    }) {
        eprintln!("[ingest] failed to send batch to parquet actor: {error}");
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
    let mut parquet_in_flight = false;
    let mut shutting_down = false;

    loop {
        let Some(cmd) = rx.recv().await else {
            break;
        };

        match cmd {
            IngestCmd::WriteEvent { event, ack } => {
                if storage::append_event(&db, next_event_id, &event, next_event_id + 1).is_err() {
                    continue;
                }
                next_event_id += 1;

                // Notify the HTTP handler that the write succeeded.
                let _ = ack.send(());
            }
            IngestCmd::BatchDone {
                next_batch_start_id: committed_next_batch_start_id,
            } => {
                next_batch_start_id = committed_next_batch_start_id;
                parquet_in_flight = false;
            }
            IngestCmd::Shutdown => {
                shutting_down = true;
            }
        }

        let pending_events = next_event_id.saturating_sub(next_batch_start_id);

        if shutting_down {
            if parquet_in_flight {
                continue;
            }

            if pending_events == 0 {
                break;
            }

            parquet_in_flight = dispatch_batch(
                &db,
                &parquet_tx,
                next_batch_start_id,
                pending_events as usize,
            );
            continue;
        }

        if !parquet_in_flight && pending_events >= batch_size {
            parquet_in_flight =
                dispatch_batch(&db, &parquet_tx, next_batch_start_id, batch_size as usize);
        }
    }
}

// ── ParquetActor ───────────────────────────────────────────────────
// Receives a full batch, writes a Parquet file (in a blocking task),
// then atomically deletes the processed keys from RocksDB and notifies
// the IngestActor that the batch is complete.

pub async fn run_parquet_actor(
    db: Arc<DB>,
    mut rx: mpsc::UnboundedReceiver<ParquetCmd>,
    ingest_tx: mpsc::UnboundedSender<IngestCmd>,
    output_dir: String,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ParquetCmd::ProcessBatch {
                events,
                batch_start,
            } => {
                let db_clone = db.clone();
                let ingest_tx_clone = ingest_tx.clone();
                let dir = output_dir.clone();

                tokio::task::spawn_blocking(move || {
                    match crate::parquet::write_parquet(&events, batch_start, &dir) {
                        Ok((file, count)) => {
                            println!("[parquet] wrote {count} events -> {file}");

                            let next_batch_start_id = batch_start + events.len() as u64;
                            if let Err(error) = storage::delete_batch(
                                &db_clone,
                                batch_start,
                                events.len(),
                                next_batch_start_id,
                            ) {
                                eprintln!("[parquet] failed to delete batch: {error}");
                                return;
                            }

                            let _ = ingest_tx_clone.send(IngestCmd::BatchDone {
                                next_batch_start_id,
                            });
                        }
                        Err(error) => {
                            eprintln!("[parquet] failed to write file: {error}");
                        }
                    }
                });
            }
        }
    }
}
