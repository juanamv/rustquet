use std::sync::Arc;

use rocksdb::{DB, WriteBatch};
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
    BatchDone,
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
// The counter is a running total (never reset).  Batch boundaries are
// derived from counter multiples of batch_size.

pub async fn run_ingest_actor(
    db: Arc<DB>,
    mut rx: mpsc::UnboundedReceiver<IngestCmd>,
    parquet_tx: mpsc::UnboundedSender<ParquetCmd>,
    batch_size: u64,
) {
    let mut counter: u64 = 0u64;
    let mut next_batch_start: u64 = 0u64;

    while let Some(cmd) = rx.recv().await {
        match cmd {
            IngestCmd::WriteEvent { event, ack } => {
                counter += 1;
                let key = format!("event_{:010}", counter);
                let value = serde_json::to_vec(&event).expect("failed to serialize event");

                let mut wb = WriteBatch::default();
                wb.put(key.as_bytes(), &value);
                db.write(wb).expect("failed to write event to rocksdb");

                // Notify the HTTP handler that the write succeeded.
                let _ = ack.send(());

                if counter.is_multiple_of(batch_size) {
                    let events =
                        match storage::read_batch(&db, next_batch_start, batch_size as usize) {
                            Ok(e) => e,
                            Err(e) => {
                                eprintln!("[ingest] failed to read batch: {e}");
                                continue;
                            }
                        };

                    if let Err(e) = parquet_tx.send(ParquetCmd::ProcessBatch {
                        events,
                        batch_start: next_batch_start,
                    }) {
                        eprintln!("[ingest] failed to send batch to parquet actor: {e}");
                    }

                    next_batch_start += batch_size;
                }
            }
            IngestCmd::BatchDone => {
                // Acknowledgement only — counter keeps running.
            }
            IngestCmd::Shutdown => {
                // Flush remaining events that didn't fill a complete batch.
                let remaining = counter - next_batch_start;
                if remaining > 0
                    && let Ok(events) =
                        storage::read_batch(&db, next_batch_start, remaining as usize)
                {
                    let _ = parquet_tx.send(ParquetCmd::ProcessBatch {
                        events,
                        batch_start: next_batch_start,
                    });
                }
                break;
            }
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

                            if let Err(e) =
                                storage::delete_batch(&db_clone, batch_start, events.len() + 1)
                            {
                                eprintln!("[parquet] failed to delete batch: {e}");
                            }

                            let _ = ingest_tx_clone.send(IngestCmd::BatchDone);
                        }
                        Err(e) => {
                            eprintln!("[parquet] failed to write file: {e}");
                        }
                    }
                });
            }
        }
    }
}
