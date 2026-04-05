use std::sync::Arc;

use rocksdb::DB;
use tokio::sync::mpsc;

use crate::config::BATCH_SIZE;
use crate::models::TelemetryEvent;
use crate::storage;

// ── Command types ──────────────────────────────────────────────────

pub enum IngestCmd {
    EventNotified,
    BatchDone,
}

pub enum ParquetCmd {
    ProcessBatch {
        events: Vec<TelemetryEvent>,
        batch_start: u64,
    },
}

// ── IngestActor ────────────────────────────────────────────────────
// Tracks how many events live in RocksDB. When the count reaches
// BATCH_SIZE it reads the batch and pushes it to the ParquetActor.

pub async fn run_ingest_actor(
    db: Arc<DB>,
    mut rx: mpsc::UnboundedReceiver<IngestCmd>,
    parquet_tx: mpsc::UnboundedSender<ParquetCmd>,
) {
    let mut counter: u64 = storage::get_counter(&db).expect("failed to read counter from rocksdb");
    let mut batch_start: u64 = counter.saturating_sub(counter % BATCH_SIZE as u64);

    while let Some(cmd) = rx.recv().await {
        match cmd {
            IngestCmd::EventNotified => {
                counter += 1;

                if counter.is_multiple_of(BATCH_SIZE as u64) {
                    let events = match storage::read_batch(&db, batch_start, BATCH_SIZE) {
                        Ok(e) => e,
                        Err(e) => {
                            eprintln!("[ingest] failed to read batch: {e}");
                            continue;
                        }
                    };

                    if let Err(e) = parquet_tx.send(ParquetCmd::ProcessBatch {
                        events,
                        batch_start,
                    }) {
                        eprintln!("[ingest] failed to send batch to parquet actor: {e}");
                    }

                    batch_start += BATCH_SIZE as u64;
                }
            }
            IngestCmd::BatchDone => {
                counter = 0;
                batch_start += BATCH_SIZE as u64;
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
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ParquetCmd::ProcessBatch {
                events,
                batch_start,
            } => {
                let db_clone = db.clone();
                let ingest_tx_clone = ingest_tx.clone();

                tokio::task::spawn_blocking(move || {
                    match crate::parquet::write_parquet(&events, batch_start) {
                        Ok((file, count)) => {
                            println!("[parquet] wrote {count} events -> {file}");

                            if let Err(e) =
                                storage::delete_batch(&db_clone, batch_start, events.len())
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
