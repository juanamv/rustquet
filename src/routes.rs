use std::sync::Arc;

use axum::{Json, Router, http::StatusCode, routing::post};
use rocksdb::DB;
use tokio::sync::mpsc;

use crate::actors::IngestCmd;
use crate::models::TelemetryEvent;
use crate::storage;

// ── Shared application state ───────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<DB>,
    pub ingest_tx: mpsc::UnboundedSender<IngestCmd>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/ingest", post(ingest_handler))
        .with_state(state)
}

// ── POST /ingest ───────────────────────────────────────────────────
// 1. Persist event to RocksDB immediately.
// 2. Notify the IngestActor so it can track the batch size.

async fn ingest_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(event): Json<TelemetryEvent>,
) -> StatusCode {
    if let Err(e) = storage::write_event(&state.db, &event) {
        eprintln!("[ingest] rocksdb write failed: {e}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    if let Err(e) = state.ingest_tx.send(IngestCmd::EventNotified) {
        eprintln!("[ingest] channel send failed: {e}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::CREATED
}
