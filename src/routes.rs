use axum::{Json, Router, http::StatusCode, routing::post};
use tokio::sync::{mpsc, oneshot};

use crate::actors::IngestCmd;
use crate::models::TelemetryEvent;

// ── Shared application state ───────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub ingest_tx: mpsc::UnboundedSender<IngestCmd>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/ingest", post(ingest_handler))
        .with_state(state)
}

// ── POST /ingest ───────────────────────────────────────────────────
// Sends the event to the IngestActor (single writer to RocksDB)
// and waits for the write acknowledgement.

async fn ingest_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(event): Json<TelemetryEvent>,
) -> StatusCode {
    let (ack_tx, ack_rx) = oneshot::channel();

    if let Err(e) = state
        .ingest_tx
        .send(IngestCmd::WriteEvent { event, ack: ack_tx })
    {
        eprintln!("[ingest] channel send failed: {e}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    if ack_rx.await.is_err() {
        eprintln!("[ingest] write acknowledgement dropped");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::CREATED
}
