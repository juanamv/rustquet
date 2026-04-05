use axum::extract::rejection::JsonRejection;
use axum::{Json, Router, http::StatusCode, routing::post};
use tokio::sync::{mpsc, oneshot};

use crate::app::actors::IngestCmd;
use crate::domain::models::TelemetryEvent;

// ── Shared application state ───────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub ingest_tx: mpsc::Sender<IngestCmd>,
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
    payload: Result<Json<TelemetryEvent>, JsonRejection>,
) -> StatusCode {
    let Json(event) = match payload {
        Ok(payload) => payload,
        Err(error) => {
            eprintln!("[ingest] invalid json payload: {error}");
            return StatusCode::BAD_REQUEST;
        }
    };

    let (ack_tx, ack_rx) = oneshot::channel();

    if let Err(error) = state
        .ingest_tx
        .send(IngestCmd::WriteEvent { event, ack: ack_tx })
        .await
    {
        eprintln!("[ingest] channel send failed: {error}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    if ack_rx.await.is_err() {
        eprintln!("[ingest] write acknowledgement dropped");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::CREATED
}
