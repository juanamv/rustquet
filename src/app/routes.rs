use std::sync::Arc;

use axum::extract::rejection::JsonRejection;
use axum::{
    Json, Router,
    http::{HeaderMap, Request, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::Response,
    routing::post,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

use crate::app::actors::IngestCmd;
use crate::domain::models::TelemetryEvent;

// ── Shared application state ───────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub ingest_tx: mpsc::Sender<IngestCmd>,
    pub bearer_token: Option<Arc<str>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/ingest", post(ingest_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_auth,
        ))
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
            warn!(error = %error, "invalid json payload");
            return StatusCode::BAD_REQUEST;
        }
    };

    let (ack_tx, ack_rx) = oneshot::channel();

    if let Err(error) = state
        .ingest_tx
        .send(IngestCmd::WriteEvent { event, ack: ack_tx })
        .await
    {
        error!(error = %error, "channel send failed");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    if ack_rx.await.is_err() {
        error!("write acknowledgement dropped");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::CREATED
}

async fn require_bearer_auth(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let Some(expected_token) = state.bearer_token.as_deref() else {
        return Ok(next.run(request).await);
    };

    if has_valid_bearer_token(request.headers(), expected_token) {
        Ok(next.run(request).await)
    } else {
        warn!("missing or invalid bearer token");
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn has_valid_bearer_token(headers: &HeaderMap, expected_token: &str) -> bool {
    let Some(header) = headers.get(AUTHORIZATION) else {
        return false;
    };
    let Ok(header) = header.to_str() else {
        return false;
    };
    let Some((scheme, token)) = header.split_once(' ') else {
        return false;
    };

    scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() && token == expected_token
}
