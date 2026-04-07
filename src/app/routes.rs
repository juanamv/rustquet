use std::sync::Arc;
use std::time::Instant;

use axum::extract::DefaultBodyLimit;
use axum::extract::rejection::JsonRejection;
use axum::{
    Json, Router,
    http::{HeaderMap, Request, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use rocksdb::DB;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};

use crate::app::actors::IngestCmd;
use crate::app::config::RuntimeConfig;
use crate::domain::models::TelemetryEvent;

// ── Shared application state ───────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ServerStats {
    pub started_at: Instant,
    pub server_addr: Arc<str>,
    pub db_path: Arc<str>,
    pub parquet_output_dir: Arc<str>,
    pub schema_config_path: Arc<str>,
    pub schema_version: u32,
    pub write_manifest: bool,
    pub push_target_count: usize,
    pub batch_size: u64,
    pub batch_max_age_ms: u64,
    pub ingest_channel_capacity: usize,
    pub parquet_channel_capacity: usize,
    pub max_event_metadata_bytes: usize,
    pub max_request_body_bytes: usize,
}

impl ServerStats {
    pub fn from_runtime(
        runtime: &RuntimeConfig,
        schema_version: u32,
        write_manifest: bool,
        push_target_count: usize,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            server_addr: Arc::<str>::from(runtime.server_addr.as_str()),
            db_path: Arc::<str>::from(runtime.db_path.as_str()),
            parquet_output_dir: Arc::<str>::from(runtime.parquet_output_dir.as_str()),
            schema_config_path: Arc::<str>::from(runtime.schema_config_path.as_str()),
            schema_version,
            write_manifest,
            push_target_count,
            batch_size: runtime.batch_size,
            batch_max_age_ms: runtime.batch_max_age_ms,
            ingest_channel_capacity: runtime.ingest_channel_capacity,
            parquet_channel_capacity: runtime.parquet_channel_capacity,
            max_event_metadata_bytes: runtime.max_event_metadata_bytes,
            max_request_body_bytes: runtime.max_request_body_bytes,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub ingest_tx: mpsc::Sender<IngestCmd>,
    pub bearer_token: Option<Arc<str>>,
    pub db: Arc<DB>,
    pub server_stats: Arc<ServerStats>,
}

#[derive(Serialize)]
struct IngestAcceptedResponse {
    status: &'static str,
    message: &'static str,
}

#[derive(Serialize)]
struct ErrorResponse {
    status: &'static str,
    error: &'static str,
    message: String,
}

pub fn router(state: AppState) -> Router {
    let body_limit = state.server_stats.max_request_body_bytes;
    Router::new()
        .route("/ingest", post(ingest_handler))
        .merge(super::health::routes())
        .layer(DefaultBodyLimit::max(body_limit))
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
) -> Response {
    let Json(event) = match payload {
        Ok(payload) => payload,
        Err(error) => {
            let message = error.to_string();
            let extracted_status = error.into_response().status();
            let status = if extracted_status == StatusCode::PAYLOAD_TOO_LARGE {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            warn!(error = %message, "invalid json payload");
            return json_error_response(
                status,
                if status == StatusCode::PAYLOAD_TOO_LARGE {
                    "request_too_large"
                } else {
                    "invalid_json"
                },
                message,
            );
        }
    };

    let metadata_size = event.metadata.to_string().len();
    if metadata_size > state.server_stats.max_event_metadata_bytes {
        warn!(
            metadata_bytes = metadata_size,
            limit = state.server_stats.max_event_metadata_bytes,
            "event metadata exceeds dynamic size limit"
        );
        return json_error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "metadata_too_large",
            format!(
                "event metadata is {} bytes; limit is {} bytes",
                metadata_size, state.server_stats.max_event_metadata_bytes
            ),
        );
    }

    let (ack_tx, ack_rx) = oneshot::channel();

    if let Err(error) = state
        .ingest_tx
        .send(IngestCmd::WriteEvent { event, ack: ack_tx })
        .await
    {
        error!(error = %error, "channel send failed");
        return json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ingest_unavailable",
            "failed to send event to ingest actor".to_string(),
        );
    }

    if ack_rx.await.is_err() {
        error!("write acknowledgement dropped");
        return json_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ingest_ack_dropped",
            "write acknowledgement dropped".to_string(),
        );
    }

    json_response(
        StatusCode::CREATED,
        IngestAcceptedResponse {
            status: "created",
            message: "event persisted to rocksdb",
        },
    )
}

async fn require_bearer_auth(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, Response> {
    let Some(expected_token) = state.bearer_token.as_deref() else {
        return Ok(next.run(request).await);
    };

    if has_valid_bearer_token(request.headers(), expected_token) {
        Ok(next.run(request).await)
    } else {
        warn!("missing or invalid bearer token");
        Err(json_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or invalid bearer token".to_string(),
        ))
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

fn json_response<T>(status: StatusCode, body: T) -> Response
where
    T: Serialize,
{
    (status, Json(body)).into_response()
}

fn json_error_response(status: StatusCode, error: &'static str, message: String) -> Response {
    json_response(
        status,
        ErrorResponse {
            status: "error",
            error,
            message,
        },
    )
}
