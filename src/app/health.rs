use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use serde::Serialize;
use tracing::error;

use crate::storage;

use super::routes::AppState;

#[derive(Serialize)]
struct PingResponse {
    status: &'static str,
    message: &'static str,
}

#[derive(Serialize)]
struct ErrorResponse {
    status: &'static str,
    error: &'static str,
    message: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    server: ServerHealthStats,
    storage: StorageHealthStats,
}

#[derive(Serialize)]
struct ServerHealthStats {
    app_version: &'static str,
    uptime_seconds: u64,
    server_addr: String,
    db_path: String,
    parquet_output_dir: String,
    schema_config_path: String,
    schema_version: u32,
    write_manifest: bool,
    push_target_count: usize,
    batch_size: u64,
    batch_max_age_secs: u64,
    ingest_channel_capacity: usize,
    parquet_channel_capacity: usize,
    max_event_metadata_bytes: usize,
    max_request_body_bytes: usize,
    bearer_auth_enabled: bool,
}

#[derive(Serialize)]
struct StorageHealthStats {
    next_event_id: u64,
    next_batch_start_id: u64,
    pending_event_count: u64,
    pending_batch_opened_at_ms: Option<u64>,
    persisted_schema_version: Option<u32>,
    active_batch: Option<ActiveBatchHealthStats>,
}

#[derive(Serialize)]
struct ActiveBatchHealthStats {
    start_event_id: u64,
    len: usize,
    status: &'static str,
    schema_version: u32,
    timestamp_min: i64,
    timestamp_max: i64,
    uploaded_pushes: Vec<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/ping", get(ping_handler))
        .route("/health", get(health_handler))
}

pub async fn ping_handler() -> Response {
    json_response(
        StatusCode::OK,
        PingResponse {
            status: "ok",
            message: "pong",
        },
    )
}

pub async fn health_handler(State(state): State<AppState>) -> Response {
    let metadata = match storage::load_or_initialize_metadata(&state.db) {
        Ok(metadata) => metadata,
        Err(error) => {
            error!(error = %error, "failed to load health metadata");
            return json_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "health_unavailable",
                format!("failed to load metadata: {error}"),
            );
        }
    };

    let active_batch = match storage::load_active_batch(&state.db) {
        Ok(active_batch) => active_batch,
        Err(error) => {
            error!(error = %error, "failed to load active batch for health");
            return json_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "health_unavailable",
                format!("failed to load active batch: {error}"),
            );
        }
    };

    let uploaded_pushes = match storage::load_active_batch_uploaded_pushes(&state.db) {
        Ok(uploaded_pushes) => uploaded_pushes,
        Err(error) => {
            error!(error = %error, "failed to load uploaded push state for health");
            return json_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "health_unavailable",
                format!("failed to load uploaded push state: {error}"),
            );
        }
    };

    let persisted_schema_version = match storage::load_current_schema_version(&state.db) {
        Ok(schema_version) => schema_version,
        Err(error) => {
            error!(error = %error, "failed to load persisted schema version for health");
            return json_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "health_unavailable",
                format!("failed to load persisted schema version: {error}"),
            );
        }
    };

    let server_stats = &state.server_stats;
    let active_batch = active_batch.map(|batch| ActiveBatchHealthStats {
        start_event_id: batch.start_event_id,
        len: batch.len,
        status: active_batch_status_name(batch.status),
        schema_version: batch.schema_version,
        timestamp_min: batch.timestamp_min,
        timestamp_max: batch.timestamp_max,
        uploaded_pushes: uploaded_pushes.into_iter().collect(),
    });

    json_response(
        StatusCode::OK,
        HealthResponse {
            status: "ok",
            server: ServerHealthStats {
                app_version: env!("CARGO_PKG_VERSION"),
                uptime_seconds: server_stats.started_at.elapsed().as_secs(),
                server_addr: server_stats.server_addr.to_string(),
                db_path: server_stats.db_path.to_string(),
                parquet_output_dir: server_stats.parquet_output_dir.to_string(),
                schema_config_path: server_stats.schema_config_path.to_string(),
                schema_version: server_stats.schema_version,
                write_manifest: server_stats.write_manifest,
                push_target_count: server_stats.push_target_count,
                batch_size: server_stats.batch_size,
                batch_max_age_secs: server_stats.batch_max_age_secs,
                ingest_channel_capacity: server_stats.ingest_channel_capacity,
                parquet_channel_capacity: server_stats.parquet_channel_capacity,
                max_event_metadata_bytes: server_stats.max_event_metadata_bytes,
                max_request_body_bytes: server_stats.max_request_body_bytes,
                bearer_auth_enabled: state.bearer_token.is_some(),
            },
            storage: StorageHealthStats {
                next_event_id: metadata.next_event_id,
                next_batch_start_id: metadata.next_batch_start_id,
                pending_event_count: metadata
                    .next_event_id
                    .saturating_sub(metadata.next_batch_start_id),
                pending_batch_opened_at_ms: metadata.pending_batch_opened_at_ms,
                persisted_schema_version,
                active_batch,
            },
        },
    )
}

fn active_batch_status_name(status: storage::ActiveBatchStatus) -> &'static str {
    match status {
        storage::ActiveBatchStatus::Writing => "writing",
        storage::ActiveBatchStatus::Written => "written",
    }
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
