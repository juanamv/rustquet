use std::sync::Arc;

use rocksdb::DB;
use rustquet::{actors, config, routes, schema, storage, uploader};
use tokio::sync::mpsc;
use tracing::info;

struct UserFacingError(String);

impl std::fmt::Display for UserFacingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Debug for UserFacingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UserFacingError {}

fn user_facing_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(UserFacingError(message.into()))
}

fn build_app_state(
    db: Arc<DB>,
    ingest_tx: mpsc::Sender<actors::IngestCmd>,
    bearer_token: Option<Arc<str>>,
    runtime: &config::RuntimeConfig,
    schema_version: u32,
    write_manifest: bool,
    push_target_count: usize,
) -> routes::AppState {
    routes::AppState {
        ingest_tx,
        bearer_token,
        db,
        server_stats: Arc::new(routes::ServerStats::from_runtime(
            runtime,
            schema_version,
            write_manifest,
            push_target_count,
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stdout)
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .init();

    let runtime = config::load_runtime_config(std::env::args())?;
    let loaded_config = schema::load_config_from_path(&runtime.schema_config_path)?;
    let active_schema = loaded_config.schema.clone();
    let db = storage::open_db(&runtime.db_path)?;
    let schemas = Arc::new(
        storage::ensure_schema(&db, &active_schema).map_err(|error| {
            user_facing_error(format!(
                "failed to validate schema from '{}' against RocksDB at '{}': {error}",
                runtime.schema_config_path, runtime.db_path
            ))
        })?,
    );
    let write_manifest = loaded_config.dataset.write_manifest;
    let push_targets = Arc::new(uploader::resolve_push_targets_from_env(
        &loaded_config.push,
        write_manifest,
        |key| std::env::var(key).ok(),
    )?);

    let budget = config::validate_memory_budget(&runtime)?;

    let (ingest_tx, ingest_rx) = mpsc::channel(runtime.ingest_channel_capacity);
    let (parquet_tx, parquet_rx) = mpsc::channel(runtime.parquet_channel_capacity);

    let db_ingest = db.clone();
    let db_parquet = db.clone();

    tokio::spawn(actors::run_ingest_actor(
        db_ingest,
        ingest_rx,
        parquet_tx,
        runtime.batch_size,
        runtime.batch_max_age_secs,
        active_schema.version,
    ));
    tokio::spawn(actors::run_parquet_actor(
        db_parquet,
        parquet_rx,
        ingest_tx.clone(),
        runtime.parquet_output_dir.clone(),
        schemas,
        write_manifest,
        push_targets.clone(),
    ));

    let bearer_token = runtime.ingest_bearer_token.clone().map(Arc::<str>::from);
    let bearer_auth_enabled = bearer_token.is_some();
    if !bearer_auth_enabled {
        println!("INGEST_BEARER_TOKEN not set; bearer auth disabled");
    }

    let state = build_app_state(
        db.clone(),
        ingest_tx,
        bearer_token,
        &runtime,
        active_schema.version,
        write_manifest,
        push_targets.len(),
    );
    let app = routes::router(state);

    info!("rustquet runtime configuration:");
    info!("  server_addr={}", runtime.server_addr);
    info!("  db_path={}", runtime.db_path);
    info!("  parquet_output_dir={}", runtime.parquet_output_dir);
    info!(
        "  batch_size={}",
        if runtime.batch_size == 0 {
            "[disabled]".to_string()
        } else {
            runtime.batch_size.to_string()
        }
    );
    info!("  batch_max_age_secs={}", runtime.batch_max_age_secs);
    info!(
        "  ingest_channel_capacity={}",
        runtime.ingest_channel_capacity
    );
    info!(
        "  parquet_channel_capacity={}",
        runtime.parquet_channel_capacity
    );
    info!("  schema_config_path={}", runtime.schema_config_path);
    info!(
        "  ingest_bearer_token={}",
        if bearer_auth_enabled {
            "[configured]"
        } else {
            "[disabled]"
        }
    );
    info!("  write_manifest={write_manifest}");
    info!("  push_targets={}", push_targets.len());
    for (index, push_target) in push_targets.iter().enumerate() {
        info!("  push[{index}].name={}", push_target.name());
        info!("  push[{index}].kind={:?}", push_target.kind());
        info!(
            "  push[{index}].artifacts={}",
            push_target
                .artifacts()
                .iter()
                .map(|artifact| format!("{artifact:?}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        info!("  push[{index}].bucket={}", push_target.bucket());
        info!("  push[{index}].prefix={}", push_target.prefix());
        info!("  push[{index}].endpoint={}", push_target.endpoint());
        info!("  push[{index}].region={}", push_target.region());
    }
    info!("  schema_version={}", active_schema.version);
    if budget.total_memory_bytes > 0 {
        info!("  memory_budget:");
        info!(
            "    total_system_ram={} MB",
            budget.total_memory_bytes / (1024 * 1024)
        );
        info!(
            "    rustquet_budget={} MB",
            budget.rustquet_budget_bytes / (1024 * 1024)
        );
        info!(
            "    rocksdb_estimate={} MB",
            budget.rocksdb_bytes / (1024 * 1024)
        );
        info!(
            "    channel_budget={} MB",
            budget.channel_bytes / (1024 * 1024)
        );
        info!(
            "    batch_peak_budget={} MB",
            budget.batch_peak_bytes / (1024 * 1024)
        );
    }
    info!(
        "  max_event_metadata_bytes={}",
        runtime.max_event_metadata_bytes
    );
    info!(
        "  max_request_body_bytes={}",
        runtime.max_request_body_bytes
    );

    let listener = tokio::net::TcpListener::bind(&runtime.server_addr).await?;
    info!("rustquet listening on {}", runtime.server_addr);

    axum::serve(listener, app).await?;

    Ok(())
}
