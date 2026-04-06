use std::sync::Arc;

use rustquet::{actors, config, routes, schema, storage};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime = config::load_runtime_config(std::env::args())?;
    let loaded_config = schema::load_config_from_path(&runtime.schema_config_path)?;
    let active_schema = loaded_config.schema;
    let db = storage::open_db(&runtime.db_path)?;
    let schemas = Arc::new(storage::ensure_schema(&db, &active_schema)?);

    let (ingest_tx, ingest_rx) = mpsc::channel(runtime.ingest_channel_capacity);
    let (parquet_tx, parquet_rx) = mpsc::channel(runtime.parquet_channel_capacity);

    let db_ingest = db.clone();
    let db_parquet = db.clone();

    tokio::spawn(actors::run_ingest_actor(
        db_ingest,
        ingest_rx,
        parquet_tx,
        runtime.batch_size,
        active_schema.version,
    ));
    tokio::spawn(actors::run_parquet_actor(
        db_parquet,
        parquet_rx,
        ingest_tx.clone(),
        runtime.parquet_output_dir.clone(),
        schemas,
    ));

    let state = routes::AppState { ingest_tx };
    let app = routes::router(state);

    println!("rustquet runtime configuration:");
    println!("  server_addr={}", runtime.server_addr);
    println!("  db_path={}", runtime.db_path);
    println!("  parquet_output_dir={}", runtime.parquet_output_dir);
    println!("  batch_size={}", runtime.batch_size);
    println!(
        "  ingest_channel_capacity={}",
        runtime.ingest_channel_capacity
    );
    println!(
        "  parquet_channel_capacity={}",
        runtime.parquet_channel_capacity
    );
    println!("  schema_config_path={}", runtime.schema_config_path);
    println!("  schema_version={}", active_schema.version);

    let listener = tokio::net::TcpListener::bind(&runtime.server_addr).await?;
    println!("rustquet listening on {}", runtime.server_addr);

    axum::serve(listener, app).await?;

    Ok(())
}
