use std::sync::Arc;

use rustquet::{actors, config, routes, schema, storage};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = config::schema_config_path_from_args(std::env::args());
    let db = storage::open_db(config::DB_PATH)?;
    let active_schema = schema::load_schema_from_path(&config_path)?;
    let schemas = Arc::new(storage::ensure_schema(&db, &active_schema)?);

    let (ingest_tx, ingest_rx) = mpsc::channel(config::INGEST_CHANNEL_CAPACITY);
    let (parquet_tx, parquet_rx) = mpsc::channel(config::PARQUET_CHANNEL_CAPACITY);

    let db_ingest = db.clone();
    let db_parquet = db.clone();

    tokio::spawn(actors::run_ingest_actor(
        db_ingest,
        ingest_rx,
        parquet_tx,
        config::BATCH_SIZE,
        active_schema.version,
    ));
    tokio::spawn(actors::run_parquet_actor(
        db_parquet,
        parquet_rx,
        ingest_tx.clone(),
        config::PARQUET_OUTPUT_DIR.to_string(),
        schemas,
    ));

    let state = routes::AppState { ingest_tx };
    let app = routes::router(state);

    let listener = tokio::net::TcpListener::bind(config::SERVER_ADDR).await?;
    println!("rustquet listening on {}", config::SERVER_ADDR);

    axum::serve(listener, app).await?;

    Ok(())
}
