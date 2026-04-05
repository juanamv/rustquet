mod actors;
mod config;
mod models;
mod parquet;
mod routes;
mod storage;

use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = storage::open_db(config::DB_PATH)?;

    let (ingest_tx, ingest_rx) = mpsc::channel(config::INGEST_CHANNEL_CAPACITY);
    let (parquet_tx, parquet_rx) = mpsc::channel(config::PARQUET_CHANNEL_CAPACITY);

    let db_ingest = db.clone();
    let db_parquet = db.clone();

    tokio::spawn(actors::run_ingest_actor(
        db_ingest,
        ingest_rx,
        parquet_tx,
        config::BATCH_SIZE,
    ));
    tokio::spawn(actors::run_parquet_actor(
        db_parquet,
        parquet_rx,
        ingest_tx.clone(),
        config::PARQUET_OUTPUT_DIR.to_string(),
    ));

    let state = routes::AppState { ingest_tx };
    let app = routes::router(state);

    let listener = tokio::net::TcpListener::bind(config::SERVER_ADDR).await?;
    println!("rustquet listening on {}", config::SERVER_ADDR);

    axum::serve(listener, app).await?;

    Ok(())
}
