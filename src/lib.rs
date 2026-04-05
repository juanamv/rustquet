pub mod actors;
pub mod config;
pub mod models;
pub mod parquet;
pub mod routes;
pub mod storage;

use tokio::sync::mpsc;

pub async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db = storage::open_db(config::DB_PATH)?;

    let (ingest_tx, ingest_rx) = mpsc::unbounded_channel();
    let (parquet_tx, parquet_rx) = mpsc::unbounded_channel();

    let db_ingest = db.clone();
    let db_parquet = db.clone();

    tokio::spawn(actors::run_ingest_actor(db_ingest, ingest_rx, parquet_tx));
    tokio::spawn(actors::run_parquet_actor(
        db_parquet,
        parquet_rx,
        ingest_tx.clone(),
    ));

    let state = routes::AppState { db, ingest_tx };
    let app = routes::router(state);

    let listener = tokio::net::TcpListener::bind(config::SERVER_ADDR).await?;
    println!("rustquet listening on {}", config::SERVER_ADDR);

    axum::serve(listener, app).await?;

    Ok(())
}
