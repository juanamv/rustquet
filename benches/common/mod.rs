use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode};
use criterion::{BenchmarkGroup, Criterion, Throughput};
use rocksdb::DB;
use rustquet::{actors, config, routes, schema, storage};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tower::util::ServiceExt;

pub const FIXED_TIMESTAMP: i64 = 1_735_603_200;
const BENCH_BEARER_TOKEN: &str = "bench-bearer-token";

const WAIT_ATTEMPTS: usize = 120;
const WAIT_INTERVAL: Duration = Duration::from_millis(100);

pub struct BenchHarness {
    pub runtime: Runtime,
    app: axum::Router,
    ingest_tx: mpsc::Sender<actors::IngestCmd>,
    db: Arc<DB>,
    parquet_dir: PathBuf,
    manifest_dir: PathBuf,
    _temp_dir: TempDir,
}

impl BenchHarness {
    pub fn new(batch_size: u64, write_manifest: bool) -> Self {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .expect("failed to build tokio runtime for benchmark");

        let temp_dir = TempDir::new().expect("failed to create benchmark temp dir");
        let db_path = temp_dir.path().to_string_lossy().into_owned();
        let db = storage::open_db(&db_path).expect("failed to open benchmark rocksdb");
        let parquet_dir = temp_dir.path().join("parquet_output");
        let manifest_dir = temp_dir.path().join("manifests");
        let output_dir = parquet_dir.to_string_lossy().into_owned();
        let runtime_config = config::RuntimeConfig {
            server_addr: config::SERVER_ADDR.to_string(),
            db_path: db_path.clone(),
            parquet_output_dir: output_dir.clone(),
            batch_size,
            batch_max_age_ms: config::BATCH_MAX_AGE_MS,
            ingest_channel_capacity: config::INGEST_CHANNEL_CAPACITY,
            parquet_channel_capacity: config::PARQUET_CHANNEL_CAPACITY,
            schema_config_path: schema::DEFAULT_CONFIG_PATH.to_string(),
            ingest_bearer_token: Some(BENCH_BEARER_TOKEN.to_string()),
            max_event_metadata_bytes: 0,
            max_request_body_bytes: 0,
        };
        let memory_budget = config::validate_memory_budget(&runtime_config)
            .expect("failed to compute benchmark memory budget");
        let active_schema = schema::load_default_schema().expect("failed to load benchmark schema");
        let schemas = Arc::new(
            storage::ensure_schema(&db, &active_schema)
                .expect("failed to persist benchmark schema history"),
        );
        let (ingest_tx, ingest_rx) = mpsc::channel(config::INGEST_CHANNEL_CAPACITY);
        let (parquet_tx, parquet_rx) = mpsc::channel(config::PARQUET_CHANNEL_CAPACITY);

        runtime.block_on(async {
            tokio::spawn(actors::run_ingest_actor(
                db.clone(),
                ingest_rx,
                parquet_tx,
                batch_size,
                config::BATCH_MAX_AGE_MS,
                active_schema.version,
            ));
            tokio::spawn(actors::run_parquet_actor(
                db.clone(),
                parquet_rx,
                ingest_tx.clone(),
                output_dir,
                schemas,
                write_manifest,
                Arc::new(Vec::new()),
            ));
        });

        let app = routes::router(routes::AppState {
            ingest_tx: ingest_tx.clone(),
            bearer_token: Some(Arc::<str>::from(BENCH_BEARER_TOKEN)),
            db: db.clone(),
            server_stats: Arc::new(routes::ServerStats::from_runtime(
                &config::RuntimeConfig {
                    max_event_metadata_bytes: memory_budget.max_event_metadata_bytes,
                    max_request_body_bytes: memory_budget.max_request_body_bytes,
                    ..runtime_config
                },
                active_schema.version,
                write_manifest,
                0,
            )),
        });

        Self {
            runtime,
            app,
            ingest_tx,
            db,
            parquet_dir,
            manifest_dir,
            _temp_dir: temp_dir,
        }
    }

    pub async fn send_payloads(&self, payloads: &[Bytes]) {
        for (index, payload) in payloads.iter().enumerate() {
            let response: axum::response::Response = self
                .app
                .clone()
                .oneshot(build_request(payload.clone()))
                .await
                .expect("benchmark request failed unexpectedly");

            assert_eq!(
                response.status(),
                StatusCode::CREATED,
                "benchmark request {index} failed"
            );
        }
    }

    pub async fn wait_for_state(
        &self,
        expected_parquet_files: usize,
        expected_manifest_files: usize,
        expected_event_count: usize,
    ) {
        let mut parquet_files = 0;
        let mut manifest_files = 0;
        let mut event_count = usize::MAX;

        for _ in 0..WAIT_ATTEMPTS {
            tokio::time::sleep(WAIT_INTERVAL).await;
            parquet_files = count_files_with_extension(&self.parquet_dir, "parquet");
            manifest_files = count_files_with_extension(&self.manifest_dir, "json");
            event_count = count_event_keys(&self.db);

            if parquet_files == expected_parquet_files
                && manifest_files == expected_manifest_files
                && event_count == expected_event_count
            {
                return;
            }
        }

        panic!(
            "benchmark state did not converge: parquet={parquet_files}, manifests={manifest_files}, events={event_count}"
        );
    }

    pub async fn shutdown_and_drain(
        &self,
        expected_parquet_files: usize,
        expected_manifest_files: usize,
    ) {
        self.ingest_tx
            .send(actors::IngestCmd::Shutdown)
            .await
            .expect("failed to request benchmark shutdown");
        self.wait_for_state(expected_parquet_files, expected_manifest_files, 0)
            .await;
    }
}

pub fn build_payloads(prefix: &str, count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|index| {
            Bytes::from(
                serde_json::json!({
                    "id": format!("{prefix}-{index}"),
                    "path": format!("/bench/{}", index % 32),
                    "event_name": if index.is_multiple_of(2) {
                        "click"
                    } else {
                        "scroll"
                    },
                    "timestamp": FIXED_TIMESTAMP,
                    "metadata": {
                        "h2o": index.is_multiple_of(2),
                        "slot": format!("slot-{}", index % 8),
                    }
                })
                .to_string(),
            )
        })
        .collect()
}

pub fn configure_group<'a>(
    c: &'a mut Criterion,
    name: &str,
    elements: u64,
    sample_size: usize,
    measurement_time: Duration,
) -> BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(elements));
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(measurement_time);
    group.sample_size(sample_size);
    group
}

fn collect_files_with_extension(root: &Path, extension: &str, files: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            collect_files_with_extension(&path, extension, files);
        } else if path.extension().is_some_and(|ext| ext == extension) {
            files.push(path);
        }
    }
}

fn count_files_with_extension(root: &Path, extension: &str) -> usize {
    let mut files = Vec::new();
    collect_files_with_extension(root, extension, &mut files);
    files.len()
}

fn count_event_keys(db: &Arc<DB>) -> usize {
    db.iterator(rocksdb::IteratorMode::Start)
        .filter_map(Result::ok)
        .filter(|(key, _)| key.starts_with(b"event_"))
        .count()
}

fn build_request(body: Bytes) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/ingest")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {BENCH_BEARER_TOKEN}"))
        .body(Body::from(body))
        .expect("failed to build benchmark request")
}
