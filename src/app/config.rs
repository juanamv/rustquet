pub const BATCH_SIZE: u64 = 10_000;
pub const INGEST_CHANNEL_CAPACITY: usize = 8_192;
pub const PARQUET_CHANNEL_CAPACITY: usize = 1;
pub const DB_PATH: &str = "rustquet_data";
pub const PARQUET_OUTPUT_DIR: &str = "parquet_output";
pub const SERVER_ADDR: &str = "127.0.0.1:3000";
