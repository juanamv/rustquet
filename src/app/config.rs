use crate::domain::schema;

pub const BATCH_SIZE: u64 = 10_000;
pub const INGEST_CHANNEL_CAPACITY: usize = 8_192;
pub const PARQUET_CHANNEL_CAPACITY: usize = 1;
pub const DB_PATH: &str = "rustquet_data";
pub const PARQUET_OUTPUT_DIR: &str = "parquet_output";
pub const SERVER_ADDR: &str = "127.0.0.1:3000";

pub fn schema_config_path_from_args<I>(args: I) -> String
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let _ = args.next();
    args.next()
        .unwrap_or_else(|| schema::DEFAULT_CONFIG_PATH.to_string())
}
