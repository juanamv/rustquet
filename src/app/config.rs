use std::io::{Error, ErrorKind};

use sysinfo::System;

use crate::domain::schema;

pub const BATCH_SIZE: u64 = 10_000;
pub const BATCH_MAX_AGE_MS: u64 = 0;
pub const INGEST_CHANNEL_CAPACITY: usize = 8_192;
pub const MAX_INGEST_CHANNEL_CAPACITY: usize = 65_536;
pub const PARQUET_CHANNEL_CAPACITY: usize = 1;
pub const DB_PATH: &str = "rustquet_data";
pub const PARQUET_OUTPUT_DIR: &str = "parquet_output";
pub const SERVER_ADDR: &str = "127.0.0.1:3000";

const ESTIMATED_EVENT_SIZE_BYTES: u64 = 2 * 1024;
const MIN_INGEST_CHANNEL_MEMORY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;
const MAX_INGEST_CHANNEL_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const INGEST_CHANNEL_MEMORY_FRACTION_DIVISOR: u64 = 100;
const CHANNEL_CAPACITY_ROUNDING: usize = 1_024;

const SERVER_ADDR_ENV: &str = "SERVER_ADDR";
const DB_PATH_ENV: &str = "DB_PATH";
const PARQUET_OUTPUT_DIR_ENV: &str = "PARQUET_OUTPUT_DIR";
const BATCH_SIZE_ENV: &str = "BATCH_SIZE";
const BATCH_MAX_AGE_MS_ENV: &str = "BATCH_MAX_AGE_MS";
const INGEST_CHANNEL_CAPACITY_ENV: &str = "INGEST_CHANNEL_CAPACITY";
const PARQUET_CHANNEL_CAPACITY_ENV: &str = "PARQUET_CHANNEL_CAPACITY";
const SCHEMA_CONFIG_PATH_ENV: &str = "SCHEMA_CONFIG_PATH";
const INGEST_BEARER_TOKEN_ENV: &str = "INGEST_BEARER_TOKEN";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub server_addr: String,
    pub db_path: String,
    pub parquet_output_dir: String,
    pub batch_size: u64,
    pub batch_max_age_ms: u64,
    pub ingest_channel_capacity: usize,
    pub parquet_channel_capacity: usize,
    pub schema_config_path: String,
    pub ingest_bearer_token: Option<String>,
}

fn invalid_input(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, message.into())
}

fn first_user_argument<I>(args: I) -> Option<String>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let _ = args.next();
    args.next()
}

pub fn schema_config_path_from_args<I>(args: I) -> String
where
    I: IntoIterator<Item = String>,
{
    first_user_argument(args).unwrap_or_else(|| schema::DEFAULT_CONFIG_PATH.to_string())
}

fn string_from_env_or_default(
    env_getter: &dyn Fn(&str) -> Option<String>,
    key: &str,
    default: &str,
) -> String {
    env_getter(key).unwrap_or_else(|| default.to_string())
}

fn optional_string_from_env(
    env_getter: &dyn Fn(&str) -> Option<String>,
    key: &str,
) -> Option<String> {
    env_getter(key).and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn parse_batch_size(
    env_getter: &dyn Fn(&str) -> Option<String>,
    batch_max_age_ms: u64,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let batch_size = match env_getter(BATCH_SIZE_ENV) {
        Some(value) => value.parse::<u64>().map_err(|error| {
            invalid_input(format!("invalid value for {BATCH_SIZE_ENV}: {error}"))
        })?,
        None if batch_max_age_ms > 0 => 0,
        None => BATCH_SIZE,
    };

    if batch_size == 0 && batch_max_age_ms == 0 {
        return Err(invalid_input(
            "count batching is disabled because BATCH_SIZE is 0, but BATCH_MAX_AGE_MS is also 0; enable at least one batch trigger",
        )
        .into());
    }

    Ok(batch_size)
}

fn parse_from_env_or_default<T>(
    env_getter: &dyn Fn(&str) -> Option<String>,
    key: &str,
    default: T,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env_getter(key) {
        Some(value) => value
            .parse::<T>()
            .map_err(|error| invalid_input(format!("invalid value for {key}: {error}")).into()),
        None => Ok(default),
    }
}

fn parse_from_env_or_else<T, F>(
    env_getter: &dyn Fn(&str) -> Option<String>,
    key: &str,
    default: F,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
    F: FnOnce() -> T,
{
    match env_getter(key) {
        Some(value) => value
            .parse::<T>()
            .map_err(|error| invalid_input(format!("invalid value for {key}: {error}")).into()),
        None => Ok(default()),
    }
}

fn detect_total_memory_bytes() -> Option<u64> {
    let mut system = System::new();
    system.refresh_memory();

    let total_memory_bytes = system.total_memory();
    (total_memory_bytes > 0).then_some(total_memory_bytes)
}

pub(crate) fn auto_ingest_channel_capacity_from_total_memory(total_memory_bytes: u64) -> usize {
    let memory_budget_bytes = (total_memory_bytes / INGEST_CHANNEL_MEMORY_FRACTION_DIVISOR).clamp(
        MIN_INGEST_CHANNEL_MEMORY_BUDGET_BYTES,
        MAX_INGEST_CHANNEL_MEMORY_BUDGET_BYTES,
    );
    let raw_capacity = memory_budget_bytes / ESTIMATED_EVENT_SIZE_BYTES;
    let bounded_capacity = usize::try_from(raw_capacity)
        .unwrap_or(MAX_INGEST_CHANNEL_CAPACITY)
        .clamp(INGEST_CHANNEL_CAPACITY, MAX_INGEST_CHANNEL_CAPACITY);
    let rounded_capacity =
        (bounded_capacity / CHANNEL_CAPACITY_ROUNDING) * CHANNEL_CAPACITY_ROUNDING;

    rounded_capacity.clamp(INGEST_CHANNEL_CAPACITY, MAX_INGEST_CHANNEL_CAPACITY)
}

pub(crate) fn auto_ingest_channel_capacity() -> usize {
    detect_total_memory_bytes()
        .map(auto_ingest_channel_capacity_from_total_memory)
        .unwrap_or(INGEST_CHANNEL_CAPACITY)
}

pub(crate) fn load_runtime_config_from_env<I, F>(
    args: I,
    env_getter: F,
) -> Result<RuntimeConfig, Box<dyn std::error::Error + Send + Sync>>
where
    I: IntoIterator<Item = String>,
    F: Fn(&str) -> Option<String>,
{
    let batch_max_age_ms =
        parse_from_env_or_default(&env_getter, BATCH_MAX_AGE_MS_ENV, BATCH_MAX_AGE_MS)?;
    let batch_size = parse_batch_size(&env_getter, batch_max_age_ms)?;

    Ok(RuntimeConfig {
        server_addr: string_from_env_or_default(&env_getter, SERVER_ADDR_ENV, SERVER_ADDR),
        db_path: string_from_env_or_default(&env_getter, DB_PATH_ENV, DB_PATH),
        parquet_output_dir: string_from_env_or_default(
            &env_getter,
            PARQUET_OUTPUT_DIR_ENV,
            PARQUET_OUTPUT_DIR,
        ),
        batch_size,
        batch_max_age_ms,
        ingest_channel_capacity: parse_from_env_or_else(
            &env_getter,
            INGEST_CHANNEL_CAPACITY_ENV,
            auto_ingest_channel_capacity,
        )?,
        parquet_channel_capacity: parse_from_env_or_default(
            &env_getter,
            PARQUET_CHANNEL_CAPACITY_ENV,
            PARQUET_CHANNEL_CAPACITY,
        )?,
        schema_config_path: first_user_argument(args)
            .or_else(|| env_getter(SCHEMA_CONFIG_PATH_ENV))
            .unwrap_or_else(|| schema::DEFAULT_CONFIG_PATH.to_string()),
        ingest_bearer_token: optional_string_from_env(&env_getter, INGEST_BEARER_TOKEN_ENV),
    })
}

pub fn load_runtime_config<I>(
    args: I,
) -> Result<RuntimeConfig, Box<dyn std::error::Error + Send + Sync>>
where
    I: IntoIterator<Item = String>,
{
    let _ = dotenvy::dotenv();
    load_runtime_config_from_env(args, |key| std::env::var(key).ok())
}
