use std::io::{Error, ErrorKind};

use crate::domain::schema;

pub const BATCH_SIZE: u64 = 10_000;
pub const INGEST_CHANNEL_CAPACITY: usize = 8_192;
pub const PARQUET_CHANNEL_CAPACITY: usize = 1;
pub const DB_PATH: &str = "rustquet_data";
pub const PARQUET_OUTPUT_DIR: &str = "parquet_output";
pub const SERVER_ADDR: &str = "127.0.0.1:3000";

const SERVER_ADDR_ENV: &str = "SERVER_ADDR";
const DB_PATH_ENV: &str = "DB_PATH";
const PARQUET_OUTPUT_DIR_ENV: &str = "PARQUET_OUTPUT_DIR";
const BATCH_SIZE_ENV: &str = "BATCH_SIZE";
const INGEST_CHANNEL_CAPACITY_ENV: &str = "INGEST_CHANNEL_CAPACITY";
const PARQUET_CHANNEL_CAPACITY_ENV: &str = "PARQUET_CHANNEL_CAPACITY";
const SCHEMA_CONFIG_PATH_ENV: &str = "SCHEMA_CONFIG_PATH";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub server_addr: String,
    pub db_path: String,
    pub parquet_output_dir: String,
    pub batch_size: u64,
    pub ingest_channel_capacity: usize,
    pub parquet_channel_capacity: usize,
    pub schema_config_path: String,
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

pub(crate) fn load_runtime_config_from_env<I, F>(
    args: I,
    env_getter: F,
) -> Result<RuntimeConfig, Box<dyn std::error::Error + Send + Sync>>
where
    I: IntoIterator<Item = String>,
    F: Fn(&str) -> Option<String>,
{
    Ok(RuntimeConfig {
        server_addr: string_from_env_or_default(&env_getter, SERVER_ADDR_ENV, SERVER_ADDR),
        db_path: string_from_env_or_default(&env_getter, DB_PATH_ENV, DB_PATH),
        parquet_output_dir: string_from_env_or_default(
            &env_getter,
            PARQUET_OUTPUT_DIR_ENV,
            PARQUET_OUTPUT_DIR,
        ),
        batch_size: parse_from_env_or_default(&env_getter, BATCH_SIZE_ENV, BATCH_SIZE)?,
        ingest_channel_capacity: parse_from_env_or_default(
            &env_getter,
            INGEST_CHANNEL_CAPACITY_ENV,
            INGEST_CHANNEL_CAPACITY,
        )?,
        parquet_channel_capacity: parse_from_env_or_default(
            &env_getter,
            PARQUET_CHANNEL_CAPACITY_ENV,
            PARQUET_CHANNEL_CAPACITY,
        )?,
        schema_config_path: first_user_argument(args)
            .or_else(|| env_getter(SCHEMA_CONFIG_PATH_ENV))
            .unwrap_or_else(|| schema::DEFAULT_CONFIG_PATH.to_string()),
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
