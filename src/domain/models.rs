use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    #[serde(default = "default_id")]
    pub id: String,
    pub path: String,
    pub event_name: String,
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
    #[serde(default = "chrono_now")]
    pub timestamp: i64,
}

fn default_metadata() -> serde_json::Value {
    serde_json::json!({})
}

fn default_id() -> String {
    Uuid::new_v4().to_string()
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64
}
