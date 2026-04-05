use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryEvent {
    pub session_id: String,
    pub path: String,
    pub event_name: String,
    #[serde(default = "chrono_now")]
    pub timestamp: i64,
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_minimal_payload() {
        let json = r#"{"session_id":"abc","path":"/page","event_name":"click"}"#;
        let event: TelemetryEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.session_id, "abc");
        assert_eq!(event.path, "/page");
        assert_eq!(event.event_name, "click");
        assert!(event.timestamp > 0);
    }

    #[test]
    fn test_deserialize_with_explicit_timestamp() {
        let json = r#"{"session_id":"s1","path":"/","event_name":"load","timestamp":1234567890}"#;
        let event: TelemetryEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.timestamp, 1234567890);
    }

    #[test]
    fn test_roundtrip_serialize_deserialize() {
        let event = TelemetryEvent {
            session_id: "sess".into(),
            path: "/dashboard".into(),
            event_name: "scroll".into(),
            timestamp: 999,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: TelemetryEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.session_id, "sess");
        assert_eq!(back.timestamp, 999);
    }
}
