use pg_analyzer::{Anomaly, PatternRow};
use pg_storage::ring::RingEntry;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    Ping,
    Status,
    Subscribe { topics: Vec<String> },
    Unsubscribe,
    Export { since_secs: Option<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecorderStatus {
    pub flight_id: u64,
    pub recording: bool,
    pub events_recorded: u64,
    pub db_path: String,
    pub redact_params: bool,
    pub dropped_events: u64,
    #[serde(default)]
    pub dropped_storage_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub version: u32,
    pub listen: String,
    pub upstream: String,
    pub recorder: RecorderStatus,
    pub anomaly_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightLogBatch {
    pub version: u32,
    pub events: Vec<FlightLogEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightLogEvent {
    pub event_id: u64,
    pub flight_id: u64,
    pub connection_id: u64,
    pub txn_id: u64,
    pub fingerprint_id: i64,
    pub canonical_sql: String,
    pub exec_us: u64,
    pub queue_wait_us: u64,
    pub rows_returned: u64,
    pub created_at_ms: u64,
    pub app_name: String,
    pub error_code: Option<String>,
}

impl From<RingEntry> for FlightLogEvent {
    fn from(e: RingEntry) -> Self {
        Self {
            event_id: e.event_id,
            flight_id: e.flight_id,
            connection_id: e.connection_id,
            txn_id: e.txn_id,
            fingerprint_id: e.fingerprint_id,
            canonical_sql: e.canonical_sql,
            exec_us: e.exec_us,
            queue_wait_us: e.queue_wait_us,
            rows_returned: e.rows_returned,
            created_at_ms: e.created_at_ms,
            app_name: e.app_name,
            error_code: e.error_code,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternSnapshot {
    pub version: u32,
    pub patterns: Vec<PatternRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyNotification {
    pub version: u32,
    pub anomaly: Anomaly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlResponse {
    Pong { version: u32 },
    Status(StatusResponse),
    FlightLogBatch(FlightLogBatch),
    PatternSnapshot(PatternSnapshot),
    AnomalyNotification(AnomalyNotification),
    ExportLine { line: String },
    Error { message: String },
}
