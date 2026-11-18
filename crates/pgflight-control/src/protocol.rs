use pg_analyzer::{Anomaly, PatternRow};
use pg_storage::ring::RingEntry;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    Ping,
    Status,
    Subscribe {
        topics: Vec<String>,
    },
    Unsubscribe,
    Export {
        since_secs: Option<u64>,
    },
    /// Resolve SQL/params from SQLite by fingerprint or (flight_id, event_id).
    Explain {
        #[serde(default)]
        fingerprint_id: Option<i64>,
        #[serde(default)]
        flight_id: Option<u64>,
        #[serde(default)]
        event_id: Option<u64>,
    },
    ReplayRun {
        pgrec_path: String,
        label: String,
        #[serde(default)]
        warmup_iters: u32,
        #[serde(default)]
        explain: bool,
    },
    /// Compare two completed replays, or optional inline plan sets when IDs are absent.
    CompareReplay {
        #[serde(default)]
        baseline: Option<ReplayResultId>,
        #[serde(default)]
        variant: Option<ReplayResultId>,
        #[serde(default)]
        baseline_plans: Option<Vec<FingerprintPlan>>,
        #[serde(default)]
        variant_plans: Option<Vec<FingerprintPlan>>,
    },
}

/// Daemon-assigned handle for a replay job (filled when replay completes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResultId {
    pub replay_id: String,
    pub label: String,
}

/// Heuristic or structural note on an EXPLAIN JSON plan (shadow worker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanHint {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub node_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResultBody {
    pub plan_json: Value,
    #[serde(default)]
    pub hints: Vec<PlanHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProgressBody {
    pub replay_id: String,
    pub label: String,
    pub phase: String,
    pub fingerprints_done: u32,
    pub fingerprints_total: u32,
    #[serde(default)]
    pub current_fingerprint_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintReplayStats {
    pub fingerprint_id: i64,
    pub exec_us_p50: u64,
    pub exec_us_p95: u64,
    pub exec_us_p99: u64,
    pub samples: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintPlan {
    pub fingerprint_id: i64,
    pub plan_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResultBody {
    pub replay: ReplayResultId,
    #[serde(default)]
    pub per_fingerprint: Vec<FingerprintReplayStats>,
    #[serde(default)]
    pub plans: Vec<FingerprintPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCompareEntry {
    pub fingerprint_id: i64,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDiffNode {
    pub path: String,
    #[serde(default)]
    pub baseline: Option<String>,
    #[serde(default)]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCompareBody {
    #[serde(default)]
    pub improved: Vec<PlanCompareEntry>,
    #[serde(default)]
    pub regressed: Vec<PlanCompareEntry>,
    #[serde(default)]
    pub diff_nodes: Vec<PlanDiffNode>,
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
pub struct ShadowStatus {
    pub enabled: bool,
    pub url: String,
    #[serde(default)]
    pub connected: bool,
    #[serde(default)]
    pub queued_jobs: u32,
    #[serde(default)]
    pub dropped_jobs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub version: u32,
    pub listen: String,
    pub upstream: String,
    pub recorder: RecorderStatus,
    pub anomaly_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow: Option<ShadowStatus>,
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
    ExplainResult(ExplainResultBody),
    ReplayProgress(ReplayProgressBody),
    ReplayResult(ReplayResultBody),
    PlanCompare(PlanCompareBody),
    Error { message: String },
}
