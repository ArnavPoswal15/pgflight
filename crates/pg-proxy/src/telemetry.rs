//! Non-blocking query telemetry (bounded channel, try_send only).

use std::sync::atomic::{AtomicU64, Ordering};

/// Cap per connection relay; daemon may wrap with shared counter.
pub const EVENT_CHANNEL_CAP: usize = 8192;

#[derive(Debug, Clone)]
pub struct RawQueryEvent {
    pub query_id: u64,
    pub txn_id: u64,
    pub connection_id: u64,
    pub app_name: String,
    pub queue_wait_us: u64,
    pub exec_us: u64,
    pub raw_sql: String,
    pub params_json: Option<String>,
    pub params_truncated: bool,
    /// Type OIDs from the preceding Parse (extended query); empty for simple query.
    pub param_oids: Vec<u32>,
    /// Bind format codes per parameter (`None` = simple query).
    pub bind_format_codes: Option<Vec<i16>>,
    /// Raw parameter values (`encode_param_values` layout) when extended bind was captured.
    pub bind_param_bytes: Vec<u8>,
    pub rows_returned: u64,
    pub error_code: Option<String>,
}

/// Simple-query or extended `SET` / `SET LOCAL` captured for `.pgrec` session-state replay.
#[derive(Debug, Clone)]
pub struct SessionSetEvent {
    pub connection_id: u64,
    pub txn_id: u64,
    pub sql: String,
}

/// Ready-for-query transaction status transition (`Z` frame), per FEATURES_AND_LOGIC §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnBoundaryKind {
    Begin,
    EndCommit,
    EndAbort,
}

#[derive(Debug, Clone)]
pub struct TxnBoundaryEvent {
    pub connection_id: u64,
    pub txn_id: u64,
    pub kind: TxnBoundaryKind,
}

#[derive(Debug, Clone)]
pub struct IdleInTxnEvent {
    pub txn_id: u64,
    pub idle_us: u64,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TelemetryEvent {
    QueryComplete(RawQueryEvent),
    SessionSet(SessionSetEvent),
    TxnBoundary(TxnBoundaryEvent),
    IdleInTxn(IdleInTxnEvent),
}

#[derive(Debug, Default)]
pub struct DroppedEventCounter(pub AtomicU64);

impl DroppedEventCounter {
    pub fn incr(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

pub fn try_send_event(
    tx: &tokio::sync::mpsc::Sender<TelemetryEvent>,
    counter: &DroppedEventCounter,
    event: TelemetryEvent,
) {
    if tx.try_send(event).is_err() {
        counter.incr();
    }
}
