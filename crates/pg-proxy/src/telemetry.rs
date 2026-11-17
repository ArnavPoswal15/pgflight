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
    pub rows_returned: u64,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IdleInTxnEvent {
    pub txn_id: u64,
    pub idle_us: u64,
}

#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    QueryComplete(RawQueryEvent),
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
