//! `FlightRecorder` — ring buffer + SQLite WAL batch writer facade.

use crate::pgrec::{NoopPgrecWriter, PgrecWriter};
use crate::ring::{RingBuffer, RingEntry, DEFAULT_RING_CAP};
use crate::sqlite::{insert_events_batch, open_db, prune_by_age, SqliteError};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

#[derive(Debug, Error)]
pub enum RecorderError {
    #[error("storage: {0}")]
    Storage(#[from] SqliteError),
    #[error("recorder is not running")]
    NotRunning,
}

#[derive(Debug, Clone)]
pub struct FlightRecorderConfig {
    pub data_dir: PathBuf,
    pub redact_params: bool,
    pub max_age_days: u64,
    pub max_db_size_mb: u64,
    pub ring_cap: usize,
}

impl Default for FlightRecorderConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("~/.pgflight"),
            redact_params: true,
            max_age_days: 14,
            max_db_size_mb: 512,
            ring_cap: DEFAULT_RING_CAP,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecordInput {
    pub connection_id: u64,
    pub txn_id: u64,
    pub fingerprint_id: i64,
    pub canonical_sql: String,
    pub raw_sql: String,
    pub params_json: Option<String>,
    pub params_truncated: bool,
    pub queue_wait_us: u64,
    pub exec_us: u64,
    pub rows_returned: u64,
    pub error_code: Option<String>,
    pub app_name: String,
}

#[derive(Debug, Clone)]
pub struct PersistedQueryEvent {
    pub flight_id: u64,
    pub connection_id: u64,
    pub txn_id: u64,
    pub fingerprint_id: i64,
    pub canonical_sql: String,
    pub raw_sql: Option<String>,
    pub params_json: Option<String>,
    pub params_truncated: bool,
    pub queue_wait_us: u64,
    pub exec_us: u64,
    pub rows_returned: u64,
    pub error_code: Option<String>,
    pub app_name: String,
    pub created_at_ms: u64,
}

pub struct FlightRecorder {
    pub flight_id: u64,
    pub redact_params: bool,
    db_path: PathBuf,
    ring: parking_lot::Mutex<RingBuffer>,
    batch_tx: mpsc::Sender<PersistedQueryEvent>,
    events_recorded: AtomicU64,
    dropped_batch_events: AtomicU64,
    _pgrec: Box<dyn PgrecWriter>,
}

static NEXT_FLIGHT_ID: AtomicU64 = AtomicU64::new(1);

impl FlightRecorder {
    pub fn db_path_from_config(cfg: &FlightRecorderConfig) -> PathBuf {
        resolve_data_dir(&cfg.data_dir).join("pgflight.sqlite")
    }

    pub fn open(cfg: FlightRecorderConfig) -> Result<Arc<Self>, RecorderError> {
        let flight_id = NEXT_FLIGHT_ID.fetch_add(1, Ordering::Relaxed);
        let db_path = Self::db_path_from_config(&cfg);
        let conn = open_db(&db_path)?;
        let (batch_tx, batch_rx) = mpsc::channel(16_384);
        let ring = RingBuffer::new(cfg.ring_cap);
        let recorder = Arc::new(Self {
            flight_id,
            redact_params: cfg.redact_params,
            db_path: db_path.clone(),
            ring: parking_lot::Mutex::new(ring),
            batch_tx,
            events_recorded: AtomicU64::new(0),
            dropped_batch_events: AtomicU64::new(0),
            _pgrec: Box::new(NoopPgrecWriter),
        });

        let max_age_days = cfg.max_age_days;
        tokio::spawn(batch_writer_task(batch_rx, conn, max_age_days));

        Ok(recorder)
    }

    pub fn append(&self, input: RecordInput) {
        let created_at_ms = crate::sqlite::now_ms();
        let params_json = if self.redact_params {
            None
        } else {
            input.params_json
        };
        let raw_sql = if self.redact_params {
            None
        } else {
            Some(input.raw_sql.clone())
        };

        let ring_entry = RingEntry {
            event_id: 0,
            flight_id: self.flight_id,
            connection_id: input.connection_id,
            txn_id: input.txn_id,
            fingerprint_id: input.fingerprint_id,
            canonical_sql: input.canonical_sql.clone(),
            exec_us: input.exec_us,
            queue_wait_us: input.queue_wait_us,
            rows_returned: input.rows_returned,
            created_at_ms,
            app_name: input.app_name.clone(),
            error_code: input.error_code.clone(),
        };
        self.ring.lock().push(ring_entry);
        self.events_recorded.fetch_add(1, Ordering::Relaxed);

        let persisted = PersistedQueryEvent {
            flight_id: self.flight_id,
            connection_id: input.connection_id,
            txn_id: input.txn_id,
            fingerprint_id: input.fingerprint_id,
            canonical_sql: input.canonical_sql,
            raw_sql,
            params_json,
            params_truncated: input.params_truncated,
            queue_wait_us: input.queue_wait_us,
            exec_us: input.exec_us,
            rows_returned: input.rows_returned,
            error_code: input.error_code,
            app_name: input.app_name,
            created_at_ms,
        };
        if self.batch_tx.try_send(persisted).is_err() {
            self.dropped_batch_events.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped_batch_events(&self) -> u64 {
        self.dropped_batch_events.load(Ordering::Relaxed)
    }

    pub fn ring_snapshot(&self) -> Vec<RingEntry> {
        self.ring.lock().snapshot()
    }

    pub fn ring_delta(&self, after_event_id: u64) -> Vec<RingEntry> {
        self.ring.lock().snapshot_since_event_id(after_event_id)
    }

    pub fn events_recorded(&self) -> u64 {
        self.events_recorded.load(Ordering::Relaxed)
    }

    pub fn latest_ring_event_id(&self) -> u64 {
        self.ring.lock().latest_event_id()
    }

    pub fn db_path(&self) -> &PathBuf {
        &self.db_path
    }
}

async fn batch_writer_task(
    mut rx: mpsc::Receiver<PersistedQueryEvent>,
    conn: Connection,
    max_age_days: u64,
) {
    let shared = Arc::new(Mutex::new(conn));
    let mut tick = interval(Duration::from_millis(250));
    let mut pending: Vec<PersistedQueryEvent> = Vec::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !pending.is_empty() {
                    let batch = mem::take(&mut pending);
                    let guard = shared.lock();
                    if let Err(e) = insert_events_batch(&guard, &batch) {
                        tracing::warn!("sqlite batch insert failed: {e}");
                    }
                    let _ = prune_by_age(&guard, max_age_days);
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(ev) => pending.push(ev),
                    None => {
                        if !pending.is_empty() {
                            let batch = mem::take(&mut pending);
                            let guard = shared.lock();
                            if let Err(e) = insert_events_batch(&guard, &batch) {
                                tracing::warn!("sqlite flush on shutdown failed: {e}");
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}

fn resolve_data_dir(dir: &Path) -> PathBuf {
    let s = dir.to_string_lossy();
    if let Some(rest) = s.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    dir.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn append_roundtrip() {
        let dir = std::env::temp_dir().join("pgflight-test-recorder");
        let _ = fs::remove_dir_all(&dir);
        let cfg = FlightRecorderConfig {
            data_dir: dir.clone(),
            ..Default::default()
        };
        let rec = FlightRecorder::open(cfg).unwrap();
        rec.append(RecordInput {
            connection_id: 1,
            txn_id: 1,
            fingerprint_id: 42,
            canonical_sql: "select 1".into(),
            raw_sql: "SELECT 1".into(),
            params_json: None,
            params_truncated: false,
            queue_wait_us: 0,
            exec_us: 100,
            rows_returned: 1,
            error_code: None,
            app_name: "test".into(),
        });
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(rec.events_recorded() >= 1);
    }
}
