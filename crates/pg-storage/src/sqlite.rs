//! SQLite schema + batch writer (250ms WAL).

use crate::recorder::PersistedQueryEvent;
use rusqlite::{params, Connection};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[error("sqlite: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
CREATE TABLE IF NOT EXISTS fingerprints (
  fingerprint_id INTEGER PRIMARY KEY,
  canonical_sql TEXT NOT NULL,
  first_seen_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS query_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  flight_id INTEGER NOT NULL,
  connection_id INTEGER NOT NULL,
  txn_id INTEGER NOT NULL,
  fingerprint_id INTEGER NOT NULL REFERENCES fingerprints(fingerprint_id),
  raw_sql TEXT,
  params_json TEXT,
  truncated INTEGER DEFAULT 0,
  queue_wait_us INTEGER NOT NULL,
  exec_us INTEGER NOT NULL,
  rows_returned INTEGER NOT NULL,
  error_code TEXT,
  app_name TEXT,
  created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_fp_time ON query_events(fingerprint_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_events_txn ON query_events(txn_id);
CREATE TABLE IF NOT EXISTS anomalies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  fingerprint_id INTEGER NOT NULL,
  parent_fingerprint INTEGER,
  repeat_count INTEGER NOT NULL,
  total_wasted_us INTEGER NOT NULL,
  confidence TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);
"#;

pub fn open_db(path: &Path) -> Result<Connection, SqliteError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        harden_storage_permissions(parent, None)?;
    }
    let conn = Connection::open(path)?;
    if let Some(parent) = path.parent() {
        harden_storage_permissions(parent, Some(path))?;
    }
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

#[cfg(unix)]
fn harden_storage_permissions(dir: &Path, db_file: Option<&Path>) -> Result<(), SqliteError> {
    use std::os::unix::fs::PermissionsExt;
    if dir.is_dir() {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    if let Some(db) = db_file {
        if db.is_file() {
            std::fs::set_permissions(db, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn harden_storage_permissions(_dir: &Path, _db_file: Option<&Path>) -> Result<(), SqliteError> {
    Ok(())
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn upsert_fingerprint(
    conn: &Connection,
    fp_id: i64,
    canonical: &str,
) -> Result<(), SqliteError> {
    let now = now_ms() as i64;
    conn.execute(
        "INSERT INTO fingerprints (fingerprint_id, canonical_sql, first_seen_ms) VALUES (?1, ?2, ?3)
         ON CONFLICT(fingerprint_id) DO NOTHING",
        params![fp_id, canonical, now],
    )?;
    Ok(())
}

pub fn insert_events_batch(
    conn: &Connection,
    batch: &[PersistedQueryEvent],
) -> Result<(), SqliteError> {
    if batch.is_empty() {
        return Ok(());
    }
    let tx = conn.unchecked_transaction()?;
    for ev in batch {
        upsert_fingerprint(&tx, ev.fingerprint_id, &ev.canonical_sql)?;
        tx.execute(
            "INSERT INTO query_events (
              flight_id, connection_id, txn_id, fingerprint_id, raw_sql, params_json, truncated,
              queue_wait_us, exec_us, rows_returned, error_code, app_name, created_at_ms
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                ev.flight_id as i64,
                ev.connection_id as i64,
                ev.txn_id as i64,
                ev.fingerprint_id,
                ev.raw_sql,
                ev.params_json,
                ev.params_truncated as i64,
                ev.queue_wait_us as i64,
                ev.exec_us as i64,
                ev.rows_returned as i64,
                ev.error_code,
                ev.app_name,
                ev.created_at_ms as i64,
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn insert_anomaly(
    conn: &Connection,
    kind: &str,
    fingerprint_id: i64,
    parent: Option<i64>,
    repeat_count: i64,
    total_wasted_us: i64,
    confidence: &str,
) -> Result<(), SqliteError> {
    let now = now_ms() as i64;
    conn.execute(
        "INSERT INTO anomalies (kind, fingerprint_id, parent_fingerprint, repeat_count, total_wasted_us, confidence, created_at_ms)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            kind,
            fingerprint_id,
            parent,
            repeat_count,
            total_wasted_us,
            confidence,
            now
        ],
    )?;
    Ok(())
}

pub fn count_events(conn: &Connection) -> Result<u64, SqliteError> {
    let n: i64 = conn.query_row("SELECT COUNT(*) FROM query_events", [], |r| r.get(0))?;
    Ok(n as u64)
}

pub fn prune_by_age(conn: &Connection, max_age_days: u64) -> Result<u64, SqliteError> {
    let cutoff = now_ms().saturating_sub(max_age_days * 86_400_000);
    let deleted = conn.execute(
        "DELETE FROM query_events WHERE created_at_ms < ?1",
        params![cutoff as i64],
    )?;
    Ok(deleted as u64)
}
