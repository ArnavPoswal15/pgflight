//! Query Flight Recorder worker — consumes proxy telemetry.

use crate::analysis::AnalysisState;
use crate::event_hub::EventHub;
use pg_proxy::telemetry::{
    DroppedEventCounter, RawQueryEvent, SessionSetEvent, TelemetryEvent, TxnBoundaryEvent,
    TxnBoundaryKind,
};
use pg_storage::{unix_now_us, FlightRecorder, PgrecRecord, PgrecRecordKind, RecordInput};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub struct RecorderState {
    pub recording: Arc<AtomicBool>,
    pub flight_recorder: Arc<FlightRecorder>,
    pub analysis: Arc<AnalysisState>,
    pub dropped_proxy: Arc<DroppedEventCounter>,
    pub hub: Arc<EventHub>,
}

pub fn spawn_recorder_worker(
    mut rx: tokio::sync::mpsc::Receiver<TelemetryEvent>,
    state: RecorderState,
) {
    tokio::spawn(async move {
        let mut last_pgrec_us = unix_now_us();
        while let Some(evt) = rx.recv().await {
            if !state.recording.load(Ordering::Relaxed) {
                continue;
            }
            match evt {
                TelemetryEvent::QueryComplete(raw) => {
                    if raw.raw_sql.trim().is_empty() {
                        continue;
                    }
                    let fp = match state.analysis.fingerprint(
                        &raw.raw_sql,
                        raw.txn_id,
                        raw.exec_us,
                        raw.rows_returned,
                    ) {
                        Some(v) => v,
                        None => continue,
                    };

                    state.flight_recorder.append(RecordInput {
                        connection_id: raw.connection_id,
                        txn_id: raw.txn_id,
                        fingerprint_id: fp.fingerprint_id,
                        canonical_sql: fp.canonical_sql.clone(),
                        raw_sql: raw.raw_sql.clone(),
                        params_json: raw.params_json.clone(),
                        params_truncated: raw.params_truncated,
                        queue_wait_us: raw.queue_wait_us,
                        exec_us: raw.exec_us,
                        rows_returned: raw.rows_returned,
                        error_code: raw.error_code.clone(),
                        app_name: raw.app_name.clone(),
                    });

                    // `TxnBoundary` pgrec rows cover BEGIN/COMMIT/ROLLBACK (incl. ReadyForQuery implicit begin).
                    if !is_txn_marker_sql(&raw.raw_sql) {
                        append_pgrec_query(&state.flight_recorder, &raw, &mut last_pgrec_us);
                    }

                    if let Some(anomaly) = state.analysis.take_pending_anomaly() {
                        match state
                            .analysis
                            .persist_anomaly(&state.flight_recorder, &anomaly)
                        {
                            Ok(()) => state.hub.notify_anomaly(anomaly),
                            Err(e) => warn!("persist anomaly: {e}"),
                        }
                    }
                }
                TelemetryEvent::SessionSet(sess) => {
                    append_pgrec_session(&state.flight_recorder, &sess, &mut last_pgrec_us);
                }
                TelemetryEvent::TxnBoundary(boundary) => {
                    append_pgrec_txn_boundary(
                        &state.flight_recorder,
                        &boundary,
                        &mut last_pgrec_us,
                    );
                }
                TelemetryEvent::IdleInTxn { .. } => {}
                _ => {}
            }
        }
    });
}

fn next_delta_us(last: &mut u64) -> u32 {
    let now = unix_now_us();
    let delta = now.saturating_sub(*last);
    *last = now;
    delta.min(u32::MAX as u64) as u32
}

fn client_id(connection_id: u64) -> u32 {
    connection_id as u32
}

fn is_txn_marker_sql(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return false;
    }
    let upper = trimmed.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "BEGIN" | "BEGIN TRANSACTION" | "BEGIN WORK" | "COMMIT" | "ROLLBACK" | "START TRANSACTION"
    )
}

fn append_pgrec_query(recorder: &FlightRecorder, raw: &RawQueryEvent, last_pgrec_us: &mut u64) {
    let param_format_codes = raw
        .bind_format_codes
        .as_ref()
        .map(|codes| {
            codes
                .iter()
                .filter_map(|&c| u16::try_from(c).ok())
                .collect()
        })
        .unwrap_or_default();
    recorder.append_pgrec_record(PgrecRecord {
        delta_us: next_delta_us(last_pgrec_us),
        client_id: client_id(raw.connection_id),
        txn_id: raw.txn_id,
        kind: PgrecRecordKind::Query,
        sql: raw.raw_sql.clone(),
        param_oids: raw.param_oids.clone(),
        param_format_codes,
        params: raw.bind_param_bytes.clone(),
    });
}

fn append_pgrec_session(
    recorder: &FlightRecorder,
    sess: &SessionSetEvent,
    last_pgrec_us: &mut u64,
) {
    recorder.append_pgrec_record(PgrecRecord {
        delta_us: next_delta_us(last_pgrec_us),
        client_id: client_id(sess.connection_id),
        txn_id: sess.txn_id,
        kind: PgrecRecordKind::SessionState,
        sql: sess.sql.clone(),
        param_oids: Vec::new(),
        param_format_codes: Vec::new(),
        params: Vec::new(),
    });
}

fn append_pgrec_txn_boundary(
    recorder: &FlightRecorder,
    boundary: &TxnBoundaryEvent,
    last_pgrec_us: &mut u64,
) {
    let sql = match boundary.kind {
        TxnBoundaryKind::Begin => "BEGIN",
        TxnBoundaryKind::EndCommit => "COMMIT",
        TxnBoundaryKind::EndAbort => "ROLLBACK",
    };
    recorder.append_pgrec_record(PgrecRecord {
        delta_us: next_delta_us(last_pgrec_us),
        client_id: client_id(boundary.connection_id),
        txn_id: boundary.txn_id,
        kind: PgrecRecordKind::TxnBoundary,
        sql: sql.into(),
        param_oids: Vec::new(),
        param_format_codes: Vec::new(),
        params: Vec::new(),
    });
}
