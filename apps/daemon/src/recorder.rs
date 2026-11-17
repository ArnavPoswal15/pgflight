//! Query Flight Recorder worker — consumes proxy telemetry.

use crate::analysis::AnalysisState;
use crate::event_hub::EventHub;
use pg_proxy::telemetry::{DroppedEventCounter, TelemetryEvent};
use pg_storage::{FlightRecorder, RecordInput};
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
        while let Some(evt) = rx.recv().await {
            if !state.recording.load(Ordering::Relaxed) {
                continue;
            }
            let TelemetryEvent::QueryComplete(raw) = evt else {
                continue;
            };
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
    });
}
