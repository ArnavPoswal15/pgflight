//! 100ms batched fan-out to control `subscribe` clients.

use crate::analysis::AnalysisState;
use parking_lot::Mutex;
use pg_storage::FlightRecorder;
use pgflight_control::protocol::{
    AnomalyNotification, ControlResponse, FlightLogBatch, FlightLogEvent, PatternSnapshot,
    PROTOCOL_VERSION,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

type SubscriberId = u64;

#[derive(Default)]
pub struct HubState {
    subscribers: HashMap<SubscriberId, mpsc::Sender<String>>,
    next_id: SubscriberId,
    last_event_id: u64,
}

pub struct EventHub {
    inner: Arc<Mutex<HubState>>,
}

impl EventHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Mutex::new(HubState::default())),
        })
    }

    pub fn subscribe(&self) -> (SubscriberId, mpsc::Receiver<String>) {
        let (tx, rx) = mpsc::channel(256);
        let mut guard = self.inner.lock();
        let id = guard.next_id + 1;
        guard.next_id = id;
        guard.subscribers.insert(id, tx);
        (id, rx)
    }

    pub fn unsubscribe(&self, id: SubscriberId) {
        self.inner.lock().subscribers.remove(&id);
    }

    pub fn spawn_flush_task(
        self: &Arc<Self>,
        recorder: Arc<FlightRecorder>,
        analysis: Arc<AnalysisState>,
        recording: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let hub = self.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(100));
            loop {
                tick.tick().await;
                if !recording.load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                let events = {
                    let mut guard = hub.inner.lock();
                    let start = guard.last_event_id;
                    let delta = recorder.ring_delta(start);
                    guard.last_event_id = recorder.latest_ring_event_id().max(start);
                    delta
                };
                if events.is_empty() {
                    continue;
                }
                let batch = ControlResponse::FlightLogBatch(FlightLogBatch {
                    version: PROTOCOL_VERSION,
                    events: events.into_iter().map(FlightLogEvent::from).collect(),
                });
                let patterns = ControlResponse::PatternSnapshot(PatternSnapshot {
                    version: PROTOCOL_VERSION,
                    patterns: analysis.leaderboard_snapshot(),
                });
                let lines = [
                    serde_json::to_string(&batch),
                    serde_json::to_string(&patterns),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                let subs: Vec<_> = hub.inner.lock().subscribers.values().cloned().collect();
                for line in lines {
                    for tx in &subs {
                        let _ = tx.try_send(format!("{line}\n"));
                    }
                }
            }
        });
    }

    pub fn notify_anomaly(&self, anomaly: pg_analyzer::Anomaly) {
        let resp = ControlResponse::AnomalyNotification(AnomalyNotification {
            version: PROTOCOL_VERSION,
            anomaly,
        });
        if let Ok(line) = serde_json::to_string(&resp) {
            let subs: Vec<_> = self.inner.lock().subscribers.values().cloned().collect();
            for tx in subs {
                let _ = tx.try_send(format!("{line}\n"));
            }
        }
    }
}
