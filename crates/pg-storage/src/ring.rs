//! In-memory ring buffer (live cockpit black box).

use std::collections::VecDeque;

pub const DEFAULT_RING_CAP: usize = 5_000;

#[derive(Debug, Clone)]
pub struct RingEntry {
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

pub struct RingBuffer {
    cap: usize,
    inner: VecDeque<RingEntry>,
    next_event_id: u64,
}

impl RingBuffer {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            inner: VecDeque::with_capacity(cap.min(512)),
            next_event_id: 1,
        }
    }

    pub fn push(&mut self, mut entry: RingEntry) {
        entry.event_id = self.next_event_id;
        self.next_event_id += 1;
        if self.inner.len() >= self.cap {
            self.inner.pop_front();
        }
        self.inner.push_back(entry);
    }

    pub fn snapshot(&self) -> Vec<RingEntry> {
        self.inner.iter().cloned().collect()
    }

    pub fn snapshot_since_event_id(&self, after: u64) -> Vec<RingEntry> {
        self.inner
            .iter()
            .filter(|e| e.event_id > after)
            .cloned()
            .collect()
    }

    pub fn latest_event_id(&self) -> u64 {
        self.inner.back().map(|e| e.event_id).unwrap_or(0)
    }
}
