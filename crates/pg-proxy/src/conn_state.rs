//! Per-connection protocol state (expected-op queue + txn + timing).

use crate::expected_op::{drain_to_next_sync, ExpectedOp};
use crate::telemetry::{try_send_event, DroppedEventCounter, RawQueryEvent, TelemetryEvent};
use parking_lot::Mutex;
use pg_wire::frame::datarow_field_count;
use pg_wire::BackendTag;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

pub const IDLE_IN_TXN: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct ConnState {
    pub expected: VecDeque<ExpectedOp>,
    pub txn_id: u64,
    pub rfq_status: u8,
    pub next_query_id: u64,
    pub exec_started: Option<Instant>,
    pub queue_enqueued: Option<Instant>,
    pub copy_mode: bool,
    pub row_count: u64,
    pub last_client_activity: Instant,
}

impl Default for ConnState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnState {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            expected: VecDeque::new(),
            txn_id: 0,
            rfq_status: b'I',
            next_query_id: 1,
            exec_started: None,
            queue_enqueued: None,
            copy_mode: false,
            row_count: 0,
            last_client_activity: now,
        }
    }

    pub fn note_client_activity(&mut self) {
        self.last_client_activity = Instant::now();
    }

    pub fn push_frontend(&mut self, tag: u8, body: &[u8]) {
        crate::expected_op::push_for_frontend(
            &mut self.expected,
            tag,
            body,
            &mut self.next_query_id,
        );
    }

    pub fn idle_in_txn_elapsed(&self) -> Option<Duration> {
        if !self.expected.is_empty() || !matches!(self.rfq_status, b'T' | b'E') {
            return None;
        }
        let elapsed = self.last_client_activity.elapsed();
        if elapsed >= IDLE_IN_TXN {
            Some(elapsed)
        } else {
            None
        }
    }
}

pub type SharedConnState = Arc<Mutex<ConnState>>;

pub fn resolve_backend(
    state: &mut ConnState,
    tag: u8,
    body: &[u8],
    event_tx: Option<&Sender<TelemetryEvent>>,
    drop_counter: &DroppedEventCounter,
) {
    let backend_tag = match BackendTag::from_byte(tag) {
        Ok(t) => t,
        Err(_) => return,
    };

    if backend_tag.is_async() {
        return;
    }

    if matches!(
        backend_tag,
        BackendTag::CopyInResponse | BackendTag::CopyOutResponse | BackendTag::CopyBothResponse
    ) {
        state.copy_mode = true;
    }
    if matches!(backend_tag, BackendTag::CopyDone) {
        state.copy_mode = false;
    }

    if tag == b'E' {
        drain_to_next_sync(&mut state.expected);
        return;
    }

    if state.copy_mode && matches!(backend_tag, BackendTag::CopyData) {
        return;
    }

    if tag == b'D' && matches!(state.expected.front(), Some(ExpectedOp::Execute { .. })) {
        let _ = datarow_field_count(body);
        state.row_count += 1;
        return;
    }

    if tag == b'T' || tag == b't' || tag == b'n' {
        resolve_describe(state, tag);
        return;
    }

    if let Some(front) = state.expected.front().cloned() {
        match (front, tag) {
            (ExpectedOp::ParseComplete, b'1') => {
                state.expected.pop_front();
            }
            (ExpectedOp::BindComplete, b'2') => {
                state.expected.pop_front();
            }
            (ExpectedOp::CloseComplete, b'3') => {
                state.expected.pop_front();
            }
            (ExpectedOp::Execute { query_id }, b'C' | b'I' | b's') => {
                state.expected.pop_front();
                emit_query_complete(state, query_id, event_tx, drop_counter);
            }
            (ExpectedOp::SimpleBatch { query_id }, b'C' | b'I') => {
                emit_query_complete(state, query_id, event_tx, drop_counter);
            }
            (ExpectedOp::SimpleBatch { query_id }, b'Z') => {
                state.expected.pop_front();
                emit_query_complete(state, query_id, event_tx, drop_counter);
                update_rfq(state, body);
            }
            (ExpectedOp::Sync, b'Z') => {
                state.expected.pop_front();
                update_rfq(state, body);
            }
            _ => {}
        }
    } else if tag == b'Z' {
        update_rfq(state, body);
    }
}

fn resolve_describe(state: &mut ConnState, tag: u8) {
    let front = match state.expected.front() {
        Some(op) => op.clone(),
        None => return,
    };
    match (&front, tag) {
        (ExpectedOp::DescribeStatement, b't') => {}
        (ExpectedOp::DescribeStatement, b'T' | b'n') => {
            state.expected.pop_front();
        }
        (ExpectedOp::DescribePortal, b'T' | b'n') => {
            state.expected.pop_front();
        }
        _ => {}
    }
}

fn update_rfq(state: &mut ConnState, body: &[u8]) {
    let status = body.first().copied().unwrap_or(b'I');
    let prev = state.rfq_status;
    state.rfq_status = status;
    if prev == b'I' && status == b'T' {
        state.txn_id += 1;
    }
}

fn emit_query_complete(
    state: &mut ConnState,
    query_id: u64,
    event_tx: Option<&Sender<TelemetryEvent>>,
    drop_counter: &DroppedEventCounter,
) {
    let now = Instant::now();
    let (queue_wait_us, exec_us) = match (state.queue_enqueued, state.exec_started) {
        (Some(q), Some(e)) => (
            e.saturating_duration_since(q).as_micros() as u64,
            now.saturating_duration_since(e).as_micros() as u64,
        ),
        (Some(q), None) => (now.saturating_duration_since(q).as_micros() as u64, 0),
        _ => (0, 0),
    };
    state.queue_enqueued = None;
    state.exec_started = None;
    if let Some(tx) = event_tx {
        try_send_event(
            tx,
            drop_counter,
            TelemetryEvent::QueryComplete(RawQueryEvent {
                query_id,
                txn_id: state.txn_id,
                queue_wait_us,
                exec_us,
            }),
        );
    }
}

pub fn note_execute_enqueued(state: &mut ConnState) {
    let now = Instant::now();
    state.queue_enqueued = Some(now);
    state.exec_started = Some(now);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expected_op::ExpectedOp;

    #[test]
    fn datarow_does_not_pop_execute() {
        let mut st = ConnState::new();
        st.expected.push_back(ExpectedOp::Execute { query_id: 1 });
        resolve_backend(
            &mut st,
            b'D',
            &[0, 2],
            None,
            &DroppedEventCounter::default(),
        );
        assert_eq!(st.row_count, 1);
        assert_eq!(st.expected.len(), 1);
    }

    #[test]
    fn error_drains_to_sync() {
        let mut st = ConnState::new();
        st.expected.push_back(ExpectedOp::ParseComplete);
        st.expected.push_back(ExpectedOp::Sync);
        resolve_backend(&mut st, b'E', &[], None, &DroppedEventCounter::default());
        assert_eq!(st.expected.front(), Some(&ExpectedOp::Sync));
    }
}
