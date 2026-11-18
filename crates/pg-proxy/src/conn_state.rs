//! Per-connection protocol state (expected-op queue + txn + timing).

use crate::expected_op::{drain_to_next_sync, ExpectedOp};
use crate::telemetry::{
    try_send_event, DroppedEventCounter, RawQueryEvent, SessionSetEvent, TelemetryEvent,
    TxnBoundaryEvent, TxnBoundaryKind,
};
use parking_lot::Mutex;
use pg_wire::frame::datarow_field_count;
use pg_wire::frontend::{
    encode_param_values, parse_bind, parse_execute_portal, parse_parse, parse_query,
};
use pg_wire::BackendTag;
use serde_json::json;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;

pub const IDLE_IN_TXN: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
struct StatementMeta {
    query: String,
    param_oids: Vec<u32>,
}

#[derive(Debug, Clone)]
struct PortalBind {
    raw_sql: String,
    params_json: Option<String>,
    bind_param_bytes: Vec<u8>,
    params_truncated: bool,
    param_oids: Vec<u32>,
    bind_format_codes: Vec<i16>,
}

#[derive(Debug, Clone)]
struct PendingQuery {
    raw_sql: String,
    params_json: Option<String>,
    bind_param_bytes: Vec<u8>,
    params_truncated: bool,
    param_oids: Vec<u32>,
    bind_format_codes: Option<Vec<i16>>,
}

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
    pub connection_id: u64,
    pub app_name: String,
    statements: HashMap<String, StatementMeta>,
    portals: HashMap<String, PortalBind>,
    pending_by_query_id: HashMap<u64, PendingQuery>,
    pub last_error_code: Option<String>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self::new(0, String::new())
    }
}

impl ConnState {
    pub fn new(connection_id: u64, app_name: String) -> Self {
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
            connection_id,
            app_name,
            statements: HashMap::new(),
            portals: HashMap::new(),
            pending_by_query_id: HashMap::new(),
            last_error_code: None,
        }
    }

    pub fn note_client_activity(&mut self) {
        self.last_client_activity = Instant::now();
    }

    pub fn push_frontend(&mut self, tag: u8, body: &[u8]) {
        match tag {
            b'P' => {
                if let Ok(p) = parse_parse(body) {
                    self.statements.insert(
                        p.statement_name,
                        StatementMeta {
                            query: p.query,
                            param_oids: p.param_oids,
                        },
                    );
                }
            }
            b'B' => {
                if let Ok(b) = parse_bind(body) {
                    let (raw_sql, param_oids) = self
                        .statements
                        .get(&b.statement_name)
                        .map(|s| (s.query.clone(), s.param_oids.clone()))
                        .unwrap_or_else(|| (String::new(), Vec::new()));
                    let params_json = params_to_json(&b.params);
                    let mut bind_param_bytes = Vec::new();
                    if !b.params.is_empty() {
                        let _ = encode_param_values(&b.params, &mut bind_param_bytes);
                    }
                    self.portals.insert(
                        b.portal_name,
                        PortalBind {
                            raw_sql,
                            params_json,
                            bind_param_bytes,
                            params_truncated: b.params_truncated,
                            param_oids,
                            bind_format_codes: b.param_format_codes,
                        },
                    );
                }
            }
            b'E' => {
                let qid = self.next_query_id;
                let portal = parse_execute_portal(body)
                    .map(|e| e.portal_name)
                    .unwrap_or_default();
                self.attach_pending_for_execute(qid, &portal);
            }
            b'Q' => {
                let qid = self.next_query_id;
                if let Ok(q) = parse_query(body) {
                    self.pending_by_query_id.insert(
                        qid,
                        PendingQuery {
                            raw_sql: q.query,
                            params_json: None,
                            bind_param_bytes: Vec::new(),
                            params_truncated: false,
                            param_oids: Vec::new(),
                            bind_format_codes: None,
                        },
                    );
                }
            }
            _ => {}
        }
        crate::expected_op::push_for_frontend(
            &mut self.expected,
            tag,
            body,
            &mut self.next_query_id,
        );
    }

    fn attach_pending_for_execute(&mut self, query_id: u64, portal_name: &str) {
        if let Some(bind) = self.portals.get(portal_name).cloned() {
            self.pending_by_query_id.insert(
                query_id,
                PendingQuery {
                    raw_sql: bind.raw_sql,
                    params_json: bind.params_json,
                    bind_param_bytes: bind.bind_param_bytes,
                    params_truncated: bind.params_truncated,
                    param_oids: bind.param_oids,
                    bind_format_codes: Some(bind.bind_format_codes),
                },
            );
        }
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

fn params_to_json(params: &[Option<Vec<u8>>]) -> Option<String> {
    if params.is_empty() {
        return None;
    }
    let arr: Vec<serde_json::Value> = params
        .iter()
        .map(|p| match p {
            None => serde_json::Value::Null,
            Some(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    json!(s)
                } else {
                    json!(format!("\\x{}", hex_prefix(bytes)))
                }
            }
        })
        .collect();
    serde_json::to_string(&arr).ok()
}

fn hex_prefix(bytes: &[u8]) -> String {
    const MAX: usize = 64;
    let take = bytes.len().min(MAX);
    let mut s = String::with_capacity(take * 2);
    for b in &bytes[..take] {
        s.push_str(&format!("{b:02x}"));
    }
    if bytes.len() > MAX {
        s.push('…');
    }
    s
}

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
        state.last_error_code = pg_wire::backend_msg::parse_error_sqlstate(body);
        if let Some(ExpectedOp::Execute { query_id }) = state.expected.front().cloned() {
            emit_query_complete(state, query_id, event_tx, drop_counter);
        }
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
                update_rfq(state, body, event_tx, drop_counter);
            }
            (ExpectedOp::Sync, b'Z') => {
                state.expected.pop_front();
                update_rfq(state, body, event_tx, drop_counter);
            }
            _ => {}
        }
    } else if tag == b'Z' {
        update_rfq(state, body, event_tx, drop_counter);
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

fn is_session_set_sql(sql: &str) -> bool {
    let bytes = sql.trim_start().as_bytes();
    if bytes.len() < 3 {
        return false;
    }
    bytes[0].eq_ignore_ascii_case(&b's')
        && bytes[1].eq_ignore_ascii_case(&b'e')
        && bytes[2].eq_ignore_ascii_case(&b't')
        && bytes.get(3).is_none_or(|b| b.is_ascii_whitespace())
}

fn update_rfq(
    state: &mut ConnState,
    body: &[u8],
    event_tx: Option<&Sender<TelemetryEvent>>,
    drop_counter: &DroppedEventCounter,
) {
    let status = body.first().copied().unwrap_or(b'I');
    let prev = state.rfq_status;
    state.rfq_status = status;
    if prev == b'I' && status == b'T' {
        state.txn_id += 1;
        emit_txn_boundary(
            state,
            state.txn_id,
            TxnBoundaryKind::Begin,
            event_tx,
            drop_counter,
        );
    } else if status == b'I' {
        if prev == b'T' {
            emit_txn_boundary(
                state,
                state.txn_id,
                TxnBoundaryKind::EndCommit,
                event_tx,
                drop_counter,
            );
        } else if prev == b'E' {
            emit_txn_boundary(
                state,
                state.txn_id,
                TxnBoundaryKind::EndAbort,
                event_tx,
                drop_counter,
            );
        }
    }
}

fn emit_txn_boundary(
    state: &ConnState,
    txn_id: u64,
    kind: TxnBoundaryKind,
    event_tx: Option<&Sender<TelemetryEvent>>,
    drop_counter: &DroppedEventCounter,
) {
    if let Some(tx) = event_tx {
        try_send_event(
            tx,
            drop_counter,
            TelemetryEvent::TxnBoundary(TxnBoundaryEvent {
                connection_id: state.connection_id,
                txn_id,
                kind,
            }),
        );
    }
}

fn emit_session_set(
    state: &ConnState,
    sql: &str,
    event_tx: Option<&Sender<TelemetryEvent>>,
    drop_counter: &DroppedEventCounter,
) {
    if let Some(tx) = event_tx {
        try_send_event(
            tx,
            drop_counter,
            TelemetryEvent::SessionSet(SessionSetEvent {
                connection_id: state.connection_id,
                txn_id: state.txn_id,
                sql: sql.to_owned(),
            }),
        );
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

    let pending = state.pending_by_query_id.remove(&query_id);
    let (raw_sql, params_json, bind_param_bytes, params_truncated, param_oids, bind_format_codes) =
        pending
            .map(|p| {
                (
                    p.raw_sql,
                    p.params_json,
                    p.bind_param_bytes,
                    p.params_truncated,
                    p.param_oids,
                    p.bind_format_codes,
                )
            })
            .unwrap_or((String::new(), None, Vec::new(), false, Vec::new(), None));
    let error_code = state.last_error_code.take();
    let rows_returned = state.row_count;
    state.row_count = 0;

    if is_session_set_sql(&raw_sql) {
        emit_session_set(state, &raw_sql, event_tx, drop_counter);
    }

    if let Some(tx) = event_tx {
        try_send_event(
            tx,
            drop_counter,
            TelemetryEvent::QueryComplete(RawQueryEvent {
                query_id,
                txn_id: state.txn_id,
                connection_id: state.connection_id,
                app_name: state.app_name.clone(),
                queue_wait_us,
                exec_us,
                raw_sql,
                params_json,
                params_truncated,
                param_oids,
                bind_format_codes,
                bind_param_bytes,
                rows_returned,
                error_code,
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
        let mut st = ConnState::new(1, "test".into());
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
        let mut st = ConnState::new(1, "test".into());
        st.expected.push_back(ExpectedOp::ParseComplete);
        st.expected.push_back(ExpectedOp::Sync);
        resolve_backend(&mut st, b'E', &[], None, &DroppedEventCounter::default());
        assert_eq!(st.expected.front(), Some(&ExpectedOp::Sync));
    }

    #[test]
    fn simple_query_captured() {
        let mut st = ConnState::new(1, "app".into());
        let body = b"SELECT 1\0";
        st.push_frontend(b'Q', body);
        assert!(st.pending_by_query_id.contains_key(&1));
    }

    #[test]
    fn rfq_i_to_t_emits_begin_boundary() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut st = ConnState::new(7, "app".into());
        st.rfq_status = b'I';
        let drop = DroppedEventCounter::default();
        resolve_backend(&mut st, b'Z', b"T", Some(&tx), &drop);
        assert_eq!(st.txn_id, 1);
        let evt = rx.try_recv();
        assert!(evt.is_ok());
        if let Ok(TelemetryEvent::TxnBoundary(b)) = evt {
            assert_eq!(b.kind, TxnBoundaryKind::Begin);
            assert_eq!(b.txn_id, 1);
            assert_eq!(b.connection_id, 7);
        } else {
            panic!("expected TxnBoundary Begin");
        }
    }

    #[test]
    fn session_set_sql_detection() {
        assert!(is_session_set_sql("SET search_path = public"));
        assert!(is_session_set_sql("  set timezone = 'UTC'"));
        assert!(!is_session_set_sql("SELECT 1"));
    }
}
