use crate::cancel::{CancelTarget, SharedCancelRegistry};
use crate::conn_state::{
    note_execute_enqueued, resolve_backend, ConnState, SharedConnState, IDLE_IN_TXN,
};
use crate::error::ProxyError;
use crate::telemetry::{try_send_event, DroppedEventCounter, IdleInTxnEvent, TelemetryEvent};
use crate::upstream::{
    connect_and_startup, split_upstream, UpstreamConfig, UpstreamRead, UpstreamWrite,
};
use bytes::{Bytes, BytesMut};
use pg_wire::ssl::decline_bytes;
use pg_wire::startup::{parse_startup_pdu, StartupMessage};
use pg_wire::STREAM_THRESHOLD;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep_until, Instant as TokioInstant};

pub async fn handle_connection(
    client: TcpStream,
    upstream_cfg: UpstreamConfig,
    registry: SharedCancelRegistry,
    event_tx: Option<Sender<TelemetryEvent>>,
    drop_counter: Arc<DroppedEventCounter>,
) -> Result<(), ProxyError> {
    let upstream_addr: SocketAddr = upstream_cfg
        .addr
        .parse()
        .map_err(|e: std::net::AddrParseError| ProxyError::Connect(e.to_string()))?;
    let cancel_target = CancelTarget {
        upstream_addr,
        use_tls: upstream_cfg.tls,
    };

    let mut client = client;
    loop {
        let pdu = read_startup_packet(&mut client).await?;
        match parse_startup_pdu(pdu)? {
            StartupMessage::CancelRequest {
                backend_pid,
                secret_key,
            } => {
                registry.send_cancel(backend_pid, secret_key).await?;
                return Ok(());
            }
            StartupMessage::SslRequest | StartupMessage::GssEncRequest => {
                client.write_all(&decline_bytes()).await?;
                continue;
            }
            StartupMessage::Startup { version, params } => {
                let upstream = connect_and_startup(&upstream_cfg, version, &params).await?;
                let (ur, uw) = split_upstream(upstream);
                bridge_connection(
                    client,
                    ur,
                    uw,
                    registry,
                    cancel_target,
                    event_tx,
                    drop_counter,
                )
                .await?;
                return Ok(());
            }
        }
    }
}

async fn read_startup_packet(client: &mut TcpStream) -> Result<Bytes, ProxyError> {
    let mut len_buf = [0u8; 4];
    client.read_exact(&mut len_buf).await?;
    let total = i32::from_be_bytes(len_buf);
    if total < 4 {
        return Err(ProxyError::Protocol("startup length too short".into()));
    }
    let mut rest = vec![0u8; (total - 4) as usize];
    if !rest.is_empty() {
        client.read_exact(&mut rest).await?;
    }
    let mut full = BytesMut::with_capacity(4 + rest.len());
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&rest);
    Ok(full.freeze())
}

async fn bridge_connection(
    client: TcpStream,
    upstream_read: UpstreamRead,
    upstream_write: UpstreamWrite,
    registry: SharedCancelRegistry,
    cancel_target: CancelTarget,
    event_tx: Option<Sender<TelemetryEvent>>,
    drop_counter: Arc<DroppedEventCounter>,
) -> Result<(), ProxyError> {
    let state: SharedConnState = Arc::new(parking_lot::Mutex::new(ConnState::new()));
    let (client_read, client_write) = split(client);

    let state_b = state.clone();
    let registry_b = registry.clone();
    let cancel_target_b = cancel_target.clone();
    let drop_b = drop_counter.clone();
    let event_tx_b = event_tx.clone();
    let mut ur = upstream_read;
    let mut cw = client_write;

    let task_b = tokio::spawn(async move {
        relay_upstream_to_client(
            &mut ur,
            &mut cw,
            &state_b,
            &registry_b,
            cancel_target_b,
            event_tx_b,
            drop_b,
        )
        .await
    });

    let state_a = state.clone();
    let drop_a = drop_counter.clone();
    let event_tx_a = event_tx;
    let mut cr = client_read;
    let mut uw = upstream_write;

    let task_a = tokio::spawn(async move {
        relay_client_to_upstream(&mut cr, &mut uw, &state_a, event_tx_a, drop_a).await
    });

    let _ = tokio::try_join!(task_a, task_b);
    Ok(())
}

async fn relay_client_to_upstream(
    client: &mut ReadHalf<TcpStream>,
    upstream: &mut UpstreamWrite,
    state: &SharedConnState,
    event_tx: Option<Sender<TelemetryEvent>>,
    drop_counter: Arc<DroppedEventCounter>,
) -> Result<(), ProxyError> {
    let mut idle_deadline = TokioInstant::now() + IDLE_IN_TXN;
    loop {
        let header_fut = read_frame_header(client);
        tokio::select! {
            _ = sleep_until(idle_deadline) => {
                let maybe_txn = {
                    let st = state.lock();
                    st.idle_in_txn_elapsed().map(|d| (st.txn_id, d))
                };
                if let (Some(tx), Some((txn_id, idle))) = (event_tx.as_ref(), maybe_txn) {
                    try_send_event(
                        tx,
                        &drop_counter,
                        TelemetryEvent::IdleInTxn(IdleInTxnEvent {
                            txn_id,
                            idle_us: idle.as_micros() as u64,
                        }),
                    );
                }
                idle_deadline = TokioInstant::now() + IDLE_IN_TXN;
            }
            res = header_fut => {
                let (tag, body_len) = match res {
                    Ok(v) => v,
                    Err(e) => return Err(e),
                };
                idle_deadline = TokioInstant::now() + IDLE_IN_TXN;
                if body_len <= STREAM_THRESHOLD {
                    let frame = read_small_frame(client, tag, body_len).await?;
                    {
                        let mut st = state.lock();
                        st.note_client_activity();
                        if matches!(tag, b'E' | b'Q') {
                            note_execute_enqueued(&mut st);
                        }
                        st.push_frontend(tag, &frame.body);
                    }
                    upstream.write_frame(tag, &frame.body).await?;
                } else {
                    {
                        let mut st = state.lock();
                        st.note_client_activity();
                        st.push_frontend(tag, &[]);
                    }
                    upstream.stream_body_from(client, tag, body_len).await?;
                }
                if tag == b'X' {
                    upstream.shutdown().await.ok();
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn relay_upstream_to_client(
    upstream: &mut UpstreamRead,
    client: &mut WriteHalf<TcpStream>,
    state: &SharedConnState,
    registry: &SharedCancelRegistry,
    cancel_target: CancelTarget,
    event_tx: Option<Sender<TelemetryEvent>>,
    drop_counter: Arc<DroppedEventCounter>,
) -> Result<(), ProxyError> {
    loop {
        let (tag, body_len) = upstream.read_frame_header().await?;
        if body_len <= STREAM_THRESHOLD {
            let frame = upstream.read_small_frame(tag, body_len).await?;
            if tag == b'K' {
                if let Some((pid, secret)) = pg_wire::frame::parse_backend_key_data(&frame.body) {
                    registry.register(pid, secret, cancel_target.clone());
                }
            }
            {
                let mut st = state.lock();
                resolve_backend(&mut st, tag, &frame.body, event_tx.as_ref(), &drop_counter);
            }
            pg_wire::frame::forward_buffered(client, tag, &frame.body)
                .await
                .map_err(ProxyError::from)?;
        } else {
            upstream.stream_body_to(client, tag, body_len).await?;
        }
    }
}

async fn read_frame_header<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
) -> Result<(u8, usize), ProxyError> {
    pg_wire::frame::read_frame_header(r)
        .await
        .map_err(ProxyError::from)
}

async fn read_small_frame<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    tag: u8,
    body_len: usize,
) -> Result<pg_wire::frame::Frame, ProxyError> {
    pg_wire::frame::read_small_frame(r, tag, body_len)
        .await
        .map_err(ProxyError::from)
}
