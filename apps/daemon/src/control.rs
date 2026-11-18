use crate::analysis::AnalysisState;
use crate::event_hub::EventHub;
use crate::recorder::RecorderState;
use crate::shadow::ShadowHandle;
use pg_storage::{count_events, open_db, sqlite::now_ms};
use pgflight_control::protocol::{
    ControlRequest, ControlResponse, PatternSnapshot, RecorderStatus, StatusResponse,
    PROTOCOL_VERSION,
};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::warn;

/// Who may connect to the Unix control socket (defense in depth with mode `0600`).
#[derive(Debug, Clone, Copy)]
pub struct ControlPeerPolicy {
    pub owner_uid: u32,
    pub extra_peer_uid: Option<u32>,
    pub allow_any: bool,
}

impl ControlPeerPolicy {
    pub fn from_daemon_uid(owner_uid: u32, extra_peer_uid: Option<u32>, allow_any: bool) -> Self {
        Self {
            owner_uid,
            extra_peer_uid,
            allow_any,
        }
    }

    pub fn is_uid_allowed(&self, peer_uid: u32) -> bool {
        if self.allow_any {
            return true;
        }
        if peer_uid == self.owner_uid {
            return true;
        }
        self.extra_peer_uid == Some(peer_uid)
    }

    pub fn peer_allowed(&self, stream: &UnixStream) -> bool {
        if self.allow_any {
            return true;
        }
        match peer_uid(stream) {
            Ok(uid) => self.is_uid_allowed(uid),
            Err(e) => {
                warn!("control socket: cannot read peer credentials: {e}");
                false
            }
        }
    }
}

#[cfg(unix)]
fn peer_uid(stream: &UnixStream) -> std::io::Result<u32> {
    use std::os::unix::io::AsRawFd;
    let fd = stream.as_raw_fd();
    #[cfg(target_os = "macos")]
    {
        let mut uid: libc::uid_t = 0;
        let mut gid: libc::gid_t = 0;
        let rc = unsafe { libc::getpeereid(fd, &mut uid, &mut gid) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(uid)
    }
    #[cfg(target_os = "linux")]
    {
        #[repr(C)]
        struct Ucred {
            pid: libc::c_int,
            uid: libc::uid_t,
            gid: libc::gid_t,
        }
        let mut cred = Ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut len = std::mem::size_of::<Ucred>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                &mut cred as *mut Ucred as *mut libc::c_void,
                &mut len,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(cred.uid)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = fd;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "peer credentials not supported on this platform",
        ))
    }
}

#[cfg(not(unix))]
fn peer_uid(_stream: &UnixStream) -> std::io::Result<u32> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Unix control socket requires a Unix platform",
    ))
}

pub struct ControlContext {
    pub listen: String,
    pub upstream: String,
    pub recorder: RecorderState,
    pub hub: Arc<EventHub>,
    pub analysis: Arc<AnalysisState>,
    pub shadow: ShadowHandle,
}

pub async fn serve_control_socket(
    path: &Path,
    ctx: Arc<ControlContext>,
    peer_policy: ControlPeerPolicy,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    if !peer_policy.peer_allowed(&stream) {
                        let peer = peer_uid(&stream).ok();
                        let owner = peer_policy.owner_uid;
                        warn!(
                            peer_uid = ?peer,
                            allowed_owner_uid = owner,
                            allowed_extra_uid = ?peer_policy.extra_peer_uid,
                            "control socket: rejected peer UID; closing connection"
                        );
                        continue;
                    }
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_control_conn(stream, ctx).await {
                            warn!("control connection error: {e}");
                        }
                    });
                }
                Err(e) => warn!("control accept error: {e}"),
            }
        }
    });
    Ok(())
}

async fn handle_control_conn(stream: UnixStream, ctx: Arc<ControlContext>) -> std::io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let Some(line) = lines.next_line().await? else {
        return Ok(());
    };
    let req: ControlRequest = match serde_json::from_str(&line) {
        Ok(v) => v,
        Err(e) => {
            write_response(
                &mut writer,
                &ControlResponse::Error {
                    message: format!("bad request: {e}"),
                },
            )
            .await?;
            return Ok(());
        }
    };

    match req {
        ControlRequest::Ping => {
            write_response(
                &mut writer,
                &ControlResponse::Pong {
                    version: PROTOCOL_VERSION,
                },
            )
            .await?;
        }
        ControlRequest::Status => {
            let status = build_status(&ctx)?;
            write_response(&mut writer, &ControlResponse::Status(status)).await?;
        }
        ControlRequest::Subscribe { topics: _ } => {
            let (sub_id, mut rx) = ctx.hub.subscribe();
            let patterns = ControlResponse::PatternSnapshot(PatternSnapshot {
                version: PROTOCOL_VERSION,
                patterns: ctx.analysis.leaderboard_snapshot(),
            });
            write_response(&mut writer, &patterns).await?;
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(chunk) => {
                                writer.write_all(chunk.as_bytes()).await?;
                            }
                            None => break,
                        }
                    }
                    line = lines.next_line() => {
                        if let Ok(Some(_)) = line {
                            ctx.hub.unsubscribe(sub_id);
                            break;
                        }
                    }
                }
            }
        }
        ControlRequest::Unsubscribe => {}
        ControlRequest::Export { since_secs } => {
            // Metadata-only export (no raw_sql / params_json) regardless of DB contents (AUD-009).
            let export_lines = {
                let cutoff = now_ms().saturating_sub(since_secs.unwrap_or(300) * 1000);
                let conn = open_db(ctx.recorder.flight_recorder.db_path())
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let mut stmt = conn
                    .prepare(
                        "SELECT connection_id, txn_id, fingerprint_id, exec_us, rows_returned, created_at_ms
                         FROM query_events WHERE created_at_ms >= ?1 ORDER BY id ASC LIMIT 5000",
                    )
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let rows = stmt
                    .query_map([cutoff as i64], |r| {
                        Ok(serde_json::json!({
                            "connection_id": r.get::<_, i64>(0)?,
                            "txn_id": r.get::<_, i64>(1)?,
                            "fingerprint_id": r.get::<_, i64>(2)?,
                            "exec_us": r.get::<_, i64>(3)?,
                            "rows_returned": r.get::<_, i64>(4)?,
                            "created_at_ms": r.get::<_, i64>(5)?,
                        }))
                    })
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                rows.flatten()
                    .map(|row| row.to_string())
                    .collect::<Vec<_>>()
            };
            for line in export_lines {
                write_response(&mut writer, &ControlResponse::ExportLine { line }).await?;
            }
        }
        ControlRequest::Explain {
            fingerprint_id,
            flight_id,
            event_id,
        } => {
            match ctx
                .shadow
                .explain(fingerprint_id, flight_id, event_id)
                .await
            {
                Ok(body) => {
                    write_response(&mut writer, &ControlResponse::ExplainResult(body)).await?;
                }
                Err(e) => {
                    write_response(&mut writer, &ControlResponse::Error { message: e }).await?;
                }
            }
        }
        ControlRequest::ReplayRun {
            pgrec_path,
            label,
            warmup_iters,
            explain,
        } => {
            match ctx
                .shadow
                .replay_run(pgrec_path, label, warmup_iters, explain)
                .await
            {
                Ok(body) => {
                    write_response(&mut writer, &ControlResponse::ReplayResult(body)).await?;
                }
                Err(e) => {
                    write_response(&mut writer, &ControlResponse::Error { message: e }).await?;
                }
            }
        }
        ControlRequest::CompareReplay {
            baseline,
            variant,
            baseline_plans,
            variant_plans,
        } => {
            match ctx
                .shadow
                .compare_replay(baseline, variant, baseline_plans, variant_plans)
                .await
            {
                Ok(body) => {
                    write_response(&mut writer, &ControlResponse::PlanCompare(body)).await?;
                }
                Err(e) => {
                    write_response(&mut writer, &ControlResponse::Error { message: e }).await?;
                }
            }
        }
    }
    Ok(())
}

fn build_status(ctx: &ControlContext) -> std::io::Result<StatusResponse> {
    let events_recorded = ctx.recorder.flight_recorder.events_recorded();
    let db_count = open_db(ctx.recorder.flight_recorder.db_path())
        .ok()
        .and_then(|c| count_events(&c).ok())
        .unwrap_or(events_recorded);
    Ok(StatusResponse {
        version: PROTOCOL_VERSION,
        listen: ctx.listen.clone(),
        upstream: ctx.upstream.clone(),
        recorder: RecorderStatus {
            flight_id: ctx.recorder.flight_recorder.flight_id,
            recording: ctx
                .recorder
                .recording
                .load(std::sync::atomic::Ordering::Relaxed),
            events_recorded: db_count.max(events_recorded),
            db_path: ctx.recorder.flight_recorder.db_path().display().to_string(),
            redact_params: ctx.recorder.flight_recorder.redact_params,
            dropped_events: ctx.recorder.dropped_proxy.get(),
            dropped_storage_events: ctx.recorder.flight_recorder.dropped_batch_events(),
        },
        anomaly_count: ctx.analysis.anomalies().len(),
        shadow: Some(ctx.shadow.status()),
    })
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &ControlResponse,
) -> std::io::Result<()> {
    let body = serde_json::to_string(resp).unwrap_or_else(|_| "{\"op\":\"error\"}".into());
    writer.write_all(body.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}

#[cfg(test)]
mod peer_policy_tests {
    use super::ControlPeerPolicy;

    #[test]
    fn default_allows_owner_and_optional_extra() {
        let policy = ControlPeerPolicy::from_daemon_uid(1000, Some(1001), false);
        assert!(policy.is_uid_allowed(1000));
        assert!(policy.is_uid_allowed(1001));
        assert!(!policy.is_uid_allowed(1002));
    }

    #[test]
    fn allow_any_disables_uid_list() {
        let policy = ControlPeerPolicy::from_daemon_uid(1000, None, true);
        assert!(policy.is_uid_allowed(42));
    }
}
