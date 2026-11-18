use parking_lot::Mutex;
use pgflight_control::protocol::{ControlRequest, ControlResponse, StatusResponse};
use pgflight_control::ControlClient;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager, State};

#[derive(Debug, Serialize, Clone)]
struct UiFlightLine {
    text: String,
}

#[derive(Debug, Serialize)]
struct UiPattern {
    fingerprint_id: i64,
    canonical_sql: String,
    exec_count: u64,
    p95_us: u64,
}

#[derive(Debug, Serialize, Clone)]
struct UiShadowStatus {
    enabled: bool,
    url: String,
    connected: bool,
    queued_jobs: u32,
    dropped_jobs: u64,
}

#[derive(Default)]
struct LiveLogState {
    lines: Mutex<Vec<UiFlightLine>>,
    started: AtomicBool,
}

const LIVE_LOG_CAP: usize = 200;

#[tauri::command]
async fn recorder_status() -> Result<String, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::Status)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::to_string(&res).map_err(|e| e.to_string())
}

#[tauri::command]
async fn shadow_status() -> Result<UiShadowStatus, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::Status)
        .await
        .map_err(|e| e.to_string())?;
    status_shadow(res).ok_or_else(|| "status response missing shadow".into())
}

fn status_shadow(res: Vec<ControlResponse>) -> Option<UiShadowStatus> {
    for r in res {
        if let ControlResponse::Status(StatusResponse {
            shadow: Some(s), ..
        }) = r
        {
            return Some(UiShadowStatus {
                enabled: s.enabled,
                url: s.url,
                connected: s.connected,
                queued_jobs: s.queued_jobs,
                dropped_jobs: s.dropped_jobs,
            });
        }
    }
    None
}

#[tauri::command]
async fn explain_query(
    fingerprint_id: Option<i64>,
    flight_id: Option<u64>,
    event_id: Option<u64>,
) -> Result<String, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::Explain {
            fingerprint_id,
            flight_id,
            event_id,
        })
        .await
        .map_err(|e| e.to_string())?;
    for r in res {
        match r {
            ControlResponse::ExplainResult(b) => {
                return serde_json::to_string_pretty(&b).map_err(|e| e.to_string());
            }
            ControlResponse::Error { message } => return Err(message),
            _ => {}
        }
    }
    Err("no explain result".into())
}

#[tauri::command]
async fn replay_run(
    pgrec_path: String,
    label: String,
    warmup_iters: u32,
    explain: bool,
) -> Result<String, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::ReplayRun {
            pgrec_path,
            label,
            warmup_iters,
            explain,
        })
        .await
        .map_err(|e| e.to_string())?;
    for r in res {
        match r {
            ControlResponse::ReplayResult(b) => {
                return serde_json::to_string_pretty(&b).map_err(|e| e.to_string());
            }
            ControlResponse::Error { message } => return Err(message),
            _ => {}
        }
    }
    Err("no replay result".into())
}

#[tauri::command]
async fn compare_replays(
    baseline_replay_id: String,
    baseline_label: String,
    variant_replay_id: String,
    variant_label: String,
) -> Result<String, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::CompareReplay {
            baseline: Some(pgflight_control::protocol::ReplayResultId {
                replay_id: baseline_replay_id,
                label: baseline_label,
            }),
            variant: Some(pgflight_control::protocol::ReplayResultId {
                replay_id: variant_replay_id,
                label: variant_label,
            }),
            baseline_plans: None,
            variant_plans: None,
        })
        .await
        .map_err(|e| e.to_string())?;
    for r in res {
        match r {
            ControlResponse::PlanCompare(b) => {
                return serde_json::to_string_pretty(&b).map_err(|e| e.to_string());
            }
            ControlResponse::Error { message } => return Err(message),
            _ => {}
        }
    }
    Err("no compare result".into())
}

#[tauri::command]
async fn fetch_patterns() -> Result<Vec<UiPattern>, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::Subscribe {
            topics: vec!["leaderboard".into()],
        })
        .await
        .map_err(|e| e.to_string())?;
    for r in res {
        if let ControlResponse::PatternSnapshot(p) = r {
            return Ok(p
                .patterns
                .into_iter()
                .map(|row| UiPattern {
                    fingerprint_id: row.fingerprint_id,
                    canonical_sql: row.canonical_sql,
                    exec_count: row.exec_count,
                    p95_us: row.p95_us,
                })
                .collect());
        }
    }
    Ok(vec![])
}

#[tauri::command]
async fn fetch_live_log(state: State<'_, Arc<LiveLogState>>) -> Result<Vec<UiFlightLine>, String> {
    Ok(state.lines.lock().clone())
}

fn spawn_live_subscriber(app: AppHandle, state: Arc<LiveLogState>) {
    if state.started.swap(true, Ordering::SeqCst) {
        return;
    }
    const RECONNECT: Duration = Duration::from_secs(2);
    tauri::async_runtime::spawn(async move {
        loop {
            let client = match ControlClient::default_socket() {
                Ok(c) => c,
                Err(_) => {
                    tokio::time::sleep(RECONNECT).await;
                    continue;
                }
            };
            let stream = match client
                .request(&ControlRequest::Subscribe {
                    topics: vec!["events".into(), "leaderboard".into()],
                })
                .await
            {
                Ok(s) => s,
                Err(_) => {
                    tokio::time::sleep(RECONNECT).await;
                    continue;
                }
            };
            for r in stream {
                match r {
                    ControlResponse::FlightLogBatch(b) => {
                        let mut guard = state.lines.lock();
                        for e in b.events {
                            guard.push(UiFlightLine {
                                text: format!(
                                    "{} fp={} {:.2}ms rows={}",
                                    e.canonical_sql,
                                    e.fingerprint_id,
                                    e.exec_us as f64 / 1000.0,
                                    e.rows_returned
                                ),
                            });
                        }
                        if guard.len() > LIVE_LOG_CAP {
                            let drop = guard.len() - LIVE_LOG_CAP;
                            guard.drain(0..drop);
                        }
                        drop(guard);
                        let _ = app.emit("pgflight://live-log", ());
                    }
                    ControlResponse::PatternSnapshot(_) => {
                        let _ = app.emit("pgflight://patterns", ());
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(RECONNECT).await;
        }
    });
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let live_state = Arc::new(LiveLogState::default());
    tauri::Builder::default()
        .manage(live_state.clone())
        .invoke_handler(tauri::generate_handler![
            recorder_status,
            shadow_status,
            explain_query,
            replay_run,
            compare_replays,
            fetch_live_log,
            fetch_patterns
        ])
        .setup(move |app| {
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.set_title("pgflight — Query Flight Recorder");
            }
            spawn_live_subscriber(app.handle().clone(), live_state);
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
