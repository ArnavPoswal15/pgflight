use pgflight_control::protocol::{ControlRequest, ControlResponse};
use pgflight_control::ControlClient;
use serde::Serialize;
use tauri::Manager;

#[derive(Debug, Serialize)]
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
async fn fetch_patterns() -> Result<Vec<UiPattern>, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::Status)
        .await
        .map_err(|e| e.to_string())?;
    if let Some(ControlResponse::Status(s)) = res.first() {
        let _ = s;
    }
    Ok(vec![])
}

#[tauri::command]
async fn fetch_live_log() -> Result<Vec<UiFlightLine>, String> {
    let client = ControlClient::default_socket().map_err(|e| e.to_string())?;
    let res = client
        .request(&ControlRequest::Subscribe {
            topics: vec!["events".into()],
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut lines = Vec::new();
    for r in res {
        if let ControlResponse::FlightLogBatch(b) = r {
            for e in b.events {
                lines.push(UiFlightLine {
                    text: format!(
                        "{} fp={} {:.2}ms rows={}",
                        e.canonical_sql,
                        e.fingerprint_id,
                        e.exec_us as f64 / 1000.0,
                        e.rows_returned
                    ),
                });
            }
        }
    }
    Ok(lines)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            recorder_status,
            fetch_live_log,
            fetch_patterns
        ])
        .setup(|app| {
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.set_title("pgflight — Query Flight Recorder");
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
