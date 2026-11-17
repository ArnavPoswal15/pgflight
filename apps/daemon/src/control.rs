use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::warn;

#[derive(Debug, Deserialize)]
struct ControlRequest {
    op: String,
}

#[derive(Debug, Serialize)]
struct ControlResponse {
    ok: bool,
}

pub async fn serve_control_socket(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
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
                    tokio::spawn(async move {
                        if let Err(e) = handle_control_conn(stream).await {
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

async fn handle_control_conn(stream: UnixStream) -> std::io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    if let Some(line) = lines.next_line().await? {
        let resp = match serde_json::from_str::<ControlRequest>(&line) {
            Ok(req) if req.op == "ping" => ControlResponse { ok: true },
            _ => ControlResponse { ok: false },
        };
        let body = serde_json::to_string(&resp).unwrap_or_else(|_| "{\"ok\":false}".into());
        writer.write_all(body.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    Ok(())
}
