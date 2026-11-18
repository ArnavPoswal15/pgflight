use crate::protocol::{ControlRequest, ControlResponse, PROTOCOL_VERSION};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("daemon error: {0}")]
    Daemon(String),
    #[error("unexpected response")]
    Unexpected,
    #[error("HOME environment variable is not set")]
    NoHome,
}

pub fn control_socket_path() -> Result<PathBuf, ControlError> {
    let home = std::env::var("HOME").map_err(|_| ControlError::NoHome)?;
    Ok(PathBuf::from(home).join(".pgflight/control.sock"))
}

pub struct ControlClient {
    path: PathBuf,
}

impl ControlClient {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn default_socket() -> Result<Self, ControlError> {
        Ok(Self::new(control_socket_path()?))
    }

    pub async fn request(
        &self,
        req: &ControlRequest,
    ) -> Result<Vec<ControlResponse>, ControlError> {
        let mut stream = UnixStream::connect(&self.path).await?;
        let req_line = serde_json::to_string(req)?;
        stream.write_all(req_line.as_bytes()).await?;
        stream.write_all(b"\n").await?;

        if matches!(req, ControlRequest::Subscribe { .. }) {
            return self.read_stream(stream).await;
        }

        let (reader, _) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        if let Some(line) = lines.next_line().await? {
            let resp: ControlResponse = serde_json::from_str(&line)?;
            return Ok(vec![resp]);
        }
        Err(ControlError::Unexpected)
    }

    async fn read_stream(&self, stream: UnixStream) -> Result<Vec<ControlResponse>, ControlError> {
        let (reader, _) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        let mut out = Vec::new();
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let resp: ControlResponse = serde_json::from_str(&line)?;
            if matches!(resp, ControlResponse::Error { .. }) {
                if let ControlResponse::Error { message } = resp {
                    return Err(ControlError::Daemon(message));
                }
            }
            out.push(resp);
            if out.len() > 10_000 {
                break;
            }
        }
        if out.is_empty() {
            out.push(ControlResponse::Pong {
                version: PROTOCOL_VERSION,
            });
        }
        Ok(out)
    }

    pub async fn ping(&self) -> Result<(), ControlError> {
        let res = self.request(&ControlRequest::Ping).await?;
        match res.first() {
            Some(ControlResponse::Pong { .. }) => Ok(()),
            Some(ControlResponse::Error { message }) => Err(ControlError::Daemon(message.clone())),
            _ => Err(ControlError::Unexpected),
        }
    }

    /// Single-response RPC (Explain, ReplayRun, CompareReplay, Status, …).
    pub async fn request_one(&self, req: &ControlRequest) -> Result<ControlResponse, ControlError> {
        let mut res = self.request(req).await?;
        match res.pop() {
            Some(ControlResponse::Error { message }) => Err(ControlError::Daemon(message)),
            Some(r) => Ok(r),
            None => Err(ControlError::Unexpected),
        }
    }
}
