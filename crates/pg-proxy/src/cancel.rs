use crate::error::ProxyError;
use dashmap::DashMap;
use pg_wire::startup::encode_cancel;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct CancelTarget {
    pub upstream_addr: SocketAddr,
    pub use_tls: bool,
}

#[derive(Debug, Default)]
pub struct CancelRegistry {
    inner: DashMap<(i32, i32), CancelTarget>,
}

impl CancelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, backend_pid: i32, secret_key: i32, target: CancelTarget) {
        self.inner.insert((backend_pid, secret_key), target);
    }

    pub async fn send_cancel(&self, backend_pid: i32, secret_key: i32) -> Result<(), ProxyError> {
        let target = self
            .inner
            .get(&(backend_pid, secret_key))
            .map(|e| e.clone())
            .ok_or(ProxyError::CancelNotFound)?;
        let mut stream = TcpStream::connect(target.upstream_addr)
            .await
            .map_err(|e| ProxyError::Connect(e.to_string()))?;
        if target.use_tls {
            // Cancel is sent before TLS on a fresh connection (same as libpq).
        }
        let pkt = encode_cancel(backend_pid, secret_key);
        stream.write_all(&pkt).await?;
        stream.shutdown().await.ok();
        Ok(())
    }
}

pub type SharedCancelRegistry = Arc<CancelRegistry>;
