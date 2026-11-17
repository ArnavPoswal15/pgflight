use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("wire error: {0}")]
    Wire(#[from] pg_wire::WireError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream connect failed: {0}")]
    Connect(String),
    #[error("tls error: {0}")]
    Tls(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("cancel not found")]
    CancelNotFound,
}
