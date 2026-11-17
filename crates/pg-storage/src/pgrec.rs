//! `.pgrec` block writer (Phase 5). Phase 3 stub only.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgrecError {
    #[error("pgrec writer not implemented (Phase 5)")]
    NotImplemented,
}

/// ZSTD `.pgrec` block writer hook — full IO deferred to Phase 5.
pub trait PgrecWriter: Send + Sync {
    fn append_record(&mut self, _payload: &[u8]) -> Result<(), PgrecError> {
        Err(PgrecError::NotImplemented)
    }
}

/// No-op stub for Phase 3.
#[derive(Debug, Default)]
pub struct NoopPgrecWriter;

impl PgrecWriter for NoopPgrecWriter {}
