//! PostgreSQL v3.0 / v3.2 framing. Frontend and backend tags are SEPARATE enums
//! because letters collide (D, C, E, S mean different things per direction).

pub mod backend_msg;
pub mod frame;
pub mod frontend;
pub mod ssl;
pub mod startup;

pub const SSL_REQUEST: i32 = 80877103;
pub const CANCEL_REQUEST: i32 = 80877102;
pub const GSSENC_REQUEST: i32 = 80877104;
pub const PROTOCOL_V3_0: i32 = 196608;
pub const PROTOCOL_V3_2: i32 = 196610;
pub const MAX_STARTUP_LEN: i32 = 10 * 1024 * 1024;
/// Bodies larger than this are streamed in chunks instead of buffered.
pub const STREAM_THRESHOLD: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("invalid frame length {0}")]
    BadLength(i32),
    #[error("unknown tag {0:#x}")]
    UnknownTag(u8),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontendTag {
    Query,
    Parse,
    Bind,
    Describe,
    Execute,
    Close,
    Sync,
    Flush,
    CopyData,
    CopyDone,
    CopyFail,
    Terminate,
    PasswordOrSasl,
}

impl FrontendTag {
    pub fn from_byte(b: u8) -> Result<Self, WireError> {
        Ok(match b {
            b'Q' => Self::Query,
            b'P' => Self::Parse,
            b'B' => Self::Bind,
            b'D' => Self::Describe,
            b'E' => Self::Execute,
            b'C' => Self::Close,
            b'S' => Self::Sync,
            b'H' => Self::Flush,
            b'd' => Self::CopyData,
            b'c' => Self::CopyDone,
            b'f' => Self::CopyFail,
            b'X' => Self::Terminate,
            b'p' => Self::PasswordOrSasl,
            other => return Err(WireError::UnknownTag(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendTag {
    Authentication,
    BackendKeyData,
    ParameterStatus,
    ReadyForQuery,
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    ParameterDescription,
    RowDescription,
    DataRow,
    CommandComplete,
    EmptyQuery,
    PortalSuspended,
    ErrorResponse,
    NoticeResponse,
    NotificationResponse,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,
    CopyData,
    CopyDone,
    NegotiateProtocolVersion,
}

impl BackendTag {
    pub fn from_byte(b: u8) -> Result<Self, WireError> {
        Ok(match b {
            b'R' => Self::Authentication,
            b'K' => Self::BackendKeyData,
            b'S' => Self::ParameterStatus,
            b'Z' => Self::ReadyForQuery,
            b'1' => Self::ParseComplete,
            b'2' => Self::BindComplete,
            b'3' => Self::CloseComplete,
            b'n' => Self::NoData,
            b't' => Self::ParameterDescription,
            b'T' => Self::RowDescription,
            b'D' => Self::DataRow,
            b'C' => Self::CommandComplete,
            b'I' => Self::EmptyQuery,
            b's' => Self::PortalSuspended,
            b'E' => Self::ErrorResponse,
            b'N' => Self::NoticeResponse,
            b'A' => Self::NotificationResponse,
            b'G' => Self::CopyInResponse,
            b'H' => Self::CopyOutResponse,
            b'W' => Self::CopyBothResponse,
            b'd' => Self::CopyData,
            b'c' => Self::CopyDone,
            b'v' => Self::NegotiateProtocolVersion,
            other => return Err(WireError::UnknownTag(other)),
        })
    }

    /// Async messages can arrive at any time and must NOT pop the expected-op queue.
    pub fn is_async(self) -> bool {
        matches!(
            self,
            Self::NoticeResponse | Self::ParameterStatus | Self::NotificationResponse
        )
    }
}

/// Parse a typed frame header: returns (tag, body_len). `len` on the wire includes its own 4 bytes.
pub fn peek_header(buf: &[u8]) -> Result<Option<(u8, usize)>, WireError> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len < 4 {
        return Err(WireError::BadLength(len));
    }
    Ok(Some((buf[0], (len - 4) as usize)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_partial_and_full() {
        assert_eq!(peek_header(&[b'Z', 0, 0]).unwrap(), None);
        assert_eq!(peek_header(&[b'Z', 0, 0, 0, 5]).unwrap(), Some((b'Z', 1)));
    }

    #[test]
    fn rejects_short_len() {
        assert!(peek_header(&[b'Q', 0, 0, 0, 3]).is_err());
    }

    #[test]
    fn tags_differ_by_direction() {
        assert_eq!(FrontendTag::from_byte(b'D').unwrap(), FrontendTag::Describe);
        assert_eq!(BackendTag::from_byte(b'D').unwrap(), BackendTag::DataRow);
        assert!(BackendTag::NoticeResponse.is_async());
    }
}
