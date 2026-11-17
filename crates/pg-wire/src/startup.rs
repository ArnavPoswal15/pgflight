//! Startup-phase messages (before v3 framed traffic).

use crate::{
    WireError, CANCEL_REQUEST, GSSENC_REQUEST, MAX_STARTUP_LEN, PROTOCOL_V3_0, SSL_REQUEST,
};
use bytes::{Buf, Bytes, BytesMut};

/// First client message classification (special requests or startup packet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupMessage {
    SslRequest,
    GssEncRequest,
    CancelRequest { backend_pid: i32, secret_key: i32 },
    Startup { version: i32, params: Bytes },
}

/// Read exactly `n` bytes for a startup-phase PDU (length-prefixed).
pub fn parse_startup_pdu(mut buf: Bytes) -> Result<StartupMessage, WireError> {
    if buf.len() < 4 {
        return Err(WireError::BadLength(buf.len() as i32));
    }
    let total_len = buf.get_i32();
    if !(4..=MAX_STARTUP_LEN).contains(&total_len) {
        return Err(WireError::BadLength(total_len));
    }
    let payload_len = (total_len - 4) as usize;
    if buf.remaining() < payload_len {
        return Err(WireError::BadLength(total_len));
    }
    if payload_len == 4 {
        let code = buf.get_i32();
        return Ok(match code {
            SSL_REQUEST => StartupMessage::SslRequest,
            GSSENC_REQUEST => StartupMessage::GssEncRequest,
            _ => return Err(WireError::BadLength(code)),
        });
    }
    if payload_len == 12 {
        let code = buf.get_i32();
        if code == CANCEL_REQUEST {
            let backend_pid = buf.get_i32();
            let secret_key = buf.get_i32();
            return Ok(StartupMessage::CancelRequest {
                backend_pid,
                secret_key,
            });
        }
        return Err(WireError::BadLength(code));
    }
    let version = buf.get_i32();
    if version != PROTOCOL_V3_0 && version != crate::PROTOCOL_V3_2 {
        return Err(WireError::BadLength(version));
    }
    let rest = buf.copy_to_bytes(buf.remaining());
    Ok(StartupMessage::Startup {
        version,
        params: rest,
    })
}

/// Serialize a startup packet (length + version + params) for upstream forward.
pub fn encode_startup(version: i32, params: &[u8]) -> BytesMut {
    let total = 4 + 4 + params.len();
    let mut out = BytesMut::with_capacity(total);
    out.extend_from_slice(&(total as i32).to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(params);
    out
}

/// Serialize cancel request (16-byte payload after length).
pub fn encode_cancel(backend_pid: i32, secret_key: i32) -> BytesMut {
    let mut out = BytesMut::with_capacity(16);
    out.extend_from_slice(&16_i32.to_be_bytes());
    out.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
    out.extend_from_slice(&backend_pid.to_be_bytes());
    out.extend_from_slice(&secret_key.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn ssl_request() {
        let mut raw = BytesMut::new();
        raw.extend_from_slice(&8_i32.to_be_bytes());
        raw.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        let msg = parse_startup_pdu(raw.freeze()).unwrap();
        assert_eq!(msg, StartupMessage::SslRequest);
    }

    #[test]
    fn gss_request() {
        let mut raw = BytesMut::new();
        raw.extend_from_slice(&8_i32.to_be_bytes());
        raw.extend_from_slice(&GSSENC_REQUEST.to_be_bytes());
        let msg = parse_startup_pdu(raw.freeze()).unwrap();
        assert_eq!(msg, StartupMessage::GssEncRequest);
    }

    #[test]
    fn cancel_request() {
        let mut raw = BytesMut::new();
        raw.extend_from_slice(&16_i32.to_be_bytes());
        raw.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        raw.extend_from_slice(&42_i32.to_be_bytes());
        raw.extend_from_slice(&99_i32.to_be_bytes());
        let msg = parse_startup_pdu(raw.freeze()).unwrap();
        assert_eq!(
            msg,
            StartupMessage::CancelRequest {
                backend_pid: 42,
                secret_key: 99
            }
        );
    }

    #[test]
    fn startup_packet() {
        let params = b"user\0postgres\0database\0shop\0\0";
        let encoded = encode_startup(PROTOCOL_V3_0, params);
        match parse_startup_pdu(encoded.freeze()).unwrap() {
            StartupMessage::Startup { version, params: p } => {
                assert_eq!(version, PROTOCOL_V3_0);
                assert_eq!(p.as_ref(), params);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rejects_short_length() {
        let raw = Bytes::from_static(&[0, 0, 0, 3]);
        assert!(parse_startup_pdu(raw).is_err());
    }
}
