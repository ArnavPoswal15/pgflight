//! v3 frame read/write with streaming for large bodies.

use crate::{peek_header, WireError, STREAM_THRESHOLD};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const IO_BUF: usize = 64 * 1024;

/// Small frame: tag + full body buffered in memory.
#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: u8,
    pub body: Bytes,
}

/// After header, body is either buffered or streamed.
#[derive(Debug)]
pub enum FrameBody {
    Buffered(Bytes),
    Stream { tag: u8, len: usize },
}

pub async fn read_frame_header<R: AsyncRead + Unpin>(r: &mut R) -> Result<(u8, usize), WireError> {
    let mut hdr = [0u8; 5];
    r.read_exact(&mut hdr).await?;
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);
    if len < 4 {
        return Err(WireError::BadLength(len));
    }
    Ok((hdr[0], (len - 4) as usize))
}

pub async fn read_small_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    tag: u8,
    body_len: usize,
) -> Result<Frame, WireError> {
    if body_len > STREAM_THRESHOLD {
        return Err(WireError::BadLength(body_len as i32));
    }
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        r.read_exact(&mut body).await?;
    }
    Ok(Frame {
        tag,
        body: Bytes::from(body),
    })
}

pub async fn read_frame_or_stream<R: AsyncRead + Unpin>(r: &mut R) -> Result<FrameBody, WireError> {
    let (tag, body_len) = read_frame_header(r).await?;
    if body_len <= STREAM_THRESHOLD {
        let frame = read_small_frame(r, tag, body_len).await?;
        Ok(FrameBody::Buffered(frame.body))
    } else {
        Ok(FrameBody::Stream { tag, len: body_len })
    }
}

/// Write a complete frame (header + body).
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    tag: u8,
    body: &[u8],
) -> Result<(), WireError> {
    let len = (body.len() + 4) as i32;
    let mut hdr = [0u8; 5];
    hdr[0] = tag;
    hdr[1..5].copy_from_slice(&len.to_be_bytes());
    w.write_all(&hdr).await?;
    if !body.is_empty() {
        w.write_all(body).await?;
    }
    Ok(())
}

/// Stream a large body after header was already parsed.
pub async fn stream_frame_body<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    r: &mut R,
    w: &mut W,
    tag: u8,
    body_len: usize,
) -> Result<(), WireError> {
    let len = (body_len + 4) as i32;
    let mut hdr = [0u8; 5];
    hdr[0] = tag;
    hdr[1..5].copy_from_slice(&len.to_be_bytes());
    w.write_all(&hdr).await?;
    copy_n(r, w, body_len).await
}

pub async fn forward_buffered<W: AsyncWrite + Unpin>(
    w: &mut W,
    tag: u8,
    body: &[u8],
) -> Result<(), WireError> {
    write_frame(w, tag, body).await
}

async fn copy_n<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    r: &mut R,
    w: &mut W,
    n: usize,
) -> Result<(), WireError> {
    let mut left = n;
    let mut buf = vec![0u8; IO_BUF.min(left.max(1))];
    while left > 0 {
        let chunk = buf.len().min(left);
        r.read_exact(&mut buf[..chunk]).await?;
        w.write_all(&buf[..chunk]).await?;
        left -= chunk;
    }
    Ok(())
}

/// Count fields in a DataRow body (without deserializing values).
pub fn datarow_field_count(body: &[u8]) -> Option<u16> {
    if body.len() < 2 {
        return None;
    }
    Some(u16::from_be_bytes([body[0], body[1]]))
}

/// Parse BackendKeyData body: backend_pid, secret_key.
pub fn parse_backend_key_data(body: &[u8]) -> Option<(i32, i32)> {
    if body.len() < 8 {
        return None;
    }
    let pid = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let secret = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
    Some((pid, secret))
}

pub fn frame_from_parts(tag: u8, body: Bytes) -> BytesMut {
    let len = (body.len() + 4) as i32;
    let mut out = BytesMut::with_capacity(5 + body.len());
    out.put_u8(tag);
    out.put_i32(len);
    out.extend_from_slice(&body);
    out
}

pub fn peek_header_sync(buf: &[u8]) -> Result<Option<(u8, usize)>, WireError> {
    peek_header(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn datarow_fields() {
        assert_eq!(datarow_field_count(&[0, 3]), Some(3));
    }

    #[test]
    fn backend_key() {
        let body = [0, 0, 0, 7, 0, 0, 0, 9];
        assert_eq!(parse_backend_key_data(&body), Some((7, 9)));
    }

    #[tokio::test]
    async fn stream_threshold_boundary() {
        let body = vec![0u8; STREAM_THRESHOLD];
        let (mut client, mut server) = duplex(STREAM_THRESHOLD + 64);
        write_frame(&mut client, b'D', &body).await.unwrap();
        let fb = read_frame_or_stream(&mut server).await.unwrap();
        match fb {
            FrameBody::Buffered(b) => assert_eq!(b.len(), STREAM_THRESHOLD),
            FrameBody::Stream { .. } => panic!("should buffer at threshold"),
        }
    }

    #[tokio::test]
    async fn over_threshold_streams() {
        let body_len = STREAM_THRESHOLD + 1;
        let (mut client, mut server) = duplex(body_len + 64);
        write_frame(&mut client, b'd', &vec![0u8; body_len])
            .await
            .unwrap();
        let (tag, len) = read_frame_header(&mut server).await.unwrap();
        assert_eq!(tag, b'd');
        assert_eq!(len, body_len);
    }
}
