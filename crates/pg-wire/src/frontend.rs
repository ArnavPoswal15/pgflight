//! Frontend message parsers (Parse / Bind / Query / Execute portal name).

use crate::WireError;

/// Max bytes captured for bind parameter payloads (per FEATURES_AND_LOGIC).
pub const PARAM_CAPTURE_CAP: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseMessage {
    pub statement_name: String,
    pub query: String,
    /// Parameter type OIDs from the Parse body (empty when `num_params == 0`).
    pub param_oids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindMessage {
    pub portal_name: String,
    pub statement_name: String,
    /// One format code per parameter (`0` = text, `1` = binary); expanded from the wire encoding.
    pub param_format_codes: Vec<i16>,
    pub params: Vec<Option<Vec<u8>>>,
    pub params_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMessage {
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteMessage {
    pub portal_name: String,
}

fn read_cstring(body: &[u8], offset: &mut usize) -> Result<String, WireError> {
    if *offset >= body.len() {
        return Err(WireError::BadLength(body.len() as i32));
    }
    let rest = &body[*offset..];
    let nul = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or(WireError::BadLength(body.len() as i32))?;
    let s = std::str::from_utf8(&rest[..nul]).map_err(|_| WireError::BadLength(nul as i32))?;
    *offset += nul + 1;
    Ok(s.to_owned())
}

fn read_i16(body: &[u8], offset: &mut usize) -> Result<i16, WireError> {
    if *offset + 2 > body.len() {
        return Err(WireError::BadLength(body.len() as i32));
    }
    let v = i16::from_be_bytes([body[*offset], body[*offset + 1]]);
    *offset += 2;
    Ok(v)
}

fn read_i32(body: &[u8], offset: &mut usize) -> Result<i32, WireError> {
    if *offset + 4 > body.len() {
        return Err(WireError::BadLength(body.len() as i32));
    }
    let v = i32::from_be_bytes([
        body[*offset],
        body[*offset + 1],
        body[*offset + 2],
        body[*offset + 3],
    ]);
    *offset += 4;
    Ok(v)
}

fn expand_param_format_codes(
    num_formats: i16,
    raw: &[i16],
    num_params: usize,
) -> Result<Vec<i16>, WireError> {
    if num_params == 0 {
        return Ok(Vec::new());
    }
    match num_formats {
        0 => Ok(vec![0; num_params]),
        1 => raw
            .first()
            .copied()
            .map(|f| vec![f; num_params])
            .ok_or(WireError::BadLength(num_params as i32)),
        nf if nf as usize == num_params => Ok(raw.to_vec()),
        _ => Err(WireError::BadLength(num_formats as i32)),
    }
}

/// Parse **Parse (`P`)** body: statement name + query (NUL-terminated) + param OIDs.
pub fn parse_parse(body: &[u8]) -> Result<ParseMessage, WireError> {
    let mut off = 0;
    let statement_name = read_cstring(body, &mut off)?;
    let query = read_cstring(body, &mut off)?;
    let num_params = read_i16(body, &mut off)?;
    if num_params < 0 {
        return Err(WireError::BadLength(num_params as i32));
    }
    let n = num_params as usize;
    let mut param_oids = Vec::with_capacity(n);
    for _ in 0..n {
        let oid = read_i32(body, &mut off)? as u32;
        param_oids.push(oid);
    }
    Ok(ParseMessage {
        statement_name,
        query,
        param_oids,
    })
}

/// Parse **Bind (`B`)** body.
pub fn parse_bind(body: &[u8]) -> Result<BindMessage, WireError> {
    let mut off = 0;
    let portal_name = read_cstring(body, &mut off)?;
    let statement_name = read_cstring(body, &mut off)?;
    let num_formats = read_i16(body, &mut off)?;
    if num_formats < 0 {
        return Err(WireError::BadLength(num_formats as i32));
    }
    let mut raw_formats = Vec::with_capacity(num_formats as usize);
    for _ in 0..num_formats {
        raw_formats.push(read_i16(body, &mut off)?);
    }
    let num_params = read_i16(body, &mut off)?;
    if num_params < 0 {
        return Err(WireError::BadLength(num_params as i32));
    }
    let n_params = num_params as usize;
    let param_format_codes = expand_param_format_codes(num_formats, &raw_formats, n_params)?;
    let mut params = Vec::with_capacity(n_params);
    let mut captured = 0usize;
    let mut params_truncated = false;
    for _ in 0..n_params {
        let len = read_i32(body, &mut off)?;
        if len < 0 {
            params.push(None);
            continue;
        }
        let len = len as usize;
        if off + len > body.len() {
            return Err(WireError::BadLength(body.len() as i32));
        }
        if captured + len > PARAM_CAPTURE_CAP {
            params_truncated = true;
            off += len;
            params.push(None);
            continue;
        }
        params.push(Some(body[off..off + len].to_vec()));
        captured += len;
        off += len;
    }
    Ok(BindMessage {
        portal_name,
        statement_name,
        param_format_codes,
        params,
        params_truncated,
    })
}

/// Parse **Query (`Q`)** body: single NUL-terminated SQL string.
pub fn parse_query(body: &[u8]) -> Result<QueryMessage, WireError> {
    let mut off = 0;
    let query = read_cstring(body, &mut off)?;
    Ok(QueryMessage { query })
}

/// Encode bind parameter values for durable replay (no portal/statement names).
///
/// Layout: `param_count u16` (LE), then for each param: `length i32` (BE, `-1` = NULL) + raw bytes.
pub fn encode_param_values(params: &[Option<Vec<u8>>], out: &mut Vec<u8>) -> Result<(), WireError> {
    let n = u16::try_from(params.len()).map_err(|_| WireError::BadLength(params.len() as i32))?;
    out.extend_from_slice(&n.to_le_bytes());
    for p in params {
        match p {
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len = i32::try_from(bytes.len())
                    .map_err(|_| WireError::BadLength(bytes.len() as i32))?;
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
    Ok(())
}

/// Decode [`encode_param_values`] payload.
pub fn decode_param_values(body: &[u8]) -> Result<Vec<Option<Vec<u8>>>, WireError> {
    if body.len() < 2 {
        return Err(WireError::BadLength(body.len() as i32));
    }
    let n = usize::from(u16::from_le_bytes([body[0], body[1]]));
    let mut off = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if off + 4 > body.len() {
            return Err(WireError::BadLength(body.len() as i32));
        }
        let len = i32::from_be_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]]);
        off += 4;
        if len < 0 {
            out.push(None);
            continue;
        }
        let len = len as usize;
        if off + len > body.len() {
            return Err(WireError::BadLength(body.len() as i32));
        }
        out.push(Some(body[off..off + len].to_vec()));
        off += len;
    }
    if off != body.len() {
        return Err(WireError::BadLength(off as i32));
    }
    Ok(out)
}

/// Parse **Execute (`E`)** portal name only (enough for correlation).
pub fn parse_execute_portal(body: &[u8]) -> Result<ExecuteMessage, WireError> {
    let mut off = 0;
    let portal_name = read_cstring(body, &mut off)?;
    Ok(ExecuteMessage { portal_name })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    #[test]
    fn parse_message_roundtrip() {
        let mut body = cstr("");
        body.extend(cstr("SELECT 1"));
        body.extend_from_slice(&0i16.to_be_bytes());
        let msg = parse_parse(&body).unwrap();
        assert_eq!(msg.statement_name, "");
        assert_eq!(msg.query, "SELECT 1");
        assert!(msg.param_oids.is_empty());
    }

    #[test]
    fn parse_message_param_oids() {
        let mut body = cstr("stmt");
        body.extend(cstr("SELECT * FROM users WHERE id = $1"));
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&23i32.to_be_bytes());
        let msg = parse_parse(&body).unwrap();
        assert_eq!(msg.statement_name, "stmt");
        assert_eq!(msg.param_oids, vec![23]);
    }

    #[test]
    fn query_simple() {
        let body = cstr("SELECT 2");
        let msg = parse_query(&body).unwrap();
        assert_eq!(msg.query, "SELECT 2");
    }

    #[test]
    fn bind_null_param() {
        let mut body = cstr("");
        body.extend(cstr("stmt"));
        body.extend_from_slice(&0i16.to_be_bytes()); // formats
        body.extend_from_slice(&1i16.to_be_bytes()); // one param
        body.extend_from_slice(&(-1i32).to_be_bytes());
        let msg = parse_bind(&body).unwrap();
        assert_eq!(msg.params, vec![None]);
        assert_eq!(msg.param_format_codes, vec![0]);
    }

    #[test]
    fn bind_single_format_applies_to_all_params() {
        let mut body = cstr("");
        body.extend(cstr("stmt"));
        body.extend_from_slice(&1i16.to_be_bytes()); // one format code for all
        body.extend_from_slice(&1i16.to_be_bytes()); // binary
        body.extend_from_slice(&2i16.to_be_bytes()); // two params
        body.extend_from_slice(&4i32.to_be_bytes());
        body.extend_from_slice(b"1234");
        body.extend_from_slice(&(-1i32).to_be_bytes());
        let msg = parse_bind(&body).unwrap();
        assert_eq!(msg.param_format_codes, vec![1, 1]);
        assert_eq!(msg.params.len(), 2);
        assert_eq!(msg.params[0].as_deref(), Some(b"1234" as &[u8]));
        assert_eq!(msg.params[1], None);
    }

    #[test]
    fn bind_per_param_formats() {
        let mut body = cstr("");
        body.extend(cstr("stmt"));
        body.extend_from_slice(&2i16.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&2i16.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        let msg = parse_bind(&body).unwrap();
        assert_eq!(msg.param_format_codes, vec![0, 1]);
    }

    #[test]
    fn param_values_roundtrip() {
        let params = vec![Some(vec![0, 2]), None, Some(b"hi".to_vec())];
        let mut enc = Vec::new();
        encode_param_values(&params, &mut enc).unwrap();
        let dec = decode_param_values(&enc).unwrap();
        assert_eq!(dec, params);
    }
}
