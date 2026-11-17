//! Frontend message parsers (Parse / Bind / Query / Execute portal name).

use crate::WireError;

/// Max bytes captured for bind parameter payloads (per FEATURES_AND_LOGIC).
pub const PARAM_CAPTURE_CAP: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseMessage {
    pub statement_name: String,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindMessage {
    pub portal_name: String,
    pub statement_name: String,
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

/// Parse **Parse (`P`)** body: statement name + query (NUL-terminated) + param OIDs.
pub fn parse_parse(body: &[u8]) -> Result<ParseMessage, WireError> {
    let mut off = 0;
    let statement_name = read_cstring(body, &mut off)?;
    let query = read_cstring(body, &mut off)?;
    let _num_params = read_i16(body, &mut off)?;
    Ok(ParseMessage {
        statement_name,
        query,
    })
}

/// Parse **Bind (`B`)** body.
pub fn parse_bind(body: &[u8]) -> Result<BindMessage, WireError> {
    let mut off = 0;
    let portal_name = read_cstring(body, &mut off)?;
    let statement_name = read_cstring(body, &mut off)?;
    let num_formats = read_i16(body, &mut off)?;
    for _ in 0..num_formats {
        let _ = read_i16(body, &mut off)?;
    }
    let num_params = read_i16(body, &mut off)?;
    let mut params = Vec::with_capacity(num_params.max(0) as usize);
    let mut captured = 0usize;
    let mut params_truncated = false;
    for _ in 0..num_params {
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
    }
}
