//! Backend message helpers (ErrorResponse SQLSTATE).

/// Extract SQLSTATE (`C` field) from an **ErrorResponse** body.
pub fn parse_error_sqlstate(body: &[u8]) -> Option<String> {
    let mut off = 0;
    while off < body.len() {
        let key = body.get(off).copied()?;
        off += 1;
        if key == 0 {
            break;
        }
        let val_start = off;
        while off < body.len() && body[off] != 0 {
            off += 1;
        }
        if off >= body.len() {
            break;
        }
        let val = std::str::from_utf8(&body[val_start..off]).ok()?;
        off += 1;
        if key == b'C' {
            return Some(val.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_field() {
        let mut body = vec![b'S'];
        body.extend(b"ERROR\0");
        body.push(b'C');
        body.extend(b"23505\0");
        body.push(0);
        assert_eq!(parse_error_sqlstate(&body).as_deref(), Some("23505"));
    }

    #[test]
    fn empty_body() {
        assert!(parse_error_sqlstate(&[]).is_none());
    }
}
