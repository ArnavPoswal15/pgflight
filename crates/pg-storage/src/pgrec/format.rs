//! Binary layout for `.pgrec` records and ZSTD blocks (see `docs/ARCHITECTURE.md`).

use super::{PgrecError, PgrecRecord, PgrecRecordKind};

pub const MAGIC: &[u8; 8] = b"PGREC001";
pub const HEADER_LEN: usize = 20;
/// Set when records include per-parameter format codes (v2 layout).
pub const FLAG_PARAM_FORMATS: u32 = 1;
pub const FLAGS: u32 = FLAG_PARAM_FORMATS;
/// Uncompressed bytes per ZSTD block before flush (may exceed slightly for one record).
pub const BLOCK_TARGET_BYTES: usize = 64 * 1024;

pub struct PgrecHeader {
    pub flags: u32,
    pub start_unix_us: u64,
}

impl PgrecHeader {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.start_unix_us.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Result<Self, PgrecError> {
        if buf.len() < HEADER_LEN {
            return Err(PgrecError::InvalidFormat("header too short".into()));
        }
        if &buf[..8] != MAGIC.as_slice() {
            return Err(PgrecError::InvalidMagic);
        }
        let flags = u32::from_le_bytes(
            buf[8..12]
                .try_into()
                .map_err(|_| PgrecError::InvalidFormat("flags".into()))?,
        );
        let start_unix_us = u64::from_le_bytes(
            buf[12..20]
                .try_into()
                .map_err(|_| PgrecError::InvalidFormat("start_unix_us".into()))?,
        );
        Ok(Self {
            flags,
            start_unix_us,
        })
    }
}

/// Encode one record into `out` (no block wrapper).
pub fn encode_record(record: &PgrecRecord, out: &mut Vec<u8>) -> Result<(), PgrecError> {
    out.extend_from_slice(&record.delta_us.to_le_bytes());
    out.extend_from_slice(&record.client_id.to_le_bytes());
    out.extend_from_slice(&record.txn_id.to_le_bytes());
    out.push(record.kind as u8);

    let sql = record.sql.as_bytes();
    let sql_len = u32::try_from(sql.len()).map_err(|_| PgrecError::SqlTooLarge)?;
    out.extend_from_slice(&sql_len.to_le_bytes());
    out.extend_from_slice(sql);

    let oid_count = u16::try_from(record.param_oids.len()).map_err(|_| PgrecError::TooManyOids)?;
    out.extend_from_slice(&oid_count.to_le_bytes());
    for oid in &record.param_oids {
        out.extend_from_slice(&oid.to_le_bytes());
    }

    let format_count =
        u16::try_from(record.param_format_codes.len()).map_err(|_| PgrecError::TooManyOids)?;
    out.extend_from_slice(&format_count.to_le_bytes());
    for code in &record.param_format_codes {
        out.extend_from_slice(&code.to_le_bytes());
    }

    let params_len = u32::try_from(record.params.len()).map_err(|_| PgrecError::ParamsTooLarge)?;
    out.extend_from_slice(&params_len.to_le_bytes());
    out.extend_from_slice(&record.params);
    Ok(())
}

/// Decode one record from `buf` starting at `offset`; returns record and new offset.
fn ensure(buf: &[u8], o: usize, n: usize) -> Result<(), PgrecError> {
    if o + n > buf.len() {
        Err(PgrecError::InvalidFormat("unexpected EOF in record".into()))
    } else {
        Ok(())
    }
}

fn read_u32_le(buf: &[u8], o: usize) -> Result<u32, PgrecError> {
    ensure(buf, o, 4)?;
    let arr: [u8; 4] = buf[o..o + 4]
        .try_into()
        .map_err(|_| PgrecError::InvalidFormat("u32".into()))?;
    Ok(u32::from_le_bytes(arr))
}

fn read_u64_le(buf: &[u8], o: usize) -> Result<u64, PgrecError> {
    ensure(buf, o, 8)?;
    let arr: [u8; 8] = buf[o..o + 8]
        .try_into()
        .map_err(|_| PgrecError::InvalidFormat("u64".into()))?;
    Ok(u64::from_le_bytes(arr))
}

fn read_u16_le(buf: &[u8], o: usize) -> Result<u16, PgrecError> {
    ensure(buf, o, 2)?;
    let arr: [u8; 2] = buf[o..o + 2]
        .try_into()
        .map_err(|_| PgrecError::InvalidFormat("u16".into()))?;
    Ok(u16::from_le_bytes(arr))
}

pub fn decode_record_at(
    buf: &[u8],
    offset: usize,
    param_formats: bool,
) -> Result<(PgrecRecord, usize), PgrecError> {
    let mut o = offset;

    let delta_us = read_u32_le(buf, o)?;
    o += 4;
    let client_id = read_u32_le(buf, o)?;
    o += 4;
    let txn_id = read_u64_le(buf, o)?;
    o += 8;
    ensure(buf, o, 1)?;
    let kind = PgrecRecordKind::try_from(buf[o])?;
    o += 1;

    let sql_len = read_u32_le(buf, o)? as usize;
    o += 4;
    ensure(buf, o, sql_len)?;
    let sql = std::str::from_utf8(&buf[o..o + sql_len])
        .map_err(|_| PgrecError::InvalidFormat("sql not utf-8".into()))?
        .to_owned();
    o += sql_len;

    let oid_count = usize::from(read_u16_le(buf, o)?);
    o += 2;
    ensure(buf, o, oid_count * 4)?;
    let mut param_oids = Vec::with_capacity(oid_count);
    for _ in 0..oid_count {
        param_oids.push(read_u32_le(buf, o)?);
        o += 4;
    }

    let param_format_codes = if param_formats {
        let format_count = usize::from(read_u16_le(buf, o)?);
        o += 2;
        ensure(buf, o, format_count * 2)?;
        let mut codes = Vec::with_capacity(format_count);
        for _ in 0..format_count {
            codes.push(read_u16_le(buf, o)?);
            o += 2;
        }
        codes
    } else {
        Vec::new()
    };

    let params_len = read_u32_le(buf, o)? as usize;
    o += 4;
    ensure(buf, o, params_len)?;
    let params = buf[o..o + params_len].to_vec();
    o += params_len;

    Ok((
        PgrecRecord {
            delta_us,
            client_id,
            txn_id,
            kind,
            sql,
            param_oids,
            param_format_codes,
            params,
        },
        o,
    ))
}

/// Parse all records from a decompressed ZSTD block payload.
pub fn decode_block_records(
    payload: &[u8],
    param_formats: bool,
) -> Result<Vec<PgrecRecord>, PgrecError> {
    let mut records = Vec::new();
    let mut offset = 0;
    while offset < payload.len() {
        let (rec, next) = decode_record_at(payload, offset, param_formats)?;
        offset = next;
        records.push(rec);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let h = PgrecHeader {
            flags: FLAGS,
            start_unix_us: 1_700_000_000_000_000,
        };
        let mut buf = Vec::new();
        h.encode(&mut buf);
        let decoded = PgrecHeader::decode(&buf).unwrap();
        assert_eq!(decoded.flags, h.flags);
        assert_eq!(decoded.start_unix_us, h.start_unix_us);
    }

    #[test]
    fn record_roundtrip() {
        let record = PgrecRecord {
            delta_us: 42,
            client_id: 7,
            txn_id: 99,
            kind: PgrecRecordKind::Query,
            sql: "SELECT $1".into(),
            param_oids: vec![25, 23],
            param_format_codes: vec![0, 1],
            params: vec![0, 1, 2, 3],
        };
        let mut buf = Vec::new();
        encode_record(&record, &mut buf).unwrap();
        let (decoded, end) = decode_record_at(&buf, 0, true).unwrap();
        assert_eq!(end, buf.len());
        assert_eq!(decoded.delta_us, record.delta_us);
        assert_eq!(decoded.client_id, record.client_id);
        assert_eq!(decoded.txn_id, record.txn_id);
        assert_eq!(decoded.kind, record.kind);
        assert_eq!(decoded.sql, record.sql);
        assert_eq!(decoded.param_oids, record.param_oids);
        assert_eq!(decoded.param_format_codes, record.param_format_codes);
        assert_eq!(decoded.params, record.params);
    }

    fn concat_bytes(parts: &[&[u8]]) -> Vec<u8> {
        let mut v = Vec::new();
        for p in parts {
            v.extend_from_slice(p);
        }
        v
    }

    #[test]
    fn legacy_record_without_format_codes() {
        let legacy = concat_bytes(&[
            &1u32.to_le_bytes(),
            &2u32.to_le_bytes(),
            &3u64.to_le_bytes(),
            &[PgrecRecordKind::Query as u8],
            &4u32.to_le_bytes(),
            b"SEL\x00",
            &0u16.to_le_bytes(),
            &4u32.to_le_bytes(),
            &[5, 6, 7, 8],
        ]);
        let (decoded, end) = decode_record_at(&legacy, 0, false).unwrap();
        assert_eq!(end, legacy.len());
        assert!(decoded.param_format_codes.is_empty());
        assert_eq!(decoded.params, vec![5, 6, 7, 8]);
    }
}
