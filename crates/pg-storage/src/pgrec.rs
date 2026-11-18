//! `.pgrec` ZSTD block writer and reader (Phase 5).

mod format;

use format::{
    decode_block_records, encode_record, BLOCK_TARGET_BYTES, FLAGS, FLAG_PARAM_FORMATS, HEADER_LEN,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub use format::{PgrecHeader, MAGIC as PGREC_MAGIC};

#[derive(Debug, Error)]
pub enum PgrecError {
    #[error("pgrec writer not enabled")]
    NotEnabled,
    #[error("invalid pgrec magic")]
    InvalidMagic,
    #[error("invalid pgrec format: {0}")]
    InvalidFormat(String),
    #[error("sql payload too large")]
    SqlTooLarge,
    #[error("too many param OIDs")]
    TooManyOids,
    #[error("params payload too large")]
    ParamsTooLarge,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("zstd: {0}")]
    Zstd(String),
    #[error("unknown record kind: {0}")]
    UnknownKind(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PgrecRecordKind {
    Query = 0,
    SessionState = 1,
    TxnBoundary = 2,
}

impl PgrecRecordKind {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for PgrecRecordKind {
    type Error = PgrecError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Query),
            1 => Ok(Self::SessionState),
            2 => Ok(Self::TxnBoundary),
            other => Err(PgrecError::UnknownKind(other)),
        }
    }
}

/// One decoded `.pgrec` row (query, session `SET`, or txn boundary).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgrecRecord {
    pub delta_us: u32,
    pub client_id: u32,
    pub txn_id: u64,
    pub kind: PgrecRecordKind,
    pub sql: String,
    pub param_oids: Vec<u32>,
    /// Per-parameter bind format (`0` text, `1` binary); empty for simple query / legacy files.
    pub param_format_codes: Vec<u16>,
    /// Raw bind parameter values ([`pg_wire::frontend::encode_param_values`] layout).
    pub params: Vec<u8>,
}

/// ZSTD `.pgrec` append hook — used from the recorder worker only (off the proxy hot path).
pub trait PgrecWriter: Send {
    fn append_record(&mut self, record: &PgrecRecord) -> Result<(), PgrecError>;

    fn flush(&mut self) -> Result<(), PgrecError> {
        Ok(())
    }
}

/// No-op writer when `enable_pgrec` is false.
#[derive(Debug, Default)]
pub struct NoopPgrecWriter;

impl PgrecWriter for NoopPgrecWriter {
    fn append_record(&mut self, _record: &PgrecRecord) -> Result<(), PgrecError> {
        Ok(())
    }
}

/// ZSTD block writer: `data_dir/pgflight-{flight_id}.pgrec`.
pub struct ZstdPgrecWriter {
    file: File,
    start_unix_us: u64,
    block_buf: Vec<u8>,
}

impl ZstdPgrecWriter {
    pub fn create(path: impl AsRef<Path>, start_unix_us: u64) -> Result<Self, PgrecError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let header = PgrecHeader {
            flags: FLAGS,
            start_unix_us,
        };
        let mut buf = Vec::with_capacity(HEADER_LEN);
        header.encode(&mut buf);
        file.write_all(&buf)?;
        file.sync_data()?;
        Ok(Self {
            file,
            start_unix_us,
            block_buf: Vec::new(),
        })
    }

    pub fn open_append(path: impl AsRef<Path>) -> Result<Self, PgrecError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut header_buf = [0u8; HEADER_LEN];
        file.read_exact(&mut header_buf)?;
        let header = PgrecHeader::decode(&header_buf)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            start_unix_us: header.start_unix_us,
            block_buf: Vec::new(),
        })
    }

    pub fn start_unix_us(&self) -> u64 {
        self.start_unix_us
    }

    fn flush_block(&mut self) -> Result<(), PgrecError> {
        if self.block_buf.is_empty() {
            return Ok(());
        }
        let compressed =
            zstd::encode_all(self.block_buf.as_slice(), zstd::DEFAULT_COMPRESSION_LEVEL)
                .map_err(|e| PgrecError::Zstd(e.to_string()))?;
        let len = u32::try_from(compressed.len())
            .map_err(|_| PgrecError::InvalidFormat("compressed block too large".into()))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&compressed)?;
        self.block_buf.clear();
        Ok(())
    }
}

impl PgrecWriter for ZstdPgrecWriter {
    fn append_record(&mut self, record: &PgrecRecord) -> Result<(), PgrecError> {
        encode_record(record, &mut self.block_buf)?;
        if self.block_buf.len() >= BLOCK_TARGET_BYTES {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), PgrecError> {
        self.flush_block()?;
        self.file.sync_data()?;
        Ok(())
    }
}

impl Drop for ZstdPgrecWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Iterator over all records in a `.pgrec` file (decompresses each block lazily).
pub struct PgrecReader {
    file: File,
    header: PgrecHeader,
    param_formats: bool,
    pending: std::vec::IntoIter<PgrecRecord>,
    eof: bool,
}

impl PgrecReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PgrecError> {
        let mut file = File::open(path.as_ref())?;
        let mut header_buf = [0u8; HEADER_LEN];
        file.read_exact(&mut header_buf)?;
        let header = PgrecHeader::decode(&header_buf)?;
        let param_formats = header.flags & FLAG_PARAM_FORMATS != 0;
        Ok(Self {
            file,
            header,
            param_formats,
            pending: Vec::new().into_iter(),
            eof: false,
        })
    }

    pub fn header(&self) -> &PgrecHeader {
        &self.header
    }

    pub fn start_unix_us(&self) -> u64 {
        self.header.start_unix_us
    }

    fn load_next_block(&mut self) -> Result<bool, PgrecError> {
        if self.eof {
            return Ok(false);
        }
        let mut len_buf = [0u8; 4];
        match self.file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.eof = true;
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
        let block_len = u32::from_le_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; block_len];
        self.file.read_exact(&mut compressed)?;
        let raw =
            zstd::decode_all(compressed.as_slice()).map_err(|e| PgrecError::Zstd(e.to_string()))?;
        let records = decode_block_records(&raw, self.param_formats)?;
        self.pending = records.into_iter();
        Ok(true)
    }

    /// Lazily yields every record in file order.
    pub fn iter_records(self) -> PgrecRecordIter {
        PgrecRecordIter { reader: self }
    }
}

pub struct PgrecRecordIter {
    reader: PgrecReader,
}

impl Iterator for PgrecRecordIter {
    type Item = Result<PgrecRecord, PgrecError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(rec) = self.reader.pending.next() {
                return Some(Ok(rec));
            }
            match self.reader.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Wall-clock micros since UNIX epoch (for new file headers).
pub fn unix_now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

pub fn pgrec_path_for_flight(data_dir: &Path, flight_id: u64) -> PathBuf {
    data_dir.join(format!("pgflight-{flight_id}.pgrec"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn sample_record(id: u32) -> PgrecRecord {
        PgrecRecord {
            delta_us: id * 10,
            client_id: 1,
            txn_id: id as u64,
            kind: PgrecRecordKind::Query,
            sql: format!("SELECT {id}"),
            param_oids: vec![23],
            param_format_codes: vec![1],
            params: vec![id as u8],
        }
    }

    #[test]
    fn zstd_block_roundtrip() {
        let dir = std::env::temp_dir().join("pgflight-pgrec-test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("flight-1.pgrec");
        let start = 9_000_000_000_000u64;

        {
            let mut w = ZstdPgrecWriter::create(&path, start).unwrap();
            for i in 0..50 {
                w.append_record(&sample_record(i)).unwrap();
            }
            w.flush().unwrap();
        }

        let reader = PgrecReader::open(&path).unwrap();
        assert_eq!(reader.start_unix_us(), start);
        let records: Vec<_> = reader
            .iter_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(records.len(), 50);
        assert_eq!(records[0].sql, "SELECT 0");
        assert_eq!(records[49].sql, "SELECT 49");
    }

    #[test]
    fn session_and_txn_kinds_roundtrip() {
        let dir = std::env::temp_dir().join("pgflight-pgrec-kinds");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("k.pgrec");

        let rows = [
            PgrecRecord {
                delta_us: 1,
                client_id: 2,
                txn_id: 0,
                kind: PgrecRecordKind::SessionState,
                sql: "SET search_path = public".into(),
                param_oids: vec![],
                param_format_codes: vec![],
                params: vec![],
            },
            PgrecRecord {
                delta_us: 2,
                client_id: 2,
                txn_id: 1,
                kind: PgrecRecordKind::TxnBoundary,
                sql: "BEGIN".into(),
                param_oids: vec![],
                param_format_codes: vec![],
                params: vec![],
            },
        ];

        {
            let mut w = ZstdPgrecWriter::create(&path, 1).unwrap();
            for r in &rows {
                w.append_record(r).unwrap();
            }
            w.flush().unwrap();
        }

        let out: Vec<_> = PgrecReader::open(&path)
            .unwrap()
            .iter_records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(out, rows);
    }
}
