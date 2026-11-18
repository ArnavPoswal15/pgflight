//! Replay `.pgrec` query records via extended or simple query protocol.

use crate::explain::{ExplainError, ExplainOutput, PgFailure, ShadowClient};
use crate::guard::assert_select_only;
use pg_storage::{PgrecError, PgrecReader, PgrecRecord, PgrecRecordKind};
use pg_wire::frontend::{decode_param_values, parse_bind, BindMessage};
use std::path::Path;
use thiserror::Error;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("pgrec: {0}")]
    Pgrec(#[from] PgrecError),
    #[error("bind decode: {0}")]
    Bind(String),
    #[error("guard: {0}")]
    Guard(#[from] crate::guard::GuardError),
    #[error("postgres: {0}")]
    Postgres(PgFailure),
    #[error("unsupported record: {0}")]
    Unsupported(String),
    #[error("explain: {0}")]
    Explain(#[from] ExplainError),
}

impl From<tokio_postgres::Error> for ReplayError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::Postgres(PgFailure::from(value))
    }
}

impl From<PgFailure> for ReplayError {
    fn from(value: PgFailure) -> Self {
        Self::Postgres(value)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReplayStats {
    pub queries: u64,
    pub session_sets: u64,
    pub txn_markers: u64,
    pub skipped: u64,
    pub rows_returned: u64,
}

#[derive(Debug, Clone)]
pub struct Replayer {
    shadow: ShadowClient,
}

impl Replayer {
    pub fn new(shadow: ShadowClient) -> Self {
        Self { shadow }
    }

    pub fn shadow(&self) -> &ShadowClient {
        &self.shadow
    }

    /// Replay every record in a `.pgrec` file against the shadow connection string.
    pub async fn run_pgrec_path(&self, path: impl AsRef<Path>) -> Result<ReplayStats, ReplayError> {
        let reader = PgrecReader::open(path)?;
        let mut stats = ReplayStats::default();
        let (client, connection) =
            tokio_postgres::connect(self.shadow.connection_string(), NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        for record in reader.iter_records() {
            self.replay_one(&client, &record?, &mut stats).await?;
        }
        Ok(stats)
    }

    /// Replay an in-memory slice of records (tests and daemon jobs).
    pub async fn run_pgrec_records(
        &self,
        records: &[PgrecRecord],
    ) -> Result<ReplayStats, ReplayError> {
        let mut stats = ReplayStats::default();
        let (client, connection) =
            tokio_postgres::connect(self.shadow.connection_string(), NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        for record in records {
            self.replay_one(&client, record, &mut stats).await?;
        }
        Ok(stats)
    }

    /// Run EXPLAIN on a single query record after replay guard checks.
    pub async fn explain_record(&self, record: &PgrecRecord) -> Result<ExplainOutput, ReplayError> {
        if record.kind != PgrecRecordKind::Query {
            return Err(ReplayError::Unsupported("not a query record".into()));
        }
        let params = typed_params(record)?;
        if params.is_empty() {
            return self
                .shadow
                .explain_select(&record.sql, &[])
                .await
                .map_err(ReplayError::from);
        }
        let types = param_types(record, params.len())?;
        let refs = param_refs(&params);
        let pairs: Vec<_> = refs.into_iter().zip(types).collect();
        self.shadow
            .explain_select_typed(&record.sql, &pairs)
            .await
            .map_err(ReplayError::from)
    }

    async fn replay_one(
        &self,
        client: &Client,
        record: &PgrecRecord,
        stats: &mut ReplayStats,
    ) -> Result<(), ReplayError> {
        match record.kind {
            PgrecRecordKind::Query => {
                if assert_select_only(&record.sql).is_err() {
                    return Ok(());
                }
                let rows = exec_record(client, record).await?;
                stats.queries += 1;
                stats.rows_returned += rows;
            }
            PgrecRecordKind::SessionState => {
                client.batch_execute(&record.sql).await?;
                stats.session_sets += 1;
            }
            PgrecRecordKind::TxnBoundary => {
                client.batch_execute(&record.sql).await?;
                stats.txn_markers += 1;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum SqlParam {
    Null,
    Text(String),
    Binary(Vec<u8>),
}

pub async fn query_pgrec_record(client: &Client, record: &PgrecRecord) -> Result<(), ReplayError> {
    exec_record(client, record).await?;
    Ok(())
}

async fn exec_record(client: &Client, record: &PgrecRecord) -> Result<u64, ReplayError> {
    assert_select_only(&record.sql)?;
    if record.params.is_empty() && record.param_oids.is_empty() {
        let rows = client.query(&record.sql, &[]).await?;
        return Ok(rows.len() as u64);
    }
    let params = decode_pgrec_params(record)?;
    let types = param_types(record, params.len())?;
    let refs = param_refs(&params);
    let pairs: Vec<_> = refs.into_iter().zip(types).collect();
    let rows = client.query_typed(&record.sql, &pairs).await?;
    Ok(rows.len() as u64)
}

fn param_types(record: &PgrecRecord, n: usize) -> Result<Vec<Type>, ReplayError> {
    if record.param_oids.is_empty() {
        return Ok(vec![Type::UNKNOWN; n]);
    }
    if record.param_oids.len() != n {
        return Err(ReplayError::Bind(format!(
            "oid count {} != param count {n}",
            record.param_oids.len()
        )));
    }
    Ok(record
        .param_oids
        .iter()
        .map(|oid| Type::from_oid(*oid).unwrap_or(Type::UNKNOWN))
        .collect())
}

pub fn decode_pgrec_params(record: &PgrecRecord) -> Result<Vec<SqlParam>, ReplayError> {
    typed_params(record)
}

fn typed_params(record: &PgrecRecord) -> Result<Vec<SqlParam>, ReplayError> {
    if record.params.is_empty() && record.param_format_codes.is_empty() {
        return Ok(Vec::new());
    }
    if !record.param_format_codes.is_empty() || looks_like_param_values(&record.params) {
        return wire_params_to_sql(record);
    }
    if let Ok(bind) = parse_bind(&record.params) {
        return bind_to_params(&bind, record);
    }
    params_from_json(&record.params)
}

fn looks_like_param_values(params: &[u8]) -> bool {
    params.len() >= 2 && !params.first().is_some_and(|b| *b == b'[')
}

fn wire_params_to_sql(record: &PgrecRecord) -> Result<Vec<SqlParam>, ReplayError> {
    let values =
        decode_param_values(&record.params).map_err(|e| ReplayError::Bind(e.to_string()))?;
    if !record.param_format_codes.is_empty() && record.param_format_codes.len() != values.len() {
        return Err(ReplayError::Bind(format!(
            "format code count {} != param count {}",
            record.param_format_codes.len(),
            values.len()
        )));
    }
    let mut out = Vec::with_capacity(values.len());
    for (i, val) in values.into_iter().enumerate() {
        let format = record.param_format_codes.get(i).copied().unwrap_or(0);
        match val {
            None => out.push(SqlParam::Null),
            Some(bytes) if format == 0 => {
                let s = std::str::from_utf8(&bytes)
                    .map_err(|_| ReplayError::Bind("text param not utf-8".into()))?;
                out.push(SqlParam::Text(s.to_string()));
            }
            Some(bytes) => out.push(SqlParam::Binary(bytes)),
        }
    }
    Ok(out)
}

fn params_from_json(bytes: &[u8]) -> Result<Vec<SqlParam>, ReplayError> {
    let arr: Vec<serde_json::Value> =
        serde_json::from_slice(bytes).map_err(|e| ReplayError::Bind(e.to_string()))?;
    Ok(arr
        .into_iter()
        .map(|v| match v {
            serde_json::Value::Null => SqlParam::Null,
            serde_json::Value::String(s) => SqlParam::Text(s),
            serde_json::Value::Number(n) => SqlParam::Text(n.to_string()),
            serde_json::Value::Bool(b) => SqlParam::Text(b.to_string()),
            other => SqlParam::Text(other.to_string()),
        })
        .collect())
}

fn bind_to_params(bind: &BindMessage, record: &PgrecRecord) -> Result<Vec<SqlParam>, ReplayError> {
    let n = bind.params.len();
    if !record.param_oids.is_empty() && record.param_oids.len() != n {
        return Err(ReplayError::Bind(format!(
            "oid count {} != bind param count {n}",
            record.param_oids.len()
        )));
    }
    let mut out = Vec::with_capacity(n);
    for (i, val) in bind.params.iter().enumerate() {
        let format = bind.param_format_codes.get(i).copied().unwrap_or(0);
        match val {
            None => out.push(SqlParam::Null),
            Some(bytes) if format == 0 => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| ReplayError::Bind("text param not utf-8".into()))?;
                out.push(SqlParam::Text(s.to_string()));
            }
            Some(bytes) => out.push(SqlParam::Binary(bytes.clone())),
        }
    }
    Ok(out)
}

fn param_refs(params: &[SqlParam]) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
    params
        .iter()
        .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect()
}

impl tokio_postgres::types::ToSql for SqlParam {
    #[allow(unused_imports)]
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use tokio_postgres::types::{IsNull, ToSql};
        match self {
            SqlParam::Null => Ok(IsNull::Yes),
            SqlParam::Text(s) => encode_text_param(ty, s, out),
            SqlParam::Binary(b) => {
                out.extend_from_slice(b);
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Bind captured text using the type Postgres assigned (extended protocol is binary).
fn encode_text_param(
    ty: &Type,
    s: &str,
    out: &mut bytes::BytesMut,
) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
    use tokio_postgres::types::ToSql;
    match *ty {
        Type::INT2 => (decode_int(s, 2)? as i16).to_sql(ty, out),
        Type::INT4 => (decode_int(s, 4)? as i32).to_sql(ty, out),
        Type::INT8 => decode_int(s, 8)?.to_sql(ty, out),
        Type::FLOAT4 => s.parse::<f32>()?.to_sql(ty, out),
        Type::FLOAT8 => s.parse::<f64>()?.to_sql(ty, out),
        Type::BOOL => {
            let b = matches!(s, "t" | "true" | "TRUE" | "1");
            b.to_sql(ty, out)
        }
        _ => s.to_sql(ty, out),
    }
}

fn decode_int(s: &str, _width: usize) -> Result<i64, Box<dyn std::error::Error + Sync + Send>> {
    Ok(s.parse()?)
}

/// Map Parse OIDs to Postgres types for typed extended replay.
pub fn types_from_oids(oids: &[u32]) -> Vec<Type> {
    oids.iter().filter_map(|oid| Type::from_oid(*oid)).collect()
}

#[cfg(test)]
mod param_tests {
    use super::*;
    use tokio_postgres::types::{IsNull, ToSql, Type};

    #[test]
    fn binary_int2_param_encodes() {
        let param = SqlParam::Binary(2i16.to_be_bytes().to_vec());
        let mut out = bytes::BytesMut::new();
        let null = param.to_sql(&Type::INT2, &mut out).unwrap();
        assert!(matches!(null, IsNull::No));
        assert_eq!(&out[..], &2i16.to_be_bytes());
    }

    #[test]
    fn decimal_int4_encodes() {
        let param = SqlParam::Text("42".into());
        let mut out = bytes::BytesMut::new();
        param.to_sql(&Type::INT4, &mut out).unwrap();
        assert_eq!(&out[..], &42i32.to_be_bytes());
    }
}
