//! Shadow EXPLAIN runner (READ ONLY transaction, statement timeout).

use crate::guard::{assert_select_only, GuardError};
use crate::heuristics::{PlanHint, PlanHints};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};

pub const DEFAULT_SHADOW_URL: &str =
    "postgresql://pgflight_shadow:pgflight_shadow@127.0.0.1:5434/shop";
pub const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 3000;

/// Postgres failure with the server message included (`tokio_postgres::Error` prints only "db error").
#[derive(Debug)]
pub struct PgFailure(pub tokio_postgres::Error);

impl std::fmt::Display for PgFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(db) = self.0.as_db_error() {
            write!(f, "{}: {}", db.code().code(), db.message())
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl std::error::Error for PgFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<tokio_postgres::Error> for PgFailure {
    fn from(value: tokio_postgres::Error) -> Self {
        Self(value)
    }
}

impl From<tokio_postgres::Error> for ExplainError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::Connect(PgFailure::from(value))
    }
}

#[derive(Debug, Error)]
pub enum ExplainError {
    #[error("guard rejected query: {0}")]
    Guard(#[from] GuardError),
    #[error("postgres: {0}")]
    Connect(#[from] PgFailure),
    #[error("shadow client not connected")]
    NotConnected,
    #[error("explain returned no rows")]
    EmptyResult,
    #[error("invalid explain json: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct ExplainOutput {
    pub plan_json: Value,
    pub hints: PlanHints,
}

#[derive(Debug, Clone)]
pub struct ShadowClient {
    conn_str: String,
    statement_timeout_ms: u64,
    client: Arc<Mutex<Option<Client>>>,
}

impl Default for ShadowClient {
    fn default() -> Self {
        Self::new(DEFAULT_SHADOW_URL)
    }
}

impl ShadowClient {
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self {
            conn_str: conn_str.into(),
            statement_timeout_ms: DEFAULT_STATEMENT_TIMEOUT_MS,
            client: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_statement_timeout_ms(mut self, ms: u64) -> Self {
        self.statement_timeout_ms = ms;
        self
    }

    pub fn connection_string(&self) -> &str {
        &self.conn_str
    }

    /// Connect (or reuse) a single client for the shadow role.
    pub async fn connect(&self) -> Result<(), ExplainError> {
        let mut slot = self.client.lock().await;
        if slot.is_some() {
            return Ok(());
        }
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("shadow postgres connection closed: {e}");
            }
        });
        *slot = Some(client);
        Ok(())
    }

    /// Run `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` inside `BEGIN READ ONLY` / `ROLLBACK`.
    pub async fn explain_select(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<ExplainOutput, ExplainError> {
        assert_select_only(sql)?;
        self.connect().await?;
        let mut guard = self.client.lock().await;
        let client = guard.as_mut().ok_or(ExplainError::NotConnected)?;
        run_explain(client, sql, params, None, self.statement_timeout_ms).await
    }

    /// Same as [`Self::explain_select`], with explicit parameter types (replay OIDs).
    pub async fn explain_select_typed(
        &self,
        sql: &str,
        params: &[(
            &(dyn tokio_postgres::types::ToSql + Sync),
            tokio_postgres::types::Type,
        )],
    ) -> Result<ExplainOutput, ExplainError> {
        assert_select_only(sql)?;
        self.connect().await?;
        let mut guard = self.client.lock().await;
        let client = guard.as_mut().ok_or(ExplainError::NotConnected)?;
        let values: Vec<_> = params.iter().map(|(v, _)| *v).collect();
        let types: Vec<_> = params.iter().map(|(_, t)| t.clone()).collect();
        run_explain(
            client,
            sql,
            &values,
            Some(&types),
            self.statement_timeout_ms,
        )
        .await
    }
}

async fn run_explain(
    client: &mut Client,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    types: Option<&[tokio_postgres::types::Type]>,
    timeout_ms: u64,
) -> Result<ExplainOutput, ExplainError> {
    let txn = client.build_transaction().read_only(true).start().await?;
    let timeout_sql = format!("SET LOCAL statement_timeout = '{timeout_ms}ms'");
    txn.batch_execute(&timeout_sql).await?;
    let explain_sql = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}");
    let row = if let Some(types) = types {
        let pairs: Vec<_> = params
            .iter()
            .zip(types.iter())
            .map(|(value, ty)| (*value, ty.clone()))
            .collect::<Vec<_>>();
        txn.query_typed(&explain_sql, &pairs)
            .await?
            .into_iter()
            .next()
            .ok_or(ExplainError::EmptyResult)?
    } else {
        txn.query_one(&explain_sql, params).await?
    };
    let plan_json = parse_explain_row(&row)?;
    txn.rollback().await?;
    let hints = PlanHints::from_json(&plan_json);
    Ok(ExplainOutput { plan_json, hints })
}

fn parse_explain_row(row: &Row) -> Result<Value, ExplainError> {
    let value: Value = row.try_get(0)?;
    Ok(value)
}

impl ExplainOutput {
    pub fn hints_slice(&self) -> &[PlanHint] {
        &self.hints.hints
    }
}
