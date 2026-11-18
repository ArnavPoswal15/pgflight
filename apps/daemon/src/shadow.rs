//! Shadow EXPLAIN / replay worker (off proxy hot path).

use hdrhistogram::Histogram;
use pg_analyzer::FingerprintCache;
use pg_shadow::{
    compare_plans, ExplainError, ExplainOutput, PlanDiffNode as ShadowDiffNode, Replayer,
    ShadowClient,
};
use pg_storage::{
    open_db, PgrecReader, PgrecRecord, PgrecRecord as StoragePgrecRecord, PgrecRecordKind,
};
use pgflight_control::protocol::{
    ExplainResultBody, FingerprintPlan, FingerprintReplayStats, PlanCompareBody, PlanCompareEntry,
    PlanDiffNode, PlanHint, ReplayResultBody, ReplayResultId,
};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_postgres::types::ToSql;

const JOB_CHANNEL_CAP: usize = 64;
const MAX_CONCURRENT: usize = 2;

#[derive(Debug, Clone)]
pub struct ShadowWorkerConfig {
    pub shadow_url: String,
    pub disabled: bool,
    pub explain_timeout_ms: u64,
    pub db_path: PathBuf,
    pub flight_id: u64,
}

#[derive(Debug)]
pub struct ShadowStatusState {
    pub enabled: bool,
    pub url: String,
    pub connected: AtomicU64,
    pub queued: AtomicU64,
    pub dropped_jobs: AtomicU64,
}

impl Default for ShadowStatusState {
    fn default() -> Self {
        Self {
            enabled: true,
            url: String::new(),
            connected: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            dropped_jobs: AtomicU64::new(0),
        }
    }
}

impl ShadowStatusState {
    pub fn snapshot(&self) -> pgflight_control::protocol::ShadowStatus {
        pgflight_control::protocol::ShadowStatus {
            enabled: self.enabled,
            url: redact_url(&self.url),
            connected: self.connected.load(Ordering::Relaxed) > 0,
            queued_jobs: self.queued.load(Ordering::Relaxed) as u32,
            dropped_jobs: self.dropped_jobs.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct ShadowHandle {
    tx: mpsc::Sender<ShadowJob>,
    status: Arc<ShadowStatusState>,
    disabled: bool,
}

enum ShadowJob {
    Explain {
        fingerprint_id: Option<i64>,
        flight_id: Option<u64>,
        event_id: Option<u64>,
        reply: oneshot::Sender<Result<ExplainResultBody, String>>,
    },
    Replay {
        pgrec_path: PathBuf,
        label: String,
        warmup_iters: u32,
        explain: bool,
        reply: oneshot::Sender<Result<ReplayResultBody, String>>,
    },
    Compare {
        baseline: Option<ReplayResultId>,
        variant: Option<ReplayResultId>,
        baseline_plans: Option<Vec<FingerprintPlan>>,
        variant_plans: Option<Vec<FingerprintPlan>>,
        reply: oneshot::Sender<Result<PlanCompareBody, String>>,
    },
}

#[derive(Clone)]
#[allow(dead_code)]
struct StoredReplay {
    label: String,
    per_fingerprint: Vec<FingerprintReplayStats>,
    plans: Vec<FingerprintPlan>,
}

struct WorkerInner {
    client: ShadowClient,
    replayer: Replayer,
    db_path: PathBuf,
    flight_id: u64,
    semaphore: Arc<Semaphore>,
    replays: parking_lot::Mutex<HashMap<String, StoredReplay>>,
    fingerprint_cache: parking_lot::Mutex<FingerprintCache>,
    next_replay_id: AtomicU64,
}

pub fn spawn_shadow_worker(cfg: ShadowWorkerConfig) -> ShadowHandle {
    let status = Arc::new(ShadowStatusState {
        enabled: !cfg.disabled,
        url: cfg.shadow_url.clone(),
        connected: AtomicU64::new(0),
        queued: AtomicU64::new(0),
        dropped_jobs: AtomicU64::new(0),
    });
    let (tx, mut rx) = mpsc::channel(JOB_CHANNEL_CAP);
    let handle = ShadowHandle {
        tx: tx.clone(),
        status: status.clone(),
        disabled: cfg.disabled,
    };

    if cfg.disabled {
        return handle;
    }

    let client =
        ShadowClient::new(cfg.shadow_url.clone()).with_statement_timeout_ms(cfg.explain_timeout_ms);
    let replayer = Replayer::new(client.clone());
    let inner = Arc::new(WorkerInner {
        client,
        replayer,
        db_path: cfg.db_path,
        flight_id: cfg.flight_id,
        semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT)),
        replays: parking_lot::Mutex::new(HashMap::new()),
        fingerprint_cache: parking_lot::Mutex::new(FingerprintCache::default()),
        next_replay_id: AtomicU64::new(1),
    });

    let inner_bg = inner.clone();
    let status_bg = status.clone();
    tokio::spawn(async move {
        if inner_bg.client.connect().await.is_ok() {
            status_bg.connected.store(1, Ordering::Relaxed);
        }
        while let Some(job) = rx.recv().await {
            status_bg.queued.fetch_sub(1, Ordering::Relaxed);
            let permit = match inner_bg.semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => continue,
            };
            let inner_job = inner_bg.clone();
            tokio::spawn(async move {
                let _permit = permit;
                match job {
                    ShadowJob::Explain {
                        fingerprint_id,
                        flight_id,
                        event_id,
                        reply,
                    } => {
                        let result = inner_job
                            .run_explain(fingerprint_id, flight_id, event_id)
                            .await;
                        let _ = reply.send(result);
                    }
                    ShadowJob::Replay {
                        pgrec_path,
                        label,
                        warmup_iters,
                        explain,
                        reply,
                    } => {
                        let result = inner_job
                            .run_replay(&pgrec_path, &label, warmup_iters, explain)
                            .await;
                        let _ = reply.send(result);
                    }
                    ShadowJob::Compare {
                        baseline,
                        variant,
                        baseline_plans,
                        variant_plans,
                        reply,
                    } => {
                        let result = inner_job.compare_replay(
                            baseline,
                            variant,
                            baseline_plans,
                            variant_plans,
                        );
                        let _ = reply.send(result);
                    }
                }
            });
        }
    });

    handle
}

impl ShadowHandle {
    pub fn status(&self) -> pgflight_control::protocol::ShadowStatus {
        self.status.snapshot()
    }

    pub async fn explain(
        &self,
        fingerprint_id: Option<i64>,
        flight_id: Option<u64>,
        event_id: Option<u64>,
    ) -> Result<ExplainResultBody, String> {
        if self.disabled {
            return Err("shadow worker disabled (--shadow-disabled)".into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = ShadowJob::Explain {
            fingerprint_id,
            flight_id,
            event_id,
            reply: reply_tx,
        };
        self.enqueue(job)?;
        reply_rx
            .await
            .map_err(|_| String::from("shadow worker dropped explain job"))?
    }

    pub async fn replay_run(
        &self,
        pgrec_path: impl Into<PathBuf>,
        label: String,
        warmup_iters: u32,
        explain: bool,
    ) -> Result<ReplayResultBody, String> {
        if self.disabled {
            return Err("shadow worker disabled (--shadow-disabled)".into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = ShadowJob::Replay {
            pgrec_path: pgrec_path.into(),
            label,
            warmup_iters,
            explain,
            reply: reply_tx,
        };
        self.enqueue(job)?;
        reply_rx
            .await
            .map_err(|_| String::from("shadow worker dropped replay job"))?
    }

    pub async fn compare_replay(
        &self,
        baseline: Option<ReplayResultId>,
        variant: Option<ReplayResultId>,
        baseline_plans: Option<Vec<FingerprintPlan>>,
        variant_plans: Option<Vec<FingerprintPlan>>,
    ) -> Result<PlanCompareBody, String> {
        if self.disabled {
            return Err("shadow worker disabled (--shadow-disabled)".into());
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = ShadowJob::Compare {
            baseline,
            variant,
            baseline_plans,
            variant_plans,
            reply: reply_tx,
        };
        self.enqueue(job)?;
        reply_rx
            .await
            .map_err(|_| String::from("shadow worker dropped compare job"))?
    }

    fn enqueue(&self, job: ShadowJob) -> Result<(), String> {
        match self.tx.try_send(job) {
            Ok(()) => {
                self.status.queued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.status.dropped_jobs.fetch_add(1, Ordering::Relaxed);
                Err("shadow job queue full (dropped)".into())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err("shadow worker stopped".into()),
        }
    }
}

impl WorkerInner {
    async fn run_explain(
        &self,
        fingerprint_id: Option<i64>,
        flight_id: Option<u64>,
        event_id: Option<u64>,
    ) -> Result<ExplainResultBody, String> {
        let resolved = resolve_sql(
            &self.db_path,
            self.flight_id,
            fingerprint_id,
            flight_id,
            event_id,
        )?;
        let output = explain_query(&self.client, &self.replayer, &resolved).await?;
        Ok(explain_body(output))
    }

    async fn run_replay(
        &self,
        path: &Path,
        label: &str,
        warmup_iters: u32,
        explain: bool,
    ) -> Result<ReplayResultBody, String> {
        for _ in 0..warmup_iters {
            self.replayer
                .run_pgrec_path(path)
                .await
                .map_err(|e| e.to_string())?;
        }

        let reader = PgrecReader::open(path).map_err(|e| e.to_string())?;
        let mut timings: HashMap<i64, Vec<u64>> = HashMap::new();
        let mut plans = Vec::new();
        let mut seen_explain = std::collections::HashSet::new();

        let (client, connection) =
            tokio_postgres::connect(self.client.connection_string(), tokio_postgres::NoTls)
                .await
                .map_err(|e| e.to_string())?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        for record in reader.iter_records() {
            let record = record.map_err(|e| e.to_string())?;
            if record.kind != PgrecRecordKind::Query {
                continue;
            }
            if pg_shadow::assert_select_only(&record.sql).is_err() {
                continue;
            }
            let fp_id = self.fingerprint_id(&record.sql)?;
            let start = Instant::now();
            replay_query_record(&client, &record)
                .await
                .map_err(|e| e.to_string())?;
            let us = start.elapsed().as_micros() as u64;
            timings.entry(fp_id).or_default().push(us);

            if explain && seen_explain.insert(fp_id) {
                let output = self
                    .replayer
                    .explain_record(&record)
                    .await
                    .map_err(|e| e.to_string())?;
                plans.push(FingerprintPlan {
                    fingerprint_id: fp_id,
                    plan_json: output.plan_json,
                });
            }
        }

        let per_fingerprint: Vec<FingerprintReplayStats> = timings
            .into_iter()
            .map(|(fingerprint_id, samples)| stats_from_samples(fingerprint_id, samples))
            .collect::<Result<Vec<_>, _>>()?;

        let replay_id = format!(
            "{}-{}",
            sanitize_label(label),
            self.next_replay_id.fetch_add(1, Ordering::Relaxed)
        );
        let body = ReplayResultBody {
            replay: ReplayResultId {
                replay_id: replay_id.clone(),
                label: label.to_string(),
            },
            per_fingerprint: per_fingerprint.clone(),
            plans: plans.clone(),
        };
        self.replays.lock().insert(
            replay_id,
            StoredReplay {
                label: label.to_string(),
                per_fingerprint,
                plans,
            },
        );
        Ok(body)
    }

    fn compare_replay(
        &self,
        baseline: Option<ReplayResultId>,
        variant: Option<ReplayResultId>,
        baseline_plans: Option<Vec<FingerprintPlan>>,
        variant_plans: Option<Vec<FingerprintPlan>>,
    ) -> Result<PlanCompareBody, String> {
        let (base_plans, var_plans) = if let (Some(b), Some(v)) =
            (baseline.as_ref(), variant.as_ref())
        {
            let store = self.replays.lock();
            let bentry = store
                .get(&b.replay_id)
                .ok_or_else(|| format!("unknown baseline replay_id {}", b.replay_id))?;
            let ventry = store
                .get(&v.replay_id)
                .ok_or_else(|| format!("unknown variant replay_id {}", v.replay_id))?;
            (bentry.plans.clone(), ventry.plans.clone())
        } else if let (Some(bp), Some(vp)) = (baseline_plans, variant_plans) {
            (bp, vp)
        } else {
            return Err(
                "compare requires baseline+variant replay IDs or inline baseline_plans+variant_plans"
                    .into(),
            );
        };

        let mut base_map: HashMap<i64, &Value> = base_plans
            .iter()
            .map(|p| (p.fingerprint_id, &p.plan_json))
            .collect();
        let mut improved = Vec::new();
        let mut regressed = Vec::new();
        let mut diff_nodes = Vec::new();

        for vp in &var_plans {
            if let Some(bplan) = base_map.remove(&vp.fingerprint_id) {
                let cmp = compare_plans(bplan, &vp.plan_json);
                for s in cmp.improved {
                    improved.push(PlanCompareEntry {
                        fingerprint_id: vp.fingerprint_id,
                        summary: s,
                    });
                }
                for s in cmp.regressed {
                    regressed.push(PlanCompareEntry {
                        fingerprint_id: vp.fingerprint_id,
                        summary: s,
                    });
                }
                for node in cmp.diff_nodes {
                    diff_nodes.push(map_diff_node(vp.fingerprint_id, node));
                }
            }
        }

        Ok(PlanCompareBody {
            improved,
            regressed,
            diff_nodes,
        })
    }

    fn fingerprint_id(&self, sql: &str) -> Result<i64, String> {
        let mut cache = self.fingerprint_cache.lock();
        cache
            .fingerprint(sql)
            .map(|fp| fp.fingerprint_id)
            .map_err(|e| e.to_string())
    }
}

fn map_diff_node(fingerprint_id: i64, node: ShadowDiffNode) -> PlanDiffNode {
    let _ = fingerprint_id;
    PlanDiffNode {
        path: node.path,
        baseline: node.baseline,
        variant: node.variant,
    }
}

fn sanitize_label(label: &str) -> String {
    label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .take(32)
        .collect()
}

fn stats_from_samples(
    fingerprint_id: i64,
    samples: Vec<u64>,
) -> Result<FingerprintReplayStats, String> {
    let mut hist = Histogram::<u64>::new(3)
        .or_else(|_| Histogram::<u64>::new(1))
        .map_err(|e| format!("histogram: {e}"))?;
    for s in &samples {
        let _ = hist.record(*s);
    }
    Ok(FingerprintReplayStats {
        fingerprint_id,
        exec_us_p50: hist.value_at_quantile(0.5),
        exec_us_p95: hist.value_at_quantile(0.95),
        exec_us_p99: hist.value_at_quantile(0.99),
        samples: samples.len() as u64,
    })
}

fn explain_body(output: ExplainOutput) -> ExplainResultBody {
    ExplainResultBody {
        plan_json: output.plan_json,
        hints: output
            .hints
            .hints
            .into_iter()
            .map(|h| PlanHint {
                code: h.code,
                message: h.message,
                node_path: h.node_path,
            })
            .collect(),
    }
}

#[derive(Debug)]
struct ResolvedQuery {
    sql: String,
    params_json: Option<String>,
    wire_params: Vec<u8>,
    param_oids: Vec<u32>,
}

fn resolve_sql(
    db_path: &Path,
    daemon_flight_id: u64,
    fingerprint_id: Option<i64>,
    flight_id: Option<u64>,
    event_id: Option<u64>,
) -> Result<ResolvedQuery, String> {
    if fingerprint_id.is_none() && event_id.is_none() {
        return Err("explain requires fingerprint_id or event_id".into());
    }
    let conn = open_db(db_path).map_err(|e| e.to_string())?;
    if let Some(eid) = event_id {
        let fid = flight_id.unwrap_or(daemon_flight_id);
        let row: (Option<String>, Option<String>, String) = conn
            .query_row(
                "SELECT e.raw_sql, e.params_json, f.canonical_sql
                 FROM query_events e
                 JOIN fingerprints f ON f.fingerprint_id = e.fingerprint_id
                 WHERE e.id = ?1 AND e.flight_id = ?2",
                (eid as i64, fid as i64),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|e| format!("event not found: {e}"))?;
        return Ok(ResolvedQuery {
            sql: row.0.filter(|s| !s.trim().is_empty()).unwrap_or(row.2),
            params_json: row.1,
            wire_params: Vec::new(),
            param_oids: Vec::new(),
        });
    }
    let fp = fingerprint_id.ok_or("explain requires fingerprint_id or event_id")?;
    let row: (Option<String>, Option<String>, String) = conn
        .query_row(
            "SELECT e.raw_sql, e.params_json, f.canonical_sql
             FROM query_events e
             JOIN fingerprints f ON f.fingerprint_id = e.fingerprint_id
             WHERE e.fingerprint_id = ?1
             ORDER BY e.id DESC LIMIT 1",
            [fp],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| format!("no events for fingerprint {fp}: {e}"))?;
    Ok(ResolvedQuery {
        sql: row.0.filter(|s| !s.trim().is_empty()).unwrap_or(row.2),
        params_json: row.1,
        wire_params: Vec::new(),
        param_oids: Vec::new(),
    })
}

async fn explain_query(
    client: &ShadowClient,
    replayer: &Replayer,
    resolved: &ResolvedQuery,
) -> Result<ExplainOutput, String> {
    if !resolved.wire_params.is_empty() {
        let record = StoragePgrecRecord {
            delta_us: 0,
            client_id: 0,
            txn_id: 0,
            kind: PgrecRecordKind::Query,
            sql: resolved.sql.clone(),
            param_oids: resolved.param_oids.clone(),
            param_format_codes: Vec::new(),
            params: resolved.wire_params.clone(),
        };
        return replayer
            .explain_record(&record)
            .await
            .map_err(|e| e.to_string());
    }
    let params = json_sql_params(resolved.params_json.as_deref()).map_err(|e| e.to_string())?;
    let refs = param_refs(&params);
    client
        .explain_select(&resolved.sql, &refs)
        .await
        .map_err(|e| e.to_string())
}

#[derive(Debug)]
enum SqlParam {
    Null,
    Text(String),
}

fn json_sql_params(params_json: Option<&str>) -> Result<Vec<SqlParam>, ExplainError> {
    let Some(raw) = params_json else {
        return Ok(Vec::new());
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if !trimmed.starts_with('[') {
        return Ok(Vec::new());
    }
    let arr: Vec<Value> = serde_json::from_str(trimmed).map_err(ExplainError::InvalidJson)?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        match v {
            Value::Null => out.push(SqlParam::Null),
            Value::String(s) => out.push(SqlParam::Text(s)),
            other => out.push(SqlParam::Text(other.to_string())),
        }
    }
    Ok(out)
}

fn param_refs(params: &[SqlParam]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

impl ToSql for SqlParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use tokio_postgres::types::IsNull;
        match self {
            SqlParam::Null => Ok(IsNull::Yes),
            SqlParam::Text(s) => tokio_postgres::types::ToSql::to_sql(s, ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

async fn replay_query_record(
    client: &tokio_postgres::Client,
    record: &PgrecRecord,
) -> Result<(), String> {
    match record.kind {
        PgrecRecordKind::Query => pg_shadow::query_pgrec_record(client, record)
            .await
            .map_err(|e| e.to_string())?,
        PgrecRecordKind::SessionState | PgrecRecordKind::TxnBoundary => {
            client
                .batch_execute(&record.sql)
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn redact_url(url: &str) -> String {
    if let Some(at) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let prefix = &url[..scheme_end + 3];
            let host = &url[at + 1..];
            return format!("{prefix}***@{host}");
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_shadow_password() {
        let u = "postgresql://user:secret@127.0.0.1:5434/db";
        assert!(redact_url(u).contains("***@"));
        assert!(!redact_url(u).contains("secret"));
    }
}
