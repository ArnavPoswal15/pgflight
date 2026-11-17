//! Fingerprint + N+1 analysis (off hot path).

use parking_lot::Mutex;
use pg_analyzer::{
    is_select_like, Anomaly, Fingerprint, FingerprintCache, Leaderboard, N1Config, N1Detector,
};
use pg_storage::{insert_anomaly, FlightRecorder, SqliteError};
use std::sync::Arc;
use tracing::warn;

pub struct AnalysisState {
    cache: Mutex<FingerprintCache>,
    leaderboard: Mutex<Leaderboard>,
    n1: Mutex<N1Detector>,
    pending_anomaly: Mutex<Option<Anomaly>>,
    parse_errors: std::sync::atomic::AtomicU64,
    last_parent_fp: Mutex<Option<i64>>,
}

impl AnalysisState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            cache: Mutex::new(FingerprintCache::default()),
            leaderboard: Mutex::new(Leaderboard::default()),
            n1: Mutex::new(N1Detector::new(N1Config::default())),
            pending_anomaly: Mutex::new(None),
            parse_errors: std::sync::atomic::AtomicU64::new(0),
            last_parent_fp: Mutex::new(None),
        })
    }

    pub fn fingerprint(
        &self,
        raw_sql: &str,
        txn_id: u64,
        exec_us: u64,
        rows_returned: u64,
    ) -> Option<Fingerprint> {
        let mut cache = self.cache.lock();
        let fp = match cache.fingerprint(raw_sql) {
            Ok(v) => v,
            Err(e) => {
                self.parse_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!("fingerprint skip: {e}");
                return None;
            }
        };
        {
            let mut board = self.leaderboard.lock();
            board.record(fp.fingerprint_id, &fp.canonical_sql, exec_us, rows_returned);
        }
        let select_like = is_select_like(&mut cache, raw_sql);
        let parent = *self.last_parent_fp.lock();
        let anomaly = {
            let mut det = self.n1.lock();
            det.observe(txn_id, fp.fingerprint_id, parent, exec_us, select_like)
        };
        if let Some(a) = anomaly {
            *self.pending_anomaly.lock() = Some(a);
        }
        if !select_like {
            *self.last_parent_fp.lock() = Some(fp.fingerprint_id);
        }
        Some(fp)
    }

    pub fn take_pending_anomaly(&self) -> Option<Anomaly> {
        self.pending_anomaly.lock().take()
    }

    pub fn leaderboard_snapshot(&self) -> Vec<pg_analyzer::PatternRow> {
        self.leaderboard.lock().top_patterns(50)
    }

    pub fn anomalies(&self) -> Vec<Anomaly> {
        self.analysis_anomalies()
    }

    fn analysis_anomalies(&self) -> Vec<Anomaly> {
        let pending = self.pending_anomaly.lock().clone();
        pending.into_iter().collect()
    }

    pub fn persist_anomaly(
        &self,
        recorder: &FlightRecorder,
        anomaly: &Anomaly,
    ) -> Result<(), SqliteError> {
        let conn = pg_storage::open_db(recorder.db_path())?;
        insert_anomaly(
            &conn,
            &anomaly.kind,
            anomaly.fingerprint_id,
            anomaly.parent_fingerprint,
            anomaly.repeat_count as i64,
            anomaly.total_wasted_us as i64,
            &anomaly.confidence,
        )
    }

    #[allow(dead_code)]
    pub fn parse_error_count(&self) -> u64 {
        self.parse_errors.load(std::sync::atomic::Ordering::Relaxed)
    }
}
