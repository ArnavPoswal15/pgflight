use crate::fingerprint::FingerprintCache;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct N1Config {
    pub min_repeat: u32,
    pub gap_ms: u64,
}

impl Default for N1Config {
    fn default() -> Self {
        Self {
            min_repeat: 5,
            gap_ms: 2_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    pub kind: String,
    pub fingerprint_id: i64,
    pub parent_fingerprint: Option<i64>,
    pub repeat_count: u32,
    pub total_wasted_us: u64,
    pub confidence: String,
    pub created_at_ms: u64,
}

#[derive(Default)]
struct BurstState {
    count: u32,
    last_ms: u64,
    last_parent: Option<i64>,
    wasted_us: u64,
}

pub struct N1Detector {
    cfg: N1Config,
    per_txn: HashMap<(u64, i64), BurstState>,
    global_gap: HashMap<i64, BurstState>,
}

impl N1Detector {
    pub fn new(cfg: N1Config) -> Self {
        Self {
            cfg,
            per_txn: HashMap::new(),
            global_gap: HashMap::new(),
        }
    }

    pub fn observe(
        &mut self,
        txn_id: u64,
        fingerprint_id: i64,
        parent_fingerprint: Option<i64>,
        exec_us: u64,
        is_select_like: bool,
    ) -> Option<Anomaly> {
        if !is_select_like {
            return None;
        }
        let now = now_ms();
        let key = (txn_id, fingerprint_id);
        let burst = self.per_txn.entry(key).or_default();
        if now.saturating_sub(burst.last_ms) > self.cfg.gap_ms {
            burst.count = 0;
            burst.wasted_us = 0;
        }
        burst.count += 1;
        burst.last_ms = now;
        burst.last_parent = parent_fingerprint;
        burst.wasted_us += exec_us;

        let gap = self.global_gap.entry(fingerprint_id).or_default();
        if now.saturating_sub(gap.last_ms) > self.cfg.gap_ms {
            gap.count = 0;
            gap.wasted_us = 0;
        }
        gap.count += 1;
        gap.last_ms = now;
        gap.wasted_us += exec_us;

        let repeat = burst.count.max(gap.count);
        if repeat >= self.cfg.min_repeat {
            return Some(Anomaly {
                kind: "n_plus_one".into(),
                fingerprint_id,
                parent_fingerprint: burst.last_parent,
                repeat_count: repeat,
                total_wasted_us: burst.wasted_us.max(gap.wasted_us),
                confidence: "frequency".into(),
                created_at_ms: now,
            });
        }
        None
    }
}

/// N+1 detection runs in the recorder worker only (not the proxy hot path).
pub fn is_select_like(_cache: &mut FingerprintCache, sql: &str) -> bool {
    pg_shadow::assert_select_only(sql).is_ok()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_select_like_rejects_dml() {
        let mut cache = FingerprintCache::default();
        assert!(!is_select_like(&mut cache, "INSERT INTO t VALUES (1)"));
        assert!(is_select_like(&mut cache, "SELECT 1"));
    }

    #[test]
    fn n_plus_one_burst_50() {
        let mut det = N1Detector::new(N1Config {
            min_repeat: 5,
            gap_ms: 60_000,
        });
        let fp = 99_i64;
        let mut last = None;
        for _ in 0..50 {
            last = det.observe(1, fp, Some(1), 1_000, true);
        }
        let a = last.expect("expected anomaly");
        assert_eq!(a.repeat_count, 50);
        assert_eq!(a.kind, "n_plus_one");
    }
}
