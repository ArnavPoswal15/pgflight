use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::collections::HashMap;

#[derive(Debug)]
pub struct QueryStats {
    pub exec_count: u64,
    pub total_us: u64,
    pub total_rows: u64,
    histogram: Mutex<Option<Histogram<u64>>>,
}

impl Default for QueryStats {
    fn default() -> Self {
        Self {
            exec_count: 0,
            total_us: 0,
            total_rows: 0,
            histogram: Mutex::new(Histogram::new_with_bounds(1, 60_000_000, 3).ok()),
        }
    }
}

impl QueryStats {
    pub fn record(&mut self, exec_us: u64, rows: u64) {
        self.exec_count += 1;
        self.total_us += exec_us;
        self.total_rows += rows;
        if let Some(h) = self.histogram.get_mut() {
            let _ = h.record(exec_us.max(1));
        }
    }

    pub fn p95_us(&self) -> u64 {
        self.histogram
            .lock()
            .as_ref()
            .map(|h| h.value_at_quantile(0.95))
            .unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct Leaderboard {
    by_fp: HashMap<i64, QueryStats>,
    canonical: HashMap<i64, String>,
}

impl Leaderboard {
    pub fn record(&mut self, fingerprint_id: i64, canonical_sql: &str, exec_us: u64, rows: u64) {
        self.canonical
            .entry(fingerprint_id)
            .or_insert_with(|| canonical_sql.to_string());
        self.by_fp
            .entry(fingerprint_id)
            .or_default()
            .record(exec_us, rows);
    }

    pub fn top_patterns(&self, limit: usize) -> Vec<PatternRow> {
        let mut rows: Vec<PatternRow> = self
            .by_fp
            .iter()
            .map(|(fp, stats)| PatternRow {
                fingerprint_id: *fp,
                canonical_sql: self
                    .canonical
                    .get(fp)
                    .cloned()
                    .unwrap_or_else(|| "?".into()),
                exec_count: stats.exec_count,
                p95_us: stats.p95_us(),
                total_us: stats.total_us,
            })
            .collect();
        rows.sort_by_key(|a| std::cmp::Reverse(a.total_us));
        rows.truncate(limit);
        rows
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PatternRow {
    pub fingerprint_id: i64,
    pub canonical_sql: String,
    pub exec_count: u64,
    pub p95_us: u64,
    pub total_us: u64,
}
