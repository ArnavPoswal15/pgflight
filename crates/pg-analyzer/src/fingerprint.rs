use lru::LruCache;
use pg_query::{normalize, parse};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use thiserror::Error;

const LRU_DEFAULT_CAP: NonZeroUsize = match NonZeroUsize::new(1024) {
    Some(n) => n,
    None => panic!("1024 is non-zero"),
};

#[derive(Debug, Error)]
pub enum FingerprintError {
    #[error("parse failed: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    pub fingerprint_id: i64,
    pub canonical_sql: String,
}

pub struct FingerprintCache {
    lru: LruCache<String, Fingerprint>,
}

impl Default for FingerprintCache {
    fn default() -> Self {
        Self::new(10_000)
    }
}

impl FingerprintCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap_or(LRU_DEFAULT_CAP);
        Self {
            lru: LruCache::new(cap),
        }
    }

    pub fn fingerprint(&mut self, sql: &str) -> Result<Fingerprint, FingerprintError> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err(FingerprintError::Parse("empty sql".into()));
        }
        if let Some(hit) = self.lru.get(trimmed) {
            return Ok(hit.clone());
        }
        let fp = compute_fingerprint(trimmed)?;
        self.lru.put(trimmed.to_string(), fp.clone());
        Ok(fp)
    }
}

fn compute_fingerprint(sql: &str) -> Result<Fingerprint, FingerprintError> {
    parse(sql).map_err(|e| FingerprintError::Parse(e.to_string()))?;
    let normalized = normalize(sql)
        .map_err(|e| FingerprintError::Parse(e.to_string()))?
        .trim()
        .to_string();
    let id = stable_id(&normalized);
    Ok(Fingerprint {
        fingerprint_id: id,
        canonical_sql: normalized,
    })
}

pub fn stable_id(canonical_sql: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    canonical_sql.hash(&mut hasher);
    (hasher.finish() & i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_sql_same_fingerprint() {
        let mut cache = FingerprintCache::new(16);
        let a = cache.fingerprint("SELECT 1").unwrap();
        let b = cache.fingerprint("SELECT 1").unwrap();
        assert_eq!(a.fingerprint_id, b.fingerprint_id);
    }

    #[test]
    fn lru_cache_hit() {
        let mut cache = FingerprintCache::new(16);
        let a = cache.fingerprint("SELECT 1").unwrap();
        let b = cache.fingerprint("SELECT 1").unwrap();
        assert_eq!(a.canonical_sql, b.canonical_sql);
    }
}
