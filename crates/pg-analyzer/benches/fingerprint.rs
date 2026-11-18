use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pg_analyzer::FingerprintCache;

const SAMPLE_SQL: &str =
    "SELECT o.id, u.email FROM orders o JOIN users u ON u.id = o.user_id WHERE o.status = $1";

fn bench_fingerprint(c: &mut Criterion) {
    let mut cache = FingerprintCache::new(10_000);
    // Warm LRU so the timed iteration is cache-hit shaped (daemon hot path after warm-up).
    let _ = cache.fingerprint(SAMPLE_SQL).expect("sample sql");

    c.bench_function("fingerprint_cache_hit", |b| {
        b.iter(|| black_box(cache.fingerprint(black_box(SAMPLE_SQL)).unwrap()));
    });
}

criterion_group!(benches, bench_fingerprint);
criterion_main!(benches);
