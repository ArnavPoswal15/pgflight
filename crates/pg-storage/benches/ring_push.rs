use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pg_storage::{RingBuffer, RingEntry};

fn sample_entry(seq: u64) -> RingEntry {
    RingEntry {
        event_id: 0,
        flight_id: 1,
        connection_id: 2,
        txn_id: 3,
        fingerprint_id: 42,
        canonical_sql: "SELECT 1".into(),
        exec_us: 100,
        queue_wait_us: 5,
        rows_returned: 1,
        created_at_ms: 1_700_000_000_000 + seq,
        app_name: "bench".into(),
        error_code: None,
    }
}

fn bench_ring_push(c: &mut Criterion) {
    c.bench_function("ring_buffer_push", |b| {
        b.iter(|| {
            let mut ring = RingBuffer::new(5_000);
            for i in 0..1_000u64 {
                ring.push(black_box(sample_entry(i)));
            }
            black_box(ring.latest_event_id())
        });
    });
}

criterion_group!(benches, bench_ring_push);
criterion_main!(benches);
