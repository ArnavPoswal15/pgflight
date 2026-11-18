use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use pg_wire::{peek_header, STREAM_THRESHOLD};

fn frame_header(body_len: usize) -> Vec<u8> {
    let len = (body_len + 4) as i32;
    let mut buf = vec![0u8; 5 + body_len.min(64)];
    buf[0] = b'Q';
    buf[1..5].copy_from_slice(&len.to_be_bytes());
    buf
}

fn bench_peek_header(c: &mut Criterion) {
    let small = frame_header(32);
    let large = frame_header(STREAM_THRESHOLD + 1);

    let mut group = c.benchmark_group("peek_header");
    group.bench_with_input(BenchmarkId::new("body_le_64kib", 32), &small, |b, buf| {
        b.iter(|| black_box(peek_header(buf).ok()));
    });
    group.bench_with_input(
        BenchmarkId::new("body_gt_64kib", STREAM_THRESHOLD + 1),
        &large,
        |b, buf| {
            b.iter(|| black_box(peek_header(buf).ok()));
        },
    );
}

criterion_group!(benches, bench_peek_header);
criterion_main!(benches);
