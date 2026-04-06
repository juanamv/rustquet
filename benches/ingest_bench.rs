use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};

mod common;

const REQUEST_SAMPLE_SIZE: usize = 512;

fn bench_ingest_absorption(c: &mut Criterion) {
    let payloads = common::build_payloads("criterion-ingest", REQUEST_SAMPLE_SIZE);
    let mut group = common::configure_group(
        c,
        "ingest_absorption",
        REQUEST_SAMPLE_SIZE as u64,
        20,
        Duration::from_secs(10),
    );

    group.bench_function("http_to_rocksdb_ack_512_requests", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let harness = common::BenchHarness::new((REQUEST_SAMPLE_SIZE as u64) * 2, false);
                let started = Instant::now();

                harness.runtime.block_on(harness.send_payloads(&payloads));

                total += started.elapsed();
                harness.runtime.block_on(harness.shutdown_and_drain(1, 0));
            }

            total
        });
    });

    group.finish();
}

criterion_group!(ingest_bench, bench_ingest_absorption);
criterion_main!(ingest_bench);
