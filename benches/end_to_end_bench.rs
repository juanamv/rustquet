use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};

mod common;

const END_TO_END_BATCH_SIZE: usize = 1_000;

fn bench_end_to_end_absorption(c: &mut Criterion) {
    let payloads = common::build_payloads("criterion-end-to-end", END_TO_END_BATCH_SIZE);
    let mut group = common::configure_group(
        c,
        "end_to_end_absorption",
        END_TO_END_BATCH_SIZE as u64,
        10,
        Duration::from_secs(15),
    );

    group.bench_function("http_to_parquet_finalized_batch_1000", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let harness = common::BenchHarness::new(END_TO_END_BATCH_SIZE as u64, true);
                let started = Instant::now();

                harness.runtime.block_on(async {
                    harness.send_payloads(&payloads).await;
                    harness.wait_for_state(1, 1, 0).await;
                });

                total += started.elapsed();
                harness.runtime.block_on(harness.shutdown_and_drain(1, 1));
            }

            total
        });
    });

    group.finish();
}

criterion_group!(end_to_end_bench, bench_end_to_end_absorption);
criterion_main!(end_to_end_bench);
