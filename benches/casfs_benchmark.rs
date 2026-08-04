//! Benchmarks for the CasFS write paths.
//!
//! Compares the inlined-metadata write path against the regular
//! block-storage write path across a range of object sizes, and the two block
//! address widths (BLAKE3 truncated to 16 bytes vs the full 32) against each
//! other on the block write path.

use bytes::Bytes;
use cas_storage::{AsyncByteStream, CasFS, Durability, Hasher, SharedMetrics, StoreOptions};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::stream;
use rand::RngExt;
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn get_shared_metrics() -> SharedMetrics {
    SharedMetrics::default()
}

// Helper function to create a temporary CasFS, addressing blocks with
// `hasher`.
fn setup_casfs_with(hasher: Hasher) -> (CasFS, TempDir) {
    let dir = TempDir::new().unwrap();
    let root_path = dir.path().to_path_buf();
    let meta_path = root_path.clone();

    let metrics = get_shared_metrics();

    let fs = CasFS::single_namespace(
        root_path,
        meta_path,
        metrics,
        StoreOptions {
            // A reasonable inline threshold for benchmarking, and buffer
            // durability: the rest is the built-in default (stripe count
            // 1024, batch cap 64, no commit station, no read verification).
            inline_metadata_size: Some(1024),
            durability: Durability::Buffer,
            hasher,
            ..StoreOptions::default()
        },
    )
    .unwrap();

    (fs, dir)
}

// Helper function to create a temporary CasFS at the default width
fn setup_casfs() -> (CasFS, TempDir) {
    setup_casfs_with(Hasher::Blake3W32)
}

// Helper to create a test bucket
fn create_test_bucket(fs: &CasFS, name: &str) {
    fs.create_bucket(name).unwrap();
}

// Helper to create random data of specified size
fn create_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut data = vec![0u8; size];
    rng.fill(&mut data[..]);
    data
}

// Convert Vec<u8> to AsyncByteStream for store_single_object_and_meta
fn vec_to_bytestream(data: Vec<u8>) -> AsyncByteStream {
    AsyncByteStream::new(stream::once(async move { Ok(Bytes::from(data)) }))
}

fn bench_store_methods(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("store_methods");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    // Test with different data sizes
    let sizes = [128, 512, 1024, 4096, 8192, 16384];

    for &size in &sizes {
        let (fs, _dir) = setup_casfs();
        let bucket_name = "test-bucket";
        create_test_bucket(&fs, bucket_name);

        // Benchmark store_inlined_object
        group.bench_function(BenchmarkId::new("store_inlined_object", size), |b| {
            b.iter(|| {
                let data = create_random_data(size);
                let key = format!("inline-key-{}", rand::rng().random::<u32>());
                black_box(rt.block_on(fs.store_inlined_object(bucket_name, &key, data))).unwrap()
            })
        });

        // Benchmark store_single_object_and_meta
        group.bench_function(
            BenchmarkId::new("store_single_object_and_meta", size),
            |b| {
                b.iter(|| {
                    let data = create_random_data(size);
                    let key = format!("single-key-{}", rand::rng().random::<u32>());
                    let len = data.len();
                    let stream = vec_to_bytestream(data);
                    black_box(rt.block_on(fs.store_single_object_and_meta(
                        bucket_name,
                        &key,
                        stream,
                        len,
                    )))
                    .unwrap()
                })
            },
        );
    }

    group.finish();
}

fn bench_inlined_object_sizes(c: &mut Criterion) {
    // The write path is async since ADR 0008: an overwrite releases the
    // replaced object's blocks, which takes stripes. These keys are random,
    // so what is measured here is still the fresh-write cost.
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("store_inlined_object_sizes");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    let (fs, _dir) = setup_casfs();
    let bucket_name = "test-bucket";
    create_test_bucket(&fs, bucket_name);

    // Get the maximum inlined data length
    let max_inlined = fs.max_inlined_data_length();

    // Test with different percentages of the max inline size
    let percentages = [25, 50, 75, 90];

    for &percentage in &percentages {
        let size = (max_inlined * percentage) / 100;

        group.bench_function(BenchmarkId::new("percentage_of_max", percentage), |b| {
            b.iter(|| {
                let data = create_random_data(size);
                let key = format!("key-{}", rand::rng().random::<u32>());
                black_box(rt.block_on(fs.store_inlined_object(bucket_name, &key, data))).unwrap()
            })
        });
    }

    group.finish();
}

fn bench_store_methods_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("store_methods_overhead");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(100);

    // Use a very small data size to measure overhead
    let size = 10; // 10 bytes

    let (fs, _dir) = setup_casfs();
    let bucket_name = "test-bucket";
    create_test_bucket(&fs, bucket_name);

    // Benchmark store_inlined_object
    group.bench_function("store_inlined_object_overhead", |b| {
        b.iter(|| {
            let data = create_random_data(size);
            let key = format!("inline-key-{}", rand::rng().random::<u32>());
            black_box(rt.block_on(fs.store_inlined_object(bucket_name, &key, data))).unwrap()
        })
    });

    // Benchmark store_single_object_and_meta
    group.bench_function("store_single_object_and_meta_overhead", |b| {
        b.iter(|| {
            let data = create_random_data(size);
            let key = format!("single-key-{}", rand::rng().random::<u32>());
            let len = data.len();
            let stream = vec_to_bytestream(data);
            black_box(rt.block_on(fs.store_single_object_and_meta(bucket_name, &key, stream, len)))
                .unwrap()
        })
    });

    group.finish();
}

/// Width 16 against width 32 on the block write path.
///
/// One group per width, same object sizes in both, so the two groups line up
/// benchmark for benchmark. What differs between them is the BLAKE3 output the
/// block address is taken from (truncated to 16 bytes vs the full 32) and the
/// 16 extra bytes per block id that the metadata records then carry, so this
/// is the number behind the width guidance for operators.
///
/// Every iteration stamps a fresh counter into the payload: identical bytes
/// would dedup after the first write and measure the block-exists path instead
/// of the write path. Sizes stay modest for the same reason the store is a
/// tempdir -- every iteration leaves a block behind.
fn bench_hash_width(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let sizes = [4096, 65536];

    for hasher in [Hasher::Blake3W16, Hasher::Blake3W32] {
        let mut group = c.benchmark_group(format!("store_by_hash_width/w{}", hasher.width()));
        group.measurement_time(Duration::from_secs(5));
        group.sample_size(20);

        let (fs, _dir) = setup_casfs_with(hasher);
        let bucket_name = "test-bucket";
        create_test_bucket(&fs, bucket_name);

        for &size in &sizes {
            group.throughput(criterion::Throughput::Bytes(size as u64));
            group.bench_function(
                BenchmarkId::new("store_single_object_and_meta", size),
                |b| {
                    let mut payload = create_random_data(size);
                    let mut counter: u64 = 0;
                    b.iter(|| {
                        counter += 1;
                        payload[..8].copy_from_slice(&counter.to_le_bytes());
                        let key = format!("w{}-{}-{counter}", hasher.width(), size);
                        let stream = vec_to_bytestream(payload.clone());
                        black_box(rt.block_on(fs.store_single_object_and_meta(
                            bucket_name,
                            &key,
                            stream,
                            size,
                        )))
                        .unwrap()
                    })
                },
            );
        }

        group.finish();
    }
}

/// Concurrent PUTs of DISTINCT blocks at writer counts K (ADR 0006).
///
/// This is the scenario the file-first redesign changes: disk writes and
/// fjall commits now run in spawn_blocking closures under per-block
/// stripes, so K writers of distinct blocks proceed in parallel instead
/// of serializing behind a worker-parking sync write. Varying K exposes
/// the `(K-1)/N` stripe-collision tail (N = 1024 stripes by default):
/// wall time per batch should scale sublinearly in K until the blocking
/// pool or the disk saturates.
///
/// Payloads are stamped per task and per iteration so no two writers ever
/// share a block -- a shared block would measure the dedup path and the
/// stripe SERIALIZATION instead of the parallel write path.
fn bench_concurrent_puts(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    const OBJECT_SIZE: usize = 65536;

    let mut group = c.benchmark_group("concurrent_puts");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    for k in [1usize, 4, 16, 64] {
        let (fs, _dir) = setup_casfs();
        let fs = std::sync::Arc::new(fs);
        let bucket_name = "test-bucket";
        create_test_bucket(&fs, bucket_name);

        // K objects per iteration = one "batch"; throughput in bytes makes
        // the K runs comparable.
        group.throughput(criterion::Throughput::Bytes((OBJECT_SIZE * k) as u64));
        group.bench_function(BenchmarkId::new("distinct_blocks", k), |b| {
            let payload = create_random_data(OBJECT_SIZE);
            let mut round: u64 = 0;
            b.iter(|| {
                round += 1;
                rt.block_on(async {
                    let mut tasks = Vec::with_capacity(k);
                    for task_no in 0..k {
                        let fs = fs.clone();
                        let mut payload = payload.clone();
                        tasks.push(tokio::spawn(async move {
                            // Unique bytes per task and round: never dedup.
                            payload[..8].copy_from_slice(&round.to_le_bytes());
                            payload[8..16].copy_from_slice(&(task_no as u64).to_le_bytes());
                            let key = format!("k{k}-r{round}-t{task_no}");
                            let stream = vec_to_bytestream(payload);
                            fs.store_single_object_and_meta(
                                "test-bucket",
                                &key,
                                stream,
                                OBJECT_SIZE,
                            )
                            .await
                            .unwrap();
                        }));
                    }
                    for t in tasks {
                        // The join yields (); the awaited work is the
                        // measurement, so there is no value to black-box.
                        t.await.unwrap();
                    }
                })
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_store_methods,
    bench_inlined_object_sizes,
    bench_store_methods_overhead,
    bench_hash_width,
    bench_concurrent_puts
);
criterion_main!(benches);
