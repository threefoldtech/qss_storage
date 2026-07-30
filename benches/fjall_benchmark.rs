//! Comparative benchmarks for the two fjall metadata backends.
//!
//! `FjallStore` is transactional (fjall's single-writer transactional
//! database), `FjallStoreNotx` is not (rollback is implemented in our own
//! code). Both are driven through the same `MetaStore` facade, so every
//! scenario below measures the backend and nothing else.

use cas_storage::{
    Block, BlockID, BucketMeta, FjallStore, FjallStoreNotx, MetaStore, Object, ObjectData,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::RngExt;
use std::hint::black_box;
use std::time::Duration;
use tempfile::TempDir;

// Helper function to create a temporary MetaStore backed by FjallStore
fn setup_fjall_store() -> (MetaStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = FjallStore::new(
        dir.path().to_path_buf(),
        Some(1024), // Use a reasonable inline metadata size for benchmarking
        None,       // Use default durability
    );
    (MetaStore::new(store, Some(1024)), dir)
}

// Helper function to create a temporary MetaStore backed by FjallStoreNotx
fn setup_fjall_notx_store() -> (MetaStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = FjallStoreNotx::new(
        dir.path().to_path_buf(),
        Some(1024), // Use a reasonable inline metadata size for benchmarking
    );
    (MetaStore::new(store, Some(1024)), dir)
}

// Helper to create a test bucket
fn create_test_bucket(name: &str) -> Vec<u8> {
    let bucket = BucketMeta::new(name.to_string());
    bucket.to_vec()
}

// Helper to create a test object with specified size
fn create_test_object(size: usize) -> Vec<u8> {
    // The payload is built but not stored in the object (the object records
    // the size and an empty block list). Kept because building it is part of
    // what the mixed workload measures.
    let mut data = vec![0u8; size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    black_box(data);

    // Create a dummy block ID
    let block_id: BlockID = std::array::from_fn(|i| i as u8);

    // Create object with SinglePart data
    let obj = Object::new(
        size as u64,
        block_id,
        ObjectData::SinglePart { blocks: vec![] },
    );
    obj.to_vec()
}

// Helper to create a block ID
fn create_block_id(id: u8) -> BlockID {
    let mut block_id = [0u8; 16];
    block_id[0] = id;
    block_id
}

// Helper to create a test block
fn create_test_block(id: u8, size: usize) -> (BlockID, Vec<u8>) {
    let block_id = create_block_id(id);
    // Create path from block_id
    let path = block_id.to_vec();
    let block = Block::new(size, path);
    (block_id, block.to_vec())
}

fn bench_insert_bucket(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_bucket");
    group.measurement_time(Duration::from_secs(10));

    // Benchmark FjallStore
    {
        let (store, _dir) = setup_fjall_store();
        group.bench_function(BenchmarkId::new("FjallStore", "insert_bucket"), |b| {
            b.iter(|| {
                let bucket_name = format!("bucket-{}", rand::rng().random::<u32>());
                let bucket_data = create_test_bucket(&bucket_name);
                black_box(store.insert_bucket(&bucket_name, bucket_data)).unwrap();
            });
        });
    }

    // Benchmark FjallStoreNotx
    {
        let (store, _dir) = setup_fjall_notx_store();
        group.bench_function(BenchmarkId::new("FjallStoreNotx", "insert_bucket"), |b| {
            b.iter(|| {
                let bucket_name = format!("bucket-{}", rand::rng().random::<u32>());
                let bucket_data = create_test_bucket(&bucket_name);
                black_box(store.insert_bucket(&bucket_name, bucket_data)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_insert_meta(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_meta");
    group.measurement_time(Duration::from_secs(10));

    // Small object (1KB)
    let small_object = create_test_object(1024);

    // Medium object (100KB)
    let medium_object = create_test_object(100 * 1024);

    // Benchmark FjallStore with small object
    {
        let (store, _dir) = setup_fjall_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        group.bench_function(BenchmarkId::new("FjallStore", "insert_small_object"), |b| {
            b.iter(|| {
                let key = format!("key-{}", rand::rng().random::<u32>());
                black_box(store.insert_meta(bucket_name, &key, small_object.clone())).unwrap();
            });
        });
    }

    // Benchmark FjallStoreNotx with small object
    {
        let (store, _dir) = setup_fjall_notx_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        group.bench_function(
            BenchmarkId::new("FjallStoreNotx", "insert_small_object"),
            |b| {
                b.iter(|| {
                    let key = format!("key-{}", rand::rng().random::<u32>());
                    black_box(store.insert_meta(bucket_name, &key, small_object.clone())).unwrap();
                });
            },
        );
    }

    // Benchmark FjallStore with medium object
    {
        let (store, _dir) = setup_fjall_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        group.bench_function(
            BenchmarkId::new("FjallStore", "insert_medium_object"),
            |b| {
                b.iter(|| {
                    let key = format!("key-{}", rand::rng().random::<u32>());
                    black_box(store.insert_meta(bucket_name, &key, medium_object.clone())).unwrap();
                });
            },
        );
    }

    // Benchmark FjallStoreNotx with medium object
    {
        let (store, _dir) = setup_fjall_notx_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        group.bench_function(
            BenchmarkId::new("FjallStoreNotx", "insert_medium_object"),
            |b| {
                b.iter(|| {
                    let key = format!("key-{}", rand::rng().random::<u32>());
                    black_box(store.insert_meta(bucket_name, &key, medium_object.clone())).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_get_meta(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_meta");
    group.measurement_time(Duration::from_secs(10));

    // Benchmark FjallStore
    {
        let (store, _dir) = setup_fjall_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        // Insert some test objects
        for i in 0..100 {
            let key = format!("key-{i}");
            let obj = create_test_object(1024);
            store.insert_meta(bucket_name, &key, obj).unwrap();
        }

        group.bench_function(BenchmarkId::new("FjallStore", "get_meta"), |b| {
            b.iter(|| {
                let key = format!("key-{}", rand::rng().random::<u8>() % 100);
                black_box(store.get_meta(bucket_name, &key)).unwrap();
            });
        });
    }

    // Benchmark FjallStoreNotx
    {
        let (store, _dir) = setup_fjall_notx_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        // Insert some test objects
        for i in 0..100 {
            let key = format!("key-{i}");
            let obj = create_test_object(1024);
            store.insert_meta(bucket_name, &key, obj).unwrap();
        }

        group.bench_function(BenchmarkId::new("FjallStoreNotx", "get_meta"), |b| {
            b.iter(|| {
                let key = format!("key-{}", rand::rng().random::<u8>() % 100);
                black_box(store.get_meta(bucket_name, &key)).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_list_buckets(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_buckets");
    group.measurement_time(Duration::from_secs(10));

    // Benchmark FjallStore
    {
        let (store, _dir) = setup_fjall_store();

        // Create some test buckets
        for i in 0..50 {
            let bucket_name = format!("bucket-{i}");
            let bucket_data = create_test_bucket(&bucket_name);
            store.insert_bucket(&bucket_name, bucket_data).unwrap();
        }

        group.bench_function(BenchmarkId::new("FjallStore", "list_buckets"), |b| {
            b.iter(|| {
                black_box(store.list_buckets()).unwrap();
            });
        });
    }

    // Benchmark FjallStoreNotx
    {
        let (store, _dir) = setup_fjall_notx_store();

        // Create some test buckets
        for i in 0..50 {
            let bucket_name = format!("bucket-{i}");
            let bucket_data = create_test_bucket(&bucket_name);
            store.insert_bucket(&bucket_name, bucket_data).unwrap();
        }

        group.bench_function(BenchmarkId::new("FjallStoreNotx", "list_buckets"), |b| {
            b.iter(|| {
                black_box(store.list_buckets()).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_transaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction");
    group.measurement_time(Duration::from_secs(10));

    // Benchmark FjallStore transaction
    {
        let (store, _dir) = setup_fjall_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        group.bench_function(BenchmarkId::new("FjallStore", "transaction"), |b| {
            b.iter(|| {
                let mut tx = store.begin_transaction();
                let (block_id, _) = create_test_block(rand::rng().random::<u8>(), 1024);
                black_box(tx.write_block(block_id, 1024, false)).unwrap();
                black_box(tx.commit()).unwrap();
            });
        });
    }

    // Benchmark FjallStoreNotx transaction
    {
        let (store, _dir) = setup_fjall_notx_store();
        let bucket_name = "test-bucket";
        let bucket_data = create_test_bucket(bucket_name);
        store.insert_bucket(bucket_name, bucket_data).unwrap();

        group.bench_function(BenchmarkId::new("FjallStoreNotx", "transaction"), |b| {
            b.iter(|| {
                let mut tx = store.begin_transaction();
                let (block_id, _) = create_test_block(rand::rng().random::<u8>(), 1024);
                black_box(tx.write_block(block_id, 1024, false)).unwrap();
                black_box(tx.commit()).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10); // Fewer samples for this more complex benchmark

    // Benchmark FjallStore with mixed workload
    {
        let (store, _dir) = setup_fjall_store();

        group.bench_function(BenchmarkId::new("FjallStore", "mixed_workload"), |b| {
            b.iter(|| {
                // Create a bucket
                let bucket_name = format!("bucket-{}", rand::rng().random::<u16>());
                let bucket_data = create_test_bucket(&bucket_name);
                store.insert_bucket(&bucket_name, bucket_data).unwrap();

                // Insert some objects
                for i in 0..10 {
                    let key = format!("key-{i}");
                    let obj = create_test_object(1024 * (i + 1));
                    store.insert_meta(&bucket_name, &key, obj).unwrap();
                }

                // Read some objects
                for i in 0..5 {
                    let key = format!("key-{i}");
                    black_box(store.get_meta(&bucket_name, &key)).unwrap();
                }

                // List buckets
                black_box(store.list_buckets()).unwrap();

                // Use a transaction
                let mut tx = store.begin_transaction();
                let (block_id, _) = create_test_block(rand::rng().random::<u8>(), 1024);
                black_box(tx.write_block(block_id, 1024, false)).unwrap();
                black_box(tx.commit()).unwrap();
            });
        });
    }

    // Benchmark FjallStoreNotx with mixed workload
    {
        let (store, _dir) = setup_fjall_notx_store();

        group.bench_function(BenchmarkId::new("FjallStoreNotx", "mixed_workload"), |b| {
            b.iter(|| {
                // Create a bucket
                let bucket_name = format!("bucket-{}", rand::rng().random::<u16>());
                let bucket_data = create_test_bucket(&bucket_name);
                store.insert_bucket(&bucket_name, bucket_data).unwrap();

                // Insert some objects
                for i in 0..10 {
                    let key = format!("key-{i}");
                    let obj = create_test_object(1024 * (i + 1));
                    store.insert_meta(&bucket_name, &key, obj).unwrap();
                }

                // Read some objects
                for i in 0..5 {
                    let key = format!("key-{i}");
                    black_box(store.get_meta(&bucket_name, &key)).unwrap();
                }

                // List buckets
                black_box(store.list_buckets()).unwrap();

                // Use a transaction
                let mut tx = store.begin_transaction();
                let (block_id, _) = create_test_block(rand::rng().random::<u8>(), 1024);
                black_box(tx.write_block(block_id, 1024, false)).unwrap();
                black_box(tx.commit()).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_bucket,
    bench_insert_meta,
    bench_get_meta,
    bench_list_buckets,
    bench_transaction,
    bench_mixed_workload
);
criterion_main!(benches);
