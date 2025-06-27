use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use slice::LSMTree;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use rand::prelude::*;
use rand::distributions::Alphanumeric;

// YCSB-style workload configurations
#[derive(Clone)]
struct YCSBWorkload {
    name: &'static str,
    read_proportion: f64,
    update_proportion: f64,
    insert_proportion: f64,
    scan_proportion: f64,
    read_modify_write_proportion: f64,
    request_distribution: RequestDistribution,
}

#[derive(Clone)]
enum RequestDistribution {
    Uniform,
    Zipfian(f64), // parameter for skewness
    Latest,       // favor recently inserted records
}

impl YCSBWorkload {
    // YCSB Workload A: Update heavy workload (50/50 reads and writes)
    fn workload_a() -> Self {
        YCSBWorkload {
            name: "YCSB-A",
            read_proportion: 0.5,
            update_proportion: 0.5,
            insert_proportion: 0.0,
            scan_proportion: 0.0,
            read_modify_write_proportion: 0.0,
            request_distribution: RequestDistribution::Zipfian(0.99),
        }
    }

    // YCSB Workload B: Read mostly workload (95/5 reads and writes)
    fn workload_b() -> Self {
        YCSBWorkload {
            name: "YCSB-B",
            read_proportion: 0.95,
            update_proportion: 0.05,
            insert_proportion: 0.0,
            scan_proportion: 0.0,
            read_modify_write_proportion: 0.0,
            request_distribution: RequestDistribution::Zipfian(0.99),
        }
    }

    // YCSB Workload C: Read only workload
    fn workload_c() -> Self {
        YCSBWorkload {
            name: "YCSB-C",
            read_proportion: 1.0,
            update_proportion: 0.0,
            insert_proportion: 0.0,
            scan_proportion: 0.0,
            read_modify_write_proportion: 0.0,
            request_distribution: RequestDistribution::Zipfian(0.99),
        }
    }

    // YCSB Workload D: Read latest workload (95% reads, 5% inserts)
    fn workload_d() -> Self {
        YCSBWorkload {
            name: "YCSB-D",
            read_proportion: 0.95,
            update_proportion: 0.0,
            insert_proportion: 0.05,
            scan_proportion: 0.0,
            read_modify_write_proportion: 0.0,
            request_distribution: RequestDistribution::Latest,
        }
    }

    // YCSB Workload E: Short ranges (95% scans, 5% inserts)
    fn workload_e() -> Self {
        YCSBWorkload {
            name: "YCSB-E",
            read_proportion: 0.0,
            update_proportion: 0.0,
            insert_proportion: 0.05,
            scan_proportion: 0.95,
            read_modify_write_proportion: 0.0,
            request_distribution: RequestDistribution::Zipfian(0.99),
        }
    }

    // YCSB Workload F: Read-modify-write (50% reads, 50% read-modify-writes)
    fn workload_f() -> Self {
        YCSBWorkload {
            name: "YCSB-F",
            read_proportion: 0.5,
            update_proportion: 0.0,
            insert_proportion: 0.0,
            scan_proportion: 0.0,
            read_modify_write_proportion: 0.5,
            request_distribution: RequestDistribution::Zipfian(0.99),
        }
    }
}

// db_bench style workload configurations
#[derive(Clone)]
struct DBBenchWorkload {
    name: &'static str,
    operation: DBBenchOperation,
    key_size: usize,
    value_size: usize,
    num_operations: usize,
    threads: usize,
}

#[derive(Clone)]
enum DBBenchOperation {
    FillSeq,
    FillRandom,
    ReadRandom,
    ReadSeq,
    ReadWhileWriting { write_rate: usize },
    Overwrite,
    SeekRandom { seek_nexts: usize },
}

impl DBBenchWorkload {
    fn fillseq() -> Self {
        DBBenchWorkload {
            name: "fillseq",
            operation: DBBenchOperation::FillSeq,
            key_size: 16,
            value_size: 100,
            num_operations: 10_000,
            threads: 1,
        }
    }

    fn fillrandom() -> Self {
        DBBenchWorkload {
            name: "fillrandom",
            operation: DBBenchOperation::FillRandom,
            key_size: 16,
            value_size: 100,
            num_operations: 10_000,
            threads: 1,
        }
    }

    fn readrandom() -> Self {
        DBBenchWorkload {
            name: "readrandom",
            operation: DBBenchOperation::ReadRandom,
            key_size: 16,
            value_size: 100,
            num_operations: 10_000,
            threads: 4,
        }
    }

    fn readseq() -> Self {
        DBBenchWorkload {
            name: "readseq",
            operation: DBBenchOperation::ReadSeq,
            key_size: 16,
            value_size: 100,
            num_operations: 10_000,
            threads: 1,
        }
    }

    fn readwhilewriting() -> Self {
        DBBenchWorkload {
            name: "readwhilewriting",
            operation: DBBenchOperation::ReadWhileWriting { write_rate: 1000 },
            key_size: 16,
            value_size: 100,
            num_operations: 10_000,
            threads: 4,
        }
    }

    fn overwrite() -> Self {
        DBBenchWorkload {
            name: "overwrite",
            operation: DBBenchOperation::Overwrite,
            key_size: 16,
            value_size: 100,
            num_operations: 10_000,
            threads: 1,
        }
    }

    fn seekrandom() -> Self {
        DBBenchWorkload {
            name: "seekrandom",
            operation: DBBenchOperation::SeekRandom { seek_nexts: 10 },
            key_size: 16,
            value_size: 100,
            num_operations: 1_000,
            threads: 4,
        }
    }
}

// Utility functions for generating test data
fn generate_key(size: usize, id: u64) -> Vec<u8> {
    let mut key = format!("key{:016}", id).into_bytes();
    key.resize(size, b'0');
    key
}

fn generate_value(size: usize, rng: &mut impl Rng) -> Vec<u8> {
    (0..size).map(|_| rng.sample(Alphanumeric) as u8).collect()
}

fn zipfian_distribution(n: usize, theta: f64) -> Vec<f64> {
    let mut weights = Vec::with_capacity(n);
    let mut sum = 0.0;
    
    for i in 1..=n {
        let weight = 1.0 / (i as f64).powf(theta);
        weights.push(weight);
        sum += weight;
    }
    
    // Normalize to create cumulative distribution
    let mut cumulative = 0.0;
    for weight in &mut weights {
        cumulative += *weight / sum;
        *weight = cumulative;
    }
    
    weights
}

fn select_zipfian(weights: &[f64], rng: &mut impl Rng) -> usize {
    let r: f64 = rng.gen();
    weights.iter().position(|&w| r <= w).unwrap_or(weights.len() - 1)
}

// YCSB benchmark implementation
async fn run_ycsb_workload(
    lsm: &LSMTree,
    workload: &YCSBWorkload,
    record_count: usize,
    operation_count: usize,
) -> (u64, u64, u64, u64, u64) { // (reads, updates, inserts, scans, rmws)
    let mut rng = thread_rng();
    let mut reads = 0u64;
    let mut updates = 0u64;
    let mut inserts = 0u64;
    let mut scans = 0u64;
    let mut rmws = 0u64;

    // Pre-generate zipfian distribution if needed
    let zipfian_weights = match &workload.request_distribution {
        RequestDistribution::Zipfian(theta) => Some(zipfian_distribution(record_count, *theta)),
        _ => None,
    };

    for _ in 0..operation_count {
        let op_choice: f64 = rng.gen();
        let mut cumulative = 0.0;

        // Determine operation type
        cumulative += workload.read_proportion;
        if op_choice < cumulative {
            // Read operation
            let key_id = match &workload.request_distribution {
                RequestDistribution::Uniform => rng.gen_range(0..record_count),
                RequestDistribution::Zipfian(_) => {
                    select_zipfian(zipfian_weights.as_ref().unwrap(), &mut rng)
                }
                RequestDistribution::Latest => {
                    // Favor recently inserted records
                    let latest_range = record_count.min(1000);
                    record_count.saturating_sub(rng.gen_range(0..latest_range))
                }
            };
            
            let key = generate_key(16, key_id as u64);
            let _ = lsm.get(&key).await;
            reads += 1;
            continue;
        }

        cumulative += workload.update_proportion;
        if op_choice < cumulative {
            // Update operation
            let key_id = match &workload.request_distribution {
                RequestDistribution::Uniform => rng.gen_range(0..record_count),
                RequestDistribution::Zipfian(_) => {
                    select_zipfian(zipfian_weights.as_ref().unwrap(), &mut rng)
                }
                RequestDistribution::Latest => {
                    let latest_range = record_count.min(1000);
                    record_count.saturating_sub(rng.gen_range(0..latest_range))
                }
            };
            
            let key = generate_key(16, key_id as u64);
            let value = generate_value(100, &mut rng);
            let _ = lsm.put(&key, &value).await;
            updates += 1;
            continue;
        }

        cumulative += workload.insert_proportion;
        if op_choice < cumulative {
            // Insert operation (new record)
            let key_id = record_count + inserts as usize;
            let key = generate_key(16, key_id as u64);
            let value = generate_value(100, &mut rng);
            let _ = lsm.put(&key, &value).await;
            inserts += 1;
            continue;
        }

        cumulative += workload.scan_proportion;
        if op_choice < cumulative {
            // Scan operation (not implemented in our LSM yet)
            scans += 1;
            continue;
        }

        cumulative += workload.read_modify_write_proportion;
        if op_choice < cumulative {
            // Read-modify-write operation
            let key_id = match &workload.request_distribution {
                RequestDistribution::Uniform => rng.gen_range(0..record_count),
                RequestDistribution::Zipfian(_) => {
                    select_zipfian(zipfian_weights.as_ref().unwrap(), &mut rng)
                }
                RequestDistribution::Latest => {
                    let latest_range = record_count.min(1000);
                    record_count.saturating_sub(rng.gen_range(0..latest_range))
                }
            };
            
            let key = generate_key(16, key_id as u64);
            
            // Read
            let existing_value = lsm.get(&key).await.unwrap_or_default();
            
            // Modify
            let mut new_value = existing_value.unwrap_or_else(|| generate_value(100, &mut rng));
            if !new_value.is_empty() {
                new_value[0] = new_value[0].wrapping_add(1);
            }
            
            // Write
            let _ = lsm.put(&key, &new_value).await;
            rmws += 1;
        }
    }

    (reads, updates, inserts, scans, rmws)
}

// db_bench benchmark implementation
async fn run_dbbench_workload(
    lsm: &LSMTree,
    workload: &DBBenchWorkload,
) -> u64 {
    let mut rng = thread_rng();
    let mut operations = 0u64;

    match &workload.operation {
        DBBenchOperation::FillSeq => {
            for i in 0..workload.num_operations {
                let key = generate_key(workload.key_size, i as u64);
                let value = generate_value(workload.value_size, &mut rng);
                let _ = lsm.put(&key, &value).await;
                operations += 1;
            }
        }
        
        DBBenchOperation::FillRandom => {
            for _ in 0..workload.num_operations {
                let key_id: u64 = rng.gen();
                let key = generate_key(workload.key_size, key_id);
                let value = generate_value(workload.value_size, &mut rng);
                let _ = lsm.put(&key, &value).await;
                operations += 1;
            }
        }
        
        DBBenchOperation::ReadRandom => {
            for _ in 0..workload.num_operations {
                let key_id = rng.gen_range(0..workload.num_operations);
                let key = generate_key(workload.key_size, key_id as u64);
                let _ = lsm.get(&key).await;
                operations += 1;
            }
        }
        
        DBBenchOperation::ReadSeq => {
            for i in 0..workload.num_operations {
                let key = generate_key(workload.key_size, i as u64);
                let _ = lsm.get(&key).await;
                operations += 1;
            }
        }
        
        DBBenchOperation::ReadWhileWriting { write_rate: _ } => {
            // Simplified version - alternating reads and writes
            for i in 0..workload.num_operations {
                if i % 10 == 0 {
                    // Write
                    let key = generate_key(workload.key_size, i as u64);
                    let value = generate_value(workload.value_size, &mut rng);
                    let _ = lsm.put(&key, &value).await;
                } else {
                    // Read
                    let key_id = rng.gen_range(0..i.max(1));
                    let key = generate_key(workload.key_size, key_id as u64);
                    let _ = lsm.get(&key).await;
                }
                operations += 1;
            }
        }
        
        DBBenchOperation::Overwrite => {
            for i in 0..workload.num_operations {
                let key = generate_key(workload.key_size, i as u64);
                let value = generate_value(workload.value_size, &mut rng);
                let _ = lsm.put(&key, &value).await;
                operations += 1;
            }
        }
        
        DBBenchOperation::SeekRandom { seek_nexts: _ } => {
            // Simplified - just random reads for now
            for _ in 0..workload.num_operations {
                let key_id = rng.gen_range(0..workload.num_operations);
                let key = generate_key(workload.key_size, key_id as u64);
                let _ = lsm.get(&key).await;
                operations += 1;
            }
        }
    }

    operations
}

// Benchmark functions
fn ycsb_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let workloads = vec![
        YCSBWorkload::workload_a(),
        YCSBWorkload::workload_b(),
        YCSBWorkload::workload_c(),
    ];

    let mut group = c.benchmark_group("YCSB_Workloads");
    group.sample_size(10);
    
    for workload in workloads {
        let temp_dir = TempDir::new().unwrap();
        let lsm = rt.block_on(async {
            let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 1000).await.unwrap();
            
            // Load initial data
            let mut rng = thread_rng();
            for i in 0..1_000 {
                let key = generate_key(16, i);
                let value = generate_value(100, &mut rng);
                lsm.put(&key, &value).await.unwrap();
            }
            
            lsm.flush().await.unwrap();
            
            // Wait for background operations
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            
            lsm
        });

        group.throughput(Throughput::Elements(1_000));
        group.bench_with_input(
            BenchmarkId::new("YCSB", workload.name),
            &workload,
            |b, workload| {
                b.to_async(&rt).iter(|| async {
                    let (reads, updates, inserts, scans, rmws) = run_ycsb_workload(
                        &lsm,
                        workload,
                        1_000,
                        1_000,
                    ).await;
                    
                    black_box((reads, updates, inserts, scans, rmws))
                });
            },
        );
    }
    
    group.finish();
}

fn dbbench_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let workloads = vec![
        DBBenchWorkload::fillseq(),
        DBBenchWorkload::fillrandom(),
        DBBenchWorkload::readrandom(),
        DBBenchWorkload::readseq(),
        DBBenchWorkload::readwhilewriting(),
        DBBenchWorkload::overwrite(),
        DBBenchWorkload::seekrandom(),
    ];

    let mut group = c.benchmark_group("DBBench_Workloads");
    group.sample_size(10);
    
    for workload in workloads {
        let temp_dir = TempDir::new().unwrap();
        let lsm = rt.block_on(async {
            let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 1000).await.unwrap();
            
            // Pre-populate for read workloads
            if matches!(workload.operation, 
                DBBenchOperation::ReadRandom | 
                DBBenchOperation::ReadSeq | 
                DBBenchOperation::ReadWhileWriting { .. } |
                DBBenchOperation::Overwrite |
                DBBenchOperation::SeekRandom { .. }
            ) {
                let mut rng = thread_rng();
                for i in 0..workload.num_operations {
                    let key = generate_key(workload.key_size, i as u64);
                    let value = generate_value(workload.value_size, &mut rng);
                    lsm.put(&key, &value).await.unwrap();
                }
                
                lsm.flush().await.unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            
            lsm
        });

        group.throughput(Throughput::Elements(workload.num_operations as u64));
        group.bench_with_input(
            BenchmarkId::new("DBBench", workload.name),
            &workload,
            |b, workload| {
                b.to_async(&rt).iter(|| async {
                    let operations = run_dbbench_workload(&lsm, workload).await;
                    black_box(operations)
                });
            },
        );
    }
    
    group.finish();
}

// Comparison with industry benchmarks
fn industry_comparison_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("Industry_Comparison");
    group.sample_size(10);
    
    // RocksDB-style benchmark (similar to their performance reports)
    let temp_dir = TempDir::new().unwrap();
    let lsm = rt.block_on(async {
        let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 1000).await.unwrap();
        
        // Load 10K records with 10-byte keys and 800-byte values (scaled down from RocksDB report)
        let mut rng = thread_rng();
        for i in 0..10_000 {
            let key = format!("key{:07}", i).into_bytes();
            let value = generate_value(800, &mut rng);
            lsm.put(&key, &value).await.unwrap();
        }
        
        lsm.flush().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        lsm
    });

    group.throughput(Throughput::Elements(5_000));
    group.bench_function("RocksDB_Style_ReadWhileWriting", |b| {
        b.to_async(&rt).iter(|| async {
            let mut rng = thread_rng();
            let mut operations = 0;
            
            // 4 readers + 1 writer pattern from RocksDB benchmark
            for _ in 0..5_000 {
                if rng.gen_bool(0.8) {
                    // Read operation (80%)
                    let key_id = rng.gen_range(0..10_000);
                    let key = format!("key{:07}", key_id).into_bytes();
                    let _ = lsm.get(&key).await;
                } else {
                    // Write operation (20%)
                    let key_id = rng.gen_range(0..10_000);
                    let key = format!("key{:07}", key_id).into_bytes();
                    let value = generate_value(800, &mut rng);
                    let _ = lsm.put(&key, &value).await;
                }
                operations += 1;
            }
            
            black_box(operations)
        });
    });
    
    group.finish();
}

criterion_group!(
    standard_benches,
    ycsb_benchmarks,
    dbbench_benchmarks,
    industry_comparison_benchmarks
);
criterion_main!(standard_benches); 