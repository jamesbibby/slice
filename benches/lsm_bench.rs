use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use slice::LSMTree;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tempfile::TempDir;

fn bench_sequential_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("sequential_writes_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let lsm = LSMTree::new(temp_dir.path()).await.unwrap();
                
                for i in 0..1000 {
                    let key = format!("key{:06}", i);
                    let value = format!("value_{}", i);
                    lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
                
                black_box(lsm);
            })
        })
    });
}

fn bench_concurrent_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("concurrent_writes");
    
    for &num_writers in &[1, 2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("writers", num_writers),
            &num_writers,
            |b, &num_writers| {
                b.iter(|| {
                    rt.block_on(async {
                        let temp_dir = tempfile::TempDir::new().unwrap();
                        let lsm = Arc::new(LSMTree::new(temp_dir.path()).await.unwrap());
                        
                        let writes_per_writer = 100;
                        let mut handles = Vec::new();
                        
                        for writer_id in 0..num_writers {
                            let lsm_clone = lsm.clone();
                            let handle = tokio::spawn(async move {
                                for i in 0..writes_per_writer {
                                    let key = format!("w{}:k{:04}", writer_id, i);
                                    let value = format!("value_{}_{}", writer_id, i);
                                    lsm_clone.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }
                        
                        for handle in handles {
                            handle.await.unwrap();
                        }
                        
                        black_box(lsm);
                    })
                })
            },
        );
    }
    group.finish();
}

fn bloom_filter_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("bloom_filter");
    group.sample_size(100);
    
    // Setup: Create LSM with data and flush to SSTables
    let temp_dir = TempDir::new().unwrap();
    let lsm = rt.block_on(async {
        let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 1000).await.unwrap();
        
        // Insert 10,000 keys
        for i in 0..10000 {
            let key = format!("user:{:06}", i);
            let value = format!("data_for_{}", key);
            lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        
        lsm.flush().await.unwrap();
        
        // Wait for flush to complete
        for _ in 0..50 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let stats = lsm.stats();
            if stats.immutable_memtables == 0 && stats.level_stats[0].1 > 0 {
                break;
            }
        }
        
        lsm
    });
    
    // Benchmark: Negative lookups (bloom filter effectiveness)
    let non_existent_keys: Vec<String> = (20000..21000)
        .map(|i| format!("missing:{:06}", i))
        .collect();
        
    group.bench_function("negative_lookups_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                for key in &non_existent_keys {
                    black_box(lsm.get(key.as_bytes()).await.unwrap());
                }
            })
        });
    });
    
    // Benchmark: Positive lookups (existing keys)
    let existing_keys: Vec<String> = (0..1000)
        .map(|i| format!("user:{:06}", i))
        .collect();
        
    group.bench_function("positive_lookups_1000", |b| {
        b.iter(|| {
            rt.block_on(async {
                for key in &existing_keys {
                    black_box(lsm.get(key.as_bytes()).await.unwrap());
                }
            })
        });
    });
    
    group.finish();
}

fn bench_read_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("reads_after_writes", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let lsm = LSMTree::new(temp_dir.path()).await.unwrap();
                
                // Write 1000 keys
                for i in 0..1000 {
                    let key = format!("key{:06}", i);
                    let value = format!("value_{}", i);
                    lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
                
                // Read all keys
                for i in 0..1000 {
                    let key = format!("key{:06}", i);
                    let _value = lsm.get(key.as_bytes()).await.unwrap();
                }
                
                black_box(lsm);
            })
        })
    });
}

fn bench_mixed_workload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("mixed_read_write", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp_dir = tempfile::TempDir::new().unwrap();
                let lsm = Arc::new(LSMTree::new(temp_dir.path()).await.unwrap());
                
                // Initialize with some data
                for i in 0..500 {
                    let key = format!("init{:06}", i);
                    let value = format!("init_value_{}", i);
                    lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
                
                // Mixed workload: 70% reads, 30% writes
                let mut handles = Vec::new();
                
                // Writer task
                let lsm_writer = lsm.clone();
                let write_handle = tokio::spawn(async move {
                    for i in 0..300 {
                        let key = format!("new{:06}", i);
                        let value = format!("new_value_{}", i);
                        lsm_writer.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                    }
                });
                handles.push(write_handle);
                
                // Reader tasks
                for reader_id in 0..2 {
                    let lsm_reader = lsm.clone();
                    let read_handle = tokio::spawn(async move {
                        for i in 0..350 {
                            let key = format!("init{:06}", (i + reader_id * 100) % 500);
                            let _value = lsm_reader.get(key.as_bytes()).await.unwrap();
                        }
                    });
                    handles.push(read_handle);
                }
                
                for handle in handles {
                    handle.await.unwrap();
                }
                
                black_box(lsm);
            })
        })
    });
}

fn bench_cache_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("cache_vs_no_cache", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                
                // Create LSM with cache
                let lsm_with_cache = LSMTree::new_with_cache_size(temp_dir.path(), 500).await.unwrap();
                
                // Insert test data
                for i in 0..1000 {
                    let key = format!("key{:04}", i);
                    let value = format!("value{:04}", i);
                    lsm_with_cache.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
                
                // Force flush to create SSTables
                lsm_with_cache.flush().await.unwrap();
                
                // Wait for flush to complete
                for _ in 0..10 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    let stats = lsm_with_cache.stats();
                    if stats.immutable_memtables == 0 && stats.level_stats[0].1 > 0 {
                        break;
                    }
                }
                
                // Warm up cache
                for i in 0..100 {
                    let key = format!("key{:04}", i);
                    black_box(lsm_with_cache.get(key.as_bytes()).await.unwrap());
                }
                
                // Benchmark cached reads
                let start = std::time::Instant::now();
                for i in 0..100 {
                    let key = format!("key{:04}", i);
                    black_box(lsm_with_cache.get(key.as_bytes()).await.unwrap());
                }
                let cached_time = start.elapsed();
                
                // Clear cache and benchmark uncached reads
                lsm_with_cache.clear_cache();
                let start = std::time::Instant::now();
                for i in 0..100 {
                    let key = format!("key{:04}", i);
                    black_box(lsm_with_cache.get(key.as_bytes()).await.unwrap());
                }
                let uncached_time = start.elapsed();
                
                println!("Cached reads: {:?}", cached_time);
                println!("Uncached reads: {:?}", uncached_time);
                println!("Speedup: {:.2}x", uncached_time.as_nanos() as f64 / cached_time.as_nanos() as f64);
                
                (cached_time, uncached_time)
            })
        });
    });
}

fn bench_lru_eviction(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("lru_eviction_behavior", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp_dir = TempDir::new().unwrap();
                
                // Create LSM with small cache to test eviction
                let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 50).await.unwrap();
                
                // Insert more data than cache can hold
                for i in 0..200 {
                    let key = format!("key{:04}", i);
                    let value = format!("value{:04}", i);
                    lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
                
                // Force flush
                lsm.flush().await.unwrap();
                
                // Wait for flush
                for _ in 0..10 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    let stats = lsm.stats();
                    if stats.immutable_memtables == 0 && stats.level_stats[0].1 > 0 {
                        break;
                    }
                }
                
                // Read all data to populate and test eviction
                for i in 0..200 {
                    let key = format!("key{:04}", i);
                    black_box(lsm.get(key.as_bytes()).await.unwrap());
                }
                
                let stats = lsm.stats();
                assert_eq!(stats.cache_stats.size, 50); // Should be at capacity
                
                // Read first 50 keys again (should be fast due to cache)
                let start = std::time::Instant::now();
                for i in 150..200 { // These should be in cache (most recent)
                    let key = format!("key{:04}", i);
                    black_box(lsm.get(key.as_bytes()).await.unwrap());
                }
                let recent_time = start.elapsed();
                
                // Read older keys (should be slower, not in cache)
                let start = std::time::Instant::now();
                for i in 0..50 { // These should be evicted from cache
                    let key = format!("key{:04}", i);
                    black_box(lsm.get(key.as_bytes()).await.unwrap());
                }
                let old_time = start.elapsed();
                
                println!("Recent keys (cached): {:?}", recent_time);
                println!("Old keys (evicted): {:?}", old_time);
                
                (recent_time, old_time)
            })
        });
    });
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_concurrent_writes,
    bench_read_performance,
    bench_mixed_workload,
    bench_cache_performance,
    bench_lru_eviction,
    bloom_filter_benchmarks
);
criterion_main!(benches); 