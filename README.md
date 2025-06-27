# Slice - LSM Key-Value Store

A high-performance, concurrent LSM (Log-Structured Merge) tree-based key-value store implemented in Rust, designed for high write throughput and concurrent access.

## Features

- **High Write Concurrency**: Optimized for concurrent write operations using async/await and efficient locking strategies
- **LSM Tree Architecture**: Multi-level storage with automatic compaction
- **LRU Cache**: Thread-safe LRU cache for SSTable entries to dramatically reduce I/O
- **Bloom Filters**: Fast negative lookups to eliminate unnecessary disk reads
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery
- **Concurrent Memtables**: Active and immutable memtables for non-blocking writes
- **Background Compaction**: Automatic SSTable merging and cleanup
- **Checksums**: Data integrity verification for all persistent storage
- **Async/Await Support**: Fully asynchronous API using Tokio

## Architecture

```
┌─────────────┐     ┌─────────────┐
│   Writes    │     │    Reads    │
└─────┬───────┘     └─────┬───────┘
      │                   │
      ▼                   ▼
┌─────────────┐    ┌─────────────┐
│     WAL     │◄───┤ Active      │───► Check first
│   (Durability)   │ MemTable    │
└─────────────┘    └─────┬───────┘
                         │ (when full)
                         ▼
                   ┌─────────────┐
                   │ Immutable   │───► Then check
                   │ MemTables   │
                   └─────┬───────┘
                         │ (background flush)
                         ▼
                   ┌─────────────┐     ┌─────────────┐
                   │   Level 0   │◄────┤ LRU Cache   │
                   │  SSTables   │     │ (Hot Data)  │
                   │   + Bloom   │     └─────────────┘
                   │   Filters   │           ▲
                   └─────┬───────┘           │
                         │ (compaction)      │
                         ▼                   │
                   ┌─────────────┐           │
                   │   Level 1+  │───────────┘
                   │  SSTables   │ 1. Bloom filter check
                   │   + Bloom   │ 2. Cache check  
                   │   Filters   │ 3. Disk read (if needed)
                   └─────────────┘
```

## Usage

### Basic Operations

```rust
use slice::LSMTree;

#[tokio::main]
async fn main() -> slice::Result<()> {
    // Initialize the LSM tree with default cache (1000 entries)
    let lsm = LSMTree::new("./data").await?;
    
    // Or initialize with custom cache size
    let lsm = LSMTree::new_with_cache_size("./data", 5000).await?;
    
    // Put key-value pairs
    lsm.put(b"user:1", b"Alice").await?;
    lsm.put(b"user:2", b"Bob").await?;
    
    // Get values
    if let Some(value) = lsm.get(b"user:1").await? {
        println!("user:1 = {}", String::from_utf8_lossy(&value));
    }
    
    // Delete keys
    lsm.delete(b"user:2").await?;
    
    // Manual flush and compaction
    lsm.flush().await?;
    lsm.compact().await?;
    
    // Check cache statistics
    let stats = lsm.stats();
    stats.print(); // Shows cache hit ratio and size
    
    // Clear cache if needed
    lsm.clear_cache();
    
    Ok(())
}
```

### Concurrent Writes

```rust
use slice::LSMTree;
use std::sync::Arc;

#[tokio::main]
async fn main() -> slice::Result<()> {
    let lsm = Arc::new(LSMTree::new("./data").await?);
    
    // Spawn multiple concurrent writers
    let mut handles = Vec::new();
    
    for writer_id in 0..10 {
        let lsm_clone = lsm.clone();
        let handle = tokio::spawn(async move {
            for i in 0..1000 {
                let key = format!("writer{}:key{}", writer_id, i);
                let value = format!("value_{}_{}", writer_id, i);
                lsm_clone.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
        });
        handles.push(handle);
    }
    
    // Wait for all writers to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    println!("All concurrent writes completed!");
    Ok(())
}
```

## Performance

The LSM tree is optimized for:

- **High Write Throughput**: Writes go to memory first (memtable) then are batched to disk
- **Fast Reads**: LRU cache provides ~20x speedup for frequently accessed data
- **Ultra-Fast Negative Lookups**: Bloom filters eliminate disk I/O for non-existent keys (~2µs per lookup)
- **Write Amplification**: Minimized through efficient compaction strategies
- **Concurrent Access**: Multiple readers and writers can operate simultaneously
- **Memory Efficiency**: Configurable memtable sizes and cache capacity

### Benchmarks

#### Standard Industry Benchmarks

We've implemented comprehensive benchmarks that mirror industry standards:

```bash
# Run standardized YCSB and db_bench workloads
cargo bench --bench standard_benchmarks

# Run specific benchmark groups
cargo bench --bench standard_benchmarks -- "YCSB"     # Yahoo! Cloud Serving Benchmark
cargo bench --bench standard_benchmarks -- "DBBench"  # RocksDB/LevelDB standard benchmarks
cargo bench --bench standard_benchmarks -- "Industry" # Industry comparison workloads
```

**Performance Results vs Industry Standards:**
- **Read Performance**: 2.8-3.9M ops/sec (competitive with RocksDB)
- **Write Performance**: 228-329K ops/sec (good LSM characteristics)
- **Mixed Workloads**: 405K-1.8M ops/sec (scales with read proportion)
- **YCSB Compliance**: Full implementation of standard workloads A, B, C

See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for detailed analysis.

#### Custom Performance Benchmarks

Run the included benchmarks to test performance on your system:

```bash
cargo bench
```

Example benchmark categories:
- Sequential writes
- Concurrent writes (1, 2, 4, 8, 16 writers)
- Read performance
- Mixed read/write workloads
- Cache performance (cached vs uncached reads)
- LRU eviction behavior
- Bloom filter effectiveness (negative lookup performance)

## Configuration

Key configuration parameters:

- **Cache Size**: Default 1000 entries (configurable via `new_with_cache_size`)
- **Memtable Size**: Default 64MB (configurable)
- **Max Levels**: Default 7 levels
- **Compaction Frequency**: Every 10 seconds
- **Flush Frequency**: Every 1 second

## File Structure

```
data/
├── wal_000000.log         # Write-ahead log files (sequential)
├── wal_000001.log         # Multiple WAL files for safety
├── sstable_000001.db      # SSTable data files
├── sstable_000001.idx     # SSTable index files
├── sstable_000001.bloom   # Bloom filter files
├── sstable_000002.db
├── sstable_000002.idx
├── sstable_000002.bloom
└── ...
```

## Development

### Building

```bash
cargo build --release
```

### Testing

```bash
cargo test
```

#### Comprehensive Test Coverage

The test suite includes critical durability and crash recovery tests:

- **Basic Operations**: Put, get, delete, and updates
- **WAL Recovery (Unflushed Memtable)**: Simulates crash before background flush completes
- **WAL Recovery with Deletions**: Ensures tombstones are properly recovered  
- **Concurrent Writes During Recovery**: Tests system stability during recovery
- **Crash Safety**: Verifies no data loss in various failure scenarios

### Running the Demo

```bash
cargo run
```

### Running Benchmarks

```bash
cargo bench
```

## Technical Details

### Components

1. **MemTable**: In-memory sorted structure (BTreeMap) with concurrent access
2. **WAL**: Write-ahead log for durability with checksums
3. **SSTable**: Persistent sorted storage with bloom filters for fast negative lookups
4. **Bloom Filter**: Probabilistic data structure to eliminate unnecessary disk reads
5. **LRU Cache**: In-memory cache for frequently accessed SSTable entries
6. **Storage Manager**: Manages multiple SSTable levels with automatic compaction
7. **Compaction Manager**: Background compaction and cleanup

### Concurrency Model

- **Readers**: Lock-free reads from memtables, shared locks for SSTables
- **Writers**: Minimal locking with async coordination
- **Background Tasks**: Separate tasks for flushing and compaction
- **WAL**: Synchronized writes for durability

### Durability Guarantees

- All writes are logged to WAL before acknowledgment
- WAL entries have checksums for corruption detection  
- **Multiple WAL files**: New WAL created when memtable becomes immutable
- **Safe WAL cleanup**: WAL files only removed AFTER successful disk flush and sync
- **Crash safety**: No data loss even if crash occurs during memtable flushing
- Automatic recovery from multiple WAL files on startup
- SSTable files include checksums for integrity

## Dependencies

- `tokio`: Async runtime
- `parking_lot`: High-performance synchronization primitives
- `serde` + `bincode`: Serialization
- `crc32fast`: Fast checksums
- `crossbeam`: Concurrent data structures
- `criterion`: Benchmarking

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Future Improvements

- [ ] Bloom filters for faster negative lookups
- [ ] Block-based SSTable format
- [ ] Compression support
- [ ] Range queries and iterators
- [ ] Snapshot isolation
- [ ] Metrics and monitoring
- [ ] Configurable compaction strategies
- [ ] Multi-threaded compaction 