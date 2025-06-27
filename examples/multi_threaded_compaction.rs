use slice::{LSMTree, Result};
use slice::compaction::CompactionConfig;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Multi-threaded Compaction Demo");
    println!("===============================");
    
    let temp_dir = TempDir::new().unwrap();
    
    // Configure multi-threaded compaction
    let compaction_config = CompactionConfig {
        max_concurrent_compactions: 4,  // Allow up to 4 concurrent compactions
        merge_parallelism: 8,           // Use 8 parallel merge tasks
        max_sstables_per_job: 5,        // Limit job size for better parallelism
        enable_parallel_merge: true,    // Enable parallel merging
    };
    
    // Create LSM tree with custom compaction configuration
    let lsm = LSMTree::new_with_config(
        temp_dir.path(),
        1000,  // cache size
        compaction_config
    ).await?;
    
    println!("Created LSM tree with multi-threaded compaction");
    println!("Configuration:");
    println!("  Max concurrent compactions: {}", lsm.compaction_config().max_concurrent_compactions);
    println!("  Merge parallelism: {}", lsm.compaction_config().merge_parallelism);
    println!("  Max SSTables per job: {}", lsm.compaction_config().max_sstables_per_job);
    println!("  Parallel merge enabled: {}", lsm.compaction_config().enable_parallel_merge);
    println!();
    
    // Insert a large amount of data to trigger compactions
    println!("Inserting data to trigger compactions...");
    for i in 0..10000 {
        let key = format!("key_{:06}", i);
        let value = format!("value_{:06}_with_some_longer_content_to_make_it_bigger", i);
        lsm.put(key.as_bytes(), value.as_bytes()).await?;
        
        if i % 1000 == 0 {
            println!("Inserted {} records", i);
            
            // Force flush and compaction periodically
            lsm.flush().await?;
            lsm.compact().await?;
            
            // Show current stats
            let stats = lsm.stats();
            println!("Current stats:");
            println!("  Total compactions: {}", stats.compaction_stats.total_compactions);
            println!("  Active compactions: {}", stats.compaction_stats.concurrent_compactions);
            println!("  Parallel merges: {}", stats.compaction_stats.parallel_merges);
            println!("  Bytes compacted: {:.2} MB", 
                    stats.compaction_stats.bytes_compacted as f64 / 1024.0 / 1024.0);
            println!();
        }
    }
    
    // Force more compactions
    println!("Triggering final compactions...");
    for _ in 0..5 {
        lsm.compact().await?;
        sleep(Duration::from_millis(100)).await;
    }
    
    // Wait a bit for background compactions to complete
    sleep(Duration::from_secs(2)).await;
    
    // Show final statistics
    println!("Final Statistics:");
    println!("================");
    let final_stats = lsm.stats();
    final_stats.print();
    
    // Verify data integrity
    println!("\nVerifying data integrity...");
    let mut verified_count = 0;
    for i in 0..1000 {  // Check first 1000 entries
        let key = format!("key_{:06}", i);
        let expected_value = format!("value_{:06}_with_some_longer_content_to_make_it_bigger", i);
        
        if let Some(actual_value) = lsm.get(key.as_bytes()).await? {
            if actual_value == expected_value.as_bytes() {
                verified_count += 1;
            } else {
                println!("Data mismatch for key: {}", key);
            }
        } else {
            println!("Missing key: {}", key);
        }
    }
    
    println!("Verified {} out of 1000 entries", verified_count);
    
    if verified_count == 1000 {
        println!("✅ All data verified successfully!");
        println!("✅ Multi-threaded compaction working correctly!");
    } else {
        println!("❌ Some data verification failed");
    }
    
    Ok(())
} 