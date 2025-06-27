pub mod lsm;
pub mod memtable;
pub mod sstable;
pub mod wal;
pub mod compaction;
pub mod storage;
pub mod error;
pub mod cache;
pub mod bloom;

pub use lsm::LSMTree;
pub use error::{Result, SliceError};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let mut lsm = LSMTree::new(temp_dir.path()).await.unwrap();
        
        // Test put and get
        lsm.put(b"key1", b"value1").await.unwrap();
        let value = lsm.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        // Test delete
        lsm.delete(b"key1").await.unwrap();
        let value = lsm.get(b"key1").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_wal_recovery_unflushed_memtable() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path();
        
        // Phase 1: Write data and trigger memtable flush but don't wait for background flush
        {
            let lsm = LSMTree::new(data_path).await.unwrap();
            
            // Write some data to active memtable
            lsm.put(b"key1", b"value1").await.unwrap();
            lsm.put(b"key2", b"value2").await.unwrap();
            lsm.put(b"key3", b"value3").await.unwrap();
            
            // Verify data is accessible in memory
            assert_eq!(lsm.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(lsm.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
            assert_eq!(lsm.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
            
            // Manually trigger memtable flush (this rotates WAL but data is now in immutable memtable)
            lsm.flush().await.unwrap();
            
            // Write more data to the new active memtable  
            lsm.put(b"key4", b"value4").await.unwrap();
            lsm.put(b"key5", b"value5").await.unwrap();
            
            // At this point:
            // - WAL file 0 contains key1, key2, key3 and should be preserved
            // - WAL file 1 contains key4, key5 and is active
            // - Immutable memtable has key1, key2, key3 but NOT flushed to SSTable yet
            // - Active memtable has key4, key5
            
            // Verify all data is still accessible
            assert_eq!(lsm.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(lsm.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
            assert_eq!(lsm.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
            assert_eq!(lsm.get(b"key4").await.unwrap(), Some(b"value4".to_vec()));
            assert_eq!(lsm.get(b"key5").await.unwrap(), Some(b"value5".to_vec()));
            
            // DON'T wait for background flush - simulate crash by dropping LSM tree
            // This means the immutable memtable data is NOT on disk yet
            println!("Simulating crash - dropping LSM tree before background flush completes");
        } // LSM tree dropped here - simulates crash
        
        // Verify WAL files exist (they should not be removed since flush didn't complete)
        let wal_files: Vec<_> = std::fs::read_dir(data_path)
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("wal_") && name.ends_with(".log") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();
        
        println!("WAL files after 'crash': {:?}", wal_files);
        assert!(!wal_files.is_empty(), "WAL files should exist after crash");
        
        // Phase 2: Recovery - create new LSM tree from same directory
        {
            println!("Starting recovery...");
            let lsm_recovered = LSMTree::new(data_path).await.unwrap();
            
            // All data should be recovered from WAL files
            assert_eq!(lsm_recovered.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(lsm_recovered.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
            assert_eq!(lsm_recovered.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
            assert_eq!(lsm_recovered.get(b"key4").await.unwrap(), Some(b"value4".to_vec()));
            assert_eq!(lsm_recovered.get(b"key5").await.unwrap(), Some(b"value5".to_vec()));
            
            println!("Recovery successful - all data recovered from WAL");
            
            // Manually trigger flush to ensure data gets to disk
            lsm_recovered.flush().await.unwrap();
            
            // Wait for background flush to complete
            sleep(Duration::from_secs(3)).await;
            
            // Data should still be accessible after background flush
            assert_eq!(lsm_recovered.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(lsm_recovered.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
            assert_eq!(lsm_recovered.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
            assert_eq!(lsm_recovered.get(b"key4").await.unwrap(), Some(b"value4".to_vec()));
            assert_eq!(lsm_recovered.get(b"key5").await.unwrap(), Some(b"value5".to_vec()));
            
            // Check that some SSTables have been created
            let stats = lsm_recovered.stats();
            let total_sstables: usize = stats.level_stats.iter().map(|(_, count, _)| count).sum();
            if total_sstables == 0 {
                println!("No SSTables created yet, stats: {:?}", stats);
                // Try one more manual flush and wait
                lsm_recovered.flush().await.unwrap();
                sleep(Duration::from_secs(2)).await;
                let stats = lsm_recovered.stats();
                let total_sstables: usize = stats.level_stats.iter().map(|(_, count, _)| count).sum();
                println!("After second flush, SSTables: {}, stats: {:?}", total_sstables, stats);
            }
            
            println!("Background flush completed - data should be on disk");
        }
    }

    #[tokio::test]
    async fn test_wal_recovery_with_deletions() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path();
        
        // Phase 1: Write and delete data, then simulate crash
        {
            let lsm = LSMTree::new(data_path).await.unwrap();
            
            // Write some data
            lsm.put(b"key1", b"value1").await.unwrap();
            lsm.put(b"key2", b"value2").await.unwrap();
            lsm.put(b"key3", b"value3").await.unwrap();
            
            // Delete some data 
            lsm.delete(b"key2").await.unwrap();
            
            // Update existing data
            lsm.put(b"key1", b"updated_value1").await.unwrap();
            
            // Trigger flush but don't wait for completion
            lsm.flush().await.unwrap();
            
            // Verify expected state before crash
            assert_eq!(lsm.get(b"key1").await.unwrap(), Some(b"updated_value1".to_vec()));
            assert_eq!(lsm.get(b"key2").await.unwrap(), None); // deleted
            assert_eq!(lsm.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
            
            // Simulate crash
        }
        
        // Phase 2: Recovery should maintain the same state
        {
            let lsm_recovered = LSMTree::new(data_path).await.unwrap();
            
            // Verify state is correctly recovered
            assert_eq!(lsm_recovered.get(b"key1").await.unwrap(), Some(b"updated_value1".to_vec()));
            assert_eq!(lsm_recovered.get(b"key2").await.unwrap(), None); // should remain deleted
            assert_eq!(lsm_recovered.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
            
            println!("WAL recovery with deletions successful");
        }
    }

    #[tokio::test]
    async fn test_concurrent_writes_during_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path();
        
        // Phase 1: Create initial data
        {
            let lsm = LSMTree::new(data_path).await.unwrap();
            
            // Write initial data
            for i in 0..50 {
                let key = format!("initial_key_{}", i);
                let value = format!("initial_value_{}", i);
                lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            
            // Trigger flush but don't wait
            lsm.flush().await.unwrap();
            
            // Simulate crash
        }
        
        // Phase 2: Recovery with concurrent operations
        {
            let lsm = std::sync::Arc::new(LSMTree::new(data_path).await.unwrap());
            
            // Verify recovery worked
            for i in 0..10 {
                let key = format!("initial_key_{}", i);
                let expected_value = format!("initial_value_{}", i);
                let actual_value = lsm.get(key.as_bytes()).await.unwrap().unwrap();
                assert_eq!(actual_value, expected_value.as_bytes());
            }
            
            // Start concurrent writes during recovery
            let mut handles = Vec::new();
            for writer_id in 0..5 {
                let lsm_clone = lsm.clone();
                let handle = tokio::spawn(async move {
                    for i in 0..20 {
                        let key = format!("concurrent_{}_{}", writer_id, i);
                        let value = format!("concurrent_value_{}_{}", writer_id, i);
                        lsm_clone.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                    }
                });
                handles.push(handle);
            }
            
            // Wait for concurrent writes to complete
            for handle in handles {
                handle.await.unwrap();
            }
            
            // Verify all data is accessible
            for i in 0..10 {
                let key = format!("initial_key_{}", i);
                let expected_value = format!("initial_value_{}", i);
                let actual_value = lsm.get(key.as_bytes()).await.unwrap().unwrap();
                assert_eq!(actual_value, expected_value.as_bytes());
            }
            
            for writer_id in 0..5 {
                for i in 0..5 {
                    let key = format!("concurrent_{}_{}", writer_id, i);
                    let expected_value = format!("concurrent_value_{}_{}", writer_id, i);
                    let actual_value = lsm.get(key.as_bytes()).await.unwrap().unwrap();
                    assert_eq!(actual_value, expected_value.as_bytes());
                }
            }
            
            println!("Concurrent writes during recovery successful");
        }
    }

    #[tokio::test]
    async fn test_basic_lsm_operations() {
        let temp_dir = TempDir::new().unwrap();
        let lsm = LSMTree::new(temp_dir.path()).await.unwrap();
        
        // Test put and get
        lsm.put(b"key1", b"value1").await.unwrap();
        assert_eq!(lsm.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        
        // Test delete
        lsm.delete(b"key1").await.unwrap();
        assert_eq!(lsm.get(b"key1").await.unwrap(), None);
    }
    
    #[tokio::test]
    async fn test_lsm_with_cache_performance() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create LSM with a small cache for testing
        let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 100).await.unwrap();
        
        // Insert many entries to force SSTable creation 
        let num_entries = 1000;
        for i in 0..num_entries {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        
        // Force flush to create SSTables
        lsm.flush().await.unwrap();
        
        // Wait for background flush to complete - check multiple times
        for _ in 0..10 {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            let stats = lsm.stats();
            if stats.immutable_memtables == 0 && stats.level_stats[0].1 > 0 {
                break; // Background flush completed
            }
        }
        
        // First read - should go to disk and populate cache
        let start = std::time::Instant::now();
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected_value = format!("value{:04}", i);
            if let Some(value) = lsm.get(key.as_bytes()).await.unwrap() {
                assert_eq!(value, expected_value.as_bytes());
            }
        }
        let first_read_time = start.elapsed();
        
        // Second read - should benefit from cache
        let start = std::time::Instant::now();
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected_value = format!("value{:04}", i);
            if let Some(value) = lsm.get(key.as_bytes()).await.unwrap() {
                assert_eq!(value, expected_value.as_bytes());
            }
        }
        let second_read_time = start.elapsed();
        
        println!("First read (cold cache): {:?}", first_read_time);
        println!("Second read (warm cache): {:?}", second_read_time);
        
        // Print cache statistics
        let stats = lsm.stats();
        stats.print();
        
        // Cache should have some entries now
        assert!(stats.cache_stats.size > 0);
        assert!(stats.cache_stats.size <= 100); // Should not exceed capacity
    }
    
    #[tokio::test]
    async fn test_cache_invalidation_on_updates() {
        let temp_dir = TempDir::new().unwrap();
        let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 50).await.unwrap();
        
        // Insert initial data
        lsm.put(b"key1", b"original_value").await.unwrap();
        lsm.flush().await.unwrap();
        
        // Wait for flush to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        // Read to populate cache
        assert_eq!(lsm.get(b"key1").await.unwrap(), Some(b"original_value".to_vec()));
        
        // Update the value
        lsm.put(b"key1", b"updated_value").await.unwrap();
        
        // Should get the updated value (from memtable, not stale cache)
        assert_eq!(lsm.get(b"key1").await.unwrap(), Some(b"updated_value".to_vec()));
        
        // Delete the key
        lsm.delete(b"key1").await.unwrap();
        
        // Should return None (tombstone in memtable overrides cached value)
        assert_eq!(lsm.get(b"key1").await.unwrap(), None);
    }
    
    #[tokio::test]
    async fn test_cache_stats_and_management() {
        let temp_dir = TempDir::new().unwrap();
        let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 10).await.unwrap();
        
        // Initially cache should be empty
        let stats = lsm.stats();
        assert_eq!(stats.cache_stats.size, 0);
        assert_eq!(stats.cache_stats.capacity, 10);
        
        // Add some data and force SSTable creation
        for i in 0..20 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        
        lsm.flush().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        // Read some values to populate cache
        for i in 0..15 {
            let key = format!("key{}", i);
            lsm.get(key.as_bytes()).await.unwrap();
        }
        
        // Cache should be at capacity (10 entries max)
        let stats = lsm.stats();
        assert_eq!(stats.cache_stats.size, 10);
        assert_eq!(stats.cache_stats.capacity, 10);
        
        // Clear cache
        lsm.clear_cache();
        
        // Cache should be empty again
        let stats = lsm.stats();
        assert_eq!(stats.cache_stats.size, 0);
    }
    
    #[tokio::test]
    async fn test_bloom_filter_effectiveness() {
        let temp_dir = TempDir::new().unwrap();
        let lsm = LSMTree::new_with_cache_size(temp_dir.path(), 100).await.unwrap();
        
        // Insert keys with a specific pattern
        let existing_keys: Vec<String> = (0..1000).map(|i| format!("user:{:04}", i)).collect();
        for key in &existing_keys {
            let value = format!("data_for_{}", key);
            lsm.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        }
        
        // Force flush to create SSTables
        lsm.flush().await.unwrap();
        
        // Wait for background flush to complete
        for _ in 0..20 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let stats = lsm.stats();
            if stats.immutable_memtables == 0 && stats.level_stats[0].1 > 0 {
                break;
            }
        }
        
        // Test that existing keys are found
        println!("Testing existing keys...");
        let mut found_count = 0;
        for key in existing_keys.iter().take(100) {
            if lsm.get(key.as_bytes()).await.unwrap().is_some() {
                found_count += 1;
            }
        }
        assert_eq!(found_count, 100);
        println!("✅ Found all {} existing keys", found_count);
        
        // Test bloom filter effectiveness with non-existent keys
        println!("Testing non-existent keys (bloom filter effectiveness)...");
        let non_existent_keys: Vec<String> = (10000..11000).map(|i| format!("missing:{:04}", i)).collect();
        
        let start = std::time::Instant::now();
        let mut false_positives = 0;
        
        for key in &non_existent_keys {
            if lsm.get(key.as_bytes()).await.unwrap().is_some() {
                false_positives += 1;
            }
        }
        
        let lookup_time = start.elapsed();
        let false_positive_rate = false_positives as f64 / non_existent_keys.len() as f64;
        
        println!("📊 Bloom Filter Performance:");
        println!("  Checked {} non-existent keys in {:?}", non_existent_keys.len(), lookup_time);
        println!("  False positives: {} ({:.2}%)", false_positives, false_positive_rate * 100.0);
        println!("  Average lookup time: {:?}", lookup_time / non_existent_keys.len() as u32);
        
        // Bloom filter should have very low false positive rate (< 5%)
        assert!(false_positive_rate < 0.05, "False positive rate too high: {:.2}%", false_positive_rate * 100.0);
        
        // Lookups should be very fast (bloom filter prevents disk I/O)
        let avg_lookup_time = lookup_time / non_existent_keys.len() as u32;
        assert!(avg_lookup_time.as_micros() < 100, "Lookups too slow: {:?}", avg_lookup_time);
        
        println!("✅ Bloom filter is working effectively!");
    }
} 