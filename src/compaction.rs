use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use futures::future::join_all;
use crate::error::Result;
use crate::storage::StorageManager;
use crate::sstable::SSTable;
use crate::memtable::Value;

/// Configuration for compaction behavior
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Number of parallel compaction threads (default: 2)
    pub max_concurrent_compactions: usize,
    /// Number of parallel merge operations within a single compaction (default: 4)
    pub merge_parallelism: usize,
    /// Maximum number of SSTables to compact in a single job (default: 10)
    pub max_sstables_per_job: usize,
    /// Enable parallel merging of SSTable chunks (default: true)
    pub enable_parallel_merge: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_compactions: 2,
            merge_parallelism: 4,
            max_sstables_per_job: 10,
            enable_parallel_merge: true,
        }
    }
}

#[derive(Debug)]
pub struct CompactionJob {
    pub level: usize,
    pub sstables: Vec<Arc<SSTable>>,
}

#[derive(Debug, PartialEq, Eq)]
struct MergeEntry {
    key: Vec<u8>,
    value: Value,
    timestamp: u64,
    sstable_id: u64,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by key
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => {
                // If keys are equal, prioritize by timestamp (newer first)
                // Then by sstable_id (newer first)
                other.timestamp.cmp(&self.timestamp)
                    .then_with(|| other.sstable_id.cmp(&self.sstable_id))
            },
            other => other, // Reverse ordering for min-heap behavior
        }
    }
}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Statistics for compaction operations
#[derive(Debug, Default)]
pub struct CompactionStats {
    pub total_compactions: u64,
    pub concurrent_compactions: u64,
    pub bytes_compacted: u64,
    pub sstables_compacted: u64,
    pub parallel_merges: u64,
}

#[derive(Debug)]
pub struct CompactionManager {
    storage: Arc<StorageManager>,
    config: CompactionConfig,
    compaction_tx: mpsc::UnboundedSender<CompactionJob>,
    _compaction_handles: Vec<JoinHandle<()>>,
    stats: Arc<parking_lot::Mutex<CompactionStats>>,
}

impl CompactionManager {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        Self::new_with_config(storage, CompactionConfig::default())
    }
    
    pub fn new_with_config(storage: Arc<StorageManager>, config: CompactionConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let storage_clone = storage.clone();
        let config_clone = config.clone();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_compactions));
        let stats = Arc::new(parking_lot::Mutex::new(CompactionStats::default()));
        
        // Create a shared receiver using Arc and Mutex for multiple workers
        let rx_shared = Arc::new(tokio::sync::Mutex::new(rx));
        
        // Create multiple worker tasks for parallel compaction
        let mut handles = Vec::new();
        for worker_id in 0..config.max_concurrent_compactions {
            let storage_worker = storage_clone.clone();
            let config_worker = config_clone.clone();
            let semaphore_worker = semaphore.clone();
            let stats_worker = stats.clone();
            let rx_worker = rx_shared.clone();
            
            let handle = tokio::spawn(async move {
                Self::compaction_worker(
                    worker_id,
                    storage_worker,
                    config_worker,
                    semaphore_worker,
                    stats_worker,
                    rx_worker,
                ).await;
            });
            handles.push(handle);
        }
        
        Self {
            storage,
            config,
            compaction_tx: tx,
            _compaction_handles: handles,
            stats,
        }
    }
    
    pub fn schedule_compaction(&self, level: usize) -> Result<()> {
        let sstables = self.storage.get_sstables_for_level(level);
        if sstables.len() > 1 {
            // Split large compaction jobs into smaller chunks for better parallelism
            let chunks = sstables.chunks(self.config.max_sstables_per_job);
            for chunk in chunks {
                if chunk.len() > 1 {
                    let job = CompactionJob { 
                        level, 
                        sstables: chunk.to_vec() 
                    };
                    self.compaction_tx.send(job).map_err(|_| {
                        crate::error::SliceError::Compaction("Failed to schedule compaction".to_string())
                    })?;
                }
            }
        }
        Ok(())
    }
    
    pub fn check_and_schedule_compactions(&self) -> Result<()> {
        for level in 0..self.storage.max_level {
            if self.storage.needs_compaction(level) {
                self.schedule_compaction(level)?;
            }
        }
        Ok(())
    }
    
    pub fn stats(&self) -> CompactionStats {
        let stats_guard = self.stats.lock();
        CompactionStats {
            total_compactions: stats_guard.total_compactions,
            concurrent_compactions: stats_guard.concurrent_compactions,
            bytes_compacted: stats_guard.bytes_compacted,
            sstables_compacted: stats_guard.sstables_compacted,
            parallel_merges: stats_guard.parallel_merges,
        }
    }
    
    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }
    
    async fn compaction_worker(
        worker_id: usize,
        storage: Arc<StorageManager>,
        config: CompactionConfig,
        semaphore: Arc<Semaphore>,
        stats: Arc<parking_lot::Mutex<CompactionStats>>,
        rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<CompactionJob>>>,
    ) {
        println!("Compaction worker {} started", worker_id);
        
        loop {
            let job = {
                let mut rx_guard = rx.lock().await;
                match rx_guard.recv().await {
                    Some(job) => job,
                    None => {
                        eprintln!("Compaction worker {} shutting down - channel closed", worker_id);
                        break;
                    }
                }
            };
            
            // Acquire semaphore permit to limit concurrent compactions
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    eprintln!("Compaction worker {} shutting down", worker_id);
                    break;
                }
            };
            
            {
                let mut stats_guard = stats.lock();
                stats_guard.concurrent_compactions += 1;
            }
            
            if let Err(e) = Self::execute_compaction(&storage, &config, &stats, job).await {
                eprintln!("Compaction failed in worker {}: {}", worker_id, e);
            }
            
            {
                let mut stats_guard = stats.lock();
                stats_guard.concurrent_compactions = stats_guard.concurrent_compactions.saturating_sub(1);
            }
        }
        
        println!("Compaction worker {} stopped", worker_id);
    }
    
    async fn execute_compaction(
        storage: &StorageManager,
        config: &CompactionConfig,
        stats: &Arc<parking_lot::Mutex<CompactionStats>>,
        job: CompactionJob,
    ) -> Result<()> {
        let CompactionJob { level, sstables } = job;
        
        if sstables.is_empty() {
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        let total_size: u64 = sstables.iter().map(|s| s.size()).sum();
        
        println!(
            "Starting compaction for level {} with {} SSTables ({:.2} MB)", 
            level, 
            sstables.len(),
            total_size as f64 / 1024.0 / 1024.0
        );
        
        // Collect all entries from SSTables in sorted order
        let merged_entries = if config.enable_parallel_merge {
            Self::merge_sstables_parallel(&sstables, config.merge_parallelism).await?
        } else {
            Self::merge_sstables_sequential(&sstables).await?
        };
        
        if merged_entries.is_empty() {
            return Ok(());
        }
        
        // Create new SSTable at the next level
        let target_level = if level < storage.max_level { level + 1 } else { level };
        let _new_sstable = storage.add_sstable(target_level, merged_entries)?;
        
        // Remove old SSTables
        let old_ids: Vec<u64> = sstables.iter().map(|s| s.id).collect();
        storage.remove_sstables(level, &old_ids)?;
        
        // Clean up old files
        for sstable in &sstables {
            let _ = std::fs::remove_file(&sstable.path);
            let index_path = sstable.path.with_extension("idx");
            let _ = std::fs::remove_file(&index_path);
        }
        
        let duration = start_time.elapsed();
        
        // Update statistics
        {
            let mut stats_guard = stats.lock();
            stats_guard.total_compactions += 1;
            stats_guard.bytes_compacted += total_size;
            stats_guard.sstables_compacted += sstables.len() as u64;
            if config.enable_parallel_merge {
                stats_guard.parallel_merges += 1;
            }
        }
        
        println!(
            "Completed compaction: {} SSTables -> 1 SSTable (level {} -> {}) in {:.2}s ({:.2} MB/s)",
            sstables.len(),
            level,
            target_level,
            duration.as_secs_f64(),
            (total_size as f64 / 1024.0 / 1024.0) / duration.as_secs_f64()
        );
        
        Ok(())
    }
    
    /// Parallel merge implementation using multiple tasks
    async fn merge_sstables_parallel(
        sstables: &[Arc<SSTable>], 
        parallelism: usize
    ) -> Result<Vec<(Vec<u8>, Value)>> {
        if sstables.len() <= 1 {
            return Self::merge_sstables_sequential(sstables).await;
        }
        
        // For small numbers of SSTables, use sequential merge
        if sstables.len() <= parallelism {
            return Self::merge_sstables_sequential(sstables).await;
        }
        
        // Split SSTables into chunks for parallel processing
        let chunk_size = (sstables.len() + parallelism - 1) / parallelism;
        let chunks: Vec<_> = sstables.chunks(chunk_size).collect();
        
        // Process chunks in parallel
        let merge_tasks: Vec<_> = chunks.into_iter().map(|chunk| {
            let chunk_vec = chunk.to_vec();
            tokio::spawn(async move {
                Self::merge_sstables_sequential(&chunk_vec).await
            })
        }).collect();
        
        // Wait for all merge tasks to complete
        let chunk_results = join_all(merge_tasks).await;
        
        // Collect results and handle errors
        let mut chunk_entries = Vec::new();
        for result in chunk_results {
            match result {
                Ok(Ok(entries)) => chunk_entries.push(entries),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(crate::error::SliceError::Compaction(
                    format!("Parallel merge task failed: {}", e)
                )),
            }
        }
        
        // Merge the pre-sorted chunks
        Self::merge_sorted_chunks(chunk_entries)
    }
    
    /// Sequential merge implementation (original algorithm)
    async fn merge_sstables_sequential(sstables: &[Arc<SSTable>]) -> Result<Vec<(Vec<u8>, Value)>> {
        let mut heap = BinaryHeap::new();
        let mut iterators = Vec::new();
        
        // Initialize iterators for all SSTables
        for sstable in sstables {
            match sstable.iter() {
                Ok(mut iter) => {
                    if let Some(Ok(entry)) = iter.next() {
                        heap.push(Reverse(MergeEntry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                            timestamp: entry.timestamp,
                            sstable_id: sstable.id,
                        }));
                        iterators.push((sstable.id, iter));
                    }
                },
                Err(e) => {
                    eprintln!("Warning: Failed to create iterator for SSTable {}: {}", sstable.id, e);
                }
            }
        }
        
        let mut result = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        
        // Merge entries in sorted order
        while let Some(Reverse(entry)) = heap.pop() {
            // Skip duplicate keys (keep the newest entry based on timestamp)
            if last_key.as_ref() != Some(&entry.key) {
                // Only include non-tombstone values in the result
                if !entry.value.is_tombstone() {
                    result.push((entry.key.clone(), entry.value));
                }
                last_key = Some(entry.key.clone());
            }
            
            // Advance the iterator that provided this entry
            if let Some((_, ref mut iter)) = iterators.iter_mut().find(|(id, _)| *id == entry.sstable_id) {
                if let Some(Ok(next_entry)) = iter.next() {
                    heap.push(Reverse(MergeEntry {
                        key: next_entry.key.clone(),
                        value: next_entry.value.clone(),
                        timestamp: next_entry.timestamp,
                        sstable_id: entry.sstable_id,
                    }));
                }
            }
        }
        
        Ok(result)
    }
    
    /// Merge multiple pre-sorted chunks into a single sorted result
    fn merge_sorted_chunks(chunks: Vec<Vec<(Vec<u8>, Value)>>) -> Result<Vec<(Vec<u8>, Value)>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        
        if chunks.len() == 1 {
            return Ok(chunks.into_iter().next().unwrap());
        }
        
        // Simple merge approach without heap (since we only have a few chunks)
        // Convert chunks to iterators
        let mut iterators: Vec<_> = chunks.into_iter()
            .map(|chunk| chunk.into_iter().peekable())
            .collect();
        
        let mut result = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;
        
        // Repeatedly find the iterator with the smallest key
        loop {
            let mut min_key: Option<Vec<u8>> = None;
            let mut min_idx = 0;
            
            // Find the iterator with the smallest current key
            for (idx, iter) in iterators.iter_mut().enumerate() {
                if let Some((key, _)) = iter.peek() {
                    if min_key.is_none() || key < min_key.as_ref().unwrap() {
                        min_key = Some(key.clone());
                        min_idx = idx;
                    }
                }
            }
            
            // If no more entries, we're done
            if min_key.is_none() {
                break;
            }
            
            // Take the entry from the selected iterator
            if let Some((key, value)) = iterators[min_idx].next() {
                // Skip duplicate keys (keep the first occurrence)
                if last_key.as_ref() != Some(&key) {
                    if !value.is_tombstone() {
                        result.push((key.clone(), value));
                    }
                    last_key = Some(key);
                }
            }
        }
        
        Ok(result)
    }
} 