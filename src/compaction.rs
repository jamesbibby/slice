use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use crate::error::Result;
use crate::storage::StorageManager;
use crate::sstable::SSTable;
use crate::memtable::Value;

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

#[derive(Debug)]
pub struct CompactionManager {
    storage: Arc<StorageManager>,
    compaction_tx: mpsc::UnboundedSender<CompactionJob>,
    _compaction_handle: JoinHandle<()>,
}

impl CompactionManager {
    pub fn new(storage: Arc<StorageManager>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let storage_clone = storage.clone();
        
        let handle = tokio::spawn(async move {
            Self::compaction_worker(storage_clone, rx).await;
        });
        
        Self {
            storage,
            compaction_tx: tx,
            _compaction_handle: handle,
        }
    }
    
    pub fn schedule_compaction(&self, level: usize) -> Result<()> {
        let sstables = self.storage.get_sstables_for_level(level);
        if sstables.len() > 1 {
            let job = CompactionJob { level, sstables };
            self.compaction_tx.send(job).map_err(|_| {
                crate::error::SliceError::Compaction("Failed to schedule compaction".to_string())
            })?;
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
    
    async fn compaction_worker(
        storage: Arc<StorageManager>,
        mut rx: mpsc::UnboundedReceiver<CompactionJob>,
    ) {
        while let Some(job) = rx.recv().await {
            if let Err(e) = Self::execute_compaction(&storage, job).await {
                eprintln!("Compaction failed: {}", e);
            }
        }
    }
    
    async fn execute_compaction(
        storage: &StorageManager,
        job: CompactionJob,
    ) -> Result<()> {
        let CompactionJob { level, sstables } = job;
        
        if sstables.is_empty() {
            return Ok(());
        }
        
        println!("Starting compaction for level {} with {} SSTables", level, sstables.len());
        
        // Collect all entries from SSTables in sorted order
        let merged_entries = Self::merge_sstables(&sstables).await?;
        
        if merged_entries.is_empty() {
            return Ok(());
        }
        
        // Create new SSTable at the next level
        let target_level = if level + 1 <= storage.max_level { level + 1 } else { level };
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
        
        println!(
            "Completed compaction: {} SSTables -> 1 SSTable (level {} -> {})",
            sstables.len(),
            level,
            target_level
        );
        
        Ok(())
    }
    
    async fn merge_sstables(sstables: &[Arc<SSTable>]) -> Result<Vec<(Vec<u8>, Value)>> {
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
} 