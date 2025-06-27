use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::time::{Duration, interval};
use crate::error::Result;
use crate::memtable::{MemTable, Value};
use crate::wal::WriteAheadLog;
use crate::storage::StorageManager;
use crate::compaction::{CompactionManager, CompactionConfig, CompactionStats};

const DEFAULT_MEMTABLE_SIZE: usize = 64 * 1024 * 1024; // 64MB
const DEFAULT_MAX_LEVELS: usize = 7;

#[derive(Debug)]
pub struct LSMTree {
    // Active memtable for writes
    active_memtable: Arc<AsyncRwLock<MemTable>>,
    
    // Immutable memtables being flushed
    immutable_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,
    
    // Write-ahead log for durability
    wal: Arc<WriteAheadLog>,
    
    // Storage manager for SSTables
    storage: Arc<StorageManager>,
    
    // Compaction manager
    compaction: Arc<CompactionManager>,
    
    // Configuration
    memtable_size_limit: usize,
    
    // Background flush task handle
    _flush_handle: tokio::task::JoinHandle<()>,
}

impl LSMTree {
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        Self::new_with_cache_size(data_dir, 1000).await // Default 1000 entries
    }
    
    pub async fn new_with_cache_size<P: AsRef<Path>>(data_dir: P, cache_size: usize) -> Result<Self> {
        Self::new_with_config(data_dir, cache_size, CompactionConfig::default()).await
    }
    
    pub async fn new_with_config<P: AsRef<Path>>(
        data_dir: P, 
        cache_size: usize, 
        compaction_config: CompactionConfig
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)?;
        
        // Initialize components
        let wal = Arc::new(WriteAheadLog::new(data_dir)?);
        let storage = Arc::new(StorageManager::new_with_cache_size(data_dir, DEFAULT_MAX_LEVELS, cache_size)?);
        let compaction = Arc::new(CompactionManager::new_with_config(storage.clone(), compaction_config));
        
        let active_memtable = Arc::new(AsyncRwLock::new(MemTable::new(DEFAULT_MEMTABLE_SIZE)));
        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));
        
        // Recover from WAL if needed
        let wal_entries = wal.replay()?;
        if !wal_entries.is_empty() {
            println!("Recovering {} entries from WAL", wal_entries.len());
            let memtable = active_memtable.write().await;
            for entry in wal_entries {
                match entry.value {
                    Value::Some(data) => {
                        memtable.put(entry.key, data)?;
                    },
                    Value::Tombstone => {
                        memtable.delete(entry.key)?;
                    }
                }
            }
        }
        
        // Start background flush task
        let flush_handle = {
            let active_memtable = active_memtable.clone();
            let immutable_memtables = immutable_memtables.clone();
            let storage = storage.clone();
            let compaction = compaction.clone();
            let wal = wal.clone();
            
            tokio::spawn(async move {
                Self::background_flush_worker(
                    active_memtable,
                    immutable_memtables,
                    storage,
                    compaction,
                    wal,
                ).await;
            })
        };
        
        Ok(LSMTree {
            active_memtable,
            immutable_memtables,
            wal,
            storage,
            compaction,
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE,
            _flush_handle: flush_handle,
        })
    }
    
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();
        let value_vec = value.to_vec();
        
        // Write to WAL first for durability
        self.wal.append(key_vec.clone(), Value::Some(value_vec.clone()))?;
        
        // Write to active memtable
        {
            let memtable = self.active_memtable.read().await;
            memtable.put(key_vec, value_vec)?;
        }
        
        // Check if memtable needs to be flushed
        self.maybe_flush_memtable().await?;
        
        Ok(())
    }
    
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // First check the active memtable
        {
            let memtable = self.active_memtable.read().await;
            if let Some(value) = memtable.get(key) {
                return match value {
                    Value::Some(data) => Ok(Some(data)),
                    Value::Tombstone => Ok(None),
                };
            }
        }
        
        // Then check immutable memtables (newest first)
        {
            let immutable = self.immutable_memtables.read();
            for memtable in immutable.iter().rev() {
                if let Some(value) = memtable.get(key) {
                    return match value {
                        Value::Some(data) => Ok(Some(data)),
                        Value::Tombstone => Ok(None),
                    };
                }
            }
        }
        
        // Finally check storage (SSTables)
        match self.storage.get(key)? {
            Some(Value::Some(data)) => Ok(Some(data)),
            Some(Value::Tombstone) => Ok(None),
            None => Ok(None),
        }
    }
    
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();
        
        // Write tombstone to WAL
        self.wal.append(key_vec.clone(), Value::Tombstone)?;
        
        // Write tombstone to active memtable
        {
            let memtable = self.active_memtable.read().await;
            memtable.delete(key_vec)?;
        }
        
        // Check if memtable needs to be flushed
        self.maybe_flush_memtable().await?;
        
        Ok(())
    }
    
    pub async fn flush(&self) -> Result<()> {
        self.flush_memtable().await
    }
    
    pub async fn compact(&self) -> Result<()> {
        self.compaction.check_and_schedule_compactions()
    }
    
    pub fn stats(&self) -> LSMStats {
        let level_stats = self.storage.get_level_stats();
        let immutable_count = self.immutable_memtables.read().len();
        let cache_stats = self.storage.cache_stats();
        let compaction_stats = self.compaction.stats();
        
        LSMStats {
            level_stats,
            immutable_memtables: immutable_count,
            cache_stats,
            compaction_stats,
        }
    }
    
    pub fn clear_cache(&self) {
        self.storage.clear_cache();
    }
    
    pub fn compaction_config(&self) -> &CompactionConfig {
        self.compaction.config()
    }
    
    async fn maybe_flush_memtable(&self) -> Result<()> {
        let should_flush = {
            let memtable = self.active_memtable.read().await;
            memtable.is_full()
        };
        
        if should_flush {
            self.flush_memtable().await?;
        }
        
        Ok(())
    }
    
    async fn flush_memtable(&self) -> Result<()> {
        // Rotate WAL to get the ID of the current WAL file
        let old_wal_id = self.wal.rotate_wal()?;
        
        // Create new active memtable
        let old_memtable = {
            let mut active = self.active_memtable.write().await;
            let new_memtable = MemTable::new(self.memtable_size_limit);
            std::mem::replace(&mut *active, new_memtable)
        };
        
        // Associate the old memtable with its WAL file ID
        let old_memtable_with_wal = MemTable::new_with_wal_id(old_memtable.size(), old_wal_id);
        
        // Copy data from old memtable to the new one with WAL ID
        let entries = old_memtable.iter();
        for (key, value) in entries {
            match value {
                Value::Some(data) => old_memtable_with_wal.put(key, data)?,
                Value::Tombstone => old_memtable_with_wal.delete(key)?,
            }
        }
        
        // Move memtable with WAL ID to immutable list
        {
            let mut immutable = self.immutable_memtables.write();
            immutable.push(Arc::new(old_memtable_with_wal));
        }
        
        Ok(())
    }
    
    async fn background_flush_worker(
        _active_memtable: Arc<AsyncRwLock<MemTable>>,
        immutable_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,
        storage: Arc<StorageManager>,
        compaction: Arc<CompactionManager>,
        _wal: Arc<WriteAheadLog>,
    ) {
        let mut flush_interval = interval(Duration::from_secs(1));
        let mut compaction_interval = interval(Duration::from_secs(10));
        
        loop {
            tokio::select! {
                _ = flush_interval.tick() => {
                    // Flush immutable memtables to SSTables
                    let memtables_to_flush = {
                        let mut immutable = immutable_memtables.write();
                        if immutable.is_empty() {
                            continue;
                        }
                        std::mem::take(&mut *immutable)
                    };
                    
                    for memtable in memtables_to_flush {
                        if memtable.is_empty() {
                            continue;
                        }
                        
                        let entries = memtable.iter();
                        if !entries.is_empty() {
                            match storage.add_sstable(0, entries) {
                                Ok(sstable) => {
                                    // SSTable was successfully created and synced to disk
                                    // Now we can safely remove the corresponding WAL file
                                                                         if let Some(wal_id) = memtable.wal_id() {
                                        if let Err(e) = _wal.remove_wal(wal_id) {
                                            eprintln!("Failed to remove WAL file {}: {}", wal_id, e);
                                        }
                                    }
                                    println!("Successfully flushed memtable to SSTable {} and removed WAL", sstable.id);
                                }
                                Err(e) => {
                                    eprintln!("Failed to flush memtable: {}", e);
                                    // Don't remove WAL file if flush failed - keep it for recovery
                                }
                            }
                                                 } else if let Some(wal_id) = memtable.wal_id() {
                            // Empty memtable, but still remove its WAL file
                            if let Err(e) = _wal.remove_wal(wal_id) {
                                eprintln!("Failed to remove WAL file {}: {}", wal_id, e);
                            }
                        }
                    }
                }
                
                _ = compaction_interval.tick() => {
                    // Trigger compaction if needed
                    if let Err(e) = compaction.check_and_schedule_compactions() {
                        eprintln!("Failed to schedule compaction: {}", e);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct LSMStats {
    pub level_stats: Vec<(usize, usize, u64)>, // (level, count, size)
    pub immutable_memtables: usize,
    pub cache_stats: crate::cache::CacheStats,
    pub compaction_stats: CompactionStats,
}

impl LSMStats {
    pub fn print(&self) {
        println!("LSM Tree Statistics:");
        println!("  Immutable memtables: {}", self.immutable_memtables);
        println!("  Levels:");
        for (level, count, size) in &self.level_stats {
            println!("    Level {}: {} SSTables, {} bytes", level, count, size);
        }
        println!("  Cache: {} / {} entries", 
                self.cache_stats.size, 
                self.cache_stats.capacity);
        println!("  Compaction:");
        println!("    Total compactions: {}", self.compaction_stats.total_compactions);
        println!("    Active compactions: {}", self.compaction_stats.concurrent_compactions);
        println!("    Bytes compacted: {} MB", self.compaction_stats.bytes_compacted / 1024 / 1024);
        println!("    SSTables compacted: {}", self.compaction_stats.sstables_compacted);
        println!("    Parallel merges: {}", self.compaction_stats.parallel_merges);
    }
} 