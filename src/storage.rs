
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::RwLock;
use crate::error::Result;
use crate::sstable::SSTable;
use crate::memtable::Value;
use crate::cache::ThreadSafeLRUCache;

#[derive(Debug)]
pub struct StorageManager {
    pub levels: Arc<RwLock<Vec<Vec<Arc<SSTable>>>>>, // levels[level_num][sstable_index]
    pub data_dir: PathBuf,
    pub next_sstable_id: Arc<parking_lot::Mutex<u64>>,
    pub max_level: usize,
    pub cache: ThreadSafeLRUCache,
}

impl StorageManager {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_level: usize) -> Result<Self> {
        Self::new_with_cache_size(data_dir, max_level, 1000) // Default cache size of 1000 entries
    }
    
    pub fn new_with_cache_size<P: AsRef<Path>>(data_dir: P, max_level: usize, cache_size: usize) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        
        let mut levels = Vec::new();
        for _ in 0..=max_level {
            levels.push(Vec::new());
        }
        
        let mut manager = Self {
            levels: Arc::new(RwLock::new(levels)),
            data_dir,
            next_sstable_id: Arc::new(parking_lot::Mutex::new(0)),
            max_level,
            cache: ThreadSafeLRUCache::new(cache_size),
        };
        
        // Load existing SSTables
        manager.load_existing_sstables()?;
        
        Ok(manager)
    }
    
    fn load_existing_sstables(&mut self) -> Result<()> {
        let entries = std::fs::read_dir(&self.data_dir)?;
        let mut sstable_files = Vec::new();
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with("sstable_") && filename.ends_with(".db") {
                    if let Some(id_str) = filename.strip_prefix("sstable_").and_then(|s| s.strip_suffix(".db")) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            sstable_files.push(id);
                        }
                    }
                }
            }
        }
        
        // Sort by ID to maintain order
        sstable_files.sort();
        
        // Load SSTables (for now, put them all in level 0)
        let mut levels = self.levels.write();
        for id in sstable_files {
            match SSTable::open(id, &self.data_dir) {
                Ok(sstable) => {
                    levels[0].push(Arc::new(sstable));
                    let mut next_id = self.next_sstable_id.lock();
                    *next_id = (*next_id).max(id + 1);
                },
                Err(e) => {
                    eprintln!("Warning: Failed to load SSTable {}: {}", id, e);
                }
            }
        }
        
        Ok(())
    }
    
    pub fn add_sstable(&self, level: usize, entries: Vec<(Vec<u8>, Value)>) -> Result<Arc<SSTable>> {
        if level > self.max_level {
            return Err(crate::error::SliceError::InvalidOperation(
                format!("Level {} exceeds max level {}", level, self.max_level)
            ));
        }
        
        let id = {
            let mut next_id = self.next_sstable_id.lock();
            let id = *next_id;
            *next_id += 1;
            id
        };
        
        let sstable = SSTable::create(id, &self.data_dir, entries)?;
        let sstable_arc = Arc::new(sstable);
        
        let mut levels = self.levels.write();
        levels[level].push(sstable_arc.clone());
        
        Ok(sstable_arc)
    }
    
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        // First check the cache
        if let Some(cached_value) = self.cache.get(key) {
            return Ok(Some(cached_value));
        }
        
        let levels = self.levels.read();
        
        // Search from level 0 (newest) to highest level (oldest)
        for level_tables in levels.iter() {
            for sstable in level_tables.iter().rev() { // Reverse to check newest first
                if sstable.contains_key(key) {
                    if let Some(value) = sstable.get(key)? {
                        // Cache the result before returning
                        self.cache.put(key.to_vec(), value.clone());
                        return Ok(Some(value));
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    pub fn get_sstables_for_level(&self, level: usize) -> Vec<Arc<SSTable>> {
        let levels = self.levels.read();
        if level < levels.len() {
            levels[level].clone()
        } else {
            Vec::new()
        }
    }
    
    pub fn remove_sstables(&self, level: usize, sstable_ids: &[u64]) -> Result<()> {
        let mut levels = self.levels.write();
        if level < levels.len() {
            levels[level].retain(|sstable| !sstable_ids.contains(&sstable.id));
        }
        Ok(())
    }
    
    pub fn get_level_stats(&self) -> Vec<(usize, usize, u64)> {
        let levels = self.levels.read();
        levels.iter().enumerate().map(|(level, tables)| {
            let count = tables.len();
            let total_size = tables.iter().map(|t| t.size()).sum();
            (level, count, total_size)
        }).collect()
    }
    
    pub fn get_bloom_filter_stats(&self) -> Vec<(usize, usize, f64)> {
        let levels = self.levels.read();
        let mut stats = Vec::new();
        
        for (level, tables) in levels.iter().enumerate() {
            for table in tables {
                let (size, hash_functions, fpr) = table.bloom_filter_stats();
                stats.push((size, hash_functions, fpr));
            }
        }
        
        stats
    }
    
    pub fn needs_compaction(&self, level: usize) -> bool {
        let levels = self.levels.read();
        if level < levels.len() {
            let target_size = self.target_size_for_level(level);
            let current_size: u64 = levels[level].iter().map(|t| t.size()).sum();
            current_size > target_size
        } else {
            false
        }
    }
    
    fn target_size_for_level(&self, level: usize) -> u64 {
        // Level 0: 4 SSTables
        // Level 1: 40MB
        // Level n: 10x previous level
        match level {
            0 => 4 * 10 * 1024 * 1024, // 40MB for level 0
            1 => 40 * 1024 * 1024,     // 40MB
            n => 40 * 1024 * 1024 * (10_u64.pow((n - 1) as u32)),
        }
    }
    
    // Cache management methods
    pub fn cache_stats(&self) -> crate::cache::CacheStats {
        self.cache.stats()
    }
    
    pub fn clear_cache(&self) {
        self.cache.clear();
    }
    
    pub fn cache_size(&self) -> usize {
        self.cache.size()
    }
} 