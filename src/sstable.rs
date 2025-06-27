use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use crc32fast::Hasher;
use crate::error::{Result, SliceError};
use crate::memtable::Value;
use crate::bloom::BloomFilter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableEntry {
    pub key: Vec<u8>,
    pub value: Value,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableIndex {
    pub entries: BTreeMap<Vec<u8>, u64>, // key -> file offset
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub entry_count: usize,
}

#[derive(Debug)]
pub struct SSTable {
    pub id: u64,
    pub path: PathBuf,
    pub index: SSTableIndex,
    pub file_size: u64,
    pub bloom_filter: BloomFilter,
}

impl SSTable {
    pub fn create<P: AsRef<Path>>(
        id: u64,
        dir: P,
        entries: Vec<(Vec<u8>, Value)>,
    ) -> Result<Self> {
        let path = dir.as_ref().join(format!("sstable_{:06}.db", id));
        let index_path = dir.as_ref().join(format!("sstable_{:06}.idx", id));
        
        // Sort entries by key for efficient range queries
        let mut sorted_entries: Vec<_> = entries.into_iter().collect();
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        if sorted_entries.is_empty() {
            return Err(SliceError::InvalidOperation("Cannot create empty SSTable".to_string()));
        }
        
        // Create bloom filter for the keys
        let mut bloom_filter = BloomFilter::with_expected_keys(sorted_entries.len(), 0.01); // 1% false positive rate
        for (key, _) in &sorted_entries {
            bloom_filter.insert(key);
        }
        
        let mut file = BufWriter::new(File::create(&path)?);
        let mut index_entries = BTreeMap::new();
        let mut offset = 0u64;
        
        let min_key = sorted_entries[0].0.clone();
        let max_key = sorted_entries[sorted_entries.len() - 1].0.clone();
        
        // Write entries to file
        for (key, value) in &sorted_entries {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
                
            let entry = SSTableEntry {
                key: key.clone(),
                value: value.clone(),
                timestamp,
            };
            
            let serialized = bincode::serialize(&entry)?;
            let len = serialized.len() as u32;
            
            // Calculate checksum
            let mut hasher = Hasher::new();
            hasher.update(&len.to_le_bytes());
            hasher.update(&serialized);
            let checksum = hasher.finalize();
            
            // Store offset in index
            index_entries.insert(key.clone(), offset);
            
            // Write: [checksum:4][length:4][data:length]
            file.write_all(&checksum.to_le_bytes())?;
            file.write_all(&len.to_le_bytes())?;
            file.write_all(&serialized)?;
            
            offset += 4 + 4 + len as u64;
        }
        
        file.flush()?;
        let file_size = offset;
        
        // Create and write index
        let index = SSTableIndex {
            entries: index_entries,
            min_key,
            max_key,
            entry_count: sorted_entries.len(),
        };
        
        let index_data = bincode::serialize(&index)?;
        std::fs::write(&index_path, &index_data)?;
        
        // Save bloom filter
        let bloom_path = dir.as_ref().join(format!("sstable_{:06}.bloom", id));
        let bloom_data = bloom_filter.to_bytes();
        std::fs::write(&bloom_path, &bloom_data)?;
        
        Ok(SSTable {
            id,
            path,
            index,
            file_size,
            bloom_filter,
        })
    }
    
    pub fn open<P: AsRef<Path>>(id: u64, dir: P) -> Result<Self> {
        let path = dir.as_ref().join(format!("sstable_{:06}.db", id));
        let index_path = dir.as_ref().join(format!("sstable_{:06}.idx", id));
        let bloom_path = dir.as_ref().join(format!("sstable_{:06}.bloom", id));
        
        let index_data = std::fs::read(&index_path)?;
        let index: SSTableIndex = bincode::deserialize(&index_data)?;
        
        // Load bloom filter
        let bloom_filter = if bloom_path.exists() {
            let bloom_data = std::fs::read(&bloom_path)?;
            BloomFilter::from_bytes(&bloom_data).map_err(|e| SliceError::Corruption(format!("Invalid bloom filter: {}", e)))?
        } else {
            // For backward compatibility with existing SSTables without bloom filters
            // Create a bloom filter that accepts everything (all bits set to true)
            let mut bloom = BloomFilter::with_expected_keys(index.entry_count, 0.01);
            for key in index.entries.keys() {
                bloom.insert(key);
            }
            bloom
        };
        
        let metadata = std::fs::metadata(&path)?;
        let file_size = metadata.len();
        
        Ok(SSTable {
            id,
            path,
            index,
            file_size,
            bloom_filter,
        })
    }
    
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        // First check bloom filter for fast negative lookups
        if !self.bloom_filter.might_contain(key) {
            return Ok(None); // Definitely not in this SSTable
        }
        
        if let Some(&offset) = self.index.entries.get(key) {
            let mut file = BufReader::new(File::open(&self.path)?);
            file.seek(SeekFrom::Start(offset))?;
            
            // Read checksum
            let mut checksum_buf = [0u8; 4];
            file.read_exact(&mut checksum_buf)?;
            let expected_checksum = u32::from_le_bytes(checksum_buf);
            
            // Read length
            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;
            
            // Read data
            let mut data = vec![0u8; len];
            file.read_exact(&mut data)?;
            
            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&len_buf);
            hasher.update(&data);
            let actual_checksum = hasher.finalize();
            
            if actual_checksum != expected_checksum {
                return Err(SliceError::Corruption(
                    "SSTable entry checksum mismatch".to_string()
                ));
            }
            
            // Deserialize entry
            let entry: SSTableEntry = bincode::deserialize(&data)?;
            Ok(Some(entry.value))
        } else {
            Ok(None)
        }
    }
    
    pub fn contains_key(&self, key: &[u8]) -> bool {
        // First check bloom filter for fast negative lookups
        if !self.bloom_filter.might_contain(key) {
            return false; // Definitely not in this SSTable
        }
        
        // Then check key range
        key >= self.index.min_key.as_slice() && key <= self.index.max_key.as_slice()
    }
    
    pub fn iter(&self) -> Result<SSTableIterator> {
        SSTableIterator::new(&self.path)
    }
    
    pub fn size(&self) -> u64 {
        self.file_size
    }
    
    pub fn key_count(&self) -> usize {
        self.index.entry_count
    }
    
    pub fn bloom_filter_stats(&self) -> (usize, usize, f64) {
        let size = self.bloom_filter.size();
        let hash_functions = self.bloom_filter.hash_functions();
        let estimated_fpr = self.bloom_filter.estimated_false_positive_rate(self.index.entry_count);
        (size, hash_functions, estimated_fpr)
    }
}

pub struct SSTableIterator {
    reader: BufReader<File>,
    current_offset: u64,
    file_size: u64,
}

impl SSTableIterator {
    fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let reader = BufReader::new(file);
        
        Ok(SSTableIterator {
            reader,
            current_offset: 0,
            file_size,
        })
    }
}

impl Iterator for SSTableIterator {
    type Item = Result<SSTableEntry>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.file_size {
            return None;
        }
        
        // Read checksum
        let mut checksum_buf = [0u8; 4];
        match self.reader.read_exact(&mut checksum_buf) {
            Ok(_) => {},
            Err(_) => return None,
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);
        
        // Read len
        let mut len_buf = [0u8; 4];
        if self.reader.read_exact(&mut len_buf).is_err() {
            return None;
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        
        // Read data
        let mut data = vec![0u8; len];
        if self.reader.read_exact(&mut data).is_err() {
            return None;
        }
        
        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&len_buf);
        hasher.update(&data);
        let actual_checksum = hasher.finalize();
        
        if actual_checksum != expected_checksum {
            return Some(Err(SliceError::Corruption(
                "SSTable entry checksum mismatch".to_string()
            )));
        }
        
        // Update offset
        self.current_offset += 4 + 4 + len as u64;
        
        // Deserialize entry
        match bincode::deserialize(&data) {
            Ok(entry) => Some(Ok(entry)),
            Err(e) => Some(Err(e.into())),
        }
    }
} 