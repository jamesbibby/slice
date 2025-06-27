use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use crc32fast::Hasher;
use crate::error::{Result, SliceError};
use crate::memtable::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub key: Vec<u8>,
    pub value: Value,
    pub timestamp: u64,
}

#[derive(Debug)]
pub struct WriteAheadLog {
    current_writer: Arc<Mutex<BufWriter<File>>>,
    data_dir: PathBuf,
    current_wal_id: Arc<Mutex<u64>>,
    next_wal_id: Arc<Mutex<u64>>,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let data_dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        
        // Find the latest WAL file to determine starting ID
        let (current_wal_id, next_wal_id) = Self::find_latest_wal_id(&data_dir)?;
        
        let wal_path = Self::wal_path(&data_dir, current_wal_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
            
        Ok(Self {
            current_writer: Arc::new(Mutex::new(BufWriter::new(file))),
            data_dir,
            current_wal_id: Arc::new(Mutex::new(current_wal_id)),
            next_wal_id: Arc::new(Mutex::new(next_wal_id)),
        })
    }
    
    fn find_latest_wal_id(data_dir: &Path) -> Result<(u64, u64)> {
        let mut max_id = 0u64;
        let mut found_any = false;
        
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Some(id_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                            if let Ok(id) = id_str.parse::<u64>() {
                                max_id = max_id.max(id);
                                found_any = true;
                            }
                        }
                    }
                }
            }
        }
        
        if found_any {
            Ok((max_id, max_id + 1))
        } else {
            Ok((0, 1))
        }
    }
    
    fn wal_path(data_dir: &Path, wal_id: u64) -> PathBuf {
        data_dir.join(format!("wal_{:06}.log", wal_id))
    }
    
    pub fn append(&self, key: Vec<u8>, value: Value) -> Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
            
        let entry = LogEntry {
            key,
            value,
            timestamp,
        };
        
        let serialized = bincode::serialize(&entry)?;
        let len = serialized.len() as u32;
        
        // Calculate CRC32 checksum
        let mut hasher = Hasher::new();
        hasher.update(&len.to_le_bytes());
        hasher.update(&serialized);
        let checksum = hasher.finalize();
        
        // Write: [checksum:4][length:4][data:length]
        let mut writer = self.current_writer.lock();
        writer.write_all(&checksum.to_le_bytes())?;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&serialized)?;
        writer.flush()?;
        
        Ok(())
    }
    
    pub fn replay(&self) -> Result<Vec<LogEntry>> {
        let mut all_entries = Vec::new();
        
        // Find all WAL files and sort them by ID
        let mut wal_files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("wal_") && name.ends_with(".log") {
                        if let Some(id_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                            if let Ok(id) = id_str.parse::<u64>() {
                                wal_files.push((id, entry.path()));
                            }
                        }
                    }
                }
            }
        }
        
        // Sort by WAL ID to replay in order
        wal_files.sort_by_key(|(id, _)| *id);
        
        // Replay each WAL file
        for (_wal_id, wal_path) in wal_files {
            println!("Replaying WAL file: {:?}", wal_path);
            let entries = self.replay_single_wal(&wal_path)?;
            all_entries.extend(entries);
        }
        
        Ok(all_entries)
    }
    
    fn replay_single_wal(&self, wal_path: &Path) -> Result<Vec<LogEntry>> {
        let file = File::open(wal_path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        
        loop {
            // Read checksum
            let mut checksum_buf = [0u8; 4];
            match reader.read_exact(&mut checksum_buf) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let expected_checksum = u32::from_le_bytes(checksum_buf);
            
            // Read length
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;
            
            // Read data
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;
            
            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(&len_buf);
            hasher.update(&data);
            let actual_checksum = hasher.finalize();
            
            if actual_checksum != expected_checksum {
                return Err(SliceError::Corruption(
                    format!("WAL entry checksum mismatch in {:?}", wal_path)
                ));
            }
            
            // Deserialize entry
            let entry: LogEntry = bincode::deserialize(&data)?;
            entries.push(entry);
        }
        
        Ok(entries)
    }
    
    /// Create a new WAL file and return the ID of the previous one
    pub fn rotate_wal(&self) -> Result<u64> {
        let old_wal_id = {
            let mut current_id = self.current_wal_id.lock();
            let mut next_id = self.next_wal_id.lock();
            
            let old_id = *current_id;
            *current_id = *next_id;
            *next_id += 1;
            
            // Sync and close the current writer
            {
                let mut writer = self.current_writer.lock();
                writer.flush()?;
                writer.get_mut().sync_all()?;
            }
            
            // Create new WAL file
            let new_wal_path = Self::wal_path(&self.data_dir, *current_id);
            let new_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_wal_path)?;
            
            // Replace the writer
            *self.current_writer.lock() = BufWriter::new(new_file);
            
            old_id
        };
        
        Ok(old_wal_id)
    }
    
    /// Remove a WAL file by ID (should only be called after successful flush to disk)
    pub fn remove_wal(&self, wal_id: u64) -> Result<()> {
        let wal_path = Self::wal_path(&self.data_dir, wal_id);
        if wal_path.exists() {
            std::fs::remove_file(&wal_path)?;
            println!("Removed WAL file: {:?}", wal_path);
        }
        Ok(())
    }
    
    pub fn sync(&self) -> Result<()> {
        let mut writer = self.current_writer.lock();
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
    
    pub fn current_wal_id(&self) -> u64 {
        *self.current_wal_id.lock()
    }
} 