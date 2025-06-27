use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Value {
    Some(Vec<u8>),
    Tombstone, // For deletions
}

impl Value {
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone)
    }
    
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Some(data) => Some(data),
            Value::Tombstone => None,
        }
    }
}

#[derive(Debug)]
pub struct MemTable {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Value>>>,
    size: Arc<parking_lot::Mutex<usize>>,
    max_size: usize,
    wal_id: Option<u64>, // WAL file ID this memtable corresponds to
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(parking_lot::Mutex::new(0)),
            max_size,
            wal_id: None,
        }
    }
    
    pub fn new_with_wal_id(max_size: usize, wal_id: u64) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(parking_lot::Mutex::new(0)),
            max_size,
            wal_id: Some(wal_id),
        }
    }
    
    pub fn wal_id(&self) -> Option<u64> {
        self.wal_id
    }
    
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let entry_size = key.len() + value.len();
        let key_len = key.len();
        let old_value = {
            let mut data = self.data.write();
            data.insert(key, Value::Some(value))
        };
        
        // Update size accounting
        let mut size = self.size.lock();
        if let Some(old_val) = old_value {
            match old_val {
                Value::Some(old_data) => *size = *size - old_data.len() + entry_size - key_len,
                Value::Tombstone => *size += entry_size,
            }
        } else {
            *size += entry_size;
        }
        
        Ok(())
    }
    
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let data = self.data.read();
        data.get(key).cloned()
    }
    
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        let old_value = {
            let mut data = self.data.write();
            data.insert(key.clone(), Value::Tombstone)
        };
        
        // Update size - tombstones are smaller than values
        let mut size = self.size.lock();
        if let Some(Value::Some(old_data)) = old_value {
            *size = *size - old_data.len() + key.len();
        } else if old_value.is_none() {
            *size += key.len();
        }
        
        Ok(())
    }
    
    pub fn is_full(&self) -> bool {
        *self.size.lock() >= self.max_size
    }
    
    pub fn size(&self) -> usize {
        *self.size.lock()
    }
    
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }
    
    pub fn iter(&self) -> Vec<(Vec<u8>, Value)> {
        let data = self.data.read();
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
    
    pub fn clear(&self) {
        let mut data = self.data.write();
        data.clear();
        let mut size = self.size.lock();
        *size = 0;
    }
} 