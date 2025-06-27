use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::memtable::Value;

#[derive(Debug, Clone)]
struct CacheEntry {
    value: Value,
    access_count: u64,
}

#[derive(Debug)]
struct LRUNode {
    key: Vec<u8>,
    entry: CacheEntry,
    prev: Option<usize>,
    next: Option<usize>,
}

#[derive(Debug)]
pub struct LRUCache {
    capacity: usize,
    size: usize,
    map: HashMap<Vec<u8>, usize>,
    nodes: Vec<Option<LRUNode>>,
    head: Option<usize>,
    tail: Option<usize>,
    free_indices: Vec<usize>,
    global_access_count: u64,
}

impl LRUCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            size: 0,
            map: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            free_indices: Vec::new(),
            global_access_count: 0,
        }
    }
    
    pub fn get(&mut self, key: &[u8]) -> Option<Value> {
        if let Some(&node_idx) = self.map.get(key) {
            self.global_access_count += 1;
            
            // Update access count
            if let Some(node) = &mut self.nodes[node_idx] {
                node.entry.access_count = self.global_access_count;
            }
            
            // Move to front (most recently used)
            self.move_to_front(node_idx);
            
            // Return cloned value
            self.nodes[node_idx].as_ref().map(|node| node.entry.value.clone())
        } else {
            None
        }
    }
    
    pub fn put(&mut self, key: Vec<u8>, value: Value) {
        self.global_access_count += 1;
        
        if let Some(&node_idx) = self.map.get(&key) {
            // Update existing entry
            if let Some(node) = &mut self.nodes[node_idx] {
                node.entry.value = value;
                node.entry.access_count = self.global_access_count;
            }
            self.move_to_front(node_idx);
        } else {
            // Add new entry
            if self.size >= self.capacity {
                self.evict_lru();
            }
            
            let new_entry = CacheEntry {
                value,
                access_count: self.global_access_count,
            };
            
            let node_idx = if let Some(free_idx) = self.free_indices.pop() {
                free_idx
            } else {
                let idx = self.nodes.len();
                self.nodes.push(None);
                idx
            };
            
            let new_node = LRUNode {
                key: key.clone(),
                entry: new_entry,
                prev: None,
                next: self.head,
            };
            
            self.nodes[node_idx] = Some(new_node);
            self.map.insert(key, node_idx);
            
            if let Some(head_idx) = self.head {
                if let Some(head_node) = &mut self.nodes[head_idx] {
                    head_node.prev = Some(node_idx);
                }
            }
            
            self.head = Some(node_idx);
            
            if self.tail.is_none() {
                self.tail = Some(node_idx);
            }
            
            self.size += 1;
        }
    }
    
    fn move_to_front(&mut self, node_idx: usize) {
        if Some(node_idx) == self.head {
            return; // Already at front
        }
        
        // Get the node's prev and next indices before borrowing mutably
        let (prev_idx, next_idx) = if let Some(node) = &self.nodes[node_idx] {
            (node.prev, node.next)
        } else {
            return;
        };
        
        // Update previous node's next pointer
        if let Some(prev_idx) = prev_idx {
            if let Some(prev_node) = &mut self.nodes[prev_idx] {
                prev_node.next = next_idx;
            }
        }
        
        // Update next node's prev pointer
        if let Some(next_idx) = next_idx {
            if let Some(next_node) = &mut self.nodes[next_idx] {
                next_node.prev = prev_idx;
            }
        } else {
            // This was the tail
            self.tail = prev_idx;
        }
        
        // Move node to front
        if let Some(node) = &mut self.nodes[node_idx] {
            node.prev = None;
            node.next = self.head;
        }
        
        // Update old head's prev pointer
        if let Some(old_head_idx) = self.head {
            if let Some(old_head) = &mut self.nodes[old_head_idx] {
                old_head.prev = Some(node_idx);
            }
        }
        
        self.head = Some(node_idx);
    }
    
    fn evict_lru(&mut self) {
        if let Some(tail_idx) = self.tail {
            if let Some(tail_node) = self.nodes[tail_idx].take() {
                self.map.remove(&tail_node.key);
                self.free_indices.push(tail_idx);
                
                if let Some(prev_idx) = tail_node.prev {
                    if let Some(prev_node) = &mut self.nodes[prev_idx] {
                        prev_node.next = None;
                    }
                    self.tail = Some(prev_idx);
                } else {
                    // Cache is now empty
                    self.head = None;
                    self.tail = None;
                }
                
                self.size -= 1;
            }
        }
    }
    
    pub fn size(&self) -> usize {
        self.size
    }
    
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    
    pub fn clear(&mut self) {
        self.map.clear();
        self.nodes.clear();
        self.free_indices.clear();
        self.head = None;
        self.tail = None;
        self.size = 0;
        self.global_access_count = 0;
    }
}

#[derive(Debug)]
pub struct ThreadSafeLRUCache {
    cache: Arc<Mutex<LRUCache>>,
}

impl ThreadSafeLRUCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LRUCache::new(capacity))),
        }
    }
    
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let mut cache = self.cache.lock();
        cache.get(key)
    }
    
    pub fn put(&self, key: Vec<u8>, value: Value) {
        let mut cache = self.cache.lock();
        cache.put(key, value);
    }
    
    pub fn size(&self) -> usize {
        let cache = self.cache.lock();
        cache.size()
    }
    
    pub fn capacity(&self) -> usize {
        let cache = self.cache.lock();
        cache.capacity()
    }
    
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
    }
    
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.lock();
        CacheStats {
            size: cache.size(),
            capacity: cache.capacity(),
            hit_ratio: 0.0, // We could add hit/miss tracking if needed
        }
    }
}

impl Clone for ThreadSafeLRUCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hit_ratio: f64,
}

impl CacheStats {
    pub fn print(&self) {
        println!("Cache Statistics:");
        println!("  Size: {} / {} entries", self.size, self.capacity);
        println!("  Hit ratio: {:.2}%", self.hit_ratio * 100.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;
    use std::thread;
    
    #[test]
    fn test_lru_cache_basic_operations() {
        let mut cache = LRUCache::new(3);
        
        // Test put and get
        cache.put(b"key1".to_vec(), Value::Some(b"value1".to_vec()));
        cache.put(b"key2".to_vec(), Value::Some(b"value2".to_vec()));
        cache.put(b"key3".to_vec(), Value::Some(b"value3".to_vec()));
        
        assert_eq!(cache.size(), 3);
        assert_eq!(cache.get(b"key1"), Some(Value::Some(b"value1".to_vec())));
        assert_eq!(cache.get(b"key2"), Some(Value::Some(b"value2".to_vec())));
        assert_eq!(cache.get(b"key3"), Some(Value::Some(b"value3".to_vec())));
    }
    
    #[test]
    fn test_lru_eviction() {
        let mut cache = LRUCache::new(2);
        
        cache.put(b"key1".to_vec(), Value::Some(b"value1".to_vec()));
        cache.put(b"key2".to_vec(), Value::Some(b"value2".to_vec()));
        
        // Access key1 to make it most recently used
        cache.get(b"key1");
        
        // Add key3, should evict key2 (least recently used)
        cache.put(b"key3".to_vec(), Value::Some(b"value3".to_vec()));
        
        assert_eq!(cache.size(), 2);
        assert_eq!(cache.get(b"key1"), Some(Value::Some(b"value1".to_vec())));
        assert_eq!(cache.get(b"key2"), None); // Evicted
        assert_eq!(cache.get(b"key3"), Some(Value::Some(b"value3".to_vec())));
    }
    
    #[test]
    fn test_lru_update_existing() {
        let mut cache = LRUCache::new(2);
        
        cache.put(b"key1".to_vec(), Value::Some(b"old_value".to_vec()));
        cache.put(b"key2".to_vec(), Value::Some(b"value2".to_vec()));
        
        // Update key1 with new value
        cache.put(b"key1".to_vec(), Value::Some(b"new_value".to_vec()));
        
        assert_eq!(cache.size(), 2);
        assert_eq!(cache.get(b"key1"), Some(Value::Some(b"new_value".to_vec())));
        assert_eq!(cache.get(b"key2"), Some(Value::Some(b"value2".to_vec())));
    }
    
    #[test]
    fn test_thread_safe_cache() {
        let cache = ThreadSafeLRUCache::new(10);
        
        cache.put(b"key1".to_vec(), Value::Some(b"value1".to_vec()));
        assert_eq!(cache.get(b"key1"), Some(Value::Some(b"value1".to_vec())));
        
        let stats = cache.stats();
        assert_eq!(stats.size, 1);
        assert_eq!(stats.capacity, 10);
    }
    
    #[test]
    fn test_concurrent_cache_access() {
        let cache = Arc::new(ThreadSafeLRUCache::new(100));
        let num_threads = 10;
        let operations_per_thread = 100;
        
        let mut handles = Vec::new();
        
        // Spawn threads that perform concurrent reads and writes
        for thread_id in 0..num_threads {
            let cache_clone = cache.clone();
            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("key_{}_{}", thread_id, i);
                    let value = format!("value_{}_{}", thread_id, i);
                    
                    // Write
                    cache_clone.put(key.as_bytes().to_vec(), Value::Some(value.as_bytes().to_vec()));
                    
                    // Read back
                    if let Some(Value::Some(read_value)) = cache_clone.get(key.as_bytes()) {
                        assert_eq!(read_value, value.as_bytes());
                    }
                }
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify cache stats
        let stats = cache.stats();
        assert!(stats.size <= 100); // Should not exceed capacity
        assert_eq!(stats.capacity, 100);
    }
    
    #[test]
    fn test_cache_with_tombstones() {
        let mut cache = LRUCache::new(5);
        
        // Add regular values
        cache.put(b"key1".to_vec(), Value::Some(b"value1".to_vec()));
        cache.put(b"key2".to_vec(), Value::Some(b"value2".to_vec()));
        
        // Add tombstone (deletion marker)
        cache.put(b"key3".to_vec(), Value::Tombstone);
        
        assert_eq!(cache.get(b"key1"), Some(Value::Some(b"value1".to_vec())));
        assert_eq!(cache.get(b"key2"), Some(Value::Some(b"value2".to_vec())));
        assert_eq!(cache.get(b"key3"), Some(Value::Tombstone));
        assert_eq!(cache.size(), 3);
    }
    
    #[test]
    fn test_cache_clear() {
        let cache = ThreadSafeLRUCache::new(10);
        
        // Add some entries
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            cache.put(key.as_bytes().to_vec(), Value::Some(value.as_bytes().to_vec()));
        }
        
        assert_eq!(cache.size(), 5);
        
        // Clear the cache
        cache.clear();
        
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.get(b"key0"), None);
    }
} 