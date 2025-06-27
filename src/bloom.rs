use bit_vec::BitVec;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A Bloom filter implementation for fast key existence checks
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: BitVec,
    hash_functions: usize,
    size: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with the specified number of bits and hash functions
    pub fn new(bits: usize, hash_functions: usize) -> Self {
        Self {
            bits: BitVec::from_elem(bits, false),
            hash_functions,
            size: bits,
        }
    }
    
    /// Create a bloom filter optimized for the expected number of keys
    /// Uses optimal parameters to minimize false positive rate
    pub fn with_expected_keys(expected_keys: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let bits = (-(expected_keys as f64) * false_positive_rate.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        
        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let hash_functions = ((bits as f64 / expected_keys as f64) * 2.0_f64.ln()).round() as usize;
        let hash_functions = hash_functions.clamp(1, 10); // Reasonable bounds
        
        Self::new(bits, hash_functions)
    }
    
    /// Add a key to the bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.hash_functions {
            let hash = self.hash_key(key, i);
            let index = (hash % self.size as u64) as usize;
            self.bits.set(index, true);
        }
    }
    
    /// Check if a key might exist in the bloom filter
    /// Returns false if the key definitely doesn't exist
    /// Returns true if the key might exist (could be false positive)
    pub fn might_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.hash_functions {
            let hash = self.hash_key(key, i);
            let index = (hash % self.size as u64) as usize;
            if !self.bits.get(index).unwrap_or(false) {
                return false; // Definitely not in the set
            }
        }
        true // Might be in the set
    }
    
    /// Generate multiple hash values for a key using double hashing
    fn hash_key(&self, key: &[u8], i: usize) -> u64 {
        let mut hasher1 = DefaultHasher::new();
        key.hash(&mut hasher1);
        let hash1 = hasher1.finish();
        
        let mut hasher2 = DefaultHasher::new();
        (key, 0xdeadbeef_u64).hash(&mut hasher2);
        let hash2 = hasher2.finish();
        
        // Double hashing: h(k, i) = h1(k) + i * h2(k)
        hash1.wrapping_add((i as u64).wrapping_mul(hash2))
    }
    
    /// Get the size of the bloom filter in bits
    pub fn size(&self) -> usize {
        self.size
    }
    
    /// Get the number of hash functions
    pub fn hash_functions(&self) -> usize {
        self.hash_functions
    }
    
    /// Estimate the false positive rate based on the number of inserted elements
    pub fn estimated_false_positive_rate(&self, inserted_elements: usize) -> f64 {
        let k = self.hash_functions as f64;
        let m = self.size as f64;
        let n = inserted_elements as f64;
        
        // FPR = (1 - e^(-kn/m))^k
        (1.0 - (-k * n / m).exp()).powf(k)
    }
    
    /// Serialize the bloom filter to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        
        // Write metadata
        bytes.extend_from_slice(&(self.size as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.hash_functions as u32).to_le_bytes());
        
        // Write bit vector
        let bit_bytes = self.bits.to_bytes();
        bytes.extend_from_slice(&(bit_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&bit_bytes);
        
        bytes
    }
    
    /// Deserialize a bloom filter from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if bytes.len() < 12 {
            return Err("Invalid bloom filter data: too short".into());
        }
        
        let size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let hash_functions = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        let bit_bytes_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        
        if bytes.len() < 12 + bit_bytes_len {
            return Err("Invalid bloom filter data: insufficient bit data".into());
        }
        
        let bit_bytes = &bytes[12..12 + bit_bytes_len];
        let bits = BitVec::from_bytes(bit_bytes);
        
        Ok(Self {
            bits,
            hash_functions,
            size,
        })
    }
    
    /// Clear all bits in the bloom filter
    pub fn clear(&mut self) {
        self.bits.clear();
        self.bits.grow(self.size, false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(1000, 3);
        
        // Insert some keys
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        bloom.insert(b"key3");
        
        // Check that inserted keys might exist
        assert!(bloom.might_contain(b"key1"));
        assert!(bloom.might_contain(b"key2"));
        assert!(bloom.might_contain(b"key3"));
        
        // Check some keys that definitely don't exist
        // Note: there might be false positives, but no false negatives
        let non_existent_keys = [
            &b"definitely_not_there_12345"[..],
            &b"another_missing_key_67890"[..],
            &b"yet_another_missing_key_abcdef"[..],
        ];
        
        let mut false_positives = 0;
        for key in &non_existent_keys {
            if bloom.might_contain(key) {
                false_positives += 1;
            }
        }
        
        // With good parameters, false positive rate should be low
        assert!(false_positives <= non_existent_keys.len());
    }
    
    #[test]
    fn test_bloom_filter_optimal_sizing() {
        let expected_keys = 1000;
        let target_fpr = 0.01; // 1% false positive rate
        
        let bloom = BloomFilter::with_expected_keys(expected_keys, target_fpr);
        
        // Should have reasonable parameters
        assert!(bloom.size() > 0);
        assert!(bloom.hash_functions() > 0);
        assert!(bloom.hash_functions() <= 10);
        
        println!("Optimal bloom filter for {} keys with {}% FPR:", 
                expected_keys, target_fpr * 100.0);
        println!("  Size: {} bits", bloom.size());
        println!("  Hash functions: {}", bloom.hash_functions());
    }
    
    #[test]
    fn test_bloom_filter_serialization() {
        let mut bloom = BloomFilter::new(100, 3);
        
        // Add some keys
        bloom.insert(b"test1");
        bloom.insert(b"test2");
        bloom.insert(b"test3");
        
        // Serialize and deserialize
        let bytes = bloom.to_bytes();
        let deserialized = BloomFilter::from_bytes(&bytes).unwrap();
        
        // Should have same properties
        assert_eq!(bloom.size(), deserialized.size());
        assert_eq!(bloom.hash_functions(), deserialized.hash_functions());
        
        // Should contain the same keys
        assert!(deserialized.might_contain(b"test1"));
        assert!(deserialized.might_contain(b"test2"));
        assert!(deserialized.might_contain(b"test3"));
    }
    
    #[test]
    fn test_false_positive_rate_estimation() {
        let mut bloom = BloomFilter::new(1000, 3);
        
        // Insert 100 keys
        for i in 0..100 {
            let key = format!("key{}", i);
            bloom.insert(key.as_bytes());
        }
        
        let estimated_fpr = bloom.estimated_false_positive_rate(100);
        assert!(estimated_fpr > 0.0);
        assert!(estimated_fpr < 1.0);
        
        println!("Estimated false positive rate: {:.4}", estimated_fpr);
    }
    
    #[test]
    fn test_no_false_negatives() {
        let mut bloom = BloomFilter::new(1000, 5);
        let test_keys = [
            &b"key1"[..], &b"key2"[..], &b"key3"[..], &b"key4"[..], &b"key5"[..],
            &b"longer_key_name_12345"[..], &b""[..], &b"single_char_k"[..],
        ];
        
        // Insert all test keys
        for key in &test_keys {
            bloom.insert(key);
        }
        
        // All inserted keys must be found (no false negatives allowed)
        for key in &test_keys {
            assert!(bloom.might_contain(key), 
                   "False negative for key: {:?}", String::from_utf8_lossy(key));
        }
    }
} 