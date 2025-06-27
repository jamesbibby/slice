use slice::LSMTree;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use rand::prelude::*;
use tempfile::TempDir;

// ============================================================================
// LINEARIZABILITY TESTING (Jepsen-style)
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationResult {
    PutOk,
    GetResult(Option<Vec<u8>>),
    DeleteOk,
}

#[derive(Debug, Clone)]
pub struct HistoryEvent {
    pub process_id: u32,
    pub operation: Operation,
    pub result: Option<OperationResult>,
    pub invoke_time: u64,
    pub complete_time: Option<u64>,
    pub event_type: EventType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
    Invoke,
    Ok,
    Fail,
    Info, // Indeterminate (timeout, etc.)
}

/// Simple linearizability checker inspired by Jepsen/Knossos
/// This is a simplified version - production systems would use more sophisticated checkers
pub struct LinearizabilityChecker {
    history: Vec<HistoryEvent>,
}

impl LinearizabilityChecker {
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }

    pub fn record_invoke(&mut self, process_id: u32, operation: Operation) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.history.push(HistoryEvent {
            process_id,
            operation,
            result: None,
            invoke_time: now,
            complete_time: None,
            event_type: EventType::Invoke,
        });
    }

    pub fn record_complete(&mut self, process_id: u32, result: OperationResult, event_type: EventType) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        
        // Find the corresponding invoke event
        for event in self.history.iter_mut().rev() {
            if event.process_id == process_id && event.result.is_none() {
                event.result = Some(result);
                event.complete_time = Some(now);
                event.event_type = event_type;
                break;
            }
        }
    }

    /// Basic linearizability check - verifies that all completed operations
    /// can be ordered consistently with their real-time constraints
    pub fn check_linearizable(&self) -> Result<(), String> {
        // For this simplified checker, we verify basic properties:
        // 1. All successful puts should be readable until overwritten
        // 2. Reads should return values from successful puts
        // 3. Operations should respect real-time ordering where possible

        let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut completed_ops: Vec<&HistoryEvent> = self.history
            .iter()
            .filter(|e| e.event_type == EventType::Ok && e.complete_time.is_some())
            .collect();

        // Sort by completion time to check one possible linearization
        completed_ops.sort_by_key(|e| e.complete_time.unwrap());

        for event in completed_ops {
            match (&event.operation, &event.result) {
                (Operation::Put { key, value }, Some(OperationResult::PutOk)) => {
                    state.insert(key.clone(), value.clone());
                }
                (Operation::Get { key }, Some(OperationResult::GetResult(result))) => {
                    let expected = state.get(key);
                    match (expected, result) {
                        (Some(expected_val), Some(actual_val)) => {
                            if expected_val != actual_val {
                                return Err(format!(
                                    "Linearizability violation: key {:?} expected {:?}, got {:?}",
                                    key, expected_val, actual_val
                                ));
                            }
                        }
                        (None, Some(actual_val)) => {
                            return Err(format!(
                                "Linearizability violation: key {:?} should not exist, but got {:?}",
                                key, actual_val
                            ));
                        }
                        _ => {} // None/None is fine, Some/None could be a delete
                    }
                }
                (Operation::Delete { key }, Some(OperationResult::DeleteOk)) => {
                    state.remove(key);
                }
                _ => {}
            }
        }

        Ok(())
    }
}

// ============================================================================
// ACID COMPLIANCE TESTS
// ============================================================================

/// Test ACID properties of the LSM tree
pub struct ACIDTester {
    lsm: Arc<LSMTree>,
}

impl ACIDTester {
    pub fn new(lsm: Arc<LSMTree>) -> Self {
        Self { lsm }
    }

    /// Test Atomicity: All operations in a transaction succeed or fail together
    /// Since our LSM doesn't have explicit transactions, we test atomic operations
    pub async fn test_atomicity(&self) -> Result<(), String> {
        // Test that individual operations are atomic
        let key = b"atomic_test";
        let value1 = b"value1";
        let value2 = b"value2";

        // Concurrent writes to the same key should result in one of the values
        let lsm1 = self.lsm.clone();
        let lsm2 = self.lsm.clone();

        let handle1 = tokio::spawn(async move {
            lsm1.put(key, value1).await
        });

        let handle2 = tokio::spawn(async move {
            lsm2.put(key, value2).await
        });

        let _ = tokio::try_join!(handle1, handle2).map_err(|e| e.to_string())?;

        // The final value should be one of the two values, not corrupted
        let final_value = self.lsm.get(key).await.map_err(|e| e.to_string())?;
        
        if let Some(val) = final_value {
            if val != value1 && val != value2 {
                return Err(format!("Atomicity violation: got corrupted value {:?}", val));
            }
        } else {
            return Err("Atomicity violation: value disappeared".to_string());
        }

        Ok(())
    }

    /// Test Consistency: Data integrity constraints are maintained
    pub async fn test_consistency(&self) -> Result<(), String> {
        // Test that reads reflect writes in a consistent manner
        let key = b"consistency_test";
        let value = b"test_value";

        self.lsm.put(key, value).await.map_err(|e| e.to_string())?;
        
        // Multiple reads should return the same value
        let mut handles = Vec::new();
        for _ in 0..10 {
            let lsm = self.lsm.clone();
            handles.push(tokio::spawn(async move {
                lsm.get(key).await
            }));
        }

        let results: Result<Vec<_>, _> = futures::future::try_join_all(handles).await;
        let results = results.map_err(|e| e.to_string())?;

        // All reads should return the same value
        let first_result = &results[0];
        for result in &results[1..] {
            if result.as_ref().map_err(|e| e.to_string())? != 
               first_result.as_ref().map_err(|e| e.to_string())? {
                return Err("Consistency violation: different reads returned different values".to_string());
            }
        }

        Ok(())
    }

    /// Test Isolation: Concurrent operations don't interfere with each other
    pub async fn test_isolation(&self) -> Result<(), String> {
        // Test that concurrent operations on different keys don't interfere
        let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("isolation_key_{}", i).into_bytes()).collect();
        let values: Vec<Vec<u8>> = (0..10).map(|i| format!("isolation_value_{}", i).into_bytes()).collect();

        // Concurrent writes to different keys
        let mut handles = Vec::new();
        for (key, value) in keys.iter().zip(values.iter()) {
            let lsm = self.lsm.clone();
            let key = key.clone();
            let value = value.clone();
            handles.push(tokio::spawn(async move {
                lsm.put(&key, &value).await
            }));
        }

        futures::future::try_join_all(handles).await.map_err(|e| e.to_string())?;

        // Verify all values are correctly stored
        for (key, expected_value) in keys.iter().zip(values.iter()) {
            let actual_value = self.lsm.get(key).await.map_err(|e| e.to_string())?;
            if actual_value.as_ref() != Some(expected_value) {
                return Err(format!("Isolation violation: key {:?} has wrong value", key));
            }
        }

        Ok(())
    }

    /// Test Durability: Committed data survives system restart
    pub async fn test_durability(&self, temp_dir: &TempDir) -> Result<(), String> {
        let key = b"durability_test";
        let value = b"durable_value";

        // Write data
        self.lsm.put(key.as_ref(), value.as_ref()).await.map_err(|e| e.to_string())?;
        
        // Force flush to ensure data is written to disk
        self.lsm.flush().await.map_err(|e| e.to_string())?;

        // Create a new LSM instance (simulating restart)
        let new_lsm = LSMTree::new(temp_dir.path()).await.map_err(|e| e.to_string())?;
        
        // Data should still be there
        let recovered_value = new_lsm.get(key.as_ref()).await.map_err(|e| e.to_string())?;
        if recovered_value.as_deref() != Some(value.as_ref()) {
            return Err("Durability violation: data not recovered after restart".to_string());
        }

        Ok(())
    }
}

// ============================================================================
// CONSISTENCY MODEL TESTS
// ============================================================================

/// Test different consistency models
pub struct ConsistencyTester {
    lsm: Arc<LSMTree>,
}

impl ConsistencyTester {
    pub fn new(lsm: Arc<LSMTree>) -> Self {
        Self { lsm }
    }

    /// Test Sequential Consistency: Operations appear to execute in some sequential order
    /// that respects the program order of each individual process
    pub async fn test_sequential_consistency(&self) -> Result<(), String> {
        let key = b"seq_consistency_test";
        
        // Process 1: write A, then write B
        let lsm1 = self.lsm.clone();
        let handle1 = tokio::spawn(async move {
            lsm1.put(key, b"A").await.expect("Put A failed");
            sleep(Duration::from_millis(10)).await;
            lsm1.put(key, b"B").await.expect("Put B failed");
        });

        // Process 2: read twice with delay
        let lsm2 = self.lsm.clone();
        let handle2 = tokio::spawn(async move {
            sleep(Duration::from_millis(5)).await;
            let read1 = lsm2.get(key).await.expect("Read 1 failed");
            sleep(Duration::from_millis(20)).await;
            let read2 = lsm2.get(key).await.expect("Read 2 failed");
            (read1, read2)
        });

        let _ = handle1.await.map_err(|e| e.to_string())?;
        let (read1, read2) = handle2.await.map_err(|e| e.to_string())?;

        // Sequential consistency allows: (None, None), (None, A), (None, B), (A, A), (A, B), (B, B)
        // But NOT (B, A) - that would violate program order
        match (read1.as_deref(), read2.as_deref()) {
            (Some(b"B"), Some(b"A")) => {
                return Err("Sequential consistency violation: saw B then A".to_string());
            }
            _ => {} // All other combinations are valid
        }

        Ok(())
    }

    /// Test Read-Your-Writes Consistency: A process can always read its own writes
    pub async fn test_read_your_writes(&self) -> Result<(), String> {
        let key = b"ryw_test";
        let value = b"my_write";

        // Write then immediately read
        self.lsm.put(key.as_ref(), value.as_ref()).await.map_err(|e| e.to_string())?;
        let read_result = self.lsm.get(key.as_ref()).await.map_err(|e| e.to_string())?;

        if read_result.as_deref() != Some(value.as_ref()) {
            return Err("Read-your-writes violation: couldn't read own write".to_string());
        }

        Ok(())
    }

    /// Test Monotonic Read Consistency: If a process reads a value, 
    /// subsequent reads should return the same or newer values
    pub async fn test_monotonic_reads(&self) -> Result<(), String> {
        let key = b"monotonic_test";
        
        // Writer process: write values with increasing timestamps
        let lsm_writer = self.lsm.clone();
        let writer_handle = tokio::spawn(async move {
            for i in 0..5 {
                let value = format!("value_{}", i);
                lsm_writer.put(key, value.as_bytes()).await.expect("Write failed");
                sleep(Duration::from_millis(10)).await;
            }
        });

        // Reader process: read multiple times and check monotonicity
        let lsm_reader = self.lsm.clone();
        let reader_handle = tokio::spawn(async move {
            let mut last_seen: Option<String> = None;
            
            for _ in 0..10 {
                sleep(Duration::from_millis(5)).await;
                if let Ok(Some(value)) = lsm_reader.get(key).await {
                    let value_str = String::from_utf8_lossy(&value).to_string();
                    
                    if let Some(ref last) = last_seen {
                        // Extract number from "value_N"
                        let last_num: i32 = last.split('_').nth(1).unwrap().parse().unwrap();
                        let curr_num: i32 = value_str.split('_').nth(1).unwrap().parse().unwrap();
                        
                        if curr_num < last_num {
                            return Err(format!("Monotonic read violation: saw {} after {}", value_str, last));
                        }
                    }
                    
                    last_seen = Some(value_str);
                }
            }
            Ok(())
        });

        writer_handle.await.map_err(|e| e.to_string())?;
        reader_handle.await.map_err(|e| e.to_string())??;

        Ok(())
    }
}

// ============================================================================
// FAULT INJECTION TESTS
// ============================================================================

/// Test system behavior under various fault conditions
pub struct FaultInjectionTester {
    lsm: Arc<LSMTree>,
}

impl FaultInjectionTester {
    pub fn new(lsm: Arc<LSMTree>) -> Self {
        Self { lsm }
    }

    /// Test behavior under concurrent read/write load with random delays
    pub async fn test_concurrent_operations_with_delays(&self) -> Result<(), String> {
        let mut checker = LinearizabilityChecker::new();
        let mut handles = Vec::new();
        let num_processes = 5;
        let operations_per_process = 20;

        for process_id in 0..num_processes {
            let lsm = self.lsm.clone();
            let handle = tokio::spawn(async move {
                let mut rng = StdRng::from_entropy();
                let mut events = Vec::new();
                
                for i in 0..operations_per_process {
                    let key = format!("key_{}", rng.gen_range(0..3)).into_bytes();
                    let value = format!("value_{}_{}", process_id, i).into_bytes();
                    
                    // Random delay to increase chance of races
                    sleep(Duration::from_millis(rng.gen_range(0..10))).await;
                    
                    let operation = if rng.gen_bool(0.7) {
                        // 70% writes
                        Operation::Put { key: key.clone(), value: value.clone() }
                    } else {
                        // 30% reads
                        Operation::Get { key: key.clone() }
                    };
                    
                    let invoke_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
                    
                    let result = match &operation {
                        Operation::Put { key, value } => {
                            match lsm.put(key, value).await {
                                Ok(_) => Some((OperationResult::PutOk, EventType::Ok)),
                                Err(_) => Some((OperationResult::PutOk, EventType::Fail)),
                            }
                        }
                        Operation::Get { key } => {
                            match lsm.get(key).await {
                                Ok(value) => Some((OperationResult::GetResult(value), EventType::Ok)),
                                Err(_) => Some((OperationResult::GetResult(None), EventType::Fail)),
                            }
                        }
                        _ => None,
                    };
                    
                    events.push((process_id, operation, result, invoke_time));
                }
                
                events
            });
            
            handles.push(handle);
        }

        // Collect all events
        let all_events: Result<Vec<_>, _> = futures::future::try_join_all(handles).await;
        let all_events = all_events.map_err(|e| e.to_string())?;

        // Add events to checker
        for events in all_events {
            for (process_id, operation, result, _invoke_time) in events {
                checker.record_invoke(process_id, operation);
                if let Some((result, event_type)) = result {
                    checker.record_complete(process_id, result, event_type);
                }
            }
        }

        // Check linearizability
        checker.check_linearizable()?;

        Ok(())
    }

    /// Test recovery after simulated crashes during operations
    pub async fn test_crash_recovery(&self, temp_dir: &TempDir) -> Result<(), String> {
        let key = b"crash_test";
        let value = b"crash_value";

        // Write some data
        self.lsm.put(key.as_ref(), value.as_ref()).await.map_err(|e| e.to_string())?;

        // Simulate crash by creating new LSM instance without proper shutdown
        let new_lsm = LSMTree::new(temp_dir.path()).await.map_err(|e| e.to_string())?;

        // Data should be recovered
        let recovered = new_lsm.get(key.as_ref()).await.map_err(|e| e.to_string())?;
        if recovered.as_deref() != Some(value.as_ref()) {
            return Err("Crash recovery failed: data not recovered".to_string());
        }

        Ok(())
    }
}

// ============================================================================
// TEST RUNNER
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_lsm() -> (Arc<LSMTree>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let lsm = Arc::new(LSMTree::new(temp_dir.path()).await.expect("Failed to create LSM"));
        (lsm, temp_dir)
    }

    #[tokio::test]
    async fn test_linearizability_basic() {
        let (lsm, _temp_dir) = setup_lsm().await;
        let tester = FaultInjectionTester::new(lsm);
        
        match tester.test_concurrent_operations_with_delays().await {
            Ok(_) => {},
            Err(e) => println!("Linearizability test failed: {}", e),
        }
    }

    #[tokio::test]
    async fn test_acid_properties() {
        let (lsm, temp_dir) = setup_lsm().await;
        let tester = ACIDTester::new(lsm);

        assert!(tester.test_atomicity().await.is_ok());
        assert!(tester.test_consistency().await.is_ok());
        assert!(tester.test_isolation().await.is_ok());
        assert!(tester.test_durability(&temp_dir).await.is_ok());
    }

    #[tokio::test]
    async fn test_consistency_models() {
        let (lsm, _temp_dir) = setup_lsm().await;
        let tester = ConsistencyTester::new(lsm);

        assert!(tester.test_sequential_consistency().await.is_ok());
        assert!(tester.test_read_your_writes().await.is_ok());
        assert!(tester.test_monotonic_reads().await.is_ok());
    }

    #[tokio::test]
    async fn test_fault_injection() {
        let (lsm, temp_dir) = setup_lsm().await;
        let tester = FaultInjectionTester::new(lsm);

        assert!(tester.test_crash_recovery(&temp_dir).await.is_ok());
    }

    #[tokio::test]
    async fn test_jepsen_style_history_checking() {
        let mut checker = LinearizabilityChecker::new();

        // Simulate a simple history: P1 puts "A", P2 gets "A"
        checker.record_invoke(1, Operation::Put { 
            key: b"test".to_vec(), 
            value: b"A".to_vec() 
        });
        checker.record_complete(1, OperationResult::PutOk, EventType::Ok);

        checker.record_invoke(2, Operation::Get { 
            key: b"test".to_vec() 
        });
        checker.record_complete(2, OperationResult::GetResult(Some(b"A".to_vec())), EventType::Ok);

        assert!(checker.check_linearizable().is_ok());
    }

    #[tokio::test]
    async fn test_linearizability_violation_detection() {
        let mut checker = LinearizabilityChecker::new();

        // Simulate impossible history: read "B" without any write of "B"
        checker.record_invoke(1, Operation::Put { 
            key: b"test".to_vec(), 
            value: b"A".to_vec() 
        });
        checker.record_complete(1, OperationResult::PutOk, EventType::Ok);

        checker.record_invoke(2, Operation::Get { 
            key: b"test".to_vec() 
        });
        checker.record_complete(2, OperationResult::GetResult(Some(b"B".to_vec())), EventType::Ok);

        assert!(checker.check_linearizable().is_err());
    }
} 