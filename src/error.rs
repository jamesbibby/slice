use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, SliceError>;

#[derive(Error, Debug)]
pub enum SliceError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    
    #[error("Corruption detected: {0}")]
    Corruption(String),
    
    #[error("Key not found")]
    KeyNotFound,
    
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    
    #[error("Compaction error: {0}")]
    Compaction(String),
    
    #[error("WAL error: {0}")]
    Wal(String),
} 