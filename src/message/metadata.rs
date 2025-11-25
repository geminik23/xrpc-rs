use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use super::types::CompressionType;

/// Optional metadata for messages
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageMetadata {
    /// Timestamp when message was created (microseconds since epoch)
    pub timestamp: u64,
    /// Timeout in milliseconds
    pub timeout_ms: Option<u32>,
    /// Compression algorithm used
    pub compression: CompressionType,
    /// Stream ID for streaming messages
    pub stream_id: Option<u64>,
    /// Sequence number for ordered delivery
    pub sequence_number: Option<u64>,
}

impl MessageMetadata {
    pub fn new() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            timeout_ms: None,
            compression: CompressionType::None,
            stream_id: None,
            sequence_number: None,
        }
    }

    pub fn with_timeout(mut self, timeout_ms: u32) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set stream ID
    pub fn with_stream(mut self, stream_id: u64, sequence: u64) -> Self {
        self.stream_id = Some(stream_id);
        self.sequence_number = Some(sequence);
        self
    }
}
