use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Result, RpcError};

pub const MAGIC: [u8; 4] = [0x58, 0x52, 0x50, 0x43]; // XRPC
pub const VERSION: u8 = 1;
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
pub const MIN_HEADER_SIZE: usize = 4 + 1 + 1 + 4 + 8 + 1;

static NEXT_MESSAGE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub u64);

impl MessageId {
    pub fn new() -> Self {
        MessageId(NEXT_MESSAGE_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub fn from_raw(id: u64) -> Self {
        MessageId(id)
    }

    pub fn raw(&self) -> u64 {
        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    Call = 0,
    Reply = 1,
    Notification = 2,
    Error = 3,
    StreamChunk = 4,
    StreamEnd = 5,
}

impl MessageType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(MessageType::Call),
            1 => Ok(MessageType::Reply),
            2 => Ok(MessageType::Notification),
            3 => Ok(MessageType::Error),
            4 => Ok(MessageType::StreamChunk),
            5 => Ok(MessageType::StreamEnd),
            _ => Err(RpcError::InvalidMessage(format!(
                "Unknown message type: {}",
                value
            ))),
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Compression algorithm used for message payload
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

impl CompressionType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Lz4),
            2 => Ok(CompressionType::Zstd),
            _ => Err(RpcError::InvalidMessage(format!(
                "Unknown compression type: {}",
                value
            ))),
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Message flags packed into a single byte
#[derive(Debug, Clone, Copy, Default)]
pub struct MessageFlags {
    /// Whether the message payload is compressed
    pub compressed: bool,
    /// Whether this is a streaming message
    pub streaming: bool,
    /// Whether this is part of a batch
    pub batch: bool,
}

impl MessageFlags {
    pub fn from_u8(value: u8) -> Self {
        Self {
            compressed: (value & 0x01) != 0,
            streaming: (value & 0x02) != 0,
            batch: (value & 0x04) != 0,
        }
    }

    pub fn to_u8(self) -> u8 {
        let mut flags = 0u8;
        if self.compressed {
            flags |= 0x01;
        }
        if self.streaming {
            flags |= 0x02;
        }
        if self.batch {
            flags |= 0x04;
        }
        flags
    }
}
