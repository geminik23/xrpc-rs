use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, RpcError, TransportError, TransportResult};

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
    #[cfg(feature = "compression-lz4")]
    Lz4 = 1,
    #[cfg(feature = "compression-zstd")]
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
            #[cfg(feature = "compression-lz4")]
            1 => Ok(CompressionType::Lz4),
            #[cfg(feature = "compression-zstd")]
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
    /// Create flags from a byte
    pub fn from_u8(value: u8) -> Self {
        Self {
            compressed: (value & 0x01) != 0,
            streaming: (value & 0x02) != 0,
            batch: (value & 0x04) != 0,
        }
    }

    /// Convert flags to a byte
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

#[derive(Debug, Clone)]
pub struct Message {
    pub id: MessageId,
    pub msg_type: MessageType,
    pub method: String,
    pub payload: Bytes,
    pub metadata: MessageMetadata,
}

impl Message {
    pub fn call<T: Serialize>(method: impl Into<String>, request: T) -> Result<Self> {
        let payload = bincode::serialize(&request)?;
        Ok(Self {
            id: MessageId::new(),
            msg_type: MessageType::Call,
            method: method.into(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
        })
    }

    pub fn reply<T: Serialize>(id: MessageId, response: T) -> Result<Self> {
        let payload = bincode::serialize(&response)?;
        Ok(Self {
            id,
            msg_type: MessageType::Reply,
            method: String::new(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
        })
    }

    pub fn notification<T: Serialize>(method: impl Into<String>, data: T) -> Result<Self> {
        let payload = bincode::serialize(&data)?;
        Ok(Self {
            id: MessageId::new(),
            msg_type: MessageType::Notification,
            method: method.into(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
        })
    }

    pub fn error(id: MessageId, error_msg: impl Into<String>) -> Self {
        let error_msg = error_msg.into();
        let payload = bincode::serialize(&error_msg).unwrap_or_default();
        Self {
            id,
            msg_type: MessageType::Error,
            method: String::new(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
        }
    }

    /// Create a stream chunk message
    pub fn stream_chunk<T: Serialize>(stream_id: u64, sequence: u64, data: T) -> Result<Self> {
        let payload = bincode::serialize(&data)?;
        Ok(Self {
            id: MessageId::new(),
            msg_type: MessageType::StreamChunk,
            method: String::new(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new().with_stream(stream_id, sequence),
        })
    }

    /// Create a stream end message
    pub fn stream_end(stream_id: u64) -> Self {
        Self {
            id: MessageId::new(),
            msg_type: MessageType::StreamEnd,
            method: String::new(),
            payload: Bytes::new(),
            metadata: MessageMetadata::new().with_stream(stream_id, 0),
        }
    }

    /// Encode message to bytes
    pub fn encode(&self) -> TransportResult<BytesMut> {
        let method_bytes = self.method.as_bytes();
        let method_len = method_bytes.len();

        if method_len > u16::MAX as usize {
            return Err(TransportError::Protocol("Method name too long".to_string()));
        }

        let metadata_bytes = bincode::serialize(&self.metadata)
            .map_err(|e| TransportError::Protocol(e.to_string()))?;

        // Calculate total size
        let total_size = MIN_HEADER_SIZE + method_len + self.payload.len() + metadata_bytes.len();

        if total_size > MAX_MESSAGE_SIZE {
            return Err(TransportError::MessageTooLarge {
                size: total_size,
                max: MAX_MESSAGE_SIZE,
            });
        }

        let mut buf = BytesMut::with_capacity(total_size);

        // Magic bytes
        buf.put_slice(&MAGIC);

        // Version
        buf.put_u8(VERSION);

        // Flags
        let flags = MessageFlags {
            compressed: self.metadata.compression != CompressionType::None,
            streaming: false,
            batch: false,
        };
        buf.put_u8(flags.to_u8());

        // Total message length (excluding magic, version, flags, and length field)
        let msg_len = total_size - 10;
        buf.put_u32_le(msg_len as u32);

        // Message ID
        buf.put_u64_le(self.id.0);

        // Message type
        buf.put_u8(self.msg_type.to_u8());

        // Method name length and data
        buf.put_u16_le(method_len as u16);
        buf.put_slice(method_bytes);

        // Payload length and data
        buf.put_u32_le(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        // Metadata length and data
        buf.put_u32_le(metadata_bytes.len() as u32);
        buf.put_slice(&metadata_bytes);

        Ok(buf)
    }

    /// Decode message from wire bytes
    pub fn decode(mut buf: impl Buf) -> TransportResult<Self> {
        if buf.remaining() < MIN_HEADER_SIZE {
            return Err(TransportError::Protocol(
                "Buffer too small for message header".to_string(),
            ));
        }

        // Verify magic bytes
        let mut magic = [0u8; 4];
        buf.copy_to_slice(&mut magic);
        if magic != MAGIC {
            return Err(TransportError::Protocol(format!(
                "Invalid magic bytes: {:?}",
                magic
            )));
        }

        // Verify version
        let version = buf.get_u8();
        if version != VERSION {
            return Err(TransportError::Protocol(format!(
                "Unsupported protocol version: {}",
                version
            )));
        }

        // Parse flags
        let _flags = MessageFlags::from_u8(buf.get_u8());

        // Message length
        let msg_len = buf.get_u32_le() as usize;

        if buf.remaining() < msg_len {
            return Err(TransportError::Protocol("Incomplete message".to_string()));
        }

        // Message ID
        let id = MessageId(buf.get_u64_le());

        // Message type
        let msg_type = MessageType::from_u8(buf.get_u8())
            .map_err(|e| TransportError::Protocol(e.to_string()))?;

        // Method name
        let method_len = buf.get_u16_le() as usize;
        let mut method_bytes = vec![0u8; method_len];
        buf.copy_to_slice(&mut method_bytes);
        let method = String::from_utf8(method_bytes)
            .map_err(|e| TransportError::Protocol(format!("Invalid method name: {}", e)))?;

        // Payload
        let payload_len = buf.get_u32_le() as usize;
        let mut payload_bytes = vec![0u8; payload_len];
        buf.copy_to_slice(&mut payload_bytes);
        let payload = Bytes::from(payload_bytes);

        // Metadata
        let metadata_len = buf.get_u32_le() as usize;
        let mut metadata_bytes = vec![0u8; metadata_len];
        buf.copy_to_slice(&mut metadata_bytes);
        let metadata: MessageMetadata = bincode::deserialize(&metadata_bytes)
            .map_err(|e| TransportError::Protocol(format!("Invalid metadata: {}", e)))?;

        Ok(Self {
            id,
            msg_type,
            method,
            payload,
            metadata,
        })
    }

    pub fn deserialize_payload<'de, T: Deserialize<'de>>(&'de self) -> Result<T> {
        bincode::deserialize(&self.payload).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_call_reply() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Request {
            value1: i32,
            value2: i32,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Response {
            result: i32,
        }

        let req = Request {
            value1: 42,
            value2: 58,
        };
        let msg = Message::call("add", req).unwrap();
        let decoded_req: Request = msg.deserialize_payload().unwrap();
        assert_eq!(decoded_req.value1, 42);
        assert_eq!(decoded_req.value2, 58);

        let resp = Response { result: 100 };
        let reply = Message::reply(msg.id, resp).unwrap();
        let decoded_resp: Response = reply.deserialize_payload().unwrap();
        assert_eq!(decoded_resp.result, 100);
    }

    #[test]
    fn test_message_notification() {
        let msg = Message::notification("event", "data").unwrap();
        assert_eq!(msg.msg_type, MessageType::Notification);
        assert_eq!(msg.method, "event");
    }

    #[test]
    fn test_message_error() {
        let id = MessageId::new();
        let msg = Message::error(id, "something went wrong");
        assert_eq!(msg.msg_type, MessageType::Error);
        assert_eq!(msg.id, id);
    }

    #[test]
    fn test_metadata() {
        let meta = MessageMetadata::new()
            .with_timeout(5000)
            .with_compression(CompressionType::None);

        assert_eq!(meta.timeout_ms, Some(5000));
        assert_eq!(meta.compression, CompressionType::None);
        assert!(meta.timestamp > 0);
    }

    #[test]
    fn test_message_size_limit() {
        let large_payload = vec![0u8; MAX_MESSAGE_SIZE + 1];
        let msg = Message {
            id: MessageId::new(),
            msg_type: MessageType::Call,
            method: "test".to_string(),
            payload: Bytes::from(large_payload),
            metadata: MessageMetadata::new(),
        };

        let result = msg.encode();
        assert!(matches!(
            result,
            Err(TransportError::MessageTooLarge { .. })
        ));
    }

    #[test]
    fn test_round_trip_with_metadata() {
        let msg = Message {
            id: MessageId::new(),
            msg_type: MessageType::Call,
            method: "complex".to_string(),
            payload: Bytes::from(vec![1, 2, 3, 4, 5]),
            metadata: MessageMetadata::new().with_timeout(1000),
        };

        let encoded = msg.encode().unwrap();
        let decoded = Message::decode(encoded).unwrap();

        assert_eq!(decoded.id, msg.id);
        assert_eq!(decoded.method, msg.method);
        assert_eq!(decoded.payload, msg.payload);
        assert_eq!(decoded.metadata.timeout_ms, Some(1000));
    }

    #[test]
    fn test_streaming_messages() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct ChunkData {
            value: String,
        }

        let stream_id = 42u64;
        let sequence = 5u64;
        let data = ChunkData {
            value: "chunk data".to_string(),
        };

        // Test stream chunk
        let chunk = Message::stream_chunk(stream_id, sequence, data).unwrap();
        assert_eq!(chunk.msg_type, MessageType::StreamChunk);
        assert_eq!(chunk.metadata.stream_id, Some(stream_id));
        assert_eq!(chunk.metadata.sequence_number, Some(sequence));

        let decoded_data: ChunkData = chunk.deserialize_payload().unwrap();
        assert_eq!(decoded_data.value, "chunk data");

        // Test encode/decode round trip
        let encoded = chunk.encode().unwrap();
        let decoded = Message::decode(encoded).unwrap();
        assert_eq!(decoded.msg_type, MessageType::StreamChunk);
        assert_eq!(decoded.metadata.stream_id, Some(stream_id));
        assert_eq!(decoded.metadata.sequence_number, Some(sequence));

        // Test stream end
        let end = Message::stream_end(stream_id);
        assert_eq!(end.msg_type, MessageType::StreamEnd);
        assert_eq!(end.metadata.stream_id, Some(stream_id));
        assert_eq!(end.payload.len(), 0);

        // Test stream end encode/decode
        let encoded_end = end.encode().unwrap();
        let decoded_end = Message::decode(encoded_end).unwrap();
        assert_eq!(decoded_end.msg_type, MessageType::StreamEnd);
        assert_eq!(decoded_end.metadata.stream_id, Some(stream_id));
    }
}
