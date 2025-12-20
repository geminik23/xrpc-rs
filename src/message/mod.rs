pub mod metadata;
pub mod types;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use self::metadata::MessageMetadata;
use self::types::{
    CompressionType, MAGIC, MAX_MESSAGE_SIZE, MIN_HEADER_SIZE, MessageFlags, MessageId,
    MessageType, VERSION,
};
use crate::codec::{BincodeCodec, Codec};
use crate::error::{Result, TransportError, TransportResult};

#[derive(Debug, Clone)]
pub struct Message<C: Codec = BincodeCodec> {
    pub id: MessageId,
    pub msg_type: MessageType,
    pub method: String,
    pub payload: Bytes,
    pub metadata: MessageMetadata,
    pub codec: C,
}

impl<C: Codec + Default> Message<C> {
    pub fn new(
        id: MessageId,
        msg_type: MessageType,
        method: impl Into<String>,
        payload: Bytes,
        metadata: MessageMetadata,
    ) -> Self {
        Self {
            id,
            msg_type,
            method: method.into(),
            payload,
            metadata,
            codec: C::default(),
        }
    }

    pub fn call<T: Serialize>(method: impl Into<String>, request: T) -> Result<Self> {
        let codec = C::default();
        let payload = codec.encode(&request)?;
        Ok(Self {
            id: MessageId::new(),
            msg_type: MessageType::Call,
            method: method.into(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
            codec,
        })
    }

    pub fn reply<T: Serialize>(id: MessageId, response: T) -> Result<Self> {
        let codec = C::default();
        let payload = codec.encode(&response)?;
        Ok(Self {
            id,
            msg_type: MessageType::Reply,
            method: String::new(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
            codec,
        })
    }

    pub fn notification<T: Serialize>(method: impl Into<String>, data: T) -> Result<Self> {
        let codec = C::default();
        let payload = codec.encode(&data)?;
        Ok(Self {
            id: MessageId::new(),
            msg_type: MessageType::Notification,
            method: method.into(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
            codec,
        })
    }

    pub fn error(id: MessageId, error_msg: impl Into<String>) -> Self {
        let error_msg = error_msg.into();
        // Errors are always simple strings, we use the codec to serialize them
        // fallback to empty if serialization fails (shouldn't happen for string)
        let codec = C::default();
        let payload = codec.encode(&error_msg).unwrap_or_default();
        Self {
            id,
            msg_type: MessageType::Error,
            method: String::new(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new(),
            codec,
        }
    }

    /// Create a stream chunk message
    pub fn stream_chunk<T: Serialize>(stream_id: u64, sequence: u64, data: T) -> Result<Self> {
        let codec = C::default();
        let payload = codec.encode(&data)?;
        Ok(Self {
            id: MessageId::new(),
            msg_type: MessageType::StreamChunk,
            method: String::new(),
            payload: Bytes::from(payload),
            metadata: MessageMetadata::new().with_stream(stream_id, sequence),
            codec,
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
            codec: C::default(),
        }
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

        // Payload (compressed)
        let payload_len = buf.get_u32_le() as usize;
        let mut payload_bytes = vec![0u8; payload_len];
        buf.copy_to_slice(&mut payload_bytes);

        // Metadata
        let metadata_len = buf.get_u32_le() as usize;
        let mut metadata_bytes = vec![0u8; metadata_len];
        buf.copy_to_slice(&mut metadata_bytes);

        // Metadata is always encoded with bincode in current protocol version
        let metadata: MessageMetadata = bincode::deserialize(&metadata_bytes)
            .map_err(|e| TransportError::Protocol(format!("Invalid metadata: {}", e)))?;

        // Decompress payload if needed
        let payload = Self::decompress_payload(&payload_bytes, &metadata)?;

        Ok(Self {
            id,
            msg_type,
            method,
            payload,
            metadata,
            codec: C::default(),
        })
    }
}

impl<C: Codec> Message<C> {
    /// Encode message to bytes
    pub fn encode(&self) -> TransportResult<BytesMut> {
        let method_bytes = self.method.as_bytes();
        let method_len = method_bytes.len();

        if method_len > u16::MAX as usize {
            return Err(TransportError::Protocol("Method name too long".to_string()));
        }

        // Compress payload if needed
        let payload_to_write = self.compress_payload()?;

        // Metadata is always encoded with bincode in current protocol version
        let metadata_bytes = bincode::serialize(&self.metadata)
            .map_err(|e| TransportError::Protocol(e.to_string()))?;

        // Calculate total size
        let total_size =
            MIN_HEADER_SIZE + method_len + payload_to_write.len() + metadata_bytes.len();

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

        // Flags - set streaming for stream messages
        let flags = MessageFlags {
            compressed: self.metadata.compression != CompressionType::None,
            streaming: matches!(
                self.msg_type,
                MessageType::StreamChunk | MessageType::StreamEnd
            ),
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
        buf.put_u32_le(payload_to_write.len() as u32);
        buf.put_slice(&payload_to_write);

        // Metadata length and data
        buf.put_u32_le(metadata_bytes.len() as u32);
        buf.put_slice(&metadata_bytes);

        Ok(buf)
    }

    /// Compress payload based on compression type
    fn compress_payload(&self) -> TransportResult<Vec<u8>> {
        match self.metadata.compression {
            CompressionType::None => Ok(self.payload.to_vec()),
            CompressionType::Lz4 => lz4::block::compress(&self.payload, None, true)
                .map_err(|e| TransportError::Protocol(format!("LZ4 compression failed: {}", e))),
            CompressionType::Zstd => zstd::bulk::compress(&self.payload, 3)
                .map_err(|e| TransportError::Protocol(format!("Zstd compression failed: {}", e))),
        }
    }

    /// Decompress payload based on compression type in metadata
    fn decompress_payload(compressed: &[u8], metadata: &MessageMetadata) -> TransportResult<Bytes> {
        let decompressed = match metadata.compression {
            CompressionType::None => compressed.to_vec(),
            CompressionType::Lz4 => lz4::block::decompress(compressed, None).map_err(|e| {
                TransportError::Protocol(format!("LZ4 decompression failed: {}", e))
            })?,
            CompressionType::Zstd => {
                zstd::bulk::decompress(compressed, MAX_MESSAGE_SIZE).map_err(|e| {
                    TransportError::Protocol(format!("Zstd decompression failed: {}", e))
                })?
            }
        };

        Ok(Bytes::from(decompressed))
    }

    pub fn deserialize_payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
        self.codec.decode(&self.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Request {
        value1: String,
        value2: i32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Response {
        result: bool,
    }

    #[test]
    fn test_message_call_reply() {
        let req = Request {
            value1: "test".to_string(),
            value2: 42,
        };

        // Create call message
        let call_msg = Message::<BincodeCodec>::call("test_method", req.clone()).unwrap();
        assert_eq!(call_msg.msg_type, MessageType::Call);
        assert_eq!(call_msg.method, "test_method");

        // Encode and decode
        let mut buf = call_msg.encode().unwrap();
        let decoded_msg: Message = Message::decode(&mut buf).unwrap();

        assert_eq!(decoded_msg.id, call_msg.id);
        assert_eq!(decoded_msg.method, call_msg.method);

        // Deserialize payload
        let decoded_req: Request = decoded_msg.deserialize_payload().unwrap();
        assert_eq!(decoded_req, req);

        // Create reply
        let resp = Response { result: true };
        let reply_msg = Message::<BincodeCodec>::reply(call_msg.id, resp.clone()).unwrap();

        let mut buf = reply_msg.encode().unwrap();
        let decoded_reply: Message = Message::decode(&mut buf).unwrap();

        let decoded_resp: Response = decoded_reply.deserialize_payload().unwrap();
        assert_eq!(decoded_resp, resp);
    }

    #[test]
    fn test_json_codec() {
        let req = Request {
            value1: "json".to_string(),
            value2: 123,
        };

        // Use JsonCodec explicitly
        let call_msg = Message::<JsonCodec>::call("json_method", req.clone()).unwrap();

        // Verify payload is JSON
        let payload_str = std::str::from_utf8(&call_msg.payload).unwrap();
        assert!(payload_str.contains("\"value1\":\"json\""));

        let mut buf = call_msg.encode().unwrap();
        let decoded_msg: Message<JsonCodec> = Message::decode(&mut buf).unwrap();

        let decoded_req: Request = decoded_msg.deserialize_payload().unwrap();
        assert_eq!(decoded_req, req);
    }

    #[test]
    fn test_metadata() {
        let req = Request {
            value1: "s".to_string(),
            value2: 1,
        };
        let mut msg = Message::<BincodeCodec>::call("m", req).unwrap();

        msg.metadata = msg
            .metadata
            .with_timeout(1000)
            .with_compression(CompressionType::Lz4);

        let mut buf = msg.encode().unwrap();
        let decoded: Message = Message::decode(&mut buf).unwrap();

        assert_eq!(decoded.metadata.timeout_ms, Some(1000));
        assert_eq!(decoded.metadata.compression, CompressionType::Lz4);
    }

    #[test]
    fn test_streaming_flag() {
        // StreamChunk should have streaming flag set
        let chunk = Message::<BincodeCodec>::stream_chunk(1, 0, 42i32).unwrap();
        let buf = chunk.encode().unwrap();
        let flags = MessageFlags::from_u8(buf[5]); // byte 5 is flags
        assert!(flags.streaming, "StreamChunk should have streaming flag");

        // StreamEnd should have streaming flag set
        let end = Message::<BincodeCodec>::stream_end(1);
        let buf = end.encode().unwrap();
        let flags = MessageFlags::from_u8(buf[5]);
        assert!(flags.streaming, "StreamEnd should have streaming flag");

        // Call should not have streaming flag
        let call = Message::<BincodeCodec>::call("test", ()).unwrap();
        let buf = call.encode().unwrap();
        let flags = MessageFlags::from_u8(buf[5]);
        assert!(!flags.streaming, "Call should not have streaming flag");
    }
}
