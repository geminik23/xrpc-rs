use crate::error::Result;
use serde::{Deserialize, Serialize};

/// Trait for encoding and decoding message payloads
pub trait Codec: Send + Sync + std::fmt::Debug {
    /// Encode a serializable value into bytes
    fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>>;

    /// Decode bytes into a deserializable value
    fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T>;
}

/// Default codec (Bincode codec)
#[derive(Debug, Clone, Copy, Default)]
pub struct BincodeCodec;

impl Codec for BincodeCodec {
    fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>> {
        Ok(bincode::serialize(data)?)
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        Ok(bincode::deserialize(data)?)
    }
}

/// JSON codec
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(data)?)
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        Ok(serde_json::from_slice(data)?)
    }
}

/// MessagePack codec - compact binary, cross-language compatible
#[cfg(feature = "codec-messagepack")]
#[derive(Debug, Clone, Copy, Default)]
pub struct MessagePackCodec;

#[cfg(feature = "codec-messagepack")]
impl Codec for MessagePackCodec {
    fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>> {
        rmp_serde::to_vec(data).map_err(|e| crate::error::RpcError::Serialization(e.to_string()))
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        rmp_serde::from_slice(data)
            .map_err(|e| crate::error::RpcError::Serialization(e.to_string()))
    }
}

/// CBOR codec - binary JSON (RFC 8949), self-describing
#[cfg(feature = "codec-cbor")]
#[derive(Debug, Clone, Copy, Default)]
pub struct CborCodec;

#[cfg(feature = "codec-cbor")]
impl Codec for CborCodec {
    fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        ciborium::into_writer(data, &mut buf)
            .map_err(|e| crate::error::RpcError::Serialization(e.to_string()))?;
        Ok(buf)
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        ciborium::from_reader(data)
            .map_err(|e| crate::error::RpcError::Serialization(e.to_string()))
    }
}

/// Postcard codec - minimal binary, no_std compatible
#[cfg(feature = "codec-postcard")]
#[derive(Debug, Clone, Copy, Default)]
pub struct PostcardCodec;

#[cfg(feature = "codec-postcard")]
impl Codec for PostcardCodec {
    fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>> {
        postcard::to_allocvec(data)
            .map_err(|e| crate::error::RpcError::Serialization(e.to_string()))
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        postcard::from_bytes(data).map_err(|e| crate::error::RpcError::Serialization(e.to_string()))
    }
}
