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
