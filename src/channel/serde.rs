//! Flexible serde-based channel for per-call typed communication (Layer 2).

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::codec::{BincodeCodec, Codec};
use crate::error::{TransportError, TransportResult};
use crate::transport::{FrameTransport, TransportStats};

/// Flexible serde-based channel for per-call typed communication (Layer 2).
#[derive(Debug)]
pub struct SerdeChannel<F: FrameTransport, C: Codec = BincodeCodec> {
    inner: F,
    codec: C,
    stats: Arc<Mutex<TransportStats>>,
}

impl<F: FrameTransport> SerdeChannel<F, BincodeCodec> {
    /// Create a new SerdeChannel with default BincodeCodec.
    pub fn new(transport: F) -> Self {
        Self {
            inner: transport,
            codec: BincodeCodec,
            stats: Arc::new(Mutex::new(TransportStats::default())),
        }
    }
}

impl<F: FrameTransport, C: Codec> SerdeChannel<F, C> {
    /// Create a new SerdeChannel with a specific codec.
    pub fn with_codec(transport: F, codec: C) -> Self {
        Self {
            inner: transport,
            codec,
            stats: Arc::new(Mutex::new(TransportStats::default())),
        }
    }

    /// Send typed data with serialization.
    pub async fn send<T: Serialize>(&self, data: &T) -> TransportResult<()> {
        let encoded = self
            .codec
            .encode(data)
            .map_err(|e| TransportError::Protocol(e.to_string()))?;

        self.inner.send_frame(&encoded).await?;

        let mut stats = self.stats.lock();
        stats.messages_sent += 1;
        stats.bytes_sent += encoded.len() as u64;

        Ok(())
    }

    /// Receive typed data with deserialization.
    pub async fn recv<T: for<'de> Deserialize<'de>>(&self) -> TransportResult<T> {
        let bytes = self.inner.recv_frame().await?;

        let data = self
            .codec
            .decode(&bytes)
            .map_err(|e| TransportError::Protocol(e.to_string()))?;

        let mut stats = self.stats.lock();
        stats.messages_received += 1;
        stats.bytes_received += bytes.len() as u64;

        Ok(data)
    }

    /// Get a reference to the inner transport.
    pub fn inner(&self) -> &F {
        &self.inner
    }

    /// Get transport statistics.
    pub fn stats(&self) -> TransportStats {
        self.stats.lock().clone()
    }
}

// Deprecated alias for backward compatibility
#[deprecated(since = "0.2.0", note = "Use SerdeChannel instead")]
pub type RawTransport<F> = SerdeChannel<F, BincodeCodec>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestRequest {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_serde_channel_send_recv() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let ch1 = SerdeChannel::new(t1);
        let ch2 = SerdeChannel::new(t2);

        let req = TestRequest {
            id: 123,
            name: "test".to_string(),
        };

        ch1.send(&req).await.unwrap();
        let received: TestRequest = ch2.recv().await.unwrap();

        assert_eq!(received, req);
    }

    #[tokio::test]
    async fn test_serde_channel_stats() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let ch1 = SerdeChannel::new(t1);
        let ch2 = SerdeChannel::new(t2);

        ch1.send(&42i32).await.unwrap();
        let _: i32 = ch2.recv().await.unwrap();

        let stats1 = ch1.stats();
        assert_eq!(stats1.messages_sent, 1);
        assert!(stats1.bytes_sent > 0);

        let stats2 = ch2.stats();
        assert_eq!(stats2.messages_received, 1);
        assert!(stats2.bytes_received > 0);
    }
}
