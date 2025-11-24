use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::{TransportError, TransportResult};
use crate::transport::{Transport, TransportStats};

/// Raw transport that bypasses Message protocol overhead
pub struct RawTransport<T: Transport> {
    inner: T,
    stats: Arc<Mutex<TransportStats>>,
}

impl<T: Transport> RawTransport<T> {
    pub fn new(transport: T) -> Self {
        Self {
            inner: transport,
            stats: Arc::new(Mutex::new(TransportStats::default())),
        }
    }

    /// Send typed data directly without Message wrapper
    pub async fn send_direct<D: Serialize>(&self, data: &D) -> TransportResult<()> {
        let encoded =
            bincode::serialize(data).map_err(|e| TransportError::Protocol(e.to_string()))?;

        self.inner.send(&encoded).await?;

        let mut stats = self.stats.lock();
        stats.messages_sent += 1;
        stats.bytes_sent += encoded.len() as u64;

        Ok(())
    }

    /// Receive typed data directly
    pub async fn recv_direct<D: for<'de> Deserialize<'de>>(&self) -> TransportResult<D> {
        let bytes = self.inner.recv().await?;

        let data =
            bincode::deserialize(&bytes).map_err(|e| TransportError::Protocol(e.to_string()))?;

        let mut stats = self.stats.lock();
        stats.messages_received += 1;
        stats.bytes_received += bytes.len() as u64;

        Ok(data)
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn stats(&self) -> TransportStats {
        self.stats.lock().clone()
    }
}

#[async_trait]
impl<T: Transport> Transport for RawTransport<T> {
    async fn send(&self, data: &[u8]) -> TransportResult<()> {
        self.inner.send(data).await
    }

    async fn recv(&self) -> TransportResult<Bytes> {
        self.inner.recv().await
    }

    fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    async fn close(&self) -> TransportResult<()> {
        self.inner.close().await
    }

    fn stats(&self) -> Option<TransportStats> {
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

impl<T: Transport> std::fmt::Debug for RawTransport<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawTransport")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Typed channel for direct communication
pub struct TypedChannel<Req, Resp, T: Transport> {
    transport: RawTransport<T>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req, Resp, T: Transport> TypedChannel<Req, Resp, T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport: RawTransport::new(transport),
            _phantom: PhantomData,
        }
    }

    pub async fn send(&self, request: &Req) -> TransportResult<()>
    where
        Req: Serialize,
    {
        self.transport.send_direct(request).await
    }

    pub async fn recv(&self) -> TransportResult<Resp>
    where
        Resp: for<'de> Deserialize<'de>,
    {
        self.transport.recv_direct().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::channel::{ChannelConfig, ChannelTransport};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestRequest {
        id: u64,
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse {
        result: i32,
    }

    #[tokio::test]
    async fn test_direct_send_recv() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let dt1 = RawTransport::new(t1);
        let dt2 = RawTransport::new(t2);

        let req = TestRequest {
            id: 123,
            name: "test".to_string(),
        };

        dt1.send_direct(&req).await.unwrap();
        let received: TestRequest = dt2.recv_direct().await.unwrap();

        assert_eq!(received, req);
    }

    #[tokio::test]
    async fn test_typed_channel() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let ch1: TypedChannel<TestRequest, TestResponse, _> = TypedChannel::new(t1);
        let ch2: TypedChannel<TestResponse, TestRequest, _> = TypedChannel::new(t2);

        let req = TestRequest {
            id: 456,
            name: "hello".to_string(),
        };

        ch1.send(&req).await.unwrap();
        let received: TestRequest = ch2.recv().await.unwrap();
        assert_eq!(received, req);

        let resp = TestResponse { result: 42 };
        ch2.send(&resp).await.unwrap();
        let received_resp: TestResponse = ch1.recv().await.unwrap();
        assert_eq!(received_resp, resp);
    }
}
