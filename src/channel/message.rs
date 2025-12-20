//! Message-level channel for RPC communication (Layer 2).

use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

use crate::codec::{BincodeCodec, Codec};
use crate::error::TransportResult;
use crate::message::Message;
use crate::transport::{FrameTransport, TransportStats};

/// Message-level channel for RPC communication (Layer 2).
#[async_trait]
pub trait MessageChannel<C: Codec = BincodeCodec>: Send + Sync + Debug {
    /// Send an RPC message.
    async fn send(&self, message: &Message<C>) -> TransportResult<()>;

    /// Receive an RPC message.
    async fn recv(&self) -> TransportResult<Message<C>>;

    /// Check if the channel is connected.
    fn is_connected(&self) -> bool;

    /// Check if the channel is healthy.
    fn is_healthy(&self) -> bool {
        self.is_connected()
    }

    /// Close the channel.
    async fn close(&self) -> TransportResult<()>;

    /// Get transport statistics.
    fn stats(&self) -> Option<TransportStats> {
        None
    }
}

/// Adapter that wraps a FrameTransport to provide MessageChannel functionality.
#[derive(Debug)]
pub struct MessageChannelAdapter<F: FrameTransport, C: Codec = BincodeCodec> {
    inner: F,
    _codec: std::marker::PhantomData<C>,
}

impl<F: FrameTransport> MessageChannelAdapter<F, BincodeCodec> {
    /// Create a new adapter with default BincodeCodec.
    pub fn new(transport: F) -> Self {
        Self {
            inner: transport,
            _codec: std::marker::PhantomData,
        }
    }
}

impl<F: FrameTransport, C: Codec> MessageChannelAdapter<F, C> {
    /// Create a new adapter with a specific codec.
    pub fn with_codec(transport: F) -> Self {
        Self {
            inner: transport,
            _codec: std::marker::PhantomData,
        }
    }

    /// Get a reference to the inner transport.
    pub fn inner(&self) -> &F {
        &self.inner
    }

    /// Consume the adapter and return the inner transport.
    pub fn into_inner(self) -> F {
        self.inner
    }
}

#[async_trait]
impl<F: FrameTransport, C: Codec + Default> MessageChannel<C> for MessageChannelAdapter<F, C> {
    async fn send(&self, message: &Message<C>) -> TransportResult<()> {
        let bytes = message.encode()?;
        self.inner.send_frame(&bytes).await
    }

    async fn recv(&self) -> TransportResult<Message<C>> {
        let bytes = self.inner.recv_frame().await?;
        Message::decode(bytes)
    }

    fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    async fn close(&self) -> TransportResult<()> {
        self.inner.close().await
    }

    fn stats(&self) -> Option<TransportStats> {
        self.inner.stats()
    }
}

#[async_trait]
impl<T: MessageChannel<C> + ?Sized, C: Codec + Default> MessageChannel<C> for Arc<T> {
    async fn send(&self, message: &Message<C>) -> TransportResult<()> {
        (**self).send(message).await
    }

    async fn recv(&self) -> TransportResult<Message<C>> {
        (**self).recv().await
    }

    fn is_connected(&self) -> bool {
        (**self).is_connected()
    }

    fn is_healthy(&self) -> bool {
        (**self).is_healthy()
    }

    async fn close(&self) -> TransportResult<()> {
        (**self).close().await
    }

    fn stats(&self) -> Option<TransportStats> {
        (**self).stats()
    }
}

#[async_trait]
impl<T: MessageChannel<C> + ?Sized, C: Codec + Default> MessageChannel<C> for Box<T> {
    async fn send(&self, message: &Message<C>) -> TransportResult<()> {
        (**self).send(message).await
    }

    async fn recv(&self) -> TransportResult<Message<C>> {
        (**self).recv().await
    }

    fn is_connected(&self) -> bool {
        (**self).is_connected()
    }

    fn is_healthy(&self) -> bool {
        (**self).is_healthy()
    }

    async fn close(&self) -> TransportResult<()> {
        (**self).close().await
    }

    fn stats(&self) -> Option<TransportStats> {
        (**self).stats()
    }
}

// Deprecated aliases for backward compatibility
#[deprecated(since = "0.2.0", note = "Use MessageChannel instead")]
pub type MessageTransport<C> = dyn MessageChannel<C>;

#[deprecated(since = "0.2.0", note = "Use MessageChannelAdapter instead")]
pub type MessageTransportAdapter<F, C> = MessageChannelAdapter<F, C>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::types::MessageType;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestRequest {
        value: i32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestResponse {
        result: String,
    }

    #[tokio::test]
    async fn test_message_channel_call_reply() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let ch1 = MessageChannelAdapter::new(t1);
        let ch2 = MessageChannelAdapter::new(t2);

        let req = TestRequest { value: 42 };
        let call_msg: Message = Message::call("test_method", req.clone()).unwrap();
        let msg_id = call_msg.id;

        ch1.send(&call_msg).await.unwrap();

        let received = ch2.recv().await.unwrap();
        assert_eq!(received.id, msg_id);
        assert_eq!(received.msg_type, MessageType::Call);
        assert_eq!(received.method, "test_method");

        let received_req: TestRequest = received.deserialize_payload().unwrap();
        assert_eq!(received_req, req);

        let resp = TestResponse {
            result: "ok".to_string(),
        };
        let reply_msg: Message = Message::reply(msg_id, resp.clone()).unwrap();
        ch2.send(&reply_msg).await.unwrap();

        let received_reply = ch1.recv().await.unwrap();
        assert_eq!(received_reply.id, msg_id);
        assert_eq!(received_reply.msg_type, MessageType::Reply);

        let received_resp: TestResponse = received_reply.deserialize_payload().unwrap();
        assert_eq!(received_resp, resp);
    }

    #[tokio::test]
    async fn test_message_channel_with_compression() {
        use crate::message::types::CompressionType;

        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("compress", config).unwrap();

        let ch1 = MessageChannelAdapter::new(t1);
        let ch2 = MessageChannelAdapter::new(t2);

        let large_data = TestRequest { value: 12345 };
        let mut msg: Message = Message::call("compressed_method", large_data.clone()).unwrap();
        msg.metadata.compression = CompressionType::Lz4;

        ch1.send(&msg).await.unwrap();

        let received = ch2.recv().await.unwrap();
        assert_eq!(received.metadata.compression, CompressionType::Lz4);

        let received_data: TestRequest = received.deserialize_payload().unwrap();
        assert_eq!(received_data, large_data);
    }

    #[tokio::test]
    async fn test_message_channel_delegation() {
        let config = ChannelConfig::default();
        let (t1, _t2) = ChannelFrameTransport::create_pair("delegate", config).unwrap();

        let ch1 = MessageChannelAdapter::new(t1);

        assert!(ch1.is_connected());
        assert!(ch1.is_healthy());
        assert!(ch1.stats().is_some());
    }
}
