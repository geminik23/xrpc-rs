use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::TransportResult;
use crate::message::Message;
use crate::transport::{Transport, TransportStats};

#[async_trait]
pub trait MessageTransport: Send + Sync + Debug {
    async fn send(&self, message: &Message) -> TransportResult<()>;
    async fn recv(&self) -> TransportResult<Message>;
    fn is_connected(&self) -> bool;
    fn is_healthy(&self) -> bool {
        self.is_connected()
    }
    async fn close(&self) -> TransportResult<()>;
    fn stats(&self) -> Option<TransportStats> {
        None
    }
}

#[derive(Debug)]
pub struct MessageTransportAdapter<T: Transport> {
    inner: T,
}

impl<T: Transport> MessageTransportAdapter<T> {
    pub fn new(transport: T) -> Self {
        Self { inner: transport }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[async_trait]
impl<T: Transport> MessageTransport for MessageTransportAdapter<T> {
    async fn send(&self, message: &Message) -> TransportResult<()> {
        let bytes = message.encode()?;
        self.inner.send(&bytes).await
    }

    async fn recv(&self) -> TransportResult<Message> {
        let bytes = self.inner.recv().await?;
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
impl<T: MessageTransport + ?Sized> MessageTransport for Arc<T> {
    async fn send(&self, message: &Message) -> TransportResult<()> {
        (**self).send(message).await
    }

    async fn recv(&self) -> TransportResult<Message> {
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
impl<T: MessageTransport + ?Sized> MessageTransport for Box<T> {
    async fn send(&self, message: &Message) -> TransportResult<()> {
        (**self).send(message).await
    }

    async fn recv(&self) -> TransportResult<Message> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::types::MessageType;
    use crate::transport::channel::{ChannelConfig, ChannelTransport};
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
    async fn test_message_transport_call_reply() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let mt1 = MessageTransportAdapter::new(t1);
        let mt2 = MessageTransportAdapter::new(t2);

        let req = TestRequest { value: 42 };
        let call_msg = Message::call("test_method", req.clone()).unwrap();
        let msg_id = call_msg.id;

        mt1.send(&call_msg).await.unwrap();

        let received = mt2.recv().await.unwrap();
        assert_eq!(received.id, msg_id);
        assert_eq!(received.msg_type, MessageType::Call);
        assert_eq!(received.method, "test_method");

        let received_req: TestRequest = received.deserialize_payload().unwrap();
        assert_eq!(received_req, req);

        let resp = TestResponse {
            result: "ok".to_string(),
        };
        let reply_msg = Message::reply(msg_id, resp.clone()).unwrap();
        mt2.send(&reply_msg).await.unwrap();

        let received_reply = mt1.recv().await.unwrap();
        assert_eq!(received_reply.id, msg_id);
        assert_eq!(received_reply.msg_type, MessageType::Reply);

        let received_resp: TestResponse = received_reply.deserialize_payload().unwrap();
        assert_eq!(received_resp, resp);
    }

    #[tokio::test]
    async fn test_message_transport_with_compression() {
        use crate::message::types::CompressionType;

        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("compress", config).unwrap();

        let mt1 = MessageTransportAdapter::new(t1);
        let mt2 = MessageTransportAdapter::new(t2);

        let large_data = TestRequest { value: 12345 };
        let mut msg = Message::call("compressed_method", large_data.clone()).unwrap();
        msg.metadata.compression = CompressionType::Lz4;

        mt1.send(&msg).await.unwrap();

        let received = mt2.recv().await.unwrap();
        assert_eq!(received.metadata.compression, CompressionType::Lz4);

        let received_data: TestRequest = received.deserialize_payload().unwrap();
        assert_eq!(received_data, large_data);
    }

    #[tokio::test]
    async fn test_message_transport_delegation() {
        let config = ChannelConfig::default();
        let (t1, _t2) = ChannelTransport::create_pair("delegate", config).unwrap();

        let mt1 = MessageTransportAdapter::new(t1);

        assert!(mt1.is_connected());
        assert!(mt1.is_healthy());
        assert!(mt1.stats().is_some());
    }
}
