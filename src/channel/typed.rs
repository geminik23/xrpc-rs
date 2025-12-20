//! Strongly-typed bidirectional channel (Layer 2).

use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::codec::{BincodeCodec, Codec};
use crate::error::TransportResult;
use crate::transport::FrameTransport;

/// Strongly-typed channel with fixed request/response types (Layer 2).
pub struct TypedChannel<Req, Resp, F: FrameTransport, C: Codec = BincodeCodec> {
    transport: F,
    codec: C,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req, Resp, F: FrameTransport> TypedChannel<Req, Resp, F, BincodeCodec> {
    /// Create a new TypedChannel with default BincodeCodec.
    pub fn new(transport: F) -> Self {
        Self {
            transport,
            codec: BincodeCodec,
            _phantom: PhantomData,
        }
    }
}

impl<Req, Resp, F: FrameTransport, C: Codec> TypedChannel<Req, Resp, F, C> {
    /// Create a new TypedChannel with a specific codec.
    pub fn with_codec(transport: F, codec: C) -> Self {
        Self {
            transport,
            codec,
            _phantom: PhantomData,
        }
    }

    /// Send a request.
    pub async fn send(&self, request: &Req) -> TransportResult<()>
    where
        Req: Serialize,
    {
        let data = self
            .codec
            .encode(request)
            .map_err(|e| crate::error::TransportError::Protocol(e.to_string()))?;
        self.transport.send_frame(&data).await
    }

    /// Receive a response.
    pub async fn recv(&self) -> TransportResult<Resp>
    where
        Resp: for<'de> Deserialize<'de>,
    {
        let bytes = self.transport.recv_frame().await?;
        self.codec
            .decode(&bytes)
            .map_err(|e| crate::error::TransportError::Protocol(e.to_string()))
    }

    /// Get a reference to the inner transport.
    pub fn transport(&self) -> &F {
        &self.transport
    }

    /// Consume the channel and return the inner transport.
    pub fn into_inner(self) -> F {
        self.transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};

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
    async fn test_typed_channel_bincode() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

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

    #[tokio::test]
    async fn test_typed_channel_json() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test_json", config).unwrap();

        let ch1 = TypedChannel::<TestRequest, TestResponse, _, _>::with_codec(t1, JsonCodec);
        let ch2 = TypedChannel::<TestResponse, TestRequest, _, _>::with_codec(t2, JsonCodec);

        let req = TestRequest {
            id: 789,
            name: "json".to_string(),
        };

        ch1.send(&req).await.unwrap();
        let received: TestRequest = ch2.recv().await.unwrap();
        assert_eq!(received, req);
    }
}
