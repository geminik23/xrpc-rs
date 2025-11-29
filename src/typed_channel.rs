use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use crate::codec::{BincodeCodec, Codec};
use crate::error::TransportResult;
use crate::transport::Transport;

/// A strongly typed channel that sends and receives specific types using a configurable codec.
/// This abstraction sits on top of any `Transport` and handles serialization/deserialization.
pub struct TypedChannel<Req, Resp, T: Transport, C: Codec = BincodeCodec> {
    transport: T,
    codec: C,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req, Resp, T: Transport> TypedChannel<Req, Resp, T, BincodeCodec> {
    /// Create a new TypedChannel with default Bincode codec
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            codec: BincodeCodec,
            _phantom: PhantomData,
        }
    }
}

impl<Req, Resp, T: Transport, C: Codec> TypedChannel<Req, Resp, T, C> {
    /// Create a new TypedChannel with a specific codec
    pub fn with_codec(transport: T, codec: C) -> Self {
        Self {
            transport,
            codec,
            _phantom: PhantomData,
        }
    }

    /// Send a request
    pub async fn send(&self, request: &Req) -> TransportResult<()>
    where
        Req: Serialize,
    {
        let data = self
            .codec
            .encode(request)
            .map_err(|e| crate::error::TransportError::Protocol(e.to_string()))?;
        self.transport.send(&data).await
    }

    /// Receive a response
    pub async fn recv(&self) -> TransportResult<Resp>
    where
        Resp: for<'de> Deserialize<'de>,
    {
        let bytes = self.transport.recv().await?;
        let data = self
            .codec
            .decode(&bytes)
            .map_err(|e| crate::error::TransportError::Protocol(e.to_string()))?;
        Ok(data)
    }

    /// Get reference to underlying transport
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Consumes the TypedChannel and returns the underlying transport
    pub fn into_inner(self) -> T {
        self.transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::JsonCodec;
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
    async fn test_typed_channel_bincode() {
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

    #[tokio::test]
    async fn test_typed_channel_json() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test_json", config).unwrap();

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
