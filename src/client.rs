use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::codec::{BincodeCodec, Codec};
use crate::error::{Result, RpcError};
use crate::message::Message;
use crate::message::types::{MessageId, MessageType};
use crate::streaming::{StreamManager, StreamReceiver, next_stream_id};
use crate::transport::message_transport::MessageTransport;

pub struct RpcClient<T, C: Codec = BincodeCodec>
where
    T: MessageTransport<C>,
{
    transport: Arc<T>,
    pending: Arc<Mutex<HashMap<MessageId, oneshot::Sender<Result<Message<C>>>>>>,
    stream_manager: Arc<StreamManager<C>>,
    codec: C,
    running: Arc<AtomicBool>,
    default_timeout: Duration,
}

impl<T: MessageTransport<BincodeCodec> + 'static> RpcClient<T, BincodeCodec> {
    pub fn new(transport: T) -> Self {
        Self::with_timeout(transport, Duration::from_secs(30))
    }

    pub fn with_timeout(transport: T, default_timeout: Duration) -> Self {
        Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            stream_manager: Arc::new(StreamManager::new()),
            codec: BincodeCodec,
            running: Arc::new(AtomicBool::new(false)),
            default_timeout,
        }
    }
}

impl<T, C> RpcClient<T, C>
where
    T: MessageTransport<C> + 'static,
    C: Codec + Clone + Default + 'static,
{
    pub fn with_codec(transport: T, codec: C) -> Self {
        Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            stream_manager: Arc::new(StreamManager::with_codec(codec.clone())),
            codec,
            running: Arc::new(AtomicBool::new(false)),
            default_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_codec_and_timeout(transport: T, codec: C, default_timeout: Duration) -> Self {
        Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            stream_manager: Arc::new(StreamManager::with_codec(codec.clone())),
            codec,
            running: Arc::new(AtomicBool::new(false)),
            default_timeout,
        }
    }

    pub fn start(&self) -> RpcClientHandle {
        self.running.store(true, Ordering::Release);

        let transport = self.transport.clone();
        let pending = self.pending.clone();
        let stream_manager = self.stream_manager.clone();
        let running = self.running.clone();

        let handle = tokio::spawn(async move {
            while running.load(Ordering::Acquire) {
                match transport.recv().await {
                    Ok(message) => match message.msg_type {
                        MessageType::Reply | MessageType::Error => {
                            if let Some(tx) = pending.lock().remove(&message.id) {
                                let _ = tx.send(Ok(message));
                            }
                        }
                        MessageType::StreamChunk | MessageType::StreamEnd => {
                            stream_manager.handle_message(&message);
                        }
                        _ => {}
                    },
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        RpcClientHandle { handle }
    }

    pub fn transport(&self) -> Arc<T> {
        self.transport.clone()
    }

    pub fn stream_manager(&self) -> Arc<StreamManager<C>> {
        self.stream_manager.clone()
    }

    pub async fn call<Req, Resp>(&self, method: &str, request: &Req) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        self.call_with_timeout(method, request, self.default_timeout)
            .await
    }

    pub async fn call_with_timeout<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
        timeout: Duration,
    ) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        let message: Message<C> = Message::call(method, request)?;
        let msg_id = message.id;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(msg_id, tx);

        if let Err(e) = self.transport.send(&message).await {
            self.pending.lock().remove(&msg_id);
            return Err(RpcError::Transport(e));
        }

        let response = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                self.pending.lock().remove(&msg_id);
                RpcError::Timeout(format!("Request {} timed out after {:?}", msg_id, timeout))
            })?
            .map_err(|_| RpcError::ConnectionClosed)??;

        match response.msg_type {
            MessageType::Reply => self.codec.decode(&response.payload),
            MessageType::Error => {
                let error_msg: String = self
                    .codec
                    .decode(&response.payload)
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(RpcError::ServerError(error_msg))
            }
            _ => Err(RpcError::InvalidMessage(format!(
                "Unexpected message type: {:?}",
                response.msg_type
            ))),
        }
    }

    pub async fn call_server_stream<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<StreamReceiver<Resp, C>>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        let stream_id = next_stream_id();
        let receiver = self.stream_manager.create_receiver::<Resp>(stream_id);

        let mut message: Message<C> = Message::call(method, request)?;
        message.metadata = message.metadata.with_stream(stream_id, 0);

        self.transport
            .send(&message)
            .await
            .map_err(RpcError::Transport)?;

        Ok(receiver)
    }

    pub async fn notify<Req: Serialize>(&self, method: &str, request: &Req) -> Result<()> {
        let message: Message<C> = Message::notification(method, request)?;
        self.transport
            .send(&message)
            .await
            .map_err(RpcError::Transport)
    }

    pub async fn call_raw(&self, method: &str, payload: Vec<u8>) -> Result<Vec<u8>> {
        self.call_raw_with_timeout(method, payload, self.default_timeout)
            .await
    }

    pub async fn call_raw_with_timeout(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        let message: Message<C> = Message::new(
            MessageId::new(),
            MessageType::Call,
            method,
            payload.into(),
            Default::default(),
        );
        let msg_id = message.id;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(msg_id, tx);

        if let Err(e) = self.transport.send(&message).await {
            self.pending.lock().remove(&msg_id);
            return Err(RpcError::Transport(e));
        }

        let response = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                self.pending.lock().remove(&msg_id);
                RpcError::Timeout(format!("Request {} timed out after {:?}", msg_id, timeout))
            })?
            .map_err(|_| RpcError::ConnectionClosed)??;

        match response.msg_type {
            MessageType::Reply => Ok(response.payload.to_vec()),
            MessageType::Error => {
                let error_msg: String = self
                    .codec
                    .decode(&response.payload)
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(RpcError::ServerError(error_msg))
            }
            _ => Err(RpcError::InvalidMessage(format!(
                "Unexpected message type: {:?}",
                response.msg_type
            ))),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.transport.is_connected()
    }

    pub fn active_streams(&self) -> usize {
        self.stream_manager.active_stream_count()
    }

    pub async fn close(&self) -> Result<()> {
        self.running.store(false, Ordering::Release);
        self.transport.close().await.map_err(RpcError::Transport)
    }
}

impl<T, C> Debug for RpcClient<T, C>
where
    T: MessageTransport<C>,
    C: Codec + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field("running", &self.running.load(Ordering::Relaxed))
            .field("pending_requests", &self.pending.lock().len())
            .field("active_streams", &self.stream_manager.active_stream_count())
            .finish()
    }
}

pub struct RpcClientHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl RpcClientHandle {
    pub async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::channel::{ChannelConfig, ChannelTransport};
    use crate::transport::message_transport::MessageTransportAdapter;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct AddRequest {
        a: i32,
        b: i32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct AddResponse {
        result: i32,
    }

    #[tokio::test]
    async fn test_client_call_reply() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let client_transport = MessageTransportAdapter::new(t1);
        let server_transport = MessageTransportAdapter::new(t2);

        let client = RpcClient::new(client_transport);
        let _handle = client.start();

        let server_handle = tokio::spawn(async move {
            let msg = server_transport.recv().await.unwrap();
            assert_eq!(msg.method, "add");

            let req: AddRequest = msg.deserialize_payload().unwrap();
            let resp = AddResponse {
                result: req.a + req.b,
            };

            let reply: Message = Message::reply(msg.id, resp).unwrap();
            server_transport.send(&reply).await.unwrap();
        });

        let response: AddResponse = client
            .call("add", &AddRequest { a: 10, b: 32 })
            .await
            .unwrap();
        assert_eq!(response.result, 42);

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_client_server_stream() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let client_transport = MessageTransportAdapter::new(t1);
        let server_transport = Arc::new(MessageTransportAdapter::new(t2));

        let client = RpcClient::new(client_transport);
        let _handle = client.start();

        let server_transport_clone = server_transport.clone();
        let server_handle = tokio::spawn(async move {
            let msg = server_transport_clone.recv().await.unwrap();
            let stream_id = msg.metadata.stream_id.unwrap();

            for i in 1..=3 {
                let chunk: Message = Message::stream_chunk(stream_id, i - 1, i as i32).unwrap();
                server_transport_clone.send(&chunk).await.unwrap();
            }

            let end: Message = Message::stream_end(stream_id);
            server_transport_clone.send(&end).await.unwrap();
        });

        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("get_numbers", &()).await.unwrap();

        let mut items = Vec::new();
        while let Some(result) = stream.recv().await {
            items.push(result.unwrap());
        }

        assert_eq!(items, vec![1, 2, 3]);
        server_handle.await.unwrap();
    }
}
