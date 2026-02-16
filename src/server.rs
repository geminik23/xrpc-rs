use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::channel::message::MessageChannel;
use crate::codec::{BincodeCodec, Codec};
use crate::error::{Result, RpcError};
use crate::message::Message;
use crate::message::metadata::MessageMetadata;
use crate::message::types::{MessageId, MessageType};
use crate::streaming::{StreamId, next_stream_id};

#[async_trait]
pub trait Handler<C: Codec>: Send + Sync {
    async fn handle(&self, request: Message<C>, codec: &C) -> Result<Message<C>>;
    fn method_name(&self) -> &str;
}

#[async_trait]
pub trait StreamHandler<C: Codec>: Send + Sync {
    async fn handle(
        &self,
        request: Message<C>,
        sender: ServerStreamSender<C>,
        codec: &C,
    ) -> Result<()>;
    fn method_name(&self) -> &str;
}

pub struct FnHandler<F, C> {
    method: String,
    func: Arc<F>,
    _codec: std::marker::PhantomData<C>,
}

impl<F, Fut, C> FnHandler<F, C>
where
    F: Fn(Message<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Message<C>>> + Send + 'static,
    C: Codec,
{
    pub fn new(method: impl Into<String>, func: F) -> Self {
        Self {
            method: method.into(),
            func: Arc::new(func),
            _codec: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, C: Codec + Default> Handler<C> for FnHandler<F, C>
where
    F: Fn(Message<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Message<C>>> + Send + 'static,
{
    async fn handle(&self, request: Message<C>, _codec: &C) -> Result<Message<C>> {
        (self.func)(request).await
    }

    fn method_name(&self) -> &str {
        &self.method
    }
}

pub struct TypedHandler<Req, Resp, F, C> {
    method: String,
    func: Arc<F>,
    _phantom: std::marker::PhantomData<(Req, Resp, C)>,
}

impl<Req, Resp, F, Fut, C> TypedHandler<Req, Resp, F, C>
where
    Req: for<'de> Deserialize<'de> + Send + 'static,
    Resp: Serialize + Send + 'static,
    F: Fn(Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Resp>> + Send + 'static,
    C: Codec,
{
    pub fn new(method: impl Into<String>, func: F) -> Self {
        Self {
            method: method.into(),
            func: Arc::new(func),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<Req, Resp, F, Fut, C> Handler<C> for TypedHandler<Req, Resp, F, C>
where
    Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
    F: Fn(Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Resp>> + Send + 'static,
    C: Codec + Default,
{
    async fn handle(&self, request: Message<C>, codec: &C) -> Result<Message<C>> {
        let req: Req = codec.decode(&request.payload)?;
        let resp = (self.func)(req).await?;
        let payload = codec.encode(&resp)?;
        Ok(Message::new(
            request.id,
            MessageType::Reply,
            "",
            Bytes::from(payload),
            MessageMetadata::new(),
        ))
    }

    fn method_name(&self) -> &str {
        &self.method
    }
}

pub struct ServerStreamSender<C: Codec> {
    stream_id: StreamId,
    tx: mpsc::UnboundedSender<Bytes>,
    sequence: std::sync::atomic::AtomicU64,
    codec: C,
}

impl<C: Codec> ServerStreamSender<C> {
    fn new(stream_id: StreamId, tx: mpsc::UnboundedSender<Bytes>, codec: C) -> Self {
        Self {
            stream_id,
            tx,
            sequence: std::sync::atomic::AtomicU64::new(0),
            codec,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn send<T: Serialize>(&self, data: T) -> Result<()> {
        let seq = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let payload = self.codec.encode(&data)?;
        let chunk: Message = Message::new(
            MessageId::new(),
            MessageType::StreamChunk,
            "",
            Bytes::from(payload),
            MessageMetadata::new().with_stream(self.stream_id, seq),
        );
        let encoded = chunk.encode().map_err(RpcError::Transport)?;

        self.tx
            .send(encoded.freeze())
            .map_err(|_| RpcError::StreamError("Stream closed".to_string()))
    }

    pub fn end(&self) -> Result<()> {
        let end_msg: Message = Message::stream_end(self.stream_id);
        let encoded = end_msg.encode().map_err(RpcError::Transport)?;

        self.tx
            .send(encoded.freeze())
            .map_err(|_| RpcError::StreamError("Stream closed".to_string()))
    }
}

pub struct TypedStreamHandler<Req, Item, F, C> {
    method: String,
    func: Arc<F>,
    _phantom: std::marker::PhantomData<(Req, Item, C)>,
}

impl<Req, Item, F, S, C> TypedStreamHandler<Req, Item, F, C>
where
    Req: for<'de> Deserialize<'de> + Send + 'static,
    Item: Serialize + Send + 'static,
    S: Stream<Item = Result<Item>> + Send + 'static,
    F: Fn(Req) -> S + Send + Sync + 'static,
    C: Codec,
{
    pub fn new(method: impl Into<String>, func: F) -> Self {
        Self {
            method: method.into(),
            func: Arc::new(func),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<Req, Item, F, S, C> StreamHandler<C> for TypedStreamHandler<Req, Item, F, C>
where
    Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
    Item: Serialize + Send + Sync + 'static,
    S: Stream<Item = Result<Item>> + Send + 'static,
    F: Fn(Req) -> S + Send + Sync + 'static,
    C: Codec + Default,
{
    async fn handle(
        &self,
        request: Message<C>,
        sender: ServerStreamSender<C>,
        codec: &C,
    ) -> Result<()> {
        use futures::StreamExt;

        let req: Req = codec.decode(&request.payload)?;
        let mut stream = Box::pin((self.func)(req));

        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => sender.send(item)?,
                Err(e) => return Err(e),
            }
        }

        sender.end()?;
        Ok(())
    }

    fn method_name(&self) -> &str {
        &self.method
    }
}

pub struct FnStreamHandler<F, C> {
    method: String,
    func: Arc<F>,
    _codec: std::marker::PhantomData<C>,
}

impl<F, Fut, C> FnStreamHandler<F, C>
where
    F: Fn(Message<C>, ServerStreamSender<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
    C: Codec,
{
    pub fn new(method: impl Into<String>, func: F) -> Self {
        Self {
            method: method.into(),
            func: Arc::new(func),
            _codec: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, C> StreamHandler<C> for FnStreamHandler<F, C>
where
    F: Fn(Message<C>, ServerStreamSender<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
    C: Codec + Default,
{
    async fn handle(
        &self,
        request: Message<C>,
        sender: ServerStreamSender<C>,
        _codec: &C,
    ) -> Result<()> {
        (self.func)(request, sender).await
    }

    fn method_name(&self) -> &str {
        &self.method
    }
}

pub struct RpcServer<C: Codec = BincodeCodec> {
    handlers: Arc<RwLock<HashMap<String, Arc<dyn Handler<C>>>>>,
    stream_handlers: Arc<RwLock<HashMap<String, Arc<dyn StreamHandler<C>>>>>,
    codec: C,
}

impl RpcServer<BincodeCodec> {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            stream_handlers: Arc::new(RwLock::new(HashMap::new())),
            codec: BincodeCodec,
        }
    }
}

impl<C: Codec + Clone + Default + 'static> RpcServer<C> {
    pub fn with_codec(codec: C) -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            stream_handlers: Arc::new(RwLock::new(HashMap::new())),
            codec,
        }
    }

    pub fn register(&self, handler: Arc<dyn Handler<C>>) {
        let method = handler.method_name().to_string();
        self.handlers.write().insert(method, handler);
    }

    pub fn register_fn<F, Fut>(&self, method: impl Into<String>, func: F)
    where
        F: Fn(Message<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Message<C>>> + Send + 'static,
    {
        let handler: Arc<FnHandler<F, C>> = Arc::new(FnHandler::new(method, func));
        self.register(handler);
    }

    pub fn register_typed<Req, Resp, F, Fut>(&self, method: impl Into<String>, func: F)
    where
        Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
        Resp: Serialize + Send + Sync + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp>> + Send + 'static,
    {
        let handler: Arc<TypedHandler<Req, Resp, F, C>> = Arc::new(TypedHandler::new(method, func));
        self.register(handler);
    }

    pub fn register_stream<Req, Item, F, S>(&self, method: impl Into<String>, func: F)
    where
        Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
        Item: Serialize + Send + Sync + 'static,
        S: Stream<Item = Result<Item>> + Send + 'static,
        F: Fn(Req) -> S + Send + Sync + 'static,
    {
        let method = method.into();
        let handler: Arc<TypedStreamHandler<Req, Item, F, C>> =
            Arc::new(TypedStreamHandler::new(method.clone(), func));
        self.stream_handlers.write().insert(method, handler);
    }

    pub fn register_stream_fn<F, Fut>(&self, method: impl Into<String>, func: F)
    where
        F: Fn(Message<C>, ServerStreamSender<C>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let method = method.into();
        let handler: Arc<FnStreamHandler<F, C>> =
            Arc::new(FnStreamHandler::new(method.clone(), func));
        self.stream_handlers.write().insert(method, handler);
    }

    /// Dispatch a single message, sending responses via `send_tx`.
    async fn dispatch_message(
        handlers: &Arc<RwLock<HashMap<String, Arc<dyn Handler<C>>>>>,
        stream_handlers: &Arc<RwLock<HashMap<String, Arc<dyn StreamHandler<C>>>>>,
        codec: &C,
        message: Message<C>,
        send_tx: &mpsc::UnboundedSender<Message<C>>,
    ) {
        match message.msg_type {
            MessageType::Call => {
                if message.metadata.stream_id.is_some() {
                    Self::dispatch_stream_call(stream_handlers, codec, message, send_tx).await;
                    return;
                }

                let handler = handlers.read().get(&message.method).cloned();
                let response = match handler {
                    Some(h) => match h.handle(message.clone(), codec).await {
                        Ok(response) => response,
                        Err(e) => Message::error(message.id, e.to_string()),
                    },
                    None => {
                        Message::error(message.id, format!("Method not found: {}", message.method))
                    }
                };
                let _ = send_tx.send(response);
            }
            MessageType::Notification => {
                let handler = handlers.read().get(&message.method).cloned();
                if let Some(h) = handler {
                    let _ = h.handle(message, codec).await;
                }
            }
            _ => {}
        }
    }

    /// Dispatch a streaming call, forwarding all chunks via `send_tx`.
    async fn dispatch_stream_call(
        stream_handlers: &Arc<RwLock<HashMap<String, Arc<dyn StreamHandler<C>>>>>,
        codec: &C,
        message: Message<C>,
        send_tx: &mpsc::UnboundedSender<Message<C>>,
    ) {
        let stream_id = message.metadata.stream_id.unwrap_or_else(next_stream_id);
        let handler = stream_handlers.read().get(&message.method).cloned();

        let Some(h) = handler else {
            let error = Message::stream_error(
                message.id,
                stream_id,
                format!("Stream method not found: {}", message.method),
            );
            let _ = send_tx.send(error);
            return;
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        let sender = ServerStreamSender::new(stream_id, tx, codec.clone());

        let send_tx_clone = send_tx.clone();
        let forward_task = async move {
            while let Some(data) = rx.recv().await {
                if let Ok(msg) = Message::<C>::decode(&data[..]) {
                    let _ = send_tx_clone.send(msg);
                }
            }
        };

        let codec = codec.clone();
        let handler_task = async {
            if let Err(e) = h.handle(message.clone(), sender, &codec).await {
                let error = Message::stream_error(message.id, stream_id, e.to_string());
                let _ = send_tx.send(error);
            }
        };

        tokio::join!(handler_task, forward_task);
    }

    /// Serve requests concurrently, dispatching each message in its own tokio task.
    pub async fn serve<T: MessageChannel<C> + 'static>(&self, transport: Arc<T>) -> Result<()> {
        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message<C>>();

        let transport_writer = transport.clone();
        let writer_handle = tokio::spawn(async move {
            while let Some(msg) = send_rx.recv().await {
                if transport_writer.send(&msg).await.is_err() {
                    break;
                }
            }
        });

        let result = loop {
            let message = match transport.recv().await {
                Ok(msg) => msg,
                Err(e) => break Err(RpcError::Transport(e)),
            };

            let handlers = self.handlers.clone();
            let stream_handlers = self.stream_handlers.clone();
            let codec = self.codec.clone();
            let send_tx = send_tx.clone();

            tokio::spawn(async move {
                Self::dispatch_message(&handlers, &stream_handlers, &codec, message, &send_tx)
                    .await;
            });
        };

        drop(send_tx);
        let _ = writer_handle.await;

        result
    }

    /// Serve requests sequentially, processing one message at a time.
    ///
    /// This preserves strict one-at-a-time message processing for cases where ordering guarantees or single-threaded handler execution is required.
    pub async fn serve_sequential<T: MessageChannel<C> + 'static>(
        &self,
        transport: Arc<T>,
    ) -> Result<()> {
        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message<C>>();

        let transport_writer = transport.clone();
        let writer_handle = tokio::spawn(async move {
            while let Some(msg) = send_rx.recv().await {
                if transport_writer.send(&msg).await.is_err() {
                    break;
                }
            }
        });

        let result = loop {
            let message = match transport.recv().await {
                Ok(msg) => msg,
                Err(e) => break Err(RpcError::Transport(e)),
            };

            Self::dispatch_message(
                &self.handlers,
                &self.stream_handlers,
                &self.codec,
                message,
                &send_tx,
            )
            .await;
        };

        drop(send_tx);
        let _ = writer_handle.await;

        result
    }

    pub fn spawn_handler<T: MessageChannel<C> + 'static>(&self, transport: T) -> ServerHandle {
        let handlers = self.handlers.clone();
        let stream_handlers = self.stream_handlers.clone();
        let codec = self.codec.clone();
        let transport = Arc::new(transport);

        let handle = tokio::spawn(async move {
            let server = RpcServer {
                handlers,
                stream_handlers,
                codec,
            };
            let _ = server.serve(transport).await;
        });

        ServerHandle { handle }
    }

    pub fn handler_count(&self) -> usize {
        self.handlers.read().len() + self.stream_handlers.read().len()
    }
}

impl Default for RpcServer<BincodeCodec> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ServerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    pub async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::message::MessageChannelAdapter;
    use crate::streaming::StreamReceiver;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};

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
    async fn test_server_typed_handler() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = MessageChannelAdapter::new(t2);

        let server = RpcServer::new();
        server.register_typed("add", |req: AddRequest| async move {
            Ok(AddResponse {
                result: req.a + req.b,
            })
        });

        let _handle = server.spawn_handler(server_channel);

        let request: Message = Message::call("add", AddRequest { a: 10, b: 32 }).unwrap();
        client_channel.send(&request).await.unwrap();

        let response = client_channel.recv().await.unwrap();
        assert_eq!(response.msg_type, MessageType::Reply);

        let resp: AddResponse = response.deserialize_payload().unwrap();
        assert_eq!(resp.result, 42);
    }

    #[tokio::test]
    async fn test_server_stream_handler() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client_channel = Arc::new(MessageChannelAdapter::new(t1));
        let server_channel = MessageChannelAdapter::new(t2);

        let server = RpcServer::new();
        server.register_stream("range", |count: i32| {
            futures::stream::iter((1..=count).map(|i| Ok(i)))
        });

        let _handle = server.spawn_handler(server_channel);

        let stream_id = next_stream_id();
        let mut request: Message = Message::call("range", 5i32).unwrap();
        request.metadata = request.metadata.with_stream(stream_id, 0);

        let manager = crate::streaming::StreamManager::new();
        let mut receiver: StreamReceiver<i32> = manager.create_receiver(stream_id);

        client_channel.send(&request).await.unwrap();

        let client_channel_clone = client_channel.clone();
        let recv_task = tokio::spawn(async move {
            loop {
                match client_channel_clone.recv().await {
                    Ok(msg) => {
                        if msg.msg_type == MessageType::StreamEnd {
                            manager.handle_message(&msg);
                            break;
                        }
                        manager.handle_message(&msg);
                    }
                    Err(_) => break,
                }
            }
        });

        let mut items = Vec::new();
        while let Some(result) = receiver.recv().await {
            items.push(result.unwrap());
        }

        recv_task.await.unwrap();
        assert_eq!(items, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_server_notification() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = MessageChannelAdapter::new(t2);

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        let server = RpcServer::new();
        server.register_fn("log", move |_msg: Message| {
            let called = called_clone.clone();
            async move {
                called.store(true, Ordering::Release);
                Ok(Message::reply(MessageId::new(), ())?)
            }
        });

        let _handle = server.spawn_handler(server_channel);

        let notification: Message = Message::notification("log", "test").unwrap();
        client_channel.send(&notification).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(called.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_unary_during_stream() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();
        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = MessageChannelAdapter::new(t2);

        let server = RpcServer::new();
        server.register_stream_fn(
            "slow_stream",
            |_msg: Message, sender: ServerStreamSender<BincodeCodec>| async move {
                for i in 0..5 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    sender.send(i as i32)?;
                }
                sender.end()?;
                Ok(())
            },
        );
        server.register_typed("add", |req: AddRequest| async move {
            Ok(AddResponse {
                result: req.a + req.b,
            })
        });

        let _handle = server.spawn_handler(server_channel);

        let client = crate::client::RpcClient::new(client_channel);
        let _client_handle = client.start();

        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("slow_stream", &()).await.unwrap();

        let response: AddResponse = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.call("add", &AddRequest { a: 10, b: 32 }),
        )
        .await
        .expect("Unary call should not be blocked by active stream")
        .unwrap();
        assert_eq!(response.result, 42);

        let mut items = Vec::new();
        while let Some(result) = stream.recv().await {
            items.push(result.unwrap());
        }
        assert_eq!(items, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_multiple_concurrent_streams() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();
        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = MessageChannelAdapter::new(t2);

        let server = RpcServer::new();
        server.register_stream_fn(
            "countdown",
            |msg: Message, sender: ServerStreamSender<BincodeCodec>| async move {
                let count: i32 = msg.deserialize_payload().unwrap_or(3);
                for i in (0..count).rev() {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    sender.send(i)?;
                }
                sender.end()?;
                Ok(())
            },
        );

        let _handle = server.spawn_handler(server_channel);

        let client = crate::client::RpcClient::new(client_channel);
        let _client_handle = client.start();

        let mut stream_a: StreamReceiver<i32> =
            client.call_server_stream("countdown", &3i32).await.unwrap();
        let mut stream_b: StreamReceiver<i32> =
            client.call_server_stream("countdown", &4i32).await.unwrap();

        let (items_a, items_b) = tokio::join!(
            async {
                let mut items = Vec::new();
                while let Some(result) = stream_a.recv().await {
                    items.push(result.unwrap());
                }
                items
            },
            async {
                let mut items = Vec::new();
                while let Some(result) = stream_b.recv().await {
                    items.push(result.unwrap());
                }
                items
            }
        );

        assert_eq!(items_a, vec![2, 1, 0]);
        assert_eq!(items_b, vec![3, 2, 1, 0]);
    }

    #[tokio::test]
    async fn test_concurrent_unary_calls() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();
        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = MessageChannelAdapter::new(t2);

        let server = RpcServer::new();
        server.register_typed("slow_add", |req: AddRequest| async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(AddResponse {
                result: req.a + req.b,
            })
        });

        let _handle = server.spawn_handler(server_channel);

        let client = crate::client::RpcClient::new(client_channel);
        let _client_handle = client.start();

        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let client = &client;
                async move {
                    let resp: AddResponse = client
                        .call("slow_add", &AddRequest { a: i, b: i * 10 })
                        .await
                        .unwrap();
                    resp.result
                }
            })
            .collect();

        let results = futures::future::join_all(handles).await;
        let elapsed = start.elapsed();

        assert_eq!(results, vec![0, 11, 22, 33, 44]);
        assert!(
            elapsed < std::time::Duration::from_millis(200),
            "Concurrent calls took {:?}, expected < 200ms",
            elapsed
        );
    }
}
