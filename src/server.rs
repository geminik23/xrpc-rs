use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use crate::error::{Result, RpcError};
use crate::message::Message;
use crate::message::types::MessageType;
use crate::transport::message_transport::MessageTransport;

#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, request: Message) -> Result<Message>;
    fn method_name(&self) -> &str;
}

pub struct FnHandler<F> {
    method: String,
    func: Arc<F>,
}

impl<F, Fut> FnHandler<F>
where
    F: Fn(Message) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Message>> + Send + 'static,
{
    pub fn new(method: impl Into<String>, func: F) -> Self {
        Self {
            method: method.into(),
            func: Arc::new(func),
        }
    }
}

#[async_trait]
impl<F, Fut> Handler for FnHandler<F>
where
    F: Fn(Message) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Message>> + Send + 'static,
{
    async fn handle(&self, request: Message) -> Result<Message> {
        (self.func)(request).await
    }

    fn method_name(&self) -> &str {
        &self.method
    }
}

pub struct TypedHandler<Req, Resp, F> {
    method: String,
    func: Arc<F>,
    _phantom: std::marker::PhantomData<(Req, Resp)>,
}

impl<Req, Resp, F, Fut> TypedHandler<Req, Resp, F>
where
    Req: for<'de> Deserialize<'de> + Send + 'static,
    Resp: Serialize + Send + 'static,
    F: Fn(Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Resp>> + Send + 'static,
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
impl<Req, Resp, F, Fut> Handler for TypedHandler<Req, Resp, F>
where
    Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
    F: Fn(Req) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Resp>> + Send + 'static,
{
    async fn handle(&self, request: Message) -> Result<Message> {
        let req: Req = request.deserialize_payload()?;
        let resp = (self.func)(req).await?;
        Message::reply(request.id, resp)
    }

    fn method_name(&self) -> &str {
        &self.method
    }
}

pub struct RpcServer {
    handlers: Arc<RwLock<HashMap<String, Arc<dyn Handler>>>>,
}

impl RpcServer {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&self, handler: Arc<dyn Handler>) {
        let method = handler.method_name().to_string();
        self.handlers.write().insert(method, handler);
    }

    pub fn register_fn<F, Fut>(&self, method: impl Into<String>, func: F)
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Message>> + Send + 'static,
    {
        let handler = Arc::new(FnHandler::new(method, func));
        self.register(handler);
    }

    pub fn register_typed<Req, Resp, F, Fut>(&self, method: impl Into<String>, func: F)
    where
        Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
        Resp: Serialize + Send + Sync + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp>> + Send + 'static,
    {
        let handler = Arc::new(TypedHandler::new(method, func));
        self.register(handler);
    }

    pub async fn handle_message(&self, message: Message) -> Option<Message> {
        match message.msg_type {
            MessageType::Call => {
                let handler = self.handlers.read().get(&message.method).cloned();

                match handler {
                    Some(h) => match h.handle(message.clone()).await {
                        Ok(response) => Some(response),
                        Err(e) => Some(Message::error(message.id, e.to_string())),
                    },
                    None => Some(Message::error(
                        message.id,
                        format!("Method not found: {}", message.method),
                    )),
                }
            }
            MessageType::Notification => {
                let handler = self.handlers.read().get(&message.method).cloned();
                if let Some(h) = handler {
                    let _ = h.handle(message).await;
                }
                None
            }
            _ => None,
        }
    }

    pub async fn serve<T: MessageTransport>(&self, transport: Arc<T>) -> Result<()> {
        loop {
            let message = transport.recv().await.map_err(RpcError::Transport)?;

            if let Some(response) = self.handle_message(message).await {
                transport
                    .send(&response)
                    .await
                    .map_err(RpcError::Transport)?;
            }
        }
    }

    pub fn spawn_handler<T: MessageTransport + 'static>(&self, transport: T) -> ServerHandle {
        let handlers = self.handlers.clone();
        let transport = Arc::new(transport);

        let handle = tokio::spawn(async move {
            let server = RpcServer { handlers };
            let _ = server.serve(transport).await;
        });

        ServerHandle { handle }
    }

    pub fn handler_count(&self) -> usize {
        self.handlers.read().len()
    }
}

impl Default for RpcServer {
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
    use crate::message::types::MessageId;
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
    async fn test_server_typed_handler() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let client_transport = MessageTransportAdapter::new(t1);
        let server_transport = MessageTransportAdapter::new(t2);

        let server = RpcServer::new();
        server.register_typed("add", |req: AddRequest| async move {
            Ok(AddResponse {
                result: req.a + req.b,
            })
        });

        assert_eq!(server.handler_count(), 1);

        let _handle = server.spawn_handler(server_transport);

        let request = Message::call("add", AddRequest { a: 10, b: 32 }).unwrap();
        client_transport.send(&request).await.unwrap();

        let response = client_transport.recv().await.unwrap();
        assert_eq!(response.msg_type, MessageType::Reply);

        let resp: AddResponse = response.deserialize_payload().unwrap();
        assert_eq!(resp.result, 42);
    }

    #[tokio::test]
    async fn test_server_method_not_found() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let client_transport = MessageTransportAdapter::new(t1);
        let server_transport = MessageTransportAdapter::new(t2);

        let server = RpcServer::new();
        let _handle = server.spawn_handler(server_transport);

        let request = Message::call("unknown", "test").unwrap();
        client_transport.send(&request).await.unwrap();

        let response = client_transport.recv().await.unwrap();
        assert_eq!(response.msg_type, MessageType::Error);
    }

    #[tokio::test]
    async fn test_server_notification() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let client_transport = MessageTransportAdapter::new(t1);
        let server_transport = MessageTransportAdapter::new(t2);

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

        let _handle = server.spawn_handler(server_transport);

        let notification = Message::notification("log", "test").unwrap();
        client_transport.send(&notification).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(called.load(Ordering::Acquire));
    }
}
