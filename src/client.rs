use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::error::{Result, RpcError};
use crate::message::Message;
use crate::message::types::{MessageId, MessageType};
use crate::transport::message_transport::MessageTransport;

type PendingRequest = oneshot::Sender<Result<Message>>;

pub struct RpcClient<T: MessageTransport> {
    transport: Arc<T>,
    pending: Arc<Mutex<HashMap<MessageId, PendingRequest>>>,
    running: Arc<AtomicBool>,
    default_timeout: Duration,
}

impl<T: MessageTransport + 'static> RpcClient<T> {
    pub fn new(transport: T) -> Self {
        Self::with_timeout(transport, Duration::from_secs(30))
    }

    pub fn with_timeout(transport: T, default_timeout: Duration) -> Self {
        let client = Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
            default_timeout,
        };
        client
    }

    pub fn start(&self) -> RpcClientHandle {
        self.running.store(true, Ordering::Release);

        let transport = self.transport.clone();
        let pending = self.pending.clone();
        let running = self.running.clone();

        let handle = tokio::spawn(async move {
            while running.load(Ordering::Acquire) {
                match transport.recv().await {
                    Ok(message) => {
                        let sender = pending.lock().remove(&message.id);
                        if let Some(tx) = sender {
                            let _ = tx.send(Ok(message));
                        }
                    }
                    Err(_e) => {
                        break;
                    }
                }
            }
        });

        RpcClientHandle { handle }
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
        let message = Message::call(method, request)?;
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
            MessageType::Reply => response.deserialize_payload(),
            MessageType::Error => {
                let error_msg: String = response
                    .deserialize_payload()
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(RpcError::ServerError(error_msg))
            }
            _ => Err(RpcError::InvalidMessage(format!(
                "Unexpected message type: {:?}",
                response.msg_type
            ))),
        }
    }

    pub async fn notify<Req: Serialize>(&self, method: &str, request: &Req) -> Result<()> {
        let message = Message::notification(method, request)?;
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
        let message = Message::new(
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
                let error_msg: String = response
                    .deserialize_payload()
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

    pub async fn close(&self) -> Result<()> {
        self.running.store(false, Ordering::Release);
        self.transport.close().await.map_err(RpcError::Transport)
    }
}

impl<T: MessageTransport> Debug for RpcClient<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field("running", &self.running.load(Ordering::Relaxed))
            .field("pending_requests", &self.pending.lock().len())
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
    async fn test_client_notify() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelTransport::create_pair("test", config).unwrap();

        let client_transport = MessageTransportAdapter::new(t1);
        let server_transport = MessageTransportAdapter::new(t2);

        let client = RpcClient::new(client_transport);

        client
            .notify("log", &"test message".to_string())
            .await
            .unwrap();

        let msg = server_transport.recv().await.unwrap();
        assert_eq!(msg.msg_type, MessageType::Notification);
        assert_eq!(msg.method, "log");
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

            let reply = Message::reply(msg.id, resp).unwrap();
            server_transport.send(&reply).await.unwrap();
        });

        let response: AddResponse = client
            .call("add", &AddRequest { a: 10, b: 32 })
            .await
            .unwrap();
        assert_eq!(response.result, 42);

        server_handle.await.unwrap();
    }
}
