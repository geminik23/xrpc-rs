use bytes::Bytes;
use futures::Stream;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use crate::channel::message::MessageChannel;
use crate::codec::{BincodeCodec, Codec};
use crate::error::{Result, RpcError};
use crate::message::Message;
use crate::message::types::{MessageId, MessageType};

pub type StreamId = u64;

type StreamSenders = Arc<Mutex<HashMap<StreamId, mpsc::UnboundedSender<Result<Bytes>>>>>;

struct StreamRegistration {
    stream_id: StreamId,
    streams: StreamSenders,
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        self.streams.lock().remove(&self.stream_id);
    }
}

static STREAM_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_stream_id() -> StreamId {
    STREAM_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct StreamSender<T: MessageChannel, C: Codec = BincodeCodec> {
    stream_id: StreamId,
    sequence: AtomicU64,
    transport: Arc<T>,
    codec: C,
    operation: AsyncMutex<()>,
    ended: std::sync::atomic::AtomicBool,
}

impl<T: MessageChannel> StreamSender<T, BincodeCodec> {
    pub fn new(stream_id: StreamId, transport: Arc<T>) -> Self {
        Self {
            stream_id,
            sequence: AtomicU64::new(0),
            transport,
            codec: BincodeCodec,
            operation: AsyncMutex::new(()),
            ended: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl<T: MessageChannel, C: Codec> StreamSender<T, C> {
    pub fn with_codec(stream_id: StreamId, transport: Arc<T>, codec: C) -> Self {
        Self {
            stream_id,
            sequence: AtomicU64::new(0),
            transport,
            codec,
            operation: AsyncMutex::new(()),
            ended: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub async fn send<D: Serialize>(&self, data: D) -> Result<()> {
        let _operation = self.operation.lock().await;
        if self.ended.load(Ordering::Acquire) {
            return Err(RpcError::StreamError("Stream already ended".to_string()));
        }

        let seq = self.sequence.load(Ordering::Relaxed);
        let payload = self.codec.encode(&data)?;
        let chunk = Message::new(
            MessageId::new(),
            MessageType::StreamChunk,
            "",
            Bytes::from(payload),
            crate::message::metadata::MessageMetadata::new().with_stream(self.stream_id, seq),
        );
        self.transport
            .send(&chunk)
            .await
            .map_err(RpcError::Transport)?;
        self.sequence.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn end(&self) -> Result<()> {
        let _operation = self.operation.lock().await;
        if self.ended.load(Ordering::Acquire) {
            return Ok(());
        }

        let end_msg: Message = Message::stream_end(self.stream_id);
        self.transport
            .send(&end_msg)
            .await
            .map_err(RpcError::Transport)?;
        self.ended.store(true, Ordering::Release);
        Ok(())
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn is_ended(&self) -> bool {
        self.ended.load(Ordering::Acquire)
    }
}

/// Receives stream chunks. Assumes transport delivers messages in order.
pub struct StreamReceiver<D, C: Codec = BincodeCodec> {
    stream_id: StreamId,
    rx: mpsc::UnboundedReceiver<Result<Bytes>>,
    ended: bool,
    codec: C,
    registration: Option<StreamRegistration>,
    on_terminate: Option<Box<dyn FnOnce(StreamId) + Send + Sync>>,
    _phantom: std::marker::PhantomData<D>,
}

impl<D, C: Codec> StreamReceiver<D, C> {
    fn finish(&mut self) {
        self.ended = true;
        self.registration.take();
        if let Some(on_terminate) = self.on_terminate.take() {
            on_terminate(self.stream_id);
        }
    }

    pub(crate) fn set_termination_callback(
        &mut self,
        callback: impl FnOnce(StreamId) + Send + Sync + 'static,
    ) {
        self.on_terminate = Some(Box::new(callback));
    }
}

impl<D, C> StreamReceiver<D, C>
where
    D: for<'de> Deserialize<'de>,
    C: Codec,
{
    fn new(
        stream_id: StreamId,
        rx: mpsc::UnboundedReceiver<Result<Bytes>>,
        codec: C,
        registration: StreamRegistration,
    ) -> Self {
        Self {
            stream_id,
            rx,
            ended: false,
            codec,
            registration: Some(registration),
            on_terminate: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn is_ended(&self) -> bool {
        self.ended
    }

    pub async fn recv(&mut self) -> Option<Result<D>> {
        if self.ended {
            return None;
        }

        match self.rx.recv().await {
            Some(Ok(data)) => Some(self.codec.decode(&data)),
            Some(Err(e)) => {
                self.finish();
                Some(Err(e))
            }
            None => {
                self.finish();
                None
            }
        }
    }

    pub async fn collect(mut self) -> Result<Vec<D>> {
        let mut items = Vec::new();
        while let Some(result) = self.recv().await {
            items.push(result?);
        }
        Ok(items)
    }

    pub fn cancel(&mut self) {
        self.finish();
        self.rx.close();
    }
}

impl<D, C> Stream for StreamReceiver<D, C>
where
    D: for<'de> Deserialize<'de> + Unpin,
    C: Codec + Unpin,
{
    type Item = Result<D>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }

        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                let result = self.codec.decode(&data);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => {
                self.finish();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<D, C: Codec> Drop for StreamReceiver<D, C> {
    fn drop(&mut self) {
        self.finish();
    }
}

pub struct StreamManager<C: Codec = BincodeCodec> {
    streams: StreamSenders,
    codec: C,
}

impl StreamManager<BincodeCodec> {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            codec: BincodeCodec,
        }
    }
}

impl<C: Codec> StreamManager<C> {
    pub fn with_codec(codec: C) -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            codec,
        }
    }

    pub fn create_receiver<D>(&self, stream_id: StreamId) -> StreamReceiver<D, C>
    where
        D: for<'de> Deserialize<'de>,
        C: Clone,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams.lock().insert(stream_id, tx);
        let registration = StreamRegistration {
            stream_id,
            streams: Arc::clone(&self.streams),
        };
        StreamReceiver::new(stream_id, rx, self.codec.clone(), registration)
    }

    pub fn handle_message(&self, message: &Message<C>) -> bool {
        let stream_id = match message.metadata.stream_id {
            Some(id) => id,
            None => return false,
        };

        let streams = self.streams.lock();
        let sender = match streams.get(&stream_id) {
            Some(tx) => tx,
            None => return false,
        };

        match message.msg_type {
            MessageType::StreamChunk => {
                let _ = sender.send(Ok(message.payload.clone()));
                true
            }
            MessageType::StreamEnd => {
                drop(streams);
                self.remove_stream(stream_id);
                true
            }
            _ => false,
        }
    }

    /// Forward an error to the stream receiver and close the stream.
    pub fn send_error(&self, stream_id: StreamId, error_msg: String) {
        let sender = self.streams.lock().remove(&stream_id);
        if let Some(sender) = sender {
            let _ = sender.send(Err(RpcError::ServerError(error_msg)));
        }
    }

    pub fn remove_stream(&self, stream_id: StreamId) {
        self.streams.lock().remove(&stream_id);
    }

    pub(crate) fn close_all(&self, error: RpcError) {
        let streams = {
            let mut streams = self.streams.lock();
            std::mem::take(&mut *streams)
        };
        for sender in streams.into_values() {
            let _ = sender.send(Err(error.clone()));
        }
    }

    pub fn active_stream_count(&self) -> usize {
        self.streams.lock().len()
    }
}

impl Default for StreamManager<BincodeCodec> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::message::MessageChannelAdapter;
    use crate::error::{TransportError, TransportResult};
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug, Default)]
    struct SerializedSendChannel {
        chunk_release: tokio::sync::Notify,
        chunk_started: tokio::sync::Notify,
        committed: Mutex<Vec<MessageType>>,
    }

    #[async_trait::async_trait]
    impl MessageChannel for SerializedSendChannel {
        async fn send(&self, message: &Message) -> TransportResult<()> {
            if message.msg_type == MessageType::StreamChunk {
                self.chunk_started.notify_one();
                self.chunk_release.notified().await;
            }
            self.committed.lock().push(message.msg_type);
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct RetryEndChannel {
        attempts: AtomicUsize,
        first_started: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl MessageChannel for RetryEndChannel {
        async fn send(&self, message: &Message) -> TransportResult<()> {
            assert_eq!(message.msg_type, MessageType::StreamEnd);
            match self.attempts.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    self.first_started.notify_one();
                    std::future::pending().await
                }
                1 => Err(TransportError::Timeout {
                    duration_ms: 1,
                    operation: "stream end".to_string(),
                }),
                _ => Ok(()),
            }
        }

        async fn recv(&self) -> TransportResult<Message> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_stream_send_and_end_commit_in_serial_order() {
        let transport = Arc::new(SerializedSendChannel::default());
        let sender = Arc::new(StreamSender::new(next_stream_id(), Arc::clone(&transport)));
        let chunk_sender = Arc::clone(&sender);
        let chunk = tokio::spawn(async move { chunk_sender.send(1).await });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            transport.chunk_started.notified(),
        )
        .await
        .unwrap();
        let end_sender = Arc::clone(&sender);
        let mut end = tokio::spawn(async move { end_sender.end().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut end)
                .await
                .is_err()
        );

        transport.chunk_release.notify_one();
        chunk.await.unwrap().unwrap();
        end.await.unwrap().unwrap();

        assert_eq!(
            *transport.committed.lock(),
            vec![MessageType::StreamChunk, MessageType::StreamEnd]
        );
        assert!(matches!(
            sender.send(2).await,
            Err(RpcError::StreamError(_))
        ));
    }

    #[tokio::test]
    async fn test_stream_end_can_retry_after_cancellation_and_failure() {
        let transport = Arc::new(RetryEndChannel::default());
        let sender = Arc::new(StreamSender::new(next_stream_id(), Arc::clone(&transport)));
        let first_sender = Arc::clone(&sender);
        let first = tokio::spawn(async move { first_sender.end().await });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            transport.first_started.notified(),
        )
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(!sender.is_ended());

        assert!(matches!(
            sender.end().await,
            Err(RpcError::Transport(TransportError::Timeout { .. }))
        ));
        assert!(!sender.is_ended());

        sender.end().await.unwrap();
        assert!(sender.is_ended());
        sender.end().await.unwrap();
        assert_eq!(transport.attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_stream_sender_receiver() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let sender_channel = Arc::new(MessageChannelAdapter::new(t1));
        let receiver_channel = MessageChannelAdapter::new(t2);

        let stream_id = next_stream_id();
        let sender = StreamSender::new(stream_id, sender_channel);

        let manager = StreamManager::new();
        let receiver: StreamReceiver<i32> = manager.create_receiver(stream_id);

        let recv_handle = tokio::spawn(async move {
            loop {
                let msg = receiver_channel.recv().await.unwrap();
                if !manager.handle_message(&msg) {
                    break;
                }
                if msg.msg_type == MessageType::StreamEnd {
                    break;
                }
            }
            receiver
        });

        sender.send(1i32).await.unwrap();
        sender.send(2i32).await.unwrap();
        sender.send(3i32).await.unwrap();
        sender.end().await.unwrap();

        let mut receiver = recv_handle.await.unwrap();

        let items: Vec<i32> = vec![
            receiver.recv().await.unwrap().unwrap(),
            receiver.recv().await.unwrap().unwrap(),
            receiver.recv().await.unwrap().unwrap(),
        ];
        assert_eq!(items, vec![1, 2, 3]);
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_stream_end_is_normal_completion() {
        let manager = StreamManager::new();
        let stream_id = next_stream_id();
        let mut receiver: StreamReceiver<i32> = manager.create_receiver(stream_id);
        let end: Message = Message::stream_end(stream_id);

        assert!(manager.handle_message(&end));
        assert!(receiver.recv().await.is_none());
        assert_eq!(manager.active_stream_count(), 0);
    }

    #[tokio::test]
    async fn test_close_all_reports_connection_closed() {
        let manager = StreamManager::new();
        let mut first: StreamReceiver<i32> = manager.create_receiver(next_stream_id());
        let mut second: StreamReceiver<i32> = manager.create_receiver(next_stream_id());

        manager.close_all(RpcError::ConnectionClosed);

        assert!(matches!(
            first.recv().await,
            Some(Err(RpcError::ConnectionClosed))
        ));
        assert!(matches!(
            second.recv().await,
            Some(Err(RpcError::ConnectionClosed))
        ));
        assert_eq!(manager.active_stream_count(), 0);
    }

    #[test]
    fn test_dropped_receiver_removes_stream_registration() {
        let manager = StreamManager::new();
        let receiver: StreamReceiver<i32> = manager.create_receiver(next_stream_id());

        assert_eq!(manager.active_stream_count(), 1);
        drop(receiver);
        assert_eq!(manager.active_stream_count(), 0);
    }

    #[tokio::test]
    async fn test_close_all_preserves_supplied_error() {
        let manager = StreamManager::new();
        let mut receiver: StreamReceiver<i32> = manager.create_receiver(next_stream_id());

        manager.close_all(RpcError::Transport(crate::error::TransportError::Protocol(
            "invalid frame".to_string(),
        )));

        assert!(matches!(
            receiver.recv().await,
            Some(Err(RpcError::Transport(crate::error::TransportError::Protocol(error))))
                if error == "invalid frame"
        ));
    }

    #[test]
    fn test_stream_id_generation() {
        let id1 = next_stream_id();
        let id2 = next_stream_id();
        assert_ne!(id1, id2);
    }
}
