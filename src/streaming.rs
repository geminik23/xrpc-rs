use bytes::Bytes;
use futures::Stream;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::sync::mpsc;

use crate::channel::message::MessageChannel;
use crate::codec::{BincodeCodec, Codec};
use crate::error::{Result, RpcError};
use crate::message::Message;
use crate::message::types::{MessageId, MessageType};

pub type StreamId = u64;

static STREAM_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_stream_id() -> StreamId {
    STREAM_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct StreamSender<T: MessageChannel, C: Codec = BincodeCodec> {
    stream_id: StreamId,
    sequence: AtomicU64,
    transport: Arc<T>,
    codec: C,
    ended: std::sync::atomic::AtomicBool,
}

impl<T: MessageChannel> StreamSender<T, BincodeCodec> {
    pub fn new(stream_id: StreamId, transport: Arc<T>) -> Self {
        Self {
            stream_id,
            sequence: AtomicU64::new(0),
            transport,
            codec: BincodeCodec,
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
            ended: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub async fn send<D: Serialize>(&self, data: D) -> Result<()> {
        if self.ended.load(Ordering::Acquire) {
            return Err(RpcError::StreamError("Stream already ended".to_string()));
        }

        let seq = self.sequence.fetch_add(1, Ordering::Relaxed);
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
            .map_err(RpcError::Transport)
    }

    pub async fn end(&self) -> Result<()> {
        if self
            .ended
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let end_msg: Message = Message::stream_end(self.stream_id);
        self.transport
            .send(&end_msg)
            .await
            .map_err(RpcError::Transport)
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
    _phantom: std::marker::PhantomData<D>,
}

impl<D, C> StreamReceiver<D, C>
where
    D: for<'de> Deserialize<'de>,
    C: Codec,
{
    pub(crate) fn new(
        stream_id: StreamId,
        rx: mpsc::UnboundedReceiver<Result<Bytes>>,
        codec: C,
    ) -> Self {
        Self {
            stream_id,
            rx,
            ended: false,
            codec,
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
                self.ended = true;
                Some(Err(e))
            }
            None => {
                self.ended = true;
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
        self.ended = true;
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
                self.ended = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct StreamManager<C: Codec = BincodeCodec> {
    streams: Arc<Mutex<HashMap<StreamId, mpsc::UnboundedSender<Result<Bytes>>>>>,
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

impl<C: Codec + Clone> StreamManager<C> {
    pub fn with_codec(codec: C) -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            codec,
        }
    }

    pub fn create_receiver<D>(&self, stream_id: StreamId) -> StreamReceiver<D, C>
    where
        D: for<'de> Deserialize<'de>,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams.lock().insert(stream_id, tx);
        StreamReceiver::new(stream_id, rx, self.codec.clone())
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

    /// Forward error to stream receiver and close the stream
    pub fn send_error(&self, stream_id: StreamId, error_msg: String) {
        let streams = self.streams.lock();
        if let Some(sender) = streams.get(&stream_id) {
            let _ = sender.send(Err(RpcError::ServerError(error_msg)));
        }
        drop(streams);
        self.remove_stream(stream_id);
    }

    pub fn remove_stream(&self, stream_id: StreamId) {
        self.streams.lock().remove(&stream_id);
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
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};

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
    }

    #[test]
    fn test_stream_id_generation() {
        let id1 = next_stream_id();
        let id2 = next_stream_id();
        assert_ne!(id1, id2);
    }
}
