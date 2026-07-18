use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

use crate::error::{TransportError, TransportResult};
use crate::transport::{FrameTransport, TransportStats};

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// Configuration for in-process channel frame transport.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    pub buffer_size: usize,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            read_timeout: Some(Duration::from_secs(5)),
            write_timeout: Some(Duration::from_secs(5)),
        }
    }
}

impl ChannelConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }
}

#[derive(Debug, Clone, Default)]
struct ChannelStats {
    messages_sent: u64,
    messages_received: u64,
    bytes_sent: u64,
    bytes_received: u64,
    send_errors: u64,
    recv_errors: u64,
}

#[derive(Clone, Copy)]
enum ChannelOperation {
    Send,
    Receive,
}

struct PendingOperation {
    stats: Arc<Mutex<ChannelStats>>,
    operation: ChannelOperation,
    completed: bool,
}

impl PendingOperation {
    fn new(stats: Arc<Mutex<ChannelStats>>, operation: ChannelOperation) -> Self {
        Self {
            stats,
            operation,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PendingOperation {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let mut stats = self.stats.lock();
        match self.operation {
            ChannelOperation::Send => stats.send_errors += 1,
            ChannelOperation::Receive => stats.recv_errors += 1,
        }
    }
}

struct ChannelQueueState {
    frames: VecDeque<Bytes>,
    sender_closed: bool,
    receiver_closed: bool,
}

struct ChannelQueue {
    state: Mutex<ChannelQueueState>,
    not_empty: Notify,
    not_full: Notify,
    capacity: usize,
}

impl ChannelQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(ChannelQueueState {
                frames: VecDeque::with_capacity(capacity),
                sender_closed: false,
                receiver_closed: false,
            }),
            not_empty: Notify::new(),
            not_full: Notify::new(),
            capacity,
        }
    }

    fn close_sender(&self) {
        self.state.lock().sender_closed = true;
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    fn close_receiver(&self) {
        self.state.lock().receiver_closed = true;
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    async fn send(&self, frame: Bytes, timeout: Option<Duration>) -> TransportResult<()> {
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);

        loop {
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut state = self.state.lock();
                if state.sender_closed || state.receiver_closed {
                    return Err(TransportError::ConnectionClosed);
                }

                if state.frames.len() < self.capacity {
                    state.frames.push_back(frame);
                    drop(state);
                    self.not_empty.notify_one();
                    return Ok(());
                }
            }

            match deadline {
                Some(deadline) => {
                    if tokio::time::Instant::now() >= deadline
                        || tokio::time::timeout_at(deadline, notified.as_mut())
                            .await
                            .is_err()
                    {
                        return Err(TransportError::Timeout {
                            duration_ms: timeout.unwrap().as_millis() as u64,
                            operation: "send".into(),
                        });
                    }
                }
                None => notified.as_mut().await,
            }
        }
    }

    async fn recv(&self, timeout: Option<Duration>) -> TransportResult<Bytes> {
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);

        loop {
            let notified = self.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut state = self.state.lock();
                if state.receiver_closed {
                    return Err(TransportError::ConnectionClosed);
                }

                if let Some(frame) = state.frames.pop_front() {
                    drop(state);
                    self.not_full.notify_one();
                    return Ok(frame);
                }

                if state.sender_closed {
                    return Err(TransportError::ConnectionClosed);
                }
            }

            match deadline {
                Some(deadline) => {
                    if tokio::time::Instant::now() >= deadline
                        || tokio::time::timeout_at(deadline, notified.as_mut())
                            .await
                            .is_err()
                    {
                        return Err(TransportError::Timeout {
                            duration_ms: timeout.unwrap().as_millis() as u64,
                            operation: "receive".into(),
                        });
                    }
                }
                None => notified.as_mut().await,
            }
        }
    }
}

/// In-process channel frame transport (Layer 1).
pub struct ChannelFrameTransport {
    sender: Arc<ChannelQueue>,
    receiver: Arc<ChannelQueue>,
    config: ChannelConfig,
    stats: Arc<Mutex<ChannelStats>>,
    local_closed: Arc<AtomicBool>,
    peer_closed: Arc<AtomicBool>,
    name: String,
}

impl ChannelFrameTransport {
    /// Create a pair of connected transports.
    pub fn create_pair(
        name: impl Into<String>,
        config: ChannelConfig,
    ) -> TransportResult<(Self, Self)> {
        let name = name.into();
        let capacity = (config.buffer_size / 1024).max(16);
        let queue1 = Arc::new(ChannelQueue::new(capacity));
        let queue2 = Arc::new(ChannelQueue::new(capacity));
        let endpoint1_closed = Arc::new(AtomicBool::new(false));
        let endpoint2_closed = Arc::new(AtomicBool::new(false));

        let transport1 = Self {
            sender: Arc::clone(&queue1),
            receiver: Arc::clone(&queue2),
            config: config.clone(),
            stats: Arc::new(Mutex::new(ChannelStats::default())),
            local_closed: Arc::clone(&endpoint1_closed),
            peer_closed: Arc::clone(&endpoint2_closed),
            name: format!("{}-client", name),
        };

        let transport2 = Self {
            sender: queue2,
            receiver: queue1,
            config,
            stats: Arc::new(Mutex::new(ChannelStats::default())),
            local_closed: endpoint2_closed,
            peer_closed: endpoint1_closed,
            name: format!("{}-server", name),
        };

        Ok((transport1, transport2))
    }

    /// Non-blocking receive (used by SharedMemoryFrameTransport).
    pub fn try_recv(&self) -> TransportResult<Option<Bytes>> {
        let result = {
            let mut state = self.receiver.state.lock();
            if state.receiver_closed {
                Err(TransportError::ConnectionClosed)
            } else if let Some(frame) = state.frames.pop_front() {
                Ok(Some(frame))
            } else if state.sender_closed {
                Err(TransportError::ConnectionClosed)
            } else {
                Ok(None)
            }
        };

        let mut stats = self.stats.lock();
        match &result {
            Ok(Some(frame)) => {
                stats.messages_received += 1;
                stats.bytes_received += frame.len() as u64;
                self.receiver.not_full.notify_one();
            }
            Err(_) => stats.recv_errors += 1,
            Ok(None) => {}
        }
        result
    }

    fn close_endpoint(&self) {
        self.local_closed.store(true, Ordering::Release);
        self.sender.close_sender();
        self.receiver.close_receiver();
    }
}

#[async_trait]
impl FrameTransport for ChannelFrameTransport {
    async fn send_frame(&self, data: &[u8]) -> TransportResult<()> {
        let mut pending = PendingOperation::new(Arc::clone(&self.stats), ChannelOperation::Send);
        let result = self
            .sender
            .send(Bytes::copy_from_slice(data), self.config.write_timeout)
            .await;

        {
            let mut stats = self.stats.lock();
            if result.is_ok() {
                stats.messages_sent += 1;
                stats.bytes_sent += data.len() as u64;
            } else {
                stats.send_errors += 1;
            }
        }
        pending.complete();
        result
    }

    async fn recv_frame(&self) -> TransportResult<Bytes> {
        let mut pending = PendingOperation::new(Arc::clone(&self.stats), ChannelOperation::Receive);
        let result = self.receiver.recv(self.config.read_timeout).await;

        {
            let mut stats = self.stats.lock();
            match &result {
                Ok(bytes) => {
                    stats.messages_received += 1;
                    stats.bytes_received += bytes.len() as u64;
                }
                Err(_) => stats.recv_errors += 1,
            }
        }
        pending.complete();
        result
    }

    fn is_connected(&self) -> bool {
        !self.local_closed.load(Ordering::Acquire) && !self.peer_closed.load(Ordering::Acquire)
    }

    async fn close(&self) -> TransportResult<()> {
        self.close_endpoint();
        Ok(())
    }

    fn stats(&self) -> Option<TransportStats> {
        let stats = self.stats.lock();
        Some(TransportStats {
            messages_sent: stats.messages_sent,
            messages_received: stats.messages_received,
            bytes_sent: stats.bytes_sent,
            bytes_received: stats.bytes_received,
            send_errors: stats.send_errors,
            recv_errors: stats.recv_errors,
            avg_latency_us: None,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for ChannelFrameTransport {
    fn drop(&mut self) {
        self.close_endpoint();
    }
}

impl std::fmt::Debug for ChannelFrameTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelFrameTransport")
            .field("name", &self.name)
            .field("config", &self.config)
            .finish()
    }
}

// Deprecated alias for backward compatibility
#[deprecated(since = "0.2.0", note = "Use ChannelFrameTransport instead")]
pub type ChannelTransport = ChannelFrameTransport;

#[cfg(test)]
mod tests {
    use super::*;

    fn small_unbounded_config() -> ChannelConfig {
        ChannelConfig {
            buffer_size: 1,
            read_timeout: None,
            write_timeout: None,
        }
    }

    #[tokio::test]
    async fn test_transport_send_recv() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let data = b"Test message";
        t1.send_frame(data).await.unwrap();

        let received = t2.recv_frame().await.unwrap();
        assert_eq!(received.as_ref(), data);
    }

    #[tokio::test]
    async fn test_transport_bidirectional() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        t1.send_frame(b"Hello from t1").await.unwrap();
        let msg = t2.recv_frame().await.unwrap();
        assert_eq!(msg.as_ref(), b"Hello from t1");

        t2.send_frame(b"Hello from t2").await.unwrap();
        let msg = t1.recv_frame().await.unwrap();
        assert_eq!(msg.as_ref(), b"Hello from t2");
    }

    #[tokio::test]
    async fn test_transport_stats() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        t1.send_frame(b"test").await.unwrap();
        t2.recv_frame().await.unwrap();

        let stats1 = t1.stats().unwrap();
        assert_eq!(stats1.messages_sent, 1);
        assert_eq!(stats1.bytes_sent, 4);

        let stats2 = t2.stats().unwrap();
        assert_eq!(stats2.messages_received, 1);
        assert_eq!(stats2.bytes_received, 4);
    }

    #[tokio::test]
    async fn test_try_recv() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        assert!(t2.try_recv().unwrap().is_none());

        t1.send_frame(b"test").await.unwrap();

        let msg = t2.try_recv().unwrap();
        assert!(msg.is_some());
        assert_eq!(msg.unwrap().as_ref(), b"test");

        assert!(t2.try_recv().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_committed_frame_is_delivered_after_peer_close() {
        let (sender, receiver) =
            ChannelFrameTransport::create_pair("test", ChannelConfig::default()).unwrap();

        sender.send_frame(b"committed").await.unwrap();
        sender.close().await.unwrap();

        assert_eq!(receiver.recv_frame().await.unwrap().as_ref(), b"committed");
        assert!(matches!(
            receiver.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_aborted_blocked_send_does_not_publish_later() {
        let config = small_unbounded_config();
        let capacity = (config.buffer_size / 1024).max(16);
        let (sender, receiver) = ChannelFrameTransport::create_pair("test", config).unwrap();
        for value in 0..capacity {
            sender.send_frame(&[value as u8]).await.unwrap();
        }

        let sender = Arc::new(sender);
        let blocked_send = tokio::spawn({
            let sender = Arc::clone(&sender);
            async move { sender.send_frame(b"cancelled").await }
        });
        tokio::task::yield_now().await;
        assert!(!blocked_send.is_finished());

        blocked_send.abort();
        assert!(blocked_send.await.unwrap_err().is_cancelled());

        for value in 0..capacity {
            assert_eq!(
                receiver.recv_frame().await.unwrap().as_ref(),
                &[value as u8]
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        sender.send_frame(b"replacement").await.unwrap();
        assert_eq!(
            receiver.recv_frame().await.unwrap().as_ref(),
            b"replacement"
        );
        assert!(receiver.try_recv().unwrap().is_none());
        let stats = sender.stats().unwrap();
        assert_eq!(stats.messages_sent, capacity as u64 + 1);
        assert_eq!(stats.send_errors, 1);
    }

    #[tokio::test]
    async fn test_aborted_blocked_receive_does_not_consume_later_frame() {
        let (sender, receiver) =
            ChannelFrameTransport::create_pair("test", small_unbounded_config()).unwrap();
        let receiver = Arc::new(receiver);
        let blocked_receive = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            async move { receiver.recv_frame().await }
        });
        tokio::task::yield_now().await;
        assert!(!blocked_receive.is_finished());

        blocked_receive.abort();
        assert!(blocked_receive.await.unwrap_err().is_cancelled());

        sender.send_frame(b"preserved").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(receiver.try_recv().unwrap().unwrap().as_ref(), b"preserved");
        let stats = receiver.stats().unwrap();
        assert_eq!(stats.messages_received, 1);
        assert_eq!(stats.recv_errors, 1);
    }

    #[tokio::test]
    async fn test_local_close_interrupts_receive() {
        let config = ChannelConfig {
            read_timeout: None,
            ..ChannelConfig::default()
        };
        let (receiver, _peer) = ChannelFrameTransport::create_pair("test", config).unwrap();
        let receiver = Arc::new(receiver);
        let receive_task = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            async move { receiver.recv_frame().await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        receiver.close().await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(250), receive_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn test_peer_close_interrupts_receive() {
        let config = ChannelConfig {
            read_timeout: None,
            ..ChannelConfig::default()
        };
        let (receiver, peer) = ChannelFrameTransport::create_pair("test", config).unwrap();
        let receiver = Arc::new(receiver);
        let receive_task = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            async move { receiver.recv_frame().await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        peer.close().await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(250), receive_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn test_peer_close_interrupts_blocked_send() {
        let config = small_unbounded_config();
        let capacity = (config.buffer_size / 1024).max(16);
        let (sender, peer) = ChannelFrameTransport::create_pair("test", config).unwrap();
        for value in 0..capacity {
            sender.send_frame(&[value as u8]).await.unwrap();
        }

        let sender = Arc::new(sender);
        let send_task = tokio::spawn({
            let sender = Arc::clone(&sender);
            async move { sender.send_frame(b"blocked").await }
        });
        tokio::task::yield_now().await;
        assert!(!send_task.is_finished());

        peer.close().await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(250), send_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn test_connected_state_tracks_close_and_drop() {
        let (t1, t2) =
            ChannelFrameTransport::create_pair("test", ChannelConfig::default()).unwrap();
        assert!(t1.is_connected());
        assert!(t2.is_connected());

        t1.close().await.unwrap();
        t1.close().await.unwrap();
        assert!(!t1.is_connected());
        assert!(!t2.is_connected());

        let (dropped, survivor) =
            ChannelFrameTransport::create_pair("test", ChannelConfig::default()).unwrap();
        drop(dropped);
        assert!(!survivor.is_connected());
    }
}
