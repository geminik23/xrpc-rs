use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

use crate::error::{TransportError, TransportResult};
use crate::transport::{FrameTransport, TransportStats};

type ArcValue = Arc<dyn Any + Send + Sync>;
const QUEUE_CAPACITY: usize = 256;

struct ArcQueueState {
    values: VecDeque<ArcValue>,
    sender_closed: bool,
    receiver_closed: bool,
}

struct ArcQueue {
    state: Mutex<ArcQueueState>,
    changed: Condvar,
    not_empty: Notify,
    not_full: Notify,
}

impl ArcQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(ArcQueueState {
                values: VecDeque::with_capacity(QUEUE_CAPACITY),
                sender_closed: false,
                receiver_closed: false,
            }),
            changed: Condvar::new(),
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    fn close_sender(&self) {
        self.state.lock().sender_closed = true;
        self.wake_all();
    }

    fn close_receiver(&self) {
        self.state.lock().receiver_closed = true;
        self.wake_all();
    }

    fn wake_all(&self) {
        self.changed.notify_all();
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    fn send(&self, value: ArcValue) -> TransportResult<()> {
        let mut state = self.state.lock();
        loop {
            if state.sender_closed || state.receiver_closed {
                return Err(TransportError::ConnectionClosed);
            }

            if state.values.len() < QUEUE_CAPACITY {
                state.values.push_back(value);
                drop(state);
                self.changed.notify_all();
                self.not_empty.notify_one();
                return Ok(());
            }

            self.changed.wait(&mut state);
        }
    }

    fn recv(&self) -> TransportResult<ArcValue> {
        let mut state = self.state.lock();
        loop {
            if state.receiver_closed {
                return Err(TransportError::ConnectionClosed);
            }

            if let Some(value) = state.values.pop_front() {
                drop(state);
                self.changed.notify_all();
                self.not_full.notify_one();
                return Ok(value);
            }

            if state.sender_closed {
                return Err(TransportError::ConnectionClosed);
            }

            self.changed.wait(&mut state);
        }
    }

    fn recv_timeout(&self, timeout: Duration) -> TransportResult<Option<ArcValue>> {
        let started = Instant::now();
        let mut state = self.state.lock();
        loop {
            if state.receiver_closed {
                return Err(TransportError::ConnectionClosed);
            }

            if let Some(value) = state.values.pop_front() {
                drop(state);
                self.changed.notify_all();
                self.not_full.notify_one();
                return Ok(Some(value));
            }

            if state.sender_closed {
                return Err(TransportError::ConnectionClosed);
            }

            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Ok(None);
            };
            if remaining.is_zero() || self.changed.wait_for(&mut state, remaining).timed_out() {
                if state.receiver_closed {
                    return Err(TransportError::ConnectionClosed);
                }
                if let Some(value) = state.values.pop_front() {
                    drop(state);
                    self.changed.notify_all();
                    self.not_full.notify_one();
                    return Ok(Some(value));
                }
                if state.sender_closed {
                    return Err(TransportError::ConnectionClosed);
                }
                return Ok(None);
            }
        }
    }

    async fn send_async(&self, value: ArcValue) -> TransportResult<()> {
        loop {
            let notified = self.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut state = self.state.lock();
                if state.sender_closed || state.receiver_closed {
                    return Err(TransportError::ConnectionClosed);
                }

                if state.values.len() < QUEUE_CAPACITY {
                    state.values.push_back(value);
                    drop(state);
                    self.changed.notify_all();
                    self.not_empty.notify_one();
                    return Ok(());
                }
            }

            notified.await;
        }
    }

    async fn recv_async(&self) -> TransportResult<ArcValue> {
        loop {
            let notified = self.not_empty.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut state = self.state.lock();
                if state.receiver_closed {
                    return Err(TransportError::ConnectionClosed);
                }

                if let Some(value) = state.values.pop_front() {
                    drop(state);
                    self.changed.notify_all();
                    self.not_full.notify_one();
                    return Ok(value);
                }

                if state.sender_closed {
                    return Err(TransportError::ConnectionClosed);
                }
            }

            notified.await;
        }
    }
}

/// Trait for zero-copy typed transport (same-process only).
pub trait ZeroCopyTransport {
    /// Send typed data without serialization.
    fn send_typed<T: Send + Sync + 'static>(&self, data: T) -> TransportResult<()>;

    /// Receive typed data without deserialization.
    fn recv_typed<T: Send + Sync + 'static>(&self) -> TransportResult<Arc<T>>;

    /// Try receive with timeout.
    fn recv_typed_timeout<T: Send + Sync + 'static>(
        &self,
        timeout: Duration,
    ) -> TransportResult<Option<Arc<T>>>;
}

/// Zero-copy in-process frame transport using Arc (Layer 1).
pub struct ArcFrameTransport {
    sender: Arc<ArcQueue>,
    receiver: Arc<ArcQueue>,
    stats: Arc<Mutex<TransportStats>>,
    local_closed: Arc<AtomicBool>,
    peer_closed: Arc<AtomicBool>,
    name: String,
}

impl ArcFrameTransport {
    /// Create a pair of connected transports.
    pub fn create_pair(name: impl Into<String>) -> TransportResult<(Self, Self)> {
        let name = name.into();
        let queue1 = Arc::new(ArcQueue::new());
        let queue2 = Arc::new(ArcQueue::new());
        let endpoint1_closed = Arc::new(AtomicBool::new(false));
        let endpoint2_closed = Arc::new(AtomicBool::new(false));

        let t1 = Self {
            sender: Arc::clone(&queue1),
            receiver: Arc::clone(&queue2),
            stats: Arc::new(Mutex::new(TransportStats::default())),
            local_closed: Arc::clone(&endpoint1_closed),
            peer_closed: Arc::clone(&endpoint2_closed),
            name: format!("{}-1", name),
        };

        let t2 = Self {
            sender: queue2,
            receiver: queue1,
            stats: Arc::new(Mutex::new(TransportStats::default())),
            local_closed: endpoint2_closed,
            peer_closed: endpoint1_closed,
            name: format!("{}-2", name),
        };

        Ok((t1, t2))
    }

    fn close_endpoint(&self) {
        self.local_closed.store(true, Ordering::Release);
        self.sender.close_sender();
        self.receiver.close_receiver();
    }
}

impl ZeroCopyTransport for ArcFrameTransport {
    /// Send typed data without serialization.
    fn send_typed<T: Send + Sync + 'static>(&self, data: T) -> TransportResult<()> {
        self.sender.send(Arc::new(data) as ArcValue)?;
        self.stats.lock().messages_sent += 1;
        Ok(())
    }

    /// Receive typed data without deserialization.
    fn recv_typed<T: Send + Sync + 'static>(&self) -> TransportResult<Arc<T>> {
        let data = self.receiver.recv()?;
        self.stats.lock().messages_received += 1;
        data.downcast::<T>()
            .map_err(|_| TransportError::Protocol("Type mismatch".into()))
    }

    /// Try receive with timeout.
    fn recv_typed_timeout<T: Send + Sync + 'static>(
        &self,
        timeout: Duration,
    ) -> TransportResult<Option<Arc<T>>> {
        match self.receiver.recv_timeout(timeout)? {
            Some(data) => {
                let mut stats = self.stats.lock();
                stats.messages_received += 1;
                data.downcast::<T>().map(Some).map_err(|_| {
                    stats.recv_errors += 1;
                    TransportError::Protocol("Type mismatch".into())
                })
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl FrameTransport for ArcFrameTransport {
    async fn send_frame(&self, data: &[u8]) -> TransportResult<()> {
        let result = self
            .sender
            .send_async(Arc::new(data.to_vec()) as ArcValue)
            .await;

        let mut stats = self.stats.lock();
        if result.is_ok() {
            stats.messages_sent += 1;
            stats.bytes_sent += data.len() as u64;
        } else {
            stats.send_errors += 1;
        }
        result
    }

    async fn recv_frame(&self) -> TransportResult<Bytes> {
        let result = match self.receiver.recv_async().await {
            Ok(value) => value
                .downcast::<Vec<u8>>()
                .map(|data| Bytes::from((*data).clone()))
                .map_err(|_| TransportError::Protocol("Type mismatch".into())),
            Err(error) => Err(error),
        };

        let mut stats = self.stats.lock();
        match &result {
            Ok(bytes) => {
                stats.messages_received += 1;
                stats.bytes_received += bytes.len() as u64;
            }
            Err(_) => stats.recv_errors += 1,
        }
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
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for ArcFrameTransport {
    fn drop(&mut self) {
        self.close_endpoint();
    }
}

impl std::fmt::Debug for ArcFrameTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcFrameTransport")
            .field("name", &self.name)
            .finish()
    }
}

// Deprecated alias for backward compatibility
#[deprecated(since = "0.2.0", note = "Use ArcFrameTransport instead")]
pub type ArcTransport = ArcFrameTransport;

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll};

    #[derive(Debug, Clone, PartialEq)]
    struct TestData {
        value: i32,
        text: String,
    }

    #[test]
    fn test_zero_copy_basic() {
        let (t1, t2) = ArcFrameTransport::create_pair("test").unwrap();

        let data = TestData {
            value: 42,
            text: "hello".to_string(),
        };

        t1.send_typed(data.clone()).unwrap();
        let received = t2.recv_typed::<TestData>().unwrap();

        assert_eq!(*received, data);
    }

    #[test]
    fn test_zero_copy_arc_sharing() {
        let (t1, t2) = ArcFrameTransport::create_pair("test").unwrap();

        let data = vec![1, 2, 3, 4, 5];
        t1.send_typed(data.clone()).unwrap();

        let received = t2.recv_typed::<Vec<i32>>().unwrap();
        assert_eq!(*received, data);
    }

    #[tokio::test]
    async fn test_frame_transport_compat() {
        let (t1, t2) = ArcFrameTransport::create_pair("test").unwrap();

        t1.send_frame(b"test").await.unwrap();
        let received = t2.recv_frame().await.unwrap();

        assert_eq!(received.as_ref(), b"test");
    }

    #[tokio::test]
    async fn test_committed_frame_is_delivered_after_peer_close() {
        let (sender, receiver) = ArcFrameTransport::create_pair("test").unwrap();

        sender.send_frame(b"committed").await.unwrap();
        sender.close().await.unwrap();

        assert_eq!(receiver.recv_frame().await.unwrap().as_ref(), b"committed");
        assert!(matches!(
            receiver.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_typed_operations_respect_close_and_drain_committed_data() {
        let (sender, receiver) = ArcFrameTransport::create_pair("test").unwrap();

        sender.send_typed(42_u32).unwrap();
        sender.close().await.unwrap();

        assert!(matches!(
            sender.send_typed(7_u32),
            Err(TransportError::ConnectionClosed)
        ));
        assert!(matches!(
            receiver.send_typed(7_u32),
            Err(TransportError::ConnectionClosed)
        ));
        assert_eq!(*receiver.recv_typed::<u32>().unwrap(), 42);
        assert!(matches!(
            receiver.recv_typed_timeout::<u32>(Duration::ZERO),
            Err(TransportError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_local_close_interrupts_receive() {
        let (receiver, _peer) = ArcFrameTransport::create_pair("test").unwrap();
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
        let (receiver, peer) = ArcFrameTransport::create_pair("test").unwrap();
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
    async fn test_close_interrupts_blocked_send() {
        let (sender, peer) = ArcFrameTransport::create_pair("test").unwrap();
        for value in 0..QUEUE_CAPACITY {
            sender.send_typed(value).unwrap();
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

    #[tokio::test(flavor = "current_thread")]
    async fn test_blocked_async_send_poll_does_not_block_runtime_thread() {
        let (sender, _receiver) = ArcFrameTransport::create_pair("test").unwrap();
        for value in 0..QUEUE_CAPACITY {
            sender.send_typed(value).unwrap();
        }

        let mut send = Box::pin(sender.send_frame(b"blocked"));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let started = Instant::now();

        assert!(matches!(send.as_mut().poll(&mut context), Poll::Pending));
        assert!(started.elapsed() < Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_connected_state_tracks_close_and_drop() {
        let (t1, t2) = ArcFrameTransport::create_pair("test").unwrap();
        assert!(t1.is_connected());
        assert!(t2.is_connected());

        t1.close().await.unwrap();
        t1.close().await.unwrap();
        assert!(!t1.is_connected());
        assert!(!t2.is_connected());

        let (dropped, survivor) = ArcFrameTransport::create_pair("test").unwrap();
        drop(dropped);
        assert!(!survivor.is_connected());
    }
}
