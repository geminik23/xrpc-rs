use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{TransportError, TransportResult};
use crate::transport::{FrameTransport, TransportStats};

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
    sender: Sender<Arc<dyn Any + Send + Sync>>,
    receiver: Receiver<Arc<dyn Any + Send + Sync>>,
    stats: Arc<Mutex<TransportStats>>,
    name: String,
}

impl ArcFrameTransport {
    /// Create a pair of connected transports.
    pub fn create_pair(name: impl Into<String>) -> TransportResult<(Self, Self)> {
        let name = name.into();

        let (tx1, rx1) = bounded(256);
        let (tx2, rx2) = bounded(256);

        let t1 = Self {
            sender: tx1,
            receiver: rx2,
            stats: Arc::new(Mutex::new(TransportStats::default())),
            name: format!("{}-1", name),
        };

        let t2 = Self {
            sender: tx2,
            receiver: rx1,
            stats: Arc::new(Mutex::new(TransportStats::default())),
            name: format!("{}-2", name),
        };

        Ok((t1, t2))
    }
}

impl ZeroCopyTransport for ArcFrameTransport {
    /// Send typed data without serialization
    fn send_typed<T: Send + Sync + 'static>(&self, data: T) -> TransportResult<()> {
        let arc_data = Arc::new(data) as Arc<dyn Any + Send + Sync>;
        self.sender
            .send(arc_data)
            .map_err(|_| TransportError::SendFailed {
                attempts: 1,
                reason: "Channel closed".into(),
            })?;

        self.stats.lock().messages_sent += 1;
        Ok(())
    }

    /// Receive typed data without deserialization
    fn recv_typed<T: Send + Sync + 'static>(&self) -> TransportResult<Arc<T>> {
        let data = self
            .receiver
            .recv()
            .map_err(|_| TransportError::ReceiveFailed {
                attempts: 1,
                reason: "Channel closed".into(),
            })?;

        self.stats.lock().messages_received += 1;

        data.downcast::<T>()
            .map_err(|_| TransportError::Protocol("Type mismatch".into()))
    }

    /// Try receive with timeout
    fn recv_typed_timeout<T: Send + Sync + 'static>(
        &self,
        timeout: Duration,
    ) -> TransportResult<Option<Arc<T>>> {
        match self.receiver.recv_timeout(timeout) {
            Ok(data) => {
                self.stats.lock().messages_received += 1;
                let typed = data
                    .downcast::<T>()
                    .map_err(|_| TransportError::Protocol("Type mismatch".into()))?;
                Ok(Some(typed))
            }
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => Ok(None),
            Err(_) => Err(TransportError::ReceiveFailed {
                attempts: 1,
                reason: "Channel closed".into(),
            }),
        }
    }
}

#[async_trait]
impl FrameTransport for ArcFrameTransport {
    async fn send_frame(&self, data: &[u8]) -> TransportResult<()> {
        let vec = data.to_vec();
        self.send_typed(vec)
    }

    async fn recv_frame(&self) -> TransportResult<Bytes> {
        let arc_vec = self.recv_typed::<Vec<u8>>()?;
        Ok(Bytes::from((*arc_vec).clone()))
    }

    fn is_connected(&self) -> bool {
        !self.sender.is_empty() || !self.receiver.is_empty()
    }

    async fn close(&self) -> TransportResult<()> {
        Ok(())
    }

    fn stats(&self) -> Option<TransportStats> {
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        &self.name
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
}
