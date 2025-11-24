use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{TransportError, TransportResult};
use crate::transport::{Transport, TransportStats};

/// Arc-based transport for same-process communication without serialization
pub struct ArcTransport {
    sender: Sender<Arc<dyn Any + Send + Sync>>,
    receiver: Receiver<Arc<dyn Any + Send + Sync>>,
    stats: Arc<Mutex<TransportStats>>,
    name: String,
}

impl ArcTransport {
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

    /// Send typed data without serialization
    pub fn send_typed<T: Send + Sync + 'static>(&self, data: T) -> TransportResult<()> {
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
    pub fn recv_typed<T: Send + Sync + 'static>(&self) -> TransportResult<Arc<T>> {
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
    pub fn recv_typed_timeout<T: Send + Sync + 'static>(
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
impl Transport for ArcTransport {
    async fn send(&self, data: &[u8]) -> TransportResult<()> {
        let vec = data.to_vec();
        self.send_typed(vec)
    }

    async fn recv(&self) -> TransportResult<Bytes> {
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

impl std::fmt::Debug for ArcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcTransport")
            .field("name", &self.name)
            .finish()
    }
}

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
        let (t1, t2) = ArcTransport::create_pair("test").unwrap();

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
        let (t1, t2) = ArcTransport::create_pair("test").unwrap();

        let data = vec![1, 2, 3, 4, 5];
        t1.send_typed(data.clone()).unwrap();

        let received = t2.recv_typed::<Vec<i32>>().unwrap();
        assert_eq!(*received, data);
    }

    #[tokio::test]
    async fn test_transport_trait_compat() {
        let (t1, t2) = ArcTransport::create_pair("test").unwrap();

        t1.send(b"test").await.unwrap();
        let received = t2.recv().await.unwrap();

        assert_eq!(received.as_ref(), b"test");
    }
}
