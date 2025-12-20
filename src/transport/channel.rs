use async_trait::async_trait;
use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

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
}

/// In-process channel frame transport using crossbeam (Layer 1).
pub struct ChannelFrameTransport {
    sender: Sender<Bytes>,
    receiver: Receiver<Bytes>,
    config: ChannelConfig,
    stats: Arc<Mutex<ChannelStats>>,
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

        let (tx1, rx1) = bounded(capacity);
        let (tx2, rx2) = bounded(capacity);

        let stats1 = Arc::new(Mutex::new(ChannelStats::default()));
        let stats2 = Arc::new(Mutex::new(ChannelStats::default()));

        let transport1 = Self {
            sender: tx1,
            receiver: rx2,
            config: config.clone(),
            stats: stats1,
            name: format!("{}-client", name),
        };

        let transport2 = Self {
            sender: tx2,
            receiver: rx1,
            config,
            stats: stats2,
            name: format!("{}-server", name),
        };

        Ok((transport1, transport2))
    }

    /// Non-blocking receive (used by SharedMemoryFrameTransport).
    pub fn try_recv(&self) -> TransportResult<Option<Bytes>> {
        match self.receiver.try_recv() {
            Ok(bytes) => {
                let mut stats = self.stats.lock();
                stats.messages_received += 1;
                stats.bytes_received += bytes.len() as u64;
                Ok(Some(bytes))
            }
            Err(crossbeam::channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                Err(TransportError::ReceiveFailed {
                    attempts: 1,
                    reason: "Channel closed".into(),
                })
            }
        }
    }
}

#[async_trait]
impl FrameTransport for ChannelFrameTransport {
    async fn send_frame(&self, data: &[u8]) -> TransportResult<()> {
        let bytes = Bytes::copy_from_slice(data);

        let result = if let Some(timeout) = self.config.write_timeout {
            tokio::select! {
                result = tokio::task::spawn_blocking({
                    let sender = self.sender.clone();
                    let bytes = bytes.clone();
                    move || sender.send(bytes)
                }) => {
                    result
                        .map_err(|e| TransportError::SendFailed {
                            attempts: 1,
                            reason: e.to_string(),
                        })?
                        .map_err(|_| TransportError::SendFailed {
                            attempts: 1,
                            reason: "Channel closed".into(),
                        })
                }
                _ = tokio::time::sleep(timeout) => {
                    Err(TransportError::Timeout {
                        duration_ms: timeout.as_millis() as u64,
                        operation: "send".into(),
                    })
                }
            }
        } else {
            self.sender
                .send(bytes)
                .map_err(|_| TransportError::SendFailed {
                    attempts: 1,
                    reason: "Channel closed".into(),
                })
        };

        if result.is_ok() {
            let mut stats = self.stats.lock();
            stats.messages_sent += 1;
            stats.bytes_sent += data.len() as u64;
        }

        result
    }

    async fn recv_frame(&self) -> TransportResult<Bytes> {
        let bytes = if let Some(timeout) = self.config.read_timeout {
            tokio::select! {
                result = tokio::task::spawn_blocking({
                    let receiver = self.receiver.clone();
                    move || receiver.recv()
                }) => {
                    result
                        .map_err(|e| TransportError::ReceiveFailed {
                            attempts: 1,
                            reason: e.to_string(),
                        })?
                        .map_err(|_| TransportError::ReceiveFailed {
                            attempts: 1,
                            reason: "Channel closed".into(),
                        })?
                }
                _ = tokio::time::sleep(timeout) => {
                    return Err(TransportError::Timeout {
                        duration_ms: timeout.as_millis() as u64,
                        operation: "receive".into(),
                    });
                }
            }
        } else {
            self.receiver
                .recv()
                .map_err(|_| TransportError::ReceiveFailed {
                    attempts: 1,
                    reason: "Channel closed".into(),
                })?
        };

        let mut stats = self.stats.lock();
        stats.messages_received += 1;
        stats.bytes_received += bytes.len() as u64;

        Ok(bytes)
    }

    fn is_connected(&self) -> bool {
        true
    }

    async fn close(&self) -> TransportResult<()> {
        Ok(())
    }

    fn stats(&self) -> Option<TransportStats> {
        let stats = self.stats.lock();
        Some(TransportStats {
            messages_sent: stats.messages_sent,
            messages_received: stats.messages_received,
            bytes_sent: stats.bytes_sent,
            bytes_received: stats.bytes_received,
            send_errors: 0,
            recv_errors: 0,
            avg_latency_us: None,
        })
    }

    fn name(&self) -> &str {
        &self.name
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
}
