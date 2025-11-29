use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;

use crate::error::TransportResult;

pub mod arc;
pub mod channel;
pub mod direct;
pub mod shared_memory;
pub mod tcp;
pub mod utils;

pub use utils::spawn_weak_loop;

/// Transport trait for abstracting communication mechanisms
#[async_trait]
pub trait Transport: Send + Sync + Debug {
    /// Send data through the transport
    async fn send(&self, data: &[u8]) -> TransportResult<()>;

    /// Receive data from the transport
    async fn recv(&self) -> TransportResult<Bytes>;

    /// Check if the transport is connected
    fn is_connected(&self) -> bool;

    /// Check if the transport is healthy
    fn is_healthy(&self) -> bool {
        true
    }

    /// Close the transport
    async fn close(&self) -> TransportResult<()>;

    /// Get transport statistics
    fn stats(&self) -> Option<TransportStats> {
        None
    }

    /// Get transport name/identifier
    fn name(&self) -> &str {
        "unknown"
    }
}

/// Statistics collected by transport implementations
#[derive(Debug, Clone, Default)]
pub struct TransportStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub send_errors: u64,
    pub recv_errors: u64,
    pub avg_latency_us: Option<u64>,
}

impl TransportStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, other: &TransportStats) {
        self.messages_sent += other.messages_sent;
        self.messages_received += other.messages_received;
        self.bytes_sent += other.bytes_sent;
        self.bytes_received += other.bytes_received;
        self.send_errors += other.send_errors;
        self.recv_errors += other.recv_errors;

        if let (Some(our_latency), Some(other_latency)) =
            (self.avg_latency_us, other.avg_latency_us)
        {
            self.avg_latency_us = Some((our_latency + other_latency) / 2);
        } else if other.avg_latency_us.is_some() {
            self.avg_latency_us = other.avg_latency_us;
        }
    }
}

impl std::fmt::Display for TransportStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Transport Statistics:")?;
        writeln!(f, "  Messages sent:     {}", self.messages_sent)?;
        writeln!(f, "  Messages received: {}", self.messages_received)?;
        writeln!(f, "  Bytes sent:        {}", self.bytes_sent)?;
        writeln!(f, "  Bytes received:    {}", self.bytes_received)?;
        writeln!(f, "  Send errors:       {}", self.send_errors)?;
        writeln!(f, "  Receive errors:    {}", self.recv_errors)?;
        if let Some(latency) = self.avg_latency_us {
            writeln!(f, "  Avg latency:       {}μs", latency)?;
        }
        Ok(())
    }
}

#[async_trait]
impl<T: Transport + ?Sized> Transport for std::sync::Arc<T> {
    async fn send(&self, data: &[u8]) -> TransportResult<()> {
        (**self).send(data).await
    }

    async fn recv(&self) -> TransportResult<Bytes> {
        (**self).recv().await
    }

    fn is_connected(&self) -> bool {
        (**self).is_connected()
    }

    fn is_healthy(&self) -> bool {
        (**self).is_healthy()
    }

    async fn close(&self) -> TransportResult<()> {
        (**self).close().await
    }

    fn stats(&self) -> Option<TransportStats> {
        (**self).stats()
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}

#[async_trait]
impl<T: Transport + ?Sized> Transport for Box<T> {
    async fn send(&self, data: &[u8]) -> TransportResult<()> {
        (**self).send(data).await
    }

    async fn recv(&self) -> TransportResult<Bytes> {
        (**self).recv().await
    }

    fn is_connected(&self) -> bool {
        (**self).is_connected()
    }

    fn is_healthy(&self) -> bool {
        (**self).is_healthy()
    }

    async fn close(&self) -> TransportResult<()> {
        (**self).close().await
    }

    fn stats(&self) -> Option<TransportStats> {
        (**self).stats()
    }

    fn name(&self) -> &str {
        (**self).name()
    }
}
