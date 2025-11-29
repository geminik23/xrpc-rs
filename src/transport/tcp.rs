use crate::error::{TransportError, TransportResult};
use crate::transport::{Transport, TransportStats};
use async_trait::async_trait;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Configuration for TCP transport
#[derive(Debug, Clone)]
pub struct TcpConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Read timeout (None for no timeout)
    pub read_timeout: Option<Duration>,
    /// Write timeout (None for no timeout)
    pub write_timeout: Option<Duration>,
    /// Enable TCP_NODELAY (disable Nagle's algorithm)
    pub nodelay: bool,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16 MB
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            nodelay: true,
        }
    }
}

impl TcpConfig {
    /// Create a new configuration with custom max message size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set read timeout
    pub fn with_read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Set write timeout
    pub fn with_write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Enable or disable TCP_NODELAY
    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = nodelay;
        self
    }
}

/// TCP transport for network-based RPC communication
#[derive(Debug)]
pub struct TcpTransport {
    config: TcpConfig,
    stream: Arc<Mutex<TcpStream>>,
    peer_addr: SocketAddr,
    connected: Arc<AtomicBool>,
    stats: Arc<Mutex<TransportStats>>,
}

impl TcpTransport {
    /// Create a new TCP transport by connecting to an address
    pub async fn connect(addr: SocketAddr, config: TcpConfig) -> TransportResult<Self> {
        let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::Timeout {
                duration_ms: config.connect_timeout.as_millis() as u64,
                operation: format!("connecting to {}", addr),
            })?
            .map_err(|e| TransportError::ConnectionFailed {
                name: addr.to_string(),
                attempts: 1,
                reason: e.to_string(),
            })?;

        // Disable Nagle's algorithm for lower latency
        if config.nodelay {
            stream.set_nodelay(true).map_err(|e| {
                TransportError::Protocol(format!("Failed to set TCP_NODELAY: {}", e))
            })?;
        }

        let peer_addr = stream
            .peer_addr()
            .map_err(|e| TransportError::Protocol(format!("Failed to get peer address: {}", e)))?;

        Ok(Self {
            config,
            stream: Arc::new(Mutex::new(stream)),
            peer_addr,
            connected: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(Mutex::new(TransportStats::default())),
        })
    }

    /// Create a new TCP transport from an existing stream
    pub fn from_stream(stream: TcpStream, config: TcpConfig) -> TransportResult<Self> {
        let peer_addr = stream
            .peer_addr()
            .map_err(|e| TransportError::Protocol(format!("Failed to get peer address: {}", e)))?;

        // Disable Nagle's algorithm for lower latency
        if config.nodelay {
            stream.set_nodelay(true).map_err(|e| {
                TransportError::Protocol(format!("Failed to set TCP_NODELAY: {}", e))
            })?;
        }

        Ok(Self {
            config,
            stream: Arc::new(Mutex::new(stream)),
            peer_addr,
            connected: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(Mutex::new(TransportStats::default())),
        })
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Send raw bytes with length prefix
    async fn send_bytes(&self, data: &[u8]) -> TransportResult<()> {
        if !self.is_connected() {
            return Err(TransportError::NotConnected);
        }

        if data.len() > self.config.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: data.len(),
                max: self.config.max_message_size,
            });
        }

        // Write length prefix (4 bytes, big-endian)
        let len = data.len() as u32;
        let len_bytes = len.to_be_bytes();

        let write_op = async {
            let mut stream = self.stream.lock().await;

            stream.write_all(&len_bytes).await.map_err(|e| {
                self.connected.store(false, Ordering::Release);
                TransportError::SendFailed {
                    attempts: 1,
                    reason: e.to_string(),
                }
            })?;

            stream.write_all(data).await.map_err(|e| {
                self.connected.store(false, Ordering::Release);
                TransportError::SendFailed {
                    attempts: 1,
                    reason: e.to_string(),
                }
            })?;

            Ok::<(), TransportError>(())
        };

        // Apply write timeout if configured
        if let Some(timeout) = self.config.write_timeout {
            tokio::time::timeout(timeout, write_op)
                .await
                .map_err(|_| TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "TCP write".to_string(),
                })??;
        } else {
            write_op.await?;
        }

        // Update stats
        let mut stats = self.stats.lock().await;
        stats.messages_sent += 1;
        stats.bytes_sent += data.len() as u64 + 4; // Include length prefix

        Ok(())
    }

    /// Receive raw bytes with length prefix
    async fn recv_bytes(&self) -> TransportResult<Bytes> {
        if !self.is_connected() {
            return Err(TransportError::NotConnected);
        }

        // Read length prefix (4 bytes)
        let mut len_bytes = [0u8; 4];

        let read_len_op = async {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut len_bytes).await.map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.connected.store(false, Ordering::Release);
                }
                TransportError::ReceiveFailed {
                    attempts: 1,
                    reason: e.to_string(),
                }
            })
        };

        if let Some(timeout) = self.config.read_timeout {
            tokio::time::timeout(timeout, read_len_op)
                .await
                .map_err(|_| TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "TCP read length".to_string(),
                })??;
        } else {
            read_len_op.await?;
        }

        let len = u32::from_be_bytes(len_bytes) as usize;

        if len > self.config.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: len,
                max: self.config.max_message_size,
            });
        }

        // Read message data
        let mut buffer = vec![0u8; len];

        let read_data_op = async {
            let mut stream = self.stream.lock().await;
            stream.read_exact(&mut buffer).await.map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.connected.store(false, Ordering::Release);
                }
                TransportError::ReceiveFailed {
                    attempts: 1,
                    reason: e.to_string(),
                }
            })
        };

        if let Some(timeout) = self.config.read_timeout {
            tokio::time::timeout(timeout, read_data_op)
                .await
                .map_err(|_| TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "TCP read data".to_string(),
                })??;
        } else {
            read_data_op.await?;
        }

        // Update stats
        let mut stats = self.stats.lock().await;
        stats.messages_received += 1;
        stats.bytes_received += len as u64 + 4; // Include length prefix

        Ok(Bytes::from(buffer))
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn send(&self, data: &[u8]) -> TransportResult<()> {
        self.send_bytes(data).await
    }

    async fn recv(&self) -> TransportResult<Bytes> {
        self.recv_bytes().await
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_healthy(&self) -> bool {
        self.is_connected()
    }

    async fn close(&self) -> TransportResult<()> {
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn stats(&self) -> Option<TransportStats> {
        // Try to get stats synchronously if possible
        if let Ok(stats) = self.stats.try_lock() {
            Some(stats.clone())
        } else {
            // Fall back to blocking wait in a runtime context
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { Some(self.stats.lock().await.clone()) })
            })
        }
    }

    fn name(&self) -> &str {
        "tcp"
    }
}

/// TCP listener for accepting incoming connections
pub struct TcpTransportListener {
    listener: TcpListener,
    config: TcpConfig,
}

impl TcpTransportListener {
    /// Bind to a socket address and listen for incoming connections
    pub async fn bind(addr: SocketAddr, config: TcpConfig) -> TransportResult<Self> {
        let listener =
            TcpListener::bind(addr)
                .await
                .map_err(|e| TransportError::ConnectionFailed {
                    name: addr.to_string(),
                    attempts: 1,
                    reason: format!("Failed to bind: {}", e),
                })?;

        Ok(Self { listener, config })
    }

    /// Get the local address the listener is bound to
    pub fn local_addr(&self) -> TransportResult<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| TransportError::Protocol(format!("Failed to get local address: {}", e)))
    }

    /// Accept an incoming connection
    pub async fn accept(&self) -> TransportResult<TcpTransport> {
        let (stream, _addr) =
            self.listener
                .accept()
                .await
                .map_err(|e| TransportError::ConnectionFailed {
                    name: "tcp_listener".to_string(),
                    attempts: 1,
                    reason: format!("Failed to accept connection: {}", e),
                })?;

        TcpTransport::from_stream(stream, self.config.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tcp_transport_connection() {
        let config = TcpConfig::default();

        // Start a listener
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
            .await
            .unwrap();

        let addr = listener.local_addr().unwrap();

        // Connect a client
        let client = tokio::spawn(async move { TcpTransport::connect(addr, config).await });

        // Accept the connection
        let server = listener.accept().await.unwrap();

        let client = client.await.unwrap().unwrap();

        assert!(client.is_connected());
        assert!(server.is_connected());
    }

    #[tokio::test]
    async fn test_tcp_send_recv() {
        let config = TcpConfig::default();

        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
            .await
            .unwrap();

        let addr = listener.local_addr().unwrap();

        // Connect a client
        let client_task = tokio::spawn(async move { TcpTransport::connect(addr, config).await });

        // Accept the connection
        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        // Send from client to server
        let test_data = b"Hello, TCP!";
        client.send(test_data).await.unwrap();

        let received = server.recv().await.unwrap();
        assert_eq!(received.as_ref(), test_data);

        // Send from server to client
        let response_data = b"Hello back!";
        server.send(response_data).await.unwrap();

        let received = client.recv().await.unwrap();
        assert_eq!(received.as_ref(), response_data);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tcp_stats() {
        let config = TcpConfig::default();

        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
            .await
            .unwrap();

        let addr = listener.local_addr().unwrap();

        let client_task = tokio::spawn(async move { TcpTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        // Send a message
        let test_data = b"Test message";
        client.send(test_data).await.unwrap();
        server.recv().await.unwrap();

        // Check stats
        let client_stats = client.stats().unwrap();
        assert_eq!(client_stats.messages_sent, 1);
        assert!(client_stats.bytes_sent > test_data.len() as u64); // Includes length prefix

        let server_stats = server.stats().unwrap();
        assert_eq!(server_stats.messages_received, 1);
        assert!(server_stats.bytes_received > test_data.len() as u64);
    }

    #[tokio::test]
    async fn test_tcp_large_message() {
        let config = TcpConfig::default();

        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
            .await
            .unwrap();

        let addr = listener.local_addr().unwrap();

        let client_task = tokio::spawn(async move { TcpTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        // Send a large message (1 MB)
        let large_data = vec![0xAB; 1024 * 1024];
        client.send(&large_data).await.unwrap();

        let received = server.recv().await.unwrap();
        assert_eq!(received.len(), large_data.len());
        assert_eq!(received.as_ref(), large_data.as_slice());
    }

    #[tokio::test]
    async fn test_tcp_message_too_large() {
        let config = TcpConfig::default().with_max_message_size(1024);

        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
            .await
            .unwrap();

        let addr = listener.local_addr().unwrap();

        let client = TcpTransport::connect(addr, config).await.unwrap();

        // Try to send a message larger than the limit
        let large_data = vec![0; 2048];
        let result = client.send(&large_data).await;

        assert!(matches!(
            result,
            Err(TransportError::MessageTooLarge { .. })
        ));
    }
}
