#![cfg(unix)]

use crate::error::{TransportError, TransportResult};
use crate::transport::{Transport, TransportStats};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

/// Configuration for Unix domain socket transport
#[derive(Debug, Clone)]
pub struct UnixConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Read timeout (None for no timeout)
    pub read_timeout: Option<Duration>,
    /// Write timeout (None for no timeout)
    pub write_timeout: Option<Duration>,
}

impl Default for UnixConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024, // 16 MB
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
        }
    }
}

impl UnixConfig {
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
}

/// Unix domain socket transport for local IPC
#[derive(Debug)]
pub struct UnixSocketTransport {
    config: UnixConfig,
    stream: Arc<Mutex<UnixStream>>,
    path: PathBuf,
    connected: Arc<AtomicBool>,
    stats: Arc<Mutex<TransportStats>>,
}

impl UnixSocketTransport {
    /// Create a new Unix socket transport by connecting to a path
    pub async fn connect<P: AsRef<Path>>(path: P, config: UnixConfig) -> TransportResult<Self> {
        let path = path.as_ref().to_path_buf();

        let stream = tokio::time::timeout(config.connect_timeout, UnixStream::connect(&path))
            .await
            .map_err(|_| TransportError::Timeout {
                duration_ms: config.connect_timeout.as_millis() as u64,
                operation: format!("connecting to {:?}", path),
            })?
            .map_err(|e| TransportError::ConnectionFailed {
                name: path.display().to_string(),
                attempts: 1,
                reason: e.to_string(),
            })?;

        Ok(Self {
            config,
            stream: Arc::new(Mutex::new(stream)),
            path,
            connected: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(Mutex::new(TransportStats::default())),
        })
    }

    /// Create a new Unix socket transport from an existing stream
    pub fn from_stream<P: AsRef<Path>>(stream: UnixStream, path: P, config: UnixConfig) -> Self {
        Self {
            config,
            stream: Arc::new(Mutex::new(stream)),
            path: path.as_ref().to_path_buf(),
            connected: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(Mutex::new(TransportStats::default())),
        }
    }

    /// Get the socket path
    pub fn path(&self) -> &Path {
        &self.path
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

        // Write length prefix (4 bytes, little-endian)
        let len = data.len() as u32;
        let len_bytes = len.to_le_bytes();

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
                    operation: "Unix socket write".to_string(),
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
                    operation: "Unix socket read length".to_string(),
                })??;
        } else {
            read_len_op.await?;
        }

        let len = u32::from_le_bytes(len_bytes) as usize;

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
                    operation: "Unix socket read data".to_string(),
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
impl Transport for UnixSocketTransport {
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
        "unix"
    }
}

/// Unix domain socket listener for accepting incoming connections
pub struct UnixSocketListener {
    listener: UnixListener,
    config: UnixConfig,
    path: PathBuf,
}

impl UnixSocketListener {
    /// Bind to a Unix socket path and listen for incoming connections
    pub async fn bind<P: AsRef<Path>>(path: P, config: UnixConfig) -> TransportResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Remove the socket file if it already exists
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| TransportError::ConnectionFailed {
                name: path.display().to_string(),
                attempts: 1,
                reason: format!("Failed to remove existing socket file: {}", e),
            })?;
        }

        let listener = UnixListener::bind(&path).map_err(|e| TransportError::ConnectionFailed {
            name: path.display().to_string(),
            attempts: 1,
            reason: format!("Failed to bind: {}", e),
        })?;

        Ok(Self {
            listener,
            config,
            path,
        })
    }

    /// Get the socket path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accept an incoming connection
    pub async fn accept(&self) -> TransportResult<UnixSocketTransport> {
        let (stream, _addr) =
            self.listener
                .accept()
                .await
                .map_err(|e| TransportError::ConnectionFailed {
                    name: "unix_listener".to_string(),
                    attempts: 1,
                    reason: format!("Failed to accept connection: {}", e),
                })?;

        Ok(UnixSocketTransport::from_stream(
            stream,
            &self.path,
            self.config.clone(),
        ))
    }
}

impl Drop for UnixSocketListener {
    fn drop(&mut self) {
        // Clean up the socket file
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_socket_path(name: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!("xrpc_test_{}_{}.sock", name, std::process::id()));
        path
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unix_transport_connection() {
        let socket_path = temp_socket_path("connection");
        let config = UnixConfig::default();

        // Start a listener
        let listener = UnixSocketListener::bind(&socket_path, config.clone())
            .await
            .unwrap();

        // Connect a client
        let client =
            tokio::spawn(async move { UnixSocketTransport::connect(&socket_path, config).await });

        // Accept the connection
        let server = listener.accept().await.unwrap();

        let client = client.await.unwrap().unwrap();

        assert!(client.is_connected());
        assert!(server.is_connected());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unix_send_recv() {
        let socket_path = temp_socket_path("send_recv");
        let config = UnixConfig::default();

        let listener = UnixSocketListener::bind(&socket_path, config.clone())
            .await
            .unwrap();

        // Connect a client
        let socket_path_clone = socket_path.clone();
        let client_task =
            tokio::spawn(
                async move { UnixSocketTransport::connect(&socket_path_clone, config).await },
            );

        // Accept the connection
        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        // Send from client to server
        let test_data = b"Hello, Unix!";
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
    async fn test_unix_stats() {
        let socket_path = temp_socket_path("stats");
        let config = UnixConfig::default();

        let listener = UnixSocketListener::bind(&socket_path, config.clone())
            .await
            .unwrap();

        let socket_path_clone = socket_path.clone();
        let client_task =
            tokio::spawn(
                async move { UnixSocketTransport::connect(&socket_path_clone, config).await },
            );

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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unix_large_message() {
        let socket_path = temp_socket_path("large");
        let config = UnixConfig::default();

        let listener = UnixSocketListener::bind(&socket_path, config.clone())
            .await
            .unwrap();

        let socket_path_clone = socket_path.clone();
        let client_task =
            tokio::spawn(
                async move { UnixSocketTransport::connect(&socket_path_clone, config).await },
            );

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        // Send a large message (1 MB)
        let large_data = vec![0xAB; 1024 * 1024];

        let server_task = tokio::spawn({
            let server = server;
            let expected = large_data.clone();
            async move {
                let received = server.recv().await.unwrap();
                assert_eq!(received.len(), expected.len());
                assert_eq!(received.as_ref(), expected.as_slice());
            }
        });

        client.send(&large_data).await.unwrap();

        server_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unix_message_too_large() {
        let socket_path = temp_socket_path("too_large");
        let config = UnixConfig::default().with_max_message_size(1024);

        let listener = UnixSocketListener::bind(&socket_path, config.clone())
            .await
            .unwrap();

        let socket_path_clone = socket_path.clone();
        let client_task =
            tokio::spawn(
                async move { UnixSocketTransport::connect(&socket_path_clone, config).await },
            );

        let _server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        // Try to send a message larger than the limit
        let large_data = vec![0; 2048];
        let result = client.send(&large_data).await;

        assert!(matches!(
            result,
            Err(TransportError::MessageTooLarge { .. })
        ));
    }
}
