use crate::error::{TransportError, TransportResult};
use crate::transport::{FrameTransport, TransportStats};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

/// Configuration for Unix frame transport.
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
            max_message_size: 16 * 1024 * 1024,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
        }
    }
}

impl UnixConfig {
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_read_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.read_timeout = timeout;
        self
    }

    pub fn with_write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.write_timeout = timeout;
        self
    }
}

/// Unix domain socket frame transport with length-prefixed framing (Layer 1).
#[derive(Debug)]
pub struct UnixFrameTransport {
    config: UnixConfig,
    stream: Arc<Mutex<UnixStream>>,
    path: PathBuf,
    connected: Arc<AtomicBool>,
    stats: Arc<SyncMutex<TransportStats>>,
}

impl UnixFrameTransport {
    /// Connect to a Unix socket at the given path.
    pub async fn connect(path: impl AsRef<Path>, config: UnixConfig) -> TransportResult<Self> {
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
            stats: Arc::new(SyncMutex::new(TransportStats::default())),
        })
    }

    /// Create from an existing stream.
    pub fn from_stream(stream: UnixStream, path: PathBuf, config: UnixConfig) -> Self {
        Self {
            config,
            stream: Arc::new(Mutex::new(stream)),
            path,
            connected: Arc::new(AtomicBool::new(true)),
            stats: Arc::new(SyncMutex::new(TransportStats::default())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

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

        if let Some(timeout) = self.config.write_timeout {
            tokio::time::timeout(timeout, write_op)
                .await
                .map_err(|_| TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "Unix write".to_string(),
                })??;
        } else {
            write_op.await?;
        }

        let mut stats = self.stats.lock();
        stats.messages_sent += 1;
        stats.bytes_sent += data.len() as u64 + 4;

        Ok(())
    }

    async fn recv_bytes(&self) -> TransportResult<Bytes> {
        if !self.is_connected() {
            return Err(TransportError::NotConnected);
        }

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
                    operation: "Unix read length".to_string(),
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
                    operation: "Unix read data".to_string(),
                })??;
        } else {
            read_data_op.await?;
        }

        let mut stats = self.stats.lock();
        stats.messages_received += 1;
        stats.bytes_received += len as u64 + 4;

        Ok(Bytes::from(buffer))
    }
}

#[async_trait]
impl FrameTransport for UnixFrameTransport {
    async fn send_frame(&self, data: &[u8]) -> TransportResult<()> {
        self.send_bytes(data).await
    }

    async fn recv_frame(&self) -> TransportResult<Bytes> {
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
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        "unix"
    }
}

/// Unix socket listener for accepting incoming connections (Layer 1).
pub struct UnixFrameTransportListener {
    listener: UnixListener,
    config: UnixConfig,
    path: PathBuf,
}

impl UnixFrameTransportListener {
    /// Bind to a Unix socket path.
    pub async fn bind(path: impl AsRef<Path>, config: UnixConfig) -> TransportResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Remove existing socket file if it exists
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| TransportError::ConnectionFailed {
                name: path.display().to_string(),
                attempts: 1,
                reason: format!("Failed to remove existing socket: {}", e),
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accept an incoming connection.
    pub async fn accept(&self) -> TransportResult<UnixFrameTransport> {
        let (stream, _addr) =
            self.listener
                .accept()
                .await
                .map_err(|e| TransportError::ConnectionFailed {
                    name: "unix_listener".to_string(),
                    attempts: 1,
                    reason: format!("Failed to accept connection: {}", e),
                })?;

        Ok(UnixFrameTransport::from_stream(
            stream,
            self.path.clone(),
            self.config.clone(),
        ))
    }
}

impl Drop for UnixFrameTransportListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

// Deprecated aliases for backward compatibility
#[deprecated(since = "0.2.0", note = "Use UnixFrameTransport instead")]
pub type UnixSocketTransport = UnixFrameTransport;

#[deprecated(since = "0.2.0", note = "Use UnixFrameTransportListener instead")]
pub type UnixSocketListener = UnixFrameTransportListener;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_socket_path() -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("xrpc_test_{}.sock", id))
    }

    #[tokio::test]
    async fn test_unix_transport_connection() {
        let path = temp_socket_path();
        let config = UnixConfig::default();

        let listener = UnixFrameTransportListener::bind(&path, config.clone())
            .await
            .unwrap();

        let path_clone = path.clone();
        let config_clone = config.clone();
        let client_task =
            tokio::spawn(
                async move { UnixFrameTransport::connect(&path_clone, config_clone).await },
            );

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        assert!(client.is_connected());
        assert!(server.is_connected());
    }

    #[tokio::test]
    async fn test_unix_send_recv() {
        let path = temp_socket_path();
        let config = UnixConfig::default();

        let listener = UnixFrameTransportListener::bind(&path, config.clone())
            .await
            .unwrap();

        let path_clone = path.clone();
        let config_clone = config.clone();
        let client_task =
            tokio::spawn(
                async move { UnixFrameTransport::connect(&path_clone, config_clone).await },
            );

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let test_data = b"Hello, Unix!";
        client.send_frame(test_data).await.unwrap();

        let received = server.recv_frame().await.unwrap();
        assert_eq!(received.as_ref(), test_data);

        let response_data = b"Hello back!";
        server.send_frame(response_data).await.unwrap();

        let received = client.recv_frame().await.unwrap();
        assert_eq!(received.as_ref(), response_data);
    }

    #[tokio::test]
    async fn test_unix_stats() {
        let path = temp_socket_path();
        let config = UnixConfig::default();

        let listener = UnixFrameTransportListener::bind(&path, config.clone())
            .await
            .unwrap();

        let path_clone = path.clone();
        let config_clone = config.clone();
        let client_task =
            tokio::spawn(
                async move { UnixFrameTransport::connect(&path_clone, config_clone).await },
            );

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let test_data = b"Test message";
        client.send_frame(test_data).await.unwrap();
        server.recv_frame().await.unwrap();

        let client_stats = client.stats().unwrap();
        assert_eq!(client_stats.messages_sent, 1);
        assert!(client_stats.bytes_sent > test_data.len() as u64);

        let server_stats = server.stats().unwrap();
        assert_eq!(server_stats.messages_received, 1);
        assert!(server_stats.bytes_received > test_data.len() as u64);
    }

    #[tokio::test]
    async fn test_unix_large_message() {
        let path = temp_socket_path();
        let config = UnixConfig::default();

        let listener = UnixFrameTransportListener::bind(&path, config.clone())
            .await
            .unwrap();

        let path_clone = path.clone();
        let config_clone = config.clone();
        let client_task =
            tokio::spawn(
                async move { UnixFrameTransport::connect(&path_clone, config_clone).await },
            );

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let large_data = vec![0xAB; 1024 * 1024];
        client.send_frame(&large_data).await.unwrap();

        let received = server.recv_frame().await.unwrap();
        assert_eq!(received.len(), large_data.len());
        assert_eq!(received.as_ref(), large_data.as_slice());
    }

    #[tokio::test]
    async fn test_unix_message_too_large() {
        let path = temp_socket_path();
        let config = UnixConfig::default().with_max_message_size(1024);

        let listener = UnixFrameTransportListener::bind(&path, config.clone())
            .await
            .unwrap();

        let path_clone = path.clone();
        let config_clone = config.clone();
        let client_task =
            tokio::spawn(
                async move { UnixFrameTransport::connect(&path_clone, config_clone).await },
            );

        let _server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let large_data = vec![0; 2048];
        let result = client.send_frame(&large_data).await;

        assert!(matches!(
            result,
            Err(TransportError::MessageTooLarge { .. })
        ));
    }
}
