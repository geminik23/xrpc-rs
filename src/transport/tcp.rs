use crate::error::{TransportError, TransportResult};
use crate::transport::{FrameTransport, TransportStats};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use socket2::{SockRef, Socket};
use std::future::Future;
use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, watch};

/// Configuration for TCP frame transport.
#[derive(Debug, Clone)]
pub struct TcpConfig {
    pub max_message_size: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub nodelay: bool,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            max_message_size: 16 * 1024 * 1024,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            nodelay: true,
        }
    }
}

impl TcpConfig {
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

    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = nodelay;
        self
    }
}

fn checked_outbound_len(size: usize, configured_max: usize) -> TransportResult<u32> {
    let max = configured_max.min(u32::MAX as usize);
    if size > max {
        return Err(TransportError::MessageTooLarge { size, max });
    }

    u32::try_from(size).map_err(|_| TransportError::MessageTooLarge { size, max })
}

/// TCP-based frame transport with length-prefixed framing (Layer 1).
#[derive(Debug)]
pub struct TcpFrameTransport {
    config: TcpConfig,
    read_half: Arc<Mutex<OwnedReadHalf>>,
    write_half: Arc<Mutex<OwnedWriteHalf>>,
    shutdown_socket: Arc<Socket>,
    peer_addr: SocketAddr,
    connected: Arc<AtomicBool>,
    peer_terminal: Arc<AtomicBool>,
    close_tx: watch::Sender<bool>,
    send_progress: AtomicUsize,
    recv_progress: AtomicUsize,
    stats: Arc<SyncMutex<TransportStats>>,
}

enum FrameDirection {
    Send,
    Receive,
}

struct FrameStatsGuard<'a> {
    stats: &'a SyncMutex<TransportStats>,
    direction: FrameDirection,
    completed: bool,
}

impl<'a> FrameStatsGuard<'a> {
    fn new(stats: &'a SyncMutex<TransportStats>, direction: FrameDirection) -> Self {
        Self {
            stats,
            direction,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for FrameStatsGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let mut stats = self.stats.lock();
        match self.direction {
            FrameDirection::Send => stats.send_errors += 1,
            FrameDirection::Receive => stats.recv_errors += 1,
        }
    }
}

struct FrameProgressGuard<'a> {
    transport: &'a TcpFrameTransport,
    progress: &'a AtomicUsize,
    completed: bool,
}

impl<'a> FrameProgressGuard<'a> {
    fn new(transport: &'a TcpFrameTransport, progress: &'a AtomicUsize) -> Self {
        Self {
            transport,
            progress,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for FrameProgressGuard<'_> {
    fn drop(&mut self) {
        if !self.completed && self.progress.load(Ordering::Relaxed) > 0 {
            self.transport.terminate_framing();
        }
    }
}

impl TcpFrameTransport {
    /// Create a new TCP frame transport by connecting to an address.
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

        if config.nodelay {
            stream.set_nodelay(true).map_err(|e| {
                TransportError::Protocol(format!("Failed to set TCP_NODELAY: {}", e))
            })?;
        }

        let peer_addr = stream
            .peer_addr()
            .map_err(|e| TransportError::Protocol(format!("Failed to get peer address: {}", e)))?;
        Self::from_prepared_stream(stream, peer_addr, config)
    }

    /// Create a new TCP frame transport from an existing stream.
    pub fn from_stream(stream: TcpStream, config: TcpConfig) -> TransportResult<Self> {
        let peer_addr = stream
            .peer_addr()
            .map_err(|e| TransportError::Protocol(format!("Failed to get peer address: {}", e)))?;

        if config.nodelay {
            stream.set_nodelay(true).map_err(|e| {
                TransportError::Protocol(format!("Failed to set TCP_NODELAY: {}", e))
            })?;
        }

        Self::from_prepared_stream(stream, peer_addr, config)
    }

    fn from_prepared_stream(
        stream: TcpStream,
        peer_addr: SocketAddr,
        config: TcpConfig,
    ) -> TransportResult<Self> {
        let shutdown_socket = SockRef::from(&stream).try_clone().map_err(|error| {
            TransportError::Protocol(format!("Failed to clone TCP socket: {error}"))
        })?;
        let (read_half, write_half) = stream.into_split();
        let (close_tx, _) = watch::channel(false);

        Ok(Self {
            config,
            read_half: Arc::new(Mutex::new(read_half)),
            write_half: Arc::new(Mutex::new(write_half)),
            shutdown_socket: Arc::new(shutdown_socket),
            peer_addr,
            connected: Arc::new(AtomicBool::new(true)),
            peer_terminal: Arc::new(AtomicBool::new(false)),
            close_tx,
            send_progress: AtomicUsize::new(0),
            recv_progress: AtomicUsize::new(0),
            stats: Arc::new(SyncMutex::new(TransportStats::default())),
        })
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    fn cancel(&self) {
        self.close_tx.send_replace(true);
        self.connected.store(false, Ordering::Release);
    }

    fn mark_peer_terminal(&self) {
        self.peer_terminal.store(true, Ordering::Release);
        self.connected.store(false, Ordering::Release);
    }

    fn terminate_framing(&self) {
        self.mark_peer_terminal();
        let _ = self.shutdown_socket.shutdown(Shutdown::Both);
    }

    fn is_peer_terminal_error(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::NotConnected
        )
    }

    fn send_error(&self, error: std::io::Error) -> TransportError {
        if Self::is_peer_terminal_error(&error) {
            self.mark_peer_terminal();
            TransportError::ConnectionClosed
        } else {
            self.connected.store(false, Ordering::Release);
            TransportError::SendFailed {
                attempts: 1,
                reason: error.to_string(),
            }
        }
    }

    fn partial_frame_error(
        &self,
        part: &'static str,
        received: usize,
        expected: usize,
        error: Option<&std::io::Error>,
    ) -> TransportError {
        self.mark_peer_terminal();
        let reason = match error {
            Some(error) => format!(
                "TCP peer closed with a partial frame {}: received {} of {} bytes: {}",
                part, received, expected, error
            ),
            None => format!(
                "TCP peer closed with a partial frame {}: received {} of {} bytes",
                part, received, expected
            ),
        };
        TransportError::Protocol(reason)
    }

    fn check_connection(&self) -> TransportResult<()> {
        if *self.close_tx.borrow() || self.peer_terminal.load(Ordering::Acquire) {
            Err(TransportError::ConnectionClosed)
        } else if !self.is_connected() {
            Err(TransportError::NotConnected)
        } else {
            Ok(())
        }
    }

    async fn run_until_closed<T>(
        &self,
        operation: impl Future<Output = TransportResult<T>>,
        timeout: Option<Duration>,
        timeout_operation: &'static str,
    ) -> TransportResult<T> {
        let mut close_rx = self.close_tx.subscribe();
        let close_notified = async {
            let already_closed = *close_rx.borrow_and_update();
            if !already_closed {
                let _ = close_rx.changed().await;
            }
        };
        let operation = async move {
            if let Some(timeout) = timeout {
                tokio::time::timeout(timeout, operation)
                    .await
                    .map_err(|_| TransportError::Timeout {
                        duration_ms: timeout.as_millis() as u64,
                        operation: timeout_operation.to_string(),
                    })?
            } else {
                operation.await
            }
        };

        tokio::select! {
            biased;
            _ = close_notified => Err(TransportError::ConnectionClosed),
            result = operation => result,
        }
    }

    async fn send_bytes(&self, data: &[u8]) -> TransportResult<()> {
        let mut stats_guard = FrameStatsGuard::new(&self.stats, FrameDirection::Send);
        self.check_connection()?;

        let len_bytes =
            checked_outbound_len(data.len(), self.config.max_message_size)?.to_le_bytes();

        let write_op = async {
            let mut writer = self.write_half.lock().await;
            self.send_progress.store(0, Ordering::Relaxed);
            let mut progress_guard = FrameProgressGuard::new(self, &self.send_progress);
            let result = async {
                for chunk in [&len_bytes[..], data] {
                    let mut written = 0;
                    while written < chunk.len() {
                        let count = writer
                            .write(&chunk[written..])
                            .await
                            .map_err(|error| self.send_error(error))?;
                        if count == 0 {
                            self.terminate_framing();
                            return Err(TransportError::ConnectionClosed);
                        }
                        written += count;
                        self.send_progress.fetch_add(count, Ordering::Relaxed);
                    }
                }

                Ok::<(), TransportError>(())
            }
            .await;
            progress_guard.complete();
            result
        };

        self.run_until_closed(write_op, self.config.write_timeout, "TCP write")
            .await?;

        {
            let mut stats = self.stats.lock();
            stats.messages_sent += 1;
            stats.bytes_sent += data.len() as u64 + 4;
        }
        stats_guard.complete();

        Ok(())
    }

    async fn recv_bytes(&self) -> TransportResult<Bytes> {
        let mut stats_guard = FrameStatsGuard::new(&self.stats, FrameDirection::Receive);
        self.check_connection()?;

        let mut len_bytes = [0u8; 4];
        let mut buffer = Vec::new();

        let read_frame_op = async {
            let mut reader = self.read_half.lock().await;
            self.recv_progress.store(0, Ordering::Relaxed);
            let mut progress_guard = FrameProgressGuard::new(self, &self.recv_progress);
            let result = async {
                let mut header_received = 0;

                while header_received < len_bytes.len() {
                    match reader.read(&mut len_bytes[header_received..]).await {
                        Ok(0) if header_received == 0 => {
                            self.mark_peer_terminal();
                            return Err(TransportError::ConnectionClosed);
                        }
                        Ok(0) => {
                            return Err(self.partial_frame_error(
                                "length",
                                header_received,
                                len_bytes.len(),
                                None,
                            ));
                        }
                        Ok(count) => {
                            header_received += count;
                            self.recv_progress.fetch_add(count, Ordering::Relaxed);
                        }
                        Err(error)
                            if Self::is_peer_terminal_error(&error) && header_received == 0 =>
                        {
                            self.mark_peer_terminal();
                            return Err(TransportError::ConnectionClosed);
                        }
                        Err(error) if Self::is_peer_terminal_error(&error) => {
                            return Err(self.partial_frame_error(
                                "length",
                                header_received,
                                len_bytes.len(),
                                Some(&error),
                            ));
                        }
                        Err(error) => {
                            self.connected.store(false, Ordering::Release);
                            return Err(TransportError::ReceiveFailed {
                                attempts: 1,
                                reason: error.to_string(),
                            });
                        }
                    }
                }

                let len = u32::from_le_bytes(len_bytes) as usize;
                if len > self.config.max_message_size {
                    self.terminate_framing();
                    return Err(TransportError::MessageTooLarge {
                        size: len,
                        max: self.config.max_message_size,
                    });
                }
                buffer.resize(len, 0);
                let mut payload_received = 0;

                while payload_received < buffer.len() {
                    match reader.read(&mut buffer[payload_received..]).await {
                        Ok(0) => {
                            return Err(self.partial_frame_error(
                                "payload",
                                payload_received,
                                buffer.len(),
                                None,
                            ));
                        }
                        Ok(count) => {
                            payload_received += count;
                            self.recv_progress.fetch_add(count, Ordering::Relaxed);
                        }
                        Err(error) if Self::is_peer_terminal_error(&error) => {
                            return Err(self.partial_frame_error(
                                "payload",
                                payload_received,
                                buffer.len(),
                                Some(&error),
                            ));
                        }
                        Err(error) => {
                            self.connected.store(false, Ordering::Release);
                            return Err(TransportError::ReceiveFailed {
                                attempts: 1,
                                reason: error.to_string(),
                            });
                        }
                    }
                }

                Ok::<(), TransportError>(())
            }
            .await;
            progress_guard.complete();
            result
        };

        self.run_until_closed(read_frame_op, self.config.read_timeout, "TCP read frame")
            .await?;

        {
            let mut stats = self.stats.lock();
            stats.messages_received += 1;
            stats.bytes_received += buffer.len() as u64 + 4;
        }
        stats_guard.complete();

        Ok(Bytes::from(buffer))
    }
}

#[async_trait]
impl FrameTransport for TcpFrameTransport {
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
        self.cancel();

        match self.shutdown_socket.shutdown(Shutdown::Both) {
            Ok(()) => Ok(()),
            Err(error) if Self::is_peer_terminal_error(&error) => Ok(()),
            Err(error) => Err(TransportError::Protocol(format!(
                "Failed to shut down TCP stream: {}",
                error
            ))),
        }
    }

    fn stats(&self) -> Option<TransportStats> {
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        "tcp"
    }
}

impl Drop for TcpFrameTransport {
    fn drop(&mut self) {
        self.cancel();
        let _ = self.shutdown_socket.shutdown(Shutdown::Both);
    }
}

/// TCP listener for accepting incoming connections (Layer 1).
pub struct TcpFrameTransportListener {
    listener: TcpListener,
    config: TcpConfig,
}

impl TcpFrameTransportListener {
    /// Bind to a socket address and listen for incoming connections.
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

    pub fn local_addr(&self) -> TransportResult<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| TransportError::Protocol(format!("Failed to get local address: {}", e)))
    }

    /// Accept an incoming connection.
    pub async fn accept(&self) -> TransportResult<TcpFrameTransport> {
        let (stream, _addr) =
            self.listener
                .accept()
                .await
                .map_err(|e| TransportError::ConnectionFailed {
                    name: "tcp_listener".to_string(),
                    attempts: 1,
                    reason: format!("Failed to accept connection: {}", e),
                })?;

        TcpFrameTransport::from_stream(stream, self.config.clone())
    }
}

// Deprecated aliases for backward compatibility
#[deprecated(since = "0.2.0", note = "Use TcpFrameTransport instead")]
pub type TcpTransport = TcpFrameTransport;

#[deprecated(since = "0.2.0", note = "Use TcpFrameTransportListener instead")]
pub type TcpTransportListener = TcpFrameTransportListener;

#[cfg(test)]
mod tests {
    use super::*;

    async fn tcp_stream_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, server) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (client.unwrap(), server.unwrap().0)
    }

    async fn wait_for_frame_progress(progress: &AtomicUsize, minimum: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while progress.load(Ordering::Relaxed) < minimum {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("frame operation did not make byte progress");
    }

    #[test]
    fn test_tcp_outbound_length_limit() {
        assert_eq!(checked_outbound_len(1024, 2048).unwrap(), 1024);
        assert!(matches!(
            checked_outbound_len(2049, 2048),
            Err(TransportError::MessageTooLarge {
                size: 2049,
                max: 2048
            })
        ));

        #[cfg(target_pointer_width = "64")]
        {
            let oversized = u32::MAX as usize + 1;
            assert_eq!(
                checked_outbound_len(u32::MAX as usize, usize::MAX).unwrap(),
                u32::MAX
            );
            assert!(matches!(
                checked_outbound_len(oversized, usize::MAX),
                Err(TransportError::MessageTooLarge { size, max })
                    if size == oversized && max == u32::MAX as usize
            ));
        }
    }

    #[tokio::test]
    async fn test_tcp_transport_connection() {
        let config = TcpConfig::default();
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client.await.unwrap().unwrap();

        assert!(client.is_connected());
        assert!(server.is_connected());
    }

    #[tokio::test]
    async fn test_tcp_send_recv() {
        let config = TcpConfig::default();
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let test_data = b"Hello, TCP!";
        client.send_frame(test_data).await.unwrap();

        let received = server.recv_frame().await.unwrap();
        assert_eq!(received.as_ref(), test_data);

        let response_data = b"Hello back!";
        server.send_frame(response_data).await.unwrap();

        let received = client.recv_frame().await.unwrap();
        assert_eq!(received.as_ref(), response_data);
    }

    #[tokio::test]
    async fn test_tcp_send_progresses_while_receive_is_idle() {
        let config = TcpConfig::default().with_read_timeout(None);
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();
        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });
        let server = Arc::new(listener.accept().await.unwrap());
        let client = client_task.await.unwrap().unwrap();

        let recv_server = Arc::clone(&server);
        let pending_receive = tokio::spawn(async move { recv_server.recv_frame().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.read_half.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("receive did not acquire the read half");

        tokio::time::timeout(Duration::from_secs(1), async {
            let (send_result, receive_result) =
                tokio::join!(server.send_frame(b"response"), client.recv_frame());
            send_result.unwrap();
            assert_eq!(receive_result.unwrap().as_ref(), b"response");
        })
        .await
        .expect("send was blocked by an idle receive");

        client.send_frame(b"request").await.unwrap();
        assert_eq!(pending_receive.await.unwrap().unwrap().as_ref(), b"request");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tcp_stats() {
        let config = TcpConfig::default();
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let test_data = b"Test message";
        client.send_frame(test_data).await.unwrap();
        server.recv_frame().await.unwrap();

        let client_stats = client.stats().unwrap();
        assert_eq!(client_stats.messages_sent, 1);
        assert!(client_stats.bytes_sent > test_data.len() as u64);
        assert_eq!(client_stats.send_errors, 0);

        let server_stats = server.stats().unwrap();
        assert_eq!(server_stats.messages_received, 1);
        assert!(server_stats.bytes_received > test_data.len() as u64);
        assert_eq!(server_stats.recv_errors, 0);
    }

    #[tokio::test]
    async fn test_tcp_large_message() {
        let config = TcpConfig::default();
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        let large_data = vec![0xAB; 1024 * 1024];
        let (send_result, receive_result) =
            tokio::join!(client.send_frame(&large_data), server.recv_frame());
        send_result.unwrap();
        let received = receive_result.unwrap();
        assert_eq!(received.len(), large_data.len());
        assert_eq!(received.as_ref(), large_data.as_slice());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tcp_aborted_partial_send_is_terminal() {
        let (stream, mut peer) = tcp_stream_pair().await;
        SockRef::from(&stream).set_send_buffer_size(4096).unwrap();
        SockRef::from(&peer).set_recv_buffer_size(4096).unwrap();

        let payload_size = 8 * 1024 * 1024;
        let config = TcpConfig::default()
            .with_max_message_size(payload_size)
            .with_read_timeout(None)
            .with_write_timeout(None);
        let transport = Arc::new(TcpFrameTransport::from_stream(stream, config).unwrap());
        let send_transport = Arc::clone(&transport);
        let send_task = tokio::spawn(async move {
            let payload = vec![0xAB; payload_size];
            send_transport.send_frame(&payload).await
        });

        let mut prefix = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut prefix))
            .await
            .expect("partial frame was not sent")
            .unwrap();
        assert_eq!(
            u32::from_le_bytes(prefix[..4].try_into().unwrap()) as usize,
            payload_size
        );
        assert_eq!(prefix[4], 0xAB);
        wait_for_frame_progress(&transport.send_progress, prefix.len()).await;
        assert!(!send_task.is_finished(), "send completed before abort");

        send_task.abort();
        assert!(send_task.await.unwrap_err().is_cancelled());

        assert!(!transport.is_connected());
        let stats = transport.stats().unwrap();
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.send_errors, 1);
        assert_eq!(stats.recv_errors, 0);

        assert!(matches!(
            transport.send_frame(b"after abort").await,
            Err(TransportError::ConnectionClosed)
        ));
        assert_eq!(transport.stats().unwrap().send_errors, 2);
        assert!(matches!(
            transport.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
        assert_eq!(transport.stats().unwrap().recv_errors, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tcp_aborted_partial_receive_is_terminal() {
        let (stream, mut peer) = tcp_stream_pair().await;
        let config = TcpConfig::default()
            .with_read_timeout(None)
            .with_write_timeout(None);
        let transport = Arc::new(TcpFrameTransport::from_stream(stream, config).unwrap());
        let recv_transport = Arc::clone(&transport);
        let recv_task = tokio::spawn(async move { recv_transport.recv_frame().await });

        peer.write_all(&1024u32.to_le_bytes()).await.unwrap();
        peer.write_all(&[0xAB]).await.unwrap();
        wait_for_frame_progress(&transport.recv_progress, 5).await;
        assert!(!recv_task.is_finished(), "receive completed before abort");

        recv_task.abort();
        assert!(recv_task.await.unwrap_err().is_cancelled());

        assert!(!transport.is_connected());
        let stats = transport.stats().unwrap();
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.bytes_received, 0);
        assert_eq!(stats.recv_errors, 1);
        assert_eq!(stats.send_errors, 0);

        assert!(matches!(
            transport.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
        assert_eq!(transport.stats().unwrap().recv_errors, 2);
        assert!(matches!(
            transport.send_frame(b"after abort").await,
            Err(TransportError::ConnectionClosed)
        ));
        assert_eq!(transport.stats().unwrap().send_errors, 1);
    }

    #[tokio::test]
    async fn test_tcp_local_close_interrupts_receive() {
        let config = TcpConfig::default().with_read_timeout(None);
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = Arc::new(listener.accept().await.unwrap());
        let client = client_task.await.unwrap().unwrap();
        let recv_server = Arc::clone(&server);
        let recv_task = tokio::spawn(async move { recv_server.recv_frame().await });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if server.read_half.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("receive did not acquire the read mutex");

        server.close().await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), recv_task)
            .await
            .expect("receive was not interrupted")
            .unwrap();
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
        assert!(!server.is_connected());
        assert!(server.read_half.try_lock().is_ok());
        assert!(matches!(
            server.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
        drop(client);
    }

    #[tokio::test]
    async fn test_tcp_peer_close_interrupts_receive() {
        let config = TcpConfig::default().with_read_timeout(None);
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = Arc::new(listener.accept().await.unwrap());
        let client = client_task.await.unwrap().unwrap();
        let recv_server = Arc::clone(&server);
        let recv_task = tokio::spawn(async move { recv_server.recv_frame().await });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if server.read_half.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("receive did not acquire the read mutex");

        client.close().await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), recv_task)
            .await
            .expect("peer close did not interrupt receive")
            .unwrap();
        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
        assert!(!server.is_connected());
        assert!(matches!(
            server.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
        assert!(matches!(
            server.send_frame(b"after peer close").await,
            Err(TransportError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_tcp_partial_frame_eof_is_protocol_error() {
        let config = TcpConfig::default().with_read_timeout(None);
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        {
            let mut writer = client.write_half.lock().await;
            writer.write_all(&8u32.to_le_bytes()).await.unwrap();
            writer.write_all(b"abc").await.unwrap();
        }
        client.close().await.unwrap();

        let result = server.recv_frame().await;
        assert!(matches!(
            result,
            Err(TransportError::Protocol(reason))
                if reason.contains("partial frame payload") && reason.contains("3 of 8")
        ));
        assert!(!server.is_connected());
        assert!(matches!(
            server.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_tcp_partial_frame_timeout_is_terminal() {
        let config = TcpConfig::default().with_read_timeout(Some(Duration::from_millis(50)));
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();
        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });
        let server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        {
            let mut writer = client.write_half.lock().await;
            writer.write_all(&8u32.to_le_bytes()).await.unwrap();
            writer.write_all(b"abc").await.unwrap();
        }

        assert!(matches!(
            server.recv_frame().await,
            Err(TransportError::Timeout { .. })
        ));
        assert_eq!(server.stats().unwrap().recv_errors, 1);
        assert!(!server.is_connected());
        assert!(matches!(
            server.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
        assert_eq!(server.stats().unwrap().recv_errors, 2);
    }

    #[tokio::test]
    async fn test_tcp_close_terminates_both_directions() {
        let config = TcpConfig::default();
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let _server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        client.close().await.unwrap();

        let mut reader = client.read_half.lock().await;
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), reader.read(&mut byte))
            .await
            .expect("shutdown read did not complete")
            .unwrap();
        assert_eq!(read, 0);
        drop(reader);

        let mut writer = client.write_half.lock().await;
        let error = writer.write_all(b"after shutdown").await.unwrap_err();
        assert!(TcpFrameTransport::is_peer_terminal_error(&error));
    }

    #[tokio::test]
    async fn test_tcp_close_is_idempotent() {
        let config = TcpConfig::default();
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client_task =
            tokio::spawn(async move { TcpFrameTransport::connect(addr, config).await });

        let _server = listener.accept().await.unwrap();
        let client = client_task.await.unwrap().unwrap();

        client.close().await.unwrap();
        client.close().await.unwrap();

        assert!(!client.is_connected());
        assert!(matches!(
            client.send_frame(b"after close").await,
            Err(TransportError::ConnectionClosed)
        ));
        assert!(matches!(
            client.recv_frame().await,
            Err(TransportError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_tcp_message_too_large() {
        let config = TcpConfig::default().with_max_message_size(1024);
        let listener =
            TcpFrameTransportListener::bind("127.0.0.1:0".parse().unwrap(), config.clone())
                .await
                .unwrap();

        let addr = listener.local_addr().unwrap();
        let client = TcpFrameTransport::connect(addr, config).await.unwrap();

        let large_data = vec![0; 2048];
        let result = client.send_frame(&large_data).await;

        assert!(matches!(
            result,
            Err(TransportError::MessageTooLarge {
                size: 2048,
                max: 1024
            })
        ));
        let stats = client.stats().unwrap();
        assert_eq!(stats.send_errors, 1);
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.bytes_sent, 0);
    }
}
