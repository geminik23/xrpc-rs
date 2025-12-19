use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use raw_sync::Timeout;
use raw_sync::events::{Event, EventImpl, EventInit, EventState};
use shared_memory::{Shmem, ShmemConf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::{TransportError, TransportResult};
use crate::transport::utils::spawn_weak_loop;
use crate::transport::{Transport, TransportStats};

const SHM_MAGIC: u64 = 0x58525043; // XRPC IN ASCII
const MIN_BUFFER_SIZE: usize = 4096;
const MAX_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

#[repr(C)]
struct SharedMemoryControlBlock {
    magic: AtomicU64,
    write_pos: AtomicU64,
    read_pos: AtomicU64,
    capacity: AtomicU64,
    connected: AtomicBool,
    error_count: AtomicU64,
    last_heartbeat: AtomicU64,
}

impl SharedMemoryControlBlock {
    fn new(capacity: usize) -> Self {
        Self {
            magic: AtomicU64::new(SHM_MAGIC),
            write_pos: AtomicU64::new(0),
            read_pos: AtomicU64::new(0),
            capacity: AtomicU64::new(capacity as u64),
            connected: AtomicBool::new(true),
            error_count: AtomicU64::new(0),
            last_heartbeat: AtomicU64::new(0),
        }
    }

    fn is_valid(&self) -> bool {
        self.magic.load(Ordering::Acquire) == SHM_MAGIC
    }

    fn available_write(&self) -> usize {
        let w = self.write_pos.load(Ordering::Acquire) as usize;
        let r = self.read_pos.load(Ordering::Acquire) as usize;
        let cap = self.capacity.load(Ordering::Acquire) as usize;
        let w_norm = w % cap;
        let r_norm = r % cap;
        if w_norm >= r_norm {
            cap - (w_norm - r_norm) - 1
        } else {
            (r_norm - w_norm) - 1
        }
    }

    fn available_read(&self) -> usize {
        let w = self.write_pos.load(Ordering::Acquire) as usize;
        let r = self.read_pos.load(Ordering::Acquire) as usize;
        let cap = self.capacity.load(Ordering::Acquire) as usize;
        let w_norm = w % cap;
        let r_norm = r % cap;
        if w_norm >= r_norm {
            w_norm - r_norm
        } else {
            cap - (r_norm - w_norm)
        }
    }

    fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    fn update_heartbeat(&self) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_heartbeat.store(timestamp, Ordering::Release);
    }

    fn tick_heartbeat(&self) {
        // Just update the timestamp without signaling
        self.update_heartbeat();
    }

    fn is_healthy(&self, timeout_secs: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let last = self.last_heartbeat.load(Ordering::Acquire);

        if last == 0 {
            return true; // Not yet initialized
        }

        now - last < timeout_secs
    }
}

struct SharedMemoryRingBuffer {
    #[allow(dead_code)]
    shmem: Shmem,
    control: *mut SharedMemoryControlBlock,
    data: *mut u8,
    capacity: usize,
    is_owner: bool,
    #[allow(dead_code)]
    name: String,
    // raw-sync events for true IPC signaling
    // write_signal: signaled when space is available (writer waits on this)
    write_signal: Box<dyn EventImpl>,
    // read_signal: signaled when data is available (reader waits on this)
    read_signal: Box<dyn EventImpl>,
}

impl std::fmt::Debug for SharedMemoryRingBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedMemoryRingBuffer")
            .field("capacity", &self.capacity)
            .field("is_owner", &self.is_owner)
            .finish()
    }
}

unsafe impl Send for SharedMemoryRingBuffer {}
unsafe impl Sync for SharedMemoryRingBuffer {}

impl SharedMemoryRingBuffer {
    fn create(name: &str, capacity: usize) -> TransportResult<Self> {
        if !(MIN_BUFFER_SIZE..=MAX_BUFFER_SIZE).contains(&capacity) {
            return Err(TransportError::InvalidBufferState(format!(
                "Invalid buffer size: {}",
                capacity
            )));
        }

        // Calculate total size: ControlBlock + 2 * Event + Buffer
        let control_size = std::mem::size_of::<SharedMemoryControlBlock>();
        let event_size = Event::size_of(None);
        let total_size = control_size + (event_size * 2) + capacity;

        let shmem = ShmemConf::new()
            .size(total_size)
            .os_id(name)
            .create()
            .map_err(|e| TransportError::SharedMemoryCreation {
                name: name.to_string(),
                reason: e.to_string(),
            })?;

        let base_ptr = shmem.as_ptr();

        // Setup pointers
        let control = base_ptr as *mut SharedMemoryControlBlock;
        let write_signal_ptr = unsafe { base_ptr.add(control_size) };
        let read_signal_ptr = unsafe { base_ptr.add(control_size + event_size) };
        let data = unsafe { base_ptr.add(control_size + (event_size * 2)) };

        // Initialize structures
        unsafe {
            std::ptr::write(control, SharedMemoryControlBlock::new(capacity));

            // Initialize events (auto-reset = true)
            let (write_signal, _) = Event::new(write_signal_ptr, true).map_err(|e| {
                TransportError::SharedMemoryCreation {
                    name: name.to_string(),
                    reason: format!("Failed to create write event: {}", e),
                }
            })?;

            let (read_signal, _) = Event::new(read_signal_ptr, true).map_err(|e| {
                TransportError::SharedMemoryCreation {
                    name: name.to_string(),
                    reason: format!("Failed to create read event: {}", e),
                }
            })?;

            // Initially set write signal because buffer is empty (writable)
            write_signal.set(EventState::Signaled).map_err(|e| {
                TransportError::SharedMemoryCreation {
                    name: name.to_string(),
                    reason: format!("Failed to set write event: {}", e),
                }
            })?;

            Ok(Self {
                shmem,
                control,
                data,
                capacity,
                is_owner: true,
                name: name.to_string(),
                write_signal,
                read_signal,
            })
        }
    }

    fn connect(name: &str) -> TransportResult<Self> {
        let shmem =
            ShmemConf::new()
                .os_id(name)
                .open()
                .map_err(|e| TransportError::ConnectionFailed {
                    name: name.to_string(),
                    attempts: 1,
                    reason: e.to_string(),
                })?;

        let base_ptr = shmem.as_ptr();
        let control = base_ptr as *mut SharedMemoryControlBlock;

        let ctrl = unsafe { &*control };
        if !ctrl.is_valid() {
            return Err(TransportError::InvalidBufferState(
                "Invalid shared memory region".to_string(),
            ));
        }

        let capacity = ctrl.capacity.load(Ordering::Acquire) as usize;
        let control_size = std::mem::size_of::<SharedMemoryControlBlock>();
        let event_size = Event::size_of(None);

        let write_signal_ptr = unsafe { base_ptr.add(control_size) };
        let read_signal_ptr = unsafe { base_ptr.add(control_size + event_size) };
        let data = unsafe { base_ptr.add(control_size + (event_size * 2)) };

        unsafe {
            let (write_signal, _) = Event::from_existing(write_signal_ptr).map_err(|e| {
                TransportError::ConnectionFailed {
                    name: name.to_string(),
                    attempts: 1,
                    reason: format!("Failed to open write event: {}", e),
                }
            })?;

            let (read_signal, _) = Event::from_existing(read_signal_ptr).map_err(|e| {
                TransportError::ConnectionFailed {
                    name: name.to_string(),
                    attempts: 1,
                    reason: format!("Failed to open read event: {}", e),
                }
            })?;

            Ok(Self {
                shmem,
                control,
                data,
                capacity,
                is_owner: false,
                name: name.to_string(),
                write_signal,
                read_signal,
            })
        }
    }

    fn write(&self, data: &[u8]) -> TransportResult<()> {
        let control = unsafe { &*self.control };

        if !control.is_valid() {
            control.record_error();
            return Err(TransportError::InvalidBufferState(
                "Bad control".to_string(),
            ));
        }

        let msg_len = data.len();
        let total_len = 4 + msg_len; // Length prefix (4 bytes) + Data

        if total_len > self.capacity {
            control.record_error();
            return Err(TransportError::MessageTooLarge {
                size: msg_len,
                max: self.capacity - 4,
            });
        }

        let start = Instant::now();
        let timeout = Duration::from_secs(5);

        // Wait for space
        loop {
            if control.available_write() >= total_len {
                break;
            }

            if start.elapsed() > timeout {
                control.record_error();
                return Err(TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "waiting for buffer space".into(),
                });
            }

            // Wait for signal from reader (that they consumed data)
            // Using wait_timeout to handle race conditions or lost signals
            let remaining = timeout.saturating_sub(start.elapsed());
            let _ = self.write_signal.wait(Timeout::Val(remaining));
        }

        let write_pos = control.write_pos.load(Ordering::Acquire) as usize;

        // Optimized write using memcpy (std::ptr::copy_nonoverlapping)

        // 1. Write Length (4 bytes)
        let len_bytes = (msg_len as u32).to_le_bytes();
        self.raw_write(write_pos, &len_bytes);

        // 2. Write Data
        self.raw_write(write_pos + 4, data);

        // Update position and heartbeat
        control
            .write_pos
            .store((write_pos + total_len) as u64, Ordering::Release);
        control.update_heartbeat();

        // Signal reader that data is available
        let _ = self.read_signal.set(EventState::Signaled);

        Ok(())
    }

    // Helper for circular buffer write
    fn raw_write(&self, offset: usize, src: &[u8]) {
        let offset = offset % self.capacity;
        let len = src.len();
        let first_chunk = std::cmp::min(len, self.capacity - offset);

        unsafe {
            // First part
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.data.add(offset), first_chunk);

            // Second part (wrap around) if needed
            if first_chunk < len {
                std::ptr::copy_nonoverlapping(
                    src.as_ptr().add(first_chunk),
                    self.data,
                    len - first_chunk,
                );
            }
        }
    }

    fn read(&self) -> TransportResult<Bytes> {
        let control = unsafe { &*self.control };

        if !control.is_valid() {
            control.record_error();
            return Err(TransportError::InvalidBufferState(
                "Bad control".to_string(),
            ));
        }

        let start = Instant::now();
        let timeout = Duration::from_secs(5);

        // Wait for data (at least 4 bytes for length)
        loop {
            if control.available_read() >= 4 {
                break;
            }

            if start.elapsed() > timeout {
                control.record_error();
                return Err(TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "waiting for data".into(),
                });
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            let _ = self.read_signal.wait(Timeout::Val(remaining));
        }

        let read_pos = control.read_pos.load(Ordering::Acquire) as usize;

        // Read length prefix
        let mut len_bytes = [0u8; 4];
        self.raw_read(read_pos, &mut len_bytes);
        let msg_len = u32::from_le_bytes(len_bytes) as usize;

        if msg_len > self.capacity {
            control.record_error();
            return Err(TransportError::InvalidBufferState(
                "Bad message length".to_string(),
            ));
        }

        // Wait for full message
        loop {
            if control.available_read() >= 4 + msg_len {
                break;
            }

            if start.elapsed() > timeout {
                control.record_error();
                return Err(TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "waiting for full message".into(),
                });
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            let _ = self.read_signal.wait(Timeout::Val(remaining));
        }

        // Read payload
        let mut buffer = vec![0u8; msg_len];
        self.raw_read(read_pos + 4, &mut buffer);

        // Update position and heartbeat
        control
            .read_pos
            .store((read_pos + 4 + msg_len) as u64, Ordering::Release);
        control.update_heartbeat();

        // Signal writer that space is available
        let _ = self.write_signal.set(EventState::Signaled);

        Ok(Bytes::from(buffer))
    }

    // Helper for circular buffer read
    fn raw_read(&self, offset: usize, dst: &mut [u8]) {
        let offset = offset % self.capacity;
        let len = dst.len();
        let first_chunk = std::cmp::min(len, self.capacity - offset);

        unsafe {
            // First part
            std::ptr::copy_nonoverlapping(self.data.add(offset), dst.as_mut_ptr(), first_chunk);

            // Second part (wrap around) if needed
            if first_chunk < len {
                std::ptr::copy_nonoverlapping(
                    self.data,
                    dst.as_mut_ptr().add(first_chunk),
                    len - first_chunk,
                );
            }
        }
    }

    fn tick_heartbeat(&self) {
        if self.is_owner {
            let control = unsafe { &*self.control };
            control.tick_heartbeat();
        }
    }
}

impl Drop for SharedMemoryRingBuffer {
    fn drop(&mut self) {
        if self.is_owner {
            let control = unsafe { &*self.control };
            control.connected.store(false, Ordering::Release);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RetryPolicy {
    Fixed {
        delay_ms: u64,
    },
    Linear {
        base_delay_ms: u64,
    },
    Exponential {
        base_delay_ms: u64,
        max_delay_ms: u64,
    },
}

impl RetryPolicy {
    pub fn delay(&self, attempt: usize) -> Duration {
        match self {
            RetryPolicy::Fixed { delay_ms } => Duration::from_millis(*delay_ms),
            RetryPolicy::Linear { base_delay_ms } => {
                Duration::from_millis(base_delay_ms * (attempt as u64 + 1))
            }
            RetryPolicy::Exponential {
                base_delay_ms,
                max_delay_ms,
            } => {
                let delay = base_delay_ms * 2u64.pow(attempt as u32);
                Duration::from_millis(delay.min(*max_delay_ms))
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy::Linear { base_delay_ms: 100 }
    }
}

#[derive(Clone, Debug)]
pub struct SharedMemoryConfig {
    pub buffer_size: usize,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub max_retry_attempts: usize,
    pub retry_policy: RetryPolicy,
    pub auto_reconnect: bool,
}

impl Default for SharedMemoryConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            read_timeout: Some(Duration::from_secs(5)),
            write_timeout: Some(Duration::from_secs(5)),
            max_retry_attempts: 3,
            retry_policy: RetryPolicy::default(),
            auto_reconnect: true,
        }
    }
}

impl SharedMemoryConfig {
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

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub fn with_max_retries(mut self, max: usize) -> Self {
        self.max_retry_attempts = max;
        self
    }

    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }
}

#[derive(Debug)]
pub struct SharedMemoryTransport {
    send_buffer: Arc<SharedMemoryRingBuffer>,
    recv_buffer: Arc<SharedMemoryRingBuffer>,
    config: SharedMemoryConfig,
    stats: Arc<Mutex<TransportStats>>,
    name: String,
    error_count: Arc<Mutex<usize>>,
    connected: Arc<AtomicBool>,
    reconnect_name: String,
}

impl SharedMemoryTransport {
    pub fn create_server(
        name: impl Into<String>,
        config: SharedMemoryConfig,
    ) -> TransportResult<Self> {
        let name = name.into();

        let send_name = format!("{}_s2c", name);
        let recv_name = format!("{}_c2s", name);

        let send_buffer = Arc::new(SharedMemoryRingBuffer::create(
            &send_name,
            config.buffer_size,
        )?);
        let recv_buffer = Arc::new(SharedMemoryRingBuffer::create(
            &recv_name,
            config.buffer_size,
        )?);

        // Spawn heartbeat task
        spawn_weak_loop(
            Arc::downgrade(&send_buffer),
            Duration::from_secs(5),
            |buffer| {
                buffer.tick_heartbeat();
            },
        );

        Ok(Self {
            send_buffer,
            recv_buffer,
            config,
            stats: Arc::new(Mutex::new(TransportStats::default())),
            name: format!("{}-server", name),
            error_count: Arc::new(Mutex::new(0)),
            connected: Arc::new(AtomicBool::new(true)),
            reconnect_name: name.clone(),
        })
    }

    pub fn connect_client_with_config(
        name: impl Into<String>,
        config: SharedMemoryConfig,
    ) -> TransportResult<Self> {
        let name = name.into();
        let send_name = format!("{}_c2s", name);
        let recv_name = format!("{}_s2c", name);
        let mut last_error = None;

        for attempt in 0..config.max_retry_attempts {
            if attempt > 0 {
                std::thread::sleep(config.retry_policy.delay(attempt - 1));
            }

            match (
                SharedMemoryRingBuffer::connect(&send_name),
                SharedMemoryRingBuffer::connect(&recv_name),
            ) {
                (Ok(send_buffer), Ok(recv_buffer)) => {
                    let send_arc = Arc::new(send_buffer);
                    let recv_arc = Arc::new(recv_buffer);

                    // Spawn heartbeat task
                    spawn_weak_loop(
                        Arc::downgrade(&send_arc),
                        Duration::from_secs(5),
                        |buffer| {
                            buffer.tick_heartbeat();
                        },
                    );

                    return Ok(Self {
                        send_buffer: send_arc,
                        recv_buffer: recv_arc,
                        config: config.clone(),
                        stats: Arc::new(Mutex::new(TransportStats::default())),
                        name: format!("{}-client", name),
                        error_count: Arc::new(Mutex::new(0)),
                        connected: Arc::new(AtomicBool::new(true)),
                        reconnect_name: name.clone(),
                    });
                }
                (Err(e), _) | (_, Err(e)) => {
                    last_error = Some(e);
                }
            }
        }

        Err(
            last_error.unwrap_or_else(|| TransportError::ConnectionFailed {
                name: name.clone(),
                attempts: config.max_retry_attempts,
                reason: "max retries exceeded".into(),
            }),
        )
    }

    pub fn connect_client(name: impl Into<String>) -> TransportResult<Self> {
        Self::connect_client_with_config(name, SharedMemoryConfig::default())
    }

    // Auto-reconnect if connection lost
    fn try_reconnect(&self) -> TransportResult<()> {
        if !self.config.auto_reconnect {
            return Err(TransportError::NotConnected);
        }

        let send_name = format!("{}_c2s", self.reconnect_name);
        let recv_name = format!("{}_s2c", self.reconnect_name);

        for attempt in 0..self.config.max_retry_attempts {
            if attempt > 0 {
                std::thread::sleep(self.config.retry_policy.delay(attempt - 1));
            }

            match (
                SharedMemoryRingBuffer::connect(&send_name),
                SharedMemoryRingBuffer::connect(&recv_name),
            ) {
                (Ok(_send), Ok(_recv)) => {
                    self.connected.store(true, Ordering::Release);
                    // Reset error count on successful reconnect
                    *self.error_count.lock() = 0;
                    return Ok(());
                }
                _ => continue,
            }
        }

        Err(TransportError::ConnectionFailed {
            name: self.reconnect_name.clone(),
            attempts: self.config.max_retry_attempts,
            reason: "reconnect failed".into(),
        })
    }

    pub fn is_healthy(&self) -> bool {
        let control = unsafe { &*self.recv_buffer.control };
        control.is_healthy(30) && *self.error_count.lock() < 10
    }

    pub fn stats(&self) -> TransportStats {
        self.stats.lock().clone()
    }
}

#[async_trait]
impl Transport for SharedMemoryTransport {
    fn is_healthy(&self) -> bool {
        self.is_healthy()
    }

    async fn send(&self, data: &[u8]) -> TransportResult<()> {
        // Try reconnect if disconnected
        if !self.connected.load(Ordering::Acquire) {
            self.try_reconnect()?;
        }

        let mut last_error = None;

        for attempt in 0..self.config.max_retry_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_policy.delay(attempt - 1)).await;
            }

            let result = tokio::task::spawn_blocking({
                let buffer = self.send_buffer.clone();
                let data = data.to_vec();
                move || buffer.write(&data)
            })
            .await;

            match result {
                Ok(Ok(())) => {
                    let mut stats = self.stats.lock();
                    stats.messages_sent += 1;
                    stats.bytes_sent += data.len() as u64;
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_error = Some(e);
                    *self.error_count.lock() += 1;
                }
                Err(e) => {
                    last_error = Some(TransportError::SendFailed {
                        attempts: attempt + 1,
                        reason: e.to_string(),
                    });
                    *self.error_count.lock() += 1;
                }
            }
        }

        self.connected.store(false, Ordering::Release);
        Err(last_error.unwrap_or_else(|| TransportError::SendFailed {
            attempts: self.config.max_retry_attempts,
            reason: "max retries exceeded".into(),
        }))
    }

    async fn recv(&self) -> TransportResult<Bytes> {
        // Try reconnect if disconnected
        if !self.connected.load(Ordering::Acquire) {
            self.try_reconnect()?;
        }

        let mut last_error = None;

        for attempt in 0..self.config.max_retry_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_policy.delay(attempt - 1)).await;
            }

            let result = tokio::task::spawn_blocking({
                let buffer = self.recv_buffer.clone();
                move || buffer.read()
            })
            .await;

            match result {
                Ok(Ok(bytes)) => {
                    let mut stats = self.stats.lock();
                    stats.messages_received += 1;
                    stats.bytes_received += bytes.len() as u64;
                    return Ok(bytes);
                }
                Ok(Err(e)) => {
                    last_error = Some(e);
                    *self.error_count.lock() += 1;
                }
                Err(e) => {
                    last_error = Some(TransportError::ReceiveFailed {
                        attempts: attempt + 1,
                        reason: e.to_string(),
                    });
                    *self.error_count.lock() += 1;
                }
            }
        }

        self.connected.store(false, Ordering::Release);
        Err(last_error.unwrap_or_else(|| TransportError::ReceiveFailed {
            attempts: self.config.max_retry_attempts,
            reason: "max retries exceeded".into(),
        }))
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    async fn close(&self) -> TransportResult<()> {
        let control = unsafe { &*self.send_buffer.control };
        control.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn stats(&self) -> Option<TransportStats> {
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cross_process_basic() {
        let config = SharedMemoryConfig::default();
        let server = SharedMemoryTransport::create_server("test-basic", config).unwrap();
        let client = SharedMemoryTransport::connect_client("test-basic").unwrap();

        client.send(b"Hello").await.unwrap();
        let msg = server.recv().await.unwrap();
        assert_eq!(msg.as_ref(), b"Hello");
    }

    #[tokio::test]
    async fn test_ring_buffer_wrapping() {
        let config = SharedMemoryConfig::default().with_buffer_size(8192);
        let server = SharedMemoryTransport::create_server("test-wrap", config).unwrap();
        let client = SharedMemoryTransport::connect_client("test-wrap").unwrap();

        let chunk_size = 1000;
        let num_chunks = 20;

        // Send 20KB total buffer is 8KB
        let send_handle = tokio::spawn(async move {
            for i in 0..num_chunks {
                let data = vec![i as u8; chunk_size];
                client.send(&data).await.unwrap();
            }
        });

        for i in 0..num_chunks {
            let msg = server.recv().await.unwrap();
            assert_eq!(msg.len(), chunk_size);
            assert!(msg.iter().all(|&b| b == i as u8), "chunk {} corrupted", i);
        }

        send_handle.await.unwrap();
    }
}
