use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use raw_sync::Timeout;
use raw_sync::events::{Event, EventImpl, EventInit, EventState};
use shared_memory::{Shmem, ShmemConf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;

use crate::error::{TransportError, TransportResult};
use crate::transport::utils::spawn_weak_loop;
use crate::transport::{FrameTransport, TransportStats};

const SHM_MAGIC: u64 = 0x5852_5043_5348_4D32;
const SHM_VERSION: u64 = 2;
const SHM_INITIALIZING: u64 = 0;
const SHM_READY: u64 = 1;
const ATTACHMENT_FREE: u64 = 0;
const ATTACHMENT_CLAIMED: u64 = 1;
const MIN_BUFFER_SIZE: usize = 4096;
const MAX_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const WRITE_COMMIT_CLOSE_TIMEOUT: Duration = Duration::from_millis(100);
const WRITE_COMMIT_CLOSE_POLL_INTERVAL: Duration = Duration::from_millis(1);
const OPERATION_PENDING: u8 = 0;
const OPERATION_CANCELLED: u8 = 1;
const OPERATION_COMMITTED: u8 = 2;
const WRITE_COMMIT_OPEN: u64 = 0;
const WRITE_COMMIT_ACTIVE: u64 = 1;
const WRITE_COMMIT_CLOSING: u64 = 2;
const WRITE_COMMIT_CLOSED: u64 = 3;

struct BlockingCancellation {
    state: Arc<AtomicU8>,
}

impl BlockingCancellation {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(OPERATION_PENDING)),
        }
    }

    fn state(&self) -> Arc<AtomicU8> {
        Arc::clone(&self.state)
    }

    fn is_cancelled(state: &AtomicU8) -> bool {
        state.load(Ordering::Acquire) == OPERATION_CANCELLED
    }

    fn commit(state: &AtomicU8) -> TransportResult<()> {
        match state.compare_exchange(
            OPERATION_PENDING,
            OPERATION_COMMITTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(OPERATION_CANCELLED) => Err(TransportError::ConnectionClosed),
            Err(_) => Err(TransportError::InvalidBufferState(
                "Blocking operation committed more than once".to_string(),
            )),
        }
    }
}

impl Drop for BlockingCancellation {
    fn drop(&mut self) {
        let _ = self.state.compare_exchange(
            OPERATION_PENDING,
            OPERATION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

#[cfg(test)]
struct ActiveBlockingOperation {
    counter: Arc<AtomicUsize>,
}

#[cfg(test)]
impl ActiveBlockingOperation {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

#[cfg(test)]
impl Drop for ActiveBlockingOperation {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
struct CommitTestHook {
    reached: std::sync::mpsc::SyncSender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(test)]
impl CommitTestHook {
    fn pause(&self) {
        self.reached
            .send(())
            .expect("commit test observer was dropped");
        self.resume
            .lock()
            .recv()
            .expect("commit test resume sender was dropped");
    }
}

#[derive(Clone, Copy)]
struct SharedMemoryLayout {
    control_size: usize,
    event_size: usize,
    read_signal_offset: usize,
    data_offset: usize,
    total_size: usize,
}

impl SharedMemoryLayout {
    fn checked(capacity: usize, control_size: usize, event_size: usize) -> Result<Self, String> {
        let read_signal_offset = control_size
            .checked_add(event_size)
            .ok_or_else(|| "write event offset overflow".to_string())?;
        let data_offset = read_signal_offset
            .checked_add(event_size)
            .ok_or_else(|| "read event offset overflow".to_string())?;
        let total_size = data_offset
            .checked_add(capacity)
            .ok_or_else(|| "shared memory size overflow".to_string())?;
        Ok(Self {
            control_size,
            event_size,
            read_signal_offset,
            data_offset,
            total_size,
        })
    }
}

#[repr(C)]
struct SharedMemoryControlBlock {
    magic: AtomicU64,
    version: AtomicU64,
    readiness: AtomicU64,
    control_size: AtomicU64,
    event_size: AtomicU64,
    total_size: AtomicU64,
    capacity: AtomicU64,
    attachment_state: AtomicU64,
    write_pos: AtomicU64,
    read_pos: AtomicU64,
    write_commit_state: AtomicU64,
    error_count: AtomicU64,
    last_heartbeat: AtomicU64,
}

impl SharedMemoryControlBlock {
    fn new(capacity: usize, layout: SharedMemoryLayout) -> Self {
        Self {
            magic: AtomicU64::new(SHM_MAGIC),
            version: AtomicU64::new(SHM_VERSION),
            readiness: AtomicU64::new(SHM_INITIALIZING),
            control_size: AtomicU64::new(layout.control_size as u64),
            event_size: AtomicU64::new(layout.event_size as u64),
            total_size: AtomicU64::new(layout.total_size as u64),
            capacity: AtomicU64::new(capacity as u64),
            attachment_state: AtomicU64::new(ATTACHMENT_FREE),
            write_pos: AtomicU64::new(0),
            read_pos: AtomicU64::new(0),
            write_commit_state: AtomicU64::new(WRITE_COMMIT_OPEN),
            error_count: AtomicU64::new(0),
            last_heartbeat: AtomicU64::new(0),
        }
    }

    fn is_valid(&self) -> bool {
        self.readiness.load(Ordering::Acquire) == SHM_READY
            && self.magic.load(Ordering::Acquire) == SHM_MAGIC
            && self.version.load(Ordering::Acquire) == SHM_VERSION
    }

    fn publish_ready(&self) {
        self.update_heartbeat();
        self.readiness.store(SHM_READY, Ordering::Release);
    }

    fn claim_attachment(&self, name: &str) -> TransportResult<()> {
        match self.attachment_state.compare_exchange(
            ATTACHMENT_FREE,
            ATTACHMENT_CLAIMED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(ATTACHMENT_CLAIMED) => Err(TransportError::ConnectionFailed {
                name: name.to_string(),
                attempts: 1,
                reason: "shared memory ring already has a client attachment".to_string(),
            }),
            Err(state) => Err(TransportError::InvalidBufferState(format!(
                "Unknown shared memory attachment state in '{}': {}",
                name, state
            ))),
        }
    }

    fn release_attachment(&self) {
        if self
            .attachment_state
            .compare_exchange(
                ATTACHMENT_CLAIMED,
                ATTACHMENT_FREE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.record_error();
        }
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

    fn is_connected(&self) -> bool {
        matches!(
            self.write_commit_state.load(Ordering::Acquire),
            WRITE_COMMIT_OPEN | WRITE_COMMIT_ACTIVE
        )
    }

    fn may_receive_more(&self) -> bool {
        self.write_commit_state.load(Ordering::Acquire) != WRITE_COMMIT_CLOSED
    }

    fn begin_write_commit(&self) -> TransportResult<SharedWriteCommit<'_>> {
        match self.write_commit_state.compare_exchange(
            WRITE_COMMIT_OPEN,
            WRITE_COMMIT_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(SharedWriteCommit { control: self }),
            Err(WRITE_COMMIT_CLOSING) | Err(WRITE_COMMIT_CLOSED) => {
                Err(TransportError::ConnectionClosed)
            }
            Err(WRITE_COMMIT_ACTIVE) => Err(TransportError::InvalidBufferState(
                "Concurrent shared memory write commit".to_string(),
            )),
            Err(state) => Err(TransportError::InvalidBufferState(format!(
                "Unknown shared memory write commit state: {}",
                state
            ))),
        }
    }

    fn close_write_commits(&self) -> TransportResult<()> {
        let started = Instant::now();
        loop {
            let state = self.write_commit_state.load(Ordering::Acquire);
            match state {
                WRITE_COMMIT_OPEN => {
                    if self
                        .write_commit_state
                        .compare_exchange(
                            WRITE_COMMIT_OPEN,
                            WRITE_COMMIT_CLOSED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                WRITE_COMMIT_ACTIVE => {
                    let _ = self.write_commit_state.compare_exchange(
                        WRITE_COMMIT_ACTIVE,
                        WRITE_COMMIT_CLOSING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
                WRITE_COMMIT_CLOSING => {
                    if started.elapsed() >= WRITE_COMMIT_CLOSE_TIMEOUT
                        && self
                            .write_commit_state
                            .compare_exchange(
                                WRITE_COMMIT_CLOSING,
                                WRITE_COMMIT_CLOSED,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                    {
                        self.record_error();
                        return Err(TransportError::InvalidBufferState(format!(
                            "Forced recovery of an abandoned shared memory write commit after {} ms",
                            WRITE_COMMIT_CLOSE_TIMEOUT.as_millis()
                        )));
                    }
                    std::thread::sleep(WRITE_COMMIT_CLOSE_POLL_INTERVAL);
                }
                WRITE_COMMIT_CLOSED => return Ok(()),
                _ => {
                    self.record_error();
                    self.write_commit_state
                        .store(WRITE_COMMIT_CLOSED, Ordering::Release);
                    return Err(TransportError::InvalidBufferState(format!(
                        "Forced recovery of unknown shared memory write commit state: {}",
                        state
                    )));
                }
            }
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

struct SharedMemoryAttachment<'a> {
    control: &'a SharedMemoryControlBlock,
    armed: bool,
}

impl SharedMemoryAttachment<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SharedMemoryAttachment<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.control.release_attachment();
        }
    }
}

struct SharedWriteCommit<'a> {
    control: &'a SharedMemoryControlBlock,
}

impl Drop for SharedWriteCommit<'_> {
    fn drop(&mut self) {
        loop {
            let state = self.control.write_commit_state.load(Ordering::Acquire);
            let next = match state {
                WRITE_COMMIT_ACTIVE => WRITE_COMMIT_OPEN,
                WRITE_COMMIT_CLOSING => WRITE_COMMIT_CLOSED,
                _ => {
                    self.control.record_error();
                    return;
                }
            };
            if self
                .control
                .write_commit_state
                .compare_exchange(state, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

#[derive(Debug)]
struct PreparedWrite {
    write_pos: usize,
    total_len: usize,
}

#[derive(Debug)]
struct PreparedRead {
    read_pos: usize,
    frame_len: usize,
    payload: Bytes,
}

struct SharedMemoryRingBuffer {
    #[allow(dead_code)]
    shmem: Shmem,
    control: *mut SharedMemoryControlBlock,
    data: *mut u8,
    capacity: usize,
    is_owner: bool,
    attachment_claimed: bool,
    #[allow(dead_code)]
    name: String,
    // raw-sync events for true IPC signaling
    // write_signal: signaled when space is available (writer waits on this)
    write_signal: Box<dyn EventImpl>,
    // read_signal: signaled when data is available (reader waits on this)
    read_signal: Box<dyn EventImpl>,
    #[cfg(test)]
    commit_test_hook: Mutex<Option<Arc<CommitTestHook>>>,
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
    fn is_event_wait_timeout(reason: &str, elapsed: Duration, requested: Duration) -> bool {
        if reason.contains("timed out") || reason.ends_with("0x102") {
            return true;
        }

        #[cfg(unix)]
        {
            // raw_sync 0.1.x collapses every non-zero pthread_cond_timedwait result
            // to this generic message. Treat it as a timeout only when the native
            // wait actually consumed the requested interval; immediate failures
            // (for example EINVAL) must remain visible as transport errors.
            let known_unix_timeout = reason == "Failed waiting for signal"
                || reason.ends_with(": 110")
                || reason.ends_with(": 60");
            // pthread_cond_timedwait and Instant can use different platform clocks;
            // allow a small scheduling/clock-boundary margin without accepting an
            // immediate generic failure as a timeout.
            let tolerance = Duration::from_millis(5).min(requested / 2);
            !requested.is_zero()
                && known_unix_timeout
                && elapsed.saturating_add(tolerance) >= requested
        }

        #[cfg(not(unix))]
        false
    }

    fn create(name: &str, capacity: usize) -> TransportResult<Self> {
        if !(MIN_BUFFER_SIZE..=MAX_BUFFER_SIZE).contains(&capacity) {
            return Err(TransportError::InvalidBufferState(format!(
                "Invalid buffer size: {}",
                capacity
            )));
        }

        let control_size = std::mem::size_of::<SharedMemoryControlBlock>();
        let event_size = Event::size_of(None);
        let layout = SharedMemoryLayout::checked(capacity, control_size, event_size)
            .map_err(TransportError::InvalidBufferState)?;
        let shmem = ShmemConf::new()
            .size(layout.total_size)
            .os_id(name)
            .create()
            .map_err(|e| TransportError::SharedMemoryCreation {
                name: name.to_string(),
                reason: e.to_string(),
            })?;

        if shmem.len() != layout.total_size {
            return Err(TransportError::SharedMemoryCreation {
                name: name.to_string(),
                reason: format!(
                    "Mapping length mismatch: expected {}, got {}",
                    layout.total_size,
                    shmem.len()
                ),
            });
        }

        let base_ptr = shmem.as_ptr();
        let control = base_ptr as *mut SharedMemoryControlBlock;
        let write_signal_ptr = unsafe { base_ptr.add(layout.control_size) };
        let read_signal_ptr = unsafe { base_ptr.add(layout.read_signal_offset) };
        let data = unsafe { base_ptr.add(layout.data_offset) };

        unsafe {
            std::ptr::write(control, SharedMemoryControlBlock::new(capacity, layout));
            let ctrl = &*control;
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
            write_signal.set(EventState::Signaled).map_err(|e| {
                TransportError::SharedMemoryCreation {
                    name: name.to_string(),
                    reason: format!("Failed to set write event: {}", e),
                }
            })?;
            ctrl.publish_ready();

            Ok(Self {
                shmem,
                control,
                data,
                capacity,
                is_owner: true,
                attachment_claimed: false,
                name: name.to_string(),
                write_signal,
                read_signal,
                #[cfg(test)]
                commit_test_hook: Mutex::new(None),
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
        let expected_control_size = std::mem::size_of::<SharedMemoryControlBlock>();
        if shmem.len() < expected_control_size {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' is too small for the versioned header: {} < {}",
                name,
                shmem.len(),
                expected_control_size
            )));
        }

        let base_ptr = shmem.as_ptr();
        let control = base_ptr as *mut SharedMemoryControlBlock;
        let ctrl = unsafe { &*control };
        if ctrl.magic.load(Ordering::Acquire) != SHM_MAGIC {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' has incompatible magic",
                name
            )));
        }
        if ctrl.version.load(Ordering::Acquire) != SHM_VERSION {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' has incompatible version",
                name
            )));
        }
        if ctrl.readiness.load(Ordering::Acquire) != SHM_READY {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' is not ready",
                name
            )));
        }

        let control_size =
            usize::try_from(ctrl.control_size.load(Ordering::Acquire)).map_err(|_| {
                TransportError::InvalidBufferState(format!(
                    "Shared memory mapping '{}' has an invalid control size",
                    name
                ))
            })?;
        let event_size =
            usize::try_from(ctrl.event_size.load(Ordering::Acquire)).map_err(|_| {
                TransportError::InvalidBufferState(format!(
                    "Shared memory mapping '{}' has an invalid event size",
                    name
                ))
            })?;
        let total_size =
            usize::try_from(ctrl.total_size.load(Ordering::Acquire)).map_err(|_| {
                TransportError::InvalidBufferState(format!(
                    "Shared memory mapping '{}' has an invalid total size",
                    name
                ))
            })?;
        let capacity = usize::try_from(ctrl.capacity.load(Ordering::Acquire)).map_err(|_| {
            TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' has an invalid capacity",
                name
            ))
        })?;
        let expected_event_size = Event::size_of(None);
        if control_size != expected_control_size || event_size != expected_event_size {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' has an incompatible control or event layout",
                name
            )));
        }
        if !(MIN_BUFFER_SIZE..=MAX_BUFFER_SIZE).contains(&capacity) {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' has an invalid capacity: {}",
                name, capacity
            )));
        }
        let layout =
            SharedMemoryLayout::checked(capacity, control_size, event_size).map_err(|e| {
                TransportError::InvalidBufferState(format!(
                    "Shared memory mapping '{}' has an invalid layout: {}",
                    name, e
                ))
            })?;
        if total_size != layout.total_size || shmem.len() != layout.total_size {
            return Err(TransportError::InvalidBufferState(format!(
                "Shared memory mapping '{}' length mismatch: header {}, computed {}, mapped {}",
                name,
                total_size,
                layout.total_size,
                shmem.len()
            )));
        }
        if !ctrl.is_connected() {
            return Err(TransportError::ConnectionClosed);
        }

        ctrl.claim_attachment(name)?;
        let mut attachment = SharedMemoryAttachment {
            control: ctrl,
            armed: true,
        };
        let write_signal_ptr = unsafe { base_ptr.add(layout.control_size) };
        let read_signal_ptr = unsafe { base_ptr.add(layout.read_signal_offset) };
        let data = unsafe { base_ptr.add(layout.data_offset) };

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
            if !ctrl.is_valid() {
                return Err(TransportError::InvalidBufferState(format!(
                    "Shared memory mapping '{}' changed while attaching",
                    name
                )));
            }
            if !ctrl.is_connected() {
                return Err(TransportError::ConnectionClosed);
            }
            attachment.disarm();

            Ok(Self {
                shmem,
                control,
                data,
                capacity,
                is_owner: false,
                attachment_claimed: true,
                name: name.to_string(),
                write_signal,
                read_signal,
                #[cfg(test)]
                commit_test_hook: Mutex::new(None),
            })
        }
    }

    fn control(&self) -> &SharedMemoryControlBlock {
        unsafe { &*self.control }
    }

    fn is_connected(&self) -> bool {
        self.control().is_connected()
    }

    #[cfg(test)]
    fn install_commit_test_hook(
        &self,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::SyncSender<()>,
    ) {
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
        *self.commit_test_hook.lock() = Some(Arc::new(CommitTestHook {
            reached: reached_tx,
            resume: Mutex::new(resume_rx),
        }));
        (reached_rx, resume_tx)
    }

    #[cfg(test)]
    fn pause_before_commit(&self) {
        let hook = self.commit_test_hook.lock().take();
        if let Some(hook) = hook {
            hook.pause();
        }
    }

    fn ensure_control_valid(&self) -> TransportResult<()> {
        let control = self.control();
        if !control.is_valid() {
            control.record_error();
            return Err(TransportError::InvalidBufferState(
                "Bad control".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_local_operation_open(
        &self,
        terminal: &AtomicBool,
        operation: &AtomicU8,
    ) -> TransportResult<()> {
        if terminal.load(Ordering::Acquire) || BlockingCancellation::is_cancelled(operation) {
            Err(TransportError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn ensure_write_open(
        &self,
        terminal: &AtomicBool,
        operation: &AtomicU8,
    ) -> TransportResult<()> {
        self.ensure_local_operation_open(terminal, operation)?;
        self.ensure_control_valid()?;
        if self.is_connected() {
            Ok(())
        } else {
            Err(TransportError::ConnectionClosed)
        }
    }

    fn ensure_read_commit(
        &self,
        terminal: &AtomicBool,
        operation: &AtomicU8,
        frame_len: usize,
    ) -> TransportResult<()> {
        self.ensure_local_operation_open(terminal, operation)?;
        self.ensure_control_valid()?;

        if self.control().available_read() >= frame_len {
            return Ok(());
        }
        if !self.is_connected() {
            return Err(TransportError::ConnectionClosed);
        }

        self.control().record_error();
        Err(TransportError::InvalidBufferState(
            "Frame changed before receive commit".to_string(),
        ))
    }

    fn wait_for_event(
        &self,
        event: &dyn EventImpl,
        duration: Duration,
        operation: &str,
    ) -> TransportResult<()> {
        let mut retried_immediate_unix_failure = false;
        loop {
            let started = Instant::now();
            match event.wait(Timeout::Val(duration)) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let reason = error.to_string();
                    let is_timeout =
                        Self::is_event_wait_timeout(&reason, started.elapsed(), duration);
                    if is_timeout {
                        return Ok(());
                    }

                    #[cfg(unix)]
                    if !retried_immediate_unix_failure
                        && !duration.is_zero()
                        && reason == "Failed waiting for signal"
                    {
                        // raw_sync 0.1.x does not normalize tv_nsec after adding the
                        // timeout to CLOCK_REALTIME. A wait that crosses a second
                        // boundary can therefore fail immediately with EINVAL, which
                        // raw_sync collapses into this generic message. Retry once
                        // after crossing that boundary; a repeated failure remains a
                        // real InvalidBufferState instead of being hidden as timeout.
                        retried_immediate_unix_failure = true;
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default();
                        let until_boundary =
                            Duration::from_nanos(1_000_000_000_u64 - u64::from(now.subsec_nanos()));
                        if until_boundary <= duration {
                            std::thread::sleep(
                                until_boundary
                                    .saturating_add(Duration::from_micros(100))
                                    .min(duration),
                            );
                        } else {
                            std::thread::yield_now();
                        }
                        continue;
                    }

                    self.control().record_error();
                    return Err(TransportError::InvalidBufferState(format!(
                        "Failed to wait for {} event in '{}': {}",
                        operation, self.name, reason
                    )));
                }
            }
        }
    }

    fn signal_event(&self, event: &dyn EventImpl) {
        if event.set(EventState::Signaled).is_err() {
            self.control().record_error();
        }
    }

    fn disconnect(&self) -> TransportResult<()> {
        let control = self.control();
        let mut failures = Vec::new();
        if let Err(error) = control.close_write_commits() {
            failures.push(format!("write commit: {}", error));
        }
        if let Err(error) = self.write_signal.set(EventState::Signaled) {
            control.record_error();
            failures.push(format!("write event: {}", error));
        }
        if let Err(error) = self.read_signal.set(EventState::Signaled) {
            control.record_error();
            failures.push(format!("read event: {}", error));
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(TransportError::InvalidBufferState(format!(
                "Failed to signal shutdown for '{}': {}",
                self.name,
                failures.join(", ")
            )))
        }
    }

    fn prepare_write(
        &self,
        data: &[u8],
        timeout: Duration,
        terminal: &AtomicBool,
        operation: &AtomicU8,
    ) -> TransportResult<PreparedWrite> {
        let control = self.control();
        self.ensure_write_open(terminal, operation)?;

        let msg_len = data.len();
        let total_len = msg_len.checked_add(4).ok_or_else(|| {
            control.record_error();
            TransportError::MessageTooLarge {
                size: msg_len,
                max: self.capacity - 5,
            }
        })?;

        if total_len >= self.capacity {
            control.record_error();
            return Err(TransportError::MessageTooLarge {
                size: msg_len,
                max: self.capacity - 5,
            });
        }

        let start = Instant::now();
        loop {
            self.ensure_write_open(terminal, operation)?;
            if control.available_write() >= total_len {
                break;
            }
            if start.elapsed() >= timeout {
                self.ensure_write_open(terminal, operation)?;
                control.record_error();
                return Err(TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "waiting for buffer space".into(),
                });
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            let wait_duration = remaining.min(SHUTDOWN_POLL_INTERVAL);
            if let Err(error) =
                self.wait_for_event(self.write_signal.as_ref(), wait_duration, "writable buffer")
            {
                self.ensure_write_open(terminal, operation)?;
                return Err(error);
            }
        }

        self.ensure_write_open(terminal, operation)?;
        let write_pos = control.write_pos.load(Ordering::Acquire) as usize;
        let len_bytes = (msg_len as u32).to_le_bytes();
        self.raw_write(write_pos, &len_bytes);
        self.raw_write(write_pos + 4, data);
        #[cfg(test)]
        self.pause_before_commit();

        Ok(PreparedWrite {
            write_pos,
            total_len,
        })
    }

    fn commit_write(
        &self,
        prepared: PreparedWrite,
        terminal: &AtomicBool,
        operation: &AtomicU8,
        state_lock: &Mutex<()>,
    ) -> TransportResult<()> {
        let control = self.control();
        let state_guard = state_lock.lock();
        self.ensure_write_open(terminal, operation)?;
        let write_commit = control.begin_write_commit()?;
        BlockingCancellation::commit(operation)?;
        control.write_pos.store(
            (prepared.write_pos + prepared.total_len) as u64,
            Ordering::Release,
        );
        drop(write_commit);
        drop(state_guard);

        control.update_heartbeat();
        self.signal_event(self.read_signal.as_ref());

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

    fn prepare_read(
        &self,
        timeout: Duration,
        terminal: &AtomicBool,
        operation: &AtomicU8,
    ) -> TransportResult<PreparedRead> {
        let control = self.control();
        self.ensure_local_operation_open(terminal, operation)?;
        self.ensure_control_valid()?;

        let start = Instant::now();
        loop {
            self.ensure_local_operation_open(terminal, operation)?;
            self.ensure_control_valid()?;
            if control.available_read() >= 4 {
                break;
            }
            if !control.may_receive_more() {
                return Err(TransportError::ConnectionClosed);
            }
            if start.elapsed() >= timeout {
                self.ensure_local_operation_open(terminal, operation)?;
                control.record_error();
                return Err(TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "waiting for data".into(),
                });
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            let wait_duration = remaining.min(SHUTDOWN_POLL_INTERVAL);
            if let Err(error) =
                self.wait_for_event(self.read_signal.as_ref(), wait_duration, "readable data")
            {
                self.ensure_local_operation_open(terminal, operation)?;
                if !control.may_receive_more() {
                    return Err(TransportError::ConnectionClosed);
                }
                return Err(error);
            }
        }

        self.ensure_local_operation_open(terminal, operation)?;
        self.ensure_control_valid()?;
        let read_pos = control.read_pos.load(Ordering::Acquire) as usize;
        let mut len_bytes = [0u8; 4];
        self.raw_read(read_pos, &mut len_bytes);
        let msg_len = u32::from_le_bytes(len_bytes) as usize;

        if msg_len > self.capacity - 5 {
            control.record_error();
            return Err(TransportError::InvalidBufferState(
                "Bad message length".to_string(),
            ));
        }

        let frame_len = 4 + msg_len;
        loop {
            self.ensure_local_operation_open(terminal, operation)?;
            self.ensure_control_valid()?;
            if control.available_read() >= frame_len {
                break;
            }
            if !control.may_receive_more() {
                return Err(TransportError::ConnectionClosed);
            }
            if start.elapsed() >= timeout {
                self.ensure_local_operation_open(terminal, operation)?;
                control.record_error();
                return Err(TransportError::Timeout {
                    duration_ms: timeout.as_millis() as u64,
                    operation: "waiting for full message".into(),
                });
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            let wait_duration = remaining.min(SHUTDOWN_POLL_INTERVAL);
            if let Err(error) =
                self.wait_for_event(self.read_signal.as_ref(), wait_duration, "complete frame")
            {
                self.ensure_local_operation_open(terminal, operation)?;
                if !control.may_receive_more() {
                    return Err(TransportError::ConnectionClosed);
                }
                return Err(error);
            }
        }

        self.ensure_local_operation_open(terminal, operation)?;
        let mut buffer = vec![0u8; msg_len];
        self.raw_read(read_pos + 4, &mut buffer);
        #[cfg(test)]
        self.pause_before_commit();

        Ok(PreparedRead {
            read_pos,
            frame_len,
            payload: Bytes::from(buffer),
        })
    }

    fn commit_read(
        &self,
        prepared: &PreparedRead,
        terminal: &AtomicBool,
        operation: &AtomicU8,
        state_lock: &Mutex<()>,
    ) -> TransportResult<()> {
        let control = self.control();
        let state_guard = state_lock.lock();
        self.ensure_read_commit(terminal, operation, prepared.frame_len)?;
        BlockingCancellation::commit(operation)?;
        control.read_pos.store(
            (prepared.read_pos + prepared.frame_len) as u64,
            Ordering::Release,
        );
        drop(state_guard);

        control.update_heartbeat();
        self.signal_event(self.write_signal.as_ref());

        Ok(())
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
        if self.is_connected() {
            self.control().tick_heartbeat();
        }
    }
}

impl Drop for SharedMemoryRingBuffer {
    fn drop(&mut self) {
        if self.is_owner {
            if self.disconnect().is_err() {
                self.control().record_error();
            }
        } else if self.attachment_claimed {
            self.control().release_attachment();
            self.attachment_claimed = false;
        }
    }
}

/// Retry policy for reconnection attempts.
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

/// Configuration for shared memory frame transport.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SharedMemoryRole {
    Client,
    Server,
}

/// Shared memory frame transport with ring buffer (Layer 1).
#[derive(Debug)]
pub struct SharedMemoryFrameTransport {
    send_buffer: ArcSwap<SharedMemoryRingBuffer>,
    recv_buffer: ArcSwap<SharedMemoryRingBuffer>,
    config: SharedMemoryConfig,
    stats: Arc<Mutex<TransportStats>>,
    name: String,
    error_count: Arc<Mutex<usize>>,
    connected: Arc<AtomicBool>,
    terminal: Arc<AtomicBool>,
    role: SharedMemoryRole,
    reconnect_name: String,
    send_lock: Arc<AsyncMutex<()>>,
    recv_lock: Arc<AsyncMutex<()>>,
    reconnect_lock: AsyncMutex<()>,
    state_lock: Arc<Mutex<()>>,
    #[cfg(test)]
    active_blocking_sends: Arc<AtomicUsize>,
    #[cfg(test)]
    active_blocking_receives: Arc<AtomicUsize>,
}

impl SharedMemoryFrameTransport {
    fn start_send_heartbeat(buffer: &Arc<SharedMemoryRingBuffer>) {
        buffer.tick_heartbeat();
        spawn_weak_loop(Arc::downgrade(buffer), Duration::from_secs(5), |buffer| {
            buffer.tick_heartbeat();
        });
    }

    /// Create a server-side transport.
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

        Self::start_send_heartbeat(&send_buffer);

        Ok(Self {
            send_buffer: ArcSwap::new(send_buffer),
            recv_buffer: ArcSwap::new(recv_buffer),
            config,
            stats: Arc::new(Mutex::new(TransportStats::default())),
            name: format!("{}-server", name),
            error_count: Arc::new(Mutex::new(0)),
            connected: Arc::new(AtomicBool::new(true)),
            terminal: Arc::new(AtomicBool::new(false)),
            role: SharedMemoryRole::Server,
            reconnect_name: name.clone(),
            send_lock: Arc::new(AsyncMutex::new(())),
            recv_lock: Arc::new(AsyncMutex::new(())),
            reconnect_lock: AsyncMutex::new(()),
            state_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            active_blocking_sends: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            active_blocking_receives: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Connect as a client with custom config.
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

                    Self::start_send_heartbeat(&send_arc);

                    return Ok(Self {
                        send_buffer: ArcSwap::new(send_arc),
                        recv_buffer: ArcSwap::new(recv_arc),
                        config: config.clone(),
                        stats: Arc::new(Mutex::new(TransportStats::default())),
                        name: format!("{}-client", name),
                        error_count: Arc::new(Mutex::new(0)),
                        connected: Arc::new(AtomicBool::new(true)),
                        terminal: Arc::new(AtomicBool::new(false)),
                        role: SharedMemoryRole::Client,
                        reconnect_name: name.clone(),
                        send_lock: Arc::new(AsyncMutex::new(())),
                        recv_lock: Arc::new(AsyncMutex::new(())),
                        reconnect_lock: AsyncMutex::new(()),
                        state_lock: Arc::new(Mutex::new(())),
                        #[cfg(test)]
                        active_blocking_sends: Arc::new(AtomicUsize::new(0)),
                        #[cfg(test)]
                        active_blocking_receives: Arc::new(AtomicUsize::new(0)),
                    });
                }
                (Err(error), _) | (_, Err(error)) => {
                    if matches!(&error, TransportError::ConnectionClosed) {
                        return Err(error);
                    }
                    last_error = Some(error);
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

    /// Connect as a client with default config.
    pub fn connect_client(name: impl Into<String>) -> TransportResult<Self> {
        Self::connect_client_with_config(name, SharedMemoryConfig::default())
    }

    fn ensure_local_open(&self) -> TransportResult<()> {
        if self.terminal.load(Ordering::Acquire) {
            Err(TransportError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn buffers_connected(&self) -> bool {
        let send_buffer = self.send_buffer.load_full();
        let recv_buffer = self.recv_buffer.load_full();
        send_buffer.is_connected() && recv_buffer.is_connected()
    }

    fn connection_is_open(&self) -> bool {
        !self.terminal.load(Ordering::Acquire)
            && self.connected.load(Ordering::Acquire)
            && self.buffers_connected()
    }

    fn is_non_partial_timeout(error: &TransportError) -> bool {
        matches!(
            error,
            TransportError::Timeout { operation, .. }
                if operation == "waiting for buffer space" || operation == "waiting for data"
        )
    }

    async fn wait_for_retry_delay(&self, duration: Duration) -> TransportResult<()> {
        let started = Instant::now();
        while started.elapsed() < duration {
            self.ensure_local_open()?;
            let remaining = duration.saturating_sub(started.elapsed());
            tokio::time::sleep(remaining.min(SHUTDOWN_POLL_INTERVAL)).await;
        }
        self.ensure_local_open()
    }

    async fn try_reconnect(&self) -> TransportResult<()> {
        self.ensure_local_open()?;
        let _reconnect_guard = self.reconnect_lock.lock().await;
        self.ensure_local_open()?;

        if self.buffers_connected() {
            self.connected.store(true, Ordering::Release);
            return Ok(());
        }
        if !self.config.auto_reconnect {
            return Err(TransportError::NotConnected);
        }
        if self.role == SharedMemoryRole::Server {
            return Err(TransportError::ConnectionFailed {
                name: self.reconnect_name.clone(),
                attempts: 0,
                reason: "server-side shared memory reconnect is not supported".into(),
            });
        }

        let send_name = format!("{}_c2s", self.reconnect_name);
        let recv_name = format!("{}_s2c", self.reconnect_name);
        let mut last_error = None;

        for attempt in 0..self.config.max_retry_attempts {
            if attempt > 0 {
                self.wait_for_retry_delay(self.config.retry_policy.delay(attempt - 1))
                    .await?;
            }
            self.ensure_local_open()?;

            match (
                SharedMemoryRingBuffer::connect(&send_name),
                SharedMemoryRingBuffer::connect(&recv_name),
            ) {
                (Ok(send_buffer), Ok(recv_buffer)) => {
                    let send_buffer = Arc::new(send_buffer);
                    let recv_buffer = Arc::new(recv_buffer);
                    let state_guard = self.state_lock.lock();
                    self.ensure_local_open()?;
                    self.send_buffer.store(Arc::clone(&send_buffer));
                    self.recv_buffer.store(recv_buffer);
                    *self.error_count.lock() = 0;
                    self.connected.store(true, Ordering::Release);
                    drop(state_guard);
                    Self::start_send_heartbeat(&send_buffer);
                    return Ok(());
                }
                (Err(error), _) | (_, Err(error)) => {
                    self.ensure_local_open()?;
                    if matches!(&error, TransportError::ConnectionClosed) {
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }

        Err(
            last_error.unwrap_or_else(|| TransportError::ConnectionFailed {
                name: self.reconnect_name.clone(),
                attempts: self.config.max_retry_attempts,
                reason: "reconnect failed".into(),
            }),
        )
    }

    fn shutdown(&self) -> TransportResult<()> {
        let _state_guard = self.state_lock.lock();
        self.terminal.store(true, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        let send_buffer = self.send_buffer.load_full();
        let recv_buffer = self.recv_buffer.load_full();
        let mut failures = Vec::new();

        if let Err(error) = send_buffer.disconnect() {
            failures.push(error.to_string());
        }
        if let Err(error) = recv_buffer.disconnect() {
            failures.push(error.to_string());
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(TransportError::InvalidBufferState(format!(
                "Failed to close '{}': {}",
                self.name,
                failures.join(", ")
            )))
        }
    }

    pub fn shm_is_healthy(&self) -> bool {
        if !self.connection_is_open() {
            return false;
        }

        let recv_buffer = self.recv_buffer.load_full();
        recv_buffer.control().is_healthy(30) && *self.error_count.lock() < 10
    }

    pub fn shm_stats(&self) -> TransportStats {
        self.stats.lock().clone()
    }

    #[cfg(test)]
    fn active_blocking_send_count(&self) -> usize {
        self.active_blocking_sends.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn active_blocking_receive_count(&self) -> usize {
        self.active_blocking_receives.load(Ordering::Acquire)
    }
}

impl Drop for SharedMemoryFrameTransport {
    fn drop(&mut self) {
        if self.shutdown().is_err() {
            *self.error_count.lock() += 1;
        }
    }
}

#[async_trait]
impl FrameTransport for SharedMemoryFrameTransport {
    fn is_healthy(&self) -> bool {
        self.shm_is_healthy()
    }

    async fn send_frame(&self, data: &[u8]) -> TransportResult<()> {
        let result: TransportResult<()> = async {
            let cancellation = BlockingCancellation::new();
            let send_guard = Arc::new(Arc::clone(&self.send_lock).lock_owned().await);
            self.ensure_local_open()?;
            if !self.connected.load(Ordering::Acquire) {
                self.try_reconnect().await?;
            }

            let mut last_error = None;
            let mut only_non_partial_timeouts = true;

            for attempt in 0..self.config.max_retry_attempts {
                if attempt > 0 {
                    self.wait_for_retry_delay(self.config.retry_policy.delay(attempt - 1))
                        .await?;
                }
                self.ensure_local_open()?;

                let operation = cancellation.state();
                let result = tokio::task::spawn_blocking({
                    let buffer = self.send_buffer.load_full();
                    let terminal = Arc::clone(&self.terminal);
                    let worker_operation = Arc::clone(&operation);
                    #[cfg(test)]
                    let active_blocking_sends = Arc::clone(&self.active_blocking_sends);
                    let operation_guard = Arc::clone(&send_guard);
                    let data = data.to_vec();
                    let timeout = self.config.write_timeout.unwrap_or(Duration::from_secs(30));
                    move || {
                        #[cfg(test)]
                        let _active_operation = ActiveBlockingOperation::new(active_blocking_sends);
                        let result = buffer.prepare_write(
                            &data,
                            timeout,
                            terminal.as_ref(),
                            worker_operation.as_ref(),
                        );
                        drop(operation_guard);
                        result.map(|prepared| (buffer, prepared))
                    }
                })
                .await;

                let operation_result = match result {
                    Ok(Ok((buffer, prepared))) => buffer.commit_write(
                        prepared,
                        self.terminal.as_ref(),
                        operation.as_ref(),
                        self.state_lock.as_ref(),
                    ),
                    Ok(Err(error)) => Err(error),
                    Err(error) => {
                        last_error = Some(TransportError::SendFailed {
                            attempts: attempt + 1,
                            reason: error.to_string(),
                        });
                        only_non_partial_timeouts = false;
                        *self.error_count.lock() += 1;
                        continue;
                    }
                };

                match operation_result {
                    Ok(()) => {
                        let mut stats = self.stats.lock();
                        stats.messages_sent += 1;
                        stats.bytes_sent += data.len() as u64;
                        return Ok(());
                    }
                    Err(error) => {
                        let non_partial_timeout = Self::is_non_partial_timeout(&error);
                        if !non_partial_timeout {
                            *self.error_count.lock() += 1;
                            only_non_partial_timeouts = false;
                        }
                        if matches!(&error, TransportError::ConnectionClosed) {
                            self.connected.store(false, Ordering::Release);
                            return Err(error);
                        }
                        last_error = Some(error);
                    }
                }
            }

            self.ensure_local_open()?;
            if only_non_partial_timeouts
                && last_error
                    .as_ref()
                    .is_some_and(Self::is_non_partial_timeout)
            {
                return Err(last_error.expect("timeout error disappeared"));
            }

            self.connected.store(false, Ordering::Release);
            Err(last_error.unwrap_or_else(|| TransportError::SendFailed {
                attempts: self.config.max_retry_attempts,
                reason: "max retries exceeded".into(),
            }))
        }
        .await;
        if result.is_err() {
            self.stats.lock().send_errors += 1;
        }
        result
    }

    async fn recv_frame(&self) -> TransportResult<Bytes> {
        let result: TransportResult<Bytes> = async {
            let cancellation = BlockingCancellation::new();
            let recv_guard = Arc::new(Arc::clone(&self.recv_lock).lock_owned().await);
            self.ensure_local_open()?;
            if !self.connected.load(Ordering::Acquire) {
                self.try_reconnect().await?;
            }

            let mut last_error = None;
            let mut only_non_partial_timeouts = true;

            for attempt in 0..self.config.max_retry_attempts {
                if attempt > 0 {
                    self.wait_for_retry_delay(self.config.retry_policy.delay(attempt - 1))
                        .await?;
                }
                self.ensure_local_open()?;

                let operation = cancellation.state();
                let result = tokio::task::spawn_blocking({
                    let buffer = self.recv_buffer.load_full();
                    let terminal = Arc::clone(&self.terminal);
                    let worker_operation = Arc::clone(&operation);
                    #[cfg(test)]
                    let active_blocking_receives = Arc::clone(&self.active_blocking_receives);
                    let operation_guard = Arc::clone(&recv_guard);
                    let timeout = self.config.read_timeout.unwrap_or(Duration::from_secs(30));
                    move || {
                        #[cfg(test)]
                        let _active_operation =
                            ActiveBlockingOperation::new(active_blocking_receives);
                        let result = buffer.prepare_read(
                            timeout,
                            terminal.as_ref(),
                            worker_operation.as_ref(),
                        );
                        drop(operation_guard);
                        result.map(|prepared| (buffer, prepared))
                    }
                })
                .await;

                let operation_result = match result {
                    Ok(Ok((buffer, prepared))) => buffer
                        .commit_read(
                            &prepared,
                            self.terminal.as_ref(),
                            operation.as_ref(),
                            self.state_lock.as_ref(),
                        )
                        .map(|()| prepared.payload),
                    Ok(Err(error)) => Err(error),
                    Err(error) => {
                        last_error = Some(TransportError::ReceiveFailed {
                            attempts: attempt + 1,
                            reason: error.to_string(),
                        });
                        only_non_partial_timeouts = false;
                        *self.error_count.lock() += 1;
                        continue;
                    }
                };

                match operation_result {
                    Ok(bytes) => {
                        let mut stats = self.stats.lock();
                        stats.messages_received += 1;
                        stats.bytes_received += bytes.len() as u64;
                        return Ok(bytes);
                    }
                    Err(error) => {
                        let non_partial_timeout = Self::is_non_partial_timeout(&error);
                        if !non_partial_timeout {
                            *self.error_count.lock() += 1;
                            only_non_partial_timeouts = false;
                        }
                        if matches!(&error, TransportError::ConnectionClosed) {
                            self.connected.store(false, Ordering::Release);
                            return Err(error);
                        }
                        last_error = Some(error);
                    }
                }
            }

            self.ensure_local_open()?;
            if only_non_partial_timeouts
                && last_error
                    .as_ref()
                    .is_some_and(Self::is_non_partial_timeout)
            {
                return Err(last_error.expect("timeout error disappeared"));
            }

            self.connected.store(false, Ordering::Release);
            Err(last_error.unwrap_or_else(|| TransportError::ReceiveFailed {
                attempts: self.config.max_retry_attempts,
                reason: "max retries exceeded".into(),
            }))
        }
        .await;
        if result.is_err() {
            self.stats.lock().recv_errors += 1;
        }
        result
    }

    fn is_connected(&self) -> bool {
        self.connection_is_open()
    }

    async fn close(&self) -> TransportResult<()> {
        self.shutdown()
    }

    fn stats(&self) -> Option<TransportStats> {
        Some(self.stats.lock().clone())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Deprecated alias for backward compatibility
#[deprecated(since = "0.2.0", note = "Use SharedMemoryFrameTransport instead")]
pub type SharedMemoryTransport = SharedMemoryFrameTransport;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MessageChannelAdapter, RpcClient};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Child, Command, Stdio};
    use std::sync::mpsc;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
    const CHILD_MODE_ENV: &str = "XRPC_SHM_SHUTDOWN_CHILD_MODE";
    const EXIT_MARKER: &str = "XRPC_SHUTDOWN_READY_FOR_RUNTIME_DROP";

    fn unique_name(prefix: &str) -> String {
        format!(
            "{}-{}-{}",
            prefix,
            std::process::id(),
            NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn test_control(capacity: usize) -> SharedMemoryControlBlock {
        let layout = SharedMemoryLayout::checked(
            capacity,
            std::mem::size_of::<SharedMemoryControlBlock>(),
            Event::size_of(None),
        )
        .unwrap();
        SharedMemoryControlBlock::new(capacity, layout)
    }

    async fn wait_until(condition: impl Fn() -> bool) {
        let started = Instant::now();
        while !condition() {
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "condition was not met before timeout"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn wait_for_commit_hook(reached: mpsc::Receiver<()>) {
        tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(1)))
            .await
            .expect("commit hook waiter panicked")
            .expect("worker did not reach commit hook");
    }

    #[test]
    fn test_event_wait_timeout_rejects_immediate_generic_failure() {
        assert!(!SharedMemoryRingBuffer::is_event_wait_timeout(
            "Failed waiting for signal",
            Duration::from_millis(1),
            Duration::from_millis(100),
        ));
        assert!(!SharedMemoryRingBuffer::is_event_wait_timeout(
            "Failed waiting for signal",
            Duration::from_micros(1),
            Duration::from_millis(1),
        ));
        assert!(!SharedMemoryRingBuffer::is_event_wait_timeout(
            "Failed waiting for signal: 22",
            Duration::from_millis(100),
            Duration::from_millis(100),
        ));
        assert!(!SharedMemoryRingBuffer::is_event_wait_timeout(
            "Unknown raw_sync failure",
            Duration::from_millis(100),
            Duration::from_millis(100),
        ));
    }

    #[cfg(unix)]
    #[test]
    fn test_event_wait_timeout_accepts_elapsed_raw_sync_timeout() {
        assert!(SharedMemoryRingBuffer::is_event_wait_timeout(
            "Failed waiting for signal",
            Duration::from_millis(100),
            Duration::from_millis(100),
        ));
    }

    #[test]
    fn test_connect_rejects_mapping_shorter_than_header() {
        let name = unique_name("test-short-header");
        let _mapping = ShmemConf::new()
            .size(std::mem::size_of::<SharedMemoryControlBlock>() - 1)
            .os_id(&name)
            .create()
            .unwrap();

        let error = SharedMemoryRingBuffer::connect(&name).unwrap_err();

        assert!(matches!(error, TransportError::InvalidBufferState(_)));
        assert!(error.to_string().contains("too small"));
    }

    #[test]
    fn test_connect_validates_version_readiness_and_layout() {
        let name = unique_name("test-header-validation");
        let ring = SharedMemoryRingBuffer::create(&name, MIN_BUFFER_SIZE).unwrap();
        let control = ring.control();
        let control_size = std::mem::size_of::<SharedMemoryControlBlock>();
        let event_size = Event::size_of(None);
        let layout =
            SharedMemoryLayout::checked(MIN_BUFFER_SIZE, control_size, event_size).unwrap();

        assert_eq!(control.readiness.load(Ordering::Acquire), SHM_READY);
        assert_eq!(
            control.control_size.load(Ordering::Acquire),
            control_size as u64
        );
        assert_eq!(
            control.event_size.load(Ordering::Acquire),
            event_size as u64
        );
        assert_eq!(
            control.total_size.load(Ordering::Acquire),
            layout.total_size as u64
        );
        assert_ne!(control.last_heartbeat.load(Ordering::Acquire), 0);

        control.magic.store(0x5852_5043, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control.magic.store(SHM_MAGIC, Ordering::Release);

        control.version.store(SHM_VERSION - 1, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control.version.store(SHM_VERSION, Ordering::Release);

        control.readiness.store(SHM_INITIALIZING, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control.readiness.store(SHM_READY, Ordering::Release);

        control
            .control_size
            .store((control_size - 8) as u64, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control
            .control_size
            .store(control_size as u64, Ordering::Release);

        control
            .event_size
            .store((event_size + 1) as u64, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control
            .event_size
            .store(event_size as u64, Ordering::Release);

        control
            .capacity
            .store((MIN_BUFFER_SIZE - 1) as u64, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control
            .capacity
            .store(MIN_BUFFER_SIZE as u64, Ordering::Release);

        control
            .total_size
            .store((layout.total_size + 1) as u64, Ordering::Release);
        assert!(SharedMemoryRingBuffer::connect(&name).is_err());
        control
            .total_size
            .store(layout.total_size as u64, Ordering::Release);

        let attached = SharedMemoryRingBuffer::connect(&name).unwrap();
        assert_eq!(
            control.attachment_state.load(Ordering::Acquire),
            ATTACHMENT_CLAIMED
        );
        drop(attached);
        assert_eq!(
            control.attachment_state.load(Ordering::Acquire),
            ATTACHMENT_FREE
        );
    }

    #[test]
    fn test_close_waits_for_active_write_commit() {
        let control = Arc::new(test_control(MIN_BUFFER_SIZE));
        let closing_control = Arc::clone(&control);
        let write_commit = control.begin_write_commit().unwrap();
        let (closed_tx, closed_rx) = mpsc::sync_channel(1);
        let close_thread = std::thread::spawn(move || {
            closed_tx
                .send(closing_control.close_write_commits())
                .unwrap();
        });

        let started = Instant::now();
        while control.write_commit_state.load(Ordering::Acquire) != WRITE_COMMIT_CLOSING {
            assert!(started.elapsed() < Duration::from_secs(1));
            std::thread::yield_now();
        }
        assert!(matches!(
            closed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        control.write_pos.store(8, Ordering::Release);
        drop(write_commit);

        closed_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        close_thread.join().unwrap();
        assert_eq!(control.write_pos.load(Ordering::Acquire), 8);
        assert_eq!(
            control.write_commit_state.load(Ordering::Acquire),
            WRITE_COMMIT_CLOSED
        );
    }

    #[test]
    fn test_close_forces_recovery_of_abandoned_write_commit() {
        let control = Arc::new(test_control(MIN_BUFFER_SIZE));
        let write_commit = control.begin_write_commit().unwrap();
        std::mem::forget(write_commit);
        let started = Instant::now();

        let error = control.close_write_commits().unwrap_err();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(error.to_string().contains("Forced recovery"));
        assert_eq!(
            control.write_commit_state.load(Ordering::Acquire),
            WRITE_COMMIT_CLOSED
        );
    }

    #[test]
    fn test_payload_must_leave_ring_sentinel_byte() {
        let name = unique_name("test-payload-sentinel");
        let ring = SharedMemoryRingBuffer::create(&name, MIN_BUFFER_SIZE).unwrap();
        let terminal = AtomicBool::new(false);
        let state_lock = Mutex::new(());
        let oversized_operation = AtomicU8::new(OPERATION_PENDING);
        let oversized = vec![0u8; MIN_BUFFER_SIZE - 4];

        let error = ring
            .prepare_write(&oversized, Duration::ZERO, &terminal, &oversized_operation)
            .unwrap_err();

        assert!(matches!(
            error,
            TransportError::MessageTooLarge { size, max }
                if size == MIN_BUFFER_SIZE - 4 && max == MIN_BUFFER_SIZE - 5
        ));

        let maximum_operation = AtomicU8::new(OPERATION_PENDING);
        let maximum = vec![1u8; MIN_BUFFER_SIZE - 5];
        let prepared = ring
            .prepare_write(&maximum, Duration::ZERO, &terminal, &maximum_operation)
            .unwrap();
        ring.commit_write(prepared, &terminal, &maximum_operation, &state_lock)
            .unwrap();
    }

    #[test]
    fn test_blocking_preparation_does_not_publish_or_consume_positions() {
        let name = unique_name("test-prepare-before-commit");
        let ring = SharedMemoryRingBuffer::create(&name, MIN_BUFFER_SIZE).unwrap();
        let terminal = AtomicBool::new(false);
        let state_lock = Mutex::new(());
        let write_operation = AtomicU8::new(OPERATION_PENDING);

        let prepared_write = ring
            .prepare_write(
                b"prepared frame",
                Duration::ZERO,
                &terminal,
                &write_operation,
            )
            .unwrap();
        assert_eq!(ring.control().write_pos.load(Ordering::Acquire), 0);
        assert_eq!(ring.control().available_read(), 0);

        ring.commit_write(prepared_write, &terminal, &write_operation, &state_lock)
            .unwrap();
        let committed_write_pos = ring.control().write_pos.load(Ordering::Acquire);
        assert_ne!(committed_write_pos, 0);

        let read_operation = AtomicU8::new(OPERATION_PENDING);
        let prepared_read = ring
            .prepare_read(Duration::ZERO, &terminal, &read_operation)
            .unwrap();
        assert_eq!(prepared_read.payload.as_ref(), b"prepared frame");
        assert_eq!(ring.control().read_pos.load(Ordering::Acquire), 0);

        ring.commit_read(&prepared_read, &terminal, &read_operation, &state_lock)
            .unwrap();
        assert_eq!(
            ring.control().read_pos.load(Ordering::Acquire),
            committed_write_pos
        );
    }

    #[tokio::test]
    async fn test_cross_process_basic() {
        let name = unique_name("test-basic");
        let config = SharedMemoryConfig::default();
        let server = SharedMemoryFrameTransport::create_server(&name, config).unwrap();
        let client = SharedMemoryFrameTransport::connect_client(&name).unwrap();

        client.send_frame(b"Hello").await.unwrap();
        let msg = server.recv_frame().await.unwrap();
        assert_eq!(msg.as_ref(), b"Hello");
    }

    #[tokio::test]
    async fn test_second_client_attachment_is_rejected() {
        let name = unique_name("test-single-client");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client =
            SharedMemoryFrameTransport::connect_client_with_config(&name, config.clone()).unwrap();

        let error =
            SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap_err();

        assert!(matches!(error, TransportError::ConnectionFailed { .. }));
        assert_eq!(
            server
                .send_buffer
                .load_full()
                .control()
                .attachment_state
                .load(Ordering::Acquire),
            ATTACHMENT_CLAIMED
        );
        assert_eq!(
            server
                .recv_buffer
                .load_full()
                .control()
                .attachment_state
                .load(Ordering::Acquire),
            ATTACHMENT_CLAIMED
        );

        client.send_frame(b"single client").await.unwrap();
        assert_eq!(
            server.recv_frame().await.unwrap().as_ref(),
            b"single client"
        );
    }

    #[tokio::test]
    async fn test_partial_client_connection_releases_first_attachment() {
        let name = unique_name("test-partial-client-claim");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let held_second_ring = SharedMemoryRingBuffer::connect(&format!("{}_s2c", name)).unwrap();

        let error = SharedMemoryFrameTransport::connect_client_with_config(&name, config.clone())
            .unwrap_err();

        assert!(matches!(error, TransportError::ConnectionFailed { .. }));
        assert_eq!(
            server
                .recv_buffer
                .load_full()
                .control()
                .attachment_state
                .load(Ordering::Acquire),
            ATTACHMENT_FREE
        );

        drop(held_second_ring);
        let client = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();
        client.send_frame(b"claimed after cleanup").await.unwrap();
        assert_eq!(
            server.recv_frame().await.unwrap().as_ref(),
            b"claimed after cleanup"
        );
    }

    #[tokio::test]
    async fn test_ring_buffer_wrapping() {
        let name = unique_name("test-wrap");
        let config = SharedMemoryConfig::default().with_buffer_size(8192);
        let server = SharedMemoryFrameTransport::create_server(&name, config).unwrap();
        let client = Arc::new(SharedMemoryFrameTransport::connect_client(&name).unwrap());

        let chunk_size = 1000;
        let num_chunks = 20;
        let send_client = Arc::clone(&client);

        let send_handle = tokio::spawn(async move {
            for i in 0..num_chunks {
                let data = vec![i as u8; chunk_size];
                send_client.send_frame(&data).await.unwrap();
            }
        });

        for i in 0..num_chunks {
            let msg = server.recv_frame().await.unwrap();
            assert_eq!(msg.len(), chunk_size);
            assert!(msg.iter().all(|&b| b == i as u8), "chunk {} corrupted", i);
        }

        send_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_auto_reconnect() {
        let name = unique_name("test-reconnect");
        let config = SharedMemoryConfig::default().with_auto_reconnect(true);
        let server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();

        client.send_frame(b"before").await.unwrap();
        let msg = server.recv_frame().await.unwrap();
        assert_eq!(msg.as_ref(), b"before");

        client.connected.store(false, Ordering::Release);
        assert!(!client.is_connected());

        client.send_frame(b"after").await.unwrap();
        assert!(client.is_connected());

        let msg = server.recv_frame().await.unwrap();
        assert_eq!(msg.as_ref(), b"after");
    }

    #[tokio::test]
    async fn test_client_heartbeats_outbound_ring_after_reconnect() {
        let name = unique_name("test-reconnect-heartbeat");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client =
            SharedMemoryFrameTransport::connect_client_with_config(&name, config.clone()).unwrap();
        let client_send = client.send_buffer.load_full();
        assert!(!client_send.is_owner);
        client_send
            .control()
            .last_heartbeat
            .store(0, Ordering::Release);
        client_send.tick_heartbeat();
        assert_ne!(
            client_send.control().last_heartbeat.load(Ordering::Acquire),
            0
        );

        server.close().await.unwrap();
        drop(server);
        let restarted_server =
            SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        restarted_server
            .recv_buffer
            .load_full()
            .control()
            .last_heartbeat
            .store(0, Ordering::Release);

        client.try_reconnect().await.unwrap();

        assert_ne!(
            restarted_server
                .recv_buffer
                .load_full()
                .control()
                .last_heartbeat
                .load(Ordering::Acquire),
            0
        );
        client.send_frame(b"after restart").await.unwrap();
        assert_eq!(
            restarted_server.recv_frame().await.unwrap().as_ref(),
            b"after restart"
        );
    }

    #[tokio::test]
    async fn test_configurable_timeout() {
        let name = unique_name("test-timeout");
        let config = SharedMemoryConfig::default()
            .with_read_timeout(Duration::from_millis(100))
            .with_write_timeout(Duration::from_millis(100));

        let _server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();

        let start = Instant::now();
        let result = client.recv_frame().await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(
            elapsed < Duration::from_secs(1),
            "timeout took too long: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_receive_timeout_keeps_transport_connected() {
        let name = unique_name("test-receive-timeout-connected");
        let config = SharedMemoryConfig::default()
            .with_read_timeout(Duration::from_millis(20))
            .with_retry_policy(RetryPolicy::Fixed { delay_ms: 1 })
            .with_max_retries(2);
        let receiver = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let sender = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();

        let result = receiver.recv_frame().await;

        assert!(matches!(
            result,
            Err(TransportError::Timeout { operation, .. }) if operation == "waiting for data"
        ));
        assert!(receiver.is_connected());
        assert!(receiver.is_healthy());
        assert!(sender.is_connected());
        assert_eq!(receiver.stats().unwrap().recv_errors, 1);

        sender.send_frame(b"frame after timeout").await.unwrap();
        assert_eq!(
            receiver.recv_frame().await.unwrap().as_ref(),
            b"frame after timeout"
        );
    }

    #[tokio::test]
    async fn test_send_timeout_keeps_transport_connected() {
        let name = unique_name("test-send-timeout-connected");
        let config = SharedMemoryConfig::default()
            .with_buffer_size(MIN_BUFFER_SIZE)
            .with_write_timeout(Duration::from_millis(20))
            .with_retry_policy(RetryPolicy::Fixed { delay_ms: 1 })
            .with_max_retries(2);
        let receiver = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let sender = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();
        let first_frame = vec![1u8; 3000];
        let blocked_frame = vec![2u8; 3000];
        let later_frame = vec![3u8; 3000];
        sender.send_frame(&first_frame).await.unwrap();

        let result = sender.send_frame(&blocked_frame).await;

        assert!(matches!(
            result,
            Err(TransportError::Timeout { operation, .. }) if operation == "waiting for buffer space"
        ));
        assert!(sender.is_connected());
        assert!(sender.is_healthy());
        assert!(receiver.is_connected());
        assert_eq!(sender.stats().unwrap().send_errors, 1);

        assert_eq!(receiver.recv_frame().await.unwrap().as_ref(), first_frame);
        sender.send_frame(&later_frame).await.unwrap();
        assert_eq!(receiver.recv_frame().await.unwrap().as_ref(), later_frame);
    }

    #[tokio::test]
    async fn test_local_close_interrupts_long_recv() {
        let name = unique_name("test-local-close-recv");
        let config = SharedMemoryConfig::default()
            .with_read_timeout(Duration::from_secs(30))
            .with_max_retries(1);
        let _server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client = Arc::new(
            SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap(),
        );
        let recv_client = Arc::clone(&client);
        let recv_task = tokio::spawn(async move { recv_client.recv_frame().await });

        tokio::time::sleep(Duration::from_millis(75)).await;
        let close_started = Instant::now();
        client.close().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), recv_task)
            .await
            .expect("recv did not stop after local close")
            .unwrap();

        assert!(
            matches!(&result, Err(TransportError::ConnectionClosed)),
            "unexpected recv result: {:?}",
            result
        );
        assert!(close_started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_peer_close_interrupts_recv() {
        let name = unique_name("test-peer-close-recv");
        let config = SharedMemoryConfig::default()
            .with_read_timeout(Duration::from_secs(30))
            .with_max_retries(1);
        let server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client = Arc::new(
            SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap(),
        );
        let recv_client = Arc::clone(&client);
        let recv_task = tokio::spawn(async move { recv_client.recv_frame().await });

        tokio::time::sleep(Duration::from_millis(75)).await;
        server.close().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), recv_task)
            .await
            .expect("recv did not stop after peer close")
            .unwrap();

        assert!(
            matches!(&result, Err(TransportError::ConnectionClosed)),
            "unexpected recv result: {:?}",
            result
        );
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_close_is_idempotent() {
        let name = unique_name("test-idempotent-close");
        let server =
            SharedMemoryFrameTransport::create_server(&name, SharedMemoryConfig::default())
                .unwrap();
        let client = SharedMemoryFrameTransport::connect_client(&name).unwrap();

        client.close().await.unwrap();
        client.close().await.unwrap();

        assert!(!client.is_connected());
        assert!(!client.is_healthy());
        assert!(!server.is_connected());
    }

    #[tokio::test]
    async fn test_no_reconnect_after_close() {
        let name = unique_name("test-no-reconnect-after-close");
        let config = SharedMemoryConfig::default().with_auto_reconnect(true);
        let _server = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let client = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();

        client.close().await.unwrap();

        assert!(matches!(
            client.try_reconnect().await,
            Err(TransportError::ConnectionClosed)
        ));
        assert!(matches!(
            client.send_frame(b"closed").await,
            Err(TransportError::ConnectionClosed)
        ));
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_connected_state_tracks_close() {
        let name = unique_name("test-connected-state");
        let server =
            SharedMemoryFrameTransport::create_server(&name, SharedMemoryConfig::default())
                .unwrap();
        let client = SharedMemoryFrameTransport::connect_client(&name).unwrap();

        assert!(server.is_connected());
        assert!(client.is_connected());
        assert!(server.is_healthy());
        assert!(client.is_healthy());

        client.close().await.unwrap();

        assert!(!client.is_connected());
        assert!(!client.is_healthy());
        assert!(!server.is_connected());
        assert!(!server.is_healthy());
    }

    #[tokio::test]
    async fn test_aborted_recv_stops_worker_without_consuming_frame() {
        let name = unique_name("test-aborted-recv");
        let config = SharedMemoryConfig::default()
            .with_read_timeout(Duration::from_secs(30))
            .with_max_retries(1);
        let receiver =
            Arc::new(SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap());
        let sender = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();
        let recv_task = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            async move { receiver.recv_frame().await }
        });

        wait_until(|| receiver.active_blocking_receive_count() == 1).await;
        recv_task.abort();
        assert!(recv_task.await.unwrap_err().is_cancelled());
        wait_until(|| receiver.active_blocking_receive_count() == 0).await;

        assert!(receiver.is_connected());
        sender.send_frame(b"later frame").await.unwrap();
        let frame = receiver.recv_frame().await.unwrap();

        assert_eq!(frame.as_ref(), b"later frame");
        assert_eq!(receiver.active_blocking_receive_count(), 0);
        assert_eq!(sender.active_blocking_send_count(), 0);
    }

    #[tokio::test]
    async fn test_aborted_send_returns_active_counter_to_zero() {
        let name = unique_name("test-aborted-send");
        let config = SharedMemoryConfig::default()
            .with_buffer_size(MIN_BUFFER_SIZE)
            .with_write_timeout(Duration::from_secs(30))
            .with_max_retries(1);
        let receiver = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let sender = Arc::new(
            SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap(),
        );
        let first_frame = vec![1u8; 3000];
        sender.send_frame(&first_frame).await.unwrap();
        let send_task = tokio::spawn({
            let sender = Arc::clone(&sender);
            async move { sender.send_frame(&vec![2u8; 3000]).await }
        });

        wait_until(|| sender.active_blocking_send_count() == 1).await;
        send_task.abort();
        assert!(send_task.await.unwrap_err().is_cancelled());
        wait_until(|| sender.active_blocking_send_count() == 0).await;

        assert!(sender.is_connected());
        assert_eq!(receiver.recv_frame().await.unwrap().as_ref(), first_frame);
        sender.send_frame(b"later frame").await.unwrap();
        assert_eq!(
            receiver.recv_frame().await.unwrap().as_ref(),
            b"later frame"
        );
        assert_eq!(sender.active_blocking_send_count(), 0);
        assert_eq!(receiver.active_blocking_receive_count(), 0);
    }

    #[tokio::test]
    async fn test_cancel_before_write_commit_does_not_publish_frame() {
        let name = unique_name("test-cancel-write-commit");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let receiver = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let sender = Arc::new(
            SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap(),
        );
        let send_buffer = sender.send_buffer.load_full();
        let initial_write_pos = send_buffer.control().write_pos.load(Ordering::Acquire);
        let (reached, resume) = send_buffer.install_commit_test_hook();
        let send_task = tokio::spawn({
            let sender = Arc::clone(&sender);
            async move { sender.send_frame(b"cancelled frame").await }
        });

        wait_for_commit_hook(reached).await;
        send_task.abort();
        assert!(send_task.await.unwrap_err().is_cancelled());
        resume.send(()).unwrap();
        wait_until(|| sender.active_blocking_send_count() == 0).await;

        assert_eq!(
            send_buffer.control().write_pos.load(Ordering::Acquire),
            initial_write_pos
        );
        sender.send_frame(b"retained frame").await.unwrap();
        assert_eq!(
            receiver.recv_frame().await.unwrap().as_ref(),
            b"retained frame"
        );
    }

    #[tokio::test]
    async fn test_cancel_before_read_commit_does_not_consume_frame() {
        let name = unique_name("test-cancel-read-commit");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let receiver =
            Arc::new(SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap());
        let sender = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();
        sender.send_frame(b"retained frame").await.unwrap();

        let recv_buffer = receiver.recv_buffer.load_full();
        let initial_read_pos = recv_buffer.control().read_pos.load(Ordering::Acquire);
        let (reached, resume) = recv_buffer.install_commit_test_hook();
        let recv_task = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            async move { receiver.recv_frame().await }
        });

        wait_for_commit_hook(reached).await;
        recv_task.abort();
        assert!(recv_task.await.unwrap_err().is_cancelled());
        resume.send(()).unwrap();
        wait_until(|| receiver.active_blocking_receive_count() == 0).await;

        assert_eq!(
            recv_buffer.control().read_pos.load(Ordering::Acquire),
            initial_read_pos
        );
        assert_eq!(
            receiver.recv_frame().await.unwrap().as_ref(),
            b"retained frame"
        );
    }

    #[tokio::test]
    async fn test_peer_close_before_write_commit_does_not_publish_frame() {
        let name = unique_name("test-peer-close-write-commit");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let receiver = SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap();
        let sender = Arc::new(
            SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap(),
        );
        let send_buffer = sender.send_buffer.load_full();
        let initial_write_pos = send_buffer.control().write_pos.load(Ordering::Acquire);
        let (reached, resume) = send_buffer.install_commit_test_hook();
        let send_task = tokio::spawn({
            let sender = Arc::clone(&sender);
            async move { sender.send_frame(b"abandoned frame").await }
        });

        wait_for_commit_hook(reached).await;
        receiver.close().await.unwrap();
        resume.send(()).unwrap();
        let result = send_task.await.unwrap();

        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
        assert_eq!(
            send_buffer.control().write_pos.load(Ordering::Acquire),
            initial_write_pos
        );
    }

    #[tokio::test]
    async fn test_local_close_before_read_commit_does_not_consume_frame() {
        let name = unique_name("test-local-close-read-commit");
        let config = SharedMemoryConfig::default().with_max_retries(1);
        let receiver =
            Arc::new(SharedMemoryFrameTransport::create_server(&name, config.clone()).unwrap());
        let sender = SharedMemoryFrameTransport::connect_client_with_config(&name, config).unwrap();
        sender.send_frame(b"unconsumed frame").await.unwrap();

        let recv_buffer = receiver.recv_buffer.load_full();
        let initial_read_pos = recv_buffer.control().read_pos.load(Ordering::Acquire);
        let (reached, resume) = recv_buffer.install_commit_test_hook();
        let recv_task = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            async move { receiver.recv_frame().await }
        });

        wait_for_commit_hook(reached).await;
        receiver.close().await.unwrap();
        resume.send(()).unwrap();
        let result = recv_task.await.unwrap();

        assert!(matches!(result, Err(TransportError::ConnectionClosed)));
        assert_eq!(
            recv_buffer.control().read_pos.load(Ordering::Acquire),
            initial_read_pos
        );
    }

    #[tokio::test]
    async fn test_complete_frame_is_delivered_after_peer_close() {
        let name = unique_name("test-frame-before-peer-close");
        let server = SharedMemoryFrameTransport::create_server(
            &name,
            SharedMemoryConfig::default().with_max_retries(1),
        )
        .unwrap();
        let client = SharedMemoryFrameTransport::connect_client_with_config(
            &name,
            SharedMemoryConfig::default().with_max_retries(1),
        )
        .unwrap();

        server.send_frame(b"committed frame").await.unwrap();
        server.close().await.unwrap();

        let frame = client.recv_frame().await.unwrap();
        assert_eq!(frame.as_ref(), b"committed frame");
    }

    #[test]
    fn shutdown_child_entry() {
        let Ok(mode) = std::env::var(CHILD_MODE_ENV) else {
            return;
        };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create Tokio runtime");

        runtime.block_on(async move {
            let name = format!("xrpc-shutdown-{}-{mode}", std::process::id());
            let config = SharedMemoryConfig::default()
                .with_read_timeout(Duration::from_secs(300))
                .with_max_retries(3);
            let _server = SharedMemoryFrameTransport::create_server(&name, config.clone())
                .expect("failed to create SHM server");
            let transport = SharedMemoryFrameTransport::connect_client_with_config(&name, config)
                .expect("failed to connect SHM client");
            let client = RpcClient::new(MessageChannelAdapter::new(transport));
            let handle = client.try_start().expect("failed to start RPC client");
            let channel = client.transport();

            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if channel.inner().active_blocking_receive_count() > 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("SHM blocking receive did not start");

            match mode.as_str() {
                "joined" => {
                    client.close().await.expect("failed to close RPC client");
                    handle.join().await.expect("receive task failed");
                }
                "dropped" => {
                    client.close().await.expect("failed to close RPC client");
                    drop(handle);
                }
                "client-dropped" => {
                    drop(handle);
                    drop(client);
                }
                _ => panic!("unknown child mode: {mode}"),
            }

            println!("{EXIT_MARKER}");
            std::io::stdout()
                .flush()
                .expect("failed to flush child stdout");
        });
    }

    fn spawn_shutdown_child(mode: &str) -> Child {
        Command::new(std::env::current_exe().expect("failed to locate unit test executable"))
            .arg("--exact")
            .arg("transport::shared_memory::tests::shutdown_child_entry")
            .arg("--nocapture")
            .env(CHILD_MODE_ENV, mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn shutdown child")
    }

    fn assert_shutdown_child_exits_promptly(mode: &str) {
        let mut child = spawn_shutdown_child(mode);
        let stdout = child.stdout.take().expect("child stdout was not captured");
        let (line_tx, line_rx) = mpsc::channel();

        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        if line_tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let marker_deadline = Instant::now() + Duration::from_secs(10);
        let mut output = Vec::new();
        let mut reached_marker = false;
        while Instant::now() < marker_deadline {
            match line_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => {
                    reached_marker |= line.contains(EXIT_MARKER);
                    output.push(line);
                    if reached_marker {
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        if !reached_marker {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "shutdown child did not reach runtime drop marker for mode {mode}: {}",
                output.join("\n")
            );
        }

        let exit_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < exit_deadline {
            if let Some(status) = child.try_wait().expect("failed to query child status") {
                assert!(
                    status.success(),
                    "shutdown child failed for mode {mode}: {status}"
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let _ = child.kill();
        let _ = child.wait();
        let mut stderr = String::new();
        if let Some(mut child_stderr) = child.stderr.take() {
            let _ = child_stderr.read_to_string(&mut stderr);
        }
        panic!(
            "shutdown child did not exit within two seconds for mode {mode}\nstdout:\n{}\nstderr:\n{stderr}",
            output.join("\n")
        );
    }

    #[test]
    fn joined_handle_exits_without_read_timeout_tail() {
        if std::env::var_os(CHILD_MODE_ENV).is_none() {
            assert_shutdown_child_exits_promptly("joined");
        }
    }

    #[test]
    fn dropped_handle_exits_without_read_timeout_tail() {
        if std::env::var_os(CHILD_MODE_ENV).is_none() {
            assert_shutdown_child_exits_promptly("dropped");
        }
    }

    #[test]
    fn dropped_client_exits_without_read_timeout_tail() {
        if std::env::var_os(CHILD_MODE_ENV).is_none() {
            assert_shutdown_child_exits_promptly("client-dropped");
        }
    }
}
