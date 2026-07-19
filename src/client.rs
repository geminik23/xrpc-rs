use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock, oneshot, watch};
use tokio::task::AbortHandle;

use crate::channel::message::MessageChannel;
use crate::codec::{BincodeCodec, Codec};
use crate::error::{Result, RpcError, TransportError};
use crate::message::Message;
use crate::message::types::{MessageId, MessageType};
use crate::streaming::{StreamId, StreamManager, StreamReceiver, next_stream_id};

type PendingRequests<C> = Arc<Mutex<HashMap<MessageId, oneshot::Sender<Result<Message<C>>>>>>;
type ReceiveTaskAbort = Arc<Mutex<Option<AbortHandle>>>;

/// Application-level response deadline policy for an RPC call.
///
/// This policy does not change transport send/read timeouts or terminal connection handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallTimeout {
    /// Use the default response timeout configured on the [`RpcClient`].
    ClientDefault,
    /// Apply the supplied application-level response timeout after the request send commits.
    After(Duration),
    /// Wait without an application-level response deadline.
    ///
    /// Explicit client close, terminal connection failure, and dropping the call future still
    /// cancel the local wait. They do not cancel work already running on the server.
    Disabled,
}

/// Per-call RPC options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallOptions {
    timeout: CallTimeout,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: CallTimeout::ClientDefault,
        }
    }
}

impl CallOptions {
    /// Use a specific application-level response timeout for this call.
    pub const fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout: CallTimeout::After(timeout),
        }
    }

    /// Disable only the application-level response deadline for this call.
    ///
    /// The local wait remains interruptible by explicit client close, terminal connection
    /// failure, and future cancellation. No cancellation is propagated to the server.
    pub const fn without_timeout() -> Self {
        Self {
            timeout: CallTimeout::Disabled,
        }
    }
}

#[derive(Debug, Default)]
struct ClientState {
    closed: bool,
    started: bool,
    running: bool,
    terminal_error: Option<RpcError>,
}

#[derive(Debug, Clone, Default)]
enum TransportCloseState {
    #[default]
    Open,
    Closing,
    Closed(Result<()>),
}

struct ClientTaskFinalizer<C: Codec> {
    state: Arc<Mutex<ClientState>>,
    pending: PendingRequests<C>,
    stream_manager: Arc<StreamManager<C>>,
    send_cancel_tx: watch::Sender<Option<RpcError>>,
    receive_abort: ReceiveTaskAbort,
    cleanup_work: bool,
}

impl<C: Codec> Drop for ClientTaskFinalizer<C> {
    fn drop(&mut self) {
        let (error, _) = mark_client_terminal(&self.state, RpcError::ConnectionClosed);
        self.receive_abort.lock().take();
        if self.cleanup_work {
            self.send_cancel_tx.send_replace(Some(error.clone()));
            close_client_work(&self.pending, &self.stream_manager, error);
        }
    }
}

struct PendingRequestRegistration<C: Codec> {
    pending: PendingRequests<C>,
    message_id: MessageId,
}

impl<C: Codec> PendingRequestRegistration<C> {
    fn new(
        pending: &PendingRequests<C>,
        message_id: MessageId,
        sender: oneshot::Sender<Result<Message<C>>>,
    ) -> Self {
        pending.lock().insert(message_id, sender);
        Self {
            pending: Arc::clone(pending),
            message_id,
        }
    }
}

impl<C: Codec> Drop for PendingRequestRegistration<C> {
    fn drop(&mut self) {
        self.pending.lock().remove(&self.message_id);
    }
}

fn close_client_work<C: Codec>(
    pending: &PendingRequests<C>,
    stream_manager: &StreamManager<C>,
    error: RpcError,
) {
    let pending = {
        let mut pending = pending.lock();
        std::mem::take(&mut *pending)
    };
    for sender in pending.into_values() {
        let _ = sender.send(Err(error.clone()));
    }
    stream_manager.close_all(error);
}

fn normalize_transport_error(error: TransportError) -> RpcError {
    match error {
        TransportError::ConnectionClosed => RpcError::ConnectionClosed,
        error => RpcError::Transport(error),
    }
}

fn mark_client_terminal(state: &Mutex<ClientState>, error: RpcError) -> (RpcError, bool) {
    let mut state = state.lock();
    state.closed = true;
    state.running = false;
    if let Some(error) = &state.terminal_error {
        return (error.clone(), false);
    }
    state.terminal_error = Some(error.clone());
    (error, true)
}

fn is_terminal_outbound_error(error: &TransportError, is_connected: bool) -> bool {
    matches!(error, TransportError::ConnectionClosed) || !is_connected
}

struct TransportCloseFinalizer {
    state: Arc<Mutex<TransportCloseState>>,
    notify: Arc<Notify>,
    completed: bool,
}

struct ClientCloseFinalizer {
    state: Arc<Mutex<TransportCloseState>>,
    notify: Arc<Notify>,
    completed: bool,
}

impl ClientCloseFinalizer {
    fn complete(&mut self, result: Result<()>) {
        *self.state.lock() = TransportCloseState::Closed(result);
        self.completed = true;
        self.notify.notify_waiters();
    }
}

impl Drop for ClientCloseFinalizer {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        *self.state.lock() = TransportCloseState::Closed(Err(RpcError::ClientError(
            "Graceful client close task was cancelled or panicked".to_string(),
        )));
        self.notify.notify_waiters();
    }
}

impl TransportCloseFinalizer {
    fn complete(&mut self, result: Result<()>) {
        *self.state.lock() = TransportCloseState::Closed(result);
        self.completed = true;
        self.notify.notify_waiters();
    }
}

impl Drop for TransportCloseFinalizer {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        *self.state.lock() = TransportCloseState::Closed(Err(RpcError::ClientError(
            "Transport close task was cancelled or panicked".to_string(),
        )));
        self.notify.notify_waiters();
    }
}

async fn close_transport_once<T, C>(
    transport: &Arc<T>,
    close_state: &Arc<Mutex<TransportCloseState>>,
    close_notify: &Arc<Notify>,
) -> Result<()>
where
    T: MessageChannel<C> + 'static,
    C: Codec + 'static,
{
    let should_start = {
        let mut state = close_state.lock();
        match &*state {
            TransportCloseState::Open => {
                *state = TransportCloseState::Closing;
                true
            }
            TransportCloseState::Closing => false,
            TransportCloseState::Closed(result) => return result.clone(),
        }
    };

    if should_start {
        let transport = Arc::clone(transport);
        let close_state = Arc::clone(close_state);
        let close_notify = Arc::clone(close_notify);
        tokio::spawn(async move {
            let mut finalizer = TransportCloseFinalizer {
                state: close_state,
                notify: close_notify,
                completed: false,
            };
            let result = transport.close().await.map_err(normalize_transport_error);
            finalizer.complete(result);
        });
    }

    loop {
        let notified = close_notify.notified();
        if let TransportCloseState::Closed(result) = &*close_state.lock() {
            return result.clone();
        }
        notified.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn close_client_gracefully_once<T, C>(
    transport: &Arc<T>,
    pending: &PendingRequests<C>,
    stream_manager: &Arc<StreamManager<C>>,
    admission: &Arc<RwLock<()>>,
    stop_tx: &watch::Sender<bool>,
    transport_close_state: &Arc<Mutex<TransportCloseState>>,
    transport_close_notify: &Arc<Notify>,
    client_close_state: &Arc<Mutex<TransportCloseState>>,
    client_close_notify: &Arc<Notify>,
    error: RpcError,
) -> Result<()>
where
    T: MessageChannel<C> + 'static,
    C: Codec + 'static,
{
    let should_start = {
        let mut state = client_close_state.lock();
        match &*state {
            TransportCloseState::Open => {
                *state = TransportCloseState::Closing;
                true
            }
            TransportCloseState::Closing => false,
            TransportCloseState::Closed(result) => return result.clone(),
        }
    };

    if should_start {
        let transport = Arc::clone(transport);
        let pending = Arc::clone(pending);
        let stream_manager = Arc::clone(stream_manager);
        let admission = Arc::clone(admission);
        let stop_tx = stop_tx.clone();
        let transport_close_state = Arc::clone(transport_close_state);
        let transport_close_notify = Arc::clone(transport_close_notify);
        let client_close_state = Arc::clone(client_close_state);
        let client_close_notify = Arc::clone(client_close_notify);
        tokio::spawn(async move {
            let mut finalizer = ClientCloseFinalizer {
                state: client_close_state,
                notify: client_close_notify,
                completed: false,
            };
            let admission = admission.write().await;
            stop_tx.send_replace(true);
            close_client_work(&pending, &stream_manager, error);
            drop(admission);

            let result =
                close_transport_once(&transport, &transport_close_state, &transport_close_notify)
                    .await;
            finalizer.complete(result);
        });
    }

    loop {
        let notified = client_close_notify.notified();
        if let TransportCloseState::Closed(result) = &*client_close_state.lock() {
            return result.clone();
        }
        notified.await;
    }
}

/// RPC client for making remote procedure calls.
pub struct RpcClient<T, C: Codec = BincodeCodec>
where
    T: MessageChannel<C>,
{
    transport: Arc<T>,
    pending: PendingRequests<C>,
    stream_manager: Arc<StreamManager<C>>,
    codec: C,
    state: Arc<Mutex<ClientState>>,
    admission: Arc<RwLock<()>>,
    send_cancel_tx: watch::Sender<Option<RpcError>>,
    stop_tx: watch::Sender<bool>,
    receive_abort: ReceiveTaskAbort,
    transport_close_state: Arc<Mutex<TransportCloseState>>,
    transport_close_notify: Arc<Notify>,
    client_close_state: Arc<Mutex<TransportCloseState>>,
    client_close_notify: Arc<Notify>,
    default_timeout: Duration,
}

impl<T, C> Drop for RpcClient<T, C>
where
    T: MessageChannel<C>,
    C: Codec,
{
    fn drop(&mut self) {
        let (error, _) = mark_client_terminal(&self.state, RpcError::ConnectionClosed);
        self.send_cancel_tx.send_replace(Some(error.clone()));
        self.stop_tx.send_replace(true);
        if let Some(abort_handle) = self.receive_abort.lock().take() {
            abort_handle.abort();
        }
        close_client_work(&self.pending, &self.stream_manager, error);
    }
}

impl<T: MessageChannel<BincodeCodec> + 'static> RpcClient<T, BincodeCodec> {
    pub fn new(transport: T) -> Self {
        Self::with_timeout(transport, Duration::from_secs(30))
    }

    pub fn with_timeout(transport: T, default_timeout: Duration) -> Self {
        let (send_cancel_tx, _) = watch::channel(None::<RpcError>);
        let (stop_tx, _) = watch::channel(false);
        Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            stream_manager: Arc::new(StreamManager::new()),
            codec: BincodeCodec,
            state: Arc::new(Mutex::new(ClientState::default())),
            admission: Arc::new(RwLock::new(())),
            send_cancel_tx,
            stop_tx,
            receive_abort: Arc::new(Mutex::new(None)),
            transport_close_state: Arc::new(Mutex::new(TransportCloseState::default())),
            transport_close_notify: Arc::new(Notify::new()),
            client_close_state: Arc::new(Mutex::new(TransportCloseState::default())),
            client_close_notify: Arc::new(Notify::new()),
            default_timeout,
        }
    }
}

impl<T, C> RpcClient<T, C>
where
    T: MessageChannel<C> + 'static,
    C: Codec + Clone + Default + 'static,
{
    pub fn with_codec(transport: T, codec: C) -> Self {
        let (send_cancel_tx, _) = watch::channel(None::<RpcError>);
        let (stop_tx, _) = watch::channel(false);
        Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            stream_manager: Arc::new(StreamManager::with_codec(codec.clone())),
            codec,
            state: Arc::new(Mutex::new(ClientState::default())),
            admission: Arc::new(RwLock::new(())),
            send_cancel_tx,
            stop_tx,
            receive_abort: Arc::new(Mutex::new(None)),
            transport_close_state: Arc::new(Mutex::new(TransportCloseState::default())),
            transport_close_notify: Arc::new(Notify::new()),
            client_close_state: Arc::new(Mutex::new(TransportCloseState::default())),
            client_close_notify: Arc::new(Notify::new()),
            default_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_codec_and_timeout(transport: T, codec: C, default_timeout: Duration) -> Self {
        let (send_cancel_tx, _) = watch::channel(None::<RpcError>);
        let (stop_tx, _) = watch::channel(false);
        Self {
            transport: Arc::new(transport),
            pending: Arc::new(Mutex::new(HashMap::new())),
            stream_manager: Arc::new(StreamManager::with_codec(codec.clone())),
            codec,
            state: Arc::new(Mutex::new(ClientState::default())),
            admission: Arc::new(RwLock::new(())),
            send_cancel_tx,
            stop_tx,
            receive_abort: Arc::new(Mutex::new(None)),
            transport_close_state: Arc::new(Mutex::new(TransportCloseState::default())),
            transport_close_notify: Arc::new(Notify::new()),
            client_close_state: Arc::new(Mutex::new(TransportCloseState::default())),
            client_close_notify: Arc::new(Notify::new()),
            default_timeout,
        }
    }

    /// Start the client's background receive loop.
    pub fn start(&self) -> RpcClientHandle {
        self.try_start().expect("RPC client cannot be started")
    }

    /// Start the background receive loop and reject duplicate or post-close startup.
    pub fn try_start(&self) -> Result<RpcClientHandle> {
        {
            let mut state = self.state.lock();
            if state.closed {
                return Err(RpcError::ConnectionClosed);
            }
            if state.started {
                return Err(RpcError::ClientError(
                    "RPC client has already been started".to_string(),
                ));
            }
            state.started = true;
            state.running = true;
        }

        let transport = self.transport.clone();
        let pending = self.pending.clone();
        let stream_manager = self.stream_manager.clone();
        let state = self.state.clone();
        let admission = self.admission.clone();
        let codec = self.codec.clone();
        let send_cancel_tx = self.send_cancel_tx.clone();
        let mut stop_rx = self.stop_tx.subscribe();
        let receive_abort = self.receive_abort.clone();
        let transport_close_state = self.transport_close_state.clone();
        let transport_close_notify = self.transport_close_notify.clone();
        let (task_ready_tx, task_ready_rx) = oneshot::channel();
        let finalizer = ClientTaskFinalizer {
            state: state.clone(),
            pending: pending.clone(),
            stream_manager: stream_manager.clone(),
            send_cancel_tx: send_cancel_tx.clone(),
            receive_abort: receive_abort.clone(),
            cleanup_work: true,
        };

        let handle = tokio::spawn(async move {
            let mut finalizer = finalizer;
            if task_ready_rx.await.is_err() {
                return;
            }
            loop {
                tokio::select! {
                    stop_result = stop_rx.changed() => {
                        if stop_result.is_err() || *stop_rx.borrow() {
                            finalizer.cleanup_work = false;
                            break;
                        }
                    }
                    receive_result = transport.recv() => match receive_result {
                        Ok(message) => match message.msg_type {
                            MessageType::Reply => {
                                if let Some(tx) = pending.lock().remove(&message.id) {
                                    let _ = tx.send(Ok(message));
                                }
                            }
                            MessageType::Error => {
                                if let Some(stream_id) = message.metadata.stream_id {
                                    let error_msg: String = codec
                                        .decode(&message.payload)
                                        .unwrap_or_else(|_| "Unknown error".to_string());
                                    stream_manager.send_error(stream_id, error_msg);
                                } else if let Some(tx) = pending.lock().remove(&message.id) {
                                    let _ = tx.send(Ok(message));
                                }
                            }
                            MessageType::StreamChunk | MessageType::StreamEnd => {
                                stream_manager.handle_message(&message);
                            }
                            _ => {}
                        },
                        Err(error) => {
                            if matches!(&error, TransportError::Timeout { .. })
                                && transport.is_connected()
                            {
                                continue;
                            }

                            let error = normalize_transport_error(error);
                            let (error, _) = mark_client_terminal(&state, error);
                            send_cancel_tx.send_replace(Some(error.clone()));
                            close_client_work(&pending, &stream_manager, error);
                            finalizer.cleanup_work = false;
                            {
                                let _admission = admission.write().await;
                            }
                            let _ = close_transport_once(
                                &transport,
                                &transport_close_state,
                                &transport_close_notify,
                            ).await;
                            break;
                        },
                    }
                }
            }
        });

        *receive_abort.lock() = Some(handle.abort_handle());
        let _ = task_ready_tx.send(());

        Ok(RpcClientHandle { handle })
    }

    fn ensure_running(state: &ClientState) -> Result<()> {
        if !state.started && !state.closed {
            return Err(RpcError::ClientError(
                "RPC client has not been started".to_string(),
            ));
        }
        if state.closed || !state.running {
            return Err(state
                .terminal_error
                .clone()
                .unwrap_or(RpcError::ConnectionClosed));
        }
        Ok(())
    }

    async fn send_until_committed(&self, message: &Message<C>) -> Result<()> {
        let mut cancel_rx = self.send_cancel_tx.subscribe();
        if let Some(error) = cancel_rx.borrow().clone() {
            return Err(error);
        }

        tokio::select! {
            biased;
            result = self.transport.send(message) => match result {
                Ok(()) => Ok(()),
                Err(error) => {
                    let terminal = is_terminal_outbound_error(
                        &error,
                        self.transport.is_connected(),
                    );
                    let error = normalize_transport_error(error);
                    if terminal {
                        Err(self.handle_terminal_outbound_error(error))
                    } else {
                        Err(error)
                    }
                }
            },
            cancel_result = cancel_rx.changed() => {
                let _ = cancel_result;
                Err(cancel_rx
                    .borrow()
                    .clone()
                    .unwrap_or(RpcError::ConnectionClosed))
            }
        }
    }

    fn handle_terminal_outbound_error(&self, error: RpcError) -> RpcError {
        let (error, claimed) = mark_client_terminal(&self.state, error);
        if !claimed {
            return error;
        }

        self.send_cancel_tx.send_replace(Some(error.clone()));
        close_client_work(&self.pending, &self.stream_manager, error.clone());
        self.stop_tx.send_replace(true);

        let admission = Arc::clone(&self.admission);
        let transport = Arc::clone(&self.transport);
        let close_state = Arc::clone(&self.transport_close_state);
        let close_notify = Arc::clone(&self.transport_close_notify);
        tokio::spawn(async move {
            {
                let _admission = admission.write().await;
            }
            let _ = close_transport_once(&transport, &close_state, &close_notify).await;
        });

        error
    }

    pub fn transport(&self) -> Arc<T> {
        self.transport.clone()
    }

    pub fn stream_manager(&self) -> Arc<StreamManager<C>> {
        self.stream_manager.clone()
    }

    async fn wait_for_response(
        &self,
        msg_id: MessageId,
        rx: oneshot::Receiver<Result<Message<C>>>,
        timeout: CallTimeout,
    ) -> Result<Message<C>> {
        let timeout = match timeout {
            CallTimeout::ClientDefault => Some(self.default_timeout),
            CallTimeout::After(timeout) => Some(timeout),
            CallTimeout::Disabled => None,
        };

        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| {
                    RpcError::Timeout(format!("Request {} timed out after {:?}", msg_id, timeout))
                })?
                .map_err(|_| RpcError::ConnectionClosed)?,
            None => {
                let mut cancel_rx = self.send_cancel_tx.subscribe();
                if let Some(error) = cancel_rx.borrow().clone() {
                    return Err(error);
                }

                tokio::select! {
                    biased;
                    response = rx => response.map_err(|_| RpcError::ConnectionClosed)?,
                    changed = cancel_rx.changed() => {
                        let _ = changed;
                        Err(cancel_rx
                            .borrow()
                            .clone()
                            .unwrap_or(RpcError::ConnectionClosed))
                    }
                }
            }
        }
    }

    /// Make a typed RPC call.
    pub async fn call<Req, Resp>(&self, method: &str, request: &Req) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        self.call_with_options(method, request, CallOptions::default())
            .await
    }

    /// Make a typed RPC call with custom timeout.
    pub async fn call_with_timeout<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
        timeout: Duration,
    ) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        self.call_with_options(method, request, CallOptions::with_timeout(timeout))
            .await
    }

    /// Make a typed RPC call with explicit per-call options.
    ///
    /// [`CallOptions::without_timeout`] disables only the application response deadline. The
    /// local wait still ends on explicit client close or terminal connection failure. Dropping
    /// the call future removes its local pending registration but does not cancel server work.
    pub async fn call_with_options<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
        options: CallOptions,
    ) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        let _admission = self.admission.read().await;
        {
            let state = self.state.lock();
            Self::ensure_running(&state)?;
        }

        let message = Message::call_with_codec(method, request, self.codec.clone())?;
        let msg_id = message.id;
        let (tx, rx) = oneshot::channel();
        let _registration = PendingRequestRegistration::new(&self.pending, msg_id, tx);

        self.send_until_committed(&message).await?;

        let response = self.wait_for_response(msg_id, rx, options.timeout).await?;

        match response.msg_type {
            MessageType::Reply => self.codec.decode(&response.payload),
            MessageType::Error => {
                let error_msg: String = self
                    .codec
                    .decode(&response.payload)
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(RpcError::ServerError(error_msg))
            }
            _ => Err(RpcError::InvalidMessage(format!(
                "Unexpected message type: {:?}",
                response.msg_type
            ))),
        }
    }

    /// Initiate a server streaming RPC call.
    pub async fn call_server_stream<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<StreamReceiver<Resp, C>>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        self.call_server_stream_with_id(method, request, next_stream_id())
            .await
    }

    pub(crate) async fn call_server_stream_with_id<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
        stream_id: StreamId,
    ) -> Result<StreamReceiver<Resp, C>>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        let _admission = self.admission.read().await;
        {
            let state = self.state.lock();
            Self::ensure_running(&state)?;
        }

        let mut message = Message::call_with_codec(method, request, self.codec.clone())?;
        message.metadata = message.metadata.with_stream(stream_id, 0);
        let receiver = self.stream_manager.create_receiver::<Resp>(stream_id);

        self.send_until_committed(&message).await?;

        Ok(receiver)
    }

    /// Send a one-way notification (no response expected).
    pub async fn notify<Req: Serialize>(&self, method: &str, request: &Req) -> Result<()> {
        let _admission = self.admission.read().await;
        {
            let state = self.state.lock();
            Self::ensure_running(&state)?;
        }
        let message = Message::notification_with_codec(method, request, self.codec.clone())?;

        self.send_until_committed(&message).await
    }

    /// Make a raw bytes RPC call.
    pub async fn call_raw(&self, method: &str, payload: Vec<u8>) -> Result<Vec<u8>> {
        self.call_raw_with_options(method, payload, CallOptions::default())
            .await
    }

    /// Make a raw bytes RPC call with custom timeout.
    pub async fn call_raw_with_timeout(
        &self,
        method: &str,
        payload: Vec<u8>,
        timeout: Duration,
    ) -> Result<Vec<u8>> {
        self.call_raw_with_options(method, payload, CallOptions::with_timeout(timeout))
            .await
    }

    /// Make a raw bytes RPC call with explicit per-call options.
    ///
    /// [`CallOptions::without_timeout`] disables only the application response deadline. The
    /// local wait remains interruptible by explicit client close, terminal connection failure,
    /// and future cancellation. These local events do not cancel server work.
    pub async fn call_raw_with_options(
        &self,
        method: &str,
        payload: Vec<u8>,
        options: CallOptions,
    ) -> Result<Vec<u8>> {
        let _admission = self.admission.read().await;
        {
            let state = self.state.lock();
            Self::ensure_running(&state)?;
        }

        let message = Message::new_with_codec(
            MessageId::new(),
            MessageType::Call,
            method,
            payload.into(),
            Default::default(),
            self.codec.clone(),
        );
        let msg_id = message.id;
        let (tx, rx) = oneshot::channel();
        let _registration = PendingRequestRegistration::new(&self.pending, msg_id, tx);

        self.send_until_committed(&message).await?;

        let response = self.wait_for_response(msg_id, rx, options.timeout).await?;

        match response.msg_type {
            MessageType::Reply => Ok(response.payload.to_vec()),
            MessageType::Error => {
                let error_msg: String = self
                    .codec
                    .decode(&response.payload)
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(RpcError::ServerError(error_msg))
            }
            _ => Err(RpcError::InvalidMessage(format!(
                "Unexpected message type: {:?}",
                response.msg_type
            ))),
        }
    }

    pub fn is_connected(&self) -> bool {
        let state = self.state.lock();
        state.running && !state.closed && self.transport.is_connected()
    }

    pub fn is_started(&self) -> bool {
        self.state.lock().started
    }

    pub fn is_running(&self) -> bool {
        self.state.lock().running
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    pub fn active_streams(&self) -> usize {
        self.stream_manager.active_stream_count()
    }

    pub async fn close(&self) -> Result<()> {
        let (error, _) = mark_client_terminal(&self.state, RpcError::ConnectionClosed);
        self.send_cancel_tx.send_replace(Some(error.clone()));

        close_client_gracefully_once(
            &self.transport,
            &self.pending,
            &self.stream_manager,
            &self.admission,
            &self.stop_tx,
            &self.transport_close_state,
            &self.transport_close_notify,
            &self.client_close_state,
            &self.client_close_notify,
            error,
        )
        .await
    }
}

impl<T, C> Debug for RpcClient<T, C>
where
    T: MessageChannel<C>,
    C: Codec + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("RpcClient")
            .field("closed", &state.closed)
            .field("started", &state.started)
            .field("running", &state.running)
            .field("pending_requests", &self.pending.lock().len())
            .field("active_streams", &self.stream_manager.active_stream_count())
            .finish()
    }
}

/// Handle for the client's background task.
pub struct RpcClientHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl RpcClientHandle {
    /// Wait for the receive task to finish without aborting it.
    pub async fn join(self) -> std::result::Result<(), tokio::task::JoinError> {
        self.handle.await
    }

    /// Abort the receive task when graceful client close is not possible.
    pub async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::message::MessageChannelAdapter;
    use crate::codec::JsonCodec;
    use crate::error::TransportResult;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct AddRequest {
        a: i32,
        b: i32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct AddResponse {
        result: i32,
    }

    #[derive(Debug)]
    struct LifecycleTestChannel {
        block_close: bool,
        block_send: bool,
        close_count: AtomicUsize,
        close_release: Notify,
        close_started: Notify,
        fail_send: bool,
        fail_close: bool,
        panic_close: bool,
        receive_error: TransportError,
        receive_failed: Notify,
        send_release: Notify,
        sent: Notify,
    }

    impl LifecycleTestChannel {
        fn new(fail_send: bool, fail_close: bool) -> Self {
            Self {
                block_close: false,
                block_send: false,
                close_count: AtomicUsize::new(0),
                close_release: Notify::new(),
                close_started: Notify::new(),
                fail_send,
                fail_close,
                panic_close: false,
                receive_error: TransportError::ConnectionClosed,
                receive_failed: Notify::new(),
                send_release: Notify::new(),
                sent: Notify::new(),
            }
        }

        fn with_blocked_send() -> Self {
            Self {
                block_send: true,
                ..Self::new(false, false)
            }
        }

        fn with_blocked_close() -> Self {
            Self {
                block_close: true,
                ..Self::new(false, false)
            }
        }

        fn with_blocked_failing_close() -> Self {
            Self {
                block_close: true,
                ..Self::new(false, true)
            }
        }

        fn with_receive_error(receive_error: TransportError) -> Self {
            Self {
                receive_error,
                ..Self::new(false, false)
            }
        }

        fn with_panicking_close() -> Self {
            Self {
                panic_close: true,
                ..Self::new(false, false)
            }
        }
    }

    #[async_trait::async_trait]
    impl MessageChannel for LifecycleTestChannel {
        async fn send(&self, _message: &Message) -> TransportResult<()> {
            self.sent.notify_one();
            if self.block_send {
                self.send_release.notified().await;
                return Err(TransportError::ConnectionClosed);
            }
            if self.fail_send {
                return Err(TransportError::ConnectionClosed);
            }
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            self.receive_failed.notified().await;
            Err(self.receive_error.clone())
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            self.close_started.notify_one();
            if self.panic_close {
                panic!("test transport close panic");
            }
            if self.block_close {
                self.close_release.notified().await;
            }
            self.send_release.notify_one();
            if self.fail_close {
                return Err(TransportError::ConnectionClosed);
            }
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct TimeoutThenReplyChannel {
        message_id: Mutex<Option<MessageId>>,
        message_sent: Notify,
        receive_count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl MessageChannel for TimeoutThenReplyChannel {
        async fn send(&self, message: &Message) -> TransportResult<()> {
            *self.message_id.lock() = Some(message.id);
            self.message_sent.notify_one();
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            let receive_count = self.receive_count.fetch_add(1, Ordering::SeqCst);
            if receive_count == 0 {
                return Err(TransportError::Timeout {
                    duration_ms: 10,
                    operation: "receive".to_string(),
                });
            }
            if receive_count > 1 {
                return std::future::pending().await;
            }

            loop {
                let notified = self.message_sent.notified();
                if let Some(message_id) = *self.message_id.lock() {
                    return Message::reply(message_id, AddResponse { result: 42 })
                        .map_err(|error| TransportError::Protocol(error.to_string()));
                }
                notified.await;
            }
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct CommitTestChannel {
        close_count: AtomicUsize,
        committed: Notify,
        message_id: Mutex<Option<MessageId>>,
        reply_release: Notify,
        send_release: Notify,
        send_started: Notify,
    }

    #[async_trait::async_trait]
    impl MessageChannel for CommitTestChannel {
        async fn send(&self, message: &Message) -> TransportResult<()> {
            self.send_started.notify_one();
            self.send_release.notified().await;
            *self.message_id.lock() = Some(message.id);
            self.committed.notify_one();
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            self.reply_release.notified().await;
            let message_id = self
                .message_id
                .lock()
                .expect("send must commit before reply release");
            Message::reply(message_id, AddResponse { result: 3 })
                .map_err(|error| TransportError::Protocol(error.to_string()))
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TerminalSendChannel {
        close_count: AtomicUsize,
        close_started: Notify,
        connected: AtomicBool,
        fail_send: AtomicBool,
        sent: Notify,
    }

    impl Default for TerminalSendChannel {
        fn default() -> Self {
            Self {
                close_count: AtomicUsize::new(0),
                close_started: Notify::new(),
                connected: AtomicBool::new(true),
                fail_send: AtomicBool::new(false),
                sent: Notify::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl MessageChannel for TerminalSendChannel {
        async fn send(&self, _message: &Message) -> TransportResult<()> {
            if self.fail_send.load(Ordering::SeqCst) {
                return Err(TransportError::NotConnected);
            }
            self.sent.notify_one();
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }

        async fn close(&self) -> TransportResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            self.close_started.notify_one();
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ConnectedTimeoutChannel {
        close_count: AtomicUsize,
        fail_next: AtomicBool,
    }

    impl Default for ConnectedTimeoutChannel {
        fn default() -> Self {
            Self {
                close_count: AtomicUsize::new(0),
                fail_next: AtomicBool::new(true),
            }
        }
    }

    #[async_trait::async_trait]
    impl MessageChannel for ConnectedTimeoutChannel {
        async fn send(&self, _message: &Message) -> TransportResult<()> {
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(TransportError::Timeout {
                    duration_ms: 10,
                    operation: "send".to_string(),
                });
            }
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct DelayedReplyChannel {
        connected: AtomicBool,
        delay: Duration,
        raw: bool,
        requests: Mutex<VecDeque<(MessageId, Vec<u8>)>>,
        request_notify: Notify,
    }

    impl DelayedReplyChannel {
        fn typed(delay: Duration) -> Self {
            Self::new(delay, false)
        }

        fn raw(delay: Duration) -> Self {
            Self::new(delay, true)
        }

        fn new(delay: Duration, raw: bool) -> Self {
            Self {
                connected: AtomicBool::new(true),
                delay,
                raw,
                requests: Mutex::new(VecDeque::new()),
                request_notify: Notify::new(),
            }
        }

        async fn next_request(&self) -> (MessageId, Vec<u8>) {
            loop {
                let notified = self.request_notify.notified();
                if let Some(request) = self.requests.lock().pop_front() {
                    return request;
                }
                notified.await;
            }
        }
    }

    #[async_trait::async_trait]
    impl MessageChannel for DelayedReplyChannel {
        async fn send(&self, message: &Message) -> TransportResult<()> {
            self.requests
                .lock()
                .push_back((message.id, message.payload.to_vec()));
            self.request_notify.notify_one();
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message> {
            let (message_id, payload) = self.next_request().await;
            tokio::time::sleep(self.delay).await;
            if self.raw {
                Ok(Message::new(
                    message_id,
                    MessageType::Reply,
                    "",
                    payload.into(),
                    Default::default(),
                ))
            } else {
                Message::reply(message_id, AddResponse { result: 42 })
                    .map_err(|error| TransportError::Protocol(error.to_string()))
            }
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::Acquire)
        }

        async fn close(&self) -> TransportResult<()> {
            self.connected.store(false, Ordering::Release);
            self.request_notify.notify_waiters();
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct StatefulCodec {
        tag: Arc<str>,
    }

    impl StatefulCodec {
        fn configured() -> Self {
            Self {
                tag: Arc::from("configured"),
            }
        }
    }

    impl Default for StatefulCodec {
        fn default() -> Self {
            Self {
                tag: Arc::from("default"),
            }
        }
    }

    impl Codec for StatefulCodec {
        fn encode<T: Serialize>(&self, data: &T) -> Result<Vec<u8>> {
            let mut payload = self.tag.as_bytes().to_vec();
            payload.push(b':');
            payload.extend(serde_json::to_vec(data)?);
            Ok(payload)
        }

        fn decode<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
            let mut prefix = self.tag.as_bytes().to_vec();
            prefix.push(b':');
            let payload = data.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                RpcError::Serialization(format!("missing {} codec prefix", self.tag))
            })?;
            Ok(serde_json::from_slice(payload)?)
        }
    }

    #[tokio::test]
    async fn test_call_options_default_and_explicit_timeout_resolution() {
        let default_client = RpcClient::with_timeout(
            DelayedReplyChannel::typed(Duration::from_millis(75)),
            Duration::from_millis(20),
        );
        let default_handle = default_client.start();
        let result: Result<AddResponse> = default_client
            .call_with_options("add", &AddRequest { a: 20, b: 22 }, CallOptions::default())
            .await;
        assert!(matches!(result, Err(RpcError::Timeout(_))));
        default_client.close().await.unwrap();
        default_handle.join().await.unwrap();

        let override_client = RpcClient::with_timeout(
            DelayedReplyChannel::typed(Duration::from_millis(75)),
            Duration::from_millis(20),
        );
        let override_handle = override_client.start();
        let response: AddResponse = override_client
            .call_with_options(
                "add",
                &AddRequest { a: 20, b: 22 },
                CallOptions::with_timeout(Duration::from_millis(250)),
            )
            .await
            .unwrap();
        assert_eq!(response.result, 42);
        override_client.close().await.unwrap();
        override_handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_deadline_free_typed_call_exceeds_client_default_timeout() {
        let client = RpcClient::with_timeout(
            DelayedReplyChannel::typed(Duration::from_millis(75)),
            Duration::from_millis(20),
        );
        let handle = client.start();

        let response: AddResponse = client
            .call_with_options(
                "add",
                &AddRequest { a: 20, b: 22 },
                CallOptions::without_timeout(),
            )
            .await
            .unwrap();
        assert_eq!(response.result, 42);

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_deadline_free_raw_call_exceeds_client_default_timeout() {
        let client = RpcClient::with_timeout(
            DelayedReplyChannel::raw(Duration::from_millis(75)),
            Duration::from_millis(20),
        );
        let handle = client.start();
        let payload = vec![1, 2, 3, 4];

        let response = client
            .call_raw_with_options("raw", payload.clone(), CallOptions::without_timeout())
            .await
            .unwrap();
        assert_eq!(response, payload);

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_close_unblocks_deadline_free_typed_call() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call_with_options::<_, AddResponse>(
                    "add",
                    &AddRequest { a: 1, b: 2 },
                    CallOptions::without_timeout(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .expect("client close was blocked by a deadline-free typed call")
            .unwrap();
        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert!(client.pending.lock().is_empty());
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_close_unblocks_deadline_free_raw_call() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call_raw_with_options("raw", vec![1, 2, 3], CallOptions::without_timeout())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .expect("client close was blocked by a deadline-free raw call")
            .unwrap();
        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert!(client.pending.lock().is_empty());
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_terminal_failure_unblocks_deadline_free_typed_call() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_receive_error(
            TransportError::Protocol("terminal typed".to_string()),
        )));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call_with_options::<_, AddResponse>(
                    "add",
                    &AddRequest { a: 1, b: 2 },
                    CallOptions::without_timeout(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        transport.receive_failed.notify_one();

        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::Transport(TransportError::Protocol(message)))
                if message == "terminal typed"
        ));
        assert!(client.pending.lock().is_empty());
        handle.join().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_terminal_failure_unblocks_deadline_free_raw_call() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_receive_error(
            TransportError::Protocol("terminal raw".to_string()),
        )));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call_raw_with_options("raw", vec![1, 2, 3], CallOptions::without_timeout())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        transport.receive_failed.notify_one();

        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::Transport(TransportError::Protocol(message)))
                if message == "terminal raw"
        ));
        assert!(client.pending.lock().is_empty());
        handle.join().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_deadline_free_typed_call_removes_pending_registration() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call_with_options::<_, AddResponse>(
                    "add",
                    &AddRequest { a: 1, b: 2 },
                    CallOptions::without_timeout(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(client.pending.lock().is_empty());

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_deadline_free_raw_call_removes_pending_registration() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call_raw_with_options("raw", vec![1, 2, 3], CallOptions::without_timeout())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(client.pending.lock().is_empty());

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[derive(Debug)]
    struct CapturedMessage {
        codec_tag: Arc<str>,
        method: String,
        payload: Vec<u8>,
        stream_id: Option<StreamId>,
    }

    #[derive(Debug)]
    struct ConfiguredCodecChannel {
        captured: Mutex<Vec<CapturedMessage>>,
        codec: StatefulCodec,
        replies: Mutex<VecDeque<(MessageId, String)>>,
        reply_ready: Notify,
    }

    impl ConfiguredCodecChannel {
        fn new(codec: StatefulCodec) -> Self {
            Self {
                captured: Mutex::new(Vec::new()),
                codec,
                replies: Mutex::new(VecDeque::new()),
                reply_ready: Notify::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl MessageChannel<StatefulCodec> for ConfiguredCodecChannel {
        async fn send(&self, message: &Message<StatefulCodec>) -> TransportResult<()> {
            self.captured.lock().push(CapturedMessage {
                codec_tag: Arc::clone(&message.codec.tag),
                method: message.method.clone(),
                payload: message.payload.to_vec(),
                stream_id: message.metadata.stream_id,
            });
            if message.metadata.stream_id.is_none()
                && matches!(message.method.as_str(), "typed" | "raw")
            {
                self.replies
                    .lock()
                    .push_back((message.id, message.method.clone()));
                self.reply_ready.notify_one();
            }
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message<StatefulCodec>> {
            loop {
                let notified = self.reply_ready.notified();
                let reply = self.replies.lock().pop_front();
                if let Some((id, method)) = reply {
                    let payload = match method.as_str() {
                        "typed" => self
                            .codec
                            .encode(&AddResponse { result: 3 })
                            .map_err(|error| TransportError::Protocol(error.to_string()))?,
                        "raw" => vec![9, 8, 7],
                        _ => unreachable!(),
                    };
                    return Ok(Message::new_with_codec(
                        id,
                        MessageType::Reply,
                        "",
                        payload.into(),
                        Default::default(),
                        self.codec.clone(),
                    ));
                }
                notified.await;
            }
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_client_call_reply() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = MessageChannelAdapter::new(t2);

        let client = RpcClient::new(client_channel);
        let handle = client.start();

        let server_handle = tokio::spawn(async move {
            let msg = server_channel.recv().await.unwrap();
            assert_eq!(msg.method, "add");

            let req: AddRequest = msg.deserialize_payload().unwrap();
            let resp = AddResponse {
                result: req.a + req.b,
            };

            let reply: Message = Message::reply(msg.id, resp).unwrap();
            server_channel.send(&reply).await.unwrap();
        });

        let response: AddResponse = client
            .call("add", &AddRequest { a: 10, b: 32 })
            .await
            .unwrap();
        assert_eq!(response.result, 42);

        server_handle.await.unwrap();
        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_configured_codec_is_retained_for_all_outbound_messages() {
        let codec = StatefulCodec::configured();
        let client = RpcClient::with_codec(ConfiguredCodecChannel::new(codec.clone()), codec);
        let transport = client.transport();
        let handle = client.start();

        let response: AddResponse = client
            .call("typed", &AddRequest { a: 1, b: 2 })
            .await
            .unwrap();
        assert_eq!(response.result, 3);
        let stream: StreamReceiver<i32, StatefulCodec> =
            client.call_server_stream("stream", &17).await.unwrap();
        drop(stream);
        client.notify("notification", &23).await.unwrap();
        assert_eq!(
            client.call_raw("raw", vec![1, 2, 3]).await.unwrap(),
            vec![9, 8, 7]
        );

        {
            let captured = transport.captured.lock();
            assert_eq!(captured.len(), 4);
            assert_eq!(
                captured
                    .iter()
                    .map(|message| message.method.as_str())
                    .collect::<Vec<_>>(),
                vec!["typed", "stream", "notification", "raw"]
            );
            assert!(
                captured
                    .iter()
                    .all(|message| message.codec_tag.as_ref() == "configured")
            );
            assert!(
                captured[..3]
                    .iter()
                    .all(|message| message.payload.starts_with(b"configured:"))
            );
            assert!(captured[1].stream_id.is_some());
            assert_eq!(captured[3].payload, vec![1, 2, 3]);
        }

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_connected_receive_timeout_keeps_client_running() {
        let client = RpcClient::new(TimeoutThenReplyChannel::default());
        let handle = client.start();

        let response: AddResponse = client
            .call("add", &AddRequest { a: 10, b: 32 })
            .await
            .unwrap();

        assert_eq!(response.result, 42);
        assert!(client.is_running());
        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_committed_blocked_send_completes_before_close() {
        let client = Arc::new(RpcClient::new(CommitTestChannel::default()));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.send_started.notified())
            .await
            .unwrap();
        transport.send_release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), transport.committed.notified())
            .await
            .unwrap();

        let close_client = Arc::clone(&client);
        let mut close = tokio::spawn(async move { close_client.close().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut close)
                .await
                .is_err(),
            "close completed before the committed call received its reply"
        );

        transport.reply_release.notify_one();
        let response = call.await.unwrap().unwrap();
        assert_eq!(response.result, 3);
        close.await.unwrap().unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_server_stream() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client_channel = MessageChannelAdapter::new(t1);
        let server_channel = Arc::new(MessageChannelAdapter::new(t2));

        let client = RpcClient::new(client_channel);
        let handle = client.start();

        let server_channel_clone = server_channel.clone();
        let server_handle = tokio::spawn(async move {
            let msg = server_channel_clone.recv().await.unwrap();
            let stream_id = msg.metadata.stream_id.unwrap();

            for i in 1..=3 {
                let chunk: Message = Message::stream_chunk(stream_id, i - 1, i as i32).unwrap();
                server_channel_clone.send(&chunk).await.unwrap();
            }

            let end: Message = Message::stream_end(stream_id);
            server_channel_clone.send(&end).await.unwrap();
        });

        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("get_numbers", &()).await.unwrap();

        let mut items = Vec::new();
        while let Some(result) = stream.recv().await {
            items.push(result.unwrap());
        }

        assert_eq!(items, vec![1, 2, 3]);
        server_handle.await.unwrap();
        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_stream_uses_caller_provided_id() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client = RpcClient::new(MessageChannelAdapter::new(t1));
        let server_channel = MessageChannelAdapter::new(t2);
        let handle = client.start();
        let stream_id = 42;

        let server_handle = tokio::spawn(async move {
            server_channel
                .recv()
                .await
                .unwrap()
                .metadata
                .stream_id
                .unwrap()
        });
        let stream: StreamReceiver<i32> = client
            .call_server_stream_with_id("numbers", &(), stream_id)
            .await
            .unwrap();

        assert_eq!(stream.stream_id(), stream_id);
        assert_eq!(server_handle.await.unwrap(), stream_id);
        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_stream_error_uses_configured_codec() {
        let config = ChannelConfig::default();
        let (t1, t2) = ChannelFrameTransport::create_pair("test", config).unwrap();

        let client_channel = MessageChannelAdapter::<_, JsonCodec>::with_codec(t1);
        let server_channel = MessageChannelAdapter::<_, JsonCodec>::with_codec(t2);

        let client = RpcClient::with_codec(client_channel, JsonCodec);
        let handle = client.start();

        let server_handle = tokio::spawn(async move {
            let msg = server_channel.recv().await.unwrap();
            let stream_id = msg.metadata.stream_id.unwrap();

            let error: Message<JsonCodec> =
                Message::stream_error(msg.id, stream_id, "method not found");
            server_channel.send(&error).await.unwrap();
        });

        let mut stream: StreamReceiver<i32, JsonCodec> = client
            .call_server_stream("unknown_method", &())
            .await
            .unwrap();

        assert!(matches!(
            stream.recv().await,
            Some(Err(RpcError::ServerError(error))) if error == "method not found"
        ));

        server_handle.await.unwrap();
        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_call_before_start_fails_promptly() {
        let client = RpcClient::new(LifecycleTestChannel::new(false, false));

        let result: Result<AddResponse> = client.call("add", &AddRequest { a: 1, b: 2 }).await;

        assert!(matches!(result, Err(RpcError::ClientError(_))));
        assert!(client.pending.lock().is_empty());
        assert!(!client.is_started());
    }

    #[tokio::test]
    async fn test_duplicate_and_post_close_start_fail() {
        let client = RpcClient::new(LifecycleTestChannel::new(false, false));
        let handle = client.try_start().unwrap();

        assert!(client.is_started());
        assert!(client.is_running());
        assert!(matches!(client.try_start(), Err(RpcError::ClientError(_))));

        client.close().await.unwrap();

        assert!(client.is_closed());
        assert!(!client.is_running());
        assert!(matches!(
            client.try_start(),
            Err(RpcError::ConnectionClosed)
        ));
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_close_waits_for_committed_call_timeout() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = client.clone();
        let call_handle = tokio::spawn(async move {
            call_client
                .call_with_timeout::<_, AddResponse>(
                    "add",
                    &AddRequest { a: 1, b: 2 },
                    Duration::from_millis(50),
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        client.close().await.unwrap();

        let result = call_handle.await.unwrap();
        assert!(matches!(result, Err(RpcError::Timeout(_))));
        assert!(client.pending.lock().is_empty());
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_terminal_receive_error_is_preserved_for_pending_work() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_receive_error(
            TransportError::Protocol("invalid frame".to_string()),
        )));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("numbers", &()).await.unwrap();
        transport.receive_failed.notify_one();

        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::Transport(TransportError::Protocol(error)))
                if error == "invalid frame"
        ));
        assert!(matches!(
            stream.recv().await,
            Some(Err(RpcError::Transport(TransportError::Protocol(error))))
                if error == "invalid frame"
        ));
        assert!(client.is_closed());
        handle.join().await.unwrap();
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_active_stream_fails_when_client_closes() {
        let client = RpcClient::new(LifecycleTestChannel::new(false, false));
        let handle = client.start();
        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("numbers", &()).await.unwrap();

        assert_eq!(client.active_streams(), 1);
        client.close().await.unwrap();

        assert!(matches!(
            stream.recv().await,
            Some(Err(RpcError::ConnectionClosed))
        ));
        assert_eq!(client.active_streams(), 0);
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_terminal_outbound_send_drains_work_and_closes_once() {
        let client = Arc::new(RpcClient::new(TerminalSendChannel::default()));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("pending", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("stream", &()).await.unwrap();
        transport.connected.store(false, Ordering::SeqCst);
        transport.fail_send.store(true, Ordering::SeqCst);

        assert!(matches!(
            client.notify("fail", &()).await,
            Err(RpcError::Transport(TransportError::NotConnected))
        ));
        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::Transport(TransportError::NotConnected))
        ));
        assert!(matches!(
            stream.recv().await,
            Some(Err(RpcError::Transport(TransportError::NotConnected)))
        ));
        tokio::time::timeout(Duration::from_secs(1), transport.close_started.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .unwrap()
            .unwrap();

        assert!(client.is_closed());
        assert!(!client.is_running());
        client.close().await.unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_connected_outbound_timeout_is_not_terminal() {
        let client = RpcClient::new(ConnectedTimeoutChannel::default());
        let transport = client.transport();
        let handle = client.start();

        assert!(matches!(
            client.notify("first", &()).await,
            Err(RpcError::Transport(TransportError::Timeout { .. }))
        ));
        assert!(client.is_running());
        assert!(!client.is_closed());
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 0);
        client.notify("second", &()).await.unwrap();

        client.close().await.unwrap();
        handle.join().await.unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_stream_registration_rolls_back_when_send_fails() {
        let client = RpcClient::new(LifecycleTestChannel::new(true, false));
        let handle = client.start();

        let result: Result<StreamReceiver<i32>> = client.call_server_stream("numbers", &()).await;

        assert!(matches!(result, Err(RpcError::ConnectionClosed)));
        assert_eq!(client.active_streams(), 0);
        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_typed_call_cleans_pending_registration_during_send() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_blocked_send()));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(client.pending.lock().is_empty());

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_raw_call_cleans_pending_registration_while_waiting() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move { call_client.call_raw("raw", vec![1, 2, 3]).await });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(client.pending.lock().is_empty());

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_stream_setup_cleans_registration_during_send() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_blocked_send()));
        let transport = client.transport();
        let handle = client.start();
        let stream_client = Arc::clone(&client);
        let stream = tokio::spawn(async move {
            stream_client
                .call_server_stream::<_, i32>("numbers", &())
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        stream.abort();
        assert!(matches!(stream.await, Err(error) if error.is_cancelled()));
        assert_eq!(client.active_streams(), 0);

        client.close().await.unwrap();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_close_is_idempotent_after_transport_error() {
        let client = RpcClient::new(LifecycleTestChannel::new(false, true));
        let transport = client.transport();
        let handle = client.start();

        assert!(matches!(
            client.close().await,
            Err(RpcError::ConnectionClosed)
        ));
        assert!(matches!(
            client.close().await,
            Err(RpcError::ConnectionClosed)
        ));
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        assert!(matches!(
            client.notify("event", &()).await,
            Err(RpcError::ConnectionClosed)
        ));
        let stream_result: Result<StreamReceiver<i32>> =
            client.call_server_stream("numbers", &()).await;
        assert!(matches!(stream_result, Err(RpcError::ConnectionClosed)));
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_close_proceeds_while_outbound_send_is_blocked() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_blocked_send()));
        let transport = client.transport();
        let handle = client.start();
        let call_client = client.clone();
        let call_handle = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            call_handle.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_receive_failure_claims_close_before_transport_await() {
        let client = RpcClient::new(LifecycleTestChannel::with_blocked_close());
        let transport = client.transport();
        let handle = client.start();

        transport.receive_failed.notify_one();
        tokio::time::timeout(Duration::from_secs(1), transport.close_started.notified())
            .await
            .unwrap();

        assert!(client.is_closed());
        assert!(!client.is_running());
        let result: Result<AddResponse> = client.call("add", &AddRequest { a: 1, b: 2 }).await;
        assert!(matches!(result, Err(RpcError::ConnectionClosed)));
        let close = client.close();
        tokio::pin!(close);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut close)
                .await
                .is_err(),
            "concurrent close returned before the in-flight transport close completed"
        );
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);

        transport.close_release.notify_one();
        close.await.unwrap();
        client.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .unwrap()
            .unwrap();

        assert!(client.receive_abort.lock().is_none());
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_concurrent_close_waiters_share_transport_error() {
        let client = Arc::new(RpcClient::new(
            LifecycleTestChannel::with_blocked_failing_close(),
        ));
        let transport = client.transport();
        let handle = client.start();

        let first_client = Arc::clone(&client);
        let first = tokio::spawn(async move { first_client.close().await });
        tokio::time::timeout(Duration::from_secs(1), transport.close_started.notified())
            .await
            .unwrap();
        let second_client = Arc::clone(&client);
        let second = tokio::spawn(async move { second_client.close().await });
        tokio::task::yield_now().await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());

        transport.close_release.notify_one();
        assert!(matches!(
            first.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert!(matches!(
            second.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancelled_close_owner_does_not_strand_waiters() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_blocked_close()));
        let transport = client.transport();
        let handle = client.start();

        let first_client = Arc::clone(&client);
        let first = tokio::spawn(async move { first_client.close().await });
        tokio::time::timeout(Duration::from_secs(1), transport.close_started.notified())
            .await
            .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let second_client = Arc::clone(&client);
        let second = tokio::spawn(async move { second_client.close().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());

        transport.close_release.notify_one();
        second.await.unwrap().unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancelled_close_before_admission_continues_in_background() {
        let client = Arc::new(RpcClient::new(CommitTestChannel::default()));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.send_started.notified())
            .await
            .unwrap();
        transport.send_release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), transport.committed.notified())
            .await
            .unwrap();

        let close_client = Arc::clone(&client);
        let first_close = tokio::spawn(async move { close_client.close().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(
                &*client.client_close_state.lock(),
                TransportCloseState::Closing
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 0);

        first_close.abort();
        assert!(first_close.await.unwrap_err().is_cancelled());
        transport.reply_release.notify_one();
        assert_eq!(call.await.unwrap().unwrap().result, 3);

        tokio::time::timeout(Duration::from_secs(1), async {
            while transport.close_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        client.close().await.unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_panicking_transport_close_releases_all_waiters() {
        let client = RpcClient::new(LifecycleTestChannel::with_panicking_close());
        let transport = client.transport();
        let handle = client.start();

        let first = tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .unwrap();

        assert!(matches!(first, Err(RpcError::ClientError(error)) if error.contains("panicked")));
        assert!(matches!(second, Err(RpcError::ClientError(error)) if error.contains("panicked")));
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
        handle.join().await.unwrap();
    }

    #[test]
    fn test_transport_error_normalization() {
        assert!(matches!(
            normalize_transport_error(TransportError::ConnectionClosed),
            RpcError::ConnectionClosed
        ));
        assert!(matches!(
            normalize_transport_error(TransportError::NotConnected),
            RpcError::Transport(TransportError::NotConnected)
        ));
    }

    #[tokio::test]
    async fn test_drop_aborts_detached_task_and_drains_work() {
        let client = RpcClient::new(LifecycleTestChannel::new(false, false));
        let state = client.state.clone();
        let receive_abort = client.receive_abort.clone();
        let stream_manager = client.stream_manager();
        let (pending_tx, pending_rx) = oneshot::channel::<Result<Message>>();
        client.pending.lock().insert(MessageId::new(), pending_tx);
        let mut stream: StreamReceiver<i32> = stream_manager.create_receiver(next_stream_id());
        let handle = client.start();

        assert!(receive_abort.lock().is_some());
        drop(handle);
        drop(client);

        assert!(state.lock().closed);
        assert!(!state.lock().running);
        assert!(receive_abort.lock().is_none());
        assert!(matches!(
            pending_rx.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert!(matches!(
            stream.recv().await,
            Some(Err(RpcError::ConnectionClosed))
        ));
        assert_eq!(stream_manager.active_stream_count(), 0);
    }

    #[tokio::test]
    async fn test_handle_join_after_graceful_close() {
        let client = RpcClient::new(LifecycleTestChannel::new(false, false));
        let handle = client.start();

        client.close().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_forced_shutdown_cancels_blocked_send() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::with_blocked_send()));
        let transport = client.transport();
        let handle = client.start();
        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        handle.shutdown().await;

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), call)
                .await
                .unwrap()
                .unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_forced_shutdown_drains_pending_call() {
        let client = Arc::new(RpcClient::new(LifecycleTestChannel::new(false, false)));
        let transport = client.transport();
        let handle = client.start();
        let call_client = client.clone();
        let call_handle = tokio::spawn(async move {
            call_client
                .call::<_, AddResponse>("add", &AddRequest { a: 1, b: 2 })
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), transport.sent.notified())
            .await
            .unwrap();
        handle.shutdown().await;

        assert!(matches!(
            call_handle.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
        assert!(client.is_closed());
        assert!(!client.is_running());
        assert!(client.receive_abort.lock().is_none());
        client.close().await.unwrap();
    }
}
