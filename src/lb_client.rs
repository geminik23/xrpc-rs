use async_trait::async_trait;
use parking_lot::{Mutex as SyncMutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock as AsyncRwLock, watch};

use crate::channel::message::MessageChannel;
use crate::client::{RpcClient, RpcClientHandle};
use crate::codec::{BincodeCodec, Codec};
use crate::discovery::Endpoint;
use crate::error::{Result, RpcError, TransportError};
use crate::loadbalancer::{LoadBalanceStrategy, LoadBalancer};
use crate::streaming::{StreamId, StreamReceiver};

#[async_trait]
pub trait ClientFactory<T, C>: Send + Sync
where
    T: MessageChannel<C>,
    C: Codec,
{
    async fn create(&self, endpoint: &Endpoint) -> Result<RpcClient<T, C>>;
}

struct ManagedClient<T, C>
where
    T: MessageChannel<C>,
    C: Codec,
{
    client: Arc<RpcClient<T, C>>,
    endpoint: Endpoint,
    handle: AsyncMutex<Option<RpcClientHandle>>,
}

async fn wait_for_close(close_rx: &mut watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }
    let _ = close_rx.changed().await;
}

type OwnedStreamIds = Arc<RwLock<HashSet<StreamId>>>;

struct StreamAffinityGuard<S>
where
    S: LoadBalanceStrategy + 'static,
{
    load_balancer: Arc<LoadBalancer<S>>,
    owned_stream_ids: OwnedStreamIds,
    stream_id: StreamId,
    armed: bool,
}

impl<S> StreamAffinityGuard<S>
where
    S: LoadBalanceStrategy + 'static,
{
    fn new(
        load_balancer: Arc<LoadBalancer<S>>,
        owned_stream_ids: OwnedStreamIds,
        stream_id: StreamId,
    ) -> Self {
        owned_stream_ids.write().insert(stream_id);
        Self {
            load_balancer,
            owned_stream_ids,
            stream_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<S> Drop for StreamAffinityGuard<S>
where
    S: LoadBalanceStrategy + 'static,
{
    fn drop(&mut self) {
        if self.armed && self.owned_stream_ids.write().remove(&self.stream_id) {
            self.load_balancer.release_stream(self.stream_id);
        }
    }
}

struct ActiveRequestGuard<S>
where
    S: LoadBalanceStrategy + 'static,
{
    load_balancer: Arc<LoadBalancer<S>>,
    server_idx: usize,
}

impl<S> ActiveRequestGuard<S>
where
    S: LoadBalanceStrategy + 'static,
{
    fn new(load_balancer: Arc<LoadBalancer<S>>, server_idx: usize) -> Self {
        load_balancer.acquire(server_idx);
        Self {
            load_balancer,
            server_idx,
        }
    }
}

impl<S> Drop for ActiveRequestGuard<S>
where
    S: LoadBalanceStrategy + 'static,
{
    fn drop(&mut self) {
        self.load_balancer.release(self.server_idx);
    }
}

#[derive(Clone, Default)]
enum LoadBalancedCloseState {
    #[default]
    Open,
    Closing,
    Closed(Result<()>),
}

struct LoadBalancedCloseFinalizer {
    state: Arc<SyncMutex<LoadBalancedCloseState>>,
    notify: Arc<Notify>,
    completed: bool,
}

impl LoadBalancedCloseFinalizer {
    fn complete(&mut self, result: Result<()>) {
        *self.state.lock() = LoadBalancedCloseState::Closed(result);
        self.completed = true;
        self.notify.notify_waiters();
    }
}

impl Drop for LoadBalancedCloseFinalizer {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        *self.state.lock() = LoadBalancedCloseState::Closed(Err(RpcError::ClientError(
            "Load-balanced close task was cancelled or panicked".to_string(),
        )));
        self.notify.notify_waiters();
    }
}

pub struct LoadBalancedClient<T, C, S>
where
    T: MessageChannel<C>,
    C: Codec,
    S: LoadBalanceStrategy,
{
    load_balancer: Arc<LoadBalancer<S>>,
    clients: RwLock<HashMap<usize, Arc<ManagedClient<T, C>>>>,
    creation_locks: RwLock<HashMap<usize, Arc<AsyncMutex<()>>>>,
    owned_stream_ids: OwnedStreamIds,
    factory: Arc<dyn ClientFactory<T, C>>,
    closed: AtomicBool,
    close_cancel_tx: watch::Sender<bool>,
    admission: AsyncRwLock<()>,
    close_lock: AsyncMutex<()>,
    close_state: Arc<SyncMutex<LoadBalancedCloseState>>,
    close_notify: Arc<Notify>,
}

impl<T, C, S> LoadBalancedClient<T, C, S>
where
    T: MessageChannel<C> + 'static,
    C: Codec + Clone + Default + 'static,
    S: LoadBalanceStrategy + 'static,
{
    pub fn new(load_balancer: Arc<LoadBalancer<S>>, factory: Arc<dyn ClientFactory<T, C>>) -> Self {
        let (close_cancel_tx, _) = watch::channel(false);
        Self {
            load_balancer,
            clients: RwLock::new(HashMap::new()),
            creation_locks: RwLock::new(HashMap::new()),
            owned_stream_ids: Arc::new(RwLock::new(HashSet::new())),
            factory,
            closed: AtomicBool::new(false),
            close_cancel_tx,
            admission: AsyncRwLock::new(()),
            close_lock: AsyncMutex::new(()),
            close_state: Arc::new(SyncMutex::new(LoadBalancedCloseState::Open)),
            close_notify: Arc::new(Notify::new()),
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.ensure_open()?;
        self.load_balancer.init().await?;
        self.ensure_open()?;

        for i in 0..self.load_balancer.server_count() {
            if let Some(endpoint) = self.load_balancer.get_endpoint(i) {
                let _ = self.get_or_create_client(i, &endpoint).await;
            }
        }

        self.ensure_open()
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(RpcError::ConnectionClosed);
        }
        Ok(())
    }

    async fn close_entry(entry: &ManagedClient<T, C>) -> Result<()> {
        let close_result = entry.client.close().await;
        let handle = entry.handle.lock().await.take();
        let join_result = match handle {
            Some(handle) => handle.join().await,
            None => Ok(()),
        };

        close_result?;

        join_result.map_err(|error| {
            RpcError::ClientError(format!("RPC client task failed while joining: {error}"))
        })
    }

    fn spawn_entry_close(entry: Arc<ManagedClient<T, C>>) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move { Self::close_entry(&entry).await })
    }

    fn close_unpublished_client(client: Arc<RpcClient<T, C>>) {
        tokio::spawn(async move {
            let _ = client.close().await;
        });
    }

    fn cached_client(
        &self,
        server_idx: usize,
        endpoint: &Endpoint,
    ) -> Option<Arc<ManagedClient<T, C>>> {
        if self.load_balancer.get_endpoint(server_idx).as_ref() != Some(endpoint) {
            return None;
        }
        match self.clients.read().get(&server_idx).cloned() {
            Some(entry) if entry.endpoint == *endpoint && entry.client.is_connected() => {
                Some(entry)
            }
            _ => None,
        }
    }

    async fn get_or_create_client(
        &self,
        server_idx: usize,
        endpoint: &Endpoint,
    ) -> Result<Arc<ManagedClient<T, C>>> {
        self.ensure_open()?;
        if let Some(entry) = self.cached_client(server_idx, endpoint) {
            self.ensure_open()?;
            return Ok(entry);
        }

        let mut close_rx = self.close_cancel_tx.subscribe();
        let creation_lock = {
            let mut creation_locks = self.creation_locks.write();
            creation_locks
                .entry(server_idx)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        let _creation = tokio::select! {
            biased;
            _ = wait_for_close(&mut close_rx) => return Err(RpcError::ConnectionClosed),
            creation = creation_lock.lock() => creation,
        };
        self.ensure_open()?;

        if let Some(entry) = self.cached_client(server_idx, endpoint) {
            return Ok(entry);
        }

        let stale = {
            let current = self.clients.read().get(&server_idx).cloned();
            match current {
                Some(entry) => {
                    let mut clients = self.clients.write();
                    match clients.get(&server_idx) {
                        Some(current) if Arc::ptr_eq(current, &entry) => {
                            clients.remove(&server_idx)
                        }
                        _ => None,
                    }
                }
                None => None,
            }
        };
        if let Some(stale) = stale {
            let mut close_task = Self::spawn_entry_close(stale);
            tokio::select! {
                biased;
                _ = wait_for_close(&mut close_rx) => return Err(RpcError::ConnectionClosed),
                _ = &mut close_task => {}
            }
        }

        self.ensure_open()?;
        let client = tokio::select! {
            biased;
            result = self.factory.create(endpoint) => Arc::new(result?),
            _ = wait_for_close(&mut close_rx) => return Err(RpcError::ConnectionClosed),
        };

        if self.closed.load(Ordering::Acquire) {
            Self::close_unpublished_client(client);
            return Err(RpcError::ConnectionClosed);
        }

        let admission = tokio::select! {
            biased;
            _ = wait_for_close(&mut close_rx) => {
                Self::close_unpublished_client(client);
                return Err(RpcError::ConnectionClosed);
            }
            admission = self.admission.read() => admission,
        };
        if let Err(error) = self.ensure_open() {
            drop(admission);
            Self::close_unpublished_client(client);
            return Err(error);
        }
        if self.load_balancer.get_endpoint(server_idx).as_ref() != Some(endpoint) {
            drop(admission);
            Self::close_unpublished_client(client);
            return Err(RpcError::ClientError(
                "Server endpoint changed during client creation".to_string(),
            ));
        }

        let handle = match client.try_start() {
            Ok(handle) => handle,
            Err(error) => {
                drop(admission);
                Self::close_unpublished_client(client);
                return Err(error);
            }
        };
        let entry = Arc::new(ManagedClient {
            client,
            endpoint: endpoint.clone(),
            handle: AsyncMutex::new(Some(handle)),
        });
        let replaced = self.clients.write().insert(server_idx, Arc::clone(&entry));
        drop(admission);

        if let Some(replaced) = replaced {
            drop(Self::spawn_entry_close(replaced));
        }

        Ok(entry)
    }

    async fn remove_and_close_client(
        &self,
        server_idx: usize,
        expected: &Arc<ManagedClient<T, C>>,
    ) {
        let removed = {
            let mut clients = self.clients.write();
            match clients.get(&server_idx) {
                Some(current) if Arc::ptr_eq(current, expected) => clients.remove(&server_idx),
                _ => None,
            }
        };

        if let Some(entry) = removed {
            let _ = Self::spawn_entry_close(entry).await;
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.close_cancel_tx.send_replace(true);

        {
            let _close = self.close_lock.lock().await;
            let state = self.close_state.lock().clone();
            match state {
                LoadBalancedCloseState::Closed(result) => return result,
                LoadBalancedCloseState::Closing => {}
                LoadBalancedCloseState::Open => {
                    let _admission = self.admission.write().await;
                    let stream_ids = self.owned_stream_ids.write().drain().collect::<Vec<_>>();
                    for stream_id in stream_ids {
                        self.load_balancer.release_stream(stream_id);
                    }
                    let mut entries = self.clients.write().drain().collect::<Vec<_>>();
                    entries.sort_by_key(|(server_idx, _)| *server_idx);
                    self.creation_locks.write().clear();

                    *self.close_state.lock() = LoadBalancedCloseState::Closing;
                    let close_state = Arc::clone(&self.close_state);
                    let close_notify = Arc::clone(&self.close_notify);
                    tokio::spawn(async move {
                        let mut finalizer = LoadBalancedCloseFinalizer {
                            state: close_state,
                            notify: close_notify,
                            completed: false,
                        };
                        let close_tasks = entries
                            .into_iter()
                            .map(|(_, entry)| Self::spawn_entry_close(entry))
                            .collect::<Vec<_>>();
                        let mut first_error = None;
                        for close_task in close_tasks {
                            let result = match close_task.await {
                                Ok(result) => result,
                                Err(error) => Err(RpcError::ClientError(format!(
                                    "RPC client close task failed while joining: {error}"
                                ))),
                            };
                            if first_error.is_none() {
                                first_error = result.err();
                            }
                        }
                        let result = match first_error {
                            Some(error) => Err(error),
                            None => Ok(()),
                        };
                        finalizer.complete(result);
                    });
                }
            }
        }

        loop {
            let notified = self.close_notify.notified();
            if let LoadBalancedCloseState::Closed(result) = &*self.close_state.lock() {
                return result.clone();
            }
            notified.await;
        }
    }

    fn child_error_is_terminal(entry: &ManagedClient<T, C>, error: &RpcError) -> bool {
        !entry.client.is_connected()
            || matches!(
                error,
                RpcError::ConnectionClosed
                    | RpcError::Transport(TransportError::ConnectionClosed)
                    | RpcError::Transport(TransportError::NotConnected)
            )
    }

    pub async fn call<Req, Resp>(&self, method: &str, request: &Req) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        self.ensure_open()?;
        let config = self.load_balancer.config();
        let max_attempts = if config.failover_enabled {
            config.max_failover_attempts as usize + 1
        } else {
            1
        };
        let mut last_error = None;
        let mut tried_servers = Vec::new();

        for _ in 0..max_attempts {
            self.ensure_open()?;
            let server_idx = match self.load_balancer.select_excluding(&tried_servers) {
                Some(idx) => idx,
                None => break,
            };
            tried_servers.push(server_idx);

            let endpoint = match self.load_balancer.get_endpoint(server_idx) {
                Some(endpoint) => endpoint,
                None => {
                    self.load_balancer.record_failure(server_idx);
                    last_error = Some(RpcError::ClientError("Server not found".to_string()));
                    continue;
                }
            };

            let entry = match self.get_or_create_client(server_idx, &endpoint).await {
                Ok(entry) => entry,
                Err(error) => {
                    if self.closed.load(Ordering::Acquire) {
                        return Err(RpcError::ConnectionClosed);
                    }
                    self.load_balancer.record_failure(server_idx);
                    last_error = Some(error);
                    continue;
                }
            };

            let active_request =
                ActiveRequestGuard::new(Arc::clone(&self.load_balancer), server_idx);
            let result = entry.client.call(method, request).await;
            drop(active_request);

            match result {
                Ok(response) => {
                    self.load_balancer.record_success(server_idx);
                    return Ok(response);
                }
                Err(error) => {
                    self.load_balancer.record_failure(server_idx);
                    if Self::child_error_is_terminal(&entry, &error) {
                        self.remove_and_close_client(server_idx, &entry).await;
                    }
                    return Err(error);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| RpcError::ClientError("No servers available".to_string())))
    }

    pub async fn call_server_stream<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<StreamReceiver<Resp, C>>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        self.ensure_open()?;
        let stream_id = crate::streaming::next_stream_id();

        let server_idx = self
            .load_balancer
            .select_for_stream(stream_id)
            .ok_or_else(|| RpcError::ClientError("No servers available".to_string()))?;
        let mut affinity = StreamAffinityGuard::new(
            Arc::clone(&self.load_balancer),
            Arc::clone(&self.owned_stream_ids),
            stream_id,
        );

        let endpoint = self
            .load_balancer
            .get_endpoint(server_idx)
            .ok_or_else(|| RpcError::ClientError("Server not found".to_string()))?;

        let entry = self.get_or_create_client(server_idx, &endpoint).await?;

        match entry
            .client
            .call_server_stream_with_id(method, request, stream_id)
            .await
        {
            Ok(mut receiver) => {
                let owned_stream_ids = Arc::clone(&self.owned_stream_ids);
                let load_balancer = Arc::clone(&self.load_balancer);
                receiver.set_termination_callback(move |stream_id| {
                    if owned_stream_ids.write().remove(&stream_id) {
                        load_balancer.release_stream(stream_id);
                    }
                });
                affinity.disarm();
                Ok(receiver)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn notify<Req: Serialize>(&self, method: &str, request: &Req) -> Result<()> {
        self.ensure_open()?;
        let server_idx = self
            .load_balancer
            .select()
            .ok_or_else(|| RpcError::ClientError("No servers available".to_string()))?;

        let endpoint = self
            .load_balancer
            .get_endpoint(server_idx)
            .ok_or_else(|| RpcError::ClientError("Server not found".to_string()))?;

        let entry = self.get_or_create_client(server_idx, &endpoint).await?;
        entry.client.notify(method, request).await
    }

    pub fn load_balancer(&self) -> &LoadBalancer<S> {
        &self.load_balancer
    }

    pub fn connection_count(&self) -> usize {
        self.clients.read().len()
    }

    pub fn release_stream(&self, stream_id: crate::streaming::StreamId) {
        if self.owned_stream_ids.write().remove(&stream_id) {
            self.load_balancer.release_stream(stream_id);
        }
    }
}

impl<T, C, S> std::fmt::Debug for LoadBalancedClient<T, C, S>
where
    T: MessageChannel<C>,
    C: Codec,
    S: LoadBalanceStrategy + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadBalancedClient")
            .field("server_count", &self.load_balancer.server_count())
            .field("available_count", &self.load_balancer.available_count())
            .field("connection_count", &self.clients.read().len())
            .field("strategy", &self.load_balancer.strategy_name())
            .finish()
    }
}

pub type DefaultLoadBalancedClient<T, S> = LoadBalancedClient<T, BincodeCodec, S>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::message::MessageChannelAdapter;
    use crate::discovery::StaticDiscovery;
    use crate::error::{TransportError, TransportResult};
    use crate::loadbalancer::{LoadBalancerConfig, RoundRobin};
    use crate::message::Message;
    use crate::streaming::StreamId;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};
    use parking_lot::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    type TestChannel = MessageChannelAdapter<ChannelFrameTransport>;

    struct MockClientFactory {
        create_count: AtomicUsize,
        created_endpoints: Mutex<Vec<Endpoint>>,
        peers: Mutex<Vec<ChannelFrameTransport>>,
        create_delay: Duration,
    }

    impl MockClientFactory {
        fn new() -> Self {
            Self::with_delay(Duration::ZERO)
        }

        fn with_delay(create_delay: Duration) -> Self {
            Self {
                create_count: AtomicUsize::new(0),
                created_endpoints: Mutex::new(Vec::new()),
                peers: Mutex::new(Vec::new()),
                create_delay,
            }
        }
    }

    #[async_trait]
    impl ClientFactory<TestChannel, BincodeCodec> for MockClientFactory {
        async fn create(
            &self,
            endpoint: &Endpoint,
        ) -> Result<RpcClient<TestChannel, BincodeCodec>> {
            self.create_count.fetch_add(1, Ordering::Relaxed);
            self.created_endpoints.lock().push(endpoint.clone());
            if !self.create_delay.is_zero() {
                tokio::time::sleep(self.create_delay).await;
            }

            let (client_transport, peer_transport) =
                ChannelFrameTransport::create_pair("mock", ChannelConfig::default()).unwrap();
            self.peers.lock().push(peer_transport);
            Ok(RpcClient::new(MessageChannelAdapter::new(client_transport)))
        }
    }

    #[derive(Debug)]
    struct FailingChannel {
        closed: AtomicBool,
        send_count: Arc<AtomicUsize>,
        sent_stream_ids: Arc<Mutex<Vec<StreamId>>>,
    }

    impl FailingChannel {
        fn new(send_count: Arc<AtomicUsize>, sent_stream_ids: Arc<Mutex<Vec<StreamId>>>) -> Self {
            Self {
                closed: AtomicBool::new(false),
                send_count,
                sent_stream_ids,
            }
        }
    }

    #[async_trait]
    impl MessageChannel<BincodeCodec> for FailingChannel {
        async fn send(&self, message: &Message<BincodeCodec>) -> TransportResult<()> {
            self.send_count.fetch_add(1, Ordering::SeqCst);
            if let Some(stream_id) = message.metadata.stream_id {
                self.sent_stream_ids.lock().push(stream_id);
            }
            Err(TransportError::ConnectionClosed)
        }

        async fn recv(&self) -> TransportResult<Message<BincodeCodec>> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            !self.closed.load(Ordering::Acquire)
        }

        async fn close(&self) -> TransportResult<()> {
            self.closed.store(true, Ordering::Release);
            Err(TransportError::ConnectionClosed)
        }
    }

    struct FailingClientFactory {
        send_count: Arc<AtomicUsize>,
        sent_stream_ids: Arc<Mutex<Vec<StreamId>>>,
    }

    impl FailingClientFactory {
        fn new() -> Self {
            Self {
                send_count: Arc::new(AtomicUsize::new(0)),
                sent_stream_ids: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ClientFactory<FailingChannel, BincodeCodec> for FailingClientFactory {
        async fn create(
            &self,
            _endpoint: &Endpoint,
        ) -> Result<RpcClient<FailingChannel, BincodeCodec>> {
            Ok(RpcClient::new(FailingChannel::new(
                Arc::clone(&self.send_count),
                Arc::clone(&self.sent_stream_ids),
            )))
        }
    }

    #[derive(Debug)]
    struct BlockingCloseControl {
        close_count: AtomicUsize,
        close_release: tokio::sync::Semaphore,
        close_started: tokio::sync::Semaphore,
    }

    impl BlockingCloseControl {
        fn new() -> Self {
            Self {
                close_count: AtomicUsize::new(0),
                close_release: tokio::sync::Semaphore::new(0),
                close_started: tokio::sync::Semaphore::new(0),
            }
        }
    }

    #[derive(Debug)]
    struct BlockingCloseChannel {
        control: Arc<BlockingCloseControl>,
    }

    #[async_trait]
    impl MessageChannel<BincodeCodec> for BlockingCloseChannel {
        async fn send(&self, _message: &Message<BincodeCodec>) -> TransportResult<()> {
            Ok(())
        }

        async fn recv(&self) -> TransportResult<Message<BincodeCodec>> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            self.control.close_count.fetch_add(1, Ordering::SeqCst);
            self.control.close_started.add_permits(1);
            self.control
                .close_release
                .acquire()
                .await
                .expect("close release semaphore must remain open")
                .forget();
            Err(TransportError::NotConnected)
        }
    }

    struct BlockingCloseFactory {
        control: Arc<BlockingCloseControl>,
    }

    #[async_trait]
    impl ClientFactory<BlockingCloseChannel, BincodeCodec> for BlockingCloseFactory {
        async fn create(
            &self,
            _endpoint: &Endpoint,
        ) -> Result<RpcClient<BlockingCloseChannel, BincodeCodec>> {
            Ok(RpcClient::new(BlockingCloseChannel {
                control: Arc::clone(&self.control),
            }))
        }
    }

    struct BlockingCreateFactory {
        started: Notify,
    }

    #[async_trait]
    impl ClientFactory<TestChannel, BincodeCodec> for BlockingCreateFactory {
        async fn create(
            &self,
            _endpoint: &Endpoint,
        ) -> Result<RpcClient<TestChannel, BincodeCodec>> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    struct FailingCreateFactory {
        create_count: AtomicUsize,
    }

    #[async_trait]
    impl ClientFactory<TestChannel, BincodeCodec> for FailingCreateFactory {
        async fn create(
            &self,
            _endpoint: &Endpoint,
        ) -> Result<RpcClient<TestChannel, BincodeCodec>> {
            self.create_count.fetch_add(1, Ordering::SeqCst);
            Err(RpcError::ClientError("client creation failed".to_string()))
        }
    }

    #[derive(Debug)]
    struct BlockingSendChannel {
        send_started: Arc<Notify>,
    }

    #[async_trait]
    impl MessageChannel<BincodeCodec> for BlockingSendChannel {
        async fn send(&self, _message: &Message<BincodeCodec>) -> TransportResult<()> {
            self.send_started.notify_one();
            std::future::pending().await
        }

        async fn recv(&self) -> TransportResult<Message<BincodeCodec>> {
            std::future::pending().await
        }

        fn is_connected(&self) -> bool {
            true
        }

        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
    }

    struct BlockingSendFactory {
        send_started: Arc<Notify>,
    }

    #[async_trait]
    impl ClientFactory<BlockingSendChannel, BincodeCodec> for BlockingSendFactory {
        async fn create(
            &self,
            _endpoint: &Endpoint,
        ) -> Result<RpcClient<BlockingSendChannel, BincodeCodec>> {
            Ok(RpcClient::new(BlockingSendChannel {
                send_started: Arc::clone(&self.send_started),
            }))
        }
    }

    fn load_balancer(server_count: usize) -> Arc<LoadBalancer<RoundRobin>> {
        load_balancer_with_config(server_count, LoadBalancerConfig::default())
    }

    fn load_balancer_with_config(
        server_count: usize,
        config: LoadBalancerConfig,
    ) -> Arc<LoadBalancer<RoundRobin>> {
        let endpoints = (0..server_count)
            .map(|index| Endpoint::tcp_from_str(&format!("127.0.0.1:{}", 8001 + index)).unwrap())
            .collect();
        let discovery = Arc::new(StaticDiscovery::new(endpoints));
        Arc::new(LoadBalancer::with_config(
            discovery,
            RoundRobin::new(),
            config,
        ))
    }

    #[tokio::test]
    async fn test_lb_client_creation() {
        let lb = load_balancer(2);
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(lb.clone(), factory.clone());

        client.init().await.unwrap();

        assert_eq!(client.load_balancer().server_count(), 2);
        assert_eq!(client.connection_count(), 2);
        assert_eq!(factory.create_count.load(Ordering::Relaxed), 2);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_child_handle_is_retained_until_close() {
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(load_balancer(1), factory);
        client.init().await.unwrap();

        let entry = { client.clients.read().get(&0).cloned().unwrap() };
        assert!(entry.handle.lock().await.is_some());

        client.close().await.unwrap();

        assert!(entry.handle.lock().await.is_none());
        assert!(entry.client.is_closed());
    }

    #[tokio::test]
    async fn test_close_drains_clients_and_is_idempotent() {
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(load_balancer(2), factory);
        client.init().await.unwrap();

        let entries = { client.clients.read().values().cloned().collect::<Vec<_>>() };
        client.close().await.unwrap();

        assert_eq!(client.connection_count(), 0);
        for entry in entries {
            assert!(entry.client.is_closed());
            assert!(entry.handle.lock().await.is_none());
        }
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_close_joins_handles_after_client_close_error() {
        let client =
            LoadBalancedClient::new(load_balancer(1), Arc::new(FailingClientFactory::new()));
        client.init().await.unwrap();
        let entry = { client.clients.read().get(&0).cloned().unwrap() };

        let result = client.close().await;

        assert!(matches!(result, Err(RpcError::ConnectionClosed)));
        assert_eq!(client.connection_count(), 0);
        assert!(entry.client.is_closed());
        assert!(entry.handle.lock().await.is_none());
        assert!(matches!(
            client.close().await,
            Err(RpcError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_cancelled_close_retries_finish_all_drained_children() {
        let control = Arc::new(BlockingCloseControl::new());
        let factory = Arc::new(BlockingCloseFactory {
            control: Arc::clone(&control),
        });
        let client = Arc::new(LoadBalancedClient::new(load_balancer(2), factory));
        client.init().await.unwrap();
        let entries = client.clients.read().values().cloned().collect::<Vec<_>>();

        let close_client = Arc::clone(&client);
        let first = tokio::spawn(async move { close_client.close().await });
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), control.close_started.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
        assert_eq!(control.close_count.load(Ordering::SeqCst), 2);
        assert_eq!(client.connection_count(), 0);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        control.close_release.add_permits(2);
        let result = tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .unwrap();

        assert!(matches!(
            result,
            Err(RpcError::Transport(TransportError::NotConnected))
        ));
        assert_eq!(control.close_count.load(Ordering::SeqCst), 2);
        for entry in entries {
            assert!(entry.client.is_closed());
            assert!(entry.handle.lock().await.is_none());
        }
        assert!(matches!(
            client.close().await,
            Err(RpcError::Transport(TransportError::NotConnected))
        ));
    }

    #[tokio::test]
    async fn test_close_cancels_hung_client_creation() {
        let factory = Arc::new(BlockingCreateFactory {
            started: Notify::new(),
        });
        let client = Arc::new(LoadBalancedClient::new(load_balancer(1), factory.clone()));
        client.load_balancer().init().await.unwrap();

        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move { call_client.call::<_, ()>("blocked", &()).await });
        tokio::time::timeout(Duration::from_secs(1), factory.started.notified())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), client.close())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            call.await.unwrap(),
            Err(RpcError::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn test_failover_removal_closes_and_joins_client() {
        let client =
            LoadBalancedClient::new(load_balancer(1), Arc::new(FailingClientFactory::new()));
        client.init().await.unwrap();
        let entry = { client.clients.read().get(&0).cloned().unwrap() };

        let result: Result<()> = client.call("fail", &()).await;

        assert!(matches!(result, Err(RpcError::ConnectionClosed)));
        assert_eq!(client.connection_count(), 0);
        assert!(entry.client.is_closed());
        assert!(entry.handle.lock().await.is_none());
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_failover_disabled_attempts_one_client_creation() {
        let config = LoadBalancerConfig {
            failover_enabled: false,
            max_failover_attempts: 10,
            ..LoadBalancerConfig::default()
        };
        let factory = Arc::new(FailingCreateFactory {
            create_count: AtomicUsize::new(0),
        });
        let client = LoadBalancedClient::new(load_balancer_with_config(3, config), factory.clone());
        client.load_balancer().init().await.unwrap();

        let result: Result<()> = client.call("create", &()).await;

        assert!(matches!(result, Err(RpcError::ClientError(_))));
        assert_eq!(factory.create_count.load(Ordering::SeqCst), 1);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_call_error_is_not_retried_after_invocation() {
        let factory = Arc::new(FailingClientFactory::new());
        let client = LoadBalancedClient::new(load_balancer(2), factory.clone());
        client.init().await.unwrap();

        let result: Result<()> = client.call("unknown_commit", &()).await;

        assert!(matches!(result, Err(RpcError::ConnectionClosed)));
        assert_eq!(factory.send_count.load(Ordering::SeqCst), 1);
        assert_eq!(client.connection_count(), 1);
        let _ = client.close().await;
    }

    #[tokio::test]
    async fn test_dropped_call_releases_active_request_count() {
        let send_started = Arc::new(Notify::new());
        let factory = Arc::new(BlockingSendFactory {
            send_started: Arc::clone(&send_started),
        });
        let client = Arc::new(LoadBalancedClient::new(load_balancer(1), factory));
        client.init().await.unwrap();

        let call_client = Arc::clone(&client);
        let call = tokio::spawn(async move { call_client.call::<_, ()>("blocked", &()).await });
        tokio::time::timeout(Duration::from_secs(1), send_started.notified())
            .await
            .unwrap();
        assert_eq!(client.load_balancer().active_requests(0), 1);

        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert_eq!(client.load_balancer().active_requests(0), 0);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_stream_setup_releases_affinity_during_creation() {
        let lb = load_balancer(2);
        let factory = Arc::new(BlockingCreateFactory {
            started: Notify::new(),
        });
        let client = Arc::new(LoadBalancedClient::new(lb.clone(), factory.clone()));
        client.load_balancer().init().await.unwrap();

        let stream_client = Arc::clone(&client);
        let setup = tokio::spawn(async move {
            stream_client
                .call_server_stream::<_, i32>("numbers", &())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), factory.started.notified())
            .await
            .unwrap();
        let stream_id = *client
            .owned_stream_ids
            .read()
            .iter()
            .next()
            .expect("stream affinity must be owned during creation");

        setup.abort();
        assert!(matches!(setup.await, Err(error) if error.is_cancelled()));
        assert!(client.owned_stream_ids.read().is_empty());
        assert_eq!(lb.select_for_stream(stream_id), Some(1));
        lb.release_stream(stream_id);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_stream_setup_releases_affinity_during_send() {
        let lb = load_balancer(2);
        let send_started = Arc::new(Notify::new());
        let factory = Arc::new(BlockingSendFactory {
            send_started: Arc::clone(&send_started),
        });
        let client = Arc::new(LoadBalancedClient::new(lb.clone(), factory));
        client.init().await.unwrap();
        let entry = client.clients.read().get(&0).cloned().unwrap();

        let stream_client = Arc::clone(&client);
        let setup = tokio::spawn(async move {
            stream_client
                .call_server_stream::<_, i32>("numbers", &())
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), send_started.notified())
            .await
            .unwrap();
        let stream_id = *client
            .owned_stream_ids
            .read()
            .iter()
            .next()
            .expect("stream affinity must be owned during send");

        setup.abort();
        assert!(matches!(setup.await, Err(error) if error.is_cancelled()));
        assert!(client.owned_stream_ids.read().is_empty());
        assert_eq!(entry.client.active_streams(), 0);
        assert_eq!(lb.select_for_stream(stream_id), Some(1));
        lb.release_stream(stream_id);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_stream_uses_affinity_stream_id() {
        let lb = load_balancer(2);
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(lb.clone(), factory.clone());
        client.init().await.unwrap();
        let peer_transport = { factory.peers.lock().remove(0) };
        let peer_channel = MessageChannelAdapter::new(peer_transport);

        let stream: StreamReceiver<i32> = client.call_server_stream("numbers", &()).await.unwrap();
        let message = peer_channel.recv().await.unwrap();
        let stream_id = stream.stream_id();

        assert_eq!(message.metadata.stream_id, Some(stream_id));
        assert_eq!(lb.select_for_stream(stream_id), Some(0));
        client.release_stream(stream_id);
        assert_eq!(lb.select_for_stream(stream_id), Some(1));
        lb.release_stream(stream_id);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_stream_error_releases_affinity() {
        let lb = load_balancer(2);
        let factory = Arc::new(FailingClientFactory::new());
        let client = LoadBalancedClient::new(lb.clone(), factory.clone());
        client.init().await.unwrap();

        let result: Result<StreamReceiver<i32>> = client.call_server_stream("numbers", &()).await;
        let sent_stream_ids = { factory.sent_stream_ids.lock().clone() };

        assert!(matches!(result, Err(RpcError::ConnectionClosed)));
        assert_eq!(sent_stream_ids.len(), 1);
        assert_eq!(lb.select_for_stream(sent_stream_ids[0]), Some(1));
        lb.release_stream(sent_stream_ids[0]);
        let _ = client.close().await;
    }

    #[tokio::test]
    async fn test_server_stream_end_releases_affinity_automatically() {
        let lb = load_balancer(2);
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(lb.clone(), factory.clone());
        client.init().await.unwrap();
        let peer = MessageChannelAdapter::new(factory.peers.lock().remove(0));

        let mut stream: StreamReceiver<i32> =
            client.call_server_stream("numbers", &()).await.unwrap();
        let request = peer.recv().await.unwrap();
        let stream_id = request.metadata.stream_id.unwrap();
        assert_eq!(lb.select_for_stream(stream_id), Some(0));

        let end: Message = Message::stream_end(stream_id);
        peer.send(&end).await.unwrap();
        assert!(stream.recv().await.is_none());
        assert_eq!(lb.select_for_stream(stream_id), Some(1));
        lb.release_stream(stream_id);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_dropped_server_stream_releases_affinity_automatically() {
        let lb = load_balancer(2);
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(lb.clone(), factory);
        client.init().await.unwrap();

        let stream: StreamReceiver<i32> = client.call_server_stream("numbers", &()).await.unwrap();
        let stream_id = stream.stream_id();
        assert_eq!(lb.select_for_stream(stream_id), Some(0));
        drop(stream);
        assert_eq!(lb.select_for_stream(stream_id), Some(1));
        lb.release_stream(stream_id);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_close_releases_only_owned_stream_affinities() {
        let lb = load_balancer(3);
        let first = LoadBalancedClient::new(lb.clone(), Arc::new(MockClientFactory::new()));
        let second = LoadBalancedClient::new(lb.clone(), Arc::new(MockClientFactory::new()));
        first.init().await.unwrap();
        second.init().await.unwrap();

        let first_stream: StreamReceiver<i32> =
            first.call_server_stream("first", &()).await.unwrap();
        let second_stream: StreamReceiver<i32> =
            second.call_server_stream("second", &()).await.unwrap();
        let first_stream_id = first_stream.stream_id();
        let second_stream_id = second_stream.stream_id();

        assert_eq!(lb.select_for_stream(first_stream_id), Some(0));
        assert_eq!(lb.select_for_stream(second_stream_id), Some(1));
        first.close().await.unwrap();
        assert_eq!(lb.select_for_stream(second_stream_id), Some(1));
        assert_eq!(lb.select_for_stream(first_stream_id), Some(2));
        lb.release_stream(first_stream_id);

        second.close().await.unwrap();
        assert_eq!(lb.select_for_stream(second_stream_id), Some(0));
        lb.release_stream(second_stream_id);
    }

    #[tokio::test]
    async fn test_endpoint_churn_replaces_connected_cached_client() {
        let load_balancer = load_balancer(1);
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(load_balancer.clone(), factory.clone());
        client.init().await.unwrap();
        let first = client.clients.read().get(&0).cloned().unwrap();
        let replacement = Endpoint::tcp_from_str("127.0.0.1:9001").unwrap();

        load_balancer.update_endpoints(vec![replacement.clone()]);
        let second = client.get_or_create_client(0, &replacement).await.unwrap();

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(second.endpoint, replacement);
        assert!(first.client.is_closed());
        assert!(first.handle.lock().await.is_none());
        assert_eq!(factory.create_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            factory.created_endpoints.lock().as_slice(),
            &[
                Endpoint::tcp_from_str("127.0.0.1:8001").unwrap(),
                replacement
            ]
        );
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_healthy_cached_client_bypasses_creation_mutex() {
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(load_balancer(1), factory);
        client.init().await.unwrap();
        let endpoint = client.load_balancer().get_endpoint(0).unwrap();
        let creation_lock = client.creation_locks.read().get(&0).cloned().unwrap();
        let _creation = creation_lock.lock().await;

        let cached = tokio::time::timeout(
            Duration::from_millis(50),
            client.get_or_create_client(0, &endpoint),
        )
        .await
        .expect("healthy cached lookup waited for the creation mutex")
        .unwrap();

        assert!(Arc::ptr_eq(&cached, client.clients.read().get(&0).unwrap()));
        drop(_creation);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_creation_is_single_flight_per_server() {
        let factory = Arc::new(MockClientFactory::with_delay(Duration::from_millis(50)));
        let client = LoadBalancedClient::new(load_balancer(1), factory.clone());
        client.load_balancer().init().await.unwrap();
        let endpoint = client.load_balancer().get_endpoint(0).unwrap();

        let (first, second, third) = tokio::join!(
            client.get_or_create_client(0, &endpoint),
            client.get_or_create_client(0, &endpoint),
            client.get_or_create_client(0, &endpoint),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        let third = third.unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(&first, &third));
        assert_eq!(factory.create_count.load(Ordering::Relaxed), 1);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_work_after_close_returns_connection_closed() {
        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(load_balancer(1), factory);
        client.init().await.unwrap();
        client.close().await.unwrap();

        let call_result: Result<()> = client.call("closed", &()).await;
        let stream_result: Result<StreamReceiver<(), BincodeCodec>> =
            client.call_server_stream("closed", &()).await;
        let notify_result = client.notify("closed", &()).await;

        assert!(matches!(call_result, Err(RpcError::ConnectionClosed)));
        assert!(matches!(stream_result, Err(RpcError::ConnectionClosed)));
        assert!(matches!(notify_result, Err(RpcError::ConnectionClosed)));
    }
}
