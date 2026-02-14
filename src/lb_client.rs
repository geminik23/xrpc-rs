use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::channel::message::MessageChannel;
use crate::client::RpcClient;
use crate::codec::{BincodeCodec, Codec};
use crate::discovery::Endpoint;
use crate::error::{Result, RpcError};
use crate::loadbalancer::{LoadBalanceStrategy, LoadBalancer};
use crate::streaming::StreamReceiver;

#[async_trait]
pub trait ClientFactory<T, C>: Send + Sync
where
    T: MessageChannel<C>,
    C: Codec,
{
    async fn create(&self, endpoint: &Endpoint) -> Result<RpcClient<T, C>>;
}

pub struct LoadBalancedClient<T, C, S>
where
    T: MessageChannel<C>,
    C: Codec,
    S: LoadBalanceStrategy,
{
    load_balancer: Arc<LoadBalancer<S>>,
    clients: RwLock<HashMap<usize, Arc<RpcClient<T, C>>>>,
    factory: Arc<dyn ClientFactory<T, C>>,
}

impl<T, C, S> LoadBalancedClient<T, C, S>
where
    T: MessageChannel<C> + 'static,
    C: Codec + Clone + Default + 'static,
    S: LoadBalanceStrategy + 'static,
{
    pub fn new(load_balancer: Arc<LoadBalancer<S>>, factory: Arc<dyn ClientFactory<T, C>>) -> Self {
        Self {
            load_balancer,
            clients: RwLock::new(HashMap::new()),
            factory,
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.load_balancer.init().await?;

        for i in 0..self.load_balancer.server_count() {
            if let Some(endpoint) = self.load_balancer.get_endpoint(i) {
                let _ = self.get_or_create_client(i, &endpoint).await;
            }
        }

        Ok(())
    }

    async fn get_or_create_client(
        &self,
        server_idx: usize,
        endpoint: &Endpoint,
    ) -> Result<Arc<RpcClient<T, C>>> {
        {
            let clients = self.clients.read();
            if let Some(client) = clients.get(&server_idx) {
                if client.is_connected() {
                    return Ok(client.clone());
                }
            }
        }

        let client = self.factory.create(endpoint).await?;
        let client = Arc::new(client);

        let _handle = client.start();

        self.clients.write().insert(server_idx, client.clone());

        Ok(client)
    }

    pub async fn call<Req, Resp>(&self, method: &str, request: &Req) -> Result<Resp>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        let max_attempts = self.load_balancer.config().max_failover_attempts as usize + 1;
        let mut last_error = None;
        let mut tried_servers = Vec::new();

        for _ in 0..max_attempts {
            let server_idx = match self.select_excluding(&tried_servers) {
                Some(idx) => idx,
                None => break,
            };

            tried_servers.push(server_idx);

            let endpoint = match self.load_balancer.get_endpoint(server_idx) {
                Some(ep) => ep,
                None => continue,
            };

            let client = match self.get_or_create_client(server_idx, &endpoint).await {
                Ok(c) => c,
                Err(e) => {
                    self.load_balancer.record_failure(server_idx);
                    last_error = Some(e);
                    continue;
                }
            };

            self.load_balancer.acquire(server_idx);
            let result = client.call(method, request).await;
            self.load_balancer.release(server_idx);

            match result {
                Ok(resp) => {
                    self.load_balancer.record_success(server_idx);
                    return Ok(resp);
                }
                Err(e) => {
                    self.load_balancer.record_failure(server_idx);
                    self.clients.write().remove(&server_idx);
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| RpcError::ClientError("No servers available".to_string())))
    }

    fn select_excluding(&self, excluded: &[usize]) -> Option<usize> {
        for _ in 0..self.load_balancer.server_count().max(10) {
            if let Some(idx) = self.load_balancer.select() {
                if !excluded.contains(&idx) {
                    return Some(idx);
                }
            } else {
                break;
            }
        }
        None
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
        let stream_id = crate::streaming::next_stream_id();

        let server_idx = self
            .load_balancer
            .select_for_stream(stream_id)
            .ok_or_else(|| RpcError::ClientError("No servers available".to_string()))?;

        let endpoint = self
            .load_balancer
            .get_endpoint(server_idx)
            .ok_or_else(|| RpcError::ClientError("Server not found".to_string()))?;

        let client = self.get_or_create_client(server_idx, &endpoint).await?;

        client.call_server_stream(method, request).await
    }

    pub async fn notify<Req: Serialize>(&self, method: &str, request: &Req) -> Result<()> {
        let server_idx = self
            .load_balancer
            .select()
            .ok_or_else(|| RpcError::ClientError("No servers available".to_string()))?;

        let endpoint = self
            .load_balancer
            .get_endpoint(server_idx)
            .ok_or_else(|| RpcError::ClientError("Server not found".to_string()))?;

        let client = self.get_or_create_client(server_idx, &endpoint).await?;
        client.notify(method, request).await
    }

    pub fn load_balancer(&self) -> &LoadBalancer<S> {
        &self.load_balancer
    }

    pub fn connection_count(&self) -> usize {
        self.clients.read().len()
    }

    pub fn release_stream(&self, stream_id: crate::streaming::StreamId) {
        self.load_balancer.release_stream(stream_id);
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
    use crate::loadbalancer::RoundRobin;
    use crate::transport::channel::{ChannelConfig, ChannelFrameTransport};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockClientFactory {
        create_count: AtomicUsize,
    }

    impl MockClientFactory {
        fn new() -> Self {
            Self {
                create_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ClientFactory<MessageChannelAdapter<ChannelFrameTransport>, BincodeCodec>
        for MockClientFactory
    {
        async fn create(
            &self,
            _endpoint: &Endpoint,
        ) -> Result<RpcClient<MessageChannelAdapter<ChannelFrameTransport>, BincodeCodec>> {
            self.create_count.fetch_add(1, Ordering::Relaxed);

            let (t1, _t2) =
                ChannelFrameTransport::create_pair("mock", ChannelConfig::default()).unwrap();
            let channel = MessageChannelAdapter::new(t1);
            Ok(RpcClient::new(channel))
        }
    }

    #[tokio::test]
    async fn test_lb_client_creation() {
        let endpoints = vec![
            Endpoint::tcp_from_str("127.0.0.1:8001").unwrap(),
            Endpoint::tcp_from_str("127.0.0.1:8002").unwrap(),
        ];

        let discovery = Arc::new(StaticDiscovery::new(endpoints));
        let lb = Arc::new(LoadBalancer::new(discovery, RoundRobin::new()));

        let factory = Arc::new(MockClientFactory::new());
        let client = LoadBalancedClient::new(lb.clone(), factory.clone());

        client.init().await.unwrap();

        assert_eq!(client.load_balancer().server_count(), 2);
        assert_eq!(factory.create_count.load(Ordering::Relaxed), 2);
    }
}
