use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::discovery::{DiscoveryEvent, Endpoint, ServiceDiscovery};
use crate::error::Result;
use crate::streaming::StreamId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerHealth {
    Healthy,
    Degraded,
    Unhealthy,
    #[default]
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ServerState<S = ()> {
    pub endpoint: Endpoint,
    pub health: ServerHealth,
    pub status: Option<S>,
    pub last_update: Instant,
    pub active_requests: usize,
    pub total_requests: u64,
    pub total_errors: u64,
    pub consecutive_failures: u32,
}

impl<S> ServerState<S> {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            health: ServerHealth::Unknown,
            status: None,
            last_update: Instant::now(),
            active_requests: 0,
            total_requests: 0,
            total_errors: 0,
            consecutive_failures: 0,
        }
    }

    pub fn is_available(&self) -> bool {
        matches!(
            self.health,
            ServerHealth::Healthy | ServerHealth::Degraded | ServerHealth::Unknown
        )
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.total_requests += 1;
        self.last_update = Instant::now();
    }

    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.total_errors += 1;
        self.total_requests += 1;
        self.last_update = Instant::now();
    }
}

pub trait LoadBalanceStrategy: Send + Sync {
    type Status: Clone + Send + Sync + 'static;

    fn select(&self, servers: &[ServerState<Self::Status>]) -> Option<usize>;

    fn update_status(&self, _server_idx: usize, _status: Self::Status) {}

    fn on_success(&self, _server_idx: usize) {}

    fn on_failure(&self, _server_idx: usize) {}

    fn name(&self) -> &'static str;
}

pub struct RoundRobin {
    counter: AtomicUsize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalanceStrategy for RoundRobin {
    type Status = ();

    fn select(&self, servers: &[ServerState<()>]) -> Option<usize> {
        let available: Vec<_> = servers
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_available())
            .collect();

        if available.is_empty() {
            return None;
        }

        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        Some(available[idx % available.len()].0)
    }

    fn name(&self) -> &'static str {
        "RoundRobin"
    }
}

pub struct Random;

impl Random {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalanceStrategy for Random {
    type Status = ();

    fn select(&self, servers: &[ServerState<()>]) -> Option<usize> {
        let available: Vec<_> = servers
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_available())
            .map(|(i, _)| i)
            .collect();

        if available.is_empty() {
            return None;
        }

        let idx = rand::thread_rng().gen_range(0..available.len());
        Some(available[idx])
    }

    fn name(&self) -> &'static str {
        "Random"
    }
}

pub struct LeastConnections;

impl LeastConnections {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LeastConnections {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalanceStrategy for LeastConnections {
    type Status = ();

    fn select(&self, servers: &[ServerState<()>]) -> Option<usize> {
        servers
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_available())
            .min_by_key(|(_, s)| s.active_requests)
            .map(|(i, _)| i)
    }

    fn name(&self) -> &'static str {
        "LeastConnections"
    }
}

#[derive(Debug, Clone)]
pub struct ServerWeight {
    pub weight: u32,
    pub current_weight: i32,
}

impl Default for ServerWeight {
    fn default() -> Self {
        Self {
            weight: 1,
            current_weight: 0,
        }
    }
}

pub struct WeightedRoundRobin {
    weights: parking_lot::Mutex<Vec<ServerWeight>>,
}

impl WeightedRoundRobin {
    pub fn new() -> Self {
        Self {
            weights: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn with_weights(weights: Vec<u32>) -> Self {
        let sw: Vec<_> = weights
            .into_iter()
            .map(|w| ServerWeight {
                weight: w,
                current_weight: 0,
            })
            .collect();
        Self {
            weights: parking_lot::Mutex::new(sw),
        }
    }

    pub fn set_weight(&self, server_idx: usize, weight: u32) {
        let mut weights = self.weights.lock();
        while weights.len() <= server_idx {
            weights.push(ServerWeight::default());
        }
        weights[server_idx].weight = weight;
    }
}

impl Default for WeightedRoundRobin {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalanceStrategy for WeightedRoundRobin {
    type Status = ();

    fn select(&self, servers: &[ServerState<()>]) -> Option<usize> {
        let mut weights = self.weights.lock();

        while weights.len() < servers.len() {
            weights.push(ServerWeight::default());
        }

        let mut total_weight = 0i32;
        let mut best_idx = None;
        let mut best_weight = i32::MIN;

        for (i, (server, sw)) in servers.iter().zip(weights.iter_mut()).enumerate() {
            if !server.is_available() {
                continue;
            }

            sw.current_weight += sw.weight as i32;
            total_weight += sw.weight as i32;

            if sw.current_weight > best_weight {
                best_weight = sw.current_weight;
                best_idx = Some(i);
            }
        }

        if let Some(idx) = best_idx {
            weights[idx].current_weight -= total_weight;
        }

        best_idx
    }

    fn name(&self) -> &'static str {
        "WeightedRoundRobin"
    }
}

pub struct ScoreBased {
    pub threshold: f32,
    pub stale_timeout: Duration,
}

impl ScoreBased {
    pub fn new() -> Self {
        Self {
            threshold: 0.95,
            stale_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_threshold(mut self, threshold: f32) -> Self {
        self.threshold = threshold;
        self
    }

    pub fn with_stale_timeout(mut self, timeout: Duration) -> Self {
        self.stale_timeout = timeout;
        self
    }
}

impl Default for ScoreBased {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalanceStrategy for ScoreBased {
    type Status = f32;

    fn select(&self, servers: &[ServerState<f32>]) -> Option<usize> {
        servers
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                if !s.is_available() {
                    return false;
                }
                if s.last_update.elapsed() > self.stale_timeout {
                    return true;
                }
                s.status.map_or(true, |score| score < self.threshold)
            })
            .min_by(|(_, a), (_, b)| {
                let a_stale = a.last_update.elapsed() > self.stale_timeout;
                let b_stale = b.last_update.elapsed() > self.stale_timeout;

                match (a_stale, b_stale) {
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => {
                        let a_score = a.status.unwrap_or(0.0);
                        let b_score = b.status.unwrap_or(0.0);
                        a_score
                            .partial_cmp(&b_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }
                }
            })
            .map(|(i, _)| i)
    }

    fn name(&self) -> &'static str {
        "ScoreBased"
    }
}

#[derive(Debug, Clone)]
pub struct LoadBalancerConfig {
    pub max_failures: u32,
    pub health_check_interval: Duration,
    pub auto_health_check: bool,
    pub failover_enabled: bool,
    pub max_failover_attempts: u32,
}

impl Default for LoadBalancerConfig {
    fn default() -> Self {
        Self {
            max_failures: 3,
            health_check_interval: Duration::from_secs(10),
            auto_health_check: true,
            failover_enabled: true,
            max_failover_attempts: 2,
        }
    }
}

pub struct LoadBalancer<S: LoadBalanceStrategy> {
    discovery: Arc<dyn ServiceDiscovery>,
    strategy: S,
    servers: Arc<RwLock<Vec<ServerState<S::Status>>>>,
    stream_affinity: RwLock<HashMap<StreamId, usize>>,
    config: LoadBalancerConfig,
}

impl<S: LoadBalanceStrategy + 'static> LoadBalancer<S> {
    pub fn new(discovery: Arc<dyn ServiceDiscovery>, strategy: S) -> Self {
        Self::with_config(discovery, strategy, LoadBalancerConfig::default())
    }

    pub fn with_config(
        discovery: Arc<dyn ServiceDiscovery>,
        strategy: S,
        config: LoadBalancerConfig,
    ) -> Self {
        Self {
            discovery,
            strategy,
            servers: Arc::new(RwLock::new(Vec::new())),
            stream_affinity: RwLock::new(HashMap::new()),
            config,
        }
    }

    pub async fn init(&self) -> Result<()> {
        let endpoints = self.discovery.discover().await?;
        self.update_endpoints(endpoints);
        Ok(())
    }

    pub fn start(&self) -> LoadBalancerHandle {
        let mut handles = Vec::new();

        if let Some(mut rx) = self.discovery.watch() {
            let servers = self.servers.clone();
            let h = tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    match event {
                        DiscoveryEvent::Updated(endpoints) => {
                            Self::update_endpoints_static(&servers, endpoints);
                        }
                        DiscoveryEvent::Added(endpoint) => {
                            servers.write().push(ServerState::new(endpoint));
                        }
                        DiscoveryEvent::Removed(endpoint) => {
                            servers.write().retain(|s| s.endpoint != endpoint);
                        }
                    }
                }
            });
            handles.push(h);
        }

        LoadBalancerHandle { handles }
    }

    fn update_endpoints(&self, endpoints: Vec<Endpoint>) {
        Self::update_endpoints_static(&self.servers, endpoints);
    }

    fn update_endpoints_static(
        servers: &RwLock<Vec<ServerState<S::Status>>>,
        endpoints: Vec<Endpoint>,
    ) {
        let mut servers = servers.write();
        servers.retain(|s| endpoints.contains(&s.endpoint));
        for ep in endpoints {
            if !servers.iter().any(|s| s.endpoint == ep) {
                servers.push(ServerState::new(ep));
            }
        }
    }

    pub fn select(&self) -> Option<usize> {
        let servers = self.servers.read();
        self.strategy.select(&servers)
    }

    pub fn select_for_stream(&self, stream_id: StreamId) -> Option<usize> {
        {
            let affinity = self.stream_affinity.read();
            if let Some(&idx) = affinity.get(&stream_id) {
                let servers = self.servers.read();
                if servers.get(idx).map_or(false, |s| s.is_available()) {
                    return Some(idx);
                }
            }
        }

        let idx = self.select()?;
        self.stream_affinity.write().insert(stream_id, idx);
        Some(idx)
    }

    pub fn release_stream(&self, stream_id: StreamId) {
        self.stream_affinity.write().remove(&stream_id);
    }

    pub fn get_endpoint(&self, server_idx: usize) -> Option<Endpoint> {
        self.servers
            .read()
            .get(server_idx)
            .map(|s| s.endpoint.clone())
    }

    pub fn report_status(&self, server_idx: usize, status: S::Status) {
        if let Some(server) = self.servers.write().get_mut(server_idx) {
            server.status = Some(status.clone());
            server.last_update = Instant::now();
        }
        self.strategy.update_status(server_idx, status);
    }

    pub fn record_success(&self, server_idx: usize) {
        if let Some(server) = self.servers.write().get_mut(server_idx) {
            server.record_success();
            server.health = ServerHealth::Healthy;
        }
        self.strategy.on_success(server_idx);
    }

    pub fn record_failure(&self, server_idx: usize) {
        let should_mark_unhealthy = {
            let mut servers = self.servers.write();
            if let Some(server) = servers.get_mut(server_idx) {
                server.record_failure();
                server.consecutive_failures >= self.config.max_failures
            } else {
                false
            }
        };

        if should_mark_unhealthy {
            self.mark_unhealthy(server_idx);
        }

        self.strategy.on_failure(server_idx);
    }

    pub fn mark_unhealthy(&self, server_idx: usize) {
        if let Some(server) = self.servers.write().get_mut(server_idx) {
            server.health = ServerHealth::Unhealthy;
        }
    }

    pub fn mark_healthy(&self, server_idx: usize) {
        if let Some(server) = self.servers.write().get_mut(server_idx) {
            server.health = ServerHealth::Healthy;
            server.consecutive_failures = 0;
        }
    }

    pub fn available_count(&self) -> usize {
        self.servers
            .read()
            .iter()
            .filter(|s| s.is_available())
            .count()
    }

    pub fn server_count(&self) -> usize {
        self.servers.read().len()
    }

    pub fn strategy_name(&self) -> &'static str {
        self.strategy.name()
    }

    pub fn acquire(&self, server_idx: usize) {
        if let Some(server) = self.servers.write().get_mut(server_idx) {
            server.active_requests += 1;
        }
    }

    pub fn release(&self, server_idx: usize) {
        if let Some(server) = self.servers.write().get_mut(server_idx) {
            server.active_requests = server.active_requests.saturating_sub(1);
        }
    }

    pub fn config(&self) -> &LoadBalancerConfig {
        &self.config
    }
}

pub struct LoadBalancerHandle {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl LoadBalancerHandle {
    pub async fn shutdown(self) {
        for h in self.handles {
            h.abort();
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::StaticDiscovery;

    fn create_test_servers(count: usize) -> Vec<ServerState<()>> {
        (0..count)
            .map(|i| {
                ServerState::new(Endpoint::tcp_from_str(&format!("127.0.0.1:800{}", i)).unwrap())
            })
            .collect()
    }

    #[test]
    fn test_round_robin() {
        let strategy = RoundRobin::new();
        let servers = create_test_servers(3);

        let selections: Vec<_> = (0..6).filter_map(|_| strategy.select(&servers)).collect();
        assert_eq!(selections, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn test_round_robin_skip_unhealthy() {
        let strategy = RoundRobin::new();
        let mut servers = create_test_servers(3);
        servers[1].health = ServerHealth::Unhealthy;

        let selections: Vec<_> = (0..4).filter_map(|_| strategy.select(&servers)).collect();
        assert_eq!(selections, vec![0, 2, 0, 2]);
    }

    #[test]
    fn test_least_connections() {
        let strategy = LeastConnections::new();
        let mut servers = create_test_servers(3);
        servers[0].active_requests = 5;
        servers[1].active_requests = 2;
        servers[2].active_requests = 3;

        assert_eq!(strategy.select(&servers), Some(1));
    }

    #[test]
    fn test_weighted_round_robin() {
        let strategy = WeightedRoundRobin::with_weights(vec![2, 1, 1]);
        let servers = create_test_servers(3);

        let mut counts = [0usize; 3];
        for _ in 0..8 {
            if let Some(idx) = strategy.select(&servers) {
                counts[idx] += 1;
            }
        }

        assert!(counts[0] > counts[1]);
        assert!(counts[0] > counts[2]);
    }

    #[tokio::test]
    async fn test_load_balancer_init() {
        let endpoints = vec![
            Endpoint::tcp_from_str("127.0.0.1:8001").unwrap(),
            Endpoint::tcp_from_str("127.0.0.1:8002").unwrap(),
        ];

        let discovery = Arc::new(StaticDiscovery::new(endpoints));
        let lb = LoadBalancer::new(discovery, RoundRobin::new());
        lb.init().await.unwrap();

        assert_eq!(lb.server_count(), 2);
        assert_eq!(lb.available_count(), 2);
    }

    #[tokio::test]
    async fn test_stream_affinity() {
        let endpoints = vec![
            Endpoint::tcp_from_str("127.0.0.1:8001").unwrap(),
            Endpoint::tcp_from_str("127.0.0.1:8002").unwrap(),
        ];

        let discovery = Arc::new(StaticDiscovery::new(endpoints));
        let lb = LoadBalancer::new(discovery, RoundRobin::new());
        lb.init().await.unwrap();

        let stream_id = 42;
        let first = lb.select_for_stream(stream_id);
        let second = lb.select_for_stream(stream_id);
        let third = lb.select_for_stream(stream_id);

        assert_eq!(first, second);
        assert_eq!(second, third);

        lb.release_stream(stream_id);
    }

    #[tokio::test]
    async fn test_failure_tracking() {
        let endpoints = vec![Endpoint::tcp_from_str("127.0.0.1:8001").unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(endpoints));
        let config = LoadBalancerConfig {
            max_failures: 2,
            ..Default::default()
        };
        let lb = LoadBalancer::with_config(discovery, RoundRobin::new(), config);
        lb.init().await.unwrap();

        lb.record_failure(0);
        assert_eq!(lb.available_count(), 1);

        lb.record_failure(0);
        assert_eq!(lb.available_count(), 0);

        lb.mark_healthy(0);
        assert_eq!(lb.available_count(), 1);
    }
}
