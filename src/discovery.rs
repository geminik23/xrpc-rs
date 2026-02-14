use async_trait::async_trait;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::error::{Result, RpcError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Endpoint {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(std::path::PathBuf),
    SharedMemory(String),
    Custom {
        scheme: String,
        address: String,
    },
}

impl Endpoint {
    pub fn tcp(addr: SocketAddr) -> Self {
        Endpoint::Tcp(addr)
    }

    pub fn tcp_from_str(addr: &str) -> std::result::Result<Self, std::net::AddrParseError> {
        Ok(Endpoint::Tcp(addr.parse()?))
    }

    #[cfg(unix)]
    pub fn unix(path: impl Into<std::path::PathBuf>) -> Self {
        Endpoint::Unix(path.into())
    }

    pub fn shared_memory(name: impl Into<String>) -> Self {
        Endpoint::SharedMemory(name.into())
    }

    pub fn custom(scheme: impl Into<String>, address: impl Into<String>) -> Self {
        Endpoint::Custom {
            scheme: scheme.into(),
            address: address.into(),
        }
    }

    pub fn scheme(&self) -> &str {
        match self {
            Endpoint::Tcp(_) => "tcp",
            #[cfg(unix)]
            Endpoint::Unix(_) => "unix",
            Endpoint::SharedMemory(_) => "shm",
            Endpoint::Custom { scheme, .. } => scheme,
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Tcp(addr) => write!(f, "tcp://{}", addr),
            #[cfg(unix)]
            Endpoint::Unix(path) => write!(f, "unix://{}", path.display()),
            Endpoint::SharedMemory(name) => write!(f, "shm://{}", name),
            Endpoint::Custom { scheme, address } => write!(f, "{}://{}", scheme, address),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    Updated(Vec<Endpoint>),
    Added(Endpoint),
    Removed(Endpoint),
}

#[async_trait]
pub trait ServiceDiscovery: Send + Sync {
    async fn discover(&self) -> Result<Vec<Endpoint>>;

    fn watch(&self) -> Option<broadcast::Receiver<DiscoveryEvent>> {
        None
    }

    async fn health_check(&self, _endpoint: &Endpoint) -> bool {
        true
    }
}

pub struct StaticDiscovery {
    endpoints: Vec<Endpoint>,
}

impl StaticDiscovery {
    pub fn new(endpoints: Vec<Endpoint>) -> Self {
        Self { endpoints }
    }

    pub fn single(endpoint: Endpoint) -> Self {
        Self::new(vec![endpoint])
    }
}

#[async_trait]
impl ServiceDiscovery for StaticDiscovery {
    async fn discover(&self) -> Result<Vec<Endpoint>> {
        Ok(self.endpoints.clone())
    }
}

pub struct DnsDiscovery {
    hostname: String,
    port: u16,
    refresh_interval: Duration,
    tx: broadcast::Sender<DiscoveryEvent>,
}

impl DnsDiscovery {
    pub fn new(hostname: impl Into<String>, port: u16) -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            hostname: hostname.into(),
            port,
            refresh_interval: Duration::from_secs(30),
            tx,
        }
    }

    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    pub fn start_refresh(&self) -> tokio::task::JoinHandle<()> {
        let hostname = self.hostname.clone();
        let port = self.port;
        let tx = self.tx.clone();
        let interval = self.refresh_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Ok(endpoints) = Self::resolve(&hostname, port).await {
                    let _ = tx.send(DiscoveryEvent::Updated(endpoints));
                }
            }
        })
    }

    async fn resolve(hostname: &str, port: u16) -> Result<Vec<Endpoint>> {
        use std::net::ToSocketAddrs;

        let addr = format!("{}:{}", hostname, port);
        let addrs: Vec<_> = tokio::task::spawn_blocking(move || {
            addr.to_socket_addrs().map(|iter| iter.collect::<Vec<_>>())
        })
        .await
        .map_err(|e| RpcError::ClientError(e.to_string()))?
        .map_err(|e| RpcError::ClientError(e.to_string()))?;

        Ok(addrs.into_iter().map(Endpoint::Tcp).collect())
    }
}

#[async_trait]
impl ServiceDiscovery for DnsDiscovery {
    async fn discover(&self) -> Result<Vec<Endpoint>> {
        Self::resolve(&self.hostname, self.port).await
    }

    fn watch(&self) -> Option<broadcast::Receiver<DiscoveryEvent>> {
        Some(self.tx.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_display() {
        let tcp = Endpoint::tcp("127.0.0.1:8080".parse().unwrap());
        assert_eq!(tcp.to_string(), "tcp://127.0.0.1:8080");
        assert_eq!(tcp.scheme(), "tcp");

        let shm = Endpoint::shared_memory("test_service");
        assert_eq!(shm.to_string(), "shm://test_service");
        assert_eq!(shm.scheme(), "shm");

        let custom = Endpoint::custom("grpc", "localhost:9090");
        assert_eq!(custom.to_string(), "grpc://localhost:9090");
        assert_eq!(custom.scheme(), "grpc");
    }

    #[test]
    fn test_endpoint_equality() {
        let ep1 = Endpoint::tcp_from_str("127.0.0.1:8080").unwrap();
        let ep2 = Endpoint::tcp_from_str("127.0.0.1:8080").unwrap();
        let ep3 = Endpoint::tcp_from_str("127.0.0.1:8081").unwrap();

        assert_eq!(ep1, ep2);
        assert_ne!(ep1, ep3);
    }

    #[tokio::test]
    async fn test_static_discovery() {
        let endpoints = vec![
            Endpoint::tcp_from_str("127.0.0.1:8001").unwrap(),
            Endpoint::tcp_from_str("127.0.0.1:8002").unwrap(),
        ];

        let discovery = StaticDiscovery::new(endpoints.clone());
        let discovered = discovery.discover().await.unwrap();

        assert_eq!(discovered, endpoints);
        assert!(discovery.watch().is_none());
    }
}
