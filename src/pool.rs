use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::error::{Result, RpcError};

pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub acquire_timeout: Duration,
    pub idle_timeout: Duration,
    pub health_check_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            health_check_interval: Duration::from_secs(30),
        }
    }
}

impl PoolConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }
}

struct PooledItem<T> {
    item: T,
    last_used: Instant,
}

impl<T> PooledItem<T> {
    fn new(item: T) -> Self {
        Self {
            item,
            last_used: Instant::now(),
        }
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    #[allow(dead_code)]
    fn is_idle(&self, timeout: Duration) -> bool {
        self.last_used.elapsed() > timeout
    }
}

type BoxedFactory<T> =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<T>> + Send>> + Send + Sync>;

pub struct ConnectionPool<T> {
    config: PoolConfig,
    factory: BoxedFactory<T>,
    available: Arc<Mutex<VecDeque<PooledItem<T>>>>,
    semaphore: Arc<Semaphore>,
    total_created: AtomicUsize,
    shutdown: AtomicBool,
}

impl<T: Send + 'static> ConnectionPool<T> {
    pub async fn new<F, Fut>(config: PoolConfig, factory: F) -> Result<Arc<Self>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let boxed_factory: BoxedFactory<T> = Box::new(move || Box::pin(factory()));

        let pool = Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(config.max_connections)),
            config,
            factory: boxed_factory,
            available: Arc::new(Mutex::new(VecDeque::new())),
            total_created: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });

        for _ in 0..pool.config.min_connections {
            let item = (pool.factory)().await?;
            pool.available.lock().push_back(PooledItem::new(item));
            pool.total_created.fetch_add(1, Ordering::Relaxed);
        }

        Ok(pool)
    }

    pub async fn get(self: &Arc<Self>) -> Result<PoolGuard<T>> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(RpcError::ClientError("Pool is shutdown".to_string()));
        }

        let permit = tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| RpcError::Timeout("Acquire connection timeout".to_string()))?
        .map_err(|_| RpcError::ClientError("Pool is closed".to_string()))?;

        if let Some(mut pooled) = self.available.lock().pop_front() {
            pooled.touch();
            return Ok(PoolGuard {
                item: Some(pooled.item),
                pool: self.clone(),
                _permit: permit,
            });
        }

        let item = (self.factory)().await?;
        self.total_created.fetch_add(1, Ordering::Relaxed);

        Ok(PoolGuard {
            item: Some(item),
            pool: self.clone(),
            _permit: permit,
        })
    }

    fn return_item(&self, item: T) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }
        self.available.lock().push_back(PooledItem::new(item));
    }

    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    pub fn total_created(&self) -> usize {
        self.total_created.load(Ordering::Relaxed)
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.available.lock().clear();
    }
}

pub struct PoolGuard<T: Send + 'static> {
    item: Option<T>,
    pool: Arc<ConnectionPool<T>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<T: Send + 'static> PoolGuard<T> {
    pub fn get(&self) -> &T {
        self.item.as_ref().unwrap()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.item.as_mut().unwrap()
    }
}

impl<T: Send + 'static> Drop for PoolGuard<T> {
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            self.pool.return_item(item);
        }
    }
}

impl<T: Send + 'static> std::ops::Deref for PoolGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<T: Send + 'static> std::ops::DerefMut for PoolGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;

    #[tokio::test]
    async fn test_pool_basic() {
        let counter = Arc::new(AtomicI32::new(0));
        let counter_clone = counter.clone();

        let config = PoolConfig::default()
            .with_min_connections(2)
            .with_max_connections(5);

        let pool = ConnectionPool::new(config, move || {
            let c = counter_clone.clone();
            async move {
                let val = c.fetch_add(1, Ordering::Relaxed);
                Ok(val)
            }
        })
        .await
        .unwrap();

        assert_eq!(pool.available_count(), 2);
        assert_eq!(pool.total_created(), 2);

        {
            let conn = pool.get().await.unwrap();
            assert!(*conn == 0 || *conn == 1);
            assert_eq!(pool.available_count(), 1);
        }

        assert_eq!(pool.available_count(), 2);
    }

    #[tokio::test]
    async fn test_pool_max_connections() {
        let config = PoolConfig::default()
            .with_min_connections(0)
            .with_max_connections(2)
            .with_acquire_timeout(Duration::from_millis(100));

        let pool = ConnectionPool::new(config, || async { Ok(()) })
            .await
            .unwrap();

        let _conn1 = pool.get().await.unwrap();
        let _conn2 = pool.get().await.unwrap();

        let result = pool.get().await;
        assert!(result.is_err());
    }
}
