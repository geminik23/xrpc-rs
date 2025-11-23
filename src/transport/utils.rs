use std::sync::Weak;
use std::time::Duration;

/// Spawns a background task that runs periodically while the owner exists.
pub fn spawn_weak_loop<T: Send + Sync + 'static>(
    owner: Weak<T>,
    interval: Duration,
    mut action: impl FnMut(&T) + Send + 'static,
) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);

        // Immediately, the first tick completes to wait for the first interval.
        timer.tick().await;

        loop {
            timer.tick().await;

            if let Some(strong) = owner.upgrade() {
                action(&strong);
            } else {
                // Owner dropped, stop the loop
                break;
            }
        }
    });
}
