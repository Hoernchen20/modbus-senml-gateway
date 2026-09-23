use std::any::Any;
use std::future::Future;

use tokio::task::{AbortHandle, JoinError};
use tracing::{error, info};

use crate::modbus::poller::Backoff;

/// §6.2: runs `task_fn()` as its own task and respawns it whenever it
/// panics, waiting `backoff` between attempts, so one connection's bug can't
/// kill polling for the rest. Returns once the task finishes normally (or is
/// cancelled from outside); connection tasks never do, so in practice this
/// runs until dropped.
///
/// Dropping or aborting the future returned here also aborts the currently
/// running task — shutdown only has to cancel the supervisor.
pub async fn supervise<F, Fut>(name: &str, mut backoff: Backoff, mut task_fn: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        let handle = tokio::spawn(task_fn());
        let _guard = AbortOnDrop(handle.abort_handle());
        match handle.await {
            Ok(()) => {
                info!(task = name, "task exited");
                return;
            }
            Err(e) if e.is_panic() => {
                error!(
                    task = name,
                    panic = panic_message(e),
                    retry_in = ?backoff.current(),
                    "task panicked, respawning"
                );
                backoff.wait().await;
            }
            Err(_) => {
                info!(task = name, "task cancelled");
                return;
            }
        }
    }
}

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn panic_message(e: JoinError) -> String {
    let payload: Box<dyn Any + Send> = e.into_panic();
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::{self, Instant};

    fn backoff() -> Backoff {
        Backoff::new(Duration::from_secs(1), Duration::from_secs(4))
    }

    /// A task that panics on its first `n` runs, then records ticks forever.
    fn flaky(
        n: usize,
        spawns: Arc<Mutex<Vec<Instant>>>,
        ticks: Arc<AtomicUsize>,
    ) -> impl FnMut() -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
        let runs = Arc::new(AtomicUsize::new(0));
        move || {
            let runs = runs.clone();
            let spawns = spawns.clone();
            let ticks = ticks.clone();
            Box::pin(async move {
                spawns.lock().unwrap().push(Instant::now());
                if runs.fetch_add(1, Ordering::SeqCst) < n {
                    panic!("injected panic");
                }
                loop {
                    ticks.fetch_add(1, Ordering::SeqCst);
                    time::sleep(Duration::from_secs(1)).await;
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn respawns_exactly_n_times_with_backoff() {
        let spawns = Arc::new(Mutex::new(Vec::new()));
        let ticks = Arc::new(AtomicUsize::new(0));
        let sup = tokio::spawn({
            let task = flaky(4, spawns.clone(), ticks.clone());
            async move { supervise("flaky", backoff(), task).await }
        });

        time::sleep(Duration::from_secs(60)).await;
        assert!(!sup.is_finished());

        let spawns = spawns.lock().unwrap().clone();
        // 4 panicking runs + the one that stays up.
        assert_eq!(spawns.len(), 5);
        let gaps: Vec<u64> = spawns.windows(2).map(|w| (w[1] - w[0]).as_secs()).collect();
        assert_eq!(gaps, [1, 2, 4, 4]);
        assert!(ticks.load(Ordering::SeqCst) > 0);
        sup.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_task_does_not_affect_sibling() {
        let bad_spawns = Arc::new(Mutex::new(Vec::new()));
        let good_spawns = Arc::new(Mutex::new(Vec::new()));
        let good_ticks = Arc::new(AtomicUsize::new(0));

        let bad = tokio::spawn({
            let task = flaky(usize::MAX, bad_spawns.clone(), Arc::new(AtomicUsize::new(0)));
            async move { supervise("bad", backoff(), task).await }
        });
        let good = tokio::spawn({
            let task = flaky(0, good_spawns.clone(), good_ticks.clone());
            async move { supervise("good", backoff(), task).await }
        });

        time::sleep(Duration::from_millis(30_500)).await;

        assert!(bad_spawns.lock().unwrap().len() > 5);
        assert_eq!(good_spawns.lock().unwrap().len(), 1);
        // Ticks at t=0..=30, uninterrupted by the sibling's panics.
        assert_eq!(good_ticks.load(Ordering::SeqCst), 31);
        bad.abort();
        good.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn normal_exit_ends_supervision() {
        let runs = Arc::new(AtomicUsize::new(0));
        supervise("oneshot", backoff(), {
            let runs = runs.clone();
            move || {
                let runs = runs.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
        .await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn aborting_supervisor_aborts_running_task() {
        let ticks = Arc::new(AtomicUsize::new(0));
        let sup = tokio::spawn({
            let task = flaky(0, Arc::new(Mutex::new(Vec::new())), ticks.clone());
            async move { supervise("child", backoff(), task).await }
        });

        time::sleep(Duration::from_millis(2_500)).await;
        sup.abort();
        let _ = sup.await;
        let after_abort = ticks.load(Ordering::SeqCst);
        time::sleep(Duration::from_secs(10)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), after_abort);
    }
}
