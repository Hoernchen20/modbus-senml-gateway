pub mod aggregator;
pub mod config;
pub mod modbus;
pub mod mqtt;
pub mod senml;
pub mod status;
pub mod syslog;
pub mod types;

use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet};
use tokio::time;
use tracing::{info, warn};

use crate::aggregator::{init_from_config, run_aggregator};
use crate::config::Config;
use crate::modbus::poller::{run_connection, Backoff};
use crate::modbus::supervisor::supervise;
use crate::mqtt::{build_mqtt_options, read_password, run_publisher, MqttSetupError};
use crate::status::{run_status_writer, StatusReporter, StatusState, STATUS_CHANNEL_CAPACITY};

/// Pollers → aggregator. Sized for bursts from many devices ticking at
/// once; pollers drop (and log) readings rather than block when it's full.
pub const READING_CHANNEL_CAPACITY: usize = 1024;

/// Aggregator → publisher: at most one batch per device per window.
pub const BATCH_CHANNEL_CAPACITY: usize = 64;

/// How long shutdown waits for the publisher to drain already-queued sends
/// and disconnect cleanly (§10).
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Bound on the status writer's final write during shutdown.
const STATUS_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    MqttSetup(#[from] MqttSetupError),

    #[error("{0} task exited unexpectedly")]
    TaskExited(&'static str),
}

/// Runs the gateway (§2) until `shutdown` resolves, then shuts down per
/// §10: polling stops, the partial aggregation window is dropped rather
/// than force-flushed, and the publisher gets `SHUTDOWN_GRACE` to drain
/// its queue and disconnect. Returns an error on MQTT setup failure, or if
/// the aggregator or publisher dies on its own.
pub async fn run(config: Config, shutdown: impl Future<Output = ()>) -> Result<(), RunError> {
    // Everything that can fail up front does so before any task starts.
    let password = read_password(&config.mqtt)?;
    let mqtt_options = build_mqtt_options(&config.mqtt, &config.gateway.id, password)?;
    mqtt::qos(config.mqtt.qos)?;

    let (status_tx, status_rx) = mpsc::channel(STATUS_CHANNEL_CAPACITY);
    let status = StatusReporter::new(status_tx);
    let status_writer = tokio::spawn(run_status_writer(
        status_rx,
        StatusState::from_config(&config),
        PathBuf::from(&config.gateway.status_file_path),
        Duration::from_secs(config.gateway.status_debounce_secs),
        Duration::from_secs(config.gateway.status_heartbeat_secs),
    ));

    let (batch_tx, batch_rx) = mpsc::channel(BATCH_CHANNEL_CAPACITY);
    let mut publisher = tokio::spawn(run_publisher(
        batch_rx,
        mqtt_options,
        config.mqtt.clone(),
        config.gateway.clone(),
        status.clone(),
    ));

    let (reading_tx, reading_rx) = mpsc::channel(READING_CHANNEL_CAPACITY);
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut aggregator = tokio::spawn(run_aggregator(
        reading_rx,
        batch_tx,
        init_from_config(&config),
        config.gateway.aggregation_window_secs,
        now_unix,
    ));

    // Started last, so the aggregator is already listening for their
    // first readings.
    let mut pollers = JoinSet::new();
    for conn in config.connections {
        // §6.2: a panicked connection respawns with its own connect backoff.
        let backoff = Backoff::new(
            Duration::from_secs(conn.reconnect_backoff_min_secs),
            Duration::from_secs(conn.reconnect_backoff_max_secs),
        );
        let tx = reading_tx.clone();
        let status = status.clone();
        pollers.spawn(async move {
            let name = conn.id.clone();
            supervise(&name, backoff, move || {
                run_connection(conn.clone(), tx.clone(), status.clone())
            })
            .await
        });
    }
    // From here on the pollers and publisher hold the only senders, so
    // their exit is what closes the aggregator and status channels.
    drop(reading_tx);
    drop(status);
    info!(gateway = %config.gateway.id, connections = pollers.len(), "gateway started");

    let failure = tokio::select! {
        () = shutdown => None,
        res = &mut publisher => Some(publisher_exit(res)),
        _ = &mut aggregator => Some(RunError::TaskExited("aggregator")),
    };

    // Aborting a supervisor also aborts its connection task (and with it
    // the last reading senders).
    pollers.shutdown().await;

    if let Some(err) = failure {
        aggregator.abort();
        publisher.abort();
        return Err(err);
    }
    info!("shutdown requested, polling stopped");

    // The aggregator returns without flushing once its input closes — the
    // partial window is intentionally lost (§10). Its exit closes the
    // publisher's input, which then drains, sends "offline" and disconnects.
    let drain = async {
        let _ = (&mut aggregator).await;
        let _ = (&mut publisher).await;
    };
    if time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        warn!(grace = ?SHUTDOWN_GRACE, "mqtt publisher did not drain in time, abandoning queued sends");
        aggregator.abort();
        publisher.abort();
        // Wait for the abort to take effect so the publisher's status
        // reporter is dropped and the status writer can finish.
        let _ = publisher.await;
    }

    // The status writer exits once its last sender is gone, writing out any
    // still-pending change first.
    if time::timeout(STATUS_FLUSH_TIMEOUT, status_writer)
        .await
        .is_err()
    {
        warn!("status writer did not finish in time");
    }

    info!("shutdown complete");
    Ok(())
}

fn publisher_exit(res: Result<Result<(), MqttSetupError>, JoinError>) -> RunError {
    match res {
        Ok(Err(e)) => e.into(),
        _ => RunError::TaskExited("mqtt publisher"),
    }
}
