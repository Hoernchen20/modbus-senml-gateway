//! End to end through `run()` (§10): a mock Modbus/TCP server feeding an
//! embedded TLS broker via the real poller → aggregator → publisher chain,
//! with a short poll interval and a 1s aggregation window, then graceful
//! shutdown within the grace period.

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::device;
use common::mqtt::{generate_certs, start_broker, Observer, TestCerts, PASSWORD, USERNAME};
use common::{register_value, MockTcpServer};
use modbus_senml_gateway::config::schema::{
    Config, ConnectionConfig, GatewayConfig, MqttConfig, Transport,
};
use modbus_senml_gateway::{run, RunError, SHUTDOWN_GRACE};
use serde_json::Value;
use tokio::sync::oneshot;

const GATEWAY_ID: &str = "gw-e2e";
const PREFIX: &str = "urn:dev:gw-e2e:";
const STATUS: &str = "telemetry/gw-e2e/status";
const PASSWORD_ENV: &str = "MODBUS_GW_E2E_PASSWORD";
const TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on `run()` returning after shutdown: the publisher's grace
/// plus the status writer's final write, with slack for a loaded CI box.
const SHUTDOWN_BOUND: Duration = Duration::from_millis(SHUTDOWN_GRACE.as_millis() as u64 + 1500);

fn status_file_path() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "modbus-gw-e2e-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("status")
}

fn config(server: &MockTcpServer, certs: &TestCerts, broker_port: u16, status: &Path) -> Config {
    Config {
        gateway: GatewayConfig {
            id: GATEWAY_ID.to_string(),
            aggregation_window_secs: 1,
            base_name_prefix: PREFIX.to_string(),
            log_level: "info".to_string(),
            status_file_path: status.display().to_string(),
            status_debounce_secs: 1,
            status_heartbeat_secs: 30,
        },
        mqtt: MqttConfig {
            broker_host: "localhost".to_string(),
            broker_port,
            client_id: format!("modbus-gateway-e2e-{broker_port}"),
            tls: true,
            ca_cert_path: certs.ca_path.display().to_string(),
            username: USERNAME.to_string(),
            password_env: PASSWORD_ENV.to_string(),
            qos: 1,
            keep_alive_secs: 30,
            topic_template: "telemetry/{device}".to_string(),
        },
        connections: vec![ConnectionConfig {
            id: "tcp-e2e".to_string(),
            transport: Transport::Tcp {
                host: server.addr.ip().to_string(),
                port: server.addr.port(),
            },
            poll_interval_secs: 1,
            io_timeout_ms: 200,
            reconnect_backoff_min_secs: 1,
            reconnect_backoff_max_secs: 2,
            devices: vec![device("meter1", 1)],
        }],
    }
}

/// Spawns `run()` with a shutdown trigger; the handle yields its result.
fn spawn_run(
    config: Config,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), RunError>>,
) {
    // Every test uses the same variable and value, so parallel tests setting
    // it concurrently can't observe anything but the right password.
    std::env::set_var(PASSWORD_ENV, PASSWORD);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(run(config, async {
        let _ = shutdown_rx.await;
    }));
    (shutdown_tx, handle)
}

#[tokio::test]
async fn batches_publish_end_to_end_then_shutdown_is_clean_and_bounded() {
    let server = MockTcpServer::start().await;
    let certs = generate_certs();
    let broker = start_broker(&certs);
    let mut observer = Observer::start(&broker).await;
    let status_path = status_file_path();

    let (shutdown, handle) = spawn_run(config(&server, &certs, broker.tls_port, &status_path));

    assert_eq!(observer.expect(STATUS, TIMEOUT).await.payload, b"online");

    let msg = observer.expect("telemetry/meter1", TIMEOUT).await;
    let pack: Value = serde_json::from_slice(&msg.payload).unwrap();
    let records = pack.as_array().expect("SenML pack is an array");
    assert_eq!(records[0]["bn"], format!("{PREFIX}meter1:"));
    let value_of = |name: &str| {
        records
            .iter()
            .find(|r| r["n"] == name)
            .unwrap_or_else(|| panic!("no record '{name}' in {pack}"))["v"]
            .as_f64()
            .unwrap()
    };
    // Every sample of a point is the same register value, so the mean is too.
    assert_eq!(value_of("holding"), register_value(1, 0x03, 10) as f64);
    assert_eq!(value_of("input"), register_value(1, 0x04, 20) as f64);

    let started = Instant::now();
    shutdown.send(()).unwrap();
    let result = tokio::time::timeout(SHUTDOWN_BOUND, handle)
        .await
        .expect("run() did not return within the shutdown bound")
        .expect("run() panicked");
    assert!(result.is_ok(), "run() failed: {result:?}");
    assert!(started.elapsed() < SHUTDOWN_BOUND);

    // A clean disconnect: the gateway's own retained "offline", not an LWT
    // after a keep-alive timeout (which would take 1.5 × 30s to fire).
    assert_eq!(
        observer
            .expect(STATUS, Duration::from_secs(2))
            .await
            .payload,
        b"offline"
    );
    // Polling stopped: the server sees the connection go away.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let accepts = server.accepts().len();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(server.accepts().len(), accepts, "poller still running");

    // The final write after shutdown reflects the clean MQTT disconnect.
    let status = std::fs::read_to_string(&status_path).expect("status file written");
    assert!(
        status.contains("mqtt: disconnected"),
        "status file: {status}"
    );
    assert!(
        status.contains("connection tcp-e2e: up, 1/1 devices ok"),
        "status file: {status}"
    );
}

#[tokio::test]
async fn shutdown_with_unreachable_broker_returns_after_grace() {
    let server = MockTcpServer::start().await;
    let certs = generate_certs();
    // Nothing listens here: the publisher can never drain.
    let dead_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let status_path = status_file_path();

    let (shutdown, handle) = spawn_run(config(&server, &certs, dead_port, &status_path));
    // Let at least one window close so a batch is stuck in the queue.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let started = Instant::now();
    shutdown.send(()).unwrap();
    let result = tokio::time::timeout(SHUTDOWN_BOUND, handle)
        .await
        .expect("run() did not return within the shutdown bound")
        .expect("run() panicked");
    assert!(result.is_ok(), "run() failed: {result:?}");
    assert!(
        started.elapsed() >= SHUTDOWN_GRACE,
        "returned before the grace period without draining"
    );
}

#[tokio::test]
async fn missing_password_fails_before_starting() {
    let server = MockTcpServer::start().await;
    let certs = generate_certs();
    let mut cfg = config(&server, &certs, 1, &status_file_path());
    cfg.mqtt.password_env = "MODBUS_GW_E2E_UNSET_PASSWORD".to_string();

    let result = run(cfg, std::future::pending()).await;
    assert!(
        matches!(result, Err(RunError::MqttSetup(_))),
        "got {result:?}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.accepts().is_empty(), "poller started anyway");
}
