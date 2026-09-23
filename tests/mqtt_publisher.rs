//! MQTT publisher (§9) against an embedded rumqttd over TLS with a
//! throwaway CA: topic/payload shape, retained online/offline status, the
//! LWT on an unclean drop, and QoS 1 redelivery across a forced disconnect.

mod common;

use std::time::Duration;

use common::mqtt::{
    generate_certs, start_broker, Observer, Proxy, TestBroker, TestCerts, PASSWORD, USERNAME,
};
use modbus_senml_gateway::config::schema::{GatewayConfig, MqttConfig};
use modbus_senml_gateway::mqtt::{build_mqtt_options, run_publisher};
use modbus_senml_gateway::senml::encode_senml;
use modbus_senml_gateway::status::{StatusEvent, StatusReporter, STATUS_CHANNEL_CAPACITY};
use modbus_senml_gateway::types::{AggregatedBatch, AggregatedPoint};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const GATEWAY_ID: &str = "gw-test";
const PREFIX: &str = "urn:dev:gw-test:";
const STATUS: &str = "telemetry/gw-test/status";
const TIMEOUT: Duration = Duration::from_secs(10);

fn gateway_config() -> GatewayConfig {
    GatewayConfig {
        id: GATEWAY_ID.to_string(),
        aggregation_window_secs: 60,
        base_name_prefix: PREFIX.to_string(),
        log_level: "info".to_string(),
        status_file_path: "/tmp/status".to_string(),
        status_debounce_secs: 2,
        status_heartbeat_secs: 30,
    }
}

fn mqtt_config(certs: &TestCerts, port: u16) -> MqttConfig {
    MqttConfig {
        // Must match the server cert's SAN; TLS is verified against our CA.
        broker_host: "localhost".to_string(),
        broker_port: port,
        client_id: "modbus-gateway-test".to_string(),
        tls: true,
        ca_cert_path: certs.ca_path.display().to_string(),
        username: USERNAME.to_string(),
        password_env: "UNUSED".to_string(),
        qos: 1,
        keep_alive_secs: 30,
        topic_template: "telemetry/{device}".to_string(),
    }
}

fn batch(device: &str, mean: f64) -> AggregatedBatch {
    AggregatedBatch {
        device_id: device.to_string(),
        window_start: 1758447120,
        window_end: 1758447180,
        points: vec![
            AggregatedPoint {
                name: "voltage_l1".to_string(),
                unit: "V".to_string(),
                mean,
            },
            AggregatedPoint {
                name: "current_l1".to_string(),
                unit: "A".to_string(),
                mean: 4.82,
            },
        ],
    }
}

struct Harness {
    broker: TestBroker,
    tx: mpsc::Sender<AggregatedBatch>,
    publisher: JoinHandle<()>,
    status: mpsc::Receiver<StatusEvent>,
}

impl Harness {
    async fn expect_status(&mut self, expected: StatusEvent) {
        let got = tokio::time::timeout(TIMEOUT, self.status.recv())
            .await
            .expect("no status event")
            .unwrap();
        assert_eq!(got, expected);
    }
}

/// Starts a publisher connected to `port`: the broker's TLS port directly,
/// or a proxy in front of it.
async fn start(certs: &TestCerts, broker: TestBroker, port: u16) -> Harness {
    let mqtt = mqtt_config(certs, port);
    let options = build_mqtt_options(&mqtt, GATEWAY_ID, PASSWORD.to_string()).unwrap();
    let (tx, rx) = mpsc::channel(16);
    let (status_tx, status) = mpsc::channel(STATUS_CHANNEL_CAPACITY);
    let reporter = StatusReporter::new(status_tx);
    let publisher = tokio::spawn(async move {
        run_publisher(rx, options, mqtt, gateway_config(), reporter)
            .await
            .unwrap();
    });
    Harness {
        broker,
        tx,
        publisher,
        status,
    }
}

#[tokio::test]
async fn publishes_senml_pack_to_device_topic_over_tls() {
    let certs = generate_certs();
    let broker = start_broker(&certs);
    let mut observer = Observer::start(&broker).await;
    let port = broker.tls_port;
    let h = start(&certs, broker, port).await;

    let online = observer.expect(STATUS, TIMEOUT).await;
    assert_eq!(online.payload, b"online");

    let sent = batch("meter1", 231.4);
    h.tx.send(sent.clone()).await.unwrap();

    let msg = observer.expect("telemetry/meter1", TIMEOUT).await;
    assert_eq!(msg.payload, encode_senml(&sent, PREFIX));
    assert!(!msg.retain, "telemetry must not be retained");
}

#[tokio::test]
async fn online_status_is_retained() {
    let certs = generate_certs();
    let broker = start_broker(&certs);
    let mut first = Observer::start(&broker).await;
    let port = broker.tls_port;
    let h = start(&certs, broker, port).await;
    first.expect(STATUS, TIMEOUT).await;

    // A subscriber arriving later still learns the gateway is online.
    let mut late = Observer::start(&h.broker).await;
    let msg = late.expect(STATUS, TIMEOUT).await;
    assert_eq!(msg.payload, b"online");
    assert!(msg.retain);
}

#[tokio::test]
async fn unclean_drop_fires_retained_offline_will_then_online_on_reconnect() {
    let certs = generate_certs();
    let broker = start_broker(&certs);
    let proxy = Proxy::start(broker.tls_port).await;
    let mut observer = Observer::start(&broker).await;
    let mut h = start(&certs, broker, proxy.port).await;
    assert_eq!(observer.expect(STATUS, TIMEOUT).await.payload, b"online");
    h.expect_status(StatusEvent::MqttConnected).await;

    // Keep the gateway away so its reconnect can't overwrite the will.
    proxy.refuse(true);
    proxy.sever();

    let will = observer.expect(STATUS, TIMEOUT).await;
    assert_eq!(will.payload, b"offline");
    let mut late = Observer::start(&h.broker).await;
    let retained = late.expect(STATUS, TIMEOUT).await;
    assert_eq!(retained.payload, b"offline");
    assert!(retained.retain);
    h.expect_status(StatusEvent::MqttDisconnected).await;

    proxy.refuse(false);
    assert_eq!(observer.expect(STATUS, TIMEOUT).await.payload, b"online");
    // Skip the repeated per-attempt disconnect reports from the outage.
    loop {
        let event = tokio::time::timeout(TIMEOUT, h.status.recv())
            .await
            .expect("no MqttConnected after reconnect")
            .unwrap();
        if event == StatusEvent::MqttConnected {
            break;
        }
        assert_eq!(event, StatusEvent::MqttDisconnected);
    }
}

#[tokio::test]
async fn qos1_publish_is_redelivered_after_forced_drop() {
    let certs = generate_certs();
    let broker = start_broker(&certs);
    let proxy = Proxy::start(broker.tls_port).await;
    let mut observer = Observer::start(&broker).await;
    let h = start(&certs, broker, proxy.port).await;
    observer.expect(STATUS, TIMEOUT).await;

    // The PUBLISH leaves the gateway but never reaches the broker, so it
    // stays unacknowledged in the client's session.
    proxy.freeze();
    let sent = batch("meter1", 229.9);
    h.tx.send(sent.clone()).await.unwrap();
    observer
        .expect_none("telemetry/meter1", Duration::from_millis(500))
        .await;

    // Sever (dropping the held bytes) and let it back in: only a resumed
    // persistent session makes the client retransmit.
    proxy.sever();
    proxy.thaw();

    let msg = observer.expect("telemetry/meter1", TIMEOUT).await;
    assert_eq!(msg.payload, encode_senml(&sent, PREFIX));
}

#[tokio::test]
async fn closing_input_publishes_offline_and_returns() {
    let certs = generate_certs();
    let broker = start_broker(&certs);
    let mut observer = Observer::start(&broker).await;
    let port = broker.tls_port;
    let Harness {
        broker,
        tx,
        publisher,
        mut status,
    } = start(&certs, broker, port).await;
    observer.expect(STATUS, TIMEOUT).await;

    // A batch queued just before shutdown still goes out ahead of it.
    let sent = batch("meter1", 230.0);
    tx.send(sent.clone()).await.unwrap();
    drop(tx);

    tokio::time::timeout(TIMEOUT, publisher)
        .await
        .expect("publisher did not return after its input closed")
        .unwrap();

    let msg = observer.expect("telemetry/meter1", TIMEOUT).await;
    assert_eq!(msg.payload, encode_senml(&sent, PREFIX));
    assert_eq!(observer.expect(STATUS, TIMEOUT).await.payload, b"offline");

    let mut late = Observer::start(&broker).await;
    let retained = late.expect(STATUS, TIMEOUT).await;
    assert_eq!(retained.payload, b"offline");
    assert!(retained.retain);

    let events: Vec<_> = std::iter::from_fn(|| status.try_recv().ok()).collect();
    assert_eq!(
        events,
        [StatusEvent::MqttConnected, StatusEvent::MqttDisconnected]
    );
}
