use std::time::Duration;

use rumqttc::{
    AsyncClient, ClientError, Event, EventLoop, LastWill, MqttOptions, Outgoing, Packet, QoS,
    TlsConfiguration, Transport,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tracing::{debug, error, info, warn};

use crate::config::schema::{GatewayConfig, MqttConfig};
use crate::senml::encode_senml;
use crate::types::AggregatedBatch;

/// Capacity of rumqttc's request queue (§9): at one publish per device per
/// window this is well over an hour of buffering while the broker is away.
pub const QUEUE_CAPACITY: usize = 100;

/// rumqttc defaults to 10 KiB, which a device with a couple of hundred
/// points could exceed as one SenML pack.
const MAX_PACKET_SIZE: usize = 256 * 1024;

const RECONNECT_DELAY: Duration = Duration::from_millis(500);

const ONLINE: &str = "online";
const OFFLINE: &str = "offline";

#[derive(Debug, Error)]
pub enum MqttSetupError {
    #[error("mqtt.password_env: environment variable '{var}' is not set")]
    PasswordMissing { var: String },

    #[error("mqtt.ca_cert_path '{path}': failed to read file: {source}")]
    CaCert {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("mqtt.qos = {0} is not a valid QoS (allowed: 0, 1, 2)")]
    InvalidQos(u8),
}

/// Retained online/offline status topic, also the LWT topic (§9). A fixed
/// pattern, independent of `topic_template`.
pub fn status_topic(gateway_id: &str) -> String {
    format!("telemetry/{gateway_id}/status")
}

/// Telemetry topic for one device's pack: `topic_template` with `{device}`
/// substituted.
pub fn batch_topic(topic_template: &str, device_id: &str) -> String {
    topic_template.replace("{device}", device_id)
}

pub fn qos(value: u8) -> Result<QoS, MqttSetupError> {
    rumqttc::qos(value).map_err(|_| MqttSetupError::InvalidQos(value))
}

/// Reads the broker password from the environment variable named by
/// `mqtt.password_env` — it never lives in the config file.
pub fn read_password(cfg: &MqttConfig) -> Result<String, MqttSetupError> {
    std::env::var(&cfg.password_env).map_err(|_| MqttSetupError::PasswordMissing {
        var: cfg.password_env.clone(),
    })
}

/// Persistent session with a stable client id, credentials, retained
/// "offline" LWT on the status topic, and — with `tls` — a trust store
/// holding only the CA at `ca_cert_path` (no system roots, no client cert).
pub fn build_mqtt_options(
    cfg: &MqttConfig,
    gateway_id: &str,
    password: String,
) -> Result<MqttOptions, MqttSetupError> {
    let mut opts = MqttOptions::new(&cfg.client_id, &cfg.broker_host, cfg.broker_port);
    opts.set_clean_session(false)
        .set_keep_alive(Duration::from_secs(cfg.keep_alive_secs))
        .set_credentials(&cfg.username, password)
        .set_max_packet_size(MAX_PACKET_SIZE, MAX_PACKET_SIZE)
        .set_last_will(LastWill::new(
            status_topic(gateway_id),
            OFFLINE,
            QoS::AtLeastOnce,
            true,
        ));

    if cfg.tls {
        // Already parsed once during config validation (§5); a read failure
        // here means the file changed underneath us since startup.
        let ca = std::fs::read(&cfg.ca_cert_path).map_err(|e| MqttSetupError::CaCert {
            path: cfg.ca_cert_path.clone(),
            source: e,
        })?;
        opts.set_transport(Transport::Tls(TlsConfiguration::Simple {
            ca,
            alpn: None,
            client_auth: None,
        }));
    }

    Ok(opts)
}

/// Publishes each batch as one SenML pack (§8, §9) until `rx` closes, then
/// publishes a retained "offline" (a clean DISCONNECT suppresses the LWT),
/// disconnects and returns once the DISCONNECT is on the wire. Queued
/// batches ahead of it are written first; if the broker is unreachable
/// this never returns, so shutdown must bound it with a timeout. Dropping
/// the future stops the eventloop task too.
pub async fn run_publisher(
    mut rx: mpsc::Receiver<AggregatedBatch>,
    options: MqttOptions,
    mqtt: MqttConfig,
    gateway: GatewayConfig,
) -> Result<(), MqttSetupError> {
    let qos = qos(mqtt.qos)?;
    let status_topic = status_topic(&gateway.id);
    let (client, eventloop) = AsyncClient::new(options, QUEUE_CAPACITY);

    let eventloop_task = tokio::spawn(drive_eventloop(
        eventloop,
        client.clone(),
        status_topic.clone(),
    ));
    let _guard = AbortOnDrop(eventloop_task.abort_handle());

    while let Some(batch) = rx.recv().await {
        let topic = batch_topic(&mqtt.topic_template, &batch.device_id);
        let payload = encode_senml(&batch, &gateway.base_name_prefix);
        // try_publish, not publish().await: a full queue must drop this
        // batch rather than stall the aggregator behind a dead broker.
        match client.try_publish(topic, qos, false, payload) {
            Ok(()) => debug!(device = %batch.device_id, "batch queued for publish"),
            Err(ClientError::TryRequest(_)) => error!(
                device = %batch.device_id,
                capacity = QUEUE_CAPACITY,
                "mqtt publish queue full, dropping batch"
            ),
            Err(e) => error!(error = %e, device = %batch.device_id, "mqtt publish failed"),
        }
    }

    info!("publisher input closed, disconnecting from mqtt broker");
    if let Err(e) = client
        .publish(&status_topic, QoS::AtLeastOnce, true, OFFLINE)
        .await
    {
        warn!(error = %e, "failed to queue offline status");
    }
    if let Err(e) = client.disconnect().await {
        warn!(error = %e, "failed to queue mqtt disconnect");
    }
    let _ = eventloop_task.await;
    Ok(())
}

/// The only thing driving rumqttc's reconnects, keep-alives and acks (§9).
/// Returns once a requested DISCONNECT has been sent.
async fn drive_eventloop(mut eventloop: EventLoop, client: AsyncClient, status_topic: String) {
    // Warn once per outage (including a broker that's unreachable at
    // startup), not on every 500ms retry while it stays away.
    let mut outage_logged = false;
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                outage_logged = false;
                info!(session_present = ack.session_present, "mqtt connected");
                // Re-assert "online" on every connect: an unclean drop since
                // the last one will have fired the "offline" LWT. Spawned
                // because this loop is what drains the request queue, so
                // awaiting a publish into a full queue here would deadlock.
                let client = client.clone();
                let topic = status_topic.clone();
                tokio::spawn(async move {
                    if let Err(e) = client.publish(topic, QoS::AtLeastOnce, true, ONLINE).await {
                        warn!(error = %e, "failed to queue online status");
                    }
                });
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                info!("mqtt disconnected");
                return;
            }
            Ok(_) => {}
            Err(e) => {
                if outage_logged {
                    debug!(error = %e, "mqtt reconnect attempt failed");
                } else {
                    warn!(error = %e, "mqtt connection error, reconnecting");
                    outage_logged = true;
                }
                tokio::time::sleep(RECONNECT_DELAY).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mqtt_config(tls: bool, ca_cert_path: &str) -> MqttConfig {
        MqttConfig {
            broker_host: "10.0.0.5".to_string(),
            broker_port: 8883,
            client_id: "modbus-gateway-router1".to_string(),
            tls,
            ca_cert_path: ca_cert_path.to_string(),
            username: "gateway".to_string(),
            password_env: "MODBUS_GW_TEST_UNSET_PASSWORD_VAR".to_string(),
            qos: 1,
            keep_alive_secs: 30,
            topic_template: "telemetry/{device}".to_string(),
        }
    }

    fn valid_ca_cert_path() -> &'static str {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/config/testdata/valid_ca.pem"
        )
    }

    #[test]
    fn status_topic_uses_gateway_id() {
        assert_eq!(status_topic("gw-router1"), "telemetry/gw-router1/status");
    }

    #[test]
    fn batch_topic_substitutes_device() {
        assert_eq!(
            batch_topic("telemetry/{device}", "meter1"),
            "telemetry/meter1"
        );
        assert_eq!(
            batch_topic("site/a/{device}/senml", "tempsensor2"),
            "site/a/tempsensor2/senml"
        );
    }

    #[test]
    fn options_use_persistent_session_and_retained_offline_will() {
        let cfg = mqtt_config(false, valid_ca_cert_path());
        let opts = build_mqtt_options(&cfg, "gw-router1", "secret".to_string()).unwrap();

        assert!(!opts.clean_session());
        assert_eq!(opts.client_id(), "modbus-gateway-router1");
        assert_eq!(opts.keep_alive(), Duration::from_secs(30));
        let login = opts.credentials().expect("credentials must be set");
        assert_eq!(login.username, "gateway");
        assert_eq!(login.password, "secret");

        let will = opts.last_will().expect("LWT must be set");
        assert_eq!(will.topic, "telemetry/gw-router1/status");
        assert_eq!(&will.message[..], b"offline");
        assert_eq!(will.qos, QoS::AtLeastOnce);
        assert!(will.retain);
    }

    #[test]
    fn tls_trusts_only_the_configured_ca() {
        let cfg = mqtt_config(true, valid_ca_cert_path());
        let opts = build_mqtt_options(&cfg, "gw", "pw".to_string()).unwrap();

        match opts.transport() {
            Transport::Tls(TlsConfiguration::Simple {
                ca, client_auth, ..
            }) => {
                assert_eq!(ca, std::fs::read(valid_ca_cert_path()).unwrap());
                assert!(client_auth.is_none(), "mTLS is a non-goal");
            }
            _ => panic!("expected TLS transport with CA only"),
        }
    }

    #[test]
    fn plain_tcp_when_tls_disabled() {
        let cfg = mqtt_config(false, valid_ca_cert_path());
        let opts = build_mqtt_options(&cfg, "gw", "pw".to_string()).unwrap();
        assert!(matches!(opts.transport(), Transport::Tcp));
    }

    #[test]
    fn missing_ca_file_is_an_error() {
        let cfg = mqtt_config(true, "/nonexistent/ca.pem");
        assert!(matches!(
            build_mqtt_options(&cfg, "gw", "pw".to_string()),
            Err(MqttSetupError::CaCert { .. })
        ));
    }

    #[test]
    fn unset_password_env_is_an_error() {
        let cfg = mqtt_config(false, valid_ca_cert_path());
        assert!(matches!(
            read_password(&cfg),
            Err(MqttSetupError::PasswordMissing { var }) if var == "MODBUS_GW_TEST_UNSET_PASSWORD_VAR"
        ));
    }

    #[test]
    fn qos_maps_valid_levels_and_rejects_others() {
        assert_eq!(qos(0).unwrap(), QoS::AtMostOnce);
        assert_eq!(qos(1).unwrap(), QoS::AtLeastOnce);
        assert_eq!(qos(2).unwrap(), QoS::ExactlyOnce);
        assert!(matches!(qos(3), Err(MqttSetupError::InvalidQos(3))));
    }
}
