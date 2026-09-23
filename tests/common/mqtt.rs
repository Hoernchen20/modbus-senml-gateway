//! MQTT test helpers: throwaway CA + server cert, an embedded rumqttd broker
//! with a TLS listener (for the gateway) and a plain one (for observers), a
//! TCP proxy that can stall or sever the gateway's connection, and an
//! observer client that records everything published under `telemetry/#`.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, ConnectionSettings, RouterConfig, ServerSettings, TlsConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;

pub const USERNAME: &str = "gateway";
pub const PASSWORD: &str = "secret";

pub struct TestCerts {
    pub ca_path: PathBuf,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// A fresh CA and a `localhost` server cert signed by it, written to a
/// unique temp directory.
pub fn generate_certs() -> TestCerts {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "modbus-gw-mqtt-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "modbus-gw test CA");
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);

    let server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let certs = TestCerts {
        ca_path: dir.join("ca.pem"),
        cert_path: dir.join("server.pem"),
        key_path: dir.join("server.key"),
    };
    std::fs::write(&certs.ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&certs.cert_path, server_cert.pem()).unwrap();
    std::fs::write(&certs.key_path, server_key.serialize_pem()).unwrap();
    certs
}

pub struct TestBroker {
    /// TLS listener requiring `USERNAME`/`PASSWORD`; what the gateway uses.
    pub tls_port: u16,
    /// Unauthenticated plain listener for observers.
    pub plain_port: u16,
}

fn free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn server_settings(
    name: &str,
    port: u16,
    tls: Option<TlsConfig>,
    auth: Option<HashMap<String, String>>,
) -> ServerSettings {
    ServerSettings {
        name: name.to_string(),
        listen: SocketAddr::from(([127, 0, 0, 1], port)),
        tls,
        next_connection_delay_ms: 1,
        connections: ConnectionSettings {
            connection_timeout_ms: 5000,
            max_payload_size: 1024 * 1024,
            max_inflight_count: 100,
            auth,
            external_auth: None,
            dynamic_filters: false,
        },
    }
}

/// Starts rumqttd on its own threads (it runs its own runtimes and never
/// returns) and waits until both listeners accept connections.
pub fn start_broker(certs: &TestCerts) -> TestBroker {
    let tls_port = free_port();
    let plain_port = free_port();

    let tls = TlsConfig::Rustls {
        capath: None,
        certpath: certs.cert_path.display().to_string(),
        keypath: certs.key_path.display().to_string(),
    };
    let auth = HashMap::from([(USERNAME.to_string(), PASSWORD.to_string())]);

    let config = rumqttd::Config {
        id: 0,
        router: RouterConfig {
            max_connections: 100,
            max_outgoing_packet_count: 200,
            max_segment_size: 10 * 1024 * 1024,
            max_segment_count: 10,
            ..Default::default()
        },
        v4: Some(HashMap::from([
            (
                "tls".to_string(),
                server_settings("tls", tls_port, Some(tls), Some(auth)),
            ),
            (
                "plain".to_string(),
                server_settings("plain", plain_port, None, None),
            ),
        ])),
        ..Default::default()
    };

    std::thread::spawn(move || {
        let mut broker = Broker::new(config);
        broker.start().expect("broker failed");
    });

    for port in [tls_port, plain_port] {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while StdTcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "broker never listened on {port}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    TestBroker {
        tls_port,
        plain_port,
    }
}

#[derive(Default)]
struct ProxyState {
    connections: Mutex<Vec<AbortHandle>>,
    refusing: AtomicBool,
}

/// TCP pass-through in front of the broker's TLS port. It can hold back
/// client→broker bytes (`freeze`), sever every live connection without an
/// MQTT DISCONNECT (`sever`), and drop new connections on accept (`refuse`).
pub struct Proxy {
    pub port: u16,
    state: Arc<ProxyState>,
    frozen: watch::Sender<bool>,
}

impl Proxy {
    pub async fn start(upstream_port: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(ProxyState::default());
        let (frozen, _) = watch::channel(false);

        let accept_state = state.clone();
        let accept_frozen = frozen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                if accept_state.refusing.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                let Ok(upstream) = TcpStream::connect(("127.0.0.1", upstream_port)).await else {
                    continue;
                };
                let (client_rx, client_tx) = client.into_split();
                let (upstream_rx, upstream_tx) = upstream.into_split();
                let up = tokio::spawn(pump(
                    client_rx,
                    upstream_tx,
                    Some(accept_frozen.subscribe()),
                ));
                let down = tokio::spawn(pump(upstream_rx, client_tx, None));
                accept_state
                    .connections
                    .lock()
                    .unwrap()
                    .extend([up.abort_handle(), down.abort_handle()]);
            }
        });

        Proxy {
            port,
            state,
            frozen,
        }
    }

    /// Stop forwarding client→broker bytes; they're held, not delivered.
    pub fn freeze(&self) {
        self.frozen.send_replace(true);
    }

    pub fn thaw(&self) {
        self.frozen.send_replace(false);
    }

    /// Drop every proxied connection. The broker sees the socket close with
    /// no DISCONNECT (an unclean drop); anything held by `freeze` is lost.
    pub fn sever(&self) {
        for handle in self.state.connections.lock().unwrap().drain(..) {
            handle.abort();
        }
    }

    pub fn refuse(&self, refuse: bool) {
        self.state.refusing.store(refuse, Ordering::SeqCst);
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut frozen: Option<watch::Receiver<bool>>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if let Some(frozen) = frozen.as_mut() {
            if frozen.wait_for(|f| !*f).await.is_err() {
                return;
            }
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[derive(Debug, Clone)]
pub struct Received {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retain: bool,
}

/// A plain-TCP client subscribed to `telemetry/#`.
pub struct Observer {
    rx: mpsc::UnboundedReceiver<Received>,
    _eventloop: AbortOnDrop,
}

static OBSERVER_ID: AtomicUsize = AtomicUsize::new(0);

impl Observer {
    /// Returns once the subscription is acknowledged, so anything published
    /// afterwards is guaranteed to be seen.
    pub async fn start(broker: &TestBroker) -> Self {
        let id = format!("observer-{}", OBSERVER_ID.fetch_add(1, Ordering::Relaxed));
        let opts = MqttOptions::new(id, "127.0.0.1", broker.plain_port);
        let (client, mut eventloop) = AsyncClient::new(opts, 10);
        client
            .subscribe("telemetry/#", QoS::AtLeastOnce)
            .await
            .unwrap();

        let (tx, rx) = mpsc::unbounded_channel();
        let (subacked_tx, subacked_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _client = client;
            let mut subacked_tx = Some(subacked_tx);
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::SubAck(_))) => {
                        if let Some(tx) = subacked_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let _ = tx.send(Received {
                            topic: p.topic.clone(),
                            payload: p.payload.to_vec(),
                            retain: p.retain,
                        });
                    }
                    Ok(_) => {}
                    Err(e) => panic!("observer connection failed: {e}"),
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(5), subacked_rx)
            .await
            .expect("observer subscription timed out")
            .unwrap();

        Observer {
            rx,
            _eventloop: AbortOnDrop(task.abort_handle()),
        }
    }

    /// The next message on `topic`, skipping others; panics after `timeout`.
    pub async fn expect(&mut self, topic: &str, timeout: Duration) -> Received {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(msg)) if msg.topic == topic => return msg,
                Ok(Some(_)) => continue,
                Ok(None) => panic!("observer closed"),
                Err(_) => panic!("no message on {topic} within {timeout:?}"),
            }
        }
    }

    /// Asserts nothing arrives on `topic` for `window`.
    pub async fn expect_none(&mut self, topic: &str, window: Duration) {
        let deadline = tokio::time::Instant::now() + window;
        while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, self.rx.recv()).await {
            assert_ne!(msg.topic, topic, "unexpected message on {topic}: {msg:?}");
        }
    }
}

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
