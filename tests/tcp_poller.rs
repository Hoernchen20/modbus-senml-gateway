mod common;

use std::time::Duration;

use common::{
    by_point, collect_for, device, key, register_value, MockTcpServer, ServerMode, UnitBehavior,
};
use modbus_senml_gateway::config::schema::{ConnectionConfig, DeviceConfig, Transport};
use modbus_senml_gateway::modbus::poller::run_connection;
use tokio::sync::mpsc;

fn connection(server: &MockTcpServer, devices: Vec<DeviceConfig>) -> ConnectionConfig {
    ConnectionConfig {
        id: "tcp-test".to_string(),
        transport: Transport::Tcp {
            host: server.addr.ip().to_string(),
            port: server.addr.port(),
        },
        poll_interval_secs: 1,
        io_timeout_ms: 200,
        reconnect_backoff_min_secs: 1,
        reconnect_backoff_max_secs: 2,
        devices,
    }
}

#[tokio::test]
async fn multiple_devices_on_one_connection_are_polled_and_decoded() {
    let server = MockTcpServer::start().await;
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&server, vec![device("meter1", 1), device("meter2", 2)]),
        tx,
    ));

    // First tick fires immediately after connect.
    let readings = collect_for(&mut rx, Duration::from_millis(500)).await;
    let values = by_point(&readings);

    assert_eq!(values.len(), 4, "got {readings:?}");
    assert_eq!(
        values[&key("meter1", "holding")],
        register_value(1, 0x03, 10) as f64
    );
    assert_eq!(
        values[&key("meter1", "input")],
        register_value(1, 0x04, 20) as f64
    );
    assert_eq!(
        values[&key("meter2", "holding")],
        register_value(2, 0x03, 10) as f64
    );
    assert_eq!(
        values[&key("meter2", "input")],
        register_value(2, 0x04, 20) as f64
    );
    assert_eq!(server.accepts().len(), 1);

    poller.abort();
}

#[tokio::test]
async fn modbus_exception_skips_only_that_device_and_keeps_connection() {
    let server = MockTcpServer::start().await;
    server.set_unit(1, UnitBehavior::Exception(0x02)); // illegal data address
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&server, vec![device("meter1", 1), device("meter2", 2)]),
        tx,
    ));

    // Two ticks' worth.
    let readings = collect_for(&mut rx, Duration::from_millis(1500)).await;

    assert!(
        readings.iter().all(|r| r.point_id.device == "meter2"),
        "got {readings:?}"
    );
    assert_eq!(
        readings.len(),
        4,
        "meter2's two points on each of two ticks"
    );
    assert_eq!(
        server.accepts().len(),
        1,
        "an exception must not trigger a reconnect"
    );

    poller.abort();
}

#[tokio::test]
async fn unanswered_request_times_out_and_skips_only_that_device() {
    let server = MockTcpServer::start().await;
    server.set_unit(1, UnitBehavior::Silent);
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&server, vec![device("meter1", 1), device("meter2", 2)]),
        tx,
    ));

    let readings = collect_for(&mut rx, Duration::from_millis(1500)).await;

    assert!(
        readings.iter().all(|r| r.point_id.device == "meter2"),
        "got {readings:?}"
    );
    assert_eq!(
        readings.len(),
        4,
        "meter2's two points on each of two ticks"
    );
    assert_eq!(
        server.accepts().len(),
        1,
        "a timeout must not trigger a reconnect"
    );

    poller.abort();
}

#[tokio::test]
async fn socket_close_reconnects_with_capped_backoff_that_resets_after_success() {
    let server = MockTcpServer::start().await;
    server.set_mode(ServerMode::CloseOnRequest);
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&server, vec![device("meter1", 1)]),
        tx,
    ));

    // Backoff min 1s, max 2s: reconnect gaps should run 1s, 2s, 2s.
    while server.accepts().len() < 4 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let accepts = server.accepts();
    let gaps: Vec<f64> = accepts
        .windows(2)
        .map(|w| (w[1] - w[0]).as_secs_f64())
        .collect();
    for (gap, expected) in gaps.iter().zip([1.0, 2.0, 2.0]) {
        assert!(
            (gap - expected).abs() < 0.3,
            "reconnect gaps {gaps:?}, expected ~[1, 2, 2]"
        );
    }

    // Let the server answer again: polling resumes on the next reconnect.
    server.set_mode(ServerMode::Normal);
    let reading = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("polling should resume after reconnect")
        .unwrap();
    assert_eq!(reading.point_id.device, "meter1");

    // The successful read reset the backoff: the next drop reconnects after
    // ~1s (min), not the 2s cap it had reached.
    let closes_before = server.closes().len();
    let accepts_before = server.accepts().len();
    server.set_mode(ServerMode::CloseOnRequest);
    while server.accepts().len() <= accepts_before {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Index rather than `.last()`: the reconnected socket may already have
    // been closed again by the time this loop notices the accept.
    let close = server.closes()[closes_before];
    let accept = server.accepts()[accepts_before];
    let gap = (accept - close).as_secs_f64();
    assert!(
        (gap - 1.0).abs() < 0.3,
        "post-success reconnect gap {gap}, expected ~1s"
    );

    poller.abort();
}

#[tokio::test]
async fn connection_refused_is_retried() {
    // Grab a free port, then close it so connects are refused.
    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let mut cfg = ConnectionConfig {
        id: "refused".to_string(),
        transport: Transport::Tcp {
            host: addr.ip().to_string(),
            port: addr.port(),
        },
        poll_interval_secs: 1,
        io_timeout_ms: 200,
        reconnect_backoff_min_secs: 1,
        reconnect_backoff_max_secs: 1,
        devices: vec![device("meter1", 1)],
    };
    cfg.devices[0].blocks.truncate(1);
    let (tx, _rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(cfg, tx));

    // Start listening on that port after the first failed attempt; the
    // poller must pick it up on a retry.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("poller should retry the connect")
        .unwrap();
    drop(stream);

    poller.abort();
}
