mod common;

use std::time::Duration;

use common::rtu::{BusEvent, MockRtuBus};
use common::{by_point, collect_for, device, key, register_value, UnitBehavior};
use modbus_senml_gateway::config::schema::{ConnectionConfig, DeviceConfig, Parity, Transport};
use modbus_senml_gateway::modbus::poller::{is_transport_fatal, run_connection};
use modbus_senml_gateway::status::StatusReporter;
use tokio::sync::mpsc;
use tokio_modbus::client::Reader;
use tokio_modbus::Slave;

fn connection(
    bus: &MockRtuBus,
    inter_frame_delay_ms: u64,
    devices: Vec<DeviceConfig>,
) -> ConnectionConfig {
    ConnectionConfig {
        id: "rtu-test".to_string(),
        transport: Transport::Rtu {
            serial_port: bus.path.to_str().unwrap().to_string(),
            baud_rate: 9600,
            data_bits: 8,
            parity: Parity::None,
            stop_bits: 1,
            inter_frame_delay_ms,
        },
        poll_interval_secs: 1,
        io_timeout_ms: 200,
        reconnect_backoff_min_secs: 1,
        reconnect_backoff_max_secs: 2,
        devices,
    }
}

fn two_devices() -> Vec<DeviceConfig> {
    vec![device("sensor1", 1), device("sensor2", 2)]
}

/// Gaps between each reply and the request that followed it on the bus.
fn reply_to_request_gaps(events: &[BusEvent]) -> Vec<Duration> {
    events
        .windows(2)
        .filter_map(|w| match (w[0], w[1]) {
            (BusEvent::Response { at: reply, .. }, BusEvent::Request { at: request, .. }) => {
                Some(request - reply)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn two_units_share_the_bus_one_request_at_a_time() {
    let bus = MockRtuBus::start();
    // A slow responder: a poller that didn't wait for each reply would be
    // caught sending the next request while one is still pending.
    bus.set_response_delay(Duration::from_millis(30));
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&bus, 0, two_devices()),
        tx,
        StatusReporter::disabled(),
    ));

    // First tick only.
    let readings = collect_for(&mut rx, Duration::from_millis(500)).await;
    let values = by_point(&readings);

    assert_eq!(values.len(), 4, "got {readings:?}");
    assert_eq!(
        values[&key("sensor1", "holding")],
        register_value(1, 0x03, 10) as f64
    );
    assert_eq!(
        values[&key("sensor1", "input")],
        register_value(1, 0x04, 20) as f64
    );
    assert_eq!(
        values[&key("sensor2", "holding")],
        register_value(2, 0x03, 10) as f64
    );
    assert_eq!(
        values[&key("sensor2", "input")],
        register_value(2, 0x04, 20) as f64
    );

    assert_eq!(
        bus.overlaps(),
        0,
        "a request was sent while another was in flight"
    );
    let sequence: Vec<(char, u8)> = bus
        .events()
        .iter()
        .map(|e| match *e {
            BusEvent::Request { unit, .. } => ('>', unit),
            BusEvent::Response { unit, .. } => ('<', unit),
        })
        .collect();
    assert_eq!(
        sequence,
        [
            ('>', 1),
            ('<', 1),
            ('>', 1),
            ('<', 1),
            ('>', 2),
            ('<', 2),
            ('>', 2),
            ('<', 2)
        ],
        "requests and replies must strictly alternate"
    );

    poller.abort();
}

#[tokio::test]
async fn inter_frame_delay_enforces_a_minimum_gap_between_frames() {
    let bus = MockRtuBus::start();
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&bus, 50, two_devices()),
        tx,
        StatusReporter::disabled(),
    ));
    collect_for(&mut rx, Duration::from_millis(600)).await;
    poller.abort();

    let gaps = reply_to_request_gaps(&bus.events());
    assert_eq!(
        gaps.len(),
        3,
        "one tick: four requests, three reply→request gaps"
    );
    assert!(
        gaps.iter().all(|g| *g >= Duration::from_millis(50)),
        "gaps {gaps:?} should all be at least inter_frame_delay_ms (50ms)"
    );

    // Control: without the delay the same bus runs back-to-back, so the gap
    // above is the setting's doing, not incidental latency.
    let bus = MockRtuBus::start();
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&bus, 0, two_devices()),
        tx,
        StatusReporter::disabled(),
    ));
    collect_for(&mut rx, Duration::from_millis(300)).await;
    poller.abort();

    let gaps = reply_to_request_gaps(&bus.events());
    assert_eq!(gaps.len(), 3);
    assert!(
        gaps.iter().all(|g| *g < Duration::from_millis(25)),
        "without a delay gaps {gaps:?} should be well under 50ms"
    );
}

#[tokio::test]
async fn silent_unit_times_out_without_affecting_the_other() {
    let bus = MockRtuBus::start();
    bus.set_unit(1, UnitBehavior::Silent);
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&bus, 0, two_devices()),
        tx,
        StatusReporter::disabled(),
    ));

    // Two ticks' worth.
    let readings = collect_for(&mut rx, Duration::from_millis(1500)).await;
    poller.abort();

    assert!(
        readings.iter().all(|r| r.point_id.device == "sensor2"),
        "got {readings:?}"
    );
    assert_eq!(
        readings.len(),
        4,
        "sensor2's two points on each of two ticks"
    );

    // The bus stays quiet for the full io_timeout after the unanswered
    // request: the poller doesn't move on to unit 2 while unit 1 might still reply.
    let events = bus.events();
    let mut checked = 0;
    for w in events.windows(2) {
        if let (
            BusEvent::Request {
                unit: 1, at: asked, ..
            },
            BusEvent::Request {
                unit: 2, at: next, ..
            },
        ) = (w[0], w[1])
        {
            assert!(
                next - asked >= Duration::from_millis(200),
                "unit 2 polled {:?} after unit 1's unanswered request",
                next - asked
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 2, "one timeout per tick, events {events:?}");
    // Unit 1's second block is skipped along with the rest of the device.
    let unit1_requests = events
        .iter()
        .filter(|e| matches!(e, BusEvent::Request { unit: 1, .. }))
        .count();
    assert_eq!(unit1_requests, 2);
}

#[tokio::test]
async fn closing_the_pty_master_surfaces_as_a_transport_error() {
    let mut bus = MockRtuBus::start();
    let port =
        tokio_serial::SerialStream::open(&tokio_serial::new(bus.path.to_str().unwrap(), 9600))
            .unwrap();
    let mut ctx = tokio_modbus::client::rtu::attach_slave(port, Slave(1));

    let words = ctx.read_holding_registers(10, 1).await.unwrap().unwrap();
    assert_eq!(words, [register_value(1, 0x03, 10)]);

    bus.unplug().await;
    let err = tokio::time::timeout(Duration::from_secs(1), ctx.read_holding_registers(10, 1))
        .await
        .expect("a dead serial port must fail fast, not hang until timeout")
        .expect_err("read from a closed PTY must fail");
    assert!(
        is_transport_fatal(&err),
        "expected a transport error, got {err:?}"
    );
}

#[tokio::test]
async fn unplugged_adapter_is_reopened_after_replug() {
    let mut bus = MockRtuBus::start();
    let (tx, mut rx) = mpsc::channel(64);
    let poller = tokio::spawn(run_connection(
        connection(&bus, 0, vec![device("sensor1", 1)]),
        tx,
        StatusReporter::disabled(),
    ));

    let reading = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("first reading")
        .unwrap();
    assert_eq!(reading.point_id.device, "sensor1");
    collect_for(&mut rx, Duration::from_millis(200)).await;

    // While unplugged the port can't be opened at all: nothing arrives, and
    // the poller keeps retrying rather than giving up.
    bus.unplug().await;
    let readings = collect_for(&mut rx, Duration::from_millis(2500)).await;
    assert!(
        readings.is_empty(),
        "got {readings:?} from an unplugged bus"
    );

    bus.replug();
    let reading = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("polling should resume once the adapter is back (backoff max 2s)")
        .unwrap();
    assert_eq!(reading.point_id.device, "sensor1");

    poller.abort();
}
