use std::ffi::OsString;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use time::format_description::FormatItem;
use time::macros::format_description;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::time::{self as ttime, Instant};
use tracing::warn;

use crate::config::schema::Config;

/// Hard cap on the status file size (§11.2).
pub const MAX_STATUS_BYTES: usize = 2048;

/// Capacity of the `StatusEvent` channel. Pollers report every device on
/// every tick, so a dropped event is corrected on the next one.
pub const STATUS_CHANNEL_CAPACITY: usize = 256;

const TIMESTAMP: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]Z");
const TIME_OF_DAY: &[FormatItem<'static>] = format_description!("[hour]:[minute]:[second]Z");

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusEvent {
    ConnectionUp {
        connection_id: String,
    },
    ConnectionDown {
        connection_id: String,
    },
    DeviceOk {
        connection_id: String,
        device_id: String,
    },
    DeviceProblem {
        connection_id: String,
        device_id: String,
        reason: String,
    },
    MqttConnected,
    MqttDisconnected,
}

/// Sending side handed to pollers and the MQTT publisher. Never blocks: a
/// full channel drops the event rather than stalling polling or publishing.
#[derive(Debug, Clone, Default)]
pub struct StatusReporter(Option<mpsc::Sender<StatusEvent>>);

impl StatusReporter {
    pub fn new(tx: mpsc::Sender<StatusEvent>) -> Self {
        StatusReporter(Some(tx))
    }

    /// A reporter that discards every event, for callers without a status
    /// writer (tests).
    pub fn disabled() -> Self {
        StatusReporter(None)
    }

    pub fn report(&self, event: StatusEvent) {
        if let Some(tx) = &self.0 {
            let _ = tx.try_send(event);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Health {
    /// Not polled since the connection (re)connected.
    Unknown,
    Ok,
    Problem {
        reason: String,
        since: OffsetDateTime,
    },
}

#[derive(Debug, Clone)]
struct DeviceStatus {
    id: String,
    health: Health,
}

#[derive(Debug, Clone)]
struct ConnectionStatus {
    id: String,
    up: bool,
    devices: Vec<DeviceStatus>,
}

/// Everything the status file shows, owned by the status writer task.
/// Connections and devices keep config order.
#[derive(Debug, Clone)]
pub struct StatusState {
    gateway_id: String,
    broker: String,
    mqtt_connected: bool,
    connections: Vec<ConnectionStatus>,
}

impl StatusState {
    /// `connections` is `(connection id, device ids)`; everything starts
    /// down/unknown until the first events arrive.
    pub fn new(
        gateway_id: impl Into<String>,
        broker: impl Into<String>,
        connections: impl IntoIterator<Item = (String, Vec<String>)>,
    ) -> Self {
        StatusState {
            gateway_id: gateway_id.into(),
            broker: broker.into(),
            mqtt_connected: false,
            connections: connections
                .into_iter()
                .map(|(id, devices)| ConnectionStatus {
                    id,
                    up: false,
                    devices: devices
                        .into_iter()
                        .map(|id| DeviceStatus {
                            id,
                            health: Health::Unknown,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    pub fn from_config(config: &Config) -> Self {
        StatusState::new(
            &config.gateway.id,
            format!("{}:{}", config.mqtt.broker_host, config.mqtt.broker_port),
            config.connections.iter().map(|c| {
                (
                    c.id.clone(),
                    c.devices.iter().map(|d| d.id.clone()).collect(),
                )
            }),
        )
    }

    /// Applies one event; returns whether anything visible changed, i.e.
    /// whether a write is due. Events for unknown ids are ignored.
    pub fn apply(&mut self, event: StatusEvent, now: OffsetDateTime) -> bool {
        match event {
            StatusEvent::MqttConnected => replace(&mut self.mqtt_connected, true),
            StatusEvent::MqttDisconnected => replace(&mut self.mqtt_connected, false),
            StatusEvent::ConnectionUp { connection_id } => {
                let Some(conn) = self.connection(&connection_id) else {
                    return false;
                };
                if conn.up {
                    return false;
                }
                conn.up = true;
                // Earlier device states describe the previous connection.
                for device in &mut conn.devices {
                    device.health = Health::Unknown;
                }
                true
            }
            StatusEvent::ConnectionDown { connection_id } => self
                .connection(&connection_id)
                .is_some_and(|conn| replace(&mut conn.up, false)),
            StatusEvent::DeviceOk {
                connection_id,
                device_id,
            } => self.set_health(&connection_id, &device_id, |_| Health::Ok),
            StatusEvent::DeviceProblem {
                connection_id,
                device_id,
                reason,
            } => self.set_health(&connection_id, &device_id, |old| {
                // `since` marks when the device first went bad, not the
                // latest failure.
                let since = match old {
                    Health::Problem { since, .. } => *since,
                    _ => now,
                };
                Health::Problem { reason, since }
            }),
        }
    }

    fn connection(&mut self, id: &str) -> Option<&mut ConnectionStatus> {
        self.connections.iter_mut().find(|c| c.id == id)
    }

    fn set_health(
        &mut self,
        connection_id: &str,
        device_id: &str,
        health: impl FnOnce(&Health) -> Health,
    ) -> bool {
        let Some(conn) = self.connection(connection_id) else {
            return false;
        };
        let Some(device) = conn.devices.iter_mut().find(|d| d.id == device_id) else {
            return false;
        };
        let new = health(&device.health);
        // A device result means the connection is up, even if the
        // ConnectionUp event itself was dropped on a full channel.
        let conn_changed = replace(&mut conn.up, true);
        let changed = device.health != new;
        device.health = new;
        conn_changed || changed
    }
}

/// Sets `*slot` to `value`, returning whether it changed.
fn replace(slot: &mut bool, value: bool) -> bool {
    std::mem::replace(slot, value) != value
}

/// Renders the §11.2 status text, capped at `MAX_STATUS_BYTES`.
pub fn render_status(state: &StatusState, now: OffsetDateTime) -> String {
    let mut out = format!(
        "gateway: {}\nupdated: {}\nmqtt: {} ({})\n",
        state.gateway_id,
        format_utc(now, TIMESTAMP),
        if state.mqtt_connected {
            "connected"
        } else {
            "disconnected"
        },
        state.broker,
    );

    for conn in &state.connections {
        if !conn.up {
            out.push_str(&format!("connection {}: down\n", conn.id));
            continue;
        }
        let ok = conn
            .devices
            .iter()
            .filter(|d| d.health == Health::Ok)
            .count();
        let problems: Vec<String> = conn
            .devices
            .iter()
            .filter_map(|d| match &d.health {
                Health::Problem { reason, since } => Some(format!(
                    "{}: {} since {}",
                    d.id,
                    reason,
                    format_utc(*since, TIME_OF_DAY)
                )),
                _ => None,
            })
            .collect();
        out.push_str(&format!(
            "connection {}: up, {}/{} devices ok",
            conn.id,
            ok,
            conn.devices.len()
        ));
        if !problems.is_empty() {
            out.push_str(&format!(" ({})", problems.join(", ")));
        }
        out.push('\n');
    }

    truncate(out)
}

fn format_utc(t: OffsetDateTime, format: &[FormatItem<'_>]) -> String {
    t.to_offset(time::UtcOffset::UTC)
        .format(format)
        .unwrap_or_else(|_| "?".to_string())
}

/// Cuts to the last complete line that fits, so a reader never sees half a
/// line; falls back to a char boundary if even the first line is too long.
fn truncate(mut s: String) -> String {
    if s.len() <= MAX_STATUS_BYTES {
        return s;
    }
    let mut end = MAX_STATUS_BYTES;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    if let Some(newline) = s[..end].rfind('\n') {
        end = newline + 1;
    }
    s.truncate(end);
    s
}

/// Writes `<path>.tmp` in the same directory, then renames it over `path`,
/// so readers only ever see a complete old or new file.
pub fn write_atomic(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);
    let mut file = File::create(&tmp)?;
    file.write_all(contents)?;
    // The target may be flash: make the data durable before the rename
    // publishes it, so a power cut can't leave an empty file behind.
    file.sync_all()?;
    fs::rename(&tmp, path)
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = OsString::from(path.as_os_str());
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Status reporter task (§11.2). Writes once at startup, then on state
/// changes (debounced) and on the heartbeat. Returns once every sender is
/// gone, after writing out any still-pending change.
pub async fn run_status_writer(
    rx: mpsc::Receiver<StatusEvent>,
    state: StatusState,
    path: PathBuf,
    debounce: Duration,
    heartbeat: Duration,
) {
    run_loop(rx, state, debounce, heartbeat, |contents| {
        let path = path.clone();
        async move {
            let target = path.clone();
            let result =
                tokio::task::spawn_blocking(move || write_atomic(&target, contents.as_bytes()))
                    .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(error = %e, path = %path.display(), "failed to write status file")
                }
                Err(e) => warn!(error = %e, "status file write task failed"),
            }
        }
    })
    .await
}

/// The write policy, separated from the filesystem so tests can observe
/// when writes happen.
async fn run_loop<F, Fut>(
    mut rx: mpsc::Receiver<StatusEvent>,
    mut state: StatusState,
    debounce: Duration,
    heartbeat: Duration,
    mut write: F,
) where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = ()>,
{
    write(render_status(&state, OffsetDateTime::now_utc())).await;
    let mut next_heartbeat = Instant::now() + heartbeat;
    // Due time of the write for the oldest unwritten change, if any.
    let mut pending: Option<Instant> = None;

    loop {
        let deadline = pending.map_or(next_heartbeat, |p| p.min(next_heartbeat));
        tokio::select! {
            event = rx.recv() => match event {
                Some(event) => {
                    if state.apply(event, OffsetDateTime::now_utc()) && pending.is_none() {
                        pending = Some(Instant::now() + debounce);
                    }
                }
                None => {
                    if pending.is_some() {
                        write(render_status(&state, OffsetDateTime::now_utc())).await;
                    }
                    return;
                }
            },
            _ = ttime::sleep_until(deadline) => {
                write(render_status(&state, OffsetDateTime::now_utc())).await;
                pending = None;
                next_heartbeat = Instant::now() + heartbeat;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use time::macros::datetime;

    use super::*;

    const NOW: OffsetDateTime = datetime!(2026-09-23 14:32:10 UTC);

    fn example_state() -> StatusState {
        StatusState::new(
            "gw-router1",
            "10.0.0.5:8883",
            [
                ("meter-tcp1".to_string(), vec!["meter1".to_string()]),
                (
                    "rs485-bus1".to_string(),
                    vec!["tempsensor1".to_string(), "tempsensor2".to_string()],
                ),
            ],
        )
    }

    fn up(conn: &str) -> StatusEvent {
        StatusEvent::ConnectionUp {
            connection_id: conn.to_string(),
        }
    }

    fn down(conn: &str) -> StatusEvent {
        StatusEvent::ConnectionDown {
            connection_id: conn.to_string(),
        }
    }

    fn ok(conn: &str, device: &str) -> StatusEvent {
        StatusEvent::DeviceOk {
            connection_id: conn.to_string(),
            device_id: device.to_string(),
        }
    }

    fn problem(conn: &str, device: &str, reason: &str) -> StatusEvent {
        StatusEvent::DeviceProblem {
            connection_id: conn.to_string(),
            device_id: device.to_string(),
            reason: reason.to_string(),
        }
    }

    fn all_ok(state: &mut StatusState) {
        state.apply(StatusEvent::MqttConnected, NOW);
        state.apply(up("meter-tcp1"), NOW);
        state.apply(ok("meter-tcp1", "meter1"), NOW);
        state.apply(up("rs485-bus1"), NOW);
        state.apply(ok("rs485-bus1", "tempsensor1"), NOW);
        state.apply(ok("rs485-bus1", "tempsensor2"), NOW);
    }

    #[test]
    fn renders_all_ok() {
        let mut state = example_state();
        all_ok(&mut state);
        assert_eq!(
            render_status(&state, NOW),
            "gateway: gw-router1\n\
             updated: 2026-09-23T14:32:10Z\n\
             mqtt: connected (10.0.0.5:8883)\n\
             connection meter-tcp1: up, 1/1 devices ok\n\
             connection rs485-bus1: up, 2/2 devices ok\n"
        );
    }

    #[test]
    fn renders_one_problem_device_as_in_design_example() {
        let mut state = example_state();
        all_ok(&mut state);
        let since = datetime!(2026-09-23 14:28:15 UTC);
        state.apply(problem("rs485-bus1", "tempsensor2", "timeout"), since);
        assert_eq!(
            render_status(&state, NOW),
            "gateway: gw-router1\n\
             updated: 2026-09-23T14:32:10Z\n\
             mqtt: connected (10.0.0.5:8883)\n\
             connection meter-tcp1: up, 1/1 devices ok\n\
             connection rs485-bus1: up, 1/2 devices ok (tempsensor2: timeout since 14:28:15Z)\n"
        );
    }

    #[test]
    fn renders_fully_down() {
        let mut state = example_state();
        all_ok(&mut state);
        state.apply(StatusEvent::MqttDisconnected, NOW);
        state.apply(down("meter-tcp1"), NOW);
        state.apply(down("rs485-bus1"), NOW);
        assert_eq!(
            render_status(&state, NOW),
            "gateway: gw-router1\n\
             updated: 2026-09-23T14:32:10Z\n\
             mqtt: disconnected (10.0.0.5:8883)\n\
             connection meter-tcp1: down\n\
             connection rs485-bus1: down\n"
        );
    }

    #[test]
    fn timestamps_render_in_utc() {
        let state = example_state();
        let local = datetime!(2026-09-23 16:32:10 +2);
        assert!(render_status(&state, local).contains("updated: 2026-09-23T14:32:10Z\n"));
    }

    #[test]
    fn unpolled_devices_are_not_counted_ok() {
        let mut state = example_state();
        state.apply(up("rs485-bus1"), NOW);
        state.apply(ok("rs485-bus1", "tempsensor1"), NOW);
        assert!(render_status(&state, NOW).contains("connection rs485-bus1: up, 1/2 devices ok\n"));
    }

    #[test]
    fn problem_keeps_first_since_and_clears_on_ok() {
        let mut state = example_state();
        all_ok(&mut state);
        let first = datetime!(2026-09-23 14:28:15 UTC);
        assert!(state.apply(problem("rs485-bus1", "tempsensor2", "timeout"), first));
        assert!(!state.apply(problem("rs485-bus1", "tempsensor2", "timeout"), NOW));
        assert!(state.apply(
            problem("rs485-bus1", "tempsensor2", "exception: Server device busy"),
            NOW
        ));
        assert!(render_status(&state, NOW)
            .contains("(tempsensor2: exception: Server device busy since 14:28:15Z)"));

        assert!(state.apply(ok("rs485-bus1", "tempsensor2"), NOW));
        assert!(!state.apply(ok("rs485-bus1", "tempsensor2"), NOW));
        assert!(render_status(&state, NOW).contains("rs485-bus1: up, 2/2 devices ok\n"));
    }

    #[test]
    fn reconnect_resets_device_states() {
        let mut state = example_state();
        all_ok(&mut state);
        state.apply(problem("rs485-bus1", "tempsensor2", "timeout"), NOW);
        state.apply(down("rs485-bus1"), NOW);
        assert!(state.apply(up("rs485-bus1"), NOW));
        assert!(render_status(&state, NOW).contains("rs485-bus1: up, 0/2 devices ok\n"));
    }

    #[test]
    fn device_event_implies_connection_up() {
        let mut state = example_state();
        assert!(state.apply(ok("meter-tcp1", "meter1"), NOW));
        assert!(render_status(&state, NOW).contains("meter-tcp1: up, 1/1 devices ok\n"));
    }

    #[test]
    fn unknown_ids_are_ignored() {
        let mut state = example_state();
        assert!(!state.apply(up("nope"), NOW));
        assert!(!state.apply(ok("meter-tcp1", "nope"), NOW));
        assert!(!state.apply(problem("nope", "meter1", "timeout"), NOW));
    }

    #[test]
    fn large_config_is_capped_at_complete_lines() {
        let connections = (0..100).map(|i| {
            (
                format!("connection-with-a-long-name-{i}"),
                (0..10).map(|d| format!("device-{i}-{d}")).collect(),
            )
        });
        let mut state = StatusState::new("gw", "broker:8883", connections);
        for i in 0..100 {
            let conn = format!("connection-with-a-long-name-{i}");
            for d in 0..10 {
                state.apply(problem(&conn, &format!("device-{i}-{d}"), "timeout"), NOW);
            }
        }

        let rendered = render_status(&state, NOW);
        assert!(rendered.len() <= MAX_STATUS_BYTES);
        assert!(rendered.starts_with("gateway: gw\n"));
        assert!(rendered.ends_with('\n'));
    }

    #[test]
    fn truncate_falls_back_to_char_boundary() {
        let long = "ä".repeat(MAX_STATUS_BYTES);
        let cut = truncate(long);
        assert!(cut.len() <= MAX_STATUS_BYTES);
        assert!(cut.len() > MAX_STATUS_BYTES - 2);
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("modbus-gw-status-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn atomic_write_never_exposes_partial_content() {
        let dir = temp_dir("race");
        let path = dir.join("status");
        let a = "a".repeat(MAX_STATUS_BYTES);
        let b = "b".repeat(MAX_STATUS_BYTES / 2);
        write_atomic(&path, a.as_bytes()).unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = {
            let (path, done, reads) = (path.clone(), done.clone(), reads.clone());
            let (a, b) = (a.clone(), b.clone());
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    let got = fs::read_to_string(&path).unwrap();
                    assert!(got == a || got == b, "partial read of {} bytes", got.len());
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        for i in 0..200 {
            let contents = if i % 2 == 0 { &b } else { &a };
            write_atomic(&path, contents.as_bytes()).unwrap();
        }
        done.store(true, Ordering::Relaxed);
        reader.join().unwrap();

        assert!(reads.load(Ordering::Relaxed) > 0);
        assert!(!tmp_path(&path).exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    type Writes = Arc<Mutex<Vec<(Duration, String)>>>;

    /// Runs the write loop against an in-memory sink, recording the time
    /// (since start) and content of every write.
    fn spawn_loop(
        debounce: Duration,
        heartbeat: Duration,
    ) -> (
        mpsc::Sender<StatusEvent>,
        Writes,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::channel(STATUS_CHANNEL_CAPACITY);
        let writes: Writes = Arc::default();
        let start = Instant::now();
        let sink = writes.clone();
        let task = tokio::spawn(run_loop(
            rx,
            example_state(),
            debounce,
            heartbeat,
            move |contents| {
                sink.lock()
                    .unwrap()
                    .push((Instant::now() - start, contents));
                async {}
            },
        ));
        (tx, writes, task)
    }

    fn times(writes: &Writes) -> Vec<u64> {
        writes
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _)| t.as_millis() as u64)
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn debounce_coalesces_a_burst_into_one_write() {
        let (tx, writes, task) = spawn_loop(Duration::from_secs(2), Duration::from_secs(30));

        // A flapping connection: 10 events 100ms apart, ending down.
        for i in 0..10 {
            ttime::sleep(Duration::from_millis(100)).await;
            let event = if i % 2 == 0 {
                up("rs485-bus1")
            } else {
                down("rs485-bus1")
            };
            tx.send(event).await.unwrap();
        }
        ttime::sleep(Duration::from_secs(5)).await;

        // Initial write, then one write 2s after the first change.
        assert_eq!(times(&writes), [0, 2100]);
        assert!(writes.lock().unwrap()[1]
            .1
            .contains("connection rs485-bus1: down\n"));

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_rewrites_without_changes() {
        let (tx, writes, task) = spawn_loop(Duration::from_secs(2), Duration::from_secs(30));
        ttime::sleep(Duration::from_secs(95)).await;
        assert_eq!(times(&writes), [0, 30_000, 60_000, 90_000]);

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_state_does_not_trigger_a_write() {
        let (tx, writes, task) = spawn_loop(Duration::from_secs(2), Duration::from_secs(30));
        // Unknown→down is no change: connections start down.
        tx.send(down("meter-tcp1")).await.unwrap();
        ttime::sleep(Duration::from_secs(10)).await;
        assert_eq!(times(&writes), [0]);

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn pending_change_is_written_on_close() {
        let (tx, writes, task) = spawn_loop(Duration::from_secs(2), Duration::from_secs(30));
        tx.send(StatusEvent::MqttConnected).await.unwrap();
        drop(tx);
        task.await.unwrap();

        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 2);
        assert!(writes[1].1.contains("mqtt: connected"));
    }

    #[tokio::test]
    async fn writer_creates_the_status_file() {
        let dir = temp_dir("writer");
        let path = dir.join("status");
        let (tx, rx) = mpsc::channel(STATUS_CHANNEL_CAPACITY);
        let writer = tokio::spawn(run_status_writer(
            rx,
            example_state(),
            path.clone(),
            Duration::from_millis(10),
            Duration::from_secs(30),
        ));
        tx.send(up("meter-tcp1")).await.unwrap();
        drop(tx);
        writer.await.unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        assert!(contents.starts_with("gateway: gw-router1\n"));
        assert!(contents.contains("connection meter-tcp1: up, 0/1 devices ok\n"));
        assert!(!tmp_path(&path).exists());
        fs::remove_dir_all(&dir).unwrap();
    }
}
