# Modbus → SenML/MQTT Gateway — Design

Implementation reference. Companion to
[modbus-senml-mqtt-gateway-architecture.md](modbus-senml-mqtt-gateway-architecture.md),
which has the rationale; this document is the spec to build against —
concrete types, config schema, and resolved decisions.

## 1. Scope

### Goals
- Poll measurements from Modbus devices over **Modbus/TCP** and
  **Modbus/RTU** (serial, including multidrop RS-485 buses).
- Average each point over a 1-minute wall-clock-aligned tumbling window.
- Encode as SenML, publish over MQTT (QoS 1).
- Run unattended, config-driven (no code changes to add/remove a device),
  on a resource-constrained router (Alpine LXC).
- Survive partial failure: one dead device or bus never stops telemetry
  from the rest.

### Non-goals (v1)
- No on-disk persistence across process restarts — buffering is MQTT's
  own bounded in-memory queue only (see [§7](#7-mqtt-publisher)); a
  restart during a broker outage loses whatever hadn't been sent.
- No hot config reload — a config change means a service restart.
- No Modbus writes — read-only gateway.
- No mTLS — server-authenticated TLS (CA cert) only, unless a future
  requirement needs client certs.
- No multi-broker fan-out — one MQTT connection.

## 2. System overview

```
Config (TOML)
     |
     v
Poller task (per connection) ---\
Poller task (per connection) ----+--> bounded mpsc channel --> Aggregator
Poller task (per connection) ---/         (1-min tumbling window,
                                            wall-clock aligned)
                                                   |
                                                   v
                                           SenML encoder (pure fn)
                                                   |
                                                   v
                                       MQTT publisher task (QoS 1,
                                       persistent session, auto-reconnect)
                                                   |
                                                   v
                                             MQTT broker
```

One task per concern, connected by **bounded** channels (backpressure +
flat memory ceiling). A **connection** is a TCP socket or a serial port;
a **device** is one Modbus unit ID living on a connection. Modbus/TCP
normally has one device per connection; Modbus/RTU commonly has several,
multidropped on one RS-485 bus. One poller task per *connection* (not per
device) — required for RTU, since only one transaction may be in flight
on a shared serial line at a time; harmless for TCP.

Task inventory at steady state: `N` poller tasks (one per `[[connection]]`
entry), 1 aggregator task, 1 MQTT publisher task, 1 MQTT eventloop task,
1 status reporter task ([§11.2](#112-status-file)), 1 supervisor.

## 3. Core data types

```rust
#[derive(Clone, Eq, PartialEq, Hash)]
struct PointId {
    device: String,
    point: String,
}

struct Reading {
    point_id: PointId,
    value: f64,       // already scaled + absolute-adjusted by decode()
}

struct PointAgg {
    sum: f64,
    count: u32,
    min: f64,
    max: f64,
    unit: String,     // static, from config — not carried per-Reading
}

struct AggregatedPoint {
    name: String,
    unit: String,
    mean: f64,
}

struct AggregatedBatch {
    device_id: String,
    window_start: u64,   // unix seconds
    window_end: u64,
    points: Vec<AggregatedPoint>,
}
```

`Reading` carries only `point_id` + `value`. Unit is static per point and
already known from config, so it's attached at aggregator init, not
threaded through every reading.

## 4. Config schema

### 4.1 Top level

```toml
[gateway]
id = "gw-router1"                       # used in LWT topic and defaults for base_name_prefix
aggregation_window_secs = 60
base_name_prefix = "urn:dev:gw-router1:"
log_level = "info"
status_file_path = "/dev/container_config/status"  # host-visible status; UTF-8, hard-capped at 2048 bytes, see §11.2
status_debounce_secs = 2                # min gap between writes triggered by a state change, see §11.2
status_heartbeat_secs = 30              # rewrite at least this often even with no change, see §11.2

[mqtt]
broker_host = "10.0.0.5"
broker_port = 8883
client_id = "modbus-gateway-router1"
tls = true
ca_cert_path = "/etc/modbus-gateway/ca.pem" # private CA root; the only CA the gateway trusts for the broker (not the system store)
username = "gateway"
password_env = "MQTT_PASSWORD"           # read from env var, never plaintext in file
qos = 1
keep_alive_secs = 30
topic_template = "telemetry/{device}"    # one pack per device (see §6) — no {point} placeholder needed

[[connection]]
# ... see §4.2
```

### 4.2 Connection (`[[connection]]`)

| Field | Type | Applies to | Default | Notes |
|---|---|---|---|---|
| `id` | string | all | required | used in logs |
| `transport` | `"tcp"` \| `"rtu"` | all | required | |
| `host` | string | tcp | required (tcp) | |
| `port` | u16 | tcp | required (tcp) | |
| `serial_port` | string | rtu | required (rtu) | prefer a udev by-id path, see [§10](#10-deployment) |
| `baud_rate` | u32 | rtu | required (rtu) | |
| `data_bits` | u8 | rtu | 8 | |
| `parity` | `"none"` \| `"even"` \| `"odd"` | rtu | `"none"` | |
| `stop_bits` | u8 | rtu | 1 | |
| `inter_frame_delay_ms` | u64 | rtu | 0 | extra gap between requests; raise if an adapter mis-frames under load |
| `poll_interval_secs` | u64 | all | required | shared by every device on this connection |
| `io_timeout_ms` | u64 | all | required | per Modbus request |
| `reconnect_backoff_min_secs` | u64 | all | 1 | |
| `reconnect_backoff_max_secs` | u64 | all | 30 | |
| `device` | array | all | required, ≥1 | see §4.3 |

**Validation is structural, not a manual checklist**: model `transport` as
a serde internally-tagged enum —

```rust
#[derive(Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase")]
enum Transport {
    Tcp { host: String, port: u16 },
    Rtu {
        serial_port: String,
        baud_rate: u32,
        #[serde(default = "default_data_bits")] data_bits: u8,
        #[serde(default)] parity: Parity,
        #[serde(default = "default_stop_bits")] stop_bits: u8,
        #[serde(default)] inter_frame_delay_ms: u64,
    },
}
```

— so a `tcp` connection with `serial_port` set, or an `rtu` connection
missing `baud_rate`, fails to deserialize at load time for free. No
hand-rolled "wrong field set for transport" check needed.

Additional validation after parsing (not expressible via serde alone):
- `unit_id` unique within a connection.
- `serial_port` path not reused across two connections (can't open the
  same device node twice).
- Per block: offsets fit within `count`, no overlapping points,
  `start + count` ≤ 125 (Modbus register read limit per request).

### 4.3 Device (`[[connection.device]]`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `id` | string | required | feeds SenML `bn` and the MQTT topic |
| `unit_id` | u8 | required | Modbus slave/unit address |
| `block` | array | required, ≥1 | see §4.4 |

### 4.4 Block (`[[connection.device.block]]`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `function` | `"holding"` \| `"input"` | required | |
| `start` | u16 | required | register address |
| `count` | u16 | required | words read in one request |
| `word_order` | `"big_endian"` \| `"little_endian"` | `"big_endian"` | per-block, a wiring convention |
| `point` | array | required, ≥1 | see §4.5 |

### 4.5 Point (`[[connection.device.block.point]]`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | string | required | |
| `offset` | u16 | required | word offset within the block |
| `data_type` | `"u16"` \| `"i16"` \| `"u32"` \| `"i32"` \| `"f32"` | required | |
| `scale` | f64 | `1.0` | `raw * scale` |
| `absolute` | bool | `false` | `.abs()` applied *after* scaling — for points where only magnitude is meaningful (e.g. a CT clamp that can be wired backwards) |
| `unit` | string | required | UCUM-style string (`Cel`, `%RH`, `W`, `kWh`) → SenML `u` |

### 4.6 Full example

```toml
[gateway]
id = "gw-router1"
aggregation_window_secs = 60
base_name_prefix = "urn:dev:gw-router1:"
log_level = "info"

[mqtt]
broker_host = "10.0.0.5"
broker_port = 8883
client_id = "modbus-gateway-router1"
tls = true
username = "gateway"
password_env = "MQTT_PASSWORD"
qos = 1
keep_alive_secs = 30
topic_template = "telemetry/{device}"

# ---- Connection 1: Modbus/TCP, one device ----
[[connection]]
id = "meter-tcp1"
transport = "tcp"
host = "192.168.1.50"
port = 502
poll_interval_secs = 10
io_timeout_ms = 1000

  [[connection.device]]
  id = "meter1"
  unit_id = 1

    [[connection.device.block]]
    function = "holding"
    start = 3000
    count = 8
    word_order = "big_endian"

      [[connection.device.block.point]]
      name = "voltage_l1"
      offset = 0
      data_type = "f32"
      unit = "V"

      [[connection.device.block.point]]
      name = "current_l1"
      offset = 2
      data_type = "f32"
      unit = "A"

      [[connection.device.block.point]]
      name = "active_power"
      offset = 4
      data_type = "i32"
      scale = 0.1
      absolute = true       # CT may be wired reversed; only magnitude matters here
      unit = "W"

    [[connection.device.block]]
    function = "holding"
    start = 3100
    count = 2

      [[connection.device.block.point]]
      name = "energy_total"
      offset = 0
      data_type = "u32"
      scale = 0.001
      unit = "kWh"

# ---- Connection 2: Modbus/RTU, RS-485 bus with two multidropped devices ----
[[connection]]
id = "rs485-bus1"
transport = "rtu"
serial_port = "/dev/serial/by-id/usb-FTDI_USB-RS485-if00-port0"
baud_rate = 9600
parity = "none"
poll_interval_secs = 15
io_timeout_ms = 500
inter_frame_delay_ms = 10

  [[connection.device]]
  id = "tempsensor1"
  unit_id = 5

    [[connection.device.block]]
    function = "input"
    start = 100
    count = 2

      [[connection.device.block.point]]
      name = "temperature"
      offset = 0
      data_type = "i16"
      scale = 0.1
      unit = "Cel"

      [[connection.device.block.point]]
      name = "humidity"
      offset = 1
      data_type = "u16"
      scale = 0.1
      unit = "%RH"

  [[connection.device]]
  id = "tempsensor2"
  unit_id = 6

    [[connection.device.block]]
    function = "input"
    start = 100
    count = 2

      [[connection.device.block.point]]
      name = "temperature"
      offset = 0
      data_type = "i16"
      scale = 0.1
      unit = "Cel"

      [[connection.device.block.point]]
      name = "humidity"
      offset = 1
      data_type = "u16"
      scale = 0.1
      unit = "%RH"
```

## 5. Config loader

Parsed once at startup with `serde` + `toml`. On any validation failure
(§4.2), print a clear error naming the offending connection/device/block
and exit non-zero — never start with a partially-valid config.

Aggregator initial state (§6) is built directly from the parsed config —
every `(device.id, point.name)` pair gets a zeroed `PointAgg` before any
poller starts.

## 6. Modbus poller (per connection)

One task per `[[connection]]`. Responsibilities: connect (transport-
specific), tick on `poll_interval_secs`, iterate its devices' blocks in
order, decode, push `Reading`s onto the aggregator channel.

```rust
async fn connect(transport: &Transport) -> Result<client::Context, io::Error> {
    match transport {
        Transport::Tcp { host, port } => {
            tcp::connect(format!("{host}:{port}").parse().unwrap()).await
        }
        Transport::Rtu { serial_port, baud_rate, data_bits, parity, stop_bits, .. } => {
            let builder = tokio_serial::new(serial_port, *baud_rate)
                .data_bits(*data_bits).parity(*parity).stop_bits(*stop_bits);
            Ok(rtu::attach(tokio_serial::SerialStream::open(&builder)?))
        }
    }
}

fn decode(block: &BlockConfig, words: &[u16]) -> Vec<(String, f64)> {
    block.point.iter().map(|p| {
        let raw = extract_raw(words, p.offset, p.data_type, block.word_order);
        let mut value = raw * p.scale;
        if p.absolute {
            value = value.abs();
        }
        (p.name.clone(), value)
    }).collect()
}
```

### 6.1 Error handling — per-device vs per-connection

This is the one place the design departs from a naive port of the old
"one device per connection" model. With several devices sharing a
connection, **not every failure should tear down the whole connection**:

| Failure | Classification | Action |
|---|---|---|
| `connect()` fails (socket refused, serial port missing) | transport | reconnect whole connection, backoff |
| Request times out (`io_timeout_ms` elapses) | ambiguous — see below | **skip this device for this tick**, keep polling the rest, log warn |
| Modbus exception response (illegal address/function, slave busy) | device-specific | **skip this device for this tick**, keep polling the rest, log warn |
| Transport I/O error mid-read (socket reset, serial device unplugged — e.g. `ENXIO`/`ENODEV`, broken pipe) | transport | reconnect whole connection, backoff |

The reasoning: a Modbus exception or timeout from *one* unit ID doesn't
mean the bus or socket is broken — it usually means that one device is
off, mid-boot, or rejected a specific request. On a shared RTU bus,
tearing down and reopening the serial port over one flaky sensor would
needlessly interrupt every other device on that bus. Only errors that
indicate the transport itself is gone should trigger a reconnect.

```rust
async fn run_connection(cfg: ConnectionConfig, tx: mpsc::Sender<Reading>) {
    let mut backoff = Backoff::new(cfg.reconnect_backoff_min, cfg.reconnect_backoff_max);
    loop {
        let mut ctx = match connect(&cfg.transport).await {
            Ok(c) => { backoff.reset(); c }
            Err(e) => { warn!(?e, connection = %cfg.id, "connect failed"); backoff.wait().await; continue; }
        };

        let mut ticker = tokio::time::interval(cfg.poll_interval);
        let transport_ok = 'poll: loop {
            ticker.tick().await;
            for device in &cfg.devices {
                ctx.set_slave(Slave(device.unit_id));
                for block in &device.blocks {
                    match timeout(cfg.io_timeout, ctx.read_holding_registers(block.start, block.count)).await {
                        Ok(Ok(words)) => {
                            for (point_name, value) in decode(block, &words) {
                                let _ = tx.try_send(Reading { point_id: PointId { device: device.id.clone(), point: point_name }, value });
                            }
                        }
                        Ok(Err(e)) if is_transport_fatal(&e) => {
                            warn!(?e, connection = %cfg.id, "transport error, reconnecting");
                            break 'poll false;
                        }
                        Ok(Err(e)) => {
                            warn!(?e, connection = %cfg.id, device = %device.id, "modbus exception, skipping device this tick");
                            break; // next device
                        }
                        Err(_) => {
                            warn!(connection = %cfg.id, device = %device.id, "io timeout, skipping device this tick");
                            break; // next device
                        }
                    }
                    if let Some(delay) = cfg.inter_frame_delay {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
            continue 'poll; // placeholder: loop never exits except via the `false` break above
        };
        let _ = transport_ok;
    }
}
```

`is_transport_fatal` inspects the concrete error tokio-modbus surfaces
for a dead socket/serial port vs. a protocol-level exception response —
pin this down against the actual `tokio-modbus` version's error type
during implementation (the table above is the contract; the exact match
arms are a detail).

A dead/offline device just shows up as `count == 0` for its points at the
next aggregator flush (§7) — a real, visible gap, not synthesized data.

### 6.2 Supervisor

Each connection task's `JoinHandle` is held by a supervisor loop that
respawns it on panic (with the same backoff as a failed connect), so one
connection's decoding bug can't kill polling for the rest.

## 7. Aggregator

Single task, tumbling window aligned to wall-clock `:00`. State:
`HashMap<PointId, PointAgg>`, pre-populated from config at startup (not
built lazily) so a point with zero samples in a window is a detectable,
loggable gap rather than a silent omission.

```rust
async fn run_aggregator(mut rx: mpsc::Receiver<Reading>, publish_tx: mpsc::Sender<AggregatedBatch>) {
    let mut agg: HashMap<PointId, PointAgg> = init_from_config();
    let mut window = next_minute_boundary();
    let sleep = tokio::time::sleep_until(window);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            maybe_reading = rx.recv() => {
                match maybe_reading {
                    Some(r) => {
                        if let Some(p) = agg.get_mut(&r.point_id) {
                            p.sum += r.value; p.count += 1;
                            p.min = p.min.min(r.value); p.max = p.max.max(r.value);
                        }
                    }
                    None => break,
                }
            }
            _ = &mut sleep => {
                let window_start = window - Duration::from_secs(60);
                let batch = flush(&mut agg, window_start, window); // per-device AggregatedBatch(es)
                for b in batch { let _ = publish_tx.send(b).await; }
                window += Duration::from_secs(60);
                sleep.as_mut().reset(window);
            }
        }
    }
}
```

`window += 60s` (not "now + 60s" recomputed) keeps it drift-free even if
a tick fires slightly late. Points with `count == 0` at flush are dropped
from the batch (logged as warning), never fabricated as zero/repeated.

## 8. SenML encoder

Pure function. **v1 decision: one pack per device**, matching one
MQTT publish per device per window — this is why `topic_template` in
§4.1 has no `{point}` placeholder.

- `bn` = `base_name_prefix` + `device.id` + `":"`.
- `bt` = `window_start` (unix seconds), set once on the first record.
- `bu`, optional, only when most points in the pack share a unit.
- `n` / `u` / `v` per point.
- **v1 decision: no `_min`/`_max`/`_sample_count` extension fields** —
  spec-pure, smaller payload. Revisit if a consumer needs them.

```json
[
  { "bn": "urn:dev:gw-router1:meter1:", "bt": 1758447120, "bu": "V",
    "n": "voltage_l1", "u": "V", "v": 231.4 },
  { "n": "current_l1", "u": "A", "v": 4.82 },
  { "n": "active_power", "u": "W", "v": 1112.6 },
  { "n": "energy_total", "u": "kWh", "v": 18452.311 }
]
```

## 9. MQTT publisher

- QoS from config (`mqtt.qos`, default 1 / at-least-once) — QoS 0 risks
  silent loss around a reconnect; QoS 2's handshake buys exactly-once
  that timestamped time-series data doesn't need.
- Persistent session (`clean_session = false`, stable `client_id`).
- `eventloop.poll()` runs continuously in its own task — nothing else
  drives reconnect/backoff/ack-tracking.
- Bounded internal queue (e.g. 100) — well over an hour of buffering per
  device at one publish/minute, without unbounded memory growth.
- `client.publish()` returning `Err` means the queue is full (real
  backpressure) — log distinctly from ordinary transient disconnects.
- LWT: retained `telemetry/{gateway.id}/status` = `"offline"` (QoS 1) as
  the will; publish `"online"` (retained) right after connecting.
- Never called directly by the aggregator — connected by a channel, so a
  slow/reconnecting MQTT link never delays polling or aggregation.
- TLS trusts only the CA at `mqtt.ca_cert_path` — deployment is a private
  network against an internal CA, not the public web, so the system trust
  store is irrelevant and isn't consulted. No client cert (mTLS is a
  non-goal, §1). The cert is loaded and parsed once during config load
  (§5); a missing or unparseable file is a startup validation failure,
  same as any other bad config value — not something discovered on first
  connect attempt.

```rust
async fn run_publisher(mut rx: mpsc::Receiver<AggregatedBatch>, cfg: MqttConfig) {
    let mqtt_opts = build_mqtt_options(&cfg); // client_id, clean_session=false, keep_alive, TLS, will
    let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 100);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_event) => {}
                Err(e) => {
                    warn!(?e, "mqtt eventloop error, auto-reconnecting");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    });

    while let Some(batch) = rx.recv().await {
        let payload = encode_senml(&batch);
        let topic = cfg.topic_template.replace("{device}", &batch.device_id);
        if let Err(e) = client.publish(topic, cfg.qos.into(), false, payload).await {
            error!(?e, device = %batch.device_id, "failed to queue publish, dropping batch");
        }
    }
}
```

## 10. Deployment

- Build: `aarch64-unknown-linux-musl` or `x86_64-unknown-linux-musl`,
  `opt-level = "z"`, `lto = true`, `codegen-units = 1`, `panic = "abort"`,
  stripped. Single static binary + OpenRC service script.
- **Serial port passthrough (RTU only)**: the LXC container needs the
  serial device node passed through (bind-mount or an `lxc.mount.entry`
  / cgroup device rule for `/dev/ttyUSB0` etc.). USB adapters can
  renumber across reboots — use a udev by-id path
  (`/dev/serial/by-id/...`) in config, not a raw `/dev/ttyUSBn`, as shown
  in the §4.6 example.
- Graceful shutdown on SIGTERM: stop poller tasks, do **not** force-flush
  a partial aggregation window (it's simply lost, which is fine — the
  next window after restart starts clean), let the publisher drain its
  already-queued sends for a short grace period (~2s), then exit.

## 11. Observability

- `tracing`, with a custom `Layer` writing directly to the Alpine
  container's syslog daemon ([§11.1](#111-syslog)) — not stdout capture,
  since that depends on OpenRC's redirection being wired to syslogd, and
  the router's virtualization docs require log output land in syslog
  directly.
- Span per connection task, named by `connection.id`; fields on
  warnings/errors: `connection`, `device`, error detail.
- A metrics endpoint is explicitly out of scope for v1 (see architecture
  notes' open items) — logs (§11.1) and the status file ([§11.2](#112-status-file))
  are the only observability surfaces until a concrete need for metrics
  shows up.

### 11.1 Syslog

- Transport: `UnixDatagram` to `/dev/log`, RFC 3164 framing. The app owns
  the connection directly rather than depending on OpenRC's stdout→syslog
  redirection actually being configured.
- Facility: `daemon`. Tag: `gateway.id` (from config) — no router-mandated
  convention for either, so picked for clarity in a shared log stream.
- Severity mapping: `tracing` `ERROR`→`err`, `WARN`→`warning`,
  `INFO`→`info`, `DEBUG`/`TRACE`→`debug`.
- Startup race: if `/dev/log` isn't present yet (syslogd not up before
  this service in boot order), retry opening the socket with backoff;
  whatever's logged in the gap falls back to stderr, nothing is buffered.
  Once connected, the datagram socket stays open for the process
  lifetime — no per-message reconnect logic needed.

### 11.2 Status file

Host-visible connectivity summary, independent of the syslog stream —
written to `gateway.status_file_path` (default
`/dev/container_config/status`), plain UTF-8 text, hard-capped at 2048
bytes (truncated defensively if ever exceeded, though the format below
stays well under that for realistic device counts).

Whether that path is tmpfs or persistent flash isn't guaranteed by the
router docs, so treat it as the latter: writes are debounced, atomic, and
infrequent rather than one per poll tick.

**Data flow**: poller tasks and the MQTT publisher emit `StatusEvent`s on
a bounded channel to a new status-reporter task, alongside their existing
`Reading` / `AggregatedBatch` traffic — same one-task-owns-the-state
pattern as the aggregator (§7).

```rust
enum StatusEvent {
    ConnectionUp { connection_id: String },
    ConnectionDown { connection_id: String },
    DeviceOk { connection_id: String, device_id: String },
    DeviceProblem { connection_id: String, device_id: String, reason: String },
    MqttConnected,
    MqttDisconnected,
}
```

**Write policy**:
- Coalesce bursts: on any event, schedule a write no sooner than
  `status_debounce_secs` (default 2s) after the first unwritten change —
  avoids a write per event while a connection is flapping.
- Heartbeat: rewrite at least every `status_heartbeat_secs` (default 30s)
  even with no state change, so `updated:` can't go stale — the host can
  treat an old timestamp as "app hung" without a separate liveness check.
- Atomic: write to `<status_file_path>.tmp` in the same directory, then
  `rename()` over the target, so a reader never observes a partial write.

**Format**: plain text (not JSON — this is read directly, not parsed by
a host UI), one rollup line per *connection* rather than per device, so
size stays bounded by connection count instead of device count:

```
gateway: gw-router1
updated: 2026-09-23T14:32:10Z
mqtt: connected (10.0.0.5:8883)
connection meter-tcp1: up, 1/1 devices ok
connection rs485-bus1: up, 1/2 devices ok (tempsensor2: timeout since 14:28:15Z)
```

A connection with every device ok omits the parenthetical; a down
connection omits the device rollup entirely (`connection rs485-bus1: down`).

## 12. Testing strategy

- **Unit — `decode()`**: each `data_type`, both `word_order`s, `scale`,
  and `absolute` (including a negative scaled value flipping to
  positive, and `absolute = false` leaving a negative value untouched).
- **Unit — aggregator**: window boundary alignment/drift-freeness, a
  point with `count == 0` dropped (not zeroed) at flush, concurrent
  readings across multiple devices don't cross-contaminate `PointId`s.
- **Unit — config validation**: overlapping points, `start + count > 125`,
  duplicate `unit_id` on one connection, duplicate `serial_port` across
  connections, `tcp`/`rtu` field-set mismatches (covered for free by the
  tagged-enum deserialize failing, per §4.2 — assert it does).
- **Integration — TCP**: a mock Modbus/TCP server (`tokio-modbus` has
  server support) exercising connect/timeout/reconnect and the
  per-device-vs-per-connection error split from §6.1.
- **Integration — RTU**: a virtual serial pair (e.g. `socat
  PTY,link=/tmp/ttyA PTY,link=/tmp/ttyB`) with a small mock RTU slave on
  one end, two unit IDs multiplexed, to verify bus serialization and that
  one unit's timeout doesn't affect the other.
- **Integration — MQTT**: an embedded/test broker to verify LWT, QoS 1
  redelivery across a forced disconnect, and topic/payload shape.
- **Unit — status file writer**: debounce coalesces rapid flapping into
  one write, heartbeat fires on schedule with no state change, the
  `.tmp`+rename leaves no partial file visible mid-write, output stays
  ≤2048 bytes and the per-connection rollup format renders correctly for
  a mix of ok/problem devices.
- **Unit — syslog layer**: RFC 3164 framing and severity mapping for each
  `tracing` level, retry-with-backoff when `/dev/log` is initially absent.

## 13. Open items (carried from architecture notes)

- ~~Pin down `is_transport_fatal` (§6.1) against the concrete error type
  `tokio-modbus` returns for the chosen version.~~ Decided (tokio-modbus
  0.17): reads return `Result<Result<T, ExceptionCode>, Error>`. Exception
  responses are the inner `Err` (skip device); `Error::Protocol` (header /
  function-code mismatch, e.g. a late reply after a timeout) also skips the
  device; only `Error::Transport(io::Error)` is fatal and reconnects.
- ~~Confirm default `inter_frame_delay_ms` is fine at 0 until a specific
  adapter proves otherwise.~~ Decided: keep 0 as default (§4.2); this is a
  field-tuning knob, not something to guess up front — raise it per-connection
  if a specific RS-485 adapter mis-frames under load.
- ~~TLS setup details for MQTT (cert pinning vs system CA store).~~
  Decided: private network with our own CA — `ca_cert_path` (§4.1) names
  the CA the gateway trusts for the broker; the system trust store is
  never consulted. No client certs (mTLS already a non-goal, §1).
  Validated at config-load time (§5, §9) like every other config field.
