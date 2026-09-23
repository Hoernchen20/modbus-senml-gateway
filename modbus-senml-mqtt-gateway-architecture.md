# Modbus → SenML/MQTT Gateway — Architecture Notes

A gateway daemon that polls measurements over Modbus/TCP and Modbus/RTU
(serial), averages them over a 1-minute tumbling window, encodes them as
SenML, and publishes over MQTT. Target deployment: Alpine LXC on a
resource-constrained router.

## Language choice: Rust (Go as fallback)

- **Rust**: no GC, near-zero idle overhead, static musl binaries that drop
  straight into Alpine with no dynamic-linking issues. Ecosystem covers
  everything needed: `tokio` (async runtime), `tokio-modbus` with the
  `rtu` + `tcp-client` features (Modbus/TCP and Modbus/RTU), `tokio-serial`
  (async serial port backing RTU), `rumqttc` (MQTT), `serde`/`serde_json`
  (config + SenML encoding).
- **Go**: same shape — single static binary, easy cross-compilation,
  `goburrow/modbus` (supports both TCP and RTU/serial clients) +
  `eclipse/paho.mqtt.golang`. Simpler to write, but the GC and goroutine
  runtime add modest overhead vs Rust's near-zero baseline.
- Recommendation: Rust, unless team familiarity strongly favors Go.

### Build/packaging for the constrained target

- Target: `aarch64-unknown-linux-musl` or `x86_64-unknown-linux-musl`
  (static binary, no glibc/musl mismatch, drops straight into Alpine).
- Release profile: `opt-level = "z"`, `lto = true`, `codegen-units = 1`,
  `panic = "abort"`, then `strip` — typically low single-digit MB binary.
- Runtime: single-threaded tokio (`#[tokio::main(flavor = "current_thread")]`)
  is sufficient for a handful of I/O-bound tasks — idle RSS typically a few MB.
- Ship as a static binary + small OpenRC service script. No runtime deps
  beyond maybe `ca-certificates` for MQTT/TLS. For Modbus/RTU, the LXC
  container also needs the serial device node passed through (see
  "Serial port access" under Open items).

## High-level pipeline

```
Config (TOML)
     |
     v
Modbus poller task (per connection) ---\
Modbus poller task (per connection) ----+--> bounded mpsc channel --> Aggregator
Modbus poller task (per connection) ---/         (1-minute tumbling window,
                                                   wall-clock aligned)
                                                      |
                                                      v
                                              SenML encoder (pure fn)
                                                      |
                                                      v
                                          MQTT publisher task (QoS 1,
                                          persistent session, rumqttc
                                          handles reconnect)
                                                      |
                                                      v
                                                MQTT broker
```

Design principle: one lightweight async task per concern, connected by
**bounded** channels. Bounded channels give natural backpressure and a hard
ceiling on memory if something downstream stalls.

A **connection** is a physical/logical channel — a TCP socket, or a serial
port — and one or more **devices** (each a distinct Modbus unit ID) live on
it. Modbus/TCP usually has one device per connection; Modbus/RTU commonly
doesn't — RS-485 is a multidrop bus, so several devices normally share one
serial port and, being a shared physical medium, can only have one
transaction in flight at a time. Polling is therefore one task **per
connection**, which visits each of its devices' blocks in sequence — this
naturally serializes bus access, which RTU requires and TCP is unaffected
by. Each connection reconnects independently — one dead connection (device
unreachable, serial adapter unplugged) never blocks other connections, the
aggregator, or the publisher. Within a shared RTU bus, an unresponsive
device still costs up to the full `io_timeout_ms` before the loop moves to
the next device on that bus — an unavoidable consequence of the bus being
physically single-transaction, not a software shortcut. Keep
`io_timeout_ms` tight on buses with several devices.

## Components

### 1. Config loader
TOML, parsed once at startup with `serde`. Describes MQTT settings, gateway
settings, and one `[[connection]]` block per TCP socket or serial port,
each holding one or more `[[connection.device]]` blocks (see format below).

### 2. Modbus poller tasks
- **One task per connection** (TCP socket or serial port), not per device
  and not per register. Modbus is effectively single-request-in-flight per
  connection anyway, and RTU's shared bus requires it: only one
  transaction may be outstanding on a serial line at a time, so a single
  task visiting each of the connection's devices in turn is what keeps the
  bus well-behaved, not just an optimization.
- **Transport is a config-time choice per connection**: `transport = "tcp"`
  (host/port) or `transport = "rtu"` (serial_port/baud_rate/data_bits/
  parity/stop_bits). `connect()` branches on it once; everything downstream
  (batching, decode, timeout, backoff) is transport-agnostic.
- **Batch reads**: group registers into contiguous blocks at config time;
  one `read_holding_registers(start, count)` call per block, not one call
  per register. Cuts round trips and load on constrained field devices —
  doubly important on RTU where every extra request costs a full serial
  turnaround.
- **Poll interval decoupled from the 1-minute average** — poll every
  5–15s so several samples get averaged per window, not one poll = one avg.
  Poll interval lives on the connection (not per device): all devices on a
  bus are scanned together each tick, matching how a shared bus is
  physically scanned in one sweep.
- `timeout()` around every I/O call — neither transport has a built-in
  liveness guarantee; without an explicit timeout a half-open TCP socket or
  a serial adapter that stops responding can hang the loop forever.
- `try_send` (not `send`) into the channel — pollers must never block on
  a slow aggregator; drop-and-count rather than stall polling.
- Exponential backoff (capped) on reconnect, reset on success. For RTU this
  means closing and reopening the serial port; for TCP, the socket.
- An optional `inter_frame_delay_ms` (RTU only, default 0) adds a fixed
  gap between requests — the Modbus RTU spec relies on a ≥3.5-character
  silent interval to frame messages, and some cheap USB–RS-485 adapters
  need extra margin beyond what strict timing would suggest.
- Decoding is a pure function, separate from I/O:
  `fn decode(block: &RegisterBlock, words: &[u16]) -> Vec<(String, f64)>` —
  handles data type (u16/i16/u32/i32/f32), word order (big/little-endian
  swap — a common cross-vendor inconsistency), scale, and finally
  `absolute` (`.abs()` the scaled value, for points where only magnitude
  is meaningful — e.g. a CT clamp that can be wired backwards).
- Supervisor wraps each connection task's `JoinHandle` and respawns it on
  panic, so one connection's decoding bug can't kill polling for the rest.

```rust
enum Transport {
    Tcp { host: String, port: u16 },
    Rtu { serial_port: String, baud_rate: u32, data_bits: DataBits, parity: Parity, stop_bits: StopBits },
}

async fn connect(transport: &Transport) -> Result<client::Context> {
    match transport {
        Transport::Tcp { host, port } => {
            tcp::connect(format!("{host}:{port}").parse()?).await
        }
        Transport::Rtu { serial_port, baud_rate, data_bits, parity, stop_bits } => {
            let builder = tokio_serial::new(serial_port, *baud_rate)
                .data_bits(*data_bits).parity(*parity).stop_bits(*stop_bits);
            rtu::attach(tokio_serial::SerialStream::open(&builder)?)
        }
    }
}

async fn run_connection(cfg: ConnectionConfig, tx: mpsc::Sender<Reading>) {
    let mut backoff = Backoff::new(cfg.reconnect_backoff_min, cfg.reconnect_backoff_max);
    loop {
        let mut ctx = match connect(&cfg.transport).await {
            Ok(c) => { backoff.reset(); c }
            Err(e) => { warn!(?e, connection = %cfg.id, "connect failed"); backoff.wait().await; continue; }
        };

        let mut ticker = tokio::time::interval(cfg.poll_interval);
        'poll: loop {
            ticker.tick().await;
            for device in &cfg.devices {
                ctx.set_slave(Slave(device.unit_id));
                for block in &device.blocks {
                    match timeout(cfg.io_timeout, ctx.read_holding_registers(block.start, block.count)).await {
                        Ok(Ok(words)) => {
                            for (point_name, value) in decode(block, &words) {
                                let _ = tx.try_send(Reading::new(&device.id, &point_name, value));
                            }
                        }
                        Ok(Err(e)) => {
                            warn!(?e, connection = %cfg.id, device = %device.id, "modbus error");
                            break 'poll; // drop connection, reconnect
                        }
                        Err(_) => {
                            warn!(connection = %cfg.id, device = %device.id, "io timeout");
                            break 'poll;
                        }
                    }
                    if let Some(delay) = cfg.inter_frame_delay {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }
}
```

### 3. Aggregator
- **Tumbling window, wall-clock aligned** (to `:00` of each minute) — not a
  sliding average, and not "60s after process start." Only running
  sum/count/min/max per point are kept, so memory stays flat regardless of
  poll rate.
- State is **pre-populated from config** at startup (every configured
  `(device, point)`), not built lazily — lets a point with zero samples in
  a window (dead device, stuck connection) be detected and logged instead
  of silently omitted.
- Single task owns the state — no locks needed.
- `window += 60s` each tick (not "now + 60s" recomputed each time) keeps it
  drift-free relative to wall clock even if a tick is briefly late.

```rust
struct PointAgg { sum: f64, count: u32, min: f64, max: f64, unit: String }

async fn run_aggregator(mut rx: mpsc::Receiver<Reading>, publish_tx: mpsc::Sender<AggregatedBatch>) {
    let mut agg: HashMap<PointId, PointAgg> = init_from_config();
    let mut window = next_minute_boundary();
    let mut sleep = tokio::time::sleep_until(window);
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
                let batch = flush(&mut agg, window_start, window);
                let _ = publish_tx.send(batch).await;
                window += Duration::from_secs(60);
                sleep.as_mut().reset(window);
            }
        }
    }
}
```

Points with `count == 0` at flush are dropped from the published batch
(logged as a warning), not fabricated as zero or repeated — a real gap
should stay visible.

### 4. SenML encoder
Pure function, no async. `serde_json::to_vec(&Vec<SenmlRecord>)` — no
dedicated SenML crate needed for a record shape this simple.

- `bn` (base name) = `base_name_prefix` (config) + `device.id`.
- `bt` (base time) = the window's `window_start` (Unix timestamp), set once
  on the first record; per-record `t` would be redundant.
- `bu` (base unit), optional, only when most points in a pack share a unit.
- `n`, `u`, `v` per point from `AveragedPoint` (`name` / config `unit` / `mean`).

Example pack (device `meter1`):
```json
[
  { "bn": "urn:dev:gw-router1:meter1:", "bt": 1758447120, "bu": "V",
    "n": "voltage_l1", "u": "V", "v": 231.4 },
  { "n": "current_l1", "u": "A", "v": 4.82 },
  { "n": "active_power", "u": "W", "v": 1112.6 },
  { "n": "energy_total", "u": "kWh", "v": 18452.311 }
]
```

Decisions to make explicitly:
- **One pack per device** (matches MQTT topic granularity, one publish/min
  per device) vs one pack per point (if topic scheme needs per-point routing).
- **min/max/sample_count**: not standard SenML fields. Either drop them (spec-pure,
  smaller payload — recommended for v1) or add as vendor-prefixed extension
  fields (`_sample_count`, `_min`, `_max`) later if a consumer needs them.

### 5. MQTT publisher task
- **QoS 1** (at-least-once): QoS 0 risks silent loss exactly during
  reconnects; QoS 2's 4-way handshake buys exactly-once you don't need —
  duplicates are harmless for timestamped time-series data.
- **Persistent session** (`clean_session = false`, stable `client_id`) —
  QoS 1 in-flight messages survive a reconnect; combined with `rumqttc`'s
  internal queue this gives two layers of buffering across a disconnect.
- **`eventloop.poll()` must run continuously in its own task** — `rumqttc`
  handles reconnect/backoff/ack-tracking inside the eventloop; nothing polls
  it, publishes silently stall.
- **Bounded internal queue** (e.g. 100) — at one publish/device/minute this
  is well over an hour of buffering per device during an outage, without
  unbounded memory growth.
- `client.publish()` returning `Err` means the queue is full (real
  backpressure), distinct from ordinary transient disconnects — log separately.
- `keep_alive`: 30s default; consider 60–90s on flaky/metered WAN links to
  avoid false-positive disconnects, trading off slower dead-connection detection.
- **Last Will and Testament**: retained `telemetry/{gateway_id}/status` =
  `"offline"` (QoS 1) as the will; publish `"online"` (retained) right after
  connecting — lets consumers distinguish "gateway is down" from "nothing new."
- Don't hand-roll a publish retry loop — duplicates what QoS 1 + persistent
  session already provide.
- Don't let the publisher block the aggregator — connected by a channel,
  never called directly, so a slow/reconnecting MQTT link never delays
  the next Modbus poll or aggregation flush.

```rust
async fn run_publisher(mut rx: mpsc::Receiver<AggregatedBatch>, cfg: MqttConfig) {
    let mqtt_opts = build_mqtt_options(&cfg); // client_id, clean_session=false, keep_alive, TLS, will
    let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 100);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_event) => { /* log Publish acks, ConnAck, etc. at debug */ }
                Err(e) => {
                    warn!(?e, "mqtt eventloop error, rumqttc will auto-reconnect");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    });

    while let Some(batch) = rx.recv().await {
        let payload = encode_senml(&batch);
        let topic = format!("telemetry/{}", batch.device_id);
        if let Err(e) = client.publish(topic, QoS::AtLeastOnce, false, payload).await {
            error!(?e, device = %batch.device_id, "failed to queue publish, dropping batch");
        }
    }
}
```

## Config format (TOML)

```toml
[gateway]
aggregation_window_secs = 60
base_name_prefix = "urn:dev:gw-router1:"
log_level = "info"

[mqtt]
broker_host = "10.0.0.5"
broker_port = 8883
client_id = "modbus-gateway-router1"
tls = true
# ca_cert_path = "/etc/modbus-gateway/ca.pem"
username = "gateway"
password_env = "MQTT_PASSWORD"   # read from env var, never plaintext in file
qos = 1
keep_alive_secs = 30
topic_template = "telemetry/{device}/{point}"

# ---- Connection 1: Modbus/TCP, one device ----
[[connection]]
id = "meter-tcp1"
transport = "tcp"
host = "192.168.1.50"
port = 502
poll_interval_secs = 10
io_timeout_ms = 1000
reconnect_backoff_min_secs = 1
reconnect_backoff_max_secs = 30

  [[connection.device]]
  id = "meter1"
  unit_id = 1

    [[connection.device.block]]
    function = "holding"       # holding | input
    start = 3000
    count = 8                  # words read in one request
    word_order = "big_endian"  # big_endian | little_endian

      [[connection.device.block.point]]
      name = "voltage_l1"
      offset = 0                # word offset within the block
      data_type = "f32"         # u16 | i16 | u32 | i32 | f32
      scale = 1.0
      unit = "V"

      [[connection.device.block.point]]
      name = "current_l1"
      offset = 2
      data_type = "f32"
      scale = 1.0
      unit = "A"

      [[connection.device.block.point]]
      name = "active_power"
      offset = 4
      data_type = "i32"
      scale = 0.1
      absolute = true            # CT may be wired reversed; only magnitude matters here
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

# ---- Connection 2: Modbus/RTU, one RS-485 bus with two multidropped devices ----
[[connection]]
id = "rs485-bus1"
transport = "rtu"
serial_port = "/dev/ttyUSB0"
baud_rate = 9600
data_bits = 8
parity = "none"               # none | even | odd
stop_bits = 1
poll_interval_secs = 15
io_timeout_ms = 500
inter_frame_delay_ms = 10      # extra gap between requests; some USB-RS485 adapters need it
reconnect_backoff_min_secs = 1
reconnect_backoff_max_secs = 30

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

Design notes on the format:
- **`connection` vs `device`**: `connection` is the physical/logical channel
  (a TCP socket or a serial port); `device` is one Modbus unit ID reachable
  on it. Multiple devices can share one connection — required for RS-485
  multidrop buses, and it also covers Modbus/TCP-to-RTU gateways that
  multiplex several unit IDs over a single socket.
- `poll_interval_secs`, `io_timeout_ms`, and the reconnect backoff live on
  `connection`, not `device` — the whole channel is polled and reconnected
  as a unit.
- `host`/`port` apply only when `transport = "tcp"`; `serial_port`/
  `baud_rate`/`data_bits`/`parity`/`stop_bits`/`inter_frame_delay_ms` only
  when `transport = "rtu"`. Reject the config at load time if the wrong
  set is present for the chosen transport.
- **`block` vs `point`**: `block` is what's actually sent over the wire (one
  contiguous Modbus read); `point` is how to slice/interpret words inside it.
  Keeps the "one request per block" batching rule visible in config.
- `offset` is in **words**, not bytes — matches Modbus register addressing
  and disambiguates 1-word (`u16`/`i16`) vs 2-word (`u32`/`f32`) types.
- `word_order` is per-block (a device-wiring convention), not per-point.
- `scale` is folded directly into decode: `raw_value * scale`.
- `absolute` (default `false`) applies `.abs()` to the value *after*
  scaling — for points where only magnitude is meaningful (e.g. a CT clamp
  that can be installed backwards, producing a negative reading with the
  correct magnitude).
- `unit` feeds straight into SenML's `u` field (use UCUM-style strings:
  `Cel`, `%RH`, `W`, `kWh`) — no separate translation table needed.
- `password_env` (not a plaintext field) keeps secrets out of the config
  file, which matters more than usual since this lives on a router's disk.

Validate at load time: offsets fit within their block's `count`, no
overlapping points, `start + count` doesn't exceed Modbus's 125-register
read limit per request, `unit_id` unique within a connection, `serial_port`
path not reused across connections (can't open the same device node
twice), and the transport-specific field set (tcp vs rtu) matches
`transport`.

## Open items / next steps
- Decide one-pack-per-device vs one-pack-per-point for SenML/MQTT topics.
- Decide whether to add SenML extension fields (`_min`, `_max`, `_sample_count`).
- Pick TLS setup details for MQTT (cert pinning vs system CA store).
- Write the supervisor/restart logic for connection poller tasks.
- Decide on metrics/observability approach (structured logs via `tracing`
  are assumed; consider whether a lightweight metrics endpoint is worth
  the added footprint).
- **Serial port access**: the Alpine LXC container needs the serial device
  node passed through (bind-mount or `lxc.mount.entry`/cgroup device rule
  for `/dev/ttyUSB0` etc.), plus a stable identifier — USB adapters can
  renumber across reboots, so a udev rule (by-id symlink) is safer than a
  raw `/dev/ttyUSBn` path in config.
- Confirm whether `inter_frame_delay_ms` needs a per-adapter default or is
  fine left at 0 until a specific device proves otherwise.
