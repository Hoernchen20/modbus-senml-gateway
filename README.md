# Modbus SenML Gateway

A small service that polls measurements from Modbus devices, averages them
over a fixed time window, and publishes the results as
[SenML](https://www.rfc-editor.org/rfc/rfc8428) JSON over MQTT.

It is built to run unattended on a resource-constrained router (Alpine
Linux in an LXC container) as a single static binary.

## Features

- **Modbus/TCP and Modbus/RTU**, including several devices sharing one
  RS-485 bus.
- **Config-driven.** Devices, registers and points are defined in one
  TOML file. No code changes are needed to add or remove a device.
- **Averaging.** Each point is averaged over a tumbling window (default 60 s)
  aligned to the wall clock. Supports scaling, absolute value, 16/32-bit
  integers and 32-bit floats, and both word orders.
- **SenML output.** One SenML pack per device per window, published to a
  per-device MQTT topic.
- **Reliable MQTT.** QoS 1 by default, persistent session, automatic
  reconnect, TLS against a private CA, and a retained online/offline status
  message (LWT).
- **Fault isolation.** One dead device or bus never stops telemetry from the
  others. A device that times out or returns a Modbus exception is skipped
  for that poll; only a broken socket or serial port triggers a reconnect,
  with exponential backoff. A panicking connection task is restarted.
- **Observability.** Logs go directly to syslog (`/dev/log`, facility
  `daemon`). A small plain-text status file summarises MQTT and connection
  health.

Read-only: the gateway never writes to Modbus devices.

## How it works

```
Config (TOML)
     │
     ▼
Poller task (one per connection) ──┐
Poller task (one per connection) ──┼──► Aggregator ──► SenML encoder ──► MQTT publisher ──► Broker
Poller task (one per connection) ──┘    (time window)
```

- A **connection** is one TCP socket or one serial port. Each connection
  has its own poller task, which reads all of its devices one after another
  every `poll_interval_secs`.
- A **device** is one Modbus unit id on a connection.
- A **block** is one Modbus read request: `count` consecutive holding or
  input registers starting at `start`.
- A **point** is one value decoded from a block (`u16`, `i16`, `u32`, `i32`
  or `f32`).

At the end of every window the aggregator computes the mean of each point
and sends one batch per device to the publisher. A point with no successful
reading in a window is left out of that window's message (and a warning is
logged). The gateway never fills gaps with zeros or repeated values.

## Configuration

The gateway takes the path to its config file as the only command-line
argument. Without one it reads `/etc/modbus-gateway/config.toml`.

```sh
modbus-senml-gateway /etc/modbus-gateway/config.toml
```

See [config.example.toml](config.example.toml) for a complete, commented
example that documents every parameter, its default, and its allowed range.

The config is checked when the gateway starts. It exits with an error
message on stderr if, for example:

- a TCP connection has serial settings or an RTU connection is missing
  `baud_rate`,
- two devices on one connection share a `unit_id`,
- two connections use the same serial port,
- a block reads more than 125 registers,
- a point extends past the end of its block, or two points overlap,
- the CA certificate file is missing or contains no PEM certificate.

The MQTT password is not stored in the config file. It is read from the
environment variable named by `mqtt.password_env`.

## Output

### Telemetry

For each device, once per window, a SenML pack is published to
`topic_template` with `{device}` replaced by the device id (for example
`telemetry/meter1`):

```json
[
  { "bn": "urn:dev:gw-router1:meter1:", "bt": 1758447120,
    "n": "voltage_l1", "u": "V", "v": 231.4 },
  { "n": "current_l1", "u": "A", "v": 4.82 },
  { "n": "active_power", "u": "W", "v": 1112.6 },
  { "n": "energy_total", "u": "kWh", "v": 18452.311 }
]
```

- `bn` = `gateway.base_name_prefix` + device id + `:`
- `bt` = start of the averaging window (Unix seconds)
- `n` / `u` / `v` = point name, unit and mean value

### Online/offline status

The topic `telemetry/<gateway.id>/status` holds a retained `online` or
`offline` message. `offline` is also registered as the MQTT Last Will, so it
appears if the gateway loses its connection unexpectedly.

### Status file

The gateway writes a short summary to `gateway.status_file_path` (default
`/dev/container_config/status`):

```
gateway: gw-router1
updated: 2026-09-23T14:32:10Z
mqtt: connected (10.0.0.5:8883)
connection meter-tcp1: up, 1/1 devices ok
connection rs485-bus1: up, 1/2 devices ok (tempsensor2: timeout since 14:28:15Z)
```

The file is rewritten shortly after any state change and at least every
`status_heartbeat_secs`, so an old `updated:` timestamp means the gateway
has stopped working. Writes are atomic (write to a temporary file, then
rename) and the file never exceeds 2048 bytes.

### Logs

Log messages are sent directly to syslog through `/dev/log` with facility
`daemon` and `gateway.id` as the tag. The level is set by
`gateway.log_level`. If syslog is not available yet at startup, messages go
to stderr until it is.

## How to build

```sh
cargo build --release
```

For the router, [deploy/router/](deploy/router/README.md) cross-compiles
the gateway for armv7 in Docker and builds the complete Alpine rootfs
(`dist/alpine-rootfs.tgz`) that is installed on the router.
Static builds for other targets are described in
[deploy/README.md](deploy/README.md#build).

## Deployment

[deploy/README.md](deploy/README.md) covers installing the binary and the
OpenRC service, passing the MQTT password, and passing a serial port into
the LXC container for RTU connections.

On SIGTERM (`rc-service modbus-gateway stop`) the gateway stops polling,
discards the unfinished averaging window, gives queued MQTT messages about
2 s to be sent, and exits.

## Limitations

- No buffering to disk. Messages are held in memory while the broker is
  unreachable (up to 100 queued messages). A restart during a broker outage
  loses them.
- No live config reload. Restart the service after changing the config.
- No Modbus writes, coils or discrete inputs. Only holding and input
  registers are read.
- No client-certificate (mTLS) authentication for MQTT.
- One MQTT broker per gateway.

## Development

```sh
cargo test
```

The tests include integration tests against a mock Modbus/TCP server, a
virtual serial port pair for RTU, and an embedded MQTT broker.

To test against real Modbus/TCP and RTU devices, [dev/](dev/README.md) has
a docker compose setup that runs the gateway together with a TLS
Mosquitto broker and a subscriber that prints the SenML messages.

Design details:

- [design.md](design.md): specification (config schema, error handling,
  aggregation, output formats)
- [modbus-senml-mqtt-gateway-architecture.md](modbus-senml-mqtt-gateway-architecture.md):
  architecture notes and rationale
