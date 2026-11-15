# Deployment

Single static musl binary, run as an OpenRC service inside an Alpine LXC
container (design.md §10).

For the router's armv7 container runtime, [router/](router/README.md)
builds the complete Alpine rootfs (binary, start script) in Docker. The
site config is mounted by the router at runtime.

## Build

The release profile in `Cargo.toml` already sets `opt-level = "z"`, LTO,
one codegen unit and stripping. `panic = "abort"` is deliberately **not**
set: the connection supervisor (`src/modbus/supervisor.rs`) relies on
unwinding to catch a panicked connection task and respawn it.

TLS uses rustls with the `ring` provider only, so the one C dependency is
`ring`'s small crypto core, and cross-building it needs a C compiler for
the target.

### x86_64

```sh
rustup target add x86_64-unknown-linux-musl

# With a musl toolchain installed (Debian/Ubuntu: apt install musl-tools):
cargo build --release --target x86_64-unknown-linux-musl

# Without one, the host gcc can build ring's C/asm for x86_64 musl:
CC_x86_64_unknown_linux_musl=gcc \
  cargo build --release --target x86_64-unknown-linux-musl
```

Check the result:

```sh
$ file target/x86_64-unknown-linux-musl/release/modbus-senml-gateway
... ELF 64-bit LSB pie executable, x86-64, ..., static-pie linked, ..., stripped
```

### aarch64

Needs an aarch64 musl C cross-compiler for `ring`. Either install one and
point cargo at it:

```sh
rustup target add aarch64-unknown-linux-musl
CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc \
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=aarch64-linux-musl-gcc \
  cargo build --release --target aarch64-unknown-linux-musl
```

or use [`cross`](https://github.com/cross-rs/cross), which provides the
toolchain in a container: `cross build --release --target aarch64-unknown-linux-musl`.

## Install (inside the container)

The service runs as the unprivileged user `modbus-gw` with group
`dialout` (serial access for RTU). Create it once:

```sh
adduser -S -D -H -h /nonexistent -s /sbin/nologin -G dialout modbus-gw
```

```sh
install -m 0755 modbus-senml-gateway        /usr/local/bin/
install -m 0755 openrc/modbus-gateway       /etc/init.d/modbus-gateway
install -m 0600 openrc/modbus-gateway.confd /etc/conf.d/modbus-gateway
install -d -m 0755 /etc/modbus-gateway
install -m 0644 config.toml /etc/modbus-gateway/config.toml
install -m 0644 ca.pem      /etc/modbus-gateway/ca.pem   # path from mqtt.ca_cert_path
```

In the config, set `gateway.status_file_path = "/run/modbus-gateway/status"`.
The init script creates `/run/modbus-gateway` at each start, owned by
`modbus-gw` (change it with `GATEWAY_STATUS_DIR` in conf.d).

**Ownership matters.** The gateway trusts its config: it reads the CA file,
opens the serial device and writes the status file at the paths given
there. Keep the config, the CA file and the directories holding them owned
by root and not writable by anyone else (as above). Otherwise whoever can
edit them can make the gateway trust another CA or write to other files.
The config holds no secrets (the password lives in conf.d, mode 0600), so
it can be world-readable.

Edit `/etc/conf.d/modbus-gateway`, then set the MQTT password there. The
variable name must match `mqtt.password_env` in the config:

```sh
supervise_daemon_args="--env MQTT_PASSWORD=..."
```

Enable and start:

```sh
rc-update add modbus-gateway default
rc-service modbus-gateway start
```

`rc-service modbus-gateway stop` sends SIGTERM. The gateway stops polling,
drops the partial aggregation window, gives queued MQTT sends ~2s to drain
and exits. The service allows 10s before escalating to SIGKILL.

Logs go straight to `/dev/log` (facility `daemon`, tag = `gateway.id`).
If syslogd isn't up yet, the gateway retries in the background and writes
to stderr in the meantime (captured only if `error_log` is set in conf.d).
Connectivity summary is in the file at
`gateway.status_file_path`.

## Broker setup

The gateway does not protect its topics on its own; the broker has to.
Any client allowed to publish to `telemetry/<gateway.id>/status` can
overwrite the retained `online`/`offline` message (it is also the Last
Will), and any client allowed to publish to a gateway's telemetry topics
can inject SenML packs.

- **Give every gateway its own username and password.** Set
  `mqtt.username` and `mqtt.client_id` per gateway, e.g. both equal to
  `gateway.id`. A shared login means one leaked password affects all
  gateways, and a second client with the same client ID disconnects the
  first.
- **Put the gateway ID in the telemetry topic** so an ACL can tell
  gateways apart. The default `topic_template = "telemetry/{device}"`
  puts every gateway's devices in one namespace; use e.g.
  `topic_template = "telemetry/gw-router1/{device}"` instead. (The config
  is rejected if a device would end up on the status topic, e.g. a device
  named `status`.)
- **Allow each gateway to write only below its own prefix**, and give
  consumers read-only access.

Mosquitto example (`acl_file` in `mosquitto.conf`), assuming
username = `gateway.id`:

```
# Gateways: write only telemetry/<own username>/...
pattern write telemetry/%u/#

# Ingest service: read everything, write nothing
user ingest
topic read telemetry/#
```

Check it from another login: a publish to `telemetry/gw-router1/status`
must be denied (Mosquitto drops it silently, so subscribe and check that
the retained message did not change).

## Modbus network

Modbus TCP and RTU have no authentication or encryption. Anyone who can
reach a device's TCP port or the RS-485 bus can change readings, spoof
devices or cause timeouts. Put Modbus devices on a separate network
segment (VLAN or firewall rules) that only the gateway can reach, and keep
the serial bus physically protected.

## Serial port passthrough (RTU only)

The container needs the host's serial device node, and the config should
name it by a stable path.

**Use a udev by-id path, not `/dev/ttyUSBn`.** USB serial adapters are
numbered in probe order, so `ttyUSB0` and `ttyUSB1` can swap after a
reboot or re-plug. udev also creates stable symlinks keyed on vendor,
model and serial number. Find them on the host with:

```sh
ls -l /dev/serial/by-id/
# usb-FTDI_USB-RS485-if00-port0 -> ../../ttyUSB0
```

**Pass the device into the LXC container.** In the container's LXC config
on the host, allow the character device's major number and bind-mount the
by-id path (the mount follows the symlink to the real node, and the node
appears inside the container under the same by-id path):

```
# USB serial (ttyUSB*) is major 188; CDC-ACM (ttyACM*) is major 166.
lxc.cgroup2.devices.allow = c 188:* rwm
lxc.mount.entry = /dev/serial/by-id/usb-FTDI_USB-RS485-if00-port0 dev/serial/by-id/usb-FTDI_USB-RS485-if00-port0 none bind,optional,create=file
```

Use the same `/dev/serial/by-id/...` path as `serial_port` in the
gateway config. The bind mount is taken when the container starts, so
re-plugging the adapter needs a container restart.

**Permissions**: the service user needs read/write on the device node,
typically via the `dialout` group (the default `GATEWAY_USER` is
`modbus-gw:dialout`). Check the node's group inside the container with
`ls -l /dev/serial/by-id/`.
