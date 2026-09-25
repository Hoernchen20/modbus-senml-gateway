# Deployment

Single static musl binary, run as an OpenRC service inside an Alpine LXC
container (design.md §10).

For the router's armv7 container runtime, [router/](router/README.md)
builds the complete Alpine rootfs (binary, start script, config) in Docker.

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

```sh
install -m 0755 modbus-senml-gateway        /usr/local/bin/
install -m 0755 openrc/modbus-gateway       /etc/init.d/modbus-gateway
install -m 0600 openrc/modbus-gateway.confd /etc/conf.d/modbus-gateway
install -d /etc/modbus-gateway
install -m 0640 config.toml /etc/modbus-gateway/config.toml
install -m 0644 ca.pem      /etc/modbus-gateway/ca.pem   # path from mqtt.ca_cert_path
```

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

**Permissions**: if the service runs as a non-root user
(`GATEWAY_USER` in conf.d), that user needs read/write on the device
node, typically via the `dialout` group, plus write access to the
directory holding `gateway.status_file_path`.
