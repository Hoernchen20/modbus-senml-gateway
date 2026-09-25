# Router container image (armv7)

Builds `dist/alpine-rootfs.tgz`, a complete Alpine rootfs for the
router's container runtime. It contains the gateway, a start script, and
optionally your site config. It follows the router manufacturer's example
scripts (download the Alpine minirootfs, add packages and binaries, add
`/etc/start.sh`, pack everything as a `.tgz`), but the whole build runs in
Docker on an x86_64 host:

- The gateway is **cross-compiled** for `armv7-unknown-linux-musleabihf`
  as a static binary. Zig (via `cargo-zigbuild`) provides the armv7
  C compiler and linker for `ring`. There is no build chroot and no qemu.
- Packages are installed into the armv7 rootfs by the build host's `apk`
  (`apk --root`). The rootfs is upgraded to the latest 3.22 packages first.

The Docker requirement is just `docker build`. BuildKit/buildx is
**not** needed.

## Build

```sh
deploy/router/build.sh
```

The first build takes a few minutes (cargo-zigbuild install plus the LTO
release build). The result is `dist/alpine-rootfs.tgz`.

Options are Docker build args, passed through by `build.sh`:

| Build arg        | Default    | Meaning                                    |
|------------------|------------|--------------------------------------------|
| `ALPINE_VERSION` | `3.22.6`   | Alpine minirootfs release (checksum-verified) |
| `ALPINE_ARCH`    | `armv7`    | Alpine architecture of the rootfs          |
| `RUST_TARGET`    | `armv7-unknown-linux-musleabihf` | Rust target, must match `ALPINE_ARCH` |
| `PACKAGES`       | `tzdata`   | Extra Alpine packages in the rootfs        |

For example, to add debugging tools as the manufacturer example does:

```sh
deploy/router/build.sh --build-arg PACKAGES="tzdata tcpdump mosquitto-clients"
```

Packages are installed with `--no-scripts`, because armv7 install scripts
can't run on the build host. That is fine for most packages. A package
whose install script creates a user or group needs that step added to the
Dockerfile by hand.

## Site config

Files in `deploy/router/site/` are copied to `/etc/modbus-gateway/` in the
rootfs. The directory is git-ignored except for `.gitkeep`, so it can hold
secrets.

| File           | Purpose                                                   |
|----------------|-----------------------------------------------------------|
| `config.toml`  | Gateway config (see `config.example.toml`, which is always included as `/etc/modbus-gateway/config.example.toml`) |
| `ca.pem`       | Broker CA certificate, at the path set in `mqtt.ca_cert_path` |
| `gateway.env`  | `MQTT_PASSWORD=...`. The variable name must match `mqtt.password_env`. Installed with mode 0600. |

Without `config.toml` the image still builds. The gateway then waits,
checking every 5 s and logging, until
`/etc/modbus-gateway/config.toml` appears in the running container.

## What runs in the container

The router runs `/etc/start.sh` when the container boots. The script:

1. creates the serial device node `/dev/ttyS1` (`mknod /dev/ttyS1 c 207 17`,
   as specified by the router documentation), if it doesn't exist yet.
2. starts busybox `syslogd`, unless `/dev/log` already exists (i.e. the
   router provides it). The gateway logs directly to `/dev/log`.
3. starts `/usr/local/sbin/run-gateway` in the background and returns.

`start.sh` must terminate. The router documentation says the container is
otherwise not reachable. Anything added to it has to be started in the
background (`&`) or be a self-daemonizing command.

`run-gateway` loads `gateway.env`, runs
`/usr/local/bin/modbus-senml-gateway /etc/modbus-gateway/config.toml` and
restarts it 5 s after any exit. SIGTERM is forwarded to the gateway, which
then shuts down gracefully (see [../README.md](../README.md)). Gateway
stderr, which is only used while `/dev/log` is missing, goes to
`/var/log/modbus-gateway.err`.

The OpenRC files in [../openrc/](../openrc/) are for a regular Alpine LXC
container with OpenRC. The router image doesn't use them.

## Serial port (RTU)

The router's serial port is `/dev/ttyS1` inside the container, created by
`start.sh` at boot. Set it in the RTU connection of `config.toml`:

```toml
serial_port = "/dev/ttyS1"
```

The generic LXC passthrough in
[../README.md](../README.md#serial-port-passthrough-rtu-only) (udev by-id
paths, `lxc.mount.entry`) doesn't apply to the router.
