# Router container image (armv7)

Builds `dist/alpine-rootfs.tgz`, a complete Alpine rootfs for the
router's container runtime. It contains the gateway and a start script.
The site config is not part of the image; the router mounts it into the
container at runtime (see [Site config](#site-config)). The build follows
the router manufacturer's example scripts (download the Alpine
minirootfs, add packages and binaries, add `/etc/start.sh`, pack
everything as a `.tgz`), but the whole build runs in Docker on an
x86_64 host:

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

The site config is not built into the rootfs. Upload the files through the
router's container configuration; the router mounts them into the running
container under `/dev/container_config/files/`. The same image can
therefore be used on every router, and changing the config needs no
rebuild.

| File           | Purpose                                                   |
|----------------|-----------------------------------------------------------|
| `config.toml`  | Gateway config (see `config.example.toml`, also included in the image as `/usr/local/share/modbus-gateway/config.example.toml`) |
| `ca.pem`       | Broker CA certificate. Set `mqtt.ca_cert_path = "/dev/container_config/files/ca.pem"` |
| `gateway.env`  | `MQTT_PASSWORD=...`. The variable name must match `mqtt.password_env`. |

`mqtt.ca_cert_path` has to point into the mounted directory:

```toml
[mqtt]
ca_cert_path = "/dev/container_config/files/ca.pem"
```

`status_file_path` already defaults to `/dev/container_config/status`,
where the router shows it outside the container.

Until `config.toml` is present, the gateway waits, checking every 5 s and
logging. `config.toml` and `gateway.env` are read each time the gateway
(re)starts, so after changing them restart the container or kill the
`modbus-senml-gateway` process; `run-gateway` starts it again with the
new files after 5 s.

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

`run-gateway` loads `/dev/container_config/files/gateway.env`, runs
`/usr/local/bin/modbus-senml-gateway /dev/container_config/files/config.toml`
and restarts it 5 s after any exit. SIGTERM is forwarded to the gateway, which
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
