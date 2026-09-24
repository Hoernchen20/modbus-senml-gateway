# Local test setup (docker compose)

Runs the gateway against real Modbus devices on the development machine,
with a local MQTT broker and a subscriber that prints what gets published.

| Service      | What it does                                                          |
|--------------|-----------------------------------------------------------------------|
| `pki`        | One-shot. Creates a self-signed CA, the broker certificate and the broker password file in `dev/pki/`. |
| `mosquitto`  | Mosquitto broker, TLS only on port 8883 (also published on the host). |
| `gateway`    | The gateway, built from this repository (static musl release build). |
| `subscriber` | `mosquitto_sub` on `telemetry/#`. Prints every SenML pack and the online/offline status. |

## Start

All commands run in `dev/`.

1. Edit [config.toml](config.toml): set the `host`/`port` of your
   Modbus/TCP device and the serial settings of your RTU device, then add
   the blocks and points you want to read. The `[gateway]` and `[mqtt]`
   sections already match the compose services. `aggregation_window_secs`
   is 10 so that messages show up quickly.

2. For RTU: pass the RS-485 adapter into the container.

   ```sh
   cp .env.example .env
   ```

   Uncomment both lines in `.env` and set `RTU_DEVICE` to the adapter on
   the host (`ls -l /dev/serial/by-id/`). It appears as `/dev/ttyRTU0` in
   the container, which is the `serial_port` already set in `config.toml`.
   The adapter must be plugged in before starting.
   Without `.env` the gateway starts without the adapter, and the `rtu1`
   connection keeps retrying while TCP works normally.

3. Start everything:

   ```sh
   docker compose up -d --build
   ```

## Watch

```sh
docker compose logs -f subscriber                  # SenML packs as they arrive
docker compose logs -f gateway                     # gateway log (stderr, no syslog in the container)
docker compose exec gateway cat /tmp/status        # status file
```

Example subscriber output (time, topic, retained flag, payload):

```
2026-09-24T06:16:54+0000 telemetry/gw-dev/status 0 online
2026-09-24T06:17:00+0000 telemetry/meter1 0 [{"bn":"urn:dev:gw-dev:meter1:","bt":1790230610,"n":"value0","u":"V","v":231.4}, ...]
```

To watch from the host with another MQTT client (such as MQTT Explorer or
`mosquitto_sub`), connect to `localhost:8883` with TLS, use `dev/pki/ca.pem`
as the CA certificate and log in as `viewer` / `viewer-dev`:

```sh
mosquitto_sub -h localhost -p 8883 --cafile pki/ca.pem -u viewer -P viewer-dev -t 'telemetry/#' -v
```

The broker certificate is valid for `mosquitto`, `localhost` and
`127.0.0.1`.

## Change and restart

- After changing `config.toml`: `docker compose restart gateway`
- After changing code: `docker compose up -d --build gateway`
- Stop everything: `docker compose down`

## Credentials

Test values only. They are generated into `dev/pki/`, which is not tracked
by git:

| User      | Password      | Used by                                     |
|-----------|---------------|---------------------------------------------|
| `gateway` | `gateway-dev` | the gateway (`MQTT_PASSWORD` in compose.yaml) |
| `viewer`  | `viewer-dev`  | the subscriber and clients on the host      |

The files in `dev/pki/` are kept between runs, so host clients can keep
trusting the same CA. To create a new CA and new credentials, delete the
folder (its files are owned by root) and start again:

```sh
docker compose down
docker run --rm -v "$PWD":/w alpine rm -rf /w/pki
```

## Notes

- The container uses Docker's default bridge network. Outgoing connections
  to Modbus/TCP devices on the LAN work through NAT, like any other
  outgoing connection from the host.
- Serial adapters are named in the order they are plugged in, so
  `/dev/ttyUSB0` can change. Use the `/dev/serial/by-id/` path for
  `RTU_DEVICE`. After unplugging and replugging the adapter, run
  `docker compose up -d --force-recreate gateway`.
