# TODO

## Configurable rounding per point

Published values carry float noise, e.g. `228.49199981689452`, which makes the
SenML messages longer than they need to be.

Add an optional setting to `[[connection.device.block.point]]` for the number
of decimal places, e.g. `decimals = 1`. Without it, values stay unrounded as
they are today.

- Round the final aggregated value just before publishing, so rounding errors
  don't feed into the aggregation.
- A rounded value can still print long, because some decimals have no exact
  binary form. Check that the JSON output comes out short.
- Document the setting in `config.example.toml`.

## Device connection status in the SenML uplink

Right now a device that can't be read sends no pack at all. On the receiving
side, a broken device looks the same as a dead router or a lost MQTT link.

Publish a boolean record per device in every aggregation window, e.g.
`{"n": "online", "vb": false}` (SenML `vb`):

- Emit the pack every window, even when no samples were collected. It then
  holds only the status record (with `bn`/`bt`).
- `online = true` if the device was read successfully at least once in the
  window, `false` otherwise (timeout, exception, protocol error, or the
  connection being down). If the connection is down, every device on it reports
  `false`.
- Get the state from the same poller results that already feed `StatusEvent`
  (`DeviceOk` / `DeviceProblem`), so the uplink and the status file agree.
- Telling the cases apart downstream: pack with `online=false` means a device
  or bus problem; no pack at all means a router, network or MQTT problem.
- Possible extension: send the share of successful polls (`0.0`–`1.0`) instead
  of a bool, to catch flaky devices. Not needed for now.
- Check how this works with `majority_unit` (the status record has no unit) and
  update the tests and `config.example.toml`/docs.

## Security review follow-ups (2026-11-08)

From `security-review.txt`. Finding numbers refer to that file.

### Worth doing

#### Tighter config validation (findings 4, 5g)

Some config values are accepted but cause tight loops, flash wear or panics:

- `status_heartbeat_secs = 0`: rewrites the status file in a tight loop (flash!).
- `reconnect_backoff_max_secs = 0`: tight reconnect loop. Also require
  `min <= max`.
- `io_timeout_ms = 0`: every request times out at once.
- Very large `aggregation_window_secs` / `poll_interval_secs` / timeouts:
  `Instant::now() + d` overflows and panics. Set reasonable upper limits.
- `keep_alive_secs > 65535` gets truncated by rumqttc (`65536` turns into `0`,
  which disables keep-alive).
- `scale` must be finite (TOML accepts `nan`/`inf`).
- Reject empty `broker_host`, `client_id`, `username` and `topic_template`.
- Add a test for each case: zero and extreme values.

#### Drop non-finite readings (finding 5)

A single NaN/Inf (e.g. an F32 register with a fault value) makes the whole
window's mean NaN, and serde_json writes it as `"v": null`, which is invalid
SenML. Drop non-finite values after scaling, before they reach the
aggregator, with a rate-limited warning. Tests for NaN/Inf. Possibly ties in
with the device status record above.

#### Validate IDs: charset and uniqueness (findings 6, 7, 11)

All checks belong in `validate.rs`:

- Gateway, connection, device and point IDs must match `[A-Za-z0-9_.-]+`, with
  a length limit. That prevents `/`, `#`, `+` in MQTT topics and newlines in
  syslog and the status file.
- `topic_template` must contain exactly one `{device}`.
- Connection IDs and device IDs must be globally unique, and point names unique
  per device. Today duplicates silently merge their readings in the aggregator.

#### Warn about unknown config keys (finding 16)

A typo in an optional key (e.g. `status_file_pth`) silently falls back to the
default. `#[serde(deny_unknown_fields)]` doesn't work on `ConnectionConfig`,
because it uses `#[serde(flatten)]` with the internally tagged `Transport`.
Options: deny it on the other structs only, or collect ignored keys with the
`serde_ignored` crate and warn or fail at startup.

#### Safer status file temp write (finding 3)

`write_atomic` creates a fixed `<path>.tmp` with `File::create`, which follows
symlinks. Cheap to fix: open with `O_NOFOLLOW` and mode `0o600`, and delete a
leftover `.tmp` first (`create_new` alone would get stuck after a crash). The
practical risk in the router container is low (only the gateway runs there),
but the change is small.

### Nice to have

- **Warn when `tls = false`** (finding 1): log a clear warning at startup that
  the password and telemetry go out unencrypted. Don't reject it, because the
  dev setup / isolated networks need plaintext.
- **Read the CA file only once** (finding 9): validation and
  `build_mqtt_options` each read it. Pass the bytes along. A small cleanup, the
  real risk is minimal.
- **Password as a secret type** (finding 10): `zeroize`/`secrecy` for the
  password, and never print `MqttOptions` with `{:?}` (the rumqttc Debug output
  includes the password).
- **Don't run as root** (finding 8): in the router container probably not
  practical (`mknod /dev/ttyS1` in `start.sh` needs root; you could drop
  privileges afterwards). For OpenRC, a dedicated user plus `dialout` would be
  doable. Document that the config and the paths it points to must be owned by
  root.
- **TLS 1.3 only** (finding 14): disable the rustls `tls12` feature if every
  broker supports 1.3.

### No action (accepted risk / already fine)

- Finding 12: Modbus has no authentication. This is a design assumption,
  covered by network segmentation.
- Finding 13: the retained status topic depends on broker ACLs. Note it in the
  deploy docs.
- Finding 15: the audit ignores are justified. Re-check `.cargo/audit.toml`
  and `.trivyignore.yaml` after every rumqttc/rumqttd bump.
