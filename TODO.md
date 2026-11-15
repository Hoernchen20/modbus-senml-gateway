# TODO

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

- Finding 15: the audit ignores are justified. Re-check `.cargo/audit.toml`
  and `.trivyignore.yaml` after every rumqttc/rumqttd bump.
