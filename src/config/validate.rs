use std::collections::{HashMap, HashSet};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use thiserror::Error;

use super::schema::{Config, Transport};

/// Upper bounds keep `Instant::now() + duration` far from overflowing and
/// catch obvious unit mistakes (ms vs. s).
const MAX_AGGREGATION_WINDOW_SECS: u64 = 86_400;
const MAX_POLL_INTERVAL_SECS: u64 = 86_400;
const MAX_IO_TIMEOUT_MS: u64 = 60_000;
const MAX_BACKOFF_SECS: u64 = 3_600;
const MAX_STATUS_SECS: u64 = 3_600;
/// rumqttc truncates keep-alive to u16 (`65536` would become `0`).
const MAX_KEEP_ALIVE_SECS: u64 = u16::MAX as u64;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("{field} = {value} is out of range (allowed: {min}..={max})")]
    OutOfRange {
        field: String,
        value: u64,
        min: u64,
        max: u64,
    },

    #[error(
        "connection '{connection}': reconnect_backoff_min_secs ({min}) must not exceed reconnect_backoff_max_secs ({max})"
    )]
    BackoffMinExceedsMax {
        connection: String,
        min: u64,
        max: u64,
    },

    #[error("device '{device}' point '{point}': scale = {scale} is not a finite number")]
    NonFiniteScale {
        device: String,
        point: String,
        scale: f64,
    },

    #[error("{0} must not be empty")]
    Empty(&'static str),

    #[error("mqtt.qos = {0} is not a valid QoS (allowed: 0, 1, 2)")]
    InvalidQos(u8),

    #[error(
        "gateway.log_level = '{0}' is not a valid level (allowed: off, error, warn, info, debug, trace)"
    )]
    InvalidLogLevel(String),

    #[error("connection '{connection}': duplicate unit_id {unit_id}")]
    DuplicateUnitId { connection: String, unit_id: u8 },

    #[error(
        "serial_port '{serial_port}' is used by both connection '{first}' and connection '{second}'"
    )]
    DuplicateSerialPort {
        serial_port: String,
        first: String,
        second: String,
    },

    #[error("connection '{connection}': {field} = {value} is not supported (allowed: {allowed})")]
    UnsupportedSerialSetting {
        connection: String,
        field: &'static str,
        value: u8,
        allowed: &'static str,
    },

    #[error(
        "connection '{connection}' device '{device}' block at {start}: count ({count}) exceeds the 125-register Modbus read limit per request"
    )]
    BlockTooLarge {
        connection: String,
        device: String,
        start: u16,
        count: u16,
    },

    #[error(
        "connection '{connection}' device '{device}' block at {start}: point '{point}' at offset {offset} (width {width}) exceeds block count {count}"
    )]
    PointOutOfBounds {
        connection: String,
        device: String,
        start: u16,
        point: String,
        offset: u16,
        width: u16,
        count: u16,
    },

    #[error(
        "connection '{connection}' device '{device}' block at {start}: points '{a}' and '{b}' overlap"
    )]
    OverlappingPoints {
        connection: String,
        device: String,
        start: u16,
        a: String,
        b: String,
    },

    #[error("mqtt.ca_cert_path '{path}': failed to read file: {source}")]
    CaCertUnreadable {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("mqtt.ca_cert_path '{path}': no PEM certificate found in file")]
    CaCertEmpty { path: String },

    #[error("mqtt.ca_cert_path '{path}': failed to parse PEM: {source}")]
    CaCertUnparseable {
        path: String,
        #[source]
        source: rustls::pki_types::pem::Error,
    },
}

fn check_range(
    field: impl Into<String>,
    value: u64,
    min: u64,
    max: u64,
) -> Result<(), ValidationError> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(ValidationError::OutOfRange {
            field: field.into(),
            value,
            min,
            max,
        })
    }
}

fn check_not_empty(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        Err(ValidationError::Empty(field))
    } else {
        Ok(())
    }
}

pub fn validate(config: &Config) -> Result<(), ValidationError> {
    let gw = &config.gateway;
    // A zero window would divide by zero in the aggregator's boundary
    // alignment; a huge one overflows `Instant::now() + wait`.
    check_range(
        "gateway.aggregation_window_secs",
        gw.aggregation_window_secs,
        1,
        MAX_AGGREGATION_WINDOW_SECS,
    )?;
    check_range(
        "gateway.status_debounce_secs",
        gw.status_debounce_secs,
        0,
        MAX_STATUS_SECS,
    )?;
    // Zero would rewrite the status file (often on flash) in a tight loop.
    check_range(
        "gateway.status_heartbeat_secs",
        gw.status_heartbeat_secs,
        1,
        MAX_STATUS_SECS,
    )?;

    let mqtt = &config.mqtt;
    check_not_empty("mqtt.broker_host", &mqtt.broker_host)?;
    check_not_empty("mqtt.client_id", &mqtt.client_id)?;
    check_not_empty("mqtt.username", &mqtt.username)?;
    check_not_empty("mqtt.topic_template", &mqtt.topic_template)?;
    check_range(
        "mqtt.keep_alive_secs",
        mqtt.keep_alive_secs,
        1,
        MAX_KEEP_ALIVE_SECS,
    )?;

    if config
        .gateway
        .log_level
        .parse::<tracing::level_filters::LevelFilter>()
        .is_err()
    {
        return Err(ValidationError::InvalidLogLevel(
            config.gateway.log_level.clone(),
        ));
    }

    if config.mqtt.qos > 2 {
        return Err(ValidationError::InvalidQos(config.mqtt.qos));
    }

    let mut serial_ports: HashMap<String, String> = HashMap::new();

    for conn in &config.connections {
        // A zero interval would panic `tokio::time::interval`.
        check_range(
            format!("connection '{}': poll_interval_secs", conn.id),
            conn.poll_interval_secs,
            1,
            MAX_POLL_INTERVAL_SECS,
        )?;
        check_range(
            format!("connection '{}': io_timeout_ms", conn.id),
            conn.io_timeout_ms,
            1,
            MAX_IO_TIMEOUT_MS,
        )?;
        // A zero backoff never grows (0 * 2 = 0): a tight reconnect loop.
        check_range(
            format!("connection '{}': reconnect_backoff_min_secs", conn.id),
            conn.reconnect_backoff_min_secs,
            1,
            MAX_BACKOFF_SECS,
        )?;
        check_range(
            format!("connection '{}': reconnect_backoff_max_secs", conn.id),
            conn.reconnect_backoff_max_secs,
            1,
            MAX_BACKOFF_SECS,
        )?;
        if conn.reconnect_backoff_min_secs > conn.reconnect_backoff_max_secs {
            return Err(ValidationError::BackoffMinExceedsMax {
                connection: conn.id.clone(),
                min: conn.reconnect_backoff_min_secs,
                max: conn.reconnect_backoff_max_secs,
            });
        }

        let mut seen_unit_ids = HashSet::new();
        for device in &conn.devices {
            for point in device.blocks.iter().flat_map(|b| &b.points) {
                if !point.scale.is_finite() {
                    return Err(ValidationError::NonFiniteScale {
                        device: device.id.clone(),
                        point: point.name.clone(),
                        scale: point.scale,
                    });
                }
            }

            if !seen_unit_ids.insert(device.unit_id) {
                return Err(ValidationError::DuplicateUnitId {
                    connection: conn.id.clone(),
                    unit_id: device.unit_id,
                });
            }
        }

        if let Transport::Rtu {
            serial_port,
            data_bits,
            stop_bits,
            ..
        } = &conn.transport
        {
            if !(5..=8).contains(data_bits) {
                return Err(ValidationError::UnsupportedSerialSetting {
                    connection: conn.id.clone(),
                    field: "data_bits",
                    value: *data_bits,
                    allowed: "5, 6, 7, 8",
                });
            }
            if !(1..=2).contains(stop_bits) {
                return Err(ValidationError::UnsupportedSerialSetting {
                    connection: conn.id.clone(),
                    field: "stop_bits",
                    value: *stop_bits,
                    allowed: "1, 2",
                });
            }
            if let Some(first) = serial_ports.get(serial_port) {
                return Err(ValidationError::DuplicateSerialPort {
                    serial_port: serial_port.clone(),
                    first: first.clone(),
                    second: conn.id.clone(),
                });
            }
            serial_ports.insert(serial_port.clone(), conn.id.clone());
        }

        for device in &conn.devices {
            for block in &device.blocks {
                // The 125-register cap is on the quantity requested in a single
                // Modbus read (count), not on the register address (start) —
                // addresses like the 3000s in design.md's own §4.6 example are
                // ordinary Modicon-style addressing and far exceed 125 on their
                // own, so `start + count` is not the right comparison despite
                // how §4.2/§6.1 phrase it.
                if block.count > 125 {
                    return Err(ValidationError::BlockTooLarge {
                        connection: conn.id.clone(),
                        device: device.id.clone(),
                        start: block.start,
                        count: block.count,
                    });
                }

                let mut spans: Vec<(u16, u16, &str)> = Vec::with_capacity(block.points.len());
                for point in &block.points {
                    let width = point.data_type.word_count();
                    let point_end = point.offset as u32 + width as u32;
                    if point_end > block.count as u32 {
                        return Err(ValidationError::PointOutOfBounds {
                            connection: conn.id.clone(),
                            device: device.id.clone(),
                            start: block.start,
                            point: point.name.clone(),
                            offset: point.offset,
                            width,
                            count: block.count,
                        });
                    }
                    spans.push((point.offset, point.offset + width, point.name.as_str()));
                }

                spans.sort_by_key(|s| s.0);
                for pair in spans.windows(2) {
                    let (_, a_end, a_name) = pair[0];
                    let (b_start, _, b_name) = pair[1];
                    if b_start < a_end {
                        return Err(ValidationError::OverlappingPoints {
                            connection: conn.id.clone(),
                            device: device.id.clone(),
                            start: block.start,
                            a: a_name.to_string(),
                            b: b_name.to_string(),
                        });
                    }
                }
            }
        }
    }

    validate_ca_cert(&config.mqtt.ca_cert_path)?;

    Ok(())
}

fn validate_ca_cert(path: &str) -> Result<(), ValidationError> {
    let bytes = std::fs::read(path).map_err(|e| ValidationError::CaCertUnreadable {
        path: path.to_string(),
        source: e,
    })?;
    let certs = CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ValidationError::CaCertUnparseable {
            path: path.to_string(),
            source: e,
        })?;
    if certs.is_empty() {
        return Err(ValidationError::CaCertEmpty {
            path: path.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::schema::{
        BlockConfig, Config, ConnectionConfig, DataType, DeviceConfig, Function, GatewayConfig,
        MqttConfig, Parity, PointConfig, WordOrder,
    };
    use super::*;

    fn point(name: &str, offset: u16, data_type: DataType) -> PointConfig {
        PointConfig {
            name: name.to_string(),
            offset,
            data_type,
            scale: 1.0,
            absolute: false,
            unit: "V".to_string(),
        }
    }

    fn block(start: u16, count: u16, points: Vec<PointConfig>) -> BlockConfig {
        BlockConfig {
            function: Function::Holding,
            start,
            count,
            word_order: WordOrder::BigEndian,
            points,
        }
    }

    fn device(id: &str, unit_id: u8, blocks: Vec<BlockConfig>) -> DeviceConfig {
        DeviceConfig {
            id: id.to_string(),
            unit_id,
            blocks,
        }
    }

    fn valid_config() -> Config {
        Config {
            gateway: GatewayConfig {
                id: "gw-test".to_string(),
                aggregation_window_secs: 60,
                base_name_prefix: "urn:dev:gw-test:".to_string(),
                log_level: "info".to_string(),
                status_file_path: "/tmp/status".to_string(),
                status_debounce_secs: 2,
                status_heartbeat_secs: 30,
            },
            mqtt: MqttConfig {
                broker_host: "10.0.0.5".to_string(),
                broker_port: 8883,
                client_id: "gw".to_string(),
                tls: false,
                ca_cert_path: valid_ca_cert_path(),
                username: "gateway".to_string(),
                password_env: "MQTT_PASSWORD".to_string(),
                qos: 1,
                keep_alive_secs: 30,
                topic_template: "telemetry/{device}".to_string(),
            },
            connections: vec![ConnectionConfig {
                id: "conn1".to_string(),
                transport: Transport::Tcp {
                    host: "192.168.1.50".to_string(),
                    port: 502,
                },
                poll_interval_secs: 10,
                io_timeout_ms: 1000,
                reconnect_backoff_min_secs: 1,
                reconnect_backoff_max_secs: 30,
                devices: vec![device(
                    "meter1",
                    1,
                    vec![block(
                        3000,
                        8,
                        vec![
                            point("voltage_l1", 0, DataType::F32),
                            point("current_l1", 2, DataType::F32),
                        ],
                    )],
                )],
            }],
        }
    }

    fn valid_ca_cert_path() -> String {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/config/testdata/valid_ca.pem"
        )
        .to_string()
    }

    #[test]
    fn valid_config_passes() {
        validate(&valid_config()).expect("fixture config must be valid");
    }

    /// Applies `set(config, value)` for each value and asserts whether
    /// validation accepts it.
    fn assert_range(name: &str, set: fn(&mut Config, u64), ok: &[u64], bad: &[u64]) {
        for &value in ok {
            let mut config = valid_config();
            set(&mut config, value);
            if let Err(e) = validate(&config) {
                panic!("{name} = {value} must be accepted: {e}");
            }
        }
        for &value in bad {
            let mut config = valid_config();
            set(&mut config, value);
            match validate(&config) {
                Err(ValidationError::OutOfRange { .. }) => {}
                other => panic!("{name} = {value} must be OutOfRange, got {other:?}"),
            }
        }
    }

    #[test]
    fn durations_reject_zero_and_extreme_values() {
        assert_range(
            "aggregation_window_secs",
            |c, v| c.gateway.aggregation_window_secs = v,
            &[1, 86_400],
            &[0, 86_401, u64::MAX],
        );
        assert_range(
            "status_heartbeat_secs",
            |c, v| c.gateway.status_heartbeat_secs = v,
            &[1, 3_600],
            &[0, 3_601, u64::MAX],
        );
        assert_range(
            "status_debounce_secs",
            |c, v| c.gateway.status_debounce_secs = v,
            &[0, 3_600],
            &[3_601, u64::MAX],
        );
        assert_range(
            "keep_alive_secs",
            |c, v| c.mqtt.keep_alive_secs = v,
            &[1, 65_535],
            &[0, 65_536, u64::MAX],
        );
        assert_range(
            "poll_interval_secs",
            |c, v| c.connections[0].poll_interval_secs = v,
            &[1, 86_400],
            &[0, 86_401, u64::MAX],
        );
        assert_range(
            "io_timeout_ms",
            |c, v| c.connections[0].io_timeout_ms = v,
            &[1, 60_000],
            &[0, 60_001, u64::MAX],
        );
        assert_range(
            "reconnect_backoff_min_secs",
            |c, v| c.connections[0].reconnect_backoff_min_secs = v,
            &[1, 30],
            &[0, u64::MAX],
        );
        assert_range(
            "reconnect_backoff_max_secs",
            |c, v| {
                c.connections[0].reconnect_backoff_min_secs = 1;
                c.connections[0].reconnect_backoff_max_secs = v;
            },
            &[1, 3_600],
            &[0, 3_601, u64::MAX],
        );
    }

    #[test]
    fn backoff_min_above_max_is_rejected() {
        let mut config = valid_config();
        config.connections[0].reconnect_backoff_min_secs = 10;
        config.connections[0].reconnect_backoff_max_secs = 5;
        assert!(matches!(
            validate(&config),
            Err(ValidationError::BackoffMinExceedsMax { .. })
        ));
    }

    #[test]
    fn non_finite_scale_is_rejected() {
        for scale in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut config = valid_config();
            config.connections[0].devices[0].blocks[0].points[0].scale = scale;
            assert!(
                matches!(
                    validate(&config),
                    Err(ValidationError::NonFiniteScale { .. })
                ),
                "scale = {scale}"
            );
        }
    }

    #[test]
    fn empty_mqtt_fields_are_rejected() {
        let setters: [fn(&mut Config); 4] = [
            |c| c.mqtt.broker_host = String::new(),
            |c| c.mqtt.client_id = " ".to_string(),
            |c| c.mqtt.username = String::new(),
            |c| c.mqtt.topic_template = String::new(),
        ];
        for set in setters {
            let mut config = valid_config();
            set(&mut config);
            assert!(matches!(validate(&config), Err(ValidationError::Empty(_))));
        }
    }

    #[test]
    fn invalid_qos_is_rejected() {
        let mut config = valid_config();
        config.mqtt.qos = 3;
        assert!(matches!(
            validate(&config),
            Err(ValidationError::InvalidQos(3))
        ));
    }

    #[test]
    fn invalid_log_level_is_rejected() {
        let mut config = valid_config();
        config.gateway.log_level = "verbose".to_string();
        assert!(matches!(
            validate(&config),
            Err(ValidationError::InvalidLogLevel(level)) if level == "verbose"
        ));
    }

    #[test]
    fn duplicate_unit_id_within_connection_is_rejected() {
        let mut config = valid_config();
        config.connections[0]
            .devices
            .push(device("meter2", 1, vec![block(4000, 2, vec![])]));

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateUnitId { .. }));
    }

    #[test]
    fn duplicate_serial_port_across_connections_is_rejected() {
        let mut config = valid_config();
        let rtu = |id: &str| ConnectionConfig {
            id: id.to_string(),
            transport: Transport::Rtu {
                serial_port: "/dev/ttyUSB0".to_string(),
                baud_rate: 9600,
                data_bits: 8,
                parity: Parity::None,
                stop_bits: 1,
                inter_frame_delay_ms: 0,
            },
            poll_interval_secs: 10,
            io_timeout_ms: 500,
            reconnect_backoff_min_secs: 1,
            reconnect_backoff_max_secs: 30,
            devices: vec![device("sensor1", 1, vec![])],
        };
        config.connections = vec![rtu("busA"), rtu("busB")];

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::DuplicateSerialPort { .. }));
    }

    #[test]
    fn unsupported_data_bits_or_stop_bits_are_rejected() {
        let rtu = |data_bits, stop_bits| {
            let mut config = valid_config();
            config.connections[0].transport = Transport::Rtu {
                serial_port: "/dev/ttyUSB0".to_string(),
                baud_rate: 9600,
                data_bits,
                parity: Parity::None,
                stop_bits,
                inter_frame_delay_ms: 0,
            };
            config
        };

        assert!(validate(&rtu(8, 2)).is_ok());
        for (data_bits, stop_bits) in [(9, 1), (4, 1), (8, 0), (8, 3)] {
            let err = validate(&rtu(data_bits, stop_bits)).unwrap_err();
            assert!(
                matches!(err, ValidationError::UnsupportedSerialSetting { .. }),
                "data_bits={data_bits} stop_bits={stop_bits}: {err}"
            );
        }
    }

    #[test]
    fn block_exceeding_125_registers_is_rejected() {
        let mut config = valid_config();
        config.connections[0].devices[0].blocks[0].start = 0;
        config.connections[0].devices[0].blocks[0].count = 126;

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::BlockTooLarge { .. }));
    }

    #[test]
    fn point_offset_beyond_block_count_is_rejected() {
        let mut config = valid_config();
        config.connections[0].devices[0].blocks[0].count = 1;

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::PointOutOfBounds { .. }));
    }

    #[test]
    fn overlapping_points_are_rejected() {
        let mut config = valid_config();
        // voltage_l1 (f32 @ offset 0, width 2) overlaps current_l1 moved to offset 1.
        config.connections[0].devices[0].blocks[0].points[1].offset = 1;

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::OverlappingPoints { .. }));
    }

    #[test]
    fn missing_ca_cert_file_is_rejected() {
        let mut config = valid_config();
        config.mqtt.ca_cert_path = "/nonexistent/ca.pem".to_string();

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::CaCertUnreadable { .. }));
    }

    #[test]
    fn garbage_ca_cert_file_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "modbus-gw-test-garbage-ca-{}.pem",
            std::process::id()
        ));
        std::fs::write(&path, b"not a certificate").unwrap();

        let mut config = valid_config();
        config.mqtt.ca_cert_path = path.display().to_string();

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::CaCertEmpty { .. }));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn malformed_pem_ca_cert_file_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "modbus-gw-test-malformed-ca-{}.pem",
            std::process::id()
        ));
        std::fs::write(
            &path,
            b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let mut config = valid_config();
        config.mqtt.ca_cert_path = path.display().to_string();

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::CaCertUnparseable { .. }));

        std::fs::remove_file(&path).ok();
    }
}
