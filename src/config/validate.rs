use std::collections::{HashMap, HashSet};

use thiserror::Error;

use super::schema::{Config, Transport};

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("{field} must be greater than 0")]
    ZeroInterval { field: String },

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
        source: std::io::Error,
    },
}

pub fn validate(config: &Config) -> Result<(), ValidationError> {
    // A zero here would panic `tokio::time::interval` (poller) or divide by
    // zero in the aggregator's boundary alignment.
    if config.gateway.aggregation_window_secs == 0 {
        return Err(ValidationError::ZeroInterval {
            field: "gateway.aggregation_window_secs".to_string(),
        });
    }

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
        if conn.poll_interval_secs == 0 {
            return Err(ValidationError::ZeroInterval {
                field: format!("connection '{}': poll_interval_secs", conn.id),
            });
        }

        let mut seen_unit_ids = HashSet::new();
        for device in &conn.devices {
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
    let mut reader = std::io::BufReader::new(bytes.as_slice());
    let certs = rustls_pemfile::certs(&mut reader)
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

    #[test]
    fn zero_aggregation_window_is_rejected() {
        let mut config = valid_config();
        config.gateway.aggregation_window_secs = 0;

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::ZeroInterval { .. }));
    }

    #[test]
    fn zero_poll_interval_is_rejected() {
        let mut config = valid_config();
        config.connections[0].poll_interval_secs = 0;

        let err = validate(&config).unwrap_err();
        assert!(matches!(err, ValidationError::ZeroInterval { .. }));
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
}
