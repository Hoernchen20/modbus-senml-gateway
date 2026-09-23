use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub gateway: GatewayConfig,
    pub mqtt: MqttConfig,
    #[serde(rename = "connection")]
    pub connections: Vec<ConnectionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    pub id: String,
    pub aggregation_window_secs: u64,
    pub base_name_prefix: String,
    pub log_level: String,
    #[serde(default = "default_status_file_path")]
    pub status_file_path: String,
    #[serde(default = "default_status_debounce_secs")]
    pub status_debounce_secs: u64,
    #[serde(default = "default_status_heartbeat_secs")]
    pub status_heartbeat_secs: u64,
}

fn default_status_file_path() -> String {
    "/dev/container_config/status".to_string()
}

fn default_status_debounce_secs() -> u64 {
    2
}

fn default_status_heartbeat_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
pub struct MqttConfig {
    pub broker_host: String,
    pub broker_port: u16,
    pub client_id: String,
    pub tls: bool,
    pub ca_cert_path: String,
    pub username: String,
    pub password_env: String,
    #[serde(default = "default_qos")]
    pub qos: u8,
    pub keep_alive_secs: u64,
    pub topic_template: String,
}

fn default_qos() -> u8 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionConfig {
    pub id: String,
    #[serde(flatten)]
    pub transport: Transport,
    pub poll_interval_secs: u64,
    pub io_timeout_ms: u64,
    #[serde(default = "default_backoff_min_secs")]
    pub reconnect_backoff_min_secs: u64,
    #[serde(default = "default_backoff_max_secs")]
    pub reconnect_backoff_max_secs: u64,
    #[serde(rename = "device")]
    pub devices: Vec<DeviceConfig>,
}

fn default_backoff_min_secs() -> u64 {
    1
}

fn default_backoff_max_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase")]
pub enum Transport {
    Tcp {
        host: String,
        port: u16,
    },
    Rtu {
        serial_port: String,
        baud_rate: u32,
        #[serde(default = "default_data_bits")]
        data_bits: u8,
        #[serde(default)]
        parity: Parity,
        #[serde(default = "default_stop_bits")]
        stop_bits: u8,
        #[serde(default)]
        inter_frame_delay_ms: u64,
    },
}

fn default_data_bits() -> u8 {
    8
}

fn default_stop_bits() -> u8 {
    1
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Parity {
    #[default]
    None,
    Even,
    Odd,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeviceConfig {
    pub id: String,
    pub unit_id: u8,
    #[serde(rename = "block")]
    pub blocks: Vec<BlockConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockConfig {
    pub function: Function,
    pub start: u16,
    pub count: u16,
    #[serde(default)]
    pub word_order: WordOrder,
    #[serde(rename = "point")]
    pub points: Vec<PointConfig>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Function {
    Holding,
    Input,
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WordOrder {
    #[default]
    BigEndian,
    LittleEndian,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PointConfig {
    pub name: String,
    pub offset: u16,
    pub data_type: DataType,
    #[serde(default = "default_scale")]
    pub scale: f64,
    #[serde(default)]
    pub absolute: bool,
    pub unit: String,
}

fn default_scale() -> f64 {
    1.0
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    U16,
    I16,
    U32,
    I32,
    F32,
}

impl DataType {
    /// Number of 16-bit registers this type occupies.
    pub fn word_count(self) -> u16 {
        match self {
            DataType::U16 | DataType::I16 => 1,
            DataType::U32 | DataType::I32 | DataType::F32 => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_EXAMPLE: &str = r#"
[gateway]
id = "gw-router1"
aggregation_window_secs = 60
base_name_prefix = "urn:dev:gw-router1:"
log_level = "info"

[mqtt]
broker_host = "10.0.0.5"
broker_port = 8883
client_id = "modbus-gateway-router1"
tls = true
ca_cert_path = "/etc/modbus-gateway/ca.pem"
username = "gateway"
password_env = "MQTT_PASSWORD"
qos = 1
keep_alive_secs = 30
topic_template = "telemetry/{device}"

[[connection]]
id = "meter-tcp1"
transport = "tcp"
host = "192.168.1.50"
port = 502
poll_interval_secs = 10
io_timeout_ms = 1000

  [[connection.device]]
  id = "meter1"
  unit_id = 1

    [[connection.device.block]]
    function = "holding"
    start = 3000
    count = 8
    word_order = "big_endian"

      [[connection.device.block.point]]
      name = "voltage_l1"
      offset = 0
      data_type = "f32"
      unit = "V"

      [[connection.device.block.point]]
      name = "current_l1"
      offset = 2
      data_type = "f32"
      unit = "A"

      [[connection.device.block.point]]
      name = "active_power"
      offset = 4
      data_type = "i32"
      scale = 0.1
      absolute = true
      unit = "W"

    [[connection.device.block]]
    function = "holding"
    start = 3100
    count = 2

      [[connection.device.block.point]]
      name = "energy_total"
      offset = 0
      data_type = "u32"
      scale = 0.001
      unit = "kWh"

[[connection]]
id = "rs485-bus1"
transport = "rtu"
serial_port = "/dev/serial/by-id/usb-FTDI_USB-RS485-if00-port0"
baud_rate = 9600
parity = "none"
poll_interval_secs = 15
io_timeout_ms = 500
inter_frame_delay_ms = 10

  [[connection.device]]
  id = "tempsensor1"
  unit_id = 5

    [[connection.device.block]]
    function = "input"
    start = 100
    count = 2

      [[connection.device.block.point]]
      name = "temperature"
      offset = 0
      data_type = "i16"
      scale = 0.1
      unit = "Cel"

      [[connection.device.block.point]]
      name = "humidity"
      offset = 1
      data_type = "u16"
      scale = 0.1
      unit = "%RH"

  [[connection.device]]
  id = "tempsensor2"
  unit_id = 6

    [[connection.device.block]]
    function = "input"
    start = 100
    count = 2

      [[connection.device.block.point]]
      name = "temperature"
      offset = 0
      data_type = "i16"
      scale = 0.1
      unit = "Cel"

      [[connection.device.block.point]]
      name = "humidity"
      offset = 1
      data_type = "u16"
      scale = 0.1
      unit = "%RH"
"#;

    #[test]
    fn full_example_deserializes() {
        let config: Config = toml::from_str(FULL_EXAMPLE).expect("valid example must parse");

        assert_eq!(config.gateway.id, "gw-router1");
        assert_eq!(config.mqtt.qos, 1);
        assert_eq!(config.connections.len(), 2);

        let tcp = &config.connections[0];
        match &tcp.transport {
            Transport::Tcp { host, port } => {
                assert_eq!(host, "192.168.1.50");
                assert_eq!(*port, 502);
            }
            Transport::Rtu { .. } => panic!("expected tcp transport"),
        }
        assert!(tcp.devices[0].blocks[0].points[2].absolute);
        assert_eq!(tcp.devices[0].blocks[1].word_order, WordOrder::BigEndian);

        let rtu = &config.connections[1];
        match &rtu.transport {
            Transport::Rtu {
                baud_rate, parity, ..
            } => {
                assert_eq!(*baud_rate, 9600);
                assert_eq!(*parity, Parity::None);
            }
            Transport::Tcp { .. } => panic!("expected rtu transport"),
        }
        assert_eq!(rtu.devices.len(), 2);
        assert_eq!(rtu.devices[1].unit_id, 6);
    }

    #[test]
    fn tcp_with_serial_fields_fails_to_deserialize() {
        let toml_str = r#"
transport = "tcp"
serial_port = "/dev/ttyUSB0"
baud_rate = 9600
"#;
        let result: Result<Transport, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "a tcp transport with rtu-only fields (and missing host/port) must fail to deserialize"
        );
    }

    #[test]
    fn rtu_missing_baud_rate_fails_to_deserialize() {
        let toml_str = r#"
transport = "rtu"
serial_port = "/dev/ttyUSB0"
"#;
        let result: Result<Transport, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "an rtu transport missing required baud_rate must fail to deserialize"
        );
    }

    #[test]
    fn tcp_missing_port_fails_to_deserialize() {
        let toml_str = r#"
transport = "tcp"
host = "10.0.0.1"
"#;
        let result: Result<Transport, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "a tcp transport missing port must fail");
    }
}
