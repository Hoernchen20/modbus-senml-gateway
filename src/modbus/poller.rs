use std::io;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{self, MissedTickBehavior};
use tokio_modbus::client::{self, Context, Reader};
use tokio_modbus::prelude::SlaveContext;
use tokio_modbus::{ExceptionCode, Slave};
use tracing::{info, warn};

use crate::config::schema::{BlockConfig, ConnectionConfig, Function, Parity, Transport};
use crate::modbus::decode::decode;
use crate::status::{StatusEvent, StatusReporter};
use crate::types::{PointId, Reading};

/// Exponential reconnect backoff, doubling from `min` up to `max`.
#[derive(Debug)]
pub struct Backoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    pub fn new(min: Duration, max: Duration) -> Self {
        Backoff {
            min,
            max,
            current: min,
        }
    }

    /// Sleeps for the current delay, then doubles it (capped at `max`).
    pub async fn wait(&mut self) {
        time::sleep(self.current).await;
        self.current = (self.current * 2).min(self.max);
    }

    pub fn reset(&mut self) {
        self.current = self.min;
    }

    pub fn current(&self) -> Duration {
        self.current
    }
}

/// §6.1 / §13: only a dead transport (socket reset, EOF, serial device gone)
/// warrants tearing down the connection. `Protocol` errors — header or
/// function-code mismatches, e.g. a late reply to a request that already
/// timed out — are device-level and just skip that device for the tick.
/// Modbus exception responses never reach here: tokio-modbus returns them as
/// the inner `Err(ExceptionCode)` of its nested result.
pub fn is_transport_fatal(e: &tokio_modbus::Error) -> bool {
    matches!(e, tokio_modbus::Error::Transport(_))
}

/// Outcome of reading one block, collapsed to what the poll loop acts on.
enum BlockResult {
    Words(Vec<u16>),
    Exception(ExceptionCode),
    Protocol(tokio_modbus::Error),
    Timeout,
    TransportFatal(tokio_modbus::Error),
}

async fn connect(transport: &Transport, timeout: Duration) -> io::Result<Context> {
    match transport {
        Transport::Tcp { host, port } => {
            let connect = async {
                // lookup_host rather than `.parse()` so hostnames work, not just IPs.
                let addr = tokio::net::lookup_host((host.as_str(), *port))
                    .await?
                    .next()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, format!("no address for {host}"))
                    })?;
                client::tcp::connect(addr).await
            };
            time::timeout(timeout, connect)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))?
        }
        Transport::Rtu {
            serial_port,
            baud_rate,
            data_bits,
            parity,
            stop_bits,
            ..
        } => {
            let builder = tokio_serial::new(serial_port, *baud_rate)
                .data_bits(serial_data_bits(*data_bits)?)
                .parity(serial_parity(*parity))
                .stop_bits(serial_stop_bits(*stop_bits)?);
            // Opened exclusively (serialport's default): a second opener of the
            // same bus would interleave frames with ours.
            let stream = tokio_serial::SerialStream::open(&builder)?;
            Ok(client::rtu::attach(stream))
        }
    }
}

// Config validation already restricts these to supported values; the errors
// are only a fallback so an unvalidated config can't panic the poller.
fn serial_data_bits(bits: u8) -> io::Result<tokio_serial::DataBits> {
    match bits {
        5 => Ok(tokio_serial::DataBits::Five),
        6 => Ok(tokio_serial::DataBits::Six),
        7 => Ok(tokio_serial::DataBits::Seven),
        8 => Ok(tokio_serial::DataBits::Eight),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported data_bits {bits}"),
        )),
    }
}

fn serial_stop_bits(bits: u8) -> io::Result<tokio_serial::StopBits> {
    match bits {
        1 => Ok(tokio_serial::StopBits::One),
        2 => Ok(tokio_serial::StopBits::Two),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported stop_bits {bits}"),
        )),
    }
}

fn serial_parity(parity: Parity) -> tokio_serial::Parity {
    match parity {
        Parity::None => tokio_serial::Parity::None,
        Parity::Even => tokio_serial::Parity::Even,
        Parity::Odd => tokio_serial::Parity::Odd,
    }
}

async fn read_block(ctx: &mut Context, block: &BlockConfig, timeout: Duration) -> BlockResult {
    let request = async {
        match block.function {
            Function::Holding => ctx.read_holding_registers(block.start, block.count).await,
            Function::Input => ctx.read_input_registers(block.start, block.count).await,
        }
    };
    match time::timeout(timeout, request).await {
        Ok(Ok(Ok(words))) => BlockResult::Words(words),
        Ok(Ok(Err(code))) => BlockResult::Exception(code),
        Ok(Err(e)) if is_transport_fatal(&e) => BlockResult::TransportFatal(e),
        Ok(Err(e)) => BlockResult::Protocol(e),
        Err(_) => BlockResult::Timeout,
    }
}

fn inter_frame_delay(transport: &Transport) -> Option<Duration> {
    match transport {
        Transport::Rtu {
            inter_frame_delay_ms,
            ..
        } if *inter_frame_delay_ms > 0 => Some(Duration::from_millis(*inter_frame_delay_ms)),
        _ => None,
    }
}

/// One task per `[[connection]]` (§6). Returns only once the aggregator side
/// of `tx` is closed; otherwise reconnects forever. Connection state and
/// every device's result on every tick go to `status` (§11.2).
pub async fn run_connection(
    cfg: ConnectionConfig,
    tx: mpsc::Sender<Reading>,
    status: StatusReporter,
) {
    let io_timeout = Duration::from_millis(cfg.io_timeout_ms);
    let poll_interval = Duration::from_secs(cfg.poll_interval_secs);
    let frame_delay = inter_frame_delay(&cfg.transport);
    let mut backoff = Backoff::new(
        Duration::from_secs(cfg.reconnect_backoff_min_secs),
        Duration::from_secs(cfg.reconnect_backoff_max_secs),
    );

    loop {
        let mut ctx = match connect(&cfg.transport, io_timeout).await {
            Ok(ctx) => {
                info!(connection = %cfg.id, "connected");
                status.report(StatusEvent::ConnectionUp {
                    connection_id: cfg.id.clone(),
                });
                ctx
            }
            Err(e) => {
                warn!(error = %e, connection = %cfg.id, retry_in = ?backoff.current(), "connect failed");
                status.report(StatusEvent::ConnectionDown {
                    connection_id: cfg.id.clone(),
                });
                backoff.wait().await;
                continue;
            }
        };

        let mut ticker = time::interval(poll_interval);
        // A tick that overran (e.g. several devices timing out) shouldn't be
        // followed by a burst of catch-up ticks.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        'poll: loop {
            ticker.tick().await;
            for device in &cfg.devices {
                ctx.set_slave(Slave(device.unit_id));
                let mut problem = None;
                for block in &device.blocks {
                    let result = read_block(&mut ctx, block, io_timeout).await;
                    if let Some(delay) = frame_delay {
                        time::sleep(delay).await;
                    }
                    match result {
                        BlockResult::Words(words) => {
                            // Reset only on an actual successful read, not on
                            // connect: a peer that accepts and immediately
                            // drops would otherwise reconnect at min backoff forever.
                            backoff.reset();
                            for (point, value) in decode(block, &words) {
                                let reading = Reading {
                                    point_id: PointId {
                                        device: device.id.clone(),
                                        point,
                                    },
                                    value,
                                };
                                match tx.try_send(reading) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        warn!(connection = %cfg.id, device = %device.id, "aggregator channel full, dropping reading");
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                                }
                            }
                        }
                        BlockResult::Exception(code) => {
                            warn!(?code, connection = %cfg.id, device = %device.id, "modbus exception, skipping device this tick");
                            problem = Some(format!("exception: {code}"));
                            break;
                        }
                        BlockResult::Protocol(e) => {
                            warn!(error = %e, connection = %cfg.id, device = %device.id, "protocol error, skipping device this tick");
                            problem = Some("protocol error".to_string());
                            break;
                        }
                        BlockResult::Timeout => {
                            warn!(connection = %cfg.id, device = %device.id, "io timeout, skipping device this tick");
                            problem = Some("timeout".to_string());
                            break;
                        }
                        BlockResult::TransportFatal(e) => {
                            warn!(error = %e, connection = %cfg.id, retry_in = ?backoff.current(), "transport error, reconnecting");
                            status.report(StatusEvent::ConnectionDown {
                                connection_id: cfg.id.clone(),
                            });
                            break 'poll;
                        }
                    }
                }
                // Reported every tick, not just on change, so an event dropped
                // on a full status channel is corrected by the next one.
                let connection_id = cfg.id.clone();
                let device_id = device.id.clone();
                status.report(match problem {
                    None => StatusEvent::DeviceOk {
                        connection_id,
                        device_id,
                    },
                    Some(reason) => StatusEvent::DeviceProblem {
                        connection_id,
                        device_id,
                        reason,
                    },
                });
            }
        }

        // Back off before reconnecting too, not just after a failed connect.
        backoff.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn backoff_doubles_up_to_max_and_resets() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(5));
        let mut waits = Vec::new();
        for _ in 0..5 {
            let before = time::Instant::now();
            backoff.wait().await;
            waits.push((time::Instant::now() - before).as_secs());
        }
        assert_eq!(waits, [1, 2, 4, 5, 5]);

        backoff.reset();
        assert_eq!(backoff.current(), Duration::from_secs(1));
    }

    #[test]
    fn transport_errors_are_fatal_protocol_errors_are_not() {
        let transport = tokio_modbus::Error::Transport(io::Error::from(io::ErrorKind::BrokenPipe));
        assert!(is_transport_fatal(&transport));

        let protocol = tokio_modbus::Error::Protocol(tokio_modbus::ProtocolError::HeaderMismatch {
            message: "transaction id".to_string(),
            result: Err(tokio_modbus::ExceptionResponse {
                function: tokio_modbus::FunctionCode::ReadHoldingRegisters,
                exception: ExceptionCode::ServerDeviceBusy,
            }),
        });
        assert!(!is_transport_fatal(&protocol));
    }

    #[test]
    fn inter_frame_delay_only_for_rtu_with_nonzero_delay() {
        let tcp = Transport::Tcp {
            host: "localhost".to_string(),
            port: 502,
        };
        assert_eq!(inter_frame_delay(&tcp), None);

        let rtu = |ms| Transport::Rtu {
            serial_port: "/dev/ttyUSB0".to_string(),
            baud_rate: 9600,
            data_bits: 8,
            parity: crate::config::schema::Parity::None,
            stop_bits: 1,
            inter_frame_delay_ms: ms,
        };
        assert_eq!(inter_frame_delay(&rtu(0)), None);
        assert_eq!(inter_frame_delay(&rtu(5)), Some(Duration::from_millis(5)));
    }
}
