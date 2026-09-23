//! Shared integration-test helpers.
//!
//! The mock Modbus/TCP server is hand-rolled over MBAP framing rather than
//! built on tokio-modbus's server so tests can script per-unit behaviour
//! (exceptions, silence) and drop the socket on demand. The RTU counterpart,
//! a responder on a PTY master, lives in [`rtu`].

#![allow(dead_code)]

pub mod mqtt;
pub mod rtu;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use modbus_senml_gateway::config::schema::{
    BlockConfig, DataType, DeviceConfig, Function, PointConfig, WordOrder,
};
use modbus_senml_gateway::types::Reading;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub fn block(function: Function, start: u16, point: &str) -> BlockConfig {
    BlockConfig {
        function,
        start,
        count: 1,
        word_order: WordOrder::BigEndian,
        points: vec![PointConfig {
            name: point.to_string(),
            offset: 0,
            data_type: DataType::U16,
            scale: 1.0,
            absolute: false,
            unit: "V".to_string(),
        }],
    }
}

/// Each device reads one holding and one input register.
pub fn device(id: &str, unit_id: u8) -> DeviceConfig {
    DeviceConfig {
        id: id.to_string(),
        unit_id,
        blocks: vec![
            block(Function::Holding, 10, "holding"),
            block(Function::Input, 20, "input"),
        ],
    }
}

pub async fn collect_for(rx: &mut mpsc::Receiver<Reading>, duration: Duration) -> Vec<Reading> {
    let mut readings = Vec::new();
    let deadline = tokio::time::Instant::now() + duration;
    while let Ok(Some(r)) = tokio::time::timeout_at(deadline, rx.recv()).await {
        readings.push(r);
    }
    readings
}

/// `(device, point) -> value` from the given readings; later readings win.
pub fn by_point(readings: &[Reading]) -> HashMap<(String, String), f64> {
    readings
        .iter()
        .map(|r| {
            (
                (r.point_id.device.clone(), r.point_id.point.clone()),
                r.value,
            )
        })
        .collect()
}

pub fn key(device: &str, point: &str) -> (String, String) {
    (device.to_string(), point.to_string())
}

/// How the server answers requests for a given unit id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitBehavior {
    /// Respond with `register_value(unit, function, addr)` for each register.
    Respond,
    /// Respond with this Modbus exception code.
    Exception(u8),
    /// Never respond.
    Silent,
}

/// Server-wide mode, applied to every request regardless of unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMode {
    Normal,
    /// Close the socket as soon as a request arrives (client sees EOF).
    CloseOnRequest,
}

/// Deterministic register contents: distinguishable per unit, function and address.
pub fn register_value(unit: u8, function: u8, addr: u16) -> u16 {
    let base = if function == 0x04 { 500 } else { 0 };
    unit as u16 * 1000 + base + addr
}

#[derive(Debug, Default)]
struct State {
    units: HashMap<u8, UnitBehavior>,
    mode: Option<ServerMode>,
    accepts: Vec<Instant>,
    closes: Vec<Instant>,
}

#[derive(Clone)]
pub struct MockTcpServer {
    pub addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl MockTcpServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));

        let accept_state = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                accept_state.lock().unwrap().accepts.push(Instant::now());
                tokio::spawn(serve_connection(stream, accept_state.clone()));
            }
        });

        MockTcpServer { addr, state }
    }

    pub fn set_unit(&self, unit: u8, behavior: UnitBehavior) {
        self.state.lock().unwrap().units.insert(unit, behavior);
    }

    pub fn set_mode(&self, mode: ServerMode) {
        self.state.lock().unwrap().mode = Some(mode);
    }

    pub fn accepts(&self) -> Vec<Instant> {
        self.state.lock().unwrap().accepts.clone()
    }

    pub fn closes(&self) -> Vec<Instant> {
        self.state.lock().unwrap().closes.clone()
    }
}

async fn serve_connection(mut stream: TcpStream, state: Arc<Mutex<State>>) {
    loop {
        let mut header = [0u8; 7];
        if stream.read_exact(&mut header).await.is_err() {
            return;
        }
        let len = u16::from_be_bytes([header[4], header[5]]) as usize;
        let unit = header[6];
        let mut pdu = vec![0u8; len.saturating_sub(1)];
        if stream.read_exact(&mut pdu).await.is_err() {
            return;
        }

        let (mode, behavior) = {
            let state = state.lock().unwrap();
            (
                state.mode.unwrap_or(ServerMode::Normal),
                state
                    .units
                    .get(&unit)
                    .copied()
                    .unwrap_or(UnitBehavior::Respond),
            )
        };

        if mode == ServerMode::CloseOnRequest {
            state.lock().unwrap().closes.push(Instant::now());
            return;
        }

        let function = pdu[0];
        let response_pdu = match behavior {
            UnitBehavior::Silent => continue,
            UnitBehavior::Exception(code) => vec![function | 0x80, code],
            UnitBehavior::Respond => {
                let start = u16::from_be_bytes([pdu[1], pdu[2]]);
                let count = u16::from_be_bytes([pdu[3], pdu[4]]);
                let mut out = vec![function, (count * 2) as u8];
                for addr in start..start + count {
                    out.extend_from_slice(&register_value(unit, function, addr).to_be_bytes());
                }
                out
            }
        };

        let mut frame = Vec::with_capacity(7 + response_pdu.len());
        frame.extend_from_slice(&header[0..4]); // transaction id + protocol id
        frame.extend_from_slice(&((response_pdu.len() + 1) as u16).to_be_bytes());
        frame.push(unit);
        frame.extend_from_slice(&response_pdu);
        if stream.write_all(&frame).await.is_err() {
            return;
        }
    }
}
