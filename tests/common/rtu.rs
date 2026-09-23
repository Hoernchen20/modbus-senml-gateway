//! Mock Modbus/RTU bus on a pseudo-terminal.
//!
//! The poller opens the PTY slave as if it were a real serial adapter; the
//! responder task owns the master end and answers as several slave units
//! sharing the bus. The poller is pointed at a symlink rather than the
//! `/dev/pts/N` node itself, standing in for a udev by-id path: `unplug()`
//! closes the master and removes the link, `replug()` opens a fresh PTY
//! behind the same link.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nix::fcntl::OFlag;
use nix::pty::{grantpt, posix_openpt, ptsname_r, unlockpt, PtyMaster};
use tokio::io::unix::AsyncFd;
use tokio::task::JoinHandle;

use super::{register_value, UnitBehavior};

/// Length of a read-holding/read-input request frame: unit, function,
/// start (2), count (2), CRC (2).
const REQUEST_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BusEvent {
    /// A complete request frame arrived from the poller.
    Request { unit: u8, function: u8, at: Instant },
    /// The responder finished writing a reply.
    Response { unit: u8, at: Instant },
}

#[derive(Debug, Default)]
struct State {
    units: HashMap<u8, UnitBehavior>,
    response_delay: Duration,
    events: Vec<BusEvent>,
    /// Requests that arrived while a reply to an earlier one was still pending.
    overlaps: usize,
}

pub struct MockRtuBus {
    /// Path to hand to the poller as `serial_port`.
    pub path: PathBuf,
    state: Arc<Mutex<State>>,
    responder: Option<JoinHandle<()>>,
}

impl MockRtuBus {
    pub fn start() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "modbus-rtu-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut bus = MockRtuBus {
            path,
            state: Arc::new(Mutex::new(State::default())),
            responder: None,
        };
        bus.replug();
        bus
    }

    pub fn set_unit(&self, unit: u8, behavior: UnitBehavior) {
        self.state.lock().unwrap().units.insert(unit, behavior);
    }

    /// How long each responding unit takes to answer. Nonzero makes overlap
    /// detection meaningful: a poller that didn't wait would get caught.
    pub fn set_response_delay(&self, delay: Duration) {
        self.state.lock().unwrap().response_delay = delay;
    }

    pub fn events(&self) -> Vec<BusEvent> {
        self.state.lock().unwrap().events.clone()
    }

    pub fn overlaps(&self) -> usize {
        self.state.lock().unwrap().overlaps
    }

    /// Closes the PTY master (the poller's side sees EOF / EIO) and removes
    /// the device link, like pulling a USB adapter.
    pub async fn unplug(&mut self) {
        if let Some(responder) = self.responder.take() {
            responder.abort();
            let _ = responder.await;
        }
        let _ = std::fs::remove_file(&self.path);
    }

    /// Opens a fresh PTY and points the device link at its slave end.
    pub fn replug(&mut self) {
        assert!(self.responder.is_none(), "replug while still plugged in");
        let master = posix_openpt(OFlag::O_RDWR | OFlag::O_NOCTTY | OFlag::O_NONBLOCK)
            .expect("posix_openpt");
        grantpt(&master).expect("grantpt");
        unlockpt(&master).expect("unlockpt");
        let slave = ptsname_r(&master).expect("ptsname_r");

        let _ = std::fs::remove_file(&self.path);
        std::os::unix::fs::symlink(slave, &self.path).expect("symlink PTY slave");

        let master = AsyncFd::new(master).expect("register PTY master");
        self.responder = Some(tokio::spawn(serve(master, self.state.clone())));
    }
}

impl Drop for MockRtuBus {
    fn drop(&mut self) {
        if let Some(responder) = self.responder.take() {
            responder.abort();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

struct PendingReply {
    unit: u8,
    frame: Vec<u8>,
    due: tokio::time::Instant,
}

async fn serve(master: AsyncFd<PtyMaster>, state: Arc<Mutex<State>>) {
    let mut buf = Vec::new();
    let mut pending: Option<PendingReply> = None;

    loop {
        let due = pending.as_ref().map(|p| p.due);
        tokio::select! {
            ready = master.readable() => {
                let mut guard = ready.expect("poll PTY master");
                let mut chunk = [0u8; 256];
                match guard.try_io(|fd| fd.get_ref().read(&mut chunk)) {
                    Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                    // EIO (no slave side open, e.g. between the poller's
                    // close and its reconnect) or EOF: nothing to read, and
                    // the fd stays readable, so back off instead of spinning.
                    Ok(_) => {
                        guard.clear_ready();
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(_would_block) => {}
                }
            }
            _ = tokio::time::sleep_until(due.unwrap_or_else(tokio::time::Instant::now)), if due.is_some() => {
                let reply = pending.take().unwrap();
                let _ = write_all(&master, &reply.frame).await;
                state.lock().unwrap().events.push(BusEvent::Response {
                    unit: reply.unit,
                    at: Instant::now(),
                });
            }
        }

        while buf.len() >= REQUEST_LEN {
            let request: Vec<u8> = buf.drain(..REQUEST_LEN).collect();
            let mut state = state.lock().unwrap();
            if pending.is_some() {
                state.overlaps += 1;
            }
            let (unit, function) = (request[0], request[1]);
            state.events.push(BusEvent::Request {
                unit,
                function,
                at: Instant::now(),
            });
            assert_eq!(
                crc16(&request[..REQUEST_LEN - 2]).to_le_bytes(),
                request[REQUEST_LEN - 2..],
                "request CRC mismatch: {request:02x?}"
            );

            let behavior = state
                .units
                .get(&unit)
                .copied()
                .unwrap_or(UnitBehavior::Respond);
            let pdu = match behavior {
                UnitBehavior::Silent => continue,
                UnitBehavior::Exception(code) => vec![function | 0x80, code],
                UnitBehavior::Respond => {
                    let start = u16::from_be_bytes([request[2], request[3]]);
                    let count = u16::from_be_bytes([request[4], request[5]]);
                    let mut out = vec![function, (count * 2) as u8];
                    for addr in start..start + count {
                        out.extend_from_slice(&register_value(unit, function, addr).to_be_bytes());
                    }
                    out
                }
            };
            let mut frame = vec![unit];
            frame.extend_from_slice(&pdu);
            frame.extend_from_slice(&crc16(&frame).to_le_bytes());
            pending = Some(PendingReply {
                unit,
                frame,
                due: tokio::time::Instant::now() + state.response_delay,
            });
        }
    }
}

async fn write_all(master: &AsyncFd<PtyMaster>, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        let mut guard = master.writable().await?;
        match guard.try_io(|fd| fd.get_ref().write(data)) {
            Ok(Ok(n)) => data = &data[n..],
            Ok(Err(e)) => return Err(e),
            Err(_would_block) => {}
        }
    }
    Ok(())
}

/// Modbus RTU CRC-16 (poly 0xA001 reflected, init 0xFFFF), sent low byte first.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for &byte in data {
        crc ^= byte as u16;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xA001
            } else {
                crc >> 1
            };
        }
    }
    crc
}
