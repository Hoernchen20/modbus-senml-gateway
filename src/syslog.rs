use std::fmt::{self, Write as _};
use std::io::{self, Write};
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use time::format_description::FormatItem;
use time::macros::format_description;
use time::OffsetDateTime;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

pub const DEFAULT_SOCKET_PATH: &str = "/dev/log";

/// RFC 3164 facility `daemon` (§11.1).
const FACILITY_DAEMON: u8 = 3;

const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// `Mmm dd hh:mm:ss`, day space-padded as RFC 3164 requires.
const TIMESTAMP: &[FormatItem<'static>] =
    format_description!("[month repr:short] [day padding:space] [hour]:[minute]:[second]");

/// §11.1 severity mapping.
pub fn severity(level: &Level) -> u8 {
    match *level {
        Level::ERROR => 3, // err
        Level::WARN => 4,  // warning
        Level::INFO => 6,  // info
        _ => 7,            // debug (DEBUG, TRACE)
    }
}

/// Frames one message as `<PRI>Mmm dd hh:mm:ss tag[pid]: msg`. The hostname
/// is omitted, as glibc's `syslog()` does for the local socket: the local
/// syslogd adds its own.
pub fn format_rfc3164(
    level: &Level,
    timestamp: OffsetDateTime,
    tag: &str,
    pid: u32,
    msg: &str,
) -> String {
    let pri = FACILITY_DAEMON * 8 + severity(level);
    let ts = timestamp
        .format(TIMESTAMP)
        .expect("timestamp format has no fallible components");
    format!("<{pri}>{ts} {tag}[{pid}]: {msg}")
}

/// `tracing` layer that sends every event to syslog over a Unix datagram
/// socket (§11.1). While the socket can't be opened (syslogd not up yet),
/// events go to the fallback writer (stderr) instead and the connect is
/// retried with backoff on later events; nothing is buffered.
pub struct SyslogLayer<W = fn() -> io::Stderr> {
    tag: String,
    pid: u32,
    conn: Mutex<Connection>,
    fallback: W,
}

struct Connection {
    path: PathBuf,
    socket: Option<UnixDatagram>,
    min: Duration,
    max: Duration,
    backoff: Duration,
    next_attempt: Instant,
}

impl Connection {
    fn new(path: PathBuf, min: Duration, max: Duration) -> Self {
        let mut conn = Connection {
            path,
            socket: None,
            min,
            max,
            backoff: min,
            next_attempt: Instant::now(),
        };
        conn.try_connect(Instant::now());
        conn
    }

    /// Returns the socket, first retrying the connect if the backoff has
    /// elapsed.
    fn socket(&mut self, now: Instant) -> Option<&UnixDatagram> {
        if self.socket.is_none() && now >= self.next_attempt {
            self.try_connect(now);
        }
        self.socket.as_ref()
    }

    fn try_connect(&mut self, now: Instant) {
        match open(&self.path) {
            Ok(socket) => {
                self.socket = Some(socket);
                self.backoff = self.min;
            }
            Err(_) => {
                self.next_attempt = now + self.backoff;
                self.backoff = (self.backoff * 2).min(self.max);
            }
        }
    }

    /// Drops a socket whose peer went away (syslogd restarted), so the next
    /// events go through the reconnect path.
    fn disconnect(&mut self, now: Instant) {
        self.socket = None;
        self.backoff = self.min;
        self.next_attempt = now + self.min;
    }
}

fn open(path: &Path) -> io::Result<UnixDatagram> {
    let socket = UnixDatagram::unbound()?;
    socket.connect(path)?;
    // A stalled syslogd must never block the logging thread (which may be a
    // runtime worker); a full queue sends that message to the fallback.
    socket.set_nonblocking(true)?;
    Ok(socket)
}

impl SyslogLayer {
    /// Logs to `/dev/log` tagged with `tag` (the gateway id), falling back
    /// to stderr.
    pub fn new(tag: impl Into<String>) -> Self {
        SyslogLayer::with_options(tag, DEFAULT_SOCKET_PATH, RETRY_MIN, RETRY_MAX, io::stderr)
    }
}

impl<W> SyslogLayer<W>
where
    W: for<'w> MakeWriter<'w> + 'static,
{
    pub fn with_options(
        tag: impl Into<String>,
        path: impl Into<PathBuf>,
        retry_min: Duration,
        retry_max: Duration,
        fallback: W,
    ) -> Self {
        SyslogLayer {
            tag: tag.into(),
            pid: std::process::id(),
            conn: Mutex::new(Connection::new(path.into(), retry_min, retry_max)),
            fallback,
        }
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        // Logging must keep working even if a panic poisoned the lock.
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn emit(&self, level: &Level, msg: &str) {
        let now = Instant::now();
        {
            let mut conn = self.conn();
            if let Some(socket) = conn.socket(now) {
                let line =
                    format_rfc3164(level, OffsetDateTime::now_utc(), &self.tag, self.pid, msg);
                match socket.send(line.as_bytes()) {
                    Ok(_) => return,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(_) => conn.disconnect(now),
                }
            }
        }
        let _ = writeln!(self.fallback.make_writer(), "{level:>5} {msg}");
    }
}

impl<S, W> Layer<S> for SyslogLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'w> MakeWriter<'w> + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = SpanFields::default();
        attrs.record(&mut FieldVisitor::new(&mut fields.0));
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut ext = span.extensions_mut();
        if let Some(fields) = ext.get_mut::<SpanFields>() {
            values.record(&mut FieldVisitor::new(&mut fields.0));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut msg = String::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                msg.push_str(span.name());
                if let Some(fields) = span.extensions().get::<SpanFields>() {
                    if !fields.0.is_empty() {
                        let _ = write!(msg, "{{{}}}", fields.0);
                    }
                }
                msg.push_str(": ");
            }
        }
        event.record(&mut FieldVisitor::new(&mut msg));
        self.emit(event.metadata().level(), &msg);
    }
}

/// A span's formatted fields, stored in its extensions.
#[derive(Default)]
struct SpanFields(String);

/// Formats `message` verbatim and every other field as ` key=value`.
struct FieldVisitor<'a> {
    out: &'a mut String,
    start: usize,
}

impl<'a> FieldVisitor<'a> {
    fn new(out: &'a mut String) -> Self {
        let start = out.len();
        FieldVisitor { out, start }
    }

    fn separator(&mut self) {
        if self.out.len() > self.start {
            self.out.push(' ');
        }
    }
}

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.separator();
        if field.name() == "message" {
            self.out.push_str(value);
        } else {
            let _ = write!(self.out, "{}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.separator();
        if field.name() == "message" {
            let _ = write!(self.out, "{value:?}");
        } else {
            let _ = write!(self.out, "{}={:?}", field.name(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use time::macros::datetime;
    use tracing_subscriber::layer::SubscriberExt;

    const TS: OffsetDateTime = datetime!(2026-09-03 04:05:06 UTC);

    #[test]
    fn frames_every_level_with_daemon_facility() {
        let cases = [
            (Level::ERROR, "<27>"),
            (Level::WARN, "<28>"),
            (Level::INFO, "<30>"),
            (Level::DEBUG, "<31>"),
            (Level::TRACE, "<31>"),
        ];
        for (level, pri) in cases {
            assert_eq!(
                format_rfc3164(&level, TS, "gw-router1", 42, "hello"),
                format!("{pri}Sep  3 04:05:06 gw-router1[42]: hello"),
                "{level}"
            );
        }
    }

    #[test]
    fn two_digit_days_are_not_padded() {
        let ts = datetime!(2026-12-24 23:59:59 UTC);
        assert_eq!(
            format_rfc3164(&Level::INFO, ts, "gw", 1, "x"),
            "<30>Dec 24 23:59:59 gw[1]: x"
        );
    }

    /// Fallback writer capturing everything written to it.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'w> MakeWriter<'w> for Captured {
        type Writer = Captured;

        fn make_writer(&'w self) -> Self::Writer {
            self.clone()
        }
    }

    fn temp_socket(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("syslog-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("log")
    }

    fn bind(path: &Path) -> UnixDatagram {
        let server = UnixDatagram::bind(path).unwrap();
        server.set_nonblocking(true).unwrap();
        server
    }

    fn recv(server: &UnixDatagram) -> Option<String> {
        let mut buf = [0u8; 4096];
        match server.recv(&mut buf) {
            Ok(n) => Some(String::from_utf8(buf[..n].to_vec()).unwrap()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => None,
            Err(e) => panic!("recv failed: {e}"),
        }
    }

    fn layer(path: &Path, fallback: Captured) -> SyslogLayer<Captured> {
        SyslogLayer::with_options(
            "gw-test",
            path,
            Duration::from_millis(50),
            Duration::from_millis(200),
            fallback,
        )
    }

    #[test]
    fn sends_framed_events_with_fields_and_spans() {
        let path = temp_socket("fields");
        let server = bind(&path);
        let fallback = Captured::default();
        let subscriber = tracing_subscriber::registry().with(layer(&path, fallback.clone()));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("conn", connection = "meter-tcp1");
            let _guard = span.enter();
            tracing::warn!(device = "meter1", retry_in = ?Duration::from_secs(2), "io timeout");
        });

        let msg = recv(&server).expect("message sent to socket");
        let pid = std::process::id();
        assert!(msg.starts_with("<28>"), "{msg}");
        assert!(
            msg.ends_with(&format!(
                " gw-test[{pid}]: conn{{connection=meter-tcp1}}: io timeout device=meter1 retry_in=2s"
            )),
            "{msg}"
        );
        assert_eq!(fallback.text(), "");
    }

    #[test]
    fn falls_back_to_stderr_until_socket_appears() {
        let path = temp_socket("retry");
        let fallback = Captured::default();
        let subscriber = tracing_subscriber::registry().with(layer(&path, fallback.clone()));

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("before syslogd");
            assert_eq!(fallback.text(), " INFO before syslogd\n");

            // The failed initial connect scheduled a retry 50ms out; the
            // next event after that picks up the socket.
            let server = bind(&path);
            std::thread::sleep(Duration::from_millis(300));
            tracing::error!("after syslogd");

            let msg = recv(&server).expect("picked up socket within backoff");
            assert!(msg.starts_with("<27>"), "{msg}");
            assert!(msg.ends_with(": after syslogd"), "{msg}");
            assert_eq!(
                fallback.text(),
                " INFO before syslogd\n",
                "nothing replayed"
            );
        });
    }

    #[test]
    fn retry_backoff_doubles_up_to_max() {
        let path = temp_socket("backoff");
        let start = Instant::now();
        let mut conn = Connection::new(path, Duration::from_millis(10), Duration::from_millis(40));
        assert!(conn.socket.is_none());
        assert_eq!(conn.backoff, Duration::from_millis(20));

        // Before the deadline: no attempt, backoff unchanged.
        assert!(conn.socket(start).is_none());
        assert_eq!(conn.backoff, Duration::from_millis(20));

        let mut now = conn.next_attempt;
        for expected in [40, 40, 40] {
            assert!(conn.socket(now).is_none());
            assert_eq!(conn.backoff, Duration::from_millis(expected));
            now = conn.next_attempt;
        }
    }

    #[test]
    fn reconnects_after_syslogd_restart() {
        let path = temp_socket("restart");
        let server = bind(&path);
        let fallback = Captured::default();
        let subscriber = tracing_subscriber::registry().with(layer(&path, fallback.clone()));

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("one");
            assert!(recv(&server).unwrap().ends_with(": one"));

            drop(server);
            std::fs::remove_file(&path).unwrap();
            tracing::info!("two");
            assert_eq!(fallback.text(), " INFO two\n");

            let server = bind(&path);
            std::thread::sleep(Duration::from_millis(100));
            tracing::info!("three");
            assert!(recv(&server).unwrap().ends_with(": three"));
        });
    }
}
