use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;
use tracing::warn;

use crate::config::schema::Config;
use crate::types::{AggregatedBatch, AggregatedPoint, PointId, Reading};

#[derive(Debug, Clone)]
pub struct PointAgg {
    pub sum: f64,
    pub count: u32,
    pub min: f64,
    pub max: f64,
    pub unit: String,
    /// From `point.decimals`; applied to the mean in `flush` only.
    pub decimals: Option<u8>,
}

impl PointAgg {
    fn new(unit: String) -> Self {
        PointAgg {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            unit,
            decimals: None,
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
    }
}

/// Every `(device.id, point.name)` pair gets a zeroed `PointAgg` up front, so
/// a point with no samples in a window is a detectable, loggable gap rather
/// than a silent omission (§7).
pub fn init_from_config(config: &Config) -> HashMap<PointId, PointAgg> {
    let mut agg = HashMap::new();
    for conn in &config.connections {
        for device in &conn.devices {
            for block in &device.blocks {
                for point in &block.points {
                    agg.insert(
                        PointId {
                            device: device.id.clone(),
                            point: point.name.clone(),
                        },
                        PointAgg {
                            decimals: point.decimals,
                            ..PointAgg::new(point.unit.clone())
                        },
                    );
                }
            }
        }
    }
    agg
}

/// Single aggregator task, tumbling window aligned to wall-clock boundaries
/// of `window_secs`. `now_unix` is the caller's current wall-clock time
/// (duration since the Unix epoch) — injected rather than sampled internally
/// so the initial alignment is deterministic in tests.
pub async fn run_aggregator(
    mut rx: mpsc::Receiver<Reading>,
    publish_tx: mpsc::Sender<AggregatedBatch>,
    mut agg: HashMap<PointId, PointAgg>,
    window_secs: u64,
    now_unix: Duration,
) {
    let window = Duration::from_secs(window_secs);
    let (wait, mut window_end_unix) = next_boundary(now_unix, window);

    let mut deadline = time::Instant::now() + wait;
    let sleep = time::sleep_until(deadline);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            maybe_reading = rx.recv() => {
                match maybe_reading {
                    Some(reading) => record(&mut agg, reading),
                    None => return,
                }
            }
            _ = &mut sleep => {
                let window_start_unix = window_end_unix - window_secs;
                for batch in flush(&mut agg, window_start_unix, window_end_unix) {
                    if publish_tx.send(batch).await.is_err() {
                        return;
                    }
                }
                // Advance the schedule by a fixed `window`, not "now + window" —
                // keeps the boundary drift-free even if this tick fired late.
                window_end_unix += window_secs;
                deadline += window;
                sleep.as_mut().reset(deadline);
            }
        }
    }
}

/// Wait duration and absolute unix-seconds boundary for the next tick,
/// starting from `now`, aligned to a multiple of `window`.
fn next_boundary(now: Duration, window: Duration) -> (Duration, u64) {
    let window_secs = window.as_secs();
    let boundary_secs = (now.as_secs() / window_secs + 1) * window_secs;
    let boundary = Duration::from_secs(boundary_secs);
    (boundary - now, boundary_secs)
}

fn record(agg: &mut HashMap<PointId, PointAgg>, reading: Reading) {
    // The poller already drops these; a NaN here would poison the mean.
    if !reading.value.is_finite() {
        return;
    }
    if let Some(point_agg) = agg.get_mut(&reading.point_id) {
        point_agg.sum += reading.value;
        point_agg.count += 1;
        point_agg.min = point_agg.min.min(reading.value);
        point_agg.max = point_agg.max.max(reading.value);
    }
}

/// Drains every point's accumulator into per-device batches and resets them
/// for the next window. A point with `count == 0` is dropped (logged), never
/// fabricated as zero/repeated; a device left with no surviving points gets
/// no batch at all rather than an empty one.
fn flush(
    agg: &mut HashMap<PointId, PointAgg>,
    window_start: u64,
    window_end: u64,
) -> Vec<AggregatedBatch> {
    let mut by_device: HashMap<String, Vec<AggregatedPoint>> = HashMap::new();

    for (point_id, point_agg) in agg.iter_mut() {
        if point_agg.count == 0 {
            warn!(device = %point_id.device, point = %point_id.point, "no samples in window, dropping point");
        } else {
            let mut mean = point_agg.sum / point_agg.count as f64;
            if let Some(decimals) = point_agg.decimals {
                mean = round_to_decimals(mean, decimals);
            }
            by_device
                .entry(point_id.device.clone())
                .or_default()
                .push(AggregatedPoint {
                    name: point_id.point.clone(),
                    unit: point_agg.unit.clone(),
                    mean,
                });
        }
        point_agg.reset();
    }

    by_device
        .into_iter()
        .map(|(device_id, points)| AggregatedBatch {
            device_id,
            window_start,
            window_end,
            points,
        })
        .collect()
}

/// Rounds half away from zero. `(v * 10^d).round() / 10^d` is the f64
/// closest to the decimal result, and serde_json prints the shortest string
/// that round-trips, so `228.49199981689452` with 1 decimal comes out as
/// `228.5`, not `228.50000000000003`.
fn round_to_decimals(value: f64, decimals: u8) -> f64 {
    let factor = 10f64.powi(decimals.into());
    let scaled = value * factor;
    // From 2^52 on, every f64 is already a whole number, so there's nothing
    // to round and dividing back could only add error. Also catches inf.
    if scaled.abs() >= 4_503_599_627_370_496.0 {
        return value;
    }
    // `+ 0.0` turns -0.0 (e.g. -0.04 at 1 decimal) into 0.0, so it isn't
    // published as "-0.0".
    (scaled.round() / factor) + 0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point_id(device: &str, point: &str) -> PointId {
        PointId {
            device: device.to_string(),
            point: point.to_string(),
        }
    }

    #[test]
    fn next_boundary_aligns_to_window_multiple() {
        let (wait, boundary) = next_boundary(Duration::from_secs(90), Duration::from_secs(60));
        assert_eq!(boundary, 120);
        assert_eq!(wait, Duration::from_secs(30));
    }

    #[test]
    fn record_ignores_non_finite_values() {
        let mut agg = HashMap::from([(point_id("d1", "p1"), PointAgg::new("V".to_string()))]);
        for value in [f64::NAN, 4.0, f64::INFINITY, f64::NEG_INFINITY] {
            record(
                &mut agg,
                Reading {
                    point_id: point_id("d1", "p1"),
                    value,
                },
            );
        }

        let p = &agg[&point_id("d1", "p1")];
        assert_eq!((p.count, p.sum, p.min, p.max), (1, 4.0, 4.0, 4.0));
    }

    #[test]
    fn record_accumulates_sum_count_min_max() {
        let mut agg = HashMap::from([(point_id("d1", "p1"), PointAgg::new("V".to_string()))]);
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 10.0,
            },
        );
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 20.0,
            },
        );
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 5.0,
            },
        );

        let p = &agg[&point_id("d1", "p1")];
        assert_eq!(p.count, 3);
        assert_eq!(p.sum, 35.0);
        assert_eq!(p.min, 5.0);
        assert_eq!(p.max, 20.0);
    }

    #[test]
    fn readings_for_unknown_point_id_are_ignored() {
        let mut agg = HashMap::from([(point_id("d1", "p1"), PointAgg::new("V".to_string()))]);
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "unknown"),
                value: 10.0,
            },
        );
        assert_eq!(agg[&point_id("d1", "p1")].count, 0);
    }

    #[test]
    fn flush_drops_zero_count_points_and_omits_empty_device_batches() {
        let mut agg = HashMap::from([
            (point_id("d1", "p1"), PointAgg::new("V".to_string())),
            (point_id("d2", "p2"), PointAgg::new("A".to_string())),
        ]);
        // Only d1's point gets a sample; d2's point has zero samples.
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 42.0,
            },
        );

        let batches = flush(&mut agg, 0, 60);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].device_id, "d1");
        assert_eq!(batches[0].points.len(), 1);
        assert_eq!(batches[0].points[0].mean, 42.0);
    }

    #[test]
    fn flush_computes_mean_and_resets_for_next_window() {
        let mut agg = HashMap::from([(point_id("d1", "p1"), PointAgg::new("V".to_string()))]);
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 10.0,
            },
        );
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 20.0,
            },
        );

        let batches = flush(&mut agg, 0, 60);
        assert_eq!(batches[0].points[0].mean, 15.0);

        // Reset: a second flush with no new readings drops the point.
        let batches = flush(&mut agg, 60, 120);
        assert!(batches.is_empty());
    }

    #[test]
    fn round_to_decimals_cases() {
        assert_eq!(round_to_decimals(228.49199981689452, 1), 228.5);
        assert_eq!(round_to_decimals(228.49199981689452, 0), 228.0);
        assert_eq!(round_to_decimals(4.8249, 2), 4.82);
        assert_eq!(round_to_decimals(-1.25, 1), -1.3);
        assert_eq!(round_to_decimals(18452.3114999, 3), 18452.311);
        let neg_zero = round_to_decimals(-0.04, 1);
        assert!(neg_zero == 0.0 && neg_zero.is_sign_positive());
        // Too large to have fractional digits at this scale: left as is.
        assert_eq!(round_to_decimals(1e300, 15), 1e300);
        assert_eq!(round_to_decimals(f64::MAX, 2), f64::MAX);
    }

    #[test]
    fn rounded_values_serialize_short() {
        // Every 0.001 step in a typical range must print with no more
        // digits than requested, e.g. never "0.30000000000000004".
        for i in -20_000..20_000 {
            let raw = i as f64 * 0.001 + 1e-7;
            let s = serde_json::to_string(&round_to_decimals(raw, 3)).unwrap();
            let frac = s.split_once('.').map_or(0, |(_, f)| f.len());
            assert!(frac <= 3, "{raw} -> {s}");
        }
        assert_eq!(
            serde_json::to_string(&round_to_decimals(228.49199981689452, 1)).unwrap(),
            "228.5"
        );
    }

    #[test]
    fn flush_rounds_the_mean_not_the_samples() {
        let mut agg = HashMap::from([(
            point_id("d1", "p1"),
            PointAgg {
                decimals: Some(0),
                ..PointAgg::new("V".to_string())
            },
        )]);
        // Rounding each sample would give mean(0, 0, 1) = 0.33; rounding
        // the mean of the raw samples gives round(0.5) = 1.
        for value in [0.4, 0.4, 0.7] {
            record(
                &mut agg,
                Reading {
                    point_id: point_id("d1", "p1"),
                    value,
                },
            );
        }

        let batches = flush(&mut agg, 0, 60);
        assert_eq!(batches[0].points[0].mean, 1.0);
    }

    #[test]
    fn flush_without_decimals_leaves_mean_unrounded() {
        let mut agg = HashMap::from([(point_id("d1", "p1"), PointAgg::new("V".to_string()))]);
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "p1"),
                value: 228.49199981689452,
            },
        );

        let batches = flush(&mut agg, 0, 60);
        assert_eq!(batches[0].points[0].mean, 228.49199981689452);
    }

    #[test]
    fn concurrent_devices_do_not_cross_contaminate() {
        let mut agg = HashMap::from([
            (
                point_id("d1", "temperature"),
                PointAgg::new("Cel".to_string()),
            ),
            (
                point_id("d2", "temperature"),
                PointAgg::new("Cel".to_string()),
            ),
        ]);
        record(
            &mut agg,
            Reading {
                point_id: point_id("d1", "temperature"),
                value: 21.0,
            },
        );
        record(
            &mut agg,
            Reading {
                point_id: point_id("d2", "temperature"),
                value: 99.0,
            },
        );

        let batches = flush(&mut agg, 0, 60);
        let by_device: HashMap<_, _> = batches
            .into_iter()
            .map(|b| (b.device_id, b.points[0].mean))
            .collect();

        assert_eq!(by_device["d1"], 21.0);
        assert_eq!(by_device["d2"], 99.0);
    }

    #[tokio::test(start_paused = true)]
    async fn window_boundaries_stay_drift_free_across_a_late_tick() {
        let (reading_tx, reading_rx) = mpsc::channel(16);
        let (publish_tx, mut publish_rx) = mpsc::channel(16);
        let agg = HashMap::from([(point_id("d1", "p1"), PointAgg::new("V".to_string()))]);
        let start = time::Instant::now();

        tokio::spawn(run_aggregator(
            reading_rx,
            publish_tx,
            agg,
            60,
            Duration::from_secs(0),
        ));
        // Let the spawned task run to its first pending point (registering its
        // sleep deadline against the *current* paused clock) before advancing
        // time — otherwise the deadline gets computed against already-advanced
        // time and the test waits on a boundary that's pushed further out.
        tokio::task::yield_now().await;

        reading_tx
            .send(Reading {
                point_id: point_id("d1", "p1"),
                value: 1.0,
            })
            .await
            .unwrap();

        // Advance well past the first boundary (60s) to simulate a late tick.
        time::advance(Duration::from_secs(90)).await;
        let first = publish_rx.recv().await.unwrap();
        assert_eq!((first.window_start, first.window_end), (0, 60));

        // The second window needs a sample too — an empty window produces no
        // batch by design (§7), so without it `recv()` would wait forever.
        reading_tx
            .send(Reading {
                point_id: point_id("d1", "p1"),
                value: 2.0,
            })
            .await
            .unwrap();

        // The next boundary must still be exactly 60s after the first —
        // not shifted by the 30s of lateness in the previous tick.
        time::advance(Duration::from_secs(30)).await;
        let second = publish_rx.recv().await.unwrap();
        assert_eq!((second.window_start, second.window_end), (60, 120));
        // Labels come from a counter, so also check the tick itself fired at
        // start+120s — a "now + window" reschedule would fire at 150s (the
        // paused clock auto-advances to it) and fail here.
        assert_eq!(time::Instant::now() - start, Duration::from_secs(120));
    }

    #[tokio::test(start_paused = true)]
    async fn aggregator_stops_when_reading_channel_closes() {
        let (reading_tx, reading_rx) = mpsc::channel::<Reading>(16);
        let (publish_tx, _publish_rx) = mpsc::channel(16);
        let agg = HashMap::new();

        let handle = tokio::spawn(run_aggregator(
            reading_rx,
            publish_tx,
            agg,
            60,
            Duration::from_secs(0),
        ));

        drop(reading_tx);
        time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("aggregator should exit promptly once the reading channel closes")
            .unwrap();
    }
}
