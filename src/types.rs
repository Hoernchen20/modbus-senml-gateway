#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PointId {
    pub device: String,
    pub point: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub point_id: PointId,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregatedPoint {
    pub name: String,
    pub unit: String,
    pub mean: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregatedBatch {
    pub device_id: String,
    pub window_start: u64,
    pub window_end: u64,
    /// At least one clean read of the device in this window.
    pub online: bool,
    pub points: Vec<AggregatedPoint>,
}

/// Poller → aggregator. `DeviceOk` is sent once per tick for every device
/// whose blocks all read cleanly, from the same result that becomes
/// `StatusEvent::DeviceOk`, so the uplink's `online` record and the status
/// file agree.
#[derive(Debug, Clone, PartialEq)]
pub enum PollerMsg {
    Reading(Reading),
    DeviceOk { device_id: String },
}
