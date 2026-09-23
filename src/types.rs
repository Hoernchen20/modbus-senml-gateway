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
    pub points: Vec<AggregatedPoint>,
}
