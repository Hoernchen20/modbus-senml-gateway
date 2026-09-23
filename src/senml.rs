use std::collections::HashMap;

use serde::Serialize;

use crate::types::{AggregatedBatch, AggregatedPoint};

#[derive(Serialize)]
struct SenmlRecord {
    #[serde(rename = "bn", skip_serializing_if = "Option::is_none")]
    base_name: Option<String>,
    #[serde(rename = "bt", skip_serializing_if = "Option::is_none")]
    base_time: Option<u64>,
    #[serde(rename = "bu", skip_serializing_if = "Option::is_none")]
    base_unit: Option<String>,
    n: String,
    u: String,
    v: f64,
}

/// One SenML pack per device (§8). `base_name_prefix` comes from
/// `gateway.base_name_prefix`; the pack's `bn` is `{base_name_prefix}{device_id}:`.
pub fn encode_senml(batch: &AggregatedBatch, base_name_prefix: &str) -> Vec<u8> {
    let base_unit = majority_unit(&batch.points);

    let records: Vec<SenmlRecord> = batch
        .points
        .iter()
        .enumerate()
        .map(|(i, p)| SenmlRecord {
            base_name: (i == 0).then(|| format!("{base_name_prefix}{}:", batch.device_id)),
            base_time: (i == 0).then_some(batch.window_start),
            base_unit: (i == 0).then(|| base_unit.clone()).flatten(),
            n: p.name.clone(),
            u: p.unit.clone(),
            v: p.mean,
        })
        .collect();

    serde_json::to_vec(&records).expect("SenML records always serialize")
}

/// `bu` is only included when a single unit is shared by a strict majority
/// of the pack's points — every point still carries its own explicit `u`
/// (§8), so `bu` is an informational default rather than load-bearing, and
/// this is a conservative reading of "most points... share a unit".
fn majority_unit(points: &[AggregatedPoint]) -> Option<String> {
    if points.is_empty() {
        return None;
    }
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for p in points {
        *counts.entry(p.unit.as_str()).or_insert(0) += 1;
    }
    let (unit, count) = counts.into_iter().max_by_key(|&(_, c)| c)?;
    (count * 2 > points.len()).then(|| unit.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(name: &str, unit: &str, mean: f64) -> AggregatedPoint {
        AggregatedPoint {
            name: name.to_string(),
            unit: unit.to_string(),
            mean,
        }
    }

    fn batch(points: Vec<AggregatedPoint>) -> AggregatedBatch {
        AggregatedBatch {
            device_id: "meter1".to_string(),
            window_start: 1758447120,
            window_end: 1758447180,
            points,
        }
    }

    fn parse(bytes: &[u8]) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    #[test]
    fn bn_and_bt_appear_only_on_first_record() {
        let b = batch(vec![
            point("voltage_l1", "V", 231.4),
            point("current_l1", "A", 4.82),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw-router1:"));

        assert_eq!(json[0]["bn"], "urn:dev:gw-router1:meter1:");
        assert_eq!(json[0]["bt"], 1758447120);
        assert!(json[1].get("bn").is_none());
        assert!(json[1].get("bt").is_none());
    }

    #[test]
    fn every_record_carries_its_own_unit_and_value() {
        let b = batch(vec![
            point("voltage_l1", "V", 231.4),
            point("current_l1", "A", 4.82),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw-router1:"));

        assert_eq!(json[0]["n"], "voltage_l1");
        assert_eq!(json[0]["u"], "V");
        assert_eq!(json[0]["v"], 231.4);
        assert_eq!(json[1]["n"], "current_l1");
        assert_eq!(json[1]["u"], "A");
        assert_eq!(json[1]["v"], 4.82);
    }

    #[test]
    fn bu_set_when_majority_of_points_share_a_unit() {
        let b = batch(vec![
            point("temperature", "Cel", 21.0),
            point("temperature2", "Cel", 22.0),
            point("temperature3", "Cel", 23.0),
            point("humidity", "%RH", 40.0),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw:"));
        assert_eq!(json[0]["bu"], "Cel");
    }

    #[test]
    fn bu_omitted_when_no_unit_has_a_majority() {
        let b = batch(vec![
            point("voltage_l1", "V", 231.4),
            point("current_l1", "A", 4.82),
            point("active_power", "W", 1112.6),
            point("energy_total", "kWh", 18452.311),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw:"));
        assert!(json[0].get("bu").is_none());
    }

    #[test]
    fn empty_points_encodes_to_empty_array() {
        let b = batch(vec![]);
        assert_eq!(encode_senml(&b, "urn:dev:gw:"), b"[]");
    }
}
