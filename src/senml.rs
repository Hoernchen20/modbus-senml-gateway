use serde::Serialize;

use crate::types::AggregatedBatch;

/// SenML record name of the per-device connection status (§8). Reserved:
/// config validation rejects a point with this name.
pub const ONLINE_RECORD: &str = "online";

#[derive(Serialize)]
struct SenmlRecord {
    #[serde(rename = "bn", skip_serializing_if = "Option::is_none")]
    base_name: Option<String>,
    #[serde(rename = "bt", skip_serializing_if = "Option::is_none")]
    base_time: Option<u64>,
    n: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    u: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    v: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    vb: Option<bool>,
}

/// One SenML pack per device (§8). `base_name_prefix` comes from
/// `gateway.base_name_prefix`; the pack's `bn` is `{base_name_prefix}{device_id}:`.
///
/// The first record is always `{"n": "online", "vb": ...}` and carries `bn`
/// and `bt`, so a device with no samples still gets a valid pack. There is
/// no `bu`: a record without `u` inherits `bu` (RFC 8428 §4.3), which would
/// give the boolean a unit, and every point carries its own `u` anyway.
pub fn encode_senml(batch: &AggregatedBatch, base_name_prefix: &str) -> Vec<u8> {
    let status = SenmlRecord {
        base_name: Some(format!("{base_name_prefix}{}:", batch.device_id)),
        base_time: Some(batch.window_start),
        n: ONLINE_RECORD.to_string(),
        u: None,
        v: None,
        vb: Some(batch.online),
    };
    let points = batch.points.iter().map(|p| SenmlRecord {
        base_name: None,
        base_time: None,
        n: p.name.clone(),
        u: Some(p.unit.clone()),
        v: Some(p.mean),
        vb: None,
    });
    let records: Vec<SenmlRecord> = std::iter::once(status).chain(points).collect();

    serde_json::to_vec(&records).expect("SenML records always serialize")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AggregatedPoint;

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
            online: true,
            points,
        }
    }

    fn parse(bytes: &[u8]) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    #[test]
    fn status_record_comes_first_and_carries_bn_and_bt() {
        let b = batch(vec![
            point("voltage_l1", "V", 231.4),
            point("current_l1", "A", 4.82),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw-router1:"));

        assert_eq!(
            json[0],
            serde_json::json!({
                "bn": "urn:dev:gw-router1:meter1:",
                "bt": 1758447120,
                "n": "online",
                "vb": true,
            })
        );
        for record in json.as_array().unwrap()[1..].iter() {
            assert!(record.get("bn").is_none());
            assert!(record.get("bt").is_none());
            assert!(record.get("vb").is_none());
        }
    }

    #[test]
    fn every_point_record_carries_its_own_unit_and_value() {
        let b = batch(vec![
            point("voltage_l1", "V", 231.4),
            point("current_l1", "A", 4.82),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw-router1:"));

        assert_eq!(json[1]["n"], "voltage_l1");
        assert_eq!(json[1]["u"], "V");
        assert_eq!(json[1]["v"], 231.4);
        assert_eq!(json[2]["n"], "current_l1");
        assert_eq!(json[2]["u"], "A");
        assert_eq!(json[2]["v"], 4.82);
    }

    #[test]
    fn no_bu_even_when_all_points_share_a_unit() {
        // A `bu` would also apply to the unitless "online" record.
        let b = batch(vec![
            point("temperature", "Cel", 21.0),
            point("temperature2", "Cel", 22.0),
        ]);
        let json = parse(&encode_senml(&b, "urn:dev:gw:"));
        assert!(json
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r.get("bu").is_none()));
    }

    #[test]
    fn offline_device_without_points_gets_status_only_pack() {
        let b = AggregatedBatch {
            online: false,
            ..batch(vec![])
        };
        assert_eq!(
            parse(&encode_senml(&b, "urn:dev:gw:")),
            serde_json::json!([{
                "bn": "urn:dev:gw:meter1:",
                "bt": 1758447120,
                "n": "online",
                "vb": false,
            }])
        );
    }
}
