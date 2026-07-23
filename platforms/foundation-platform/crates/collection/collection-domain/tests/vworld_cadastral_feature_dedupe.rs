//! Contract tests for `VWorld` cadastral feature deduplication.

use collection_domain::{
    dedupe_vworld_cadastral_features_by_pnu, VWorldCadastralFeatureDedupeAccumulator,
};
use serde_json::json;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn dedupe_vworld_cadastral_features_keeps_one_record_per_pnu_in_deterministic_order() -> TestResult
{
    let payload_a = json!({
        "response": {
            "result": {
                "featureCollection": {
                    "features": [
                        cadastral_feature("9999900801105800002", "580-2", 127.123_470_234_330),
                        cadastral_feature("9999900801105800001", "580-1", 127.123_470_234_300)
                    ]
                }
            }
        }
    });
    let payload_b = json!({
        "response": {
            "result": {
                "featureCollection": {
                    "features": [
                        cadastral_feature("9999900801105800001", "580-1", 127.123_470_234_300)
                    ]
                }
            }
        }
    });

    let report = dedupe_vworld_cadastral_features_by_pnu(&[payload_a, payload_b])?;

    assert_eq!(report.duplicate_count, 1);
    assert_eq!(
        report
            .records
            .iter()
            .map(|record| (record.pnu.as_str(), record.occurrence_count))
            .collect::<Vec<_>>(),
        vec![("9999900801105800001", 2), ("9999900801105800002", 1)]
    );
    assert_eq!(report.records[0].properties["jibun"], "580-1");
    assert_eq!(report.records[0].geometry["type"], "MultiPolygon");
    Ok(())
}

#[test]
fn dedupe_vworld_cadastral_features_rejects_geometry_conflicts_for_same_pnu() -> TestResult {
    let payload_a = payload_with_features(&[cadastral_feature(
        "9999900801105800001",
        "580-1",
        127.123_470_234_300,
    )]);
    let payload_b = payload_with_features(&[cadastral_feature(
        "9999900801105800001",
        "580-1",
        127.123_470_234_350,
    )]);

    let error = dedupe_vworld_cadastral_features_by_pnu(&[payload_a, payload_b])
        .err()
        .ok_or("same PNU with different geometry must be rejected")?;

    assert!(
        error.to_string().contains("geometry conflict"),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("9999900801105800001"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn dedupe_vworld_cadastral_features_rejects_invalid_pnu() -> TestResult {
    let payload = payload_with_features(&[cadastral_feature(
        "999990080110580",
        "580",
        127.123_470_234_300,
    )]);

    let error = dedupe_vworld_cadastral_features_by_pnu(&[payload])
        .err()
        .ok_or("invalid PNU must be rejected")?;

    assert!(
        error.to_string().contains("pnu must be exactly 19 digits"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn dedupe_vworld_cadastral_features_preserves_19_digit_provider_pnu_before_canonicalization(
) -> TestResult {
    let provider_pnu = "9999900801005800001";
    let payload = payload_with_features(&[cadastral_feature(
        provider_pnu,
        "580-1",
        127.123_470_234_300,
    )]);

    let report = dedupe_vworld_cadastral_features_by_pnu(&[payload])?;

    assert_eq!(report.invalid_pnu_feature_count, 0);
    assert_eq!(report.records.len(), 1);
    assert_eq!(report.records[0].pnu, provider_pnu);
    Ok(())
}

#[test]
fn dedupe_vworld_cadastral_features_treats_provider_not_found_zero_record_payload_as_empty(
) -> TestResult {
    let empty_payload = json!({
        "response": {
            "status": "NOT_FOUND",
            "record": {
                "total": "0",
                "current": "0"
            },
            "page": {
                "total": "1",
                "current": "1",
                "size": "100"
            }
        }
    });
    let payload = payload_with_features(&[cadastral_feature(
        "9999900801105800001",
        "580-1",
        127.123_470_234_300,
    )]);

    let report = dedupe_vworld_cadastral_features_by_pnu(&[empty_payload, payload])?;

    assert_eq!(report.duplicate_count, 0);
    assert_eq!(report.records.len(), 1);
    assert_eq!(report.records[0].pnu, "9999900801105800001");
    Ok(())
}

#[test]
fn dedupe_accumulator_accepts_incremental_payload_chunks() -> TestResult {
    let mut accumulator = VWorldCadastralFeatureDedupeAccumulator::new();

    accumulator.ingest_payload(&payload_with_features(&[
        cadastral_feature("9999900801105800002", "580-2", 127.123_470_234_330),
        cadastral_feature("9999900801105800001", "580-1", 127.123_470_234_300),
    ]))?;
    accumulator.ingest_payload(&payload_with_features(&[cadastral_feature(
        "9999900801105800001",
        "580-1",
        127.123_470_234_300,
    )]))?;

    let report = accumulator.finish();

    assert_eq!(report.duplicate_count, 1);
    assert_eq!(
        report
            .records
            .iter()
            .map(|record| (record.pnu.as_str(), record.occurrence_count))
            .collect::<Vec<_>>(),
        vec![("9999900801105800001", 2), ("9999900801105800002", 1)]
    );
    Ok(())
}

#[test]
fn dedupe_accumulator_can_quarantine_invalid_pnu_features_for_national_exports() -> TestResult {
    let mut accumulator =
        VWorldCadastralFeatureDedupeAccumulator::new_with_invalid_pnu_quarantine();

    accumulator.ingest_payload(&payload_with_features(&[
        cadastral_feature("999990080110580", "invalid", 127.123_470_234_290),
        cadastral_feature("9999900801105800001", "580-1", 127.123_470_234_300),
    ]))?;

    let report = accumulator.finish();

    assert_eq!(report.invalid_pnu_feature_count, 1);
    assert_eq!(report.records.len(), 1);
    assert_eq!(report.records[0].pnu, "9999900801105800001");
    Ok(())
}

#[test]
fn dedupe_accumulator_rejects_geometry_conflicts_across_payload_chunks() -> TestResult {
    let mut accumulator = VWorldCadastralFeatureDedupeAccumulator::new();

    accumulator.ingest_payload(&payload_with_features(&[cadastral_feature(
        "9999900801105800001",
        "580-1",
        127.123_470_234_300,
    )]))?;
    let error = accumulator
        .ingest_payload(&payload_with_features(&[cadastral_feature(
            "9999900801105800001",
            "580-1",
            127.123_470_234_350,
        )]))
        .err()
        .ok_or("same PNU with different geometry across chunks must be rejected")?;

    assert!(
        error.to_string().contains("geometry conflict"),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("payload 1"),
        "unexpected error: {error}"
    );
    Ok(())
}

fn payload_with_features(features: &[serde_json::Value]) -> serde_json::Value {
    json!({
        "response": {
            "result": {
                "featureCollection": {
                    "features": features
                }
            }
        }
    })
}

fn cadastral_feature(pnu: &str, jibun: &str, first_x: f64) -> serde_json::Value {
    json!({
        "type": "Feature",
        "properties": {
            "pnu": pnu,
            "jibun": jibun,
            "bonbun": "0580",
            "bubun": "0001"
        },
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[
                [first_x, 36.123_440_0],
                [first_x + 0.0010, 36.123_440_0],
                [first_x + 0.0010, 36.123_441_0],
                [first_x, 36.123_441_0],
                [first_x, 36.123_440_0]
            ]]]
        }
    })
}
