//! Compatibility corpus for Lakehouse quality policy evaluation.

use lakehouse_domain::{evaluate_lakehouse_quality_rules, LakehouseQualityRules, SparkRunSummary};
use serde_json::{json, Value};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const QUALITY_RULES_FIXTURE: &str =
    include_str!("../../../../docs/data-quality/lakehouse-quality-rules.v1.example.json");

#[test]
fn valid_summary_passes_the_published_rule_set() -> TestResult {
    let summary = summary()?;
    let rules = LakehouseQualityRules::from_json_str(QUALITY_RULES_FIXTURE)?;

    let outcome = evaluate_lakehouse_quality_rules(&summary, &rules)?;

    assert_eq!(outcome.table, "silver.industrial_complexes");
    assert_eq!(outcome.evaluated_rule_count, 6);
    assert!(outcome.violations.is_empty());
    assert!(!outcome.is_blocked());
    Ok(())
}

#[test]
fn missing_and_failed_metrics_preserve_cli_violation_text() -> TestResult {
    let mut summary_value = summary_json();
    summary_value["quality_metrics"]
        .as_object_mut()
        .ok_or_else(|| std::io::Error::other("quality_metrics must be an object"))?
        .remove("invalid_checksum_count");
    summary_value["quality_metrics"]["complex_name__empty_count"] = json!(2);
    let summary = SparkRunSummary::from_json_value(summary_value)?;
    let rules = LakehouseQualityRules::from_json_str(QUALITY_RULES_FIXTURE)?;

    let outcome = evaluate_lakehouse_quality_rules(&summary, &rules)?;

    assert_eq!(
        outcome.violations,
        vec![
            "lakehouse quality rule failed: rule=silver-industrial-complexes-complex-name-empty metric=complex_name__empty_count actual=2 threshold={\"kind\":\"equals\",\"value\":0}".to_owned(),
            "lakehouse quality metric missing: rule=silver-industrial-complexes-invalid-checksum metric=invalid_checksum_count".to_owned(),
        ]
    );
    assert!(outcome.is_blocked());
    Ok(())
}

#[test]
fn rejects_unsupported_rule_kind_with_stable_message() -> TestResult {
    let summary = summary()?;
    let mut rules_value: Value = serde_json::from_str(QUALITY_RULES_FIXTURE)?;
    rules_value["rule_sets"][0]["rules"][0]["threshold"]["kind"] = json!("maximum");
    let rules = LakehouseQualityRules::from_json_value(rules_value)?;

    let Err(error) = evaluate_lakehouse_quality_rules(&summary, &rules) else {
        return Err("unsupported threshold unexpectedly passed".into());
    };

    assert_eq!(
        error.to_string(),
        "Unsupported lakehouse quality threshold kind: maximum"
    );
    Ok(())
}

fn summary() -> TestResult<SparkRunSummary> {
    SparkRunSummary::from_json_value(summary_json()).map_err(Into::into)
}

fn summary_json() -> Value {
    json!({
        "schema_version": "foundation-platform.spark_run_summary.v1",
        "job_name": "industrial_complex_bronze_to_silver",
        "contract": "silver.industrial_complexes",
        "created_at_utc": "2026-05-18T12:00:00Z",
        "input": {"kind": "silver_handoff_jsonl", "path": "target/input.jsonl"},
        "target": {"kind": "parquet", "path": "target/output.parquet"},
        "write_mode": "parquet",
        "write_disposition": "parquet_overwrite",
        "row_count": 2,
        "persisted_row_count": 2,
        "quality_metrics": {
            "row_count": 2,
            "complex_id__null_count": 0,
            "complex_name__empty_count": 0,
            "invalid_official_area_count": 0,
            "invalid_checksum_count": 0
        },
        "column_count": 1,
        "columns": ["complex_id"],
        "required_columns": ["complex_id"],
        "source_snapshot_count": 1,
        "source_snapshot_ids": ["snapshot-1"],
        "source_snapshot_truncated": false
    })
}
