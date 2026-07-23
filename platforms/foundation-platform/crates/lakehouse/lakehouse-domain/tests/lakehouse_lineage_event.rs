//! Compatibility corpus for the provider-neutral Lakehouse lineage contract.

use lakehouse_domain::validate_lakehouse_lineage_event;
use serde_json::{json, Value};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const LINEAGE_EVENT_FIXTURE: &str =
    include_str!("../../../../docs/events/lineage/lakehouse-lineage-event.v1.example.json");

#[test]
fn accepts_the_published_lineage_fixture_without_mutating_it() -> TestResult {
    let event: Value = serde_json::from_str(LINEAGE_EVENT_FIXTURE)?;
    let before = event.clone();

    validate_lakehouse_lineage_event(&event)?;

    assert_eq!(event, before);
    Ok(())
}

#[test]
fn rejects_the_existing_attack_corpus_with_stable_messages() -> TestResult {
    let base: Value = serde_json::from_str(LINEAGE_EVENT_FIXTURE)?;
    let cases: Vec<(&str, Value, &str)> = vec![
        (
            "schema mismatch",
            with_value(&base, &["schema_version"], json!("unsupported"))?,
            "lineage event schema_version mismatch",
        ),
        (
            "blank producer",
            with_value(&base, &["producer"], json!(" "))?,
            "lineage event missing required field: producer",
        ),
        (
            "empty source lineage",
            with_value(&base, &["source_snapshot_ids"], json!([]))?,
            "lineage event source_snapshot_ids must not be empty",
        ),
        (
            "truncated source lineage",
            with_value(&base, &["source_snapshot_truncated"], json!(true))?,
            "lineage event source_snapshot_truncated must be false",
        ),
        (
            "zero rows",
            with_value(&base, &["quality_metrics", "row_count"], json!(0))?,
            "lineage event quality_metrics.row_count must be positive",
        ),
        (
            "empty column lineage",
            with_value(&base, &["column_lineage"], json!([]))?,
            "lineage event column_lineage must not be empty",
        ),
        (
            "missing OpenLineage mapping",
            without_field(&base, "openlineage_mapping")?,
            "lineage event missing required field: openlineage_mapping.event_type",
        ),
    ];

    for (name, event, expected) in cases {
        let Err(error) = validate_lakehouse_lineage_event(&event) else {
            return Err(format!("attack corpus event unexpectedly passed: {name}").into());
        };
        assert_eq!(error.to_string(), expected, "case={name}");
    }
    Ok(())
}

fn with_value(base: &Value, path: &[&str], replacement: Value) -> TestResult<Value> {
    let mut value = base.clone();
    let (leaf, parents) = path
        .split_last()
        .ok_or_else(|| std::io::Error::other("mutation path must not be empty"))?;
    let mut cursor = &mut value;
    for parent in parents {
        cursor = cursor
            .get_mut(*parent)
            .ok_or_else(|| std::io::Error::other(format!("missing fixture path: {parent}")))?;
    }
    cursor[*leaf] = replacement;
    Ok(value)
}

fn without_field(base: &Value, field: &str) -> TestResult<Value> {
    let mut value = base.clone();
    value
        .as_object_mut()
        .ok_or_else(|| std::io::Error::other("lineage fixture must be an object"))?
        .remove(field);
    Ok(value)
}
