//! Provider-neutral Lakehouse lineage event contract.

use serde_json::Value;
use thiserror::Error;

/// Current Lakehouse lineage event schema version.
pub const LAKEHOUSE_LINEAGE_EVENT_SCHEMA_VERSION: &str =
    "foundation-platform.lakehouse_lineage_event.v1";

/// Current Lakehouse materialization event type.
pub const LAKEHOUSE_LINEAGE_EVENT_TYPE: &str = "lakehouse.lineage.dataset_materialized.v1";

/// Error raised when a Lakehouse lineage event violates the contract.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct LakehouseLineageEventError {
    message: String,
}

impl LakehouseLineageEventError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Validates one provider-neutral Lakehouse lineage event without network I/O.
///
/// # Errors
/// Returns `LakehouseLineageEventError` when required identity, source lineage,
/// quality, column lineage, or `OpenLineage` mapping fields are invalid.
pub fn validate_lakehouse_lineage_event(event: &Value) -> Result<(), LakehouseLineageEventError> {
    assert_string_eq(
        event,
        "schema_version",
        LAKEHOUSE_LINEAGE_EVENT_SCHEMA_VERSION,
    )?;
    assert_string_eq(event, "event_type", LAKEHOUSE_LINEAGE_EVENT_TYPE)?;
    assert_string_eq(
        event,
        "run_summary_schema_version",
        crate::SPARK_RUN_SUMMARY_SCHEMA_VERSION,
    )?;
    assert_non_blank(event, "producer")?;
    assert_non_blank(event, "job_name")?;
    assert_non_blank(event, "run_id")?;
    assert_nested_non_blank(event, "input_dataset", "qualified_name")?;
    assert_nested_non_blank(event, "output_dataset", "qualified_name")?;
    assert_source_snapshots(event)?;
    assert_source_lineage_not_truncated(event)?;
    assert_positive_quality_metric(event, "row_count")?;
    assert_column_lineage(event)?;
    assert_openlineage_mapping(event)?;
    Ok(())
}

fn assert_string_eq(
    event: &Value,
    field: &str,
    expected: &str,
) -> Result<(), LakehouseLineageEventError> {
    if json_string(event, field) == Some(expected) {
        return Ok(());
    }
    Err(LakehouseLineageEventError::new(format!(
        "lineage event {field} mismatch"
    )))
}

fn assert_non_blank(event: &Value, field: &str) -> Result<(), LakehouseLineageEventError> {
    if json_string(event, field).is_some_and(|value| !value.trim().is_empty()) {
        return Ok(());
    }
    Err(LakehouseLineageEventError::new(format!(
        "lineage event missing required field: {field}"
    )))
}

fn assert_nested_non_blank(
    event: &Value,
    parent: &str,
    field: &str,
) -> Result<(), LakehouseLineageEventError> {
    let value = event
        .get(parent)
        .and_then(|nested| nested.get(field))
        .and_then(Value::as_str);
    if value.is_some_and(|text| !text.trim().is_empty()) {
        return Ok(());
    }
    Err(LakehouseLineageEventError::new(format!(
        "lineage event missing required field: {parent}.{field}"
    )))
}

fn assert_source_snapshots(event: &Value) -> Result<(), LakehouseLineageEventError> {
    let snapshots = event
        .get("source_snapshot_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LakehouseLineageEventError::new("lineage event source_snapshot_ids must not be empty")
        })?;
    if snapshots.is_empty() {
        return Err(LakehouseLineageEventError::new(
            "lineage event source_snapshot_ids must not be empty",
        ));
    }
    if snapshots.iter().any(|snapshot| {
        snapshot
            .as_str()
            .is_none_or(|value| value.trim().is_empty())
    }) {
        return Err(LakehouseLineageEventError::new(
            "lineage event source_snapshot_ids includes blank value",
        ));
    }
    Ok(())
}

fn assert_source_lineage_not_truncated(event: &Value) -> Result<(), LakehouseLineageEventError> {
    if event
        .get("source_snapshot_truncated")
        .and_then(Value::as_bool)
        == Some(false)
    {
        return Ok(());
    }
    Err(LakehouseLineageEventError::new(
        "lineage event source_snapshot_truncated must be false",
    ))
}

fn assert_positive_quality_metric(
    event: &Value,
    metric: &str,
) -> Result<(), LakehouseLineageEventError> {
    let value = event
        .get("quality_metrics")
        .and_then(|metrics| metrics.get(metric))
        .and_then(Value::as_i64);
    if value.is_some_and(|count| count > 0) {
        return Ok(());
    }
    Err(LakehouseLineageEventError::new(format!(
        "lineage event quality_metrics.{metric} must be positive"
    )))
}

fn assert_column_lineage(event: &Value) -> Result<(), LakehouseLineageEventError> {
    let entries = event
        .get("column_lineage")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LakehouseLineageEventError::new("lineage event column_lineage must not be empty")
        })?;
    if entries.is_empty() {
        return Err(LakehouseLineageEventError::new(
            "lineage event column_lineage must not be empty",
        ));
    }
    for entry in entries {
        if entry
            .get("output_column")
            .and_then(Value::as_str)
            .is_none_or(|column| column.trim().is_empty())
        {
            return Err(LakehouseLineageEventError::new(
                "lineage event missing required field: column_lineage.output_column",
            ));
        }
        if entry
            .get("inputs")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
        {
            return Err(LakehouseLineageEventError::new(
                "lineage event column_lineage.inputs must not be empty",
            ));
        }
    }
    Ok(())
}

fn assert_openlineage_mapping(event: &Value) -> Result<(), LakehouseLineageEventError> {
    let mapping = event.get("openlineage_mapping").ok_or_else(|| {
        LakehouseLineageEventError::new(
            "lineage event missing required field: openlineage_mapping.event_type",
        )
    })?;
    if mapping.get("event_type").and_then(Value::as_str) != Some("COMPLETE") {
        return Err(LakehouseLineageEventError::new(
            "lineage event openlineage_mapping.event_type must be COMPLETE",
        ));
    }
    for field in [
        "job_namespace",
        "job_name",
        "input_namespace",
        "output_namespace",
    ] {
        if mapping
            .get(field)
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(LakehouseLineageEventError::new(format!(
                "lineage event missing required field: openlineage_mapping.{field}"
            )));
        }
    }
    Ok(())
}

fn json_string<'a>(event: &'a Value, field: &str) -> Option<&'a str> {
    event.get(field).and_then(Value::as_str)
}
