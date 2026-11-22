//! The FLOOR history gate's outcome, as the cycle accepts it (root ADR-0338): the gate is
//! `building_register_floor_silver_export::history_gate_run`.
use anyhow::{ensure, Context};
use serde_json::Value;

const HISTORY_SCHEMA: &str = "foundation-platform.floor-history-check.v1";

pub(super) fn retained_outcome(
    raw: &str,
    source: &super::floor_source::FloorSource,
) -> anyhow::Result<Option<u64>> {
    let value: Value = serde_json::from_str(raw)?;
    if value.get("schema_version").and_then(Value::as_str) != Some(HISTORY_SCHEMA) {
        return Ok(None);
    }
    let inputs = &value["inputs"];
    for (role, name, slug) in [
        (
            "floor",
            &source.floor,
            crate::building_register_source_role::SourceRole::Floor.slug(),
        ),
        (
            "title",
            &source.title,
            crate::building_register_source_role::SourceRole::Title.slug(),
        ),
    ] {
        ensure!(
            inputs[role]["provider_file_id"].as_str() == name.strip_suffix(".zip")
                && inputs[role]["role_slug"] == slug,
            "retained FLOOR outcome belongs to different inputs"
        );
    }
    ensure!(
        value.get("derivation") == Some(&serde_json::json!(source.derivation)),
        "retained FLOOR derivation differs"
    );
    let identity = crate::building_register_floor_silver_export::source_identity_from_semantic(
        inputs.clone(),
        &source.history,
    )?;
    ensure!(
        value["source_snapshot_id"] == identity,
        "retained FLOOR content identity differs"
    );
    let witness = source.history.value()?;
    if value["retention_kind"] == "registered_append" {
        return registered_outcome(&value, &witness).map(Some);
    }
    ensure!(
        value.get("retention_kind").is_none() && source.derivation.is_none(),
        "historical base cannot authenticate a derivation"
    );
    ensure!(
        value["action"] == "already_retained"
            && value["persisted_row_count"].as_u64() == Some(0)
            && [
                "source_snapshot_id",
                "table_uuid",
                "snapshot_id",
                "retained_row_count"
            ]
            .iter()
            .all(|key| {
                let witness_key = if *key == "retained_row_count" {
                    "row_count"
                } else {
                    key
                };
                value[*key] == witness[witness_key]
            }),
        "invalid historical FLOOR retention outcome"
    );
    let head = value["observed_head_snapshot_id"]
        .as_str()
        .context("historical FLOOR outcome has no observed head")?;
    ensure!(
        head.parse::<u64>().is_ok_and(|id| id > 0),
        "invalid observed Iceberg head"
    );
    Ok(Some(
        witness["row_count"]
            .as_u64()
            .context("missing retained row count")?,
    ))
}

fn registered_outcome(value: &Value, witness: &Value) -> anyhow::Result<u64> {
    let source = value["source_snapshot_id"]
        .as_str()
        .context("missing retained FLOOR source")?;
    let new_source = source
        .strip_prefix("building-register-floor-content-v1-")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    ensure!(
        value["action"] == "already_retained"
            && value["persisted_row_count"].as_u64() == Some(0)
            && value["table_uuid"] == witness["table_uuid"]
            && (new_source || value["source_snapshot_id"] == witness["source_snapshot_id"]),
        "invalid registered FLOOR retention outcome"
    );
    for key in ["snapshot_id", "observed_head_snapshot_id"] {
        ensure!(
            value[key]
                .as_str()
                .is_some_and(|id| id.parse::<u64>().is_ok_and(|id| id > 0)),
            "invalid retained FLOOR snapshot"
        );
    }
    chrono::DateTime::parse_from_rfc3339(
        value["retained_ingested_at_utc"]
            .as_str()
            .context("missing retained FLOOR time")?,
    )?;
    let rows = value["retained_row_count"]
        .as_u64()
        .context("missing retained FLOOR rows")?;
    ensure!(rows > 0, "empty retained FLOOR append");
    Ok(rows)
}
