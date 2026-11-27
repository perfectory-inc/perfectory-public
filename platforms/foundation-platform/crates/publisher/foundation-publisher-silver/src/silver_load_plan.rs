//! The plan `plan-silver-load-objects` writes, as every Silver stage reads it (root ADR-0280, amended
//! 2026-11-28 by root ADR-0626 §4).
//!
//! The planner takes each owed object from the Bronze ledger, so an entry carries what the ledger
//! knows of it: `collected_at`, when its collector's run collected it, and `landed_at`, when its row
//! was written. A stage reads a time from the entry, never from the key: a key names a provider file
//! and its bytes, and carries no run stamp (root ADR-0626 rule 5, G5).

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::Value;

/// The plan's format.
pub const PLAN_SCHEMA: &str = "foundation-platform.silver_load_objects.v1";

/// The format of a `source_snapshot_id`: the run's start to the second, in UTC.
const SNAPSHOT_FORMAT: &str = "%Y%m%dT%H%M%SZ";

fn instant(entry: &Value, field: &str) -> anyhow::Result<DateTime<Utc>> {
    let key = entry["object_key"].as_str().unwrap_or("a planned object");
    let text = entry[field]
        .as_str()
        .with_context(|| format!("{key}: the plan carries no {field}"))?;
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .with_context(|| format!("{key}: {field} {text:?} is not an instant"))
}

/// The `source_snapshot_id` of a planned object: its ledger row's `collected_at`, the run's start
/// (`20990102T030405Z`). A collector's run gives all of its objects this one value.
///
/// # Errors
/// An entry without a readable `collected_at`.
pub fn snapshot_id(entry: &Value) -> anyhow::Result<String> {
    Ok(instant(entry, "collected_at")?
        .format(SNAPSHOT_FORMAT)
        .to_string())
}

/// When a planned object landed: its ledger row's creation, which a re-key keeps (root ADR-0626 §4).
///
/// # Errors
/// An entry without a readable `landed_at`.
pub fn landed_at(entry: &Value) -> anyhow::Result<DateTime<Utc>> {
    instant(entry, "landed_at")
}

#[cfg(test)]
mod tests {
    use super::{landed_at, snapshot_id};
    use serde_json::json;

    #[test]
    fn the_snapshot_is_the_ledgers_collected_at_never_the_key() -> anyhow::Result<()> {
        let entry = json!({
            "object_key": "bronze/source=datagokr__onbid_real_estate_item/item-list--sha256-00.gz",
            "collected_at": "2099-01-02T03:04:05.678+00:00",
            "landed_at": "2099-01-02T03:09:00Z",
        });
        assert_eq!(snapshot_id(&entry)?, "20990102T030405Z");
        assert_eq!(landed_at(&entry)?.to_rfc3339(), "2099-01-02T03:09:00+00:00");
        assert!(snapshot_id(&json!({"object_key": "x"})).is_err());
        assert!(snapshot_id(&json!({"collected_at": "yesterday"})).is_err());
        Ok(())
    }
}
