//! The served edition of the parcel source contract (root ADR-0067, ADR-0148).
//!
//! `vworld-parcel-source-objects.json` holds every cadastral edition the Bronze prefix carries,
//! each with its own objects and handoff prefix, and names the one the map and the catalog are
//! built from (`served_edition`). The readers here — the catalog projection load, the national
//! PostGIS mirror and the 시군구 crosswalk's cadastral 시도 — all read that one edition, and read it
//! through this view so the choice is made in one place: the shape they parsed before editions
//! (`load_granularity`, `handoff_prefix`, `handoff_suffix`, `granularity_counts`, `objects`),
//! filled from the served edition. Reading every edition's objects would put each parcel in once
//! per edition. Python and the shell loaders read the same file through
//! `infra/lakehouse/spark/jobs/vworld_parcel_editions.py`, which also checks it.

use anyhow::{bail, Context};
use serde_json::{json, Value};

/// The contract version this view reads.
pub const SCHEMA_VERSION: u64 = 2;

/// The served edition of the contract, in the per-edition reader shape.
///
/// # Errors
///
/// When the contract is not JSON, is not schema version 2, names no served edition or one it does
/// not hold, or the served edition lacks a field the readers need.
pub fn served_edition_view(contract_json: &str) -> anyhow::Result<Value> {
    let contract: Value =
        serde_json::from_str(contract_json).context("parcel source contract is not valid JSON")?;
    let version = contract.get("schema_version").and_then(Value::as_u64);
    if version != Some(SCHEMA_VERSION) {
        bail!(
            "parcel source contract schema_version {version:?} is not the {SCHEMA_VERSION} this command reads"
        );
    }
    let served = contract
        .get("served_edition")
        .and_then(Value::as_str)
        .context("parcel source contract must name its served_edition")?;
    let edition = contract
        .get("editions")
        .and_then(|editions| editions.get(served))
        .with_context(|| format!("parcel source contract has no served edition {served}"))?;
    let field = |from: &Value, name: &str| -> anyhow::Result<Value> {
        from.get(name)
            .cloned()
            .with_context(|| format!("parcel source contract must name {name}"))
    };
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "edition": served,
        "load_granularity": field(&contract, "load_granularity")?,
        "handoff_suffix": field(&contract, "handoff_suffix")?,
        "handoff_prefix": field(edition, "handoff_prefix")?,
        "granularity_counts": field(edition, "granularity_counts")?,
        "objects": field(edition, "objects")?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_EDITIONS: &str = r#"{
        "schema_version": 2, "load_granularity": "sigungu", "handoff_suffix": ".jsonl.gz",
        "served_edition": "209906",
        "editions": {
            "209906": {"handoff_prefix": "h/old", "granularity_counts": {"sido": 1, "sigungu": 1},
                       "objects": [{"object_key": "b/a.zip", "granularity": "sigungu"}]},
            "209909": {"handoff_prefix": "h/new", "granularity_counts": {"sido": 1, "sigungu": 1},
                       "objects": [{"object_key": "b/c.zip", "granularity": "sigungu"}]}
        }
    }"#;

    #[test]
    fn the_view_is_the_served_edition_only() -> anyhow::Result<()> {
        let view = served_edition_view(TWO_EDITIONS)?;
        assert_eq!(view["edition"], "209906");
        assert_eq!(view["handoff_prefix"], "h/old");
        assert_eq!(view["objects"].as_array().map(Vec::len), Some(1));
        assert_eq!(view["objects"][0]["object_key"], "b/a.zip");
        Ok(())
    }

    #[test]
    fn moving_the_served_edition_moves_every_reader() -> anyhow::Result<()> {
        let moved = TWO_EDITIONS.replace(
            r#""served_edition": "209906""#,
            r#""served_edition": "209909""#,
        );
        let view = served_edition_view(&moved)?;
        assert_eq!(view["handoff_prefix"], "h/new");
        assert_eq!(view["objects"][0]["object_key"], "b/c.zip");
        Ok(())
    }

    #[test]
    fn a_contract_without_its_served_edition_or_of_another_version_is_refused() {
        let missing = TWO_EDITIONS.replace(
            r#""served_edition": "209906""#,
            r#""served_edition": "209912""#,
        );
        let older = TWO_EDITIONS.replace(r#""schema_version": 2"#, r#""schema_version": 1"#);
        for (contract, says) in [
            (missing.as_str(), "no served edition 209912"),
            (older.as_str(), "is not the 2"),
        ] {
            let error = served_edition_view(contract)
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();
            assert!(error.contains(says), "{error}");
        }
    }
}
