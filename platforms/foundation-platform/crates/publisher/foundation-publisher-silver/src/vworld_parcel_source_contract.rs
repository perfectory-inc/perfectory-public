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
//!
//! The served edition is the last promotion of the contract's ledger, followed by the host's
//! promotions ledger where the host keeps one (root ADR-0490 §5, `host_promotions_ledger`): the
//! readiness judgement appends a granted promotion there, and it is served with no pull request.

use std::path::Path;

use anyhow::{bail, ensure, Context};
use serde_json::{json, Value};

/// The contract version this view reads.
pub const SCHEMA_VERSION: u64 = 2;
/// Names another host ledger than the contract's (the tests'), as in the Python reader.
pub const LEDGER_ENV: &str = "VWORLD_PARCEL_PROMOTIONS_LEDGER";

/// The served edition of the contract with the host's promotions ledger laid over it, in the
/// per-edition reader shape.
///
/// # Errors
///
/// When the contract is not JSON, is not schema version 2, names no served edition or one it does
/// not hold, the served edition lacks a field the readers need, or the host ledger cannot be read
/// or does not continue the contract's ledger.
pub fn served_edition_view(contract_json: &str) -> anyhow::Result<Value> {
    let contract = parse(contract_json)?;
    let ledger = host_ledger(&contract)?;
    view(&contract, ledger.as_deref())
}

/// [`served_edition_view`] over a given host ledger (`None`: the contract's own ledger only).
///
/// # Errors
///
/// As [`served_edition_view`].
pub fn served_edition_view_over(
    contract_json: &str,
    ledger: Option<&str>,
) -> anyhow::Result<Value> {
    view(&parse(contract_json)?, ledger)
}

fn parse(contract_json: &str) -> anyhow::Result<Value> {
    let contract: Value =
        serde_json::from_str(contract_json).context("parcel source contract is not valid JSON")?;
    let version = contract.get("schema_version").and_then(Value::as_u64);
    if version != Some(SCHEMA_VERSION) {
        bail!(
            "parcel source contract schema_version {version:?} is not the {SCHEMA_VERSION} this command reads"
        );
    }
    Ok(contract)
}

/// The host ledger's text: `$VWORLD_PARCEL_PROMOTIONS_LEDGER`, else the contract's host path or its
/// path inside the Spark containers, the first whose directory exists. No file there is no host
/// promotion yet; no such directory is not the host.
fn host_ledger(contract: &Value) -> anyhow::Result<Option<String>> {
    let declared = contract.get("host_promotions_ledger");
    let path = match std::env::var(LEDGER_ENV) {
        Ok(path) if !path.is_empty() => Some(path),
        _ => ["path", "container_path"].iter().find_map(|key| {
            declared
                .and_then(|declared| declared.get(key))
                .and_then(Value::as_str)
                .filter(|path| Path::new(path).parent().is_some_and(Path::is_dir))
                .map(str::to_owned)
        }),
    };
    match path {
        Some(path) if Path::new(&path).is_file() => std::fs::read_to_string(&path)
            .map(Some)
            .with_context(|| format!("cannot read the host promotions ledger {path}")),
        _ => Ok(None),
    }
}

fn promotion_key(entry: &Value) -> (String, String) {
    let text = |name: &str| {
        entry
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    (text("edition"), text("promoted_at_utc"))
}

/// The served edition: the contract's own when the host ledger adds nothing, else the last host
/// promotion. A host promotion already copied into the contract (same edition and instant) is the
/// contract's; each other must follow the one before it (`from_edition`, a later instant).
fn served(contract: &Value, ledger: Option<&str>) -> anyhow::Result<String> {
    let own = contract
        .get("served_edition")
        .and_then(Value::as_str)
        .context("parcel source contract must name its served_edition")?;
    let held: Vec<(String, String)> = contract
        .get("promotions")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().map(promotion_key).collect())
        .unwrap_or_default();
    let (mut served, mut at) = held
        .last()
        .cloned()
        .unwrap_or_else(|| (own.to_owned(), String::new()));
    ensure!(
        served == own,
        "parcel source contract serves {own}, not its last promotion's {served}"
    );
    for (number, line) in ledger.unwrap_or_default().lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: Value = serde_json::from_str(line).with_context(|| {
            format!(
                "host promotions ledger line {} is not a promotion",
                number + 1
            )
        })?;
        let (edition, promoted_at) = promotion_key(&entry);
        if held.contains(&(edition.clone(), promoted_at.clone())) {
            continue;
        }
        let from = entry.get("from_edition").and_then(Value::as_str);
        ensure!(
            from == Some(served.as_str()) && promoted_at > at,
            "host promotions ledger line {} ({edition}) does not follow the served {served}: the ledger only grows",
            number + 1
        );
        (served, at) = (edition, promoted_at);
    }
    Ok(served)
}

fn view(contract: &Value, ledger: Option<&str>) -> anyhow::Result<Value> {
    let served = served(contract, ledger)?;
    let served = served.as_str();
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
        "load_granularity": field(contract, "load_granularity")?,
        "handoff_suffix": field(contract, "handoff_suffix")?,
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

    const LEDGERED_FIRST: &str = r#""served_edition": "209906", "promotions": [{"edition": "209906",
        "from_edition": null, "promoted_at_utc": "2099-07-01T00:00:00Z"}],"#;
    const PROMOTED: &str = r#"{"edition": "209909", "from_edition": "209906", "promoted_at_utc": "2099-10-01T00:00:00Z"}"#;

    fn ledgered() -> String {
        TWO_EDITIONS.replace(r#""served_edition": "209906","#, LEDGERED_FIRST)
    }

    /// Root ADR-0490 §5: a granted promotion on the host's ledger is served with no pull request.
    /// 2026-10-21..11-11 a grant could only become a contract file a person had to open a pull
    /// request for, and none was ever opened.
    #[test]
    fn the_host_ledger_moves_every_reader() -> anyhow::Result<()> {
        let contract = ledgered();
        let view = served_edition_view_over(&contract, Some(&format!("{PROMOTED}\n")))?;
        assert_eq!(view["edition"], "209909");
        assert_eq!(view["handoff_prefix"], "h/new");
        assert_eq!(
            served_edition_view_over(&contract, None)?["edition"],
            "209906"
        );
        // A rollback returns to the edition the promotion left (ADR-0490 §7).
        let back = r#"{"edition": "209906", "from_edition": "209909", "promoted_at_utc": "2099-10-02T00:00:00Z"}"#;
        let rolled = served_edition_view_over(&contract, Some(&format!("{PROMOTED}\n{back}\n")))?;
        assert_eq!(rolled["edition"], "209906");
        // A copy of the host promotion in the contract is not counted twice.
        let copied = contract
            .replace(
                r#""served_edition": "209906","#,
                r#""served_edition": "209909","#,
            )
            .replace(
                r#""promoted_at_utc": "2099-07-01T00:00:00Z"}]"#,
                &format!(r#""promoted_at_utc": "2099-07-01T00:00:00Z"}}, {PROMOTED}]"#),
            );
        assert_eq!(
            served_edition_view_over(&copied, Some(PROMOTED))?["edition"],
            "209909"
        );
        Ok(())
    }

    #[test]
    fn a_host_ledger_that_does_not_continue_the_contract_is_refused() {
        let contract = ledgered();
        for line in [
            r#"{"edition": "209909", "from_edition": "209909", "promoted_at_utc": "2099-10-01T00:00:00Z"}"#,
            r#"{"edition": "209909", "from_edition": "209906", "promoted_at_utc": "2099-01-01T00:00:00Z"}"#,
            "{",
        ] {
            assert!(
                served_edition_view_over(&contract, Some(line)).is_err(),
                "{line}"
            );
        }
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
