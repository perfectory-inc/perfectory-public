//! Which Bronze objects a Silver load still owes, decided from R2 alone (root ADR-0280).
//!
//! The record of what a table has loaded is the table itself: every `append_batch_once` commit
//! lists its Bronze objects in the snapshot summary (`foundation.ingest-batch-objects`), and the
//! catalog answers them without Spark (`load_ingested_batch_objects`, as the Silver refresh lanes
//! ask, root ADR-0169 §3). What exists to load is the Bronze prefix of each source. So a load needs
//! no queue beside them: it lists the prefixes, takes away what the table records, and fetches the
//! rest into a directory of its own run, which the Spark load reads and the script removes.
//!
//! Two bounds. The horizon is how far back a table's record can be believed: an expired snapshot
//! takes its record with it, so only objects landed within `lakehouse-maintenance.contract.json`'s
//! `retain_days` (less a margin) are compared at all; an older one would read as unloaded and be
//! appended again. The window is how late an object may arrive in a healthy pipeline. An
//! unrecorded object inside the horizon but older than the window is still owed and loaded, and is
//! named `late` in the plan: the load script fails the run after loading it, so a load that had
//! been failing for longer than the window cannot pass unseen (a check that sees nothing happening
//! must not read as green).

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Duration, Utc};
use foundation_outbox::object_storage::{R2InventoryRequest, R2ObjectStorageConfig};
use foundation_outbox::R2ObjectStorage;
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};

use crate::public_data_control_support::{required_env_value, write_json_file};

const PREFIX: &str = "FOUNDATION_PLATFORM_SILVER_LOAD";
const MAINTENANCE_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/lakehouse-maintenance.contract.json");
const LIST_PAGE: i32 = 1000;
/// Days kept between the horizon and the snapshot retention, so a record about to expire is not
/// read as an answer.
const HORIZON_MARGIN_DAYS: i64 = 2;

/// One Bronze object as R2 lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Listed {
    pub(crate) key: String,
    pub(crate) size_bytes: i64,
    pub(crate) last_modified: DateTime<Utc>,
}

/// What a load owes, and what it saw.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct Selection {
    pub(crate) owed: Vec<String>,
    pub(crate) listed: usize,
    pub(crate) recorded: usize,
    pub(crate) beyond_bound: usize,
    /// Owed objects older than the window: loaded, and the run fails after loading them.
    pub(crate) late: Vec<String>,
    /// Objects older than the horizon, whose record may have expired: neither compared nor loaded.
    pub(crate) beyond_horizon: usize,
}

/// The snapshot retention the window must stay under.
pub(crate) fn snapshot_retain_days() -> anyhow::Result<i64> {
    let contract: JsonValue = serde_json::from_str(MAINTENANCE_CONTRACT)
        .context("lakehouse-maintenance.contract.json is not valid")?;
    find_retain_days(&contract).context("lakehouse-maintenance.contract.json names no retain_days")
}

fn find_retain_days(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Object(map) => map
            .get("retain_days")
            .and_then(JsonValue::as_i64)
            .or_else(|| map.values().find_map(find_retain_days)),
        JsonValue::Array(items) => items.iter().find_map(find_retain_days),
        _ => None,
    }
}

/// Unrecorded objects inside the horizon, oldest first, at most `max_objects` of them; those older
/// than the window are also named late.
pub(crate) fn select(
    listed: &[Listed],
    recorded: &BTreeSet<String>,
    now: DateTime<Utc>,
    window_days: i64,
    horizon_days: i64,
    max_objects: usize,
) -> Selection {
    let late_before = now - Duration::days(window_days);
    let horizon = now - Duration::days(horizon_days);
    let mut owed = Vec::new();
    let mut selection = Selection {
        listed: listed.len(),
        ..Selection::default()
    };
    for object in listed {
        if object.last_modified < horizon {
            selection.beyond_horizon += 1;
        } else if recorded.contains(&object.key) {
            selection.recorded += 1;
        } else {
            owed.push(object);
        }
    }
    owed.sort_by(|a, b| (a.last_modified, &a.key).cmp(&(b.last_modified, &b.key)));
    selection.beyond_bound = owed.len().saturating_sub(max_objects);
    for object in owed.into_iter().take(max_objects) {
        if object.last_modified < late_before {
            selection.late.push(object.key.clone());
        }
        selection.owed.push(object.key.clone());
    }
    selection
}

/// `bronze/source=<slug>/<leaf>` → `<slug>`.
pub(crate) fn source_of(key: &str) -> anyhow::Result<&str> {
    key.strip_prefix("bronze/source=")
        .and_then(|rest| rest.split_once('/'))
        .map(|(slug, _)| slug)
        .filter(|slug| {
            !slug.is_empty()
                && slug
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
        .with_context(|| format!("{key} is not a Bronze object key"))
}

fn local_path(output_dir: &Path, key: &str) -> anyhow::Result<PathBuf> {
    let leaf = key.rsplit('/').next().unwrap_or_default();
    ensure!(
        !leaf.is_empty()
            && leaf
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')),
        "{key} has an unexpected leaf"
    );
    Ok(output_dir.join("objects").join(source_of(key)?).join(leaf))
}

fn parse_modified(key: &str, value: Option<&str>) -> anyhow::Result<DateTime<Utc>> {
    let text = value.with_context(|| format!("R2 lists {key} without a modification time"))?;
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .with_context(|| format!("R2 lists {key} with an unreadable modification time {text:?}"))
}

/// Lists the sources, asks the table, fetches what it owes and writes `plan.json`.
///
/// # Errors
/// Returns an error when the configuration is invalid, the window is not shorter than the
/// snapshot retention, R2 or the catalog cannot be read, or a file cannot be written.
pub async fn run() -> anyhow::Result<()> {
    let table = required_env_value(&format!("{PREFIX}_TABLE"))?;
    let slugs = required_env_value(&format!("{PREFIX}_SOURCE_SLUGS"))?
        .split(',')
        .map(str::trim)
        .filter(|slug| !slug.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    ensure!(!slugs.is_empty(), "{PREFIX}_SOURCE_SLUGS names no source");
    let window_days: i64 = required_env_value(&format!("{PREFIX}_WINDOW_DAYS"))?
        .trim()
        .parse()
        .with_context(|| format!("{PREFIX}_WINDOW_DAYS must be a whole number of days"))?;
    let max_objects: usize = required_env_value(&format!("{PREFIX}_MAX_OBJECTS"))?
        .trim()
        .parse()
        .with_context(|| format!("{PREFIX}_MAX_OBJECTS must be a whole number"))?;
    let output_dir = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT_DIR"))?);
    let horizon_days = snapshot_retain_days()? - HORIZON_MARGIN_DAYS;
    if window_days < 1 || window_days >= horizon_days {
        bail!(
            "{PREFIX}_WINDOW_DAYS is {window_days}; it must be at least 1 and shorter than the horizon ({horizon_days} days: the snapshot retention less {HORIZON_MARGIN_DAYS}), past which a load record may have expired"
        );
    }

    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::lakehouse_reader_from_env()
            .context("a Silver load plan reads R2 with the lakehouse reader key")?,
    );
    let mut listed = Vec::new();
    for slug in &slugs {
        let prefix = format!("bronze/source={slug}/");
        let report = storage
            .inventory_audit(R2InventoryRequest::new(Some(&prefix), Some(LIST_PAGE))?)
            .await
            .with_context(|| format!("failed to list {prefix}"))?;
        for object in report.objects() {
            listed.push(Listed {
                key: object.key.clone(),
                size_bytes: object.size_bytes,
                last_modified: parse_modified(&object.key, object.last_modified.as_deref())?,
            });
        }
    }
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let recorded = catalog
        .load_ingested_batch_objects(&table)
        .await?
        .unwrap_or_default();
    let selection = select(
        &listed,
        &recorded,
        Utc::now(),
        window_days,
        horizon_days,
        max_objects,
    );

    let mut objects = Vec::new();
    for key in &selection.owed {
        let path = local_path(&output_dir, key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let bytes = storage
            .get_object_bytes(key)
            .await
            .with_context(|| format!("failed to read {key}"))?;
        std::fs::write(&path, &bytes)
            .with_context(|| format!("failed to write {}", path.display()))?;
        // When R2 says the object landed: the edition of a source whose key names no run (a
        // content-qualified export, root ADR-0348 §5).
        let landed = listed
            .iter()
            .find(|object| &object.key == key)
            .map(|object| object.last_modified.to_rfc3339());
        objects.push(json!({
            "object_key": key,
            "source_slug": source_of(key)?,
            "last_modified": landed,
            "local_path": path.strip_prefix(&output_dir).unwrap_or(&path).to_string_lossy().replace('\\', "/"),
            "size_bytes": bytes.len(),
        }));
    }
    let plan = json!({
        "schema_version": "foundation-platform.silver_load_objects.v1",
        "table": table,
        "window_days": window_days,
        "horizon_days": horizon_days,
        "selection": selection,
        "objects": objects,
    });
    write_json_file(&output_dir.join("plan.json"), &plan)?;
    println!(
        "silver-load-plan {}",
        json!({"table": plan["table"], "owed": objects.len(), "listed": selection.listed,
               "recorded": selection.recorded, "beyond_bound": selection.beyond_bound,
               "late": selection.late.len(), "beyond_horizon": selection.beyond_horizon})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{select, snapshot_retain_days, source_of, Listed};
    use chrono::{DateTime, Utc};
    use std::collections::BTreeSet;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_default()
    }

    fn object(leaf: &str, when: &str) -> Listed {
        Listed {
            key: format!("bronze/source=datagokr__onbid_real_estate_item/{leaf}"),
            size_bytes: 1,
            last_modified: at(when),
        }
    }

    #[test]
    fn a_load_owes_the_unrecorded_objects_inside_the_horizon_oldest_first() {
        let listed = [
            object("c.gz", "2099-01-10T03:00:00Z"),
            object("a.gz", "2099-01-10T00:00:00Z"),
            object("b.gz", "2099-01-09T00:00:00Z"),
            object("late.gz", "2098-12-25T00:00:00Z"),
            object("expired.gz", "2098-11-01T00:00:00Z"),
        ];
        let recorded = BTreeSet::from([listed[1].key.clone()]);
        let selection = select(&listed, &recorded, at("2099-01-10T06:00:00Z"), 7, 28, 10);
        let leaves = |keys: &[String]| {
            keys.iter()
                .map(|key| key.rsplit('/').next().unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(leaves(&selection.owed), ["late.gz", "b.gz", "c.gz"]);
        assert_eq!(
            leaves(&selection.late),
            ["late.gz"],
            "a late object is loaded and named, so the run fails after it"
        );
        assert_eq!(
            (
                selection.listed,
                selection.recorded,
                selection.beyond_horizon
            ),
            (5, 1, 1),
            "an object past the horizon may have lost its record; it is not compared"
        );
        let bounded = select(&listed, &recorded, at("2099-01-10T06:00:00Z"), 7, 28, 1);
        assert_eq!((bounded.owed.len(), bounded.beyond_bound), (1, 2));
    }

    #[test]
    fn the_horizon_stays_inside_the_snapshot_retention() -> anyhow::Result<()> {
        // An expired snapshot takes its load record with it; a horizon that reached past the
        // retention would read a loaded object as owed and append it again.
        assert!(snapshot_retain_days()? - super::HORIZON_MARGIN_DAYS > 7);
        Ok(())
    }

    #[test]
    fn only_a_bronze_key_names_a_source() -> anyhow::Result<()> {
        assert_eq!(
            source_of("bronze/source=datagokr__onbid_real_estate_item/x.gz")?,
            "datagokr__onbid_real_estate_item"
        );
        assert!(source_of("silver/x.gz").is_err());
        assert!(source_of("bronze/source=../x.gz").is_err());
        Ok(())
    }
}
