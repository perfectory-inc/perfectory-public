//! `build-parcel-real-transaction-versions`: the 실거래 kind's Gold, `gold.parcel_real_transaction_versions`
//! (root ADR-0350, ADR-0371, ADR-0564 §6). `DuckDB` inside the publisher computes it
//! (`foundation_publisher_silver::real_transaction_versions_stage`) and the publisher appends the keys
//! that changed (`gold_versions_commit`). It replaces the Spark job `real_transaction_silver_to_gold.py`.
//!
//! While the lane serves `gold.parcel_real_transaction`, this table is its shadow: its inputs are
//! pinned to that table's build-tag commit, so the takeover check compares two engines over one set
//! of inputs, and its first build holds every key. One run: create the table if it is not there,
//! plan, mend the tags, and stop when the inputs, the served edition and the code (with the build
//! month every document names) are what the base commit was built from. Otherwise spool every input
//! once, build every unit, diff against the head's current rows, refuse a build that loses or changes
//! more than its bounds, and append.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use chrono::{Datelike, NaiveDate};
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    parcel_nearby_stage::{NearbyPins, Source},
    process_memory,
    real_transaction_gold_stage::history_coverage,
    real_transaction_placement_stage::PARCEL_SOURCE,
    real_transaction_versions_stage::{
        build_unit, code_version, spool, UnitBuild, BUILT_TABLE, INPUTS,
    },
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, RECORDED_EXPLANATION_PROPERTY,
        SERVED_EDITION_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    parcel_nearby_versions_build::{
        contract, lakehouse_read, max_row_loss_fraction, recorded_or_refused, served_edition,
        write_summary, MEMORY_SHARE,
    },
    public_data_control_support::required_env_value,
};

/// The table this command writes.
const TABLE: &str = "gold.parcel_real_transaction_versions";
/// Its by-PNU lane, whose served Gold says whether this run is a shadow (root ADR-0564 §6).
const LANE: &str = "parcel-real-transaction";
/// The table whose columns it carries and whose build a shadow follows.
const REPLACED: &str = "gold.parcel_real_transaction";
const JOB: &str = "build-parcel-real-transaction-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
const RT_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/rt-molit-real-transaction.contract.json");

/// The build's code version: the rules' and the build month's, which every document names
/// (`as_of_month`, and the windows it opens), so a new month rebuilds every PNU as a code change does.
fn versioned(rules: &str, as_of: NaiveDate) -> String {
    format!("{rules}@{:04}-{:02}", as_of.year(), as_of.month())
}

/// Why a commit whose code version moved changed every PNU: the rules, or only the build month.
fn code_moved(base: &str, code: &str) -> Option<String> {
    if base == code {
        return None;
    }
    let (base_rules, base_month) = base.split_once('@').unwrap_or((base, ""));
    let (rules, month) = code.split_once('@').unwrap_or((code, ""));
    Some(if base_rules == rules {
        format!("the build month moved {base_month} -> {month}")
    } else {
        format!("the code version moved to {rules}")
    })
}

/// Builds and appends the 실거래 Gold's changed keys.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let now = chrono::Utc::now();
    let published_at_utc = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let as_of = now
        .date_naive()
        .with_day(1)
        .context("a month has a first day")?;
    let rt_contract: Value = serde_json::from_str(RT_CONTRACT)?;
    let inference = &rt_contract["place_inference"];
    let versions_contract = contract(TABLE)?;
    let content_columns: Vec<&'static str> = contract(REPLACED)?
        .columns
        .iter()
        .map(|column| column.name)
        .collect();
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let created = committer.create_table_if_absent(versions_contract).await?;

    // Plan.
    let history = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist after its creation"))?;
    let base = versions::base(&history)?;
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let lane =
        ByPnuLane::named(LANE)?.with_context(|| format!("the contract names no {LANE} lane"))?;
    let versions::Planned {
        pins,
        served_edition: edition,
        shadow,
    } = versions::planned_inputs(
        &catalog,
        TABLE,
        by_pnu_serving_manifest_publish::gold_table(lane),
        &INPUTS,
        served_edition()?,
    )
    .await?;
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = versioned(&code_version(inference), as_of);
    if let Some(base) = &base {
        versions::pin(&catalog, TABLE, base.snapshot_id, &base.pins).await?;
    }
    let moved = base
        .as_ref()
        .map_or_else(BTreeSet::new, |base| versions::moved(base, &pins));
    let unchanged = base.as_ref().is_some_and(|base| {
        moved.is_empty()
            && base.code_version == code
            && base.served_edition.as_deref() == Some(edition.as_str())
    });
    if unchanged {
        write_summary(
            &summary_path,
            &json!({
                "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "unchanged",
                "pins": pins, "served_edition": edition, "shadow_of": shadow_build,
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {TABLE} nothing to do: its inputs, edition and code are the base commit's");
        return Ok(());
    }
    // A moved code version (the rules or the build month) or served edition explains a change of
    // any size (root ADR-0346); the commit records which.
    let moved_code_or_edition = base.as_ref().and_then(|base| {
        code_moved(&base.code_version, &code).or_else(|| {
            (base.served_edition.as_deref() != Some(edition.as_str())).then(|| {
                format!(
                    "the served parcel edition moved {} -> {edition}",
                    base.served_edition.as_deref().unwrap_or("-")
                )
            })
        })
    });

    // Compute: every input once into the spool, then every unit.
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = stage.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    let connection = stage.connection();
    let spool_dir = work.join(".rt-spool");
    if spool_dir.exists() {
        std::fs::remove_dir_all(&spool_dir)
            .with_context(|| format!("cannot clear {}", spool_dir.display()))?;
    }
    std::fs::create_dir_all(&spool_dir)?;
    let rt_pins = NearbyPins(pins.clone());
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &rt_pins,
    };
    let spooled = spool(connection, &source, &edition, &spool_dir)?;
    for (input, millis) in &spooled.read_millis {
        println!(
            "gold-versions {TABLE} read {input} in {:.1}s",
            *millis as f64 / 1000.0
        );
    }
    println!(
        "gold-versions {TABLE} spooled {} deals ({} unaddressed) and {} served parcels into {} units \
         in {:.1}s (memory peak {:?} bytes)",
        spooled.deals,
        spooled.unaddressed,
        spooled.served_parcels,
        spooled.units.len(),
        started.elapsed().as_secs_f64(),
        process_memory::cgroup_peak_bytes()
    );
    let first_months: BTreeMap<String, String> = rt_contract["datasets"]
        .as_object()
        .map(|datasets| {
            datasets
                .iter()
                .filter_map(|(slug, dataset)| {
                    Some((slug.clone(), dataset["first_month"].as_str()?.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default();
    let recent = u32::try_from(
        rt_contract["recent_history_months"]
            .as_u64()
            .context("the 실거래 contract names no recent_history_months")?,
    )?;
    let coverage = history_coverage(
        spooled
            .months
            .iter()
            .map(|(dataset, month)| (dataset.as_str(), month.as_str())),
        &first_months,
        as_of,
        recent,
    )?;
    let parcels_snapshot = pins
        .get(PARCEL_SOURCE)
        .context("the parcels are pinned")?
        .to_string();
    connection.execute_batch(BUILT_TABLE)?;
    let build = UnitBuild {
        inference,
        as_of,
        coverage: &coverage,
        source_snapshot_id: &parcels_snapshot,
        published_at_utc: &published_at_utc,
    };
    let mut seconds_by_unit = Vec::new();
    for (index, unit) in spooled.units.iter().enumerate() {
        let at = Instant::now();
        let rows = build_unit(connection, &spool_dir, unit, &build)?;
        seconds_by_unit.push(at.elapsed().as_secs_f64());
        println!(
            "gold-versions {TABLE} built unit {}/{} ({} 시군구, {} deals) into {rows} rows in {:.1}s \
             (memory peak {:?} bytes)",
            index + 1,
            spooled.units.len(),
            unit.nodes.len(),
            unit.deals,
            at.elapsed().as_secs_f64(),
            process_memory::cgroup_peak_bytes()
        );
    }
    std::fs::remove_dir_all(&spool_dir)
        .with_context(|| format!("cannot remove {}", spool_dir.display()))?;
    let invalid: i64 = connection.query_row(
        "SELECT count(*) FROM built WHERE NOT regexp_full_match(pnu, '[0-9]{19}') \
         OR real_transaction_json IS NULL OR row_digest IS NULL",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        invalid == 0,
        "{invalid} built rows have a malformed PNU, no document or no digest: nothing was appended"
    );

    // Diff and check: every unit was built, so the diff reads every key.
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table: TABLE,
        versions: versions_contract,
        content_columns,
        key: "pnu",
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: parcels_snapshot.clone(),
        published_at_utc: published_at_utc.clone(),
    };
    let diff = versions::diff(connection, &commit, "TRUE")?;
    let (fingerprint, current_records) = versions::after(connection, &commit, &diff)?;
    let mut recorded_explanation = None;
    if let Some(base) = &base {
        let max_loss = max_row_loss_fraction()?;
        let floor = (1.0 - max_loss) * base.current_records as f64;
        if (current_records as f64) < floor {
            bail!(
                "{TABLE} would hold {current_records} current rows, fewer than the {floor:.0} its \
                 {} allow (gold-panel-rebuild.contract.json kind_tables.max_row_loss_fraction): \
                 nothing was appended",
                base.current_records
            );
        }
        let fraction = diff.keys() as f64 / (base.current_records.max(1)) as f64;
        recorded_explanation =
            recorded_or_refused(fraction, moved_code_or_edition, None).map_err(|why| {
                anyhow::anyhow!(
                    "{} of {TABLE}'s {} current rows changed: {why}: nothing was appended \
                     (root ADR-0564 §3.4)",
                    diff.keys(),
                    base.current_records
                )
            })?;
        if let Some(why) = &recorded_explanation {
            println!(
                "gold-versions {TABLE} {fraction:.3} of its current rows changed, recorded: {why}"
            );
        }
    }

    // Commit.
    let run = versions::run_id(TABLE, &pins, &code, &edition);
    let shape = committer.table_shape(TABLE).await?;
    let files = versions::stage(&stage, &shape, &run, "pnu")?;
    let mut summary = BTreeMap::from([
        (VERSION_SEQ_PROPERTY.to_owned(), version_seq.to_string()),
        (
            CURRENT_RECORDS_PROPERTY.to_owned(),
            current_records.to_string(),
        ),
        (
            FINGERPRINT_PROPERTY.to_owned(),
            versions::fingerprint_text(fingerprint),
        ),
        (
            SOURCE_SNAPSHOTS_PROPERTY.to_owned(),
            versions::pins_property(&pins)?,
        ),
        (KEYS_CHANGED_PROPERTY.to_owned(), diff.keys().to_string()),
        (CODE_VERSION_PROPERTY.to_owned(), code.clone()),
        (SERVED_EDITION_PROPERTY.to_owned(), edition.clone()),
    ]);
    if let Some(why) = &recorded_explanation {
        summary.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
    }
    let snapshot = versions::append(&committer, &catalog, &commit, &files, &run, summary).await?;
    drop(stage);
    let tags = versions::pin(&catalog, TABLE, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": JOB, "engine": "duckdb", "table": TABLE,
            "outcome": "committed", "created_table": created, "snapshot_id": snapshot,
            "parent_snapshot_id": head, "version_seq": version_seq, "run": run,
            "pins": pins, "moved": moved, "served_edition": edition, "shadow_of": shadow_build,
            "code_version": code, "as_of_month": format!("{:04}-{:02}", as_of.year(), as_of.month()),
            "recorded_explanation": recorded_explanation,
            "deals": spooled.deals, "unaddressed_deals": spooled.unaddressed,
            "served_parcels": spooled.served_parcels, "units": spooled.units,
            "seconds_by_unit": seconds_by_unit, "read_millis": spooled.read_millis,
            "diff": diff, "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": files.len(), "tags": tags, "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "committed {TABLE}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records} units={} in {:.0}s",
        diff.keys(),
        diff.added,
        diff.changed,
        diff.removed,
        spooled.units.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_build_month_is_recorded_as_the_month_not_as_new_rules() -> anyhow::Result<()> {
        let october = NaiveDate::from_ymd_opt(2026, 10, 1).context("a date")?;
        let november = NaiveDate::from_ymd_opt(2026, 11, 1).context("a date")?;
        let base = versioned("abc", october);
        assert_eq!(base, "abc@2026-10");
        assert_eq!(code_moved(&base, &base), None);
        assert_eq!(
            code_moved(&base, &versioned("abc", november)).as_deref(),
            Some("the build month moved 2026-10 -> 2026-11")
        );
        assert_eq!(
            code_moved(&base, &versioned("def", october)).as_deref(),
            Some("the code version moved to def")
        );
        Ok(())
    }
}
