//! `build-parcel-register-masters-versions`: the parcel 총괄표제부 kind's Gold as a table of
//! appended row versions, `gold.parcel_register_masters_versions` (root ADR-0450, ADR-0564 §6, the
//! first lane of the takeover path). `DuckDB` inside the publisher computes it
//! (`foundation_publisher_silver::building_register_master_stage`) and the publisher appends the keys
//! that changed (`gold_versions_commit`).
//!
//! While the lane serves the Spark table `gold.parcel_register_masters`, this table is its shadow:
//! its inputs are pinned to that table's build-tag commit (`planned_inputs`), so the takeover verdict
//! compares two engines over one set of inputs, and its first build holds every key. One run: create
//! the table if it is not there, plan, mend the tags, stop when the inputs, the served edition and the
//! code are what the base commit was built from; otherwise rebuild the PNUs whose sites or parcels
//! changed (every PNU for a first build, a new code version or edition, or a moved code change
//! table), diff them against the head's current rows, refuse a build that loses or changes more than
//! its bounds, and append.

use std::{collections::BTreeSet, path::Path, time::Instant};

use anyhow::{bail, ensure, Context};
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    building_register_master_stage::{
        build, changed_pnus, code_version, inputs, load_inputs, whole_table_inputs, PARCEL_SOURCE,
    },
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    parcel_nearby_stage::{NearbyPins, Source},
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, RECORDED_EXPLANATION_PROPERTY,
        SERVED_EDITION_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    parcel_nearby_versions_build::{
        contract, lakehouse_read, max_row_loss_fraction, served_edition, write_summary,
        MAX_CHANGED_KEY_FRACTION, MEMORY_SHARE,
    },
    public_data_control_support::required_env_value,
};

/// The table this command writes.
pub(crate) const TABLE: &str = "gold.parcel_register_masters_versions";
/// Its by-PNU lane, whose served Gold says whether this run is a shadow (root ADR-0564 §6).
const LANE: &str = "parcel-register-masters";
/// The table whose columns it carries and whose build a shadow follows.
const REPLACED: &str = "gold.parcel_register_masters";
const JOB: &str = "build-parcel-register-masters-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";

/// Builds and appends the 총괄표제부 Gold's changed keys.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
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
        &inputs(),
        served_edition()?,
    )
    .await?;
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = code_version();
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
    let whole = whole_table_inputs();
    let every_pnu = base.as_ref().is_none_or(|base| {
        base.code_version != code
            || base.served_edition.as_deref() != Some(edition.as_str())
            || moved.iter().any(|input| whole.contains(&input.as_str()))
            || !base.pins.keys().all(|input| pins.contains_key(input))
    });
    // A moved code version or served edition explains a change of any size (root ADR-0346).
    let moved_code_or_edition = base.as_ref().and_then(|base| {
        if base.code_version != code {
            Some(format!("the code version moved to {code}"))
        } else if base.served_edition.as_deref() != Some(edition.as_str()) {
            Some(format!(
                "the served parcel edition moved {} -> {edition}",
                base.served_edition.as_deref().unwrap_or("-")
            ))
        } else {
            None
        }
    });

    // Compute.
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = stage.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    let connection = stage.connection();
    let new_pins = NearbyPins(pins.clone());
    let read = load_inputs(
        connection,
        &Source::Pinned {
            catalog: LAKEHOUSE_CATALOG,
            pins: &new_pins,
        },
        &edition,
        "",
    )?;
    let targets = if every_pnu {
        None
    } else {
        let base = base.as_ref().context("a partial run has a base")?;
        let old_pins = NearbyPins(base.pins.clone());
        let old_edition = base
            .served_edition
            .clone()
            .context("a partial run's base names its served edition")?;
        load_inputs(
            connection,
            &Source::Pinned {
                catalog: LAKEHOUSE_CATALOG,
                pins: &old_pins,
            },
            &old_edition,
            "_old",
        )?;
        Some(changed_pnus(connection)?)
    };
    let parcels_snapshot = pins
        .get(PARCEL_SOURCE)
        .context("the parcels are pinned")?
        .to_string();
    build(
        connection,
        targets.is_none(),
        &parcels_snapshot,
        &published_at_utc,
    )?;
    println!(
        "gold-versions {TABLE} built {} PNUs of {} served ({} sites) in {:.1}s (memory peak {:?} bytes)",
        targets.map_or_else(|| "every".to_owned(), |count| count.to_string()),
        read.served_parcels,
        read.sites,
        started.elapsed().as_secs_f64(),
        process_memory::cgroup_peak_bytes()
    );
    let invalid: i64 = connection.query_row(
        "SELECT count(*) FROM built WHERE NOT regexp_full_match(pnu, '[0-9]{19}') \
         OR register_masters_json IS NULL",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        invalid == 0,
        "{invalid} built rows have a malformed PNU or no section: nothing was appended"
    );

    // Diff and check.
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
    let range = match targets {
        None => "TRUE",
        // A partial run that names no PNU diffs nothing and records the new pins.
        Some(0) => "FALSE",
        Some(_) => "pnu IN (SELECT pnu FROM targets)",
    };
    let diff = versions::diff(connection, &commit, range)?;
    let (fingerprint, current_records) = versions::after(connection, &commit, &diff)?;
    let mut recorded_explanation = None;
    if let Some(base) = &base {
        let max_loss = max_row_loss_fraction()?;
        #[allow(clippy::cast_precision_loss)]
        let floor = (1.0 - max_loss) * base.current_records as f64;
        #[allow(clippy::cast_precision_loss)]
        if (current_records as f64) < floor {
            bail!(
                "{TABLE} would hold {current_records} current rows, fewer than the {floor:.0} its \
                 {} allow (gold-panel-rebuild.contract.json kind_tables.max_row_loss_fraction): \
                 nothing was appended",
                base.current_records
            );
        }
        #[allow(clippy::cast_precision_loss)]
        let fraction = diff.keys() as f64 / (base.current_records.max(1)) as f64;
        match moved_code_or_edition {
            Some(why) => recorded_explanation = Some(why),
            None if fraction > MAX_CHANGED_KEY_FRACTION => bail!(
                "{} of {TABLE}'s {} current rows changed ({fraction:.3}, over \
                 {MAX_CHANGED_KEY_FRACTION}) with neither the code nor the served edition moved: \
                 nothing was appended (root ADR-0564 §3.4)",
                diff.keys(),
                base.current_records
            ),
            None => {}
        }
    }

    // Commit.
    let run = versions::run_id(TABLE, &pins, &code, &edition);
    let shape = committer.table_shape(TABLE).await?;
    let files = versions::stage(&stage, &shape, &run, "pnu")?;
    let mut properties = std::collections::BTreeMap::from([
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
        properties.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
    }
    let snapshot =
        versions::append(&committer, &catalog, &commit, &files, &run, properties).await?;
    drop(stage);
    let tags = versions::pin(&catalog, TABLE, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "moved": moved,
            "served_edition": edition, "shadow_of": shadow_build, "code_version": code,
            "every_pnu": every_pnu, "targets": targets, "recorded_explanation": recorded_explanation,
            "served_parcels": read.served_parcels, "sites": read.sites, "read_millis": read.read_millis,
            "diff": diff, "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": files.len(), "tags": tags, "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    println!(
        "committed {TABLE}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records} in {:.0}s",
        diff.keys(),
        diff.added,
        diff.changed,
        diff.removed,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
