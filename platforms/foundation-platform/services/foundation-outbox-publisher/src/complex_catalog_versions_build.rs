//! `build-complex-catalog-versions`: the industrial-complex catalog's table of row versions.
//!
//! `gold.complex_catalog_versions` (root ADR-0426, ADR-0564), computed by `DuckDB` inside the
//! publisher (`complex_catalog_stage`). It was `industrial_complex_silver_to_gold.py`, run by hand,
//! and the `--follow-region-codes` merge of `lineage_stewardship`.
//!
//! One run reads every input once at its pin, builds the whole catalog (1,442 rows, 2026-11-22: the
//! table's declared exception to the 시도 loop and to a partition, `gold_versions_table`), diffs it
//! against the table's current rows and appends the complexes whose content changed in one commit.
//! The codes are carried through the pairs on every build, so a new pair reaches the catalog when
//! the build its input starts runs. Nothing to do when the base commit was built from the same pins
//! and code.
//!
//! While the canonical load and the profile and edge exports read `gold.complex_catalog` it is that
//! table's shadow (root ADR-0564 §6): the table's commits carry no build tag, so the build follows
//! its newest commit that is not a compaction and reads each input as of that commit
//! (`gold_versions_commit::untagged_shadow`), and the takeover verdict (`takeover_equal` on 시도 41,
//! by `sido_code`) compares the two.

use std::{collections::BTreeSet, path::Path, time::Instant};

use anyhow::{bail, Context};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    complex_catalog_stage::{self as complex, ComplexPins, INPUTS, REPLACED, SILVER},
    duckdb_parquet_stage::DuckDbParquetStage,
    place_identity_stage::{CODE_CHANGE_TABLE, DONG_NAME_TABLE},
    process_memory,
};
use lakehouse_domain::{GOLD_VERSIONS_REMOVAL_COLUMN, GOLD_VERSION_SEQ_COLUMN};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, RECORDED_EXPLANATION_PROPERTY,
        SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    parcel_nearby_versions_build::{
        contract, lakehouse_read, max_row_loss_fraction, write_summary, MAX_CHANGED_KEY_FRACTION,
        MEMORY_SHARE,
    },
    public_data_control_support::required_env_value,
};

const TABLE: &str = "gold.complex_catalog_versions";
const JOB: &str = "build-complex-catalog-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
// one-off: until the takeover switch, which moves the readers to TABLE and this to it.
/// The table the readers read: while it is the replaced one, a build is its shadow.
const SERVED: &str = REPLACED;

/// Builds and appends the catalog's changed complexes.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(TABLE)?;
    // The replaced table's columns, then the digest the versions table adds to them.
    let content_columns: Vec<&'static str> = versions_contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| ![GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(name))
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
    let versions::Planned { pins, shadow, .. } =
        versions::planned_inputs(&catalog, TABLE, SERVED, &INPUTS, String::new()).await?;
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = complex::code_version();
    if let Some(base) = &base {
        versions::pin(&catalog, TABLE, base.snapshot_id, &base.pins).await?;
    }
    let moved = base
        .as_ref()
        .map_or_else(BTreeSet::new, |base| versions::moved(base, &pins));
    if base
        .as_ref()
        .is_some_and(|base| moved.is_empty() && base.code_version == code)
    {
        write_summary(
            &summary_path,
            &json!({
                "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "unchanged",
                "pins": pins, "shadow_of": shadow_build,
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {TABLE} nothing to do: its inputs and code are the base commit's");
        return Ok(());
    }
    let pin = |table: &str| {
        pins.get(table)
            .copied()
            .with_context(|| format!("{table} is not pinned: its build read none of it"))
    };
    let complex_pins = ComplexPins {
        silver: pin(SILVER)?,
        code_changes: pin(CODE_CHANGE_TABLE)?,
        dong_names: pin(DONG_NAME_TABLE)?,
    };

    // Compute: the whole catalog in one pass.
    // Asked before the DuckDB session opens: the session is not `Send` across an await.
    let shape = committer.table_shape(TABLE).await?;
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = duck.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    duck.require_room(0)?;
    let connection = duck.connection();
    complex::load(connection, &complex_pins)?;
    let rows = complex::build(connection, complex_pins.silver, &published_at_utc)?;
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table: TABLE,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: complex_pins.silver.to_string(),
        published_at_utc: published_at_utc.clone(),
    };
    let run = versions::run_id(TABLE, &pins, &code, "");
    let key = commit.key()?.join(", ");
    let diff = versions::diff(connection, &commit, "TRUE")?;
    let digests = versions::digests(connection, &commit)?;
    // One file: the table has no partition, and its key (a UUID) names no 시도.
    let files = if diff.keys() > 0 {
        vec![duck.write(
            &shape,
            &format!("SELECT * EXCLUDE (change) FROM appended ORDER BY {key}"),
            &run,
        )?]
    } else {
        Vec::new()
    };
    let (fingerprint, current_records) = versions::after_digests(&commit, &diff, digests)?;
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
        if base.code_version != code {
            recorded_explanation = Some(format!("the code version moved to {code}"));
        } else if fraction > MAX_CHANGED_KEY_FRACTION {
            bail!(
                "{} of {TABLE}'s {} current rows changed ({fraction:.3}, over \
                 {MAX_CHANGED_KEY_FRACTION}) with the code unmoved: nothing was appended (root \
                 ADR-0564 §3.4)",
                diff.keys(),
                base.current_records
            );
        }
    }

    // Commit.
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
    ]);
    if let Some(why) = &recorded_explanation {
        properties.insert(RECORDED_EXPLANATION_PROPERTY.to_owned(), why.clone());
    }
    let snapshot =
        versions::append(&committer, &catalog, &commit, &files, &run, properties).await?;
    drop(duck);
    let tags = versions::pin(&catalog, TABLE, snapshot, &pins).await?;
    write_summary(
        &summary_path,
        &json!({
            "job": JOB, "engine": "duckdb", "table": TABLE, "outcome": "committed",
            "created_table": created, "snapshot_id": snapshot, "parent_snapshot_id": head,
            "version_seq": version_seq, "run": run, "pins": pins, "moved": moved,
            "shadow_of": shadow_build, "code_version": code, "rows": rows,
            "recorded_explanation": recorded_explanation, "diff": diff,
            "current_records": current_records,
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
