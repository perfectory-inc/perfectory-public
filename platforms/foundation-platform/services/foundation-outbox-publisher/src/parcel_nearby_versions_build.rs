//! `build-parcel-nearby-versions`: the parcel nearby kind's Gold, `gold.parcel_nearby_versions`
//! (root ADR-0465, ADR-0564 §9 turn 2), born as a table of appended row versions. `DuckDB` inside the
//! publisher computes it (`foundation_publisher_silver::parcel_nearby_stage`) and the publisher
//! appends the keys that changed (`gold_versions_commit`). It was the Spark job
//! `parcel_nearby_silver_to_gold.py`, which never committed.
//!
//! One run: create the table if it is not there, plan from the newest commit that records the
//! version sequence, pin every input at its head, mend the tags, and stop when the inputs, the
//! served parcel edition and the code are what that commit was built from. Otherwise recompute the
//! 시도 the moved inputs touch (every 시도 for a first build, a new code version or edition, or a
//! moved facility file, highway file or code change table), diff them against the head's current
//! rows, refuse a build that loses or changes more than its bounds, and append. The summary file
//! and the last stdout line are what the unit's script and the cut-over's gold step read.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_extensions,
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG},
    parcel_nearby_stage::{
        build_sido, changed_sidos, code_version, inputs, load_inputs, whole_table_inputs,
        NearbyPins, Source, PARCEL_SOURCE,
    },
    process_memory, vworld_parcel_source_contract,
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, SERVED_EDITION_PROPERTY,
        SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    public_data_control_support::required_env_value,
};

/// The table this command writes.
pub(crate) const TABLE: &str = "gold.parcel_nearby_versions";
/// The table whose columns it carries (never created: root ADR-0564 §1).
const REPLACED: &str = "gold.parcel_nearby";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
const PARCEL_SOURCE_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/vworld-parcel-source-objects.json");
const REBUILD_CONTRACT: &str =
    include_str!("../../../infra/lakehouse/contracts/gold-panel-rebuild.contract.json");
/// Above this share of the current rows changed in one commit the run stops, unless the code or
/// the served edition moved, which explains a change of any size (root ADR-0346, ADR-0564 §3-4).
const MAX_CHANGED_KEY_FRACTION: f64 = 0.5;
/// The share of the memory left that the buffer manager takes; the rest is `DuckDB`'s own
/// structures and the commit's uploads (W81b measured a 5.01 GiB peak at a 5 GB limit).
const MEMORY_SHARE: (u64, u64) = (3, 5);

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no lakehouse contract for {table}"))
}

/// The bound a by-PNU kind's Gold may lose rows by (`gold-panel-rebuild.contract.json`
/// `kind_tables.max_row_loss_fraction`, the kinds' one value).
fn max_row_loss_fraction() -> anyhow::Result<f64> {
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    contract["kind_tables"]["max_row_loss_fraction"]
        .as_f64()
        .context("gold-panel-rebuild.contract.json names no kind_tables.max_row_loss_fraction")
}

/// The served parcel edition's `silver.parcel_boundaries.source_snapshot_id`.
fn served_edition() -> anyhow::Result<String> {
    let view = vworld_parcel_source_contract::served_edition_view(PARCEL_SOURCE_CONTRACT)?;
    let contract: Value = serde_json::from_str(PARCEL_SOURCE_CONTRACT)?;
    let prefix = contract["snapshot_id_prefix"]
        .as_str()
        .context("the parcel source contract names no snapshot_id_prefix")?;
    let edition = view["edition"]
        .as_str()
        .context("the served edition view names no edition")?;
    Ok(format!("{prefix}{edition}"))
}

fn lakehouse_read() -> anyhow::Result<LakehouseRead> {
    let catalog = LakehouseCatalogConfig::from_env()
        .context("failed to configure the lakehouse catalog for the nearby build")?;
    let storage = R2ObjectStorageConfig::from_env()
        .context("failed to configure lakehouse R2 reads for the nearby build")?;
    Ok(LakehouseRead {
        extensions: duckdb_extensions::release_directory()?,
        catalog_uri: catalog.catalog_uri,
        warehouse: catalog.warehouse,
        catalog_token: catalog
            .catalog_token
            .context("the lakehouse catalog token is required")?,
        s3_endpoint: storage.endpoint,
        s3_access_key_id: storage.access_key_id,
        s3_secret_access_key: storage.secret_access_key,
    })
}

async fn head_of(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<i64> {
    catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| format!("{table} holds no snapshot"))
}

fn write_summary(path: &str, summary: &Value) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(summary)?)
        .with_context(|| format!("cannot write {path}"))
}

/// Builds and appends the nearby Gold's changed keys.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let versions_contract = contract(TABLE)?;
    let replaced = contract(REPLACED)?;
    let content_columns: Vec<&'static str> =
        replaced.columns.iter().map(|column| column.name).collect();
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
    let mut pins = BTreeMap::new();
    for input in inputs() {
        pins.insert(input.to_owned(), head_of(&catalog, input).await?);
    }
    let edition = served_edition()?;
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
        let summary = json!({
            "job": "build-parcel-nearby-versions", "engine": "duckdb", "table": TABLE,
            "outcome": "unchanged", "pins": pins, "served_edition": edition,
            "seconds": started.elapsed().as_secs_f64(),
        });
        write_summary(&summary_path, &summary)?;
        println!("gold-versions {TABLE} nothing to do: its inputs, edition and code are the base commit's");
        return Ok(());
    }
    let whole = whole_table_inputs();
    let every_sido = base.as_ref().is_none_or(|base| {
        base.code_version != code
            || base.served_edition.as_deref() != Some(edition.as_str())
            || moved.iter().any(|input| whole.contains(&input.as_str()))
            || !base.pins.keys().all(|input| pins.contains_key(input))
    });
    let explained = base.as_ref().is_none_or(|base| {
        base.code_version != code || base.served_edition.as_deref() != Some(edition.as_str())
    });

    // Compute.
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = stage.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    let connection = stage.connection();
    let nearby_pins = NearbyPins(pins.clone());
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &nearby_pins,
    };
    let read = load_inputs(connection, &source, &edition)?;
    let sidos: BTreeSet<String> = if every_sido {
        read.parcels_by_sido.keys().cloned().collect()
    } else {
        let base = base.as_ref().context("a partial run has a base")?;
        let old_pins = NearbyPins(base.pins.clone());
        let old = Source::Pinned {
            catalog: LAKEHOUSE_CATALOG,
            pins: &old_pins,
        };
        changed_sidos(connection, &old, &source, &moved, &edition)?
            .into_iter()
            .filter(|sido| read.parcels_by_sido.contains_key(sido))
            .collect()
    };
    let parcels_snapshot = pins
        .get(PARCEL_SOURCE)
        .context("the parcels are pinned")?
        .to_string();
    let mut seconds_by_sido = BTreeMap::new();
    for sido in &sidos {
        let at = Instant::now();
        build_sido(connection, sido, &parcels_snapshot, &published_at_utc)?;
        seconds_by_sido.insert(sido.clone(), at.elapsed().as_secs_f64());
        println!(
            "gold-versions {TABLE} built {sido} in {:.1}s (memory peak {:?} bytes)",
            at.elapsed().as_secs_f64(),
            process_memory::cgroup_peak_bytes()
        );
    }
    let invalid: i64 = connection.query_row(
        "SELECT count(*) FROM built WHERE NOT regexp_full_match(pnu, '[0-9]{19}') OR nearby_json IS NULL",
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
    let range = if every_sido {
        "TRUE".to_owned()
    } else if sidos.is_empty() {
        // A partial run that touches no 시도 diffs nothing and records the new pins.
        "FALSE".to_owned()
    } else {
        format!(
            "substr(pnu, 1, 2) IN ({})",
            sidos
                .iter()
                .map(|sido| format!("'{sido}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let diff = versions::diff(connection, &commit, &range)?;
    let (fingerprint, current_records) = versions::after(connection, &commit, &diff)?;
    if let Some(base) = &base {
        let floor = (1.0 - max_row_loss_fraction()?) * base.current_records as f64;
        if (current_records as f64) < floor {
            bail!(
                "{TABLE} would hold {current_records} current rows, fewer than the {floor:.0} its \
                 {} allow (gold-panel-rebuild.contract.json kind_tables.max_row_loss_fraction): \
                 nothing was appended",
                base.current_records
            );
        }
        let fraction = diff.keys() as f64 / (base.current_records.max(1)) as f64;
        if fraction > MAX_CHANGED_KEY_FRACTION && !explained {
            bail!(
                "{} of {TABLE}'s {} current rows changed ({fraction:.3}), over {MAX_CHANGED_KEY_FRACTION} \
                 with neither the code nor the served edition moved: nothing was appended (root ADR-0564 §3-4)",
                diff.keys(),
                base.current_records
            );
        }
    }

    // Commit.
    let run = versions::run_id(TABLE, &pins, &code, &edition);
    let shape = committer.table_shape(TABLE).await?;
    let files = versions::stage(&stage, &shape, &run, "pnu")?;
    let summary = BTreeMap::from([
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
    let snapshot = versions::append(&committer, &catalog, &commit, &files, &run, summary).await?;
    drop(stage);
    let tags = versions::pin(&catalog, TABLE, snapshot, &pins).await?;
    let summary = json!({
        "job": "build-parcel-nearby-versions", "engine": "duckdb", "table": TABLE,
        "outcome": "committed", "created_table": created, "snapshot_id": snapshot,
        "parent_snapshot_id": head, "version_seq": version_seq, "run": run,
        "pins": pins, "moved": moved, "served_edition": edition, "code_version": code,
        "every_sido": every_sido, "sidos": sidos, "seconds_by_sido": seconds_by_sido,
        "inputs": read, "diff": diff, "current_records": current_records,
        "content_fingerprint": versions::fingerprint_text(fingerprint),
        "files": files.len(), "tags": tags, "duckdb_memory_limit_bytes": memory_limit,
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
        "seconds": started.elapsed().as_secs_f64(),
    });
    write_summary(&summary_path, &summary)?;
    println!(
        "committed {TABLE}: snapshot {snapshot} version_seq={version_seq} keys_changed={} \
         (added {}, changed {}, removed {}) current_records={current_records} sidos={} in {:.0}s",
        diff.keys(),
        diff.added,
        diff.changed,
        diff.removed,
        sidos.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
