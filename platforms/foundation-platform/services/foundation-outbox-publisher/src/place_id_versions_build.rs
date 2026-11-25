//! The publisher's build of the place-id release as tables of appended row versions (root ADR-0564
//! §6-9, ADR-0113 §8, ADR-0481): `build-place-id-versions`.
//!
//! `DuckDB` inside the publisher computes both units of the registry and the bridge
//! (`foundation_publisher_silver::place_id_release_stage`): the parcel unit from
//! `silver.parcel_registry` as of the served edition, one region after another over one read; the
//! admin unit from the snapshots of `silver.administrative_boundaries`; both with the legal-dong
//! code crosswalk. The publisher appends the keys that changed to each versions table, one commit a
//! table (`gold_versions_commit`, keyed as the replaced tables and partitioned by (unit, scope)).
//! No Spark session.
//!
//! A unit is rebuilt when one of its inputs, the served edition or the code moved. The parcel unit
//! reads its registry whole: an id's region is the 시도 of its first row, whichever 시도 its later
//! rows are in (the Spark job read it whole once a region). A region no registry row names is not
//! diffed, so its rows stay, as the Spark job never rewrites a region it does not read. The
//! changelog stays with the Spark job until the switch: an event table is not shadowed twice.
//!
//! While the Spark job still writes the replaced tables and every kind row's `place_id` is read from
//! them, these tables are their shadow (root ADR-0564 §6): they read the inputs the replaced
//! registry's newest build read, as of its commit (`gold_versions_commit::UNTAGGED_BUILDS`: the
//! Spark job records no pins). Nothing reads them until the takeover verdict is equal.
//!
//! Of root ADR-0564 §3-4's checks it keeps one row per key (the diff) and the registry's statuses;
//! it has no floor on lost rows, as an id is never removed from the registry and the Spark job had
//! none.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{ensure, Context};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    kind_versions_stage::ensure_no_commit_tables,
    parcel_nearby_stage::{NearbyPins, Source},
    parcel_registry_stage::snapshot_ids_through,
    place_id_release_stage::{
        self as release, ADMIN_SOURCE, ADMIN_UNIT, CODE_CHANGE_SOURCE, PARCEL_UNIT, REGISTRY_SOURCE,
    },
    process_memory,
};
use lakehouse_domain::{
    LakehouseTableContract, GOLD_VERSIONS_REMOVAL_COLUMN, GOLD_VERSION_SEQ_COLUMN,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    gold_versions_commit::{
        self as versions, Base, Commit, Diff, Digests, CODE_VERSION_PROPERTY,
        CURRENT_RECORDS_PROPERTY, FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY,
        SERVED_EDITION_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    gold_versions_support::{contract, lakehouse_read, write_summary},
    onbid_silver_load::{served_snapshot_id, PARCEL_SOURCE},
    public_data_control_support::required_env_value,
};

const REGISTRY: &str = "gold.place_id_registry_versions";
const BRIDGE: &str = "gold.place_id_bridge_versions";
const REGISTRY_REPLACED: &str = "gold.place_id_registry";
const JOB: &str = "build-place-id-versions";
const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_GOLD_VERSIONS_SUMMARY";
// The summary's `table` is the registry (the takeover check reads one table a run,
// `gold_takeover_check.declared`); `tables` names both.

/// `built` from the registry rows of one region or unit. `release_id` is the run's (`<unit>-` and
/// the version), a stamp neither the digest nor the verdict reads; `row_digest` (the column the
/// versions table adds, root ADR-0564 §2) is the SHA-256 of `to_json` of the content.
const REGISTRY_BUILT_SQL: &str = "CREATE OR REPLACE TABLE built AS \
    SELECT unit, scope, place_id, status, current_code, redirect_to, \
      unit || '-@release@' AS release_id, CAST('@published_at_utc@' AS TIMESTAMPTZ) AS published_at_utc, \
      sha256(to_json({'unit': unit, 'scope': scope, 'place_id': place_id, 'status': status, \
        'current_code': current_code, 'redirect_to': redirect_to})::VARCHAR) AS row_digest \
    FROM built_registry;";

/// `built` from the bridge rows of one region or unit, as [`REGISTRY_BUILT_SQL`].
const BRIDGE_BUILT_SQL: &str = "CREATE OR REPLACE TABLE built AS \
    SELECT unit, scope, place_id, code, valid_from, valid_to, \
      unit || '-@release@' AS release_id, CAST('@published_at_utc@' AS TIMESTAMPTZ) AS published_at_utc, \
      sha256(to_json({'unit': unit, 'scope': scope, 'place_id': place_id, 'code': code, \
        'valid_from': valid_from, 'valid_to': valid_to})::VARCHAR) AS row_digest \
    FROM built_bridge;";

/// The registry rows that break the replaced table's gates: a status it has not, or a redirect on an
/// id that is not redirected.
const GATES_SQL: &str = "SELECT count(*) FROM built_registry \
    WHERE status NOT IN ('current', 'historic', 'redirected') \
       OR (redirect_to IS NOT NULL AND status <> 'redirected')";

/// One versions table's plan: its contract, head, base and the diff so far.
struct Table {
    versions: &'static LakehouseTableContract,
    built_sql: &'static str,
    head: Option<i64>,
    base: Option<Base>,
    content_columns: Vec<&'static str>,
    diff: Diff,
    digests: Digests,
    files: Vec<foundation_publisher_core::lakehouse_data_commit::StagedFile>,
}

impl Table {
    fn commit(&self, published_at_utc: &str) -> Commit<'_> {
        Commit {
            table: self.versions.table_name,
            versions: self.versions,
            content_columns: self.content_columns.clone(),
            head: self.head,
            base: self.base.clone(),
            version_seq: self.version_seq(),
            source_snapshot_id: String::new(),
            published_at_utc: published_at_utc.to_owned(),
        }
    }

    fn version_seq(&self) -> i64 {
        self.base.as_ref().map_or(1, |base| base.version_seq + 1)
    }
}

/// The units a table rebuilds against its base: every unit on a first build or a new code, else the
/// units one of whose inputs (or, for the parcel unit, the served edition) moved.
fn units_to_rebuild(
    base: Option<&Base>,
    pins: &BTreeMap<String, i64>,
    code: &str,
    served: &str,
) -> BTreeSet<&'static str> {
    let every = BTreeSet::from([PARCEL_UNIT, ADMIN_UNIT]);
    let Some(base) = base else {
        return every;
    };
    if base.code_version != code {
        return every;
    }
    let moved = versions::moved(base, pins);
    let mut units = BTreeSet::new();
    if moved.contains(CODE_CHANGE_SOURCE) {
        return every;
    }
    if moved.contains(REGISTRY_SOURCE) || base.served_edition.as_deref() != Some(served) {
        units.insert(PARCEL_UNIT);
    }
    if moved.contains(ADMIN_SOURCE) {
        units.insert(ADMIN_UNIT);
    }
    units
}

/// Builds and appends the changed rows of the registry and the bridge.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let published_at_utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let committer = LakehouseDataCommitter::from_env().await?;

    // Plan.
    let shadow = versions::shadow_of(&catalog, REGISTRY, REGISTRY_REPLACED)
        .await?
        .with_context(|| format!("{REGISTRY} is not a shadow of {REGISTRY_REPLACED}"))?;
    let pins = shadow.pins.clone();
    ensure!(
        pins.contains_key(CODE_CHANGE_SOURCE),
        "{REGISTRY_REPLACED} build {} read no {CODE_CHANGE_SOURCE}: there is no crosswalk to read",
        shadow.build
    );
    let served = served_snapshot_id()?;
    let code = release::code_version();
    let mut tables = Vec::new();
    let mut created = Vec::new();
    let mut units = BTreeSet::new();
    let mut shapes = Vec::new();
    for (name, built_sql) in [(REGISTRY, REGISTRY_BUILT_SQL), (BRIDGE, BRIDGE_BUILT_SQL)] {
        let versions_contract = contract(name)?;
        if committer.create_table_if_absent(versions_contract).await? {
            created.push(name);
        }
        let history = catalog
            .load_main_snapshot_history(name)
            .await?
            .with_context(|| format!("{name} does not exist after its creation"))?;
        let base = versions::base(&history)?;
        // Mends the tags a run that died between its commit and its tags left (as every lane).
        if let Some(base) = &base {
            versions::pin(&catalog, name, base.snapshot_id, &base.pins).await?;
        }
        units.extend(units_to_rebuild(base.as_ref(), &pins, &code, &served));
        shapes.push(committer.table_shape(name).await?);
        tables.push(Table {
            versions: versions_contract,
            built_sql,
            head: history.first().map(|snapshot| snapshot.snapshot_id),
            base,
            content_columns: versions_contract
                .columns
                .iter()
                .map(|column| column.name)
                .filter(|name| {
                    ![GOLD_VERSION_SEQ_COLUMN, GOLD_VERSIONS_REMOVAL_COLUMN].contains(name)
                })
                .collect(),
            diff: Diff::default(),
            digests: Digests::default(),
            files: Vec::new(),
        });
    }
    // An input the replaced build had no snapshot of yet: its unit is not built.
    if !pins.contains_key(REGISTRY_SOURCE) {
        units.remove(PARCEL_UNIT);
    }
    if !pins.contains_key(ADMIN_SOURCE) {
        units.remove(ADMIN_UNIT);
    }
    let shadow_of = json!({"table": shadow.replaced, "build": shadow.build});
    if units.is_empty() {
        write_summary(
            &summary_path,
            &json!({
                "job": JOB, "engine": "duckdb", "table": REGISTRY, "tables": [REGISTRY, BRIDGE],
                "outcome": "unchanged",
                "pins": pins, "served_edition": served, "shadow_of": shadow_of,
                "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {REGISTRY} {BRIDGE} nothing to do: inputs and code are the bases'");
        return Ok(());
    }

    // Compute, one region or unit at a time.
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = duck.memory_limit_bytes()?;
    let connection = duck.connection();
    connection.execute_batch("SET preserve_insertion_order = false;")?;
    let new_pins = NearbyPins(pins.clone());
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &new_pins,
    };
    release::load_remap(connection, &source)?;
    let (remap, ambiguous) = release::remap_of(connection)?;
    let runs: Vec<String> = tables
        .iter()
        .map(|table| versions::run_id(table.versions.table_name, &pins, &code, &served))
        .collect();
    let mut steps: Vec<(String, String)> = Vec::new();
    let spool = tempfile::tempdir_in(work).context("no spool directory for the registry")?;
    let mut regions = BTreeMap::new();
    if units.contains(PARCEL_UNIT) {
        let through = snapshot_ids_through(PARCEL_SOURCE, &served)?;
        regions = release::load_parcel_rows(connection, &source, &through, spool.path())?;
        ensure!(
            !regions.is_empty(),
            "{REGISTRY_SOURCE} holds no row of the served edition {served}: an empty release would \
             erase the served ids"
        );
        steps.extend(regions.keys().map(|scope| {
            (
                scope.clone(),
                format!("unit = '{PARCEL_UNIT}' AND scope = '{scope}'"),
            )
        }));
    }
    if units.contains(ADMIN_UNIT) {
        steps.push((ADMIN_UNIT.to_owned(), format!("unit = '{ADMIN_UNIT}'")));
    }
    let mut built_rows = BTreeMap::new();
    let mut snapshots_read = Vec::new();
    for (step, range) in &steps {
        release::create_built(connection)?;
        if step == ADMIN_UNIT {
            let snapshots = release::load_admin_snapshots(connection, &source)?;
            ensure!(
                !snapshots.is_empty(),
                "{ADMIN_SOURCE} holds no snapshot: an empty release would erase the served ids"
            );
            let (registry_rows, bridge_rows) =
                release::admin_release(&snapshots, &remap, &ambiguous);
            release::insert_admin(connection, &registry_rows, &bridge_rows)?;
            snapshots_read = snapshots.into_iter().map(|(label, _)| label).collect();
        } else {
            release::build_parcel_scope(connection, step)?;
        }
        let broken: i64 = connection.query_row(GATES_SQL, [], |row| row.get(0))?;
        ensure!(
            broken == 0,
            "{broken} built registry rows of {step} hold a status or a redirect the registry has \
             not: nothing was appended"
        );
        built_rows.insert(step.clone(), release::built_counts(connection)?);
        for (table, (run, shape)) in tables.iter_mut().zip(runs.iter().zip(&shapes)) {
            let version_seq = table.version_seq().to_string();
            connection.execute_batch(
                &table
                    .built_sql
                    .replace("@release@", &format!("versions-{version_seq}"))
                    .replace("@published_at_utc@", &published_at_utc),
            )?;
            ensure_no_commit_tables(connection)?;
            let commit = table.commit(&published_at_utc);
            let diff = versions::diff(connection, &commit, range)?;
            let digests = versions::digests(connection, &commit)?;
            let files = versions::stage(&duck, shape, run, table.versions)?;
            table.diff += diff;
            table.digests += digests;
            table.files.extend(files);
        }
    }
    drop(spool);
    println!(
        "gold-versions {REGISTRY} {BRIDGE} built {} regions and {} in {:.1}s (memory peak {:?} bytes)",
        regions.len(),
        if units.contains(ADMIN_UNIT) { "the admin unit" } else { "no admin unit" },
        started.elapsed().as_secs_f64(),
        process_memory::cgroup_peak_bytes()
    );

    // Commit, one table after the other.
    let mut committed = Vec::new();
    for (table, run) in tables.iter().zip(&runs) {
        let commit = table.commit(&published_at_utc);
        let (fingerprint, current_records) =
            versions::after_digests(&commit, &table.diff, table.digests)?;
        let properties = BTreeMap::from([
            (
                VERSION_SEQ_PROPERTY.to_owned(),
                table.version_seq().to_string(),
            ),
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
            (
                KEYS_CHANGED_PROPERTY.to_owned(),
                table.diff.keys().to_string(),
            ),
            (CODE_VERSION_PROPERTY.to_owned(), code.clone()),
            (SERVED_EDITION_PROPERTY.to_owned(), served.clone()),
        ]);
        let snapshot =
            versions::append(&committer, &catalog, &commit, &table.files, run, properties).await?;
        let tags = versions::pin(&catalog, commit.table, snapshot, &pins).await?;
        println!(
            "committed {}: snapshot {snapshot} version_seq={} keys_changed={} (added {}, changed {}, \
             removed {}) current_records={current_records}",
            commit.table,
            commit.version_seq,
            table.diff.keys(),
            table.diff.added,
            table.diff.changed,
            table.diff.removed
        );
        committed.push(json!({
            "table": commit.table, "snapshot_id": snapshot, "parent_snapshot_id": table.head,
            "version_seq": commit.version_seq, "run": run, "diff": table.diff,
            "current_records": current_records,
            "content_fingerprint": versions::fingerprint_text(fingerprint),
            "files": table.files.len(), "tags": tags,
        }));
    }
    drop(duck);
    let built: BTreeMap<&String, Value> = built_rows
        .iter()
        .map(|(step, (registry, bridge))| (step, json!({"registry": registry, "bridge": bridge})))
        .collect();
    write_summary(
        &summary_path,
        &json!({
            "job": JOB, "engine": "duckdb", "table": REGISTRY, "tables": [REGISTRY, BRIDGE],
            "outcome": "committed", "created_tables": created,
            "units": units, "pins": pins, "served_edition": served, "shadow_of": shadow_of,
            "code_version": code, "code_pairs": remap.len(), "code_pairs_ambiguous": ambiguous.len(),
            "regions": regions, "admin_snapshots": snapshots_read, "built": built,
            "committed": committed, "duckdb_memory_limit_bytes": memory_limit,
            "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
            "seconds": started.elapsed().as_secs_f64(),
        }),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
