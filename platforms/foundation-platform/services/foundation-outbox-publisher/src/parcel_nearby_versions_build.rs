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
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_extensions,
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG},
    parcel_nearby_stage::{
        build_sido, changed_sidos, code_version, inputs, load_inputs, parcels_outside_cells,
        whole_table_inputs, ChangedSidos, NearbyPins, Source, PARCEL_SOURCE, STORE_H3_RING,
        STORE_SOURCE,
    },
    process_memory, vworld_parcel_source_contract,
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_versions_commit::{
        self as versions, Commit, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, RECORDED_EXPLANATION_PROPERTY,
        SERVED_EDITION_PROPERTY, SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    public_data_control_support::required_env_value,
};

/// The table this command writes.
pub(crate) const TABLE: &str = "gold.parcel_nearby_versions";
/// Its by-PNU lane, whose served Gold says whether this run is a shadow (root ADR-0564 §6).
const LANE: &str = "parcel-nearby";
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
/// The document field a store-only commit may change, and nothing else (root ADR-0596).
const STORE_COUNT_FIELD: &str = "commercial_store_count";
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

fn write_summary(path: &str, summary: &Value) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(summary)?)
        .with_context(|| format!("cannot write {path}"))
}

/// Why a commit whose code and served edition stayed is the stores' doing, or `None` (root ADR-0596).
///
/// It is when only the stores moved, the located stores did not fall to none or move by more than
/// `max_loss` of the head's (a broken or emptied store file would otherwise move every count), no key
/// came or went, every changed row differs from the head's in [`STORE_COUNT_FIELD`] and in nothing
/// else, and every changed parcel lies in `stores.store_cells` (within [`STORE_H3_RING`] rings of an
/// H3 cell a store left or entered). Reads `appended` and `current_rows` as [`versions::diff`] leaves
/// them.
pub(crate) fn store_only(
    connection: &foundation_publisher_silver::duckdb::Connection,
    moved: &BTreeSet<String>,
    diff: &versions::Diff,
    stores: &ChangedSidos,
    max_loss: f64,
) -> anyhow::Result<Option<String>> {
    if moved.len() != 1 || !moved.contains(STORE_SOURCE) || diff.added != 0 || diff.removed != 0 {
        return Ok(None);
    }
    let Some((before, after)) = stores.located_stores else {
        return Ok(None);
    };
    #[allow(clippy::cast_precision_loss)]
    let moved_share = before.abs_diff(after) as f64 / before.max(1) as f64;
    if before == 0 || after == 0 || moved_share > max_loss {
        return Ok(None);
    }
    let field = format!("'$.{STORE_COUNT_FIELD}'");
    let without = format!("'{{\"{STORE_COUNT_FIELD}\": null}}'");
    let otherwise: i64 = connection.query_row(
        &format!(
            "SELECT count(*) FROM appended a JOIN current_rows c USING (pnu) WHERE a.change = 'changed' \
             AND (json_extract(a.nearby_json, {field}) IS NOT DISTINCT FROM json_extract(c.nearby_json, {field}) \
                  OR json_merge_patch(a.nearby_json, {without})::VARCHAR \
                     IS DISTINCT FROM json_merge_patch(c.nearby_json, {without})::VARCHAR \
                  OR a.place_id IS DISTINCT FROM c.place_id OR a.attached_via IS DISTINCT FROM c.attached_via)"
        ),
        [],
        |row| row.get(0),
    )?;
    if otherwise != 0 {
        return Ok(None);
    }
    let outside = parcels_outside_cells(
        connection,
        &stores.store_cells,
        "SELECT pnu FROM appended WHERE change = 'changed'",
    )?;
    if outside != 0 {
        return Ok(None);
    }
    Ok(Some(format!(
        "store-only: {} changed keys, each differing from the head only in {STORE_COUNT_FIELD}, every one \
         within {STORE_H3_RING} rings of the {} H3 cells a store of {STORE_SOURCE} left or entered \
         ({before} -> {after} located stores)",
        diff.changed,
        stores.store_cells.len()
    )))
}

/// What the commit records about why it changed, or why it is refused (root ADR-0564 §3.4,
/// ADR-0596): a moved code version or served edition, else a store-only explanation, explains a
/// change of any size; with neither, `fraction` (the share of the current rows changed) must stay
/// within [`MAX_CHANGED_KEY_FRACTION`].
pub(crate) fn recorded_or_refused(
    fraction: f64,
    moved_code_or_edition: Option<String>,
    store_only: Option<String>,
) -> Result<Option<String>, String> {
    match (moved_code_or_edition, store_only) {
        (Some(why), _) | (None, Some(why)) => Ok(Some(why)),
        (None, None) if fraction > MAX_CHANGED_KEY_FRACTION => Err(format!(
            "{fraction:.3} of the current rows changed, over {MAX_CHANGED_KEY_FRACTION}, with neither the \
             code nor the served edition moved and no store-only explanation"
        )),
        (None, None) => Ok(None),
    }
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
    // A shadow names the replaced table's build it was built from (root ADR-0564 §6).
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
        let summary = json!({
            "job": "build-parcel-nearby-versions", "engine": "duckdb", "table": TABLE,
            "outcome": "unchanged", "pins": pins, "served_edition": edition, "shadow_of": shadow_build,
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
    // A moved code version or served edition explains a change of any size (root ADR-0346); the
    // commit records which, so the bake reads it beside a store-only one (root ADR-0596).
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
    let nearby_pins = NearbyPins(pins.clone());
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &nearby_pins,
    };
    let read = load_inputs(connection, &source, &edition)?;
    for (input, millis) in &read.read_millis {
        println!(
            "gold-versions {TABLE} read {input} in {:.1}s",
            *millis as f64 / 1000.0
        );
    }
    println!(
        "gold-versions {TABLE} read its inputs in {:.1}s (memory peak {:?} bytes)",
        started.elapsed().as_secs_f64(),
        process_memory::cgroup_peak_bytes()
    );
    let mut stores = ChangedSidos::default();
    let sidos: BTreeSet<String> = if every_sido {
        read.parcels_by_sido.keys().cloned().collect()
    } else {
        let base = base.as_ref().context("a partial run has a base")?;
        let old_pins = NearbyPins(base.pins.clone());
        let old = Source::Pinned {
            catalog: LAKEHOUSE_CATALOG,
            pins: &old_pins,
        };
        stores = changed_sidos(connection, &old, &source, &moved, &edition)?;
        stores
            .sidos
            .clone()
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
        build_sido(
            connection,
            &read.store_cells,
            sido,
            &parcels_snapshot,
            &published_at_utc,
        )?;
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
        // Checked at any size, so store-only commits under the bound are recorded too: several in a
        // row while the bake is behind must not sum past its bound unexplained (root ADR-0596).
        let store = store_only(connection, &moved, &diff, &stores, max_loss)?;
        recorded_explanation = recorded_or_refused(fraction, moved_code_or_edition, store)
            .map_err(|why| {
                let whole = if every_sido {
                    "; every 시도 was rebuilt (a whole-table input or the pin set moved), so the stores were not compared"
                } else {
                    ""
                };
                anyhow::anyhow!(
                    "{} of {TABLE}'s {} current rows changed: {why}{whole}: nothing was appended \
                     (root ADR-0564 §3.4, ADR-0596)",
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
    let summary = json!({
        "job": "build-parcel-nearby-versions", "engine": "duckdb", "table": TABLE,
        "outcome": "committed", "created_table": created, "snapshot_id": snapshot,
        "parent_snapshot_id": head, "version_seq": version_seq, "run": run,
        "pins": pins, "moved": moved, "served_edition": edition, "shadow_of": shadow_build, "code_version": code,
        "every_sido": every_sido, "sidos": sidos, "seconds_by_sido": seconds_by_sido,
        "recorded_explanation": recorded_explanation,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use foundation_publisher_silver::{duckdb::Connection, parcel_nearby_stage::store_cell};

    use super::*;

    const HEAD: &str = r#"{"airport":{"name":"A","distance_m":9000},"commercial_store_count":3}"#;
    const COUNT_ONLY: &str =
        r#"{"airport":{"name":"A","distance_m":9000},"commercial_store_count":5}"#;
    const LOSS: f64 = 0.1;

    /// The head's row and the build's row of one parcel at (37.5, 127.1235), as `versions::diff` leaves
    /// them. PNUs are synthetic (99999...).
    fn tables(built: &str, place_id: &str, attached_via: &str) -> anyhow::Result<Connection> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(&format!(
            "CREATE TABLE parcels AS SELECT '9999900000100000000' AS pnu, 37.5::DOUBLE AS lat, 127.1235::DOUBLE AS lon; \
             CREATE TABLE current_rows AS SELECT '9999900000100000000' AS pnu, '{HEAD}' AS nearby_json, \
               NULL::VARCHAR AS attached_via, 'P1' AS place_id; \
             CREATE TABLE appended AS SELECT '9999900000100000000' AS pnu, '{built}' AS nearby_json, \
               {attached_via} AS attached_via, '{place_id}' AS place_id, 'changed' AS change;"
        ))?;
        Ok(connection)
    }

    fn stores_only() -> BTreeSet<String> {
        BTreeSet::from([STORE_SOURCE.to_owned()])
    }

    fn changed(keys: u64) -> versions::Diff {
        versions::Diff {
            built: keys,
            changed: keys,
            current_in_range: keys,
            ..versions::Diff::default()
        }
    }

    /// A store move over the parcel's own H3 cell, `before` -> `after` located stores.
    fn moved_here(before: u64, after: u64) -> anyhow::Result<ChangedSidos> {
        Ok(ChangedSidos {
            sidos: BTreeSet::from(["99".to_owned()]),
            store_cells: BTreeSet::from([store_cell(37.5, 127.1235)?]),
            located_stores: Some((before, after)),
        })
    }

    #[test]
    fn a_change_of_the_store_count_alone_in_the_touched_cells_is_explained() -> anyhow::Result<()> {
        // Root ADR-0596: a quarter's 상가 file moves the count around most parcels (11-19 Gold: 80.38%
        // of parcels count a store), past the bound, and nothing else.
        let connection = tables(COUNT_ONLY, "P1", "NULL")?;
        let why = store_only(
            &connection,
            &stores_only(),
            &changed(1),
            &moved_here(100, 95)?,
            LOSS,
        )?;
        let why = why.context("explained")?;
        assert!(why.starts_with("store-only: 1 changed keys"), "{why}");
        assert!(why.contains("(100 -> 95 located stores)"), "{why}");
        assert!(
            !why.contains("  "),
            "no run of spaces enters the recorded text: {why}"
        );
        Ok(())
    }

    #[test]
    fn anything_but_the_store_count_moving_is_not_store_only() -> anyhow::Result<()> {
        let here = moved_here(100, 95)?;
        let distance = r#"{"airport":{"name":"A","distance_m":8000},"commercial_store_count":5}"#;
        let cases: [(&str, Connection, BTreeSet<String>, versions::Diff); 6] = [
            (
                "a distance moved",
                tables(distance, "P1", "NULL")?,
                stores_only(),
                changed(1),
            ),
            (
                "the place id moved",
                tables(COUNT_ONLY, "P2", "NULL")?,
                stores_only(),
                changed(1),
            ),
            (
                "attached_via moved",
                tables(COUNT_ONLY, "P1", "'lineage'")?,
                stores_only(),
                changed(1),
            ),
            (
                "the count stayed (a digest-only drift)",
                tables(HEAD, "P1", "NULL")?,
                stores_only(),
                changed(1),
            ),
            (
                "the parcels moved too",
                tables(COUNT_ONLY, "P1", "NULL")?,
                BTreeSet::from([STORE_SOURCE.to_owned(), PARCEL_SOURCE.to_owned()]),
                changed(1),
            ),
            (
                "a key left",
                tables(COUNT_ONLY, "P1", "NULL")?,
                stores_only(),
                versions::Diff {
                    removed: 1,
                    ..changed(1)
                },
            ),
        ];
        for (case, connection, moved, diff) in cases {
            assert_eq!(
                store_only(&connection, &moved, &diff, &here, LOSS)?,
                None,
                "{case}"
            );
        }
        let connection = tables(COUNT_ONLY, "P1", "NULL")?;
        let added = versions::Diff {
            added: 1,
            ..changed(1)
        };
        assert_eq!(
            store_only(&connection, &stores_only(), &added, &here, LOSS)?,
            None,
            "a key came"
        );
        let elsewhere = ChangedSidos {
            store_cells: BTreeSet::from([store_cell(38.0, 127.1235)?]),
            ..moved_here(100, 95)?
        };
        assert_eq!(
            store_only(&connection, &stores_only(), &changed(1), &elsewhere, LOSS)?,
            None,
            "outside the cells"
        );
        Ok(())
    }

    #[test]
    fn an_emptied_or_halved_store_file_is_not_store_only() -> anyhow::Result<()> {
        // A file whose coordinates do not parse, or half loaded, moves every count near a store.
        let connection = tables(COUNT_ONLY, "P1", "NULL")?;
        for (before, after) in [(2, 0), (0, 2), (100, 50), (100, 150)] {
            let stores = moved_here(before, after)?;
            assert_eq!(
                store_only(&connection, &stores_only(), &changed(1), &stores, LOSS)?,
                None,
                "{before} -> {after}"
            );
        }
        let no_totals = ChangedSidos {
            located_stores: None,
            ..moved_here(100, 95)?
        };
        assert_eq!(
            store_only(&connection, &stores_only(), &changed(1), &no_totals, LOSS)?,
            None
        );
        Ok(())
    }

    #[test]
    fn a_commit_records_why_it_changed_or_is_refused_over_the_bound() {
        let code = Some("the code version moved to c".to_owned());
        let store = Some("store-only: 9 changed keys".to_owned());
        assert_eq!(
            recorded_or_refused(0.9, code.clone(), store.clone()),
            Ok(code.clone())
        );
        assert_eq!(
            recorded_or_refused(0.9, None, store.clone()),
            Ok(store.clone())
        );
        // Under the bound a store-only commit is recorded too, and an unexplained one is not refused.
        assert_eq!(recorded_or_refused(0.2, None, store.clone()), Ok(store));
        assert_eq!(recorded_or_refused(0.2, None, None), Ok(None));
        assert!(recorded_or_refused(0.6, None, None).is_err_and(|why| why.contains("over 0.5")));
    }
}
