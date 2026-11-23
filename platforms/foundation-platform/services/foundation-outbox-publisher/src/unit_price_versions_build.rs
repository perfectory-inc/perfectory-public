//! `build-unit-price-versions`: the unit price Gold as a table of appended row versions,
//! `gold.unit_price_versions` (root ADR-0190, ADR-0204, ADR-0564 §9 차례 4), computed by `DuckDB` in the
//! publisher (`foundation_publisher_silver::unit_price_stage`). It was the Spark job
//! `unit_price_panel_silver_to_gold.py`.
//!
//! The plan, the diff, the bounds and the append are the kind builds' (`kind_versions_build`, the
//! shared `gold_versions_commit`); what differs is the read. The price table keeps every vintage's
//! batch, and the newest one is read, a vintage's ties broken by the order the table appended them
//! (`unit_price_stage::batch`). The register inputs are pinned to the snapshots the building Gold was
//! built from (`gold-panel-rebuild.contract.json` `pins_from`), so both documents place each unit
//! alike; while the lane serves `gold.unit_price`, the shadow takes that table's build pins whole. A
//! moved price batch explains a change of any size, as a moved code version does: a new reference
//! date a year changes most rows. Before anything is appended, a build in which more of the current
//! Gold's units would keep fewer price records than the contract allows is refused
//! (`max_price_history_shrink_fraction`, the Spark job's `assert_price_histories_kept`).

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Instant,
};

use anyhow::{bail, ensure, Context};
use foundation_publisher_by_pnu::by_pnu_serving_manifest_publish;
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LAKEHOUSE_CATALOG},
    kind_versions_stage::ensure_no_commit_tables,
    parcel_nearby_stage::{NearbyPins, Source},
    process_memory,
    unit_price_stage::{
        self as stage, batch, build, history_lengths_sql, load_region, read_registers,
        shrink_verdict, spool_prices, PRICE_SOURCE, TITLE_SOURCE, UNIT_SOURCE,
    },
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::{
    by_pnu_gateway_contract::ByPnuLane,
    gold_versions_commit::{
        self as versions, Commit, Diff, Digests, CODE_VERSION_PROPERTY, CURRENT_RECORDS_PROPERTY,
        FINGERPRINT_PROPERTY, KEYS_CHANGED_PROPERTY, RECORDED_EXPLANATION_PROPERTY,
        SOURCE_SNAPSHOTS_PROPERTY, VERSION_SEQ_PROPERTY,
    },
    kind_versions_build::{head_sidos, sido_range, Spool, SUMMARY_ENV},
    parcel_nearby_versions_build::{
        contract, lakehouse_read, max_row_loss_fraction_of, write_summary,
        MAX_CHANGED_KEY_FRACTION, MEMORY_SHARE, REBUILD_CONTRACT,
    },
    public_data_control_support::required_env_value,
};

/// The versions table.
pub(crate) const TABLE: &str = "gold.unit_price_versions";
/// The Gold it replaces, whose build a shadow follows.
const REPLACED: &str = "gold.unit_price";
/// The by-PNU lane that serves it.
const LANE: &str = "unit-price";
const JOB: &str = "build-unit-price-versions";
/// The inputs, every one the Spark job read.
const INPUTS: [&str; 3] = [TITLE_SOURCE, UNIT_SOURCE, PRICE_SOURCE];
/// The summary property a commit names the price batch it read under.
const PRICE_BATCH_PROPERTY: &str = "foundation.unit-price-batch";

/// The rebuild contract's entry for the replaced Gold.
fn rebuild_entry() -> anyhow::Result<Value> {
    let contract: Value = serde_json::from_str(REBUILD_CONTRACT)?;
    Ok(contract["tables"][REPLACED].clone())
}

/// The register pins `pins_from` names: the snapshots its Gold's build-tag commit read.
async fn host_pins(catalog: &IcebergRestCatalog) -> anyhow::Result<BTreeMap<String, i64>> {
    let entry = rebuild_entry()?;
    let host = entry["pins_from"]["gold_table"]
        .as_str()
        .with_context(|| format!("the rebuild contract names no pins_from of {REPLACED}"))?;
    // The host's build-tag commit and its pins, read as a shadow of the host reads them. When the
    // building lane serves its versions table, its takeover PR names that table in pins_from, whose
    // build tag then moves instead (root ADR-0564 §6).
    let shadow = versions::shadow_of(catalog, &format!("{host}_versions"), host)
        .await?
        .with_context(|| format!("{host} has no build to pin {REPLACED}'s registers to"))?;
    let mut pins = BTreeMap::new();
    for input in entry["pins_from"]["inputs"]
        .as_array()
        .context("pins_from names no inputs")?
    {
        let input = input
            .as_str()
            .context("a pins_from input is not a table name")?;
        let snapshot = shadow
            .pins
            .get(input)
            .with_context(|| format!("{host} build {} records no pin of {input}", shadow.build))?;
        pins.insert(input.to_owned(), *snapshot);
    }
    Ok(pins)
}

/// The units the current rows of `range` hold, the shrink check's denominator over every 시도.
fn current_units(
    connection: &foundation_publisher_silver::duckdb::Connection,
) -> anyhow::Result<u64> {
    let held: Option<i64> = connection.query_row(
        "SELECT CAST(sum(json_array_length(units_json)) AS BIGINT) FROM current_rows",
        [],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(held.unwrap_or(0))?)
}

/// Builds and appends the unit price Gold's changed keys.
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
    let max_shrink = rebuild_entry()?["max_price_history_shrink_fraction"]
        .as_f64()
        .with_context(|| {
            format!("the rebuild contract names no {REPLACED} max_price_history_shrink_fraction")
        })?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let created = committer.create_table_if_absent(versions_contract).await?;

    // Plan.
    let history = catalog
        .load_main_snapshot_history(TABLE)
        .await?
        .with_context(|| format!("{TABLE} does not exist after its creation"))?;
    let base = versions::base(&history)?;
    let base_batch = base
        .as_ref()
        .and_then(|base| {
            history
                .iter()
                .find(|commit| commit.snapshot_id == base.snapshot_id)
        })
        .and_then(|commit| commit.summary.get(PRICE_BATCH_PROPERTY).cloned());
    let head = history.first().map(|snapshot| snapshot.snapshot_id);
    let lane =
        ByPnuLane::named(LANE)?.with_context(|| format!("the contract names no {LANE} lane"))?;
    // The price table reads no served parcel edition; none is planned or recorded.
    let versions::Planned {
        mut pins, shadow, ..
    } = versions::planned_inputs(
        &catalog,
        TABLE,
        by_pnu_serving_manifest_publish::gold_table(lane),
        &INPUTS,
        String::new(),
    )
    .await?;
    if shadow.is_none() {
        pins.extend(host_pins(&catalog).await?);
    }
    let shadow_build = shadow
        .as_ref()
        .map(|shadow| json!({"table": shadow.replaced, "build": shadow.build}));
    let code = stage::code_version();
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
                "pins": pins, "shadow_of": shadow_build, "seconds": started.elapsed().as_secs_f64(),
            }),
        )?;
        println!("gold-versions {TABLE} nothing to do: its inputs and code are the base commit's");
        return Ok(());
    }
    let log = catalog
        .load_ingest_commit_log(PRICE_SOURCE)
        .await?
        .with_context(|| format!("{PRICE_SOURCE} does not exist"))?;
    let order = batch::order_batches(&log.ingested, log.head, &log.snapshots);

    // Compute: the registers placed once, the chosen batch spooled by 시도, each 시도 built, diffed
    // and staged before the next replaces it; one append commits them all.
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Asked before the DuckDB session opens: the session is not `Send` across an await.
    let shape = committer.table_shape(TABLE).await?;
    let duck = DuckDbParquetStage::open_reading_lakehouse(work, &lakehouse_read()?)?;
    let memory_limit = duck.use_share_of_available_memory(MEMORY_SHARE.0, MEMORY_SHARE.1)?;
    duck.require_room(0)?;
    let connection = duck.connection();
    let pinned = NearbyPins(pins.clone());
    let source = Source::Pinned {
        catalog: LAKEHOUSE_CATALOG,
        pins: &pinned,
    };
    let price_batch = batch::chosen_batch(&stage::batch_ids(connection, &source)?, &order)?;
    let registers = read_registers(connection, &source)?;
    let spool_dir = Spool::new(work.join("spool"))?;
    let spooled = spool_prices(connection, &source, &price_batch, spool_dir.path())?;
    let mut sidos = spooled.sidos.clone();
    // A 시도 the head holds and the build no longer does is diffed too: its keys get removal markers.
    if let Some(head) = head {
        sidos.extend(head_sidos(connection, TABLE, head)?);
    }
    duck.require_room(0)?;
    let price_snapshot = pins
        .get(PRICE_SOURCE)
        .context("the prices are pinned")?
        .to_string();
    let version_seq = base.as_ref().map_or(1, |base| base.version_seq + 1);
    let commit = Commit {
        table: TABLE,
        versions: versions_contract,
        content_columns,
        head,
        base: base.clone(),
        version_seq,
        source_snapshot_id: price_snapshot.clone(),
        published_at_utc: published_at_utc.clone(),
    };
    let run = versions::run_id(TABLE, &pins, &code, &price_batch);
    connection.execute_batch(
        "CREATE OR REPLACE TABLE shrink_before (id VARCHAR, records BIGINT); \
         CREATE OR REPLACE TABLE shrink_after (id VARCHAR, records BIGINT);",
    )?;
    let (mut diff, mut digests, mut files) = (Diff::default(), Digests::default(), Vec::new());
    let (mut rows, mut units_now) = (0, 0);
    for sido in &sidos {
        rows += load_region(connection, spool_dir.path(), sido)?;
        build(connection, &price_snapshot, &published_at_utc)?;
        ensure_no_commit_tables(connection)?;
        let invalid: i64 = connection.query_row(
            &format!(
                "SELECT count(*) FROM built WHERE NOT regexp_full_match(pnu, '[0-9]{{19}}') \
                 OR substr(pnu, 1, 2) <> '{sido}' OR building_id IS NULL OR units_json IS NULL \
                 OR units_json = '[]'"
            ),
            [],
            |row| row.get(0),
        )?;
        ensure!(
            invalid == 0,
            "{invalid} built rows of 시도 {sido} have a malformed PNU, another 시도's, no building or \
             no priced unit: nothing was appended"
        );
        diff += versions::diff(connection, &commit, &sido_range(sido)?)?;
        digests += versions::digests(connection, &commit)?;
        // The rows the commit writes, before and after (a unit that moved 시도 meets its new row in
        // the national comparison below).
        connection.execute_batch(&format!(
            "INSERT INTO shrink_before {}; INSERT INTO shrink_after {};",
            history_lengths_sql(
                "SELECT c.units_json FROM current_rows c SEMI JOIN appended a USING (pnu, building_id)"
            ),
            history_lengths_sql("SELECT units_json FROM appended WHERE change <> 'removed'")
        ))?;
        units_now += current_units(connection)?;
        files.extend(versions::stage(&duck, &shape, &run, versions_contract)?);
    }
    drop(spool_dir);
    let shrunk: i64 = connection.query_row(
        "SELECT count(*) FROM shrink_before b LEFT JOIN shrink_after a USING (id) \
         WHERE coalesce(a.records, 0) < b.records",
        [],
        |row| row.get(0),
    )?;
    let shrunk = u64::try_from(shrunk)?;
    if let Some(why) = shrink_verdict(shrunk, units_now, max_shrink) {
        bail!("{why}: nothing was appended");
    }
    println!(
        "gold-versions {TABLE} built {rows} prices of batch {price_batch} in {} 시도 in {:.1}s (memory peak {:?} bytes)",
        sidos.len(),
        started.elapsed().as_secs_f64(),
        process_memory::cgroup_peak_bytes()
    );
    let (fingerprint, current_records) = versions::after_digests(&commit, &diff, digests)?;
    let mut recorded_explanation = None;
    if let Some(base) = &base {
        let max_loss = max_row_loss_fraction_of(REPLACED)?;
        #[allow(clippy::cast_precision_loss)]
        let floor = (1.0 - max_loss) * base.current_records as f64;
        #[allow(clippy::cast_precision_loss)]
        if (current_records as f64) < floor {
            bail!(
                "{TABLE} would hold {current_records} current rows, fewer than the {floor:.0} its {} \
                 allow (gold-panel-rebuild.contract.json max_row_loss_fraction): nothing was appended",
                base.current_records
            );
        }
        // A moved code version or price batch explains a change of any size (root ADR-0346).
        let moved_code_or_batch = if base.code_version != code {
            Some(format!("the code version moved to {code}"))
        } else if base_batch.as_deref() != Some(price_batch.as_str()) {
            Some(format!(
                "the price batch moved {} -> {price_batch}",
                base_batch.as_deref().unwrap_or("-")
            ))
        } else {
            None
        };
        #[allow(clippy::cast_precision_loss)]
        let fraction = diff.keys() as f64 / (base.current_records.max(1)) as f64;
        match moved_code_or_batch {
            Some(why) => recorded_explanation = Some(why),
            None if fraction > MAX_CHANGED_KEY_FRACTION => bail!(
                "{} of {TABLE}'s {} current rows changed ({fraction:.3}, over \
                 {MAX_CHANGED_KEY_FRACTION}) with neither the code nor the price batch moved: \
                 nothing was appended (root ADR-0564 §3.4)",
                diff.keys(),
                base.current_records
            ),
            None => {}
        }
    }

    // Commit.
    let mut properties = BTreeMap::from([
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
        (PRICE_BATCH_PROPERTY.to_owned(), price_batch.clone()),
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
            "shadow_of": shadow_build, "code_version": code, "price_batch": price_batch,
            "recorded_explanation": recorded_explanation, "registers": registers,
            "unplaced_prices": spooled.unplaced_prices,
            "unplaced_price_unit_count": spooled.unplaced_price_units,
            "price_history_shrunk_unit_count": shrunk, "rows": rows, "sidos": sidos,
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

#[cfg(test)]
mod tests {
    use foundation_publisher_silver::building_unit_placement::{
        catalog_digest_sql, catalog_id_sql,
    };
    use foundation_publisher_silver::duckdb::Connection;

    /// The ids the stage writes in SQL are the catalog domain's (root ADR-0072): a building's and a
    /// unit's, byte for byte, for synthetic register keys.
    #[test]
    fn the_sql_catalog_ids_are_the_catalog_domain_s() -> anyhow::Result<()> {
        let connection = Connection::open_in_memory()?;
        for key in ["99999-000000001", "99999-100000002", "99999-0000000a3"] {
            for (kind, expected) in [
                ("building", catalog_domain::building_id_for_register_pk(key)),
                (
                    "building_unit",
                    catalog_domain::building_unit_id_for_register_pk(key),
                ),
            ] {
                let id: String = connection.query_row(
                    &format!(
                        "SELECT {} FROM (SELECT {} AS d)",
                        catalog_id_sql("d"),
                        catalog_digest_sql(kind, &format!("'{key}'"))
                    ),
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(id, expected.to_string(), "{kind} {key}");
            }
        }
        Ok(())
    }
}
