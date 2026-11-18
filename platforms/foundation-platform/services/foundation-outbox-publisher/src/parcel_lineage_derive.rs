//! `derive-parcel-lineage`: the scheduled parcel lineage derivation (root ADR-0113 §1, ADR-0148).
//!
//! `lineage-stewardship-cycle.sh` runs it once a day. For each consecutive pair of the parcel
//! editions the source contract holds it pins the current heads of the inputs once, groups the
//! sido prefixes, and derives each group `silver.parcel_lineage` does not hold yet: `DuckDB` inside
//! the publisher reads the pinned inputs and applies the rules
//! (`foundation_publisher_silver::parcel_lineage_stage`), and the publisher appends the rows in one
//! Iceberg commit per group (root ADR-0560). It was the Spark jobs `parcel_lineage_scheduled.py`
//! and `parcel_lineage_to_silver.py`.
//!
//! No group starts after the deadline; the next run continues where this one stopped. A group the
//! rules refuse (a vanished 동 code the code change table does not pair) is recorded with its
//! codes and the others go on; the run says so (`refused`) and the stewardship unit then fails
//! loudly. The summary file and the last stdout lines are the ones the Spark job wrote.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, TableShape,
};
use foundation_publisher_silver::{
    duckdb_extensions,
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead, LAKEHOUSE_CATALOG},
    parcel_lineage_schedule::{
        awaits_load, edition_pairs, is_held, sido_groups, sido_of_edition, superseded_since,
        Edition, BOUNDARIES_TABLE, CODE_CHANGE_TABLE, HISTORY_TABLE,
    },
    parcel_lineage_stage::{
        load_inputs, stage_group, LineageGroup, LineagePins, UnrecordedDongCodes, LINEAGE_TABLE,
        RULES_VERSION,
    },
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::{json, Value};

use crate::public_data_control_support::required_env_value;

const SUMMARY_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_LINEAGE_SUMMARY";
const DEADLINE_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_LINEAGE_DEADLINE_MINUTES";

/// What one run needs to derive and append a group.
///
/// `DuckDB`'s connection is not shared between threads, so the stage sits behind a lock that is
/// taken for each synchronous step and never held across an await.
struct Run<'a> {
    stage: &'a Mutex<DuckDbParquetStage>,
    committer: &'a LakehouseDataCommitter,
    catalog: &'a IcebergRestCatalog,
    contract: &'a LakehouseTableContract,
    shape: &'a TableShape,
}

impl Run<'_> {
    fn stage(&self) -> anyhow::Result<MutexGuard<'_, DuckDbParquetStage>> {
        self.stage
            .lock()
            .map_err(|_| anyhow::anyhow!("an earlier step of the derivation panicked"))
    }
}

/// The Iceberg snapshot `main` of `table` is at now.
async fn current_snapshot(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<i64> {
    catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id)
        .with_context(|| format!("{table} holds no snapshot"))
}

fn lakehouse_read() -> anyhow::Result<LakehouseRead> {
    let catalog = LakehouseCatalogConfig::from_env()
        .context("failed to configure the lakehouse catalog for the lineage derivation")?;
    let storage = R2ObjectStorageConfig::from_env()
        .context("failed to configure lakehouse R2 reads for the lineage derivation")?;
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

fn group_json(from_sido: &[String], to_sido: &[String]) -> serde_json::Map<String, Value> {
    let mut group = serde_json::Map::new();
    group.insert("from_sido".to_owned(), json!(from_sido.join(",")));
    group.insert("to_sido".to_owned(), json!(to_sido.join(",")));
    group
}

/// Derives one group and appends it once; what the summary keeps of it.
async fn derive_one(
    run: &Run<'_>,
    group: &LineageGroup,
    pins: LineagePins,
) -> anyhow::Result<Value> {
    let staged = {
        let stage = run.stage()?;
        load_inputs(&stage, LAKEHOUSE_CATALOG, group, pins)?;
        stage_group(&stage, run.shape, group, pins, chrono::Utc::now())?
    };
    let appended = match &staged.file {
        None => false,
        Some(file) => {
            // The re-run guard, read the way every load reads it (root ADR-0069), now: an earlier
            // group of this run has moved `main` since the run began.
            let already_ingested = run
                .catalog
                .load_ingested_batch_objects(LINEAGE_TABLE)
                .await?
                .with_context(|| format!("{LINEAGE_TABLE} does not exist"))?;
            let outcome = run
                .committer
                .append(DataAppend {
                    table_name: LINEAGE_TABLE,
                    contract: run.contract,
                    files: std::slice::from_ref(file),
                    batch: std::slice::from_ref(&staged.derivation_run_id),
                    already_ingested: &already_ingested,
                    summary: std::collections::BTreeMap::new(),
                })
                .await?;
            matches!(outcome, AppendOutcome::Appended { .. })
        }
    };
    Ok(json!({
        "derivation_run_id": staged.derivation_run_id,
        "rows": staged.rows,
        "appended": appended,
        "links_by_relation": staged.summary.links_by_relation,
        "links_by_grade": staged.summary.links_by_grade,
        "reconciliation": staged.summary.reconciliation,
        "same_day_facts": staged.summary.same_day_facts,
    }))
}

/// One pair's groups: held ones skipped, none started after `deadline`, a refusal recorded.
async fn run_pair(
    run: &Run<'_>,
    from: &Edition,
    to: &Edition,
    pins: LineagePins,
    deadline: Instant,
) -> anyhow::Result<Value> {
    let (before, after) = {
        let stage = run.stage()?;
        (
            sido_of_edition(&stage, pins, &from.snapshot_id)?,
            sido_of_edition(&stage, pins, &to.snapshot_id)?,
        )
    };
    if awaits_load(&before, &after) {
        println!(
            "parcel-lineage-waiting {}->{}: {} is not loaded into silver.parcel_boundaries yet",
            from.name, to.name, to.name
        );
        return Ok(
            json!({"from": from.name, "to": to.name, "waiting": "not loaded into silver.parcel_boundaries"}),
        );
    }
    let (mut derived, mut held, mut deferred, mut refused) = (vec![], vec![], vec![], vec![]);
    for (from_sido, to_sido) in sido_groups(&before, &after)? {
        let group = LineageGroup {
            from_snapshot_id: from.snapshot_id.clone(),
            to_snapshot_id: to.snapshot_id.clone(),
            from_date: from.date.clone(),
            to_date: to.date.clone(),
            from_sido,
            to_sido,
        };
        let mut entry = group_json(&group.from_sido, &group.to_sido);
        let is_held = {
            let stage = run.stage()?;
            let since = superseded_since(&stage, pins, &group.from_sido)?;
            is_held(&stage, &group, since)?
        };
        if is_held {
            held.push(Value::Object(entry));
        } else if Instant::now() >= deadline {
            deferred.push(Value::Object(entry));
        } else {
            match derive_one(run, &group, pins).await {
                Ok(Value::Object(outcome)) => {
                    entry.extend(outcome);
                    derived.push(Value::Object(entry));
                }
                Ok(other) => bail!("a derivation's outcome is not an object: {other}"),
                Err(error) => {
                    // Only the rules' own refusal is recorded; anything else stops the run.
                    let Some(codes) = error.downcast_ref::<UnrecordedDongCodes>() else {
                        return Err(error);
                    };
                    let reason = codes.to_string().chars().take(2000).collect::<String>();
                    entry.insert("reason".to_owned(), json!(reason));
                    entry.insert("unpaired_codes".to_owned(), json!(codes.codes));
                    refused.push(Value::Object(entry));
                }
            }
        }
    }
    Ok(json!({
        "from": from.name, "to": to.name,
        "pins": {
            "boundaries_from": {"table": BOUNDARIES_TABLE, "snapshot_id": pins.boundaries},
            "boundaries_to": {"table": BOUNDARIES_TABLE, "snapshot_id": pins.boundaries},
            "code_changes": {"table": CODE_CHANGE_TABLE, "snapshot_id": pins.code_changes},
            "history": {"table": HISTORY_TABLE, "snapshot_id": pins.history},
        },
        "derived": derived, "held": held, "deferred": deferred, "refused": refused,
    }))
}

fn count(pairs: &[Value], key: &str) -> usize {
    pairs
        .iter()
        .map(|pair| pair[key].as_array().map_or(0, Vec::len))
        .sum()
}

/// Derives every group of every edition pair the lineage does not hold yet and writes the summary.
pub(crate) async fn run() -> anyhow::Result<()> {
    let summary_path = required_env_value(SUMMARY_ENV)?;
    let minutes: f64 = required_env_value(DEADLINE_ENV)?
        .parse()
        .with_context(|| format!("{DEADLINE_ENV} must be a number of minutes"))?;
    if !(minutes.is_finite() && minutes > 0.0) {
        bail!("{DEADLINE_ENV} must be positive");
    }
    let deadline = Instant::now() + Duration::from_secs_f64(minutes * 60.0);
    let contract = industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == LINEAGE_TABLE)
        .with_context(|| format!("no contract for {LINEAGE_TABLE}"))?;
    let committer = LakehouseDataCommitter::from_env().await?;
    let shape = committer.table_shape(LINEAGE_TABLE).await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let work = Path::new(&summary_path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        work,
        &lakehouse_read()?,
    )?);
    let run = Run {
        stage: &stage,
        committer: &committer,
        catalog: &catalog,
        contract,
        shape: &shape,
    };
    let mut pairs = Vec::new();
    for (from, to) in edition_pairs()? {
        // One set of pins per pair: every group of it reads the same inputs.
        let pins = LineagePins {
            boundaries: current_snapshot(&catalog, BOUNDARIES_TABLE).await?,
            code_changes: current_snapshot(&catalog, CODE_CHANGE_TABLE).await?,
            history: current_snapshot(&catalog, HISTORY_TABLE).await?,
        };
        pairs.push(run_pair(&run, &from, &to, pins, deadline).await?);
    }
    drop(stage);
    let waiting = pairs
        .iter()
        .filter(|pair| pair.get("waiting").is_some())
        .map(|pair| {
            format!(
                "{}->{}",
                pair["from"].as_str().unwrap_or_default(),
                pair["to"].as_str().unwrap_or_default()
            )
        })
        .collect::<BTreeSet<_>>();
    let totals = json!({
        "derived": count(&pairs, "derived"),
        "deferred": count(&pairs, "deferred"),
        "refused": count(&pairs, "refused"),
        "waiting": waiting,
        "rules_version": RULES_VERSION,
    });
    let mut summary = json!({"job": "derive-parcel-lineage", "engine": "duckdb", "pairs": pairs});
    if let (Value::Object(summary), Value::Object(totals)) = (&mut summary, totals.clone()) {
        summary.extend(totals);
    }
    std::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("cannot write {summary_path}"))?;
    println!("parcel-lineage-scheduled-json {totals}");
    for pair in summary["pairs"].as_array().into_iter().flatten() {
        for group in pair["refused"].as_array().into_iter().flatten() {
            println!(
                "parcel-lineage-refused {}->{} {}->{}: {}",
                pair["from"].as_str().unwrap_or_default(),
                pair["to"].as_str().unwrap_or_default(),
                group["from_sido"].as_str().unwrap_or_default(),
                group["to_sido"].as_str().unwrap_or_default(),
                group["reason"]
                    .as_str()
                    .unwrap_or_default()
                    .chars()
                    .take(300)
                    .collect::<String>()
            );
        }
    }
    Ok(())
}
