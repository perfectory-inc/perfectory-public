//! `probe-parcel-registry` and `load-parcel-registry`: the next step of `silver.parcel_registry`
//! (root ADR-0113 §2·§3·§7(b), ADR-0243, ADR-0490 §4a).
//!
//! `parcel-registry-cycle.sh` asks which regions a run takes (`probe-parcel-registry`: the 시도 the
//! registry holds nothing for, or the regions no row registers the candidate edition for yet) and
//! runs one step per region (`load-parcel-registry`: bootstrap, advance or upgrade, chosen from
//! what the registry as of the edition holds). `DuckDB` inside the publisher builds the step
//! (`foundation_publisher_silver::parcel_registry_stage`) and the publisher appends it in one
//! Iceberg commit that records the run's id, so the same step is never appended twice (root
//! ADR-0559, ADR-0560). It was the Spark job `parcel_registry_to_silver.py`. The lines the script
//! reads are the ones the Spark job printed: `parcel-registry-probe {json}`,
//! `parcel-registry-advance-probe {json}` and `parcel-registry-summary-json {json}`.

use std::{path::PathBuf, sync::Mutex, time::Instant};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::LakehouseDataCommitter;
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    onbid_silver_stage::write,
    parcel_registry_stage::{
        advance_regions, build_step, missing_regions, snapshot_ids_through, Pins, StepRequest,
        BOUNDARY_TABLE, LINEAGE_TABLE, TABLE,
    },
    process_memory,
};
use lakehouse_infrastructure::{IcebergRestCatalog, LakehouseCatalogConfig};
use serde_json::json;

use crate::{
    onbid_silver_load::{
        append, contract, head, locked, pinned, seconds, served_snapshot_id, PARCEL_SOURCE,
    },
    public_data_control_support::required_env_value,
};

const PREFIX: &str = "FOUNDATION_PLATFORM_PARCEL_REGISTRY_";

fn optional(name: &str) -> Option<String> {
    std::env::var(format!("{PREFIX}{name}"))
        .ok()
        .filter(|value| !value.is_empty())
}

fn required(name: &str) -> anyhow::Result<String> {
    required_env_value(&format!("{PREFIX}{name}"))
}

fn sido(text: &str) -> Vec<String> {
    text.split(',').map(str::to_owned).collect()
}

/// The edition asked for: the served one unless another is named, and another only when the
/// caller says so (`ALLOW_NON_SERVED_EDITION=1`), as the Spark job's reader flag did.
fn to_snapshot() -> anyhow::Result<String> {
    let served = served_snapshot_id()?;
    let Some(asked) = optional("TO_SNAPSHOT_ID") else {
        return Ok(served);
    };
    ensure!(
        asked == served || optional("ALLOW_NON_SERVED_EDITION").as_deref() == Some("1"),
        "{asked} is not the served edition {served}; set {PREFIX}ALLOW_NON_SERVED_EDITION=1 to read another"
    );
    Ok(asked)
}

struct Opened {
    committer: LakehouseDataCommitter,
    catalog: IcebergRestCatalog,
    stage: Mutex<DuckDbParquetStage>,
    pins: Pins,
}

/// The catalog, a stage under `STAGE_DIR`, and the heads of the three tables, pinned once.
async fn open() -> anyhow::Result<Opened> {
    let committer = LakehouseDataCommitter::from_env().await?;
    let catalog = IcebergRestCatalog::new(LakehouseCatalogConfig::from_env()?)?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        &PathBuf::from(required("STAGE_DIR")?),
        &LakehouseRead::from_env()?,
    )?);
    let pins = Pins {
        registry: head(&catalog, TABLE).await?,
        boundaries: pinned(&catalog, BOUNDARY_TABLE).await?,
        lineage: head(&catalog, LINEAGE_TABLE).await?,
    };
    Ok(Opened {
        committer,
        catalog,
        stage,
        pins,
    })
}

/// Prints which regions a run takes; writes nothing.
pub(crate) async fn probe() -> anyhow::Result<()> {
    let question = required("PROBE")?;
    let snapshot = to_snapshot()?;
    let through = snapshot_ids_through(PARCEL_SOURCE, &snapshot)?;
    let opened = open().await?;
    let table_contract = contract(TABLE)?;
    let stage = &*locked(&opened.stage)?;
    match question.as_str() {
        "missing" => {
            let missing = missing_regions(stage, table_contract, opened.pins, &snapshot, &through)?;
            println!(
                "parcel-registry-probe {}",
                json!({"to_snapshot_id": snapshot, "missing": missing})
            );
        }
        "advance" => {
            let waiting = advance_regions(stage, table_contract, opened.pins, &snapshot, &through)?
                .into_iter()
                .map(|region| {
                    json!([
                        region.to_sido,
                        region.from_sido,
                        region.from_snapshot,
                        region.parcels
                    ])
                })
                .collect::<Vec<_>>();
            println!(
                "parcel-registry-advance-probe {}",
                json!({"to_snapshot_id": snapshot, "waiting": waiting})
            );
        }
        other => bail!("{PREFIX}PROBE is `missing` or `advance`, not {other:?}"),
    }
    Ok(())
}

/// Builds and appends one region's step, and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    let started = Instant::now();
    let request = StepRequest {
        to_snapshot: to_snapshot()?,
        to_date: required("TO_DATE")?,
        to_sido: sido(&required("TO_SIDO")?),
        from_sido: optional("FROM_SIDO").as_deref().map(sido),
        from_snapshot: optional("FROM_SNAPSHOT_ID"),
        bootstrap: optional("BOOTSTRAP").as_deref() == Some("1"),
    };
    ensure!(
        request.bootstrap || request.from_snapshot.is_some(),
        "{PREFIX}FROM_SNAPSHOT_ID is required unless {PREFIX}BOOTSTRAP=1"
    );
    let through = snapshot_ids_through(PARCEL_SOURCE, &request.to_snapshot)?;
    let opened = open().await?;
    let table_contract = contract(TABLE)?;
    // A table is born on its first commit (root ADR-0564 §1), as the Spark job made it.
    opened
        .committer
        .create_table_if_absent(table_contract)
        .await?;
    let shape = opened.committer.table_shape(TABLE).await?;
    // One instant for the step, to the second, as the Spark job took one.
    let recorded_at = DateTime::from_timestamp(Utc::now().timestamp(), 0)
        .context("the clock names no instant")?;
    let (step, staged) = {
        let stage = &*locked(&opened.stage)?;
        let step = build_step(
            stage,
            table_contract,
            opened.pins,
            &request,
            &through,
            recorded_at,
        )?;
        let staged = write(
            stage,
            &shape,
            TABLE,
            "registry_staged",
            &step.derivation_run_id,
        )?;
        (step, staged)
    };
    // Recorded by the run's id: the same inputs give the same id, and a step the table already
    // holds appends nothing. A step with no row writes nothing at all.
    let appended = match &staged {
        Some(file) => {
            append(
                &opened.committer,
                &opened.catalog,
                TABLE,
                Some(file),
                &step.derivation_run_id,
                opened.pins.registry,
            )
            .await?
        }
        None => false,
    };
    drop(opened.stage);
    let mut summary = json!({
        "job": "parcel_registry_to_silver",
        "engine": "duckdb",
        "mode": step.mode.name(),
        "rows": step.rows,
        "snapshot_parcels": step.snapshot_parcels,
        "derivation_run_id": step.derivation_run_id,
        "appended": appended,
        "to_snapshot_id": request.to_snapshot,
        "seconds": seconds(started),
        // The unit's limit is set from this (root ADR-0455): the most its cgroup held this run.
        "memory_peak_bytes": process_memory::cgroup_peak_bytes(),
    });
    for (name, value) in &step.counts {
        summary[name] = json!(value);
    }
    if let Some(path) = optional("SUMMARY_OUTPUT") {
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&summary)?),
        )
        .with_context(|| format!("cannot write {path}"))?;
    }
    println!("parcel-registry-summary-json {summary}");
    Ok(())
}
