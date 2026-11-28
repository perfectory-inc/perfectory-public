//! `load-industrial-land-sale-notice-silver`: the Silver half of the industrial land sale notice
//! lane (root ADR-0603).
//!
//! `industrial-land-sale-notice-silver-load.sh` runs it after `plan-silver-load-objects` fetched the
//! detail objects of the lane's three sources that the read table does not record. For each object,
//! oldest run first, `DuckDB` inside the publisher builds its rows
//! (`foundation_publisher_silver::industrial_land_sale_notice_stage`) and the publisher appends,
//! each in one Iceberg commit that records the object even when it holds no row (root ADR-0559,
//! ADR-0560): first the attachments the media store holds that are new or changed, to
//! `silver.industrial_land_sale_notice_attachment` (the index the media reconciler reads the store
//! through, root ADR-0387); then every read, to `silver.industrial_land_sale_notice_fetch_attempt`, whose
//! record the collector's next frontier and the next plan are decided from. A table is created by
//! its first commit. The last stdout line is `industrial-land-sale-notice-summary-json {json}`.
//!
//! `run-industrial-land-sale-notice-silver` is the lane's one command on AWS (config/aws-platform.contract.json
//! collect.lanes, root ADR-0624 A1, W109e): the plan (`plan-silver-load-objects`' own code, in this process,
//! with the lane contract's window and bound), then this load, then the late check, ending with the
//! `foundation-job-outcome` line, as `industrial-land-sale-notice-silver-load.sh` does on the host. On the
//! Lakekeeper catalog the tables are the reconciler's (root ADR-0624 P8) and, before each commit of new or
//! changed attachment rows, the media keys they name are written under `control/media_index/` for the
//! host's media reconciler, which cannot reach the catalog (coordinator 2026-11-28).

use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::Context;
use foundation_outbox::{object_storage::R2ObjectStorageConfig, R2ObjectStorage};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile,
};
use foundation_publisher_silver::{
    duckdb_parquet_stage::{DuckDbParquetStage, LakehouseRead},
    industrial_land_sale_notice_stage::{
        changed_media_keys, detail_source_slugs, planned_objects, refuse_repeated, stage_object,
        ATTACHMENT_TABLE, FETCH_ATTEMPT_TABLE,
    },
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::IcebergRestCatalog;
use serde_json::{json, Value};

use crate::public_data_control_support::required_env_value;

const PLAN_DIR_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_LAND_SALE_NOTICE_PLAN_DIR";
/// The lane's work directory on AWS (collect_lane.py gives `<env_prefix>_OUTPUT_DIR`).
const LANE_OUTPUT_ENV: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_LAND_SALE_NOTICE_SILVER_OUTPUT_DIR";

/// The lane contract's `silver_load`: (window days, the most objects one run loads).
fn silver_load_settings() -> anyhow::Result<(i64, usize)> {
    let contract: Value =
        serde_json::from_str(crate::industrial_land_sale_notice_collect::CONTRACT_JSON)
            .context("industrial-land-sale-notice.contract.json is not valid")?;
    let window = contract["silver_load"]["window_days"]
        .as_i64()
        .context("the contract's silver_load names no window_days")?;
    let bound = contract["silver_load"]["max_objects"]
        .as_u64()
        .context("the contract's silver_load names no max_objects")?;
    Ok((window, usize::try_from(bound)?))
}

fn contract(table: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == table)
        .with_context(|| format!("no contract for {table}"))
}

/// The snapshot `main` of `table` is at now; `None` for a table with none.
async fn head(catalog: &IcebergRestCatalog, table: &str) -> anyhow::Result<Option<i64>> {
    Ok(catalog
        .load_main_snapshot_history(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?
        .first()
        .map(|snapshot| snapshot.snapshot_id))
}

fn locked(stage: &Mutex<DuckDbParquetStage>) -> anyhow::Result<MutexGuard<'_, DuckDbParquetStage>> {
    stage
        .lock()
        .map_err(|_| anyhow::anyhow!("an earlier step of the load panicked"))
}

/// Appends one object's file (or none) to `table` on `expected_head`, recording the object.
async fn append(
    committer: &LakehouseDataCommitter,
    catalog: &IcebergRestCatalog,
    table: &str,
    file: Option<&StagedFile>,
    object_key: &str,
    expected_head: Option<i64>,
) -> anyhow::Result<bool> {
    let already_ingested = catalog
        .load_ingested_batch_objects(table)
        .await?
        .with_context(|| format!("{table} does not exist"))?;
    let outcome = committer
        .append_at_head(
            DataAppend {
                table_name: table,
                contract: contract(table)?,
                files: file.map_or(&[][..], std::slice::from_ref),
                batch: &[object_key.to_owned()],
                already_ingested: &already_ingested,
                summary: std::collections::BTreeMap::new(),
            },
            expected_head,
        )
        .await?;
    Ok(matches!(outcome, AppendOutcome::Appended { .. }))
}

/// Loads every planned object of the plan in `PLAN_DIR` and prints the summary line.
pub(crate) async fn run() -> anyhow::Result<()> {
    load(Path::new(&required_env_value(PLAN_DIR_ENV)?)).await
}

/// The lane's command: plan, load, late check, outcome line.
///
/// # Errors
/// As the plan and the load; and after loading them, the objects that waited longer than the window
/// fail the run, so a gap is seen (the host script's rule).
pub(crate) async fn run_lane() -> anyhow::Result<()> {
    let work = Path::new(&required_env_value(LANE_OUTPUT_ENV)?).join("details");
    let (window_days, max_objects) = silver_load_settings()?;
    let slugs = detail_source_slugs()?;
    let planned = crate::silver_load_objects::plan(&crate::silver_load_objects::PlanRequest {
        table: FETCH_ATTEMPT_TABLE,
        slugs: &slugs,
        window_days,
        max_objects,
        output_dir: &work,
    })
    .await?;
    if planned.owed == 0 {
        println!("foundation-job-outcome unchanged nothing owed");
        return Ok(());
    }
    load(&work).await?;
    anyhow::ensure!(
        planned.late == 0,
        "{} Bronze object(s) waited more than {window_days} days unloaded while the table was there; they are loaded now, and this run fails so the gap is seen",
        planned.late
    );
    println!("foundation-job-outcome changed");
    Ok(())
}

/// On the Lakekeeper catalog, where the attachment index's keys go for the host's media reconciler.
fn index_delta_store() -> anyhow::Result<Option<R2ObjectStorage>> {
    let lakekeeper = foundation_publisher_core::lakehouse_catalog::CatalogSettings::from_env()?
        .client
        .is_some();
    if !lakekeeper || !crate::media_index_delta::is_index_table(ATTACHMENT_TABLE)? {
        return Ok(None);
    }
    Ok(Some(R2ObjectStorage::from_config(
        R2ObjectStorageConfig::from_env()
            .context("the index delta is written with the lane's writer credential")?,
    )))
}

/// Loads every object of the plan in `plan_dir` and prints the summary line.
async fn load(plan_dir: &Path) -> anyhow::Result<()> {
    let objects = planned_objects(plan_dir, &detail_source_slugs()?)?;
    refuse_repeated(&objects)?;
    let committer = LakehouseDataCommitter::from_env().await?;
    // Both tables are born on their first commit (root ADR-0564 §1): the lake migration leaves an
    // absent table to the load that makes it.
    for table in [ATTACHMENT_TABLE, FETCH_ATTEMPT_TABLE] {
        committer.create_table_if_absent(contract(table)?).await?;
    }
    let reads_shape = committer.table_shape(FETCH_ATTEMPT_TABLE).await?;
    let attachments_shape = committer.table_shape(ATTACHMENT_TABLE).await?;
    // The catalog the environment names: the current one, or Lakekeeper with the lane's client.
    let catalog = foundation_publisher_core::lakehouse_catalog::CatalogSettings::from_env()?
        .rest_catalog()
        .await?;
    let stage = Mutex::new(DuckDbParquetStage::open_reading_lakehouse(
        plan_dir,
        &LakehouseRead::from_env()?,
    )?);
    let delta_store = index_delta_store()?;
    let ingested_at = chrono::Utc::now();
    let attachment_contract = contract(ATTACHMENT_TABLE)?;
    let mut loaded = Vec::with_capacity(objects.len());
    for object in &objects {
        let attachment_head = head(&catalog, ATTACHMENT_TABLE).await?;
        let staged = stage_object(
            &*locked(&stage)?,
            (&reads_shape, &attachments_shape),
            attachment_contract,
            object,
            attachment_head,
            ingested_at,
        )?;
        if let Some(store) = &delta_store {
            let keys = changed_media_keys(&*locked(&stage)?)?;
            if !keys.is_empty() {
                crate::media_index_delta::write_delta(
                    store,
                    ATTACHMENT_TABLE,
                    &object.object_key,
                    &keys,
                )
                .await?;
            }
        }
        let attachments_appended = append(
            &committer,
            &catalog,
            ATTACHMENT_TABLE,
            staged.attachments_file.as_ref(),
            &object.object_key,
            attachment_head,
        )
        .await?;
        let read_head = head(&catalog, FETCH_ATTEMPT_TABLE).await?;
        let reads_appended = append(
            &committer,
            &catalog,
            FETCH_ATTEMPT_TABLE,
            staged.reads_file.as_ref(),
            &object.object_key,
            read_head,
        )
        .await?;
        loaded.push(json!({
            "object": object.object_key,
            "reads": staged.reads,
            "attachments": staged.attachments,
            "attachment_appended_rows": staged.changed_attachments,
            "attachment_appended": attachments_appended,
            "read_appended_rows": staged.reads,
            "read_appended": reads_appended,
        }));
    }
    drop(stage);
    let summary: Value = json!({
        "job": "load-industrial-land-sale-notice-silver",
        "engine": "duckdb",
        "status": "loaded",
        "objects": loaded,
    });
    println!("industrial-land-sale-notice-summary-json {summary}");
    Ok(())
}
